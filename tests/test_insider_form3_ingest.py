"""Integration tests for ``ingest_form_3_filings`` (#768 PR 2/N).

Covers:
- Happy path: header + non-derivative + derivative + footnote land
  across insider_filings + insider_filers + insider_transaction_footnotes
  + insider_initial_holdings.
- Tombstones on fetch 404 / parse miss; second pass does not re-fetch.
- Per-instrument scope + non-Form-3 filings excluded.
- XSL-rendered URL canonicalisation (Form 3 reuses the Form 4 helper
  since the XSL prefix is the same across Forms 3/4/5).
- Idempotency: re-running upserts in place; replace-then-insert on
  holdings drops stale rows from a prior parse.
"""

from __future__ import annotations

from decimal import Decimal

import psycopg
import pytest

from app.services.insider_form3_ingest import (
    ingest_form_3_filings,
    ingest_form_3_filings_for_instrument,
    list_baseline_only_insider_holdings,
)

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


class _StubFetcher:
    def __init__(self, by_url: dict[str, str | None]) -> None:
        self._by_url = by_url
        self.calls: list[str] = []

    def fetch_document_text(self, absolute_url: str) -> str | None:
        self.calls.append(absolute_url)
        return self._by_url.get(absolute_url)


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int = 991, symbol: str = "AAPL") -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name) VALUES (%s, %s, %s) RETURNING instrument_id",
            (iid, symbol, "Test Co"),
        )
        row = cur.fetchone()
        assert row is not None
    conn.commit()
    return int(row[0])


def _seed_filing(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    url: str,
    filing_type: str = "3",
    filing_date: str = "2026-01-15",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO filing_events
                (instrument_id, filing_date, filing_type, provider,
                 provider_filing_id, primary_document_url)
            VALUES (%s, %s, %s, 'sec', %s, %s)
            """,
            (instrument_id, filing_date, filing_type, accession, url),
        )
    conn.commit()


_FORM_3_RICH = """<?xml version="1.0"?>
<ownershipDocument>
  <schemaVersion>X0202</schemaVersion>
  <documentType>3</documentType>
  <periodOfReport>2026-01-15</periodOfReport>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001000001</rptOwnerCik>
      <rptOwnerName>Smith, Jane</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerAddress>
      <rptOwnerStreet1>1 Apple Park Way</rptOwnerStreet1>
      <rptOwnerCity>Cupertino</rptOwnerCity>
      <rptOwnerState>CA</rptOwnerState>
      <rptOwnerZipCode>95014</rptOwnerZipCode>
    </reportingOwnerAddress>
    <reportingOwnerRelationship>
      <isOfficer>1</isOfficer>
      <officerTitle>Chief Financial Officer</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeHolding>
      <securityTitle><value>Common Stock</value></securityTitle>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>50000</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership>
          <value>I</value>
          <footnoteId id="F1"/>
        </directOrIndirectOwnership>
        <natureOfOwnership>
          <value>By Trust</value>
          <footnoteId id="F1"/>
        </natureOfOwnership>
      </ownershipNature>
    </nonDerivativeHolding>
  </nonDerivativeTable>
  <derivativeTable>
    <derivativeHolding>
      <securityTitle><value>Stock Option (Right to Buy)</value></securityTitle>
      <conversionOrExercisePrice><value>120.00</value></conversionOrExercisePrice>
      <exerciseDate><value>2025-01-01</value></exerciseDate>
      <expirationDate><value>2030-01-01</value></expirationDate>
      <underlyingSecurity>
        <underlyingSecurityTitle><value>Common Stock</value></underlyingSecurityTitle>
        <underlyingSecurityShares><value>10000</value></underlyingSecurityShares>
      </underlyingSecurity>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>10000</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </derivativeHolding>
  </derivativeTable>
  <footnotes>
    <footnote id="F1">Held by family trust of which the reporting person is trustee.</footnote>
  </footnotes>
  <ownerSignature>
    <signatureName>Jane Smith</signatureName>
    <signatureDate>2026-01-16</signatureDate>
  </ownerSignature>
</ownershipDocument>
"""


# ---------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------


class TestRichHappyPath:
    def test_raw_payload_persisted_for_form3_xml(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Form 3 ingester must persist the XML body to
        ``filing_raw_documents`` before parsing — operator audit
        2026-05-03 + PR #808 contract."""
        from app.services import raw_filings

        iid = _seed_instrument(ebull_test_conn)
        url = "https://www.sec.gov/Archives/edgar/data/320193/000119312526RAW001/form3.xml"
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="RAW-FORM3-26-000001",
            url=url,
        )
        fetcher = _StubFetcher({url: _FORM_3_RICH})
        ingest_form_3_filings(ebull_test_conn, fetcher)

        doc = raw_filings.read_raw(
            ebull_test_conn,
            accession_number="RAW-FORM3-26-000001",
            document_kind="form3_xml",
        )
        assert doc is not None
        assert "<ownershipDocument>" in doc.payload
        assert doc.parser_version == "form3-v1"
        assert doc.source_url == url

    def test_full_filing_lands_across_four_tables(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        url = "https://www.sec.gov/Archives/edgar/data/320193/000119312526001000/form3.xml"
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="0001193125-26-001000",
            url=url,
        )
        fetcher = _StubFetcher({url: _FORM_3_RICH})

        result = ingest_form_3_filings(ebull_test_conn, fetcher)
        assert result.filings_scanned == 1
        assert result.filings_parsed == 1
        assert result.rows_inserted == 2  # 1 non-derivative + 1 derivative
        assert result.fetch_errors == 0
        assert result.parse_misses == 0

        with ebull_test_conn.cursor() as cur:
            # insider_filings: header captured, document_type='3'.
            cur.execute(
                "SELECT document_type, period_of_report, signature_name, is_tombstone "
                "FROM insider_filings WHERE accession_number = '0001193125-26-001000'"
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "3"
            assert row[1].isoformat() == "2026-01-15"
            assert row[2] == "Jane Smith"
            assert row[3] is False

            # insider_filers: one row.
            cur.execute(
                "SELECT filer_cik, is_officer, officer_title FROM insider_filers "
                "WHERE accession_number = '0001193125-26-001000'"
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "0001000001"
            assert row[1] is True
            assert row[2] == "Chief Financial Officer"

            # insider_transaction_footnotes: F1 body persisted.
            cur.execute(
                "SELECT footnote_id, footnote_text FROM insider_transaction_footnotes "
                "WHERE accession_number = '0001193125-26-001000'"
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "F1"
            assert "family trust" in row[1]

            # insider_initial_holdings: 2 rows, row_num interleaved.
            cur.execute(
                "SELECT row_num, is_derivative, shares, direct_indirect, security_title "
                "FROM insider_initial_holdings "
                "WHERE accession_number = '0001193125-26-001000' "
                "ORDER BY row_num"
            )
            rows = cur.fetchall()
            assert len(rows) == 2
            assert rows[0][0] == 0
            assert rows[0][1] is False
            assert rows[0][2] == Decimal("50000")
            assert rows[0][3] == "I"
            assert rows[0][4] == "Common Stock"
            assert rows[1][0] == 1
            assert rows[1][1] is True
            assert rows[1][2] == Decimal("10000")
            assert rows[1][3] == "D"


class TestTombstones:
    def test_fetch_404_writes_tombstone_and_skips_next_run(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        url = "https://www.sec.gov/Archives/edgar/data/320193/000099/form3.xml"
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000000099-26-000001",
            url=url,
        )
        fetcher = _StubFetcher({url: None})  # 404

        result = ingest_form_3_filings(ebull_test_conn, fetcher)
        assert result.fetch_errors == 1
        assert result.filings_parsed == 0

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT is_tombstone FROM insider_filings WHERE accession_number = '0000000099-26-000001'")
            row = cur.fetchone()
            assert row is not None
            assert row[0] is True

        # Second pass does not re-fetch — the tombstone row anchors
        # the LEFT JOIN exclusion in the candidate selector.
        result2 = ingest_form_3_filings(ebull_test_conn, fetcher)
        assert result2.filings_scanned == 0

    def test_parse_miss_writes_tombstone(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        url = "https://www.sec.gov/Archives/edgar/data/320193/000098/form3.xml"
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000000098-26-000001",
            url=url,
        )
        fetcher = _StubFetcher({url: "<not-an-ownership-doc/>"})

        result = ingest_form_3_filings(ebull_test_conn, fetcher)
        assert result.parse_misses == 1

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT is_tombstone FROM insider_filings WHERE accession_number = '0000000098-26-000001'")
            row = cur.fetchone()
            assert row is not None
            assert row[0] is True


class TestScoping:
    def test_form_4_filings_are_not_picked_up_by_form_3_ingester(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        # Form 4 in filing_events with no Form 3 — universe-wide scan
        # should report zero candidates (filing_type = '4' fails the
        # candidate selector's IN ('3', '3/A')).
        iid = _seed_instrument(ebull_test_conn)
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000000097-26-000001",
            url="https://example.test/form4.xml",
            filing_type="4",
        )
        fetcher = _StubFetcher({})

        result = ingest_form_3_filings(ebull_test_conn, fetcher)
        assert result.filings_scanned == 0
        # Fetch was never called.
        assert fetcher.calls == []

    def test_per_instrument_scope_excludes_other_instruments(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid_a = _seed_instrument(ebull_test_conn, iid=991, symbol="AAPL")
        iid_b = _seed_instrument(ebull_test_conn, iid=992, symbol="MSFT")
        url_a = "https://example.test/aapl-form3.xml"
        url_b = "https://example.test/msft-form3.xml"
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid_a,
            accession="0000000091-26-000001",
            url=url_a,
        )
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid_b,
            accession="0000000092-26-000001",
            url=url_b,
        )
        fetcher = _StubFetcher({url_a: _FORM_3_RICH, url_b: _FORM_3_RICH})

        result = ingest_form_3_filings_for_instrument(
            ebull_test_conn,
            fetcher,
            instrument_id=iid_a,
        )
        assert result.filings_scanned == 1
        assert result.filings_parsed == 1
        # Only the AAPL URL should have been fetched.
        assert fetcher.calls == [url_a]


class TestIdempotency:
    def test_rerun_refreshes_holdings_and_drops_stale_rows(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # Re-parse drops rows that no longer appear in the latest XML.
        # Simulated by ingesting the rich XML, then ingesting a smaller
        # XML at the same accession (parser version "bump" simulated
        # by stripping the derivative table).
        iid = _seed_instrument(ebull_test_conn)
        url = "https://example.test/form3.xml"
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000000093-26-000001",
            url=url,
        )

        fetcher_v1 = _StubFetcher({url: _FORM_3_RICH})
        ingest_form_3_filings(ebull_test_conn, fetcher_v1)
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM insider_initial_holdings WHERE accession_number = '0000000093-26-000001'")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 2

        # Re-ingest at the same accession would normally short-circuit
        # via the "no existing insider_filings row" gate. To exercise
        # the upsert path explicitly, call upsert_form_3_filing
        # directly with the new parsed shape.
        from app.services.insider_form3_ingest import upsert_form_3_filing
        from app.services.insider_transactions import parse_form_3_xml

        smaller = _FORM_3_RICH.split("<derivativeTable>")[0] + _FORM_3_RICH.split("</derivativeTable>")[1]
        parsed = parse_form_3_xml(smaller)
        assert parsed is not None

        upsert_form_3_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession_number="0000000093-26-000001",
            primary_document_url=url,
            parsed=parsed,
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM insider_initial_holdings WHERE accession_number = '0000000093-26-000001'")
            row = cur.fetchone()
            assert row is not None
            # Stale derivative row dropped; only the non-derivative
            # row remains.
            assert row[0] == 1


class TestParserVersionRefresh:
    def test_existing_filing_below_current_parser_version_is_re_picked(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        # Codex round 1+2 review of #768 PR2: bumping
        # _FORM3_PARSER_VERSION must trigger re-ingest of stored
        # accessions (the module docstring promises this). Pin the
        # selector branch so a future regression that drops the
        # ``OR fil.parser_version < %s`` clause is caught.
        iid = _seed_instrument(ebull_test_conn)
        url = "https://example.test/form3.xml"
        accession = "0000000111-26-000001"
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession=accession,
            url=url,
        )

        # Pre-populate insider_filings with a stale parser_version.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO insider_filings (
                    accession_number, instrument_id, document_type,
                    primary_document_url, parser_version, is_tombstone
                ) VALUES (%s, %s, '3', %s, 0, FALSE)
                """,
                (accession, iid, url),
            )
        ebull_test_conn.commit()

        # Even though the filing already has an insider_filings row,
        # parser_version=0 is below _FORM3_PARSER_VERSION=1 so the
        # selector picks it up for re-parse.
        fetcher = _StubFetcher({url: _FORM_3_RICH})
        result = ingest_form_3_filings(ebull_test_conn, fetcher)
        assert result.filings_parsed == 1

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT parser_version FROM insider_filings WHERE accession_number = %s",
                (accession,),
            )
            row = cur.fetchone()
            assert row is not None
            # Parser version bumped to current.
            assert row[0] >= 1


class TestUpsertFailureTombstone:
    def test_upsert_exception_writes_tombstone_so_scheduler_does_not_retry(
        self, ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Codex round 2 review of #768 PR2: a persistent upsert
        # failure (e.g. DB constraint violation, deterministic bug in
        # the upsert path) must tombstone the accession so the
        # scheduler doesn't re-fetch the same dead XML on every tick.
        # Pre-fix the except branch only rollback'd + continue'd,
        # leaving the accession eligible for re-fetch forever.
        from app.services import insider_form3_ingest

        iid = _seed_instrument(ebull_test_conn)
        url = "https://example.test/form3.xml"
        accession = "0000000222-26-000001"
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession=accession,
            url=url,
        )

        # Force the upsert path to raise. Monkey-patch is more
        # contract-faithful than dropping FKs at runtime — we want
        # to test the EXCEPT branch, not a specific kind of DB
        # failure.
        def _boom(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("simulated upsert failure")

        monkeypatch.setattr(insider_form3_ingest, "upsert_form_3_filing", _boom)

        fetcher = _StubFetcher({url: _FORM_3_RICH})
        result = ingest_form_3_filings(ebull_test_conn, fetcher)
        # Upsert raised — accession tombstoned, parsed_count=0.
        assert result.filings_parsed == 0

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT is_tombstone FROM insider_filings WHERE accession_number = %s",
                (accession,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] is True

        # Second pass: with the tombstone written, the candidate
        # selector excludes the row (parser_version >= current,
        # tombstone or not). Confirm by un-patching and re-running.
        monkeypatch.undo()
        result2 = ingest_form_3_filings(ebull_test_conn, fetcher)
        assert result2.filings_scanned == 0


class TestStaleChildCleanup:
    def test_reparse_to_smaller_xml_drops_stale_footnotes_and_filers(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        # Codex review of #768 PR2: the original "replace-on-reparse"
        # contract only DELETE-then-INSERT'd holdings. Footnotes and
        # filers were ON CONFLICT-only — a parser version that stops
        # emitting a footnote or secondary filer would leave the
        # stale rows pinned to the accession. Pin the cleanup
        # behaviour so a future regression of either DELETE is caught.
        from app.services.insider_form3_ingest import upsert_form_3_filing
        from app.services.insider_transactions import parse_form_3_xml

        iid = _seed_instrument(ebull_test_conn)
        url = "https://example.test/form3.xml"
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000000095-26-000001",
            url=url,
        )
        # First parse — rich XML with one footnote.
        parsed_v1 = parse_form_3_xml(_FORM_3_RICH)
        assert parsed_v1 is not None
        upsert_form_3_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession_number="0000000095-26-000001",
            primary_document_url=url,
            parsed=parsed_v1,
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM insider_transaction_footnotes WHERE accession_number = '0000000095-26-000001'"
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 1

        # Second parse — strip the footnote from the XML.
        smaller = _FORM_3_RICH.split("<footnotes>")[0] + _FORM_3_RICH.split("</footnotes>")[1]
        parsed_v2 = parse_form_3_xml(smaller)
        assert parsed_v2 is not None
        assert len(parsed_v2.footnotes) == 0

        upsert_form_3_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession_number="0000000095-26-000001",
            primary_document_url=url,
            parsed=parsed_v2,
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM insider_transaction_footnotes WHERE accession_number = '0000000095-26-000001'"
            )
            row = cur.fetchone()
            assert row is not None
            # Stale footnote dropped on reparse.
            assert row[0] == 0


class TestAmendmentPath:
    def test_3a_amendment_filing_picked_up_by_candidate_selector(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        # filing_type IN ('3', '3/A') in the selector — confirm 3/A
        # amendments aren't accidentally excluded by an over-tight
        # equality predicate.
        iid = _seed_instrument(ebull_test_conn)
        url = "https://example.test/form3a.xml"
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000000096-26-000001",
            url=url,
            filing_type="3/A",
        )
        amended_xml = _FORM_3_RICH.replace(
            "<documentType>3</documentType>",
            "<documentType>3/A</documentType>",
        )
        fetcher = _StubFetcher({url: amended_xml})

        result = ingest_form_3_filings(ebull_test_conn, fetcher)
        assert result.filings_parsed == 1

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT document_type FROM insider_filings WHERE accession_number = '0000000096-26-000001'")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "3/A"


class TestUrlCanonicalisation:
    def test_xsl_rendered_url_normalised_before_fetch(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # SEC primaryDocument for Forms 3/4/5 commonly points at an
        # XSL-rendered HTML view. The candidate selector strips the
        # xslF345 prefix so the fetcher pulls raw XML, not HTML.
        # Reuses the Form 4 helper (the prefix is shared across forms).
        iid = _seed_instrument(ebull_test_conn)
        rendered = "https://www.sec.gov/Archives/edgar/data/320193/000099/xslF345X06/form3.xml"
        canonical = "https://www.sec.gov/Archives/edgar/data/320193/000099/form3.xml"
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000000094-26-000001",
            url=rendered,
        )
        fetcher = _StubFetcher({canonical: _FORM_3_RICH})

        ingest_form_3_filings(ebull_test_conn, fetcher)
        # Fetch hit the canonical URL, not the rendered one.
        assert fetcher.calls == [canonical]


# ---------------------------------------------------------------------
# #768 PR3 — baseline-only reader
# ---------------------------------------------------------------------


def _seed_form_4_for_filer(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    filer_cik: str,
    filer_name: str,
    accession: str,
    txn_date: str = "2026-01-20",
) -> None:
    """Plant a non-tombstoned Form 4 row for ``filer_cik`` so the
    baseline-only reader excludes them. Mirrors what
    ``ingest_insider_transactions`` would write."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO insider_filings (
                accession_number, instrument_id, document_type,
                primary_document_url, parser_version, is_tombstone
            ) VALUES (%s, %s, '4', 'https://example.test/form4.xml', 1, FALSE)
            """,
            (accession, instrument_id),
        )
        cur.execute(
            """
            INSERT INTO insider_filers (
                accession_number, filer_cik, filer_name, is_officer
            ) VALUES (%s, %s, %s, TRUE)
            """,
            (accession, filer_cik, filer_name),
        )
        cur.execute(
            """
            INSERT INTO insider_transactions (
                instrument_id, accession_number, txn_row_num,
                filer_name, filer_cik, txn_date, txn_code, shares
            ) VALUES (%s, %s, 0, %s, %s, %s, 'P', 100)
            """,
            (instrument_id, accession, filer_name, filer_cik, txn_date),
        )
    conn.commit()


class TestBaselineOnlyReader:
    def test_returns_form_3_holding_when_filer_has_no_form_4_activity(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        # Operationally meaningful case: officer received a Form 3
        # initial holding and never traded after — invisible without
        # the baseline reader.
        iid = _seed_instrument(ebull_test_conn)
        url = "https://example.test/form3.xml"
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000000301-26-000001",
            url=url,
        )
        fetcher = _StubFetcher({url: _FORM_3_RICH})
        ingest_form_3_filings(ebull_test_conn, fetcher)

        rows = list_baseline_only_insider_holdings(ebull_test_conn, instrument_id=iid)

        # _FORM_3_RICH has 2 holdings: 1 non-derivative + 1 derivative.
        # No Form 4 activity for filer 0001000001 → both surface.
        assert len(rows) == 2
        # Largest-first ordering by shares.
        assert rows[0].is_derivative is False
        assert rows[0].shares == Decimal("50000")
        assert rows[1].is_derivative is True
        assert rows[1].shares == Decimal("10000")
        assert rows[0].filer_cik == "0001000001"
        assert rows[0].filer_name == "Smith, Jane"

    def test_excludes_filers_with_form_4_activity(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # Filer who has both Form 3 baseline AND Form 4 transactions
        # should NOT appear in the baseline-only list — their
        # cumulative balance is derivable from the latest
        # post_transaction_shares observation. Including them here
        # would double-count the per-filer wedge on the ownership
        # ring.
        iid = _seed_instrument(ebull_test_conn)
        url = "https://example.test/form3.xml"
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000000302-26-000001",
            url=url,
        )
        fetcher = _StubFetcher({url: _FORM_3_RICH})
        ingest_form_3_filings(ebull_test_conn, fetcher)

        # Plant a Form 4 row for the same filer.
        _seed_form_4_for_filer(
            ebull_test_conn,
            instrument_id=iid,
            filer_cik="0001000001",
            filer_name="Smith, Jane",
            accession="0000000302-26-000002",
        )

        rows = list_baseline_only_insider_holdings(ebull_test_conn, instrument_id=iid)
        assert rows == []

    def test_excludes_tombstoned_form_3_filings(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # A tombstoned Form 3 (fetch / parse failure) should not
        # surface its (zero) holdings via the baseline reader. The
        # tombstone path writes the filings row but no holdings rows;
        # this test pins the INNER JOIN exclusion contract by directly
        # inserting a tombstoned filing + a phantom holding row.
        iid = _seed_instrument(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO insider_filings (
                    accession_number, instrument_id, document_type,
                    primary_document_url, parser_version, is_tombstone
                ) VALUES (%s, %s, '3', 'https://example.test/dead.xml', 1, TRUE)
                """,
                ("0000000303-26-000001", iid),
            )
            cur.execute(
                """
                INSERT INTO insider_initial_holdings (
                    instrument_id, accession_number, row_num,
                    filer_cik, filer_name, as_of_date,
                    security_title, shares, is_derivative
                ) VALUES (%s, %s, 0, %s, %s, %s, 'Common Stock', 999, FALSE)
                """,
                (iid, "0000000303-26-000001", "0009999998", "Phantom", "2026-01-15"),
            )
        ebull_test_conn.commit()

        rows = list_baseline_only_insider_holdings(ebull_test_conn, instrument_id=iid)
        assert rows == []

    def test_3a_amendment_at_same_as_of_date_supersedes_original(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        # Codex review of #768 PR3: the realistic 3/A case keeps the
        # same periodOfReport as the original — the tie-break must
        # prefer the amendment via accession_number. Without the
        # tie-break the original could win, silently surfacing stale
        # baseline shares on the ownership ring.
        iid = _seed_instrument(ebull_test_conn)
        as_of = "2026-02-01"
        with ebull_test_conn.cursor() as cur:
            # Original Form 3 — earlier accession sequence.
            for accn, doc_type, shares in (
                ("0000000305-26-000001", "3", 4000),  # original
                ("0000000305-26-000002", "3/A", 4500),  # amendment, same as_of
            ):
                cur.execute(
                    """
                    INSERT INTO insider_filings (
                        accession_number, instrument_id, document_type,
                        primary_document_url, parser_version, is_tombstone
                    ) VALUES (%s, %s, %s, 'https://example.test/x.xml', 1, FALSE)
                    """,
                    (accn, iid, doc_type),
                )
                cur.execute(
                    """
                    INSERT INTO insider_initial_holdings (
                        instrument_id, accession_number, row_num,
                        filer_cik, filer_name, as_of_date,
                        security_title, shares, is_derivative
                    ) VALUES (%s, %s, 0, '0001000300', 'Roe, Richard', %s, 'Common Stock', %s, FALSE)
                    """,
                    (iid, accn, as_of, shares),
                )
        ebull_test_conn.commit()

        rows = list_baseline_only_insider_holdings(ebull_test_conn, instrument_id=iid)
        # Amendment wins via accession_number DESC tie-break.
        assert len(rows) == 1
        assert rows[0].shares == Decimal("4500")
        assert rows[0].as_of_date.isoformat() == as_of

    def test_picks_latest_as_of_date_per_filer_security_pair(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # If a filer has two Form 3 amendments (3 + 3/A) for the same
        # security, the reader must surface only the latest snapshot
        # — older amendments are superseded.
        iid = _seed_instrument(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            for accn, as_of, shares in (
                ("0000000304-26-000001", "2026-01-10", 5000),
                ("0000000304-26-000002", "2026-03-15", 7500),
            ):
                cur.execute(
                    """
                    INSERT INTO insider_filings (
                        accession_number, instrument_id, document_type,
                        primary_document_url, parser_version, is_tombstone
                    ) VALUES (%s, %s, '3', 'https://example.test/x.xml', 1, FALSE)
                    """,
                    (accn, iid),
                )
                cur.execute(
                    """
                    INSERT INTO insider_initial_holdings (
                        instrument_id, accession_number, row_num,
                        filer_cik, filer_name, as_of_date,
                        security_title, shares, is_derivative
                    ) VALUES (%s, %s, 0, '0001000099', 'Doe, John', %s, 'Common Stock', %s, FALSE)
                    """,
                    (iid, accn, as_of, shares),
                )
        ebull_test_conn.commit()

        rows = list_baseline_only_insider_holdings(ebull_test_conn, instrument_id=iid)
        assert len(rows) == 1
        assert rows[0].shares == Decimal("7500")
        assert rows[0].as_of_date.isoformat() == "2026-03-15"
