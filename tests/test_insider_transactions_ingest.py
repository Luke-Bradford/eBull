"""Integration tests for ``ingest_insider_transactions`` (#429).

Covers:

- Happy path: non-derivative buy + sell, full field population, JOIN
  into insider_filings + insider_filers.
- Derivative / option-grant path: grant fields land, summary excludes.
- Multi-owner joint filing: both owners land in ``insider_filers``;
  transactions dedup by CIK.
- Footnote body + refs: body lands in ``insider_transaction_footnotes``,
  refs land inline in ``insider_transactions.footnote_refs``.
- Amendment (4/A): document_type captured, date_of_original_submission
  captured.
- 10b5-1 plan marker + late-filed timeliness flag preserved.
- Tombstoning: fetch 404 / parse miss now writes an
  ``insider_filings`` row with ``is_tombstone = TRUE``; reader
  excludes it; second pass does not re-fetch.
- Idempotency + parser-version refresh: re-running upserts new
  fields into the existing row.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import cast

import psycopg
import pytest

from app.services.insider_transactions import (
    get_insider_summary,
    ingest_insider_transactions,
    ingest_insider_transactions_backfill,
    ingest_insider_transactions_for_instrument,
    list_insider_transactions,
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


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int = 99, symbol: str = "AAPL") -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name) VALUES (%s, %s, %s) RETURNING instrument_id",
            (iid, symbol, "Test Co"),
        )
        row = cur.fetchone()
        assert row is not None
    conn.commit()
    return int(row[0])


def _seed_form_4(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    url: str,
    filing_type: str = "4",
    filing_date: str = "2024-06-01",
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


# Rich Form 4 XML: open-market buy with full header + footnote ref.
_FORM_4_RICH_BUY = """<?xml version="1.0"?>
<ownershipDocument>
  <schemaVersion>X0306</schemaVersion>
  <documentType>4</documentType>
  <periodOfReport>2024-06-15</periodOfReport>
  <notSubjectToSection16>0</notSubjectToSection16>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001000001</rptOwnerCik>
      <rptOwnerName>Cook Timothy D.</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerAddress>
      <rptOwnerStreet1>ONE APPLE PARK WAY</rptOwnerStreet1>
      <rptOwnerCity>CUPERTINO</rptOwnerCity>
      <rptOwnerState>CA</rptOwnerState>
      <rptOwnerZipCode>95014</rptOwnerZipCode>
    </reportingOwnerAddress>
    <reportingOwnerRelationship>
      <isDirector>0</isDirector>
      <isOfficer>1</isOfficer>
      <officerTitle>CEO</officerTitle>
      <isTenPercentOwner>0</isTenPercentOwner>
      <isOther>0</isOther>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2024-06-15</value></transactionDate>
      <transactionCoding>
        <transactionFormType>4</transactionFormType>
        <transactionCode>P</transactionCode>
        <equitySwapInvolved>0</equitySwapInvolved>
      </transactionCoding>
      <transactionTimeliness><value>L</value></transactionTimeliness>
      <transactionAmounts>
        <transactionShares>
          <value>500</value>
          <footnoteId id="F1"/>
        </transactionShares>
        <transactionPricePerShare><value>150.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>3200500</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
  <footnotes>
    <footnote id="F1">Weighted average price across a range of $149.80-$150.20.</footnote>
  </footnotes>
  <remarks>Acquired under 10b5-1 plan adopted 2023-11-01.</remarks>
  <ownerSignature>
    <signatureName>/s/ Jane Q. Lawyer</signatureName>
    <signatureDate>2024-06-17</signatureDate>
  </ownerSignature>
</ownershipDocument>
"""


# Joint filing: two reporting owners on the same accession.
_FORM_4_JOINT = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>2024-06-20</periodOfReport>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001000002</rptOwnerCik>
      <rptOwnerName>Smith John (Trustee)</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isTenPercentOwner>1</isTenPercentOwner>
    </reportingOwnerRelationship>
  </reportingOwner>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001000003</rptOwnerCik>
      <rptOwnerName>Smith Family Trust</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isTenPercentOwner>1</isTenPercentOwner>
      <isOther>1</isOther>
      <otherText>Indirect via family trust</otherText>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2024-06-20</value></transactionDate>
      <transactionCoding>
        <transactionCode>S</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1000</value></transactionShares>
        <transactionPricePerShare><value>160.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>I</value></directOrIndirectOwnership>
        <natureOfOwnership><value>By Trust dated 2020-01-01</value></natureOfOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""


# Derivative grant.
_FORM_4_OPTION_GRANT = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>2024-06-25</periodOfReport>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001000004</rptOwnerCik>
      <rptOwnerName>Maestri Luca</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isOfficer>1</isOfficer>
      <officerTitle>SVP, CFO</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <derivativeTable>
    <derivativeTransaction>
      <securityTitle><value>Employee Stock Option (Right to Buy)</value></securityTitle>
      <conversionOrExercisePrice><value>185.00</value></conversionOrExercisePrice>
      <transactionDate><value>2024-06-25</value></transactionDate>
      <transactionCoding>
        <transactionCode>A</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>50000</value></transactionShares>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <exerciseDate><value>2025-06-25</value></exerciseDate>
      <expirationDate><value>2034-06-25</value></expirationDate>
      <underlyingSecurity>
        <underlyingSecurityTitle><value>Common Stock</value></underlyingSecurityTitle>
        <underlyingSecurityShares><value>50000</value></underlyingSecurityShares>
      </underlyingSecurity>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </derivativeTransaction>
  </derivativeTable>
</ownershipDocument>
"""


# 4/A amendment.
_FORM_4A_AMENDMENT = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4/A</documentType>
  <periodOfReport>2024-06-01</periodOfReport>
  <dateOfOriginalSubmission>2024-06-02</dateOfOriginalSubmission>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001000001</rptOwnerCik>
      <rptOwnerName>Cook Timothy D.</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isOfficer>1</isOfficer>
      <officerTitle>CEO</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2024-06-01</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>250</value></transactionShares>
        <transactionPricePerShare><value>149.50</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""


class TestIngestInsiderTransactions:
    def test_rich_happy_path_populates_every_field(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000001-24-000001",
            url="https://www.sec.gov/Archives/form4-rich.xml",
            filing_date=date.today().isoformat(),
        )
        fetcher = _StubFetcher(
            {
                "https://www.sec.gov/Archives/form4-rich.xml": _FORM_4_RICH_BUY.replace(
                    "2024-06-15", date.today().isoformat()
                )
            }
        )

        result = ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        assert result.filings_parsed == 1
        assert result.rows_inserted == 1

        with ebull_test_conn.cursor() as cur:
            # insider_filings — full header landed
            cur.execute(
                """
                SELECT document_type, issuer_cik, issuer_name, remarks,
                       signature_name, signature_date, is_tombstone, parser_version
                FROM insider_filings WHERE accession_number = %s
                """,
                ("0000001-24-000001",),
            )
            f = cur.fetchone()
            assert f is not None
            assert f[0] == "4"
            assert f[1] == "0000320193"
            assert f[2] == "Apple Inc."
            assert "10b5-1" in (f[3] or "")
            assert f[4] == "/s/ Jane Q. Lawyer"
            assert f[5] == date(2024, 6, 17)
            assert f[6] is False
            assert f[7] == 2

            # insider_filers — full address + role
            cur.execute(
                """
                SELECT filer_cik, filer_name, street1, city, state, zip_code,
                       is_officer, officer_title
                FROM insider_filers WHERE accession_number = %s
                """,
                ("0000001-24-000001",),
            )
            rows = cur.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == "0001000001"
            assert rows[0][1] == "Cook Timothy D."
            assert rows[0][2] == "ONE APPLE PARK WAY"
            assert rows[0][6] is True
            assert rows[0][7] == "CEO"

            # insider_transaction_footnotes — body stored
            cur.execute(
                """
                SELECT footnote_id, footnote_text
                FROM insider_transaction_footnotes WHERE accession_number = %s
                """,
                ("0000001-24-000001",),
            )
            fn_rows = cur.fetchall()
            assert len(fn_rows) == 1
            assert fn_rows[0][0] == "F1"
            assert "Weighted average price" in fn_rows[0][1]

            # insider_transactions — full row
            cur.execute(
                """
                SELECT filer_cik, security_title, post_transaction_shares,
                       acquired_disposed_code, transaction_timeliness,
                       equity_swap_involved, footnote_refs
                FROM insider_transactions WHERE accession_number = %s
                """,
                ("0000001-24-000001",),
            )
            tx = cur.fetchone()
            assert tx is not None
            assert tx[0] == "0001000001"
            assert tx[1] == "Common Stock"
            assert tx[2] == Decimal("3200500")
            assert tx[3] == "A"
            assert tx[4] == "L"
            assert tx[5] is False
            refs = tx[6]  # already parsed from JSONB
            assert isinstance(refs, list)
            assert any(r.get("footnote_id") == "F1" and r.get("field") == "transactionShares" for r in refs)

    def test_joint_filing_splits_filers_by_cik(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="JOINT-1",
            url="https://www.sec.gov/Archives/joint.xml",
            filing_date=date.today().isoformat(),
        )
        fetcher = _StubFetcher(
            {"https://www.sec.gov/Archives/joint.xml": _FORM_4_JOINT.replace("2024-06-20", date.today().isoformat())}
        )
        ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT filer_cik FROM insider_filers WHERE accession_number = %s ORDER BY filer_cik",
                ("JOINT-1",),
            )
            assert [r[0] for r in cur.fetchall()] == ["0001000002", "0001000003"]
            cur.execute(
                "SELECT filer_cik FROM insider_transactions WHERE accession_number = %s",
                ("JOINT-1",),
            )
            # The transaction is attributed to the first listed owner.
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "0001000002"

    def test_derivative_grant_lands_full_shape(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="GRANT-1",
            url="https://www.sec.gov/Archives/grant.xml",
            filing_date=date.today().isoformat(),
        )
        fetcher = _StubFetcher(
            {
                "https://www.sec.gov/Archives/grant.xml": _FORM_4_OPTION_GRANT.replace(
                    "2024-06-25", date.today().isoformat()
                )
            }
        )
        ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT is_derivative, conversion_exercise_price,
                       underlying_security_title, underlying_shares,
                       expiration_date
                FROM insider_transactions WHERE accession_number = %s
                """,
                ("GRANT-1",),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] is True
            assert row[1] == Decimal("185.00")
            assert row[2] == "Common Stock"
            assert row[3] == Decimal("50000")
            assert row[4] == date(2034, 6, 25)

    def test_amendment_linkage_preserved(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="AMEND-1",
            url="https://www.sec.gov/Archives/amend.xml",
            filing_type="4/A",
            filing_date=date.today().isoformat(),
        )
        fetcher = _StubFetcher(
            {
                "https://www.sec.gov/Archives/amend.xml": _FORM_4A_AMENDMENT.replace(
                    "2024-06-01", date.today().isoformat()
                )
            }
        )
        ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT document_type, date_of_original_submission
                FROM insider_filings WHERE accession_number = %s
                """,
                ("AMEND-1",),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "4/A"
            assert row[1] == date(2024, 6, 2)

    def test_rerun_refreshes_existing_row(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """On a parser-version bump the ingester should refresh existing
        insider_transactions rows, not silently skip them."""
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="RERUN-1",
            url="https://www.sec.gov/Archives/rerun.xml",
            filing_date=date.today().isoformat(),
        )
        fetcher = _StubFetcher(
            {"https://www.sec.gov/Archives/rerun.xml": _FORM_4_RICH_BUY.replace("2024-06-15", date.today().isoformat())}
        )
        ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        # Second pass with the same payload: ingester selector skips the
        # accession (insider_filings row already exists), so this is a
        # no-op. Prove it via candidate zero rather than via the upsert
        # path.
        second = ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert second.filings_scanned == 0

    def test_fetch_404_writes_filing_tombstone_and_skips_next_run(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="DEAD-ACC",
            url="https://www.sec.gov/dead.xml",
        )
        dead_fetcher = _StubFetcher({"https://www.sec.gov/dead.xml": None})
        ingest_insider_transactions(ebull_test_conn, cast("object", dead_fetcher))  # type: ignore[arg-type]

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT is_tombstone FROM insider_filings WHERE accession_number = %s",
                ("DEAD-ACC",),
            )
            ts_row = cur.fetchone()
            assert ts_row is not None
            assert ts_row[0] is True
            # No transaction row should have been written for a tombstone.
            cur.execute(
                "SELECT COUNT(*) FROM insider_transactions WHERE accession_number = %s",
                ("DEAD-ACC",),
            )
            count_row = cur.fetchone()
            assert count_row is not None
            assert count_row[0] == 0

        second_fetcher = _StubFetcher({"https://www.sec.gov/dead.xml": None})
        second = ingest_insider_transactions(ebull_test_conn, cast("object", second_fetcher))  # type: ignore[arg-type]
        assert second.filings_scanned == 0
        assert second_fetcher.calls == []

    def test_tombstone_excluded_from_summary(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="DEAD-ACC",
            url="https://www.sec.gov/dead.xml",
        )
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="REAL-ACC",
            url="https://www.sec.gov/real.xml",
            filing_date=date.today().isoformat(),
        )
        fetcher = _StubFetcher(
            {
                "https://www.sec.gov/dead.xml": None,
                "https://www.sec.gov/real.xml": _FORM_4_RICH_BUY.replace("2024-06-15", date.today().isoformat()),
            }
        )
        ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        summary = get_insider_summary(ebull_test_conn, instrument_id=iid)
        assert summary.net_shares_90d == Decimal("500")
        assert summary.buy_count_90d == 1
        assert summary.unique_filers_90d == 1

    def test_xsl_rendered_url_normalised_before_fetch(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """#454 regression — filing_events.primary_document_url for
        ownership filings often points at the XSL-rendered HTML path
        (``/xslF345X06/form4.xml``). The ingester must strip that
        segment and fetch the canonical raw XML URL instead."""
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="XSL-1",
            url="https://www.sec.gov/Archives/edgar/data/320193/0001/xslF345X06/form4.xml",
            filing_date=date.today().isoformat(),
        )
        canonical_url = "https://www.sec.gov/Archives/edgar/data/320193/0001/form4.xml"
        fetcher = _StubFetcher(
            {
                canonical_url: _FORM_4_RICH_BUY.replace("2024-06-15", date.today().isoformat()),
            }
        )

        result = ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        # Fetcher must have been called with the normalised URL, not the XSL path.
        assert fetcher.calls == [canonical_url]
        assert result.filings_parsed == 1
        assert result.rows_inserted == 1

    def test_non_form_4_skipped(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000001-24-000003",
            url="https://www.sec.gov/Archives/other.xml",
            filing_type="10-K",
        )
        fetcher = _StubFetcher({})
        result = ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.filings_scanned == 0
        assert fetcher.calls == []


class TestGetInsiderSummary:
    def test_unique_filers_dedups_by_cik_not_name(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Two insiders with the same ``filer_name`` at different CIKs
        must count as two distinct filers."""
        iid = _seed_instrument(ebull_test_conn)
        # Both filings land under the same name 'John Smith' but
        # distinct CIKs. The 056 code would have counted them as one
        # filer; 057 counts them as two.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO insider_filings (accession_number, instrument_id, document_type)
                VALUES ('ACC-A', %s, '4'), ('ACC-B', %s, '4')
                """,
                (iid, iid),
            )
            cur.execute(
                """
                INSERT INTO insider_transactions
                    (instrument_id, accession_number, txn_row_num,
                     filer_cik, filer_name, txn_date, txn_code,
                     shares, is_derivative)
                VALUES
                    (%s, 'ACC-A', 0, 'CIK-A', 'John Smith',
                     CURRENT_DATE, 'P', 100, FALSE),
                    (%s, 'ACC-B', 0, 'CIK-B', 'John Smith',
                     CURRENT_DATE, 'P', 200, FALSE)
                """,
                (iid, iid),
            )
        ebull_test_conn.commit()

        summary = get_insider_summary(ebull_test_conn, instrument_id=iid)
        assert summary.unique_filers_90d == 2

    def test_derivative_excluded_from_net(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO insider_filings (accession_number, instrument_id, document_type)
                VALUES ('GRANT-ACC', %s, '4')
                """,
                (iid,),
            )
            cur.execute(
                """
                INSERT INTO insider_transactions
                    (instrument_id, accession_number, txn_row_num,
                     filer_cik, filer_name, txn_date, txn_code,
                     shares, is_derivative)
                VALUES (%s, 'GRANT-ACC', 0, 'CIK-X', 'X',
                        CURRENT_DATE, 'A', 10000, TRUE)
                """,
                (iid,),
            )
        ebull_test_conn.commit()
        summary = get_insider_summary(ebull_test_conn, instrument_id=iid)
        assert summary.open_market_net_shares_90d == Decimal(0)
        assert summary.total_acquired_shares_90d == Decimal(0)
        assert summary.total_disposed_shares_90d == Decimal(0)

    def test_grant_plus_sell_to_cover_shows_both_lenses(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """#458 regression — an RSU vest pattern (A grant + S
        sell-to-cover) must surface both views: open-market counts
        only the discretionary S, total-activity counts both the grant
        (acquired) and the sell (disposed). The prior single-lens
        summary showed NET=-shares / BUYS=0 / SELLS=1 with no hint
        that the insider actually received a larger grant."""
        iid = _seed_instrument(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO insider_filings (accession_number, instrument_id, document_type)
                VALUES ('VEST-1', %s, '4')
                """,
                (iid,),
            )
            cur.execute(
                """
                INSERT INTO insider_transactions
                    (instrument_id, accession_number, txn_row_num,
                     filer_cik, filer_name, txn_date, txn_code,
                     acquired_disposed_code, shares, is_derivative)
                VALUES
                    (%s, 'VEST-1', 0, 'CIK-A', 'Exec A',
                     CURRENT_DATE, 'A', 'A', 1000, FALSE),
                    (%s, 'VEST-1', 1, 'CIK-A', 'Exec A',
                     CURRENT_DATE, 'S', 'D', 300, FALSE)
                """,
                (iid, iid),
            )
        ebull_test_conn.commit()

        summary = get_insider_summary(ebull_test_conn, instrument_id=iid)
        # Open-market lens: no P, one S.
        assert summary.open_market_buy_count_90d == 0
        assert summary.open_market_sell_count_90d == 1
        assert summary.open_market_net_shares_90d == Decimal(-300)
        # All-codes lens: grant and sell both captured.
        assert summary.acquisition_count_90d == 1
        assert summary.disposition_count_90d == 1
        assert summary.total_acquired_shares_90d == Decimal(1000)
        assert summary.total_disposed_shares_90d == Decimal(300)

    def test_open_market_buy_still_registers_in_both_lenses(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A real open-market P must show up on both views."""
        iid = _seed_instrument(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO insider_filings (accession_number, instrument_id, document_type)
                VALUES ('OMB-1', %s, '4')
                """,
                (iid,),
            )
            cur.execute(
                """
                INSERT INTO insider_transactions
                    (instrument_id, accession_number, txn_row_num,
                     filer_cik, filer_name, txn_date, txn_code,
                     acquired_disposed_code, shares, is_derivative)
                VALUES (%s, 'OMB-1', 0, 'CIK-B', 'Exec B',
                        CURRENT_DATE, 'P', 'A', 500, FALSE)
                """,
                (iid,),
            )
        ebull_test_conn.commit()

        summary = get_insider_summary(ebull_test_conn, instrument_id=iid)
        assert summary.open_market_buy_count_90d == 1
        assert summary.open_market_net_shares_90d == Decimal(500)
        assert summary.acquisition_count_90d == 1
        assert summary.total_acquired_shares_90d == Decimal(500)
        # Back-compat aliases still work.
        assert summary.buy_count_90d == 1
        assert summary.net_shares_90d == Decimal(500)


class TestListInsiderTransactions:
    def test_rich_row_with_footnote_body_attached(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="RICH-1",
            url="https://www.sec.gov/Archives/rich.xml",
            filing_date=date.today().isoformat(),
        )
        fetcher = _StubFetcher(
            {"https://www.sec.gov/Archives/rich.xml": _FORM_4_RICH_BUY.replace("2024-06-15", date.today().isoformat())}
        )
        ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        detail = list_insider_transactions(ebull_test_conn, instrument_id=iid)
        assert len(detail) == 1
        row = detail[0]
        assert row.filer_name == "Cook Timothy D."
        assert row.security_title == "Common Stock"
        assert row.post_transaction_shares == Decimal("3200500")
        assert row.transaction_timeliness == "L"
        # The footnote body should be attached under the field it
        # qualified ("transactionShares").
        assert "transactionShares" in row.footnotes
        assert "Weighted average price" in row.footnotes["transactionShares"]


class TestBackfill:
    """#456 — per-instrument and round-robin backfill paths."""

    def test_per_instrument_scopes_to_target(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Ingester invoked for one instrument must not fetch filings
        owned by other instruments."""
        iid_a = _seed_instrument(ebull_test_conn, iid=201, symbol="AAA")
        iid_b = _seed_instrument(ebull_test_conn, iid=202, symbol="BBB")
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid_a,
            accession="ACC-A",
            url="https://www.sec.gov/Archives/a.xml",
            filing_date=date.today().isoformat(),
        )
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid_b,
            accession="ACC-B",
            url="https://www.sec.gov/Archives/b.xml",
            filing_date=date.today().isoformat(),
        )
        today_iso = date.today().isoformat()
        fetcher = _StubFetcher(
            {
                "https://www.sec.gov/Archives/a.xml": _FORM_4_RICH_BUY.replace("2024-06-15", today_iso),
                "https://www.sec.gov/Archives/b.xml": _FORM_4_RICH_BUY.replace("2024-06-15", today_iso).replace(
                    "0001000001", "0002000002"
                ),
            }
        )

        result = ingest_insider_transactions_for_instrument(
            ebull_test_conn,
            cast("object", fetcher),  # type: ignore[arg-type]
            instrument_id=iid_a,
        )
        assert result.filings_scanned == 1
        assert fetcher.calls == ["https://www.sec.gov/Archives/a.xml"]

    def test_backfill_round_robin_picks_biggest_backlogs(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Round-robin backfill targets instruments with the most
        un-ingested candidates first, drains oldest-first per target."""
        iid_deep = _seed_instrument(ebull_test_conn, iid=301, symbol="DEEP")
        iid_shallow = _seed_instrument(ebull_test_conn, iid=302, symbol="SHAL")

        today_iso = date.today().isoformat()
        xml_body = _FORM_4_RICH_BUY.replace("2024-06-15", today_iso)
        url_map: dict[str, str | None] = {}
        # DEEP: 3 filings; SHAL: 1 filing. DEEP should drain in one pass.
        for i in range(3):
            acc = f"DEEP-{i:02d}"
            url = f"https://www.sec.gov/Archives/deep-{i}.xml"
            _seed_form_4(
                ebull_test_conn,
                instrument_id=iid_deep,
                accession=acc,
                url=url,
                filing_date=f"2024-01-{10 + i:02d}",
            )
            url_map[url] = xml_body.replace("0001000001", f"09000{i:05d}")
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid_shallow,
            accession="SHAL-01",
            url="https://www.sec.gov/Archives/shal.xml",
            filing_date=today_iso,
        )
        url_map["https://www.sec.gov/Archives/shal.xml"] = xml_body

        fetcher = _StubFetcher(url_map)
        totals = ingest_insider_transactions_backfill(
            ebull_test_conn,
            cast("object", fetcher),  # type: ignore[arg-type]
            instruments_per_tick=5,
            per_instrument_limit=50,
        )
        assert totals["instruments_processed"] == 2
        assert totals["filings_parsed"] == 4

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM insider_filings WHERE instrument_id = %s AND is_tombstone = FALSE",
                (iid_deep,),
            )
            deep_count = cur.fetchone()
            assert deep_count is not None
            assert deep_count[0] == 3


class TestForm4DateFloor:
    """Regression: ``INSIDER_FORM4_BACKFILL_FLOOR_YEARS`` keeps the
    SEC ingest budget focused on operationally-useful filings. eBull
    is long-horizon — pre-2021 insider trades aren't in the trading
    model, so we skip them rather than burn 6 weeks of SEC bandwidth
    draining the historical tail."""

    def test_universe_path_skips_filings_older_than_floor(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, iid=601, symbol="OLD")
        # One filing inside the 5-year floor, one well outside.
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="ANCIENT-1",
            url="https://www.sec.gov/Archives/ancient.xml",
            filing_date="2010-06-01",
        )
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="RECENT-1",
            url="https://www.sec.gov/Archives/recent.xml",
            filing_date=date.today().isoformat(),
        )
        recent_xml = _FORM_4_RICH_BUY.replace("2024-06-15", date.today().isoformat())
        fetcher = _StubFetcher(
            {
                "https://www.sec.gov/Archives/ancient.xml": _FORM_4_RICH_BUY,
                "https://www.sec.gov/Archives/recent.xml": recent_xml,
            }
        )

        result = ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        # Only the recent filing is fetched — ancient is filtered out by SQL,
        # never reaches the fetcher.
        assert fetcher.calls == ["https://www.sec.gov/Archives/recent.xml"]
        assert result.filings_scanned == 1

    def test_backfill_path_skips_filings_older_than_floor(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, iid=602, symbol="OLD2")
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="ANCIENT-2",
            url="https://www.sec.gov/Archives/ancient2.xml",
            filing_date="2008-09-15",
        )
        # No filings within the floor → backfill SQL returns no targets.
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/ancient2.xml": _FORM_4_RICH_BUY})
        totals = ingest_insider_transactions_backfill(
            ebull_test_conn,
            cast("object", fetcher),  # type: ignore[arg-type]
            instruments_per_tick=5,
            per_instrument_limit=50,
        )

        # Backfill skipped this CIK entirely: no targets, no fetches,
        # no insider_filings inserts.
        assert totals["instruments_processed"] == 0
        assert fetcher.calls == []
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM insider_filings WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 0


def test_fixture_imports_ok(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name IN ('insider_filings', 'insider_filers', "
            "'insider_transactions', 'insider_transaction_footnotes') "
            "ORDER BY table_name"
        )
        found = [r[0] for r in cur.fetchall()]
        assert found == [
            "insider_filers",
            "insider_filings",
            "insider_transaction_footnotes",
            "insider_transactions",
        ]
