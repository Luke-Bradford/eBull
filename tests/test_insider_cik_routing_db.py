"""DB-tier tests for #828 PR-1 — insider writer CIK routing.

Covers the two new SQL mechanisms end-to-end:

1. ``upsert_filing`` / ``upsert_form_3_filing`` route entity-level rows +
   the filing_events bridge by the PARSED issuer CIK when the discovery
   instrument is outside a non-empty issuer sibling set (the owner-stream
   mislink: a BAC Form 4 discovered via Berkshire's EDGAR feed).
2. The ``filing_events`` ownership-form write guard in
   ``app/services/filings.py`` blocks re-creation of a non-sibling binding
   once the accession is parsed (owner stream walked AFTER the issuer's
   parse, or a full submissions re-walk).

Spec: docs/proposals/etl/2026-07-22-828-insider-cik-routing.md §PR-1.
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg

from app.providers.filings import FilingSearchResult
from app.services.filings import _upsert_filing
from app.services.insider_form3_ingest import upsert_form_3_filing
from app.services.insider_transactions import (
    parse_form_3_xml,
    parse_form_4_xml,
    upsert_filing,
)

_ISSUER_CIK = "0000070858"  # plays "BAC"
_OWNER_CIK = "0001067983"  # plays "BRK" (reporting owner's own issuer CIK)

_FORM_4_MISLINK = f"""<?xml version="1.0"?>
<ownershipDocument>
  <schemaVersion>X0306</schemaVersion>
  <documentType>4</documentType>
  <periodOfReport>2026-06-15</periodOfReport>
  <issuer>
    <issuerCik>{_ISSUER_CIK}</issuerCik>
    <issuerName>Bank Test Corp</issuerName>
    <issuerTradingSymbol>BAC</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>{_OWNER_CIK}</rptOwnerCik>
      <rptOwnerName>Holding Test Inc</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isTenPercentOwner>1</isTenPercentOwner>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-06-15</value></transactionDate>
      <transactionCoding>
        <transactionFormType>4</transactionFormType>
        <transactionCode>P</transactionCode>
        <equitySwapInvolved>0</equitySwapInvolved>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1000</value></transactionShares>
        <transactionPricePerShare><value>40.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>1001000</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""

_FORM_3_MISLINK = f"""<?xml version="1.0"?>
<ownershipDocument>
  <schemaVersion>X0202</schemaVersion>
  <documentType>3</documentType>
  <periodOfReport>2026-06-15</periodOfReport>
  <issuer>
    <issuerCik>{_ISSUER_CIK}</issuerCik>
    <issuerName>Bank Test Corp</issuerName>
    <issuerTradingSymbol>BAC</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>{_OWNER_CIK}</rptOwnerCik>
      <rptOwnerName>Holding Test Inc</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isTenPercentOwner>1</isTenPercentOwner>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeHolding>
      <securityTitle><value>Common Stock</value></securityTitle>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>1000000</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeHolding>
  </nonDerivativeTable>
  <ownerSignature>
    <signatureName>A Signer</signatureName>
    <signatureDate>2026-06-16</signatureDate>
  </ownerSignature>
</ownershipDocument>
"""


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str, cik: str | None) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
            (iid, symbol, f"{symbol} Test Co"),
        )
        if cik is not None:
            cur.execute(
                """
                INSERT INTO external_identifiers
                    (instrument_id, provider, identifier_type, identifier_value, is_primary)
                VALUES (%s, 'sec', 'cik', %s, TRUE)
                """,
                (iid, cik),
            )
    conn.commit()
    return iid


def _seed_owner_filing_event(
    conn: psycopg.Connection[tuple], *, instrument_id: int, accession: str, filing_type: str
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO filing_events
                (instrument_id, filing_date, filing_type, provider,
                 provider_filing_id, primary_document_url)
            VALUES (%s, '2026-06-16', %s, 'sec', %s, 'https://sec.test/doc.xml')
            """,
            (instrument_id, filing_type, accession),
        )
    conn.commit()


def _filing_event_instruments(conn: psycopg.Connection[tuple], accession: str) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT instrument_id FROM filing_events "
            "WHERE provider = 'sec' AND provider_filing_id = %s ORDER BY instrument_id",
            (accession,),
        )
        return [int(r[0]) for r in cur.fetchall()]


class TestForm4MislinkRouting:
    def test_owner_stream_mislink_routes_to_issuer(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        issuer = _seed_instrument(ebull_test_conn, 201, "BAC", _ISSUER_CIK)
        owner = _seed_instrument(ebull_test_conn, 300, "BRKB", _OWNER_CIK)
        accession = "0000000828-26-000001"
        _seed_owner_filing_event(ebull_test_conn, instrument_id=owner, accession=accession, filing_type="4")

        parsed = parse_form_4_xml(_FORM_4_MISLINK)
        assert parsed is not None
        upsert_filing(
            ebull_test_conn,
            instrument_id=owner,  # discovery-time linkage = the OWNER's instrument
            accession_number=accession,
            primary_document_url="https://sec.test/doc.xml",
            parsed=parsed,
            filed_at=datetime(2026, 6, 16, 12, 0, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT instrument_id FROM insider_filings WHERE accession_number = %s",
                (accession,),
            )
            row = cur.fetchone()
            assert row is not None
            assert int(row[0]) == issuer

            cur.execute(
                "SELECT DISTINCT instrument_id FROM insider_transactions WHERE accession_number = %s",
                (accession,),
            )
            assert [int(r[0]) for r in cur.fetchall()] == [issuer]

            cur.execute(
                "SELECT DISTINCT instrument_id FROM ownership_insiders_observations "
                "WHERE source_accession = %s AND known_to IS NULL",
                (accession,),
            )
            assert [int(r[0]) for r in cur.fetchall()] == [issuer]

        # The owner's bridge row (the L2 pollution) is gone; the issuer's
        # bridge row exists so the filing is immediately visible on the
        # issuer's per-instrument read paths.
        assert _filing_event_instruments(ebull_test_conn, accession) == [issuer]

    def test_unambiguous_history_wins_inside_sibling_set(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_instrument(ebull_test_conn, 201, "BAC", _ISSUER_CIK)
        preferred = _seed_instrument(ebull_test_conn, 202, "BAC.B", _ISSUER_CIK)
        owner = _seed_instrument(ebull_test_conn, 300, "BRKB", _OWNER_CIK)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO instrument_cik_history (instrument_id, cik, effective_from, source_event) "
                "VALUES (%s, %s, '2020-01-01', 'imported')",
                (preferred, _ISSUER_CIK),
            )
        ebull_test_conn.commit()
        accession = "0000000828-26-000002"

        parsed = parse_form_4_xml(_FORM_4_MISLINK)
        assert parsed is not None
        upsert_filing(
            ebull_test_conn,
            instrument_id=owner,
            accession_number=accession,
            primary_document_url="https://sec.test/doc.xml",
            parsed=parsed,
            filed_at=datetime(2026, 6, 16, 12, 0, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT instrument_id FROM insider_filings WHERE accession_number = %s",
                (accession,),
            )
            row = cur.fetchone()
            assert row is not None
            assert int(row[0]) == preferred
            # Observations fan out across BOTH siblings regardless of the
            # entity-row pick.
            cur.execute(
                "SELECT DISTINCT instrument_id FROM ownership_insiders_observations "
                "WHERE source_accession = %s AND known_to IS NULL ORDER BY instrument_id",
                (accession,),
            )
            assert [int(r[0]) for r in cur.fetchall()] == [201, 202]

    def test_rewash_repairs_historically_mislinked_accession(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Codex ckpt-2 findings 1+2: re-parsing an accession whose entity
        rows + observations are ALREADY bound to the owner instrument
        (pre-#828 historical state / PR-2 rewash path) must move the entity
        rows, tombstone the owner's live observations, and refresh the
        owner's _current."""
        issuer = _seed_instrument(ebull_test_conn, 201, "BAC", _ISSUER_CIK)
        owner = _seed_instrument(ebull_test_conn, 300, "BRKB", _OWNER_CIK)
        accession = "0000000828-26-000008"
        _seed_owner_filing_event(ebull_test_conn, instrument_id=owner, accession=accession, filing_type="4")

        # First parse simulating the pre-#828 mislinked state: entity rows +
        # observations under the OWNER. Cheapest faithful setup is a real
        # parse with the issuer temporarily unroutable (no extid row yet is
        # not an option — issuer already seeded — so write the wrong state
        # directly through the pre-#828 shape).
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO insider_filings
                    (accession_number, instrument_id, document_type, issuer_cik,
                     parser_version, is_tombstone)
                VALUES (%s, %s, '4', %s, 0, FALSE)
                """,
                (accession, owner, _ISSUER_CIK),
            )
            cur.execute(
                """
                INSERT INTO ownership_insiders_observations
                    (instrument_id, holder_cik, holder_name, ownership_nature,
                     shares, source, source_document_id, source_accession,
                     filed_at, period_end, ingest_run_id)
                VALUES (%s, %s, 'Holding Test Inc', 'direct',
                        1000, 'form4', %s, %s,
                        '2026-06-16T12:00:00Z', '2026-06-15', gen_random_uuid())
                """,
                (owner, _OWNER_CIK, f"{accession}#legacy", accession),
            )
        ebull_test_conn.commit()

        parsed = parse_form_4_xml(_FORM_4_MISLINK)
        assert parsed is not None
        upsert_filing(
            ebull_test_conn,
            instrument_id=owner,  # rewash invokes with the stored (wrong) instrument
            accession_number=accession,
            primary_document_url="https://sec.test/doc.xml",
            parsed=parsed,
            filed_at=datetime(2026, 6, 16, 12, 0, tzinfo=UTC),
            is_rewash=True,
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            # Entity row MOVED (ON CONFLICT now updates instrument_id).
            cur.execute(
                "SELECT instrument_id FROM insider_filings WHERE accession_number = %s",
                (accession,),
            )
            row = cur.fetchone()
            assert row is not None
            assert int(row[0]) == issuer
            # Owner's legacy observation tombstoned; only issuer rows live.
            cur.execute(
                "SELECT DISTINCT instrument_id FROM ownership_insiders_observations "
                "WHERE source_accession = %s AND known_to IS NULL",
                (accession,),
            )
            assert [int(r[0]) for r in cur.fetchall()] == [issuer]
            # Owner's _current snapshot dropped the rows.
            cur.execute(
                "SELECT COUNT(*) FROM ownership_insiders_current WHERE instrument_id = %s",
                (owner,),
            )
            row = cur.fetchone()
            assert row is not None
            assert int(row[0]) == 0
        assert _filing_event_instruments(ebull_test_conn, accession) == [issuer]

    def test_unroutable_issuer_keeps_discovery_linkage(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # Issuer CIK maps to NO in-universe instrument (empty sibling set) —
        # the 24.7k-row unroutable cohort keeps today's behaviour.
        owner = _seed_instrument(ebull_test_conn, 300, "BRKB", _OWNER_CIK)
        accession = "0000000828-26-000003"
        _seed_owner_filing_event(ebull_test_conn, instrument_id=owner, accession=accession, filing_type="4")

        xml = _FORM_4_MISLINK.replace(_ISSUER_CIK, "0009999999")
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        upsert_filing(
            ebull_test_conn,
            instrument_id=owner,
            accession_number=accession,
            primary_document_url="https://sec.test/doc.xml",
            parsed=parsed,
            filed_at=datetime(2026, 6, 16, 12, 0, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT instrument_id FROM insider_filings WHERE accession_number = %s",
                (accession,),
            )
            row = cur.fetchone()
            assert row is not None
            assert int(row[0]) == owner
        # Discovery-time bridge row untouched.
        assert _filing_event_instruments(ebull_test_conn, accession) == [owner]


class TestForm3MislinkRouting:
    def test_owner_stream_mislink_routes_to_issuer(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        issuer = _seed_instrument(ebull_test_conn, 201, "BAC", _ISSUER_CIK)
        owner = _seed_instrument(ebull_test_conn, 300, "BRKB", _OWNER_CIK)
        accession = "0000000828-26-000004"
        _seed_owner_filing_event(ebull_test_conn, instrument_id=owner, accession=accession, filing_type="3")

        parsed = parse_form_3_xml(_FORM_3_MISLINK)
        assert parsed is not None
        upsert_form_3_filing(
            ebull_test_conn,
            instrument_id=owner,
            accession_number=accession,
            primary_document_url="https://sec.test/doc.xml",
            parsed=parsed,
            filed_at=datetime(2026, 6, 16, 12, 0, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT instrument_id FROM insider_filings WHERE accession_number = %s",
                (accession,),
            )
            row = cur.fetchone()
            assert row is not None
            assert int(row[0]) == issuer
            cur.execute(
                "SELECT DISTINCT instrument_id FROM insider_initial_holdings WHERE accession_number = %s",
                (accession,),
            )
            assert [int(r[0]) for r in cur.fetchall()] == [issuer]
        assert _filing_event_instruments(ebull_test_conn, accession) == [issuer]


class TestOwnershipFilingEventWriteGuard:
    def _search_result(self, accession: str, filing_type: str = "4") -> FilingSearchResult:
        return FilingSearchResult(
            provider_filing_id=accession,
            symbol="BAC",
            filed_at=datetime(2026, 6, 16, 12, 0, tzinfo=UTC),
            filing_type=filing_type,
            period_of_report=None,
            primary_document_url="https://sec.test/doc.xml",
        )

    def test_guard_blocks_owner_rewrite_after_parse(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        issuer = _seed_instrument(ebull_test_conn, 201, "BAC", _ISSUER_CIK)
        owner = _seed_instrument(ebull_test_conn, 300, "BRKB", _OWNER_CIK)
        accession = "0000000828-26-000005"

        parsed = parse_form_4_xml(_FORM_4_MISLINK)
        assert parsed is not None
        upsert_filing(
            ebull_test_conn,
            instrument_id=owner,
            accession_number=accession,
            primary_document_url="https://sec.test/doc.xml",
            parsed=parsed,
            filed_at=datetime(2026, 6, 16, 12, 0, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        # Owner stream walked AFTER the parse (or full re-walk): the fe
        # writer must refuse to re-create the owner binding…
        assert _upsert_filing(ebull_test_conn, str(owner), "sec", self._search_result(accession)) is False
        # …while the issuer sibling stays writable…
        assert _upsert_filing(ebull_test_conn, str(issuer), "sec", self._search_result(accession)) is True
        ebull_test_conn.commit()
        assert _filing_event_instruments(ebull_test_conn, accession) == [issuer]

    def test_guard_fails_open_for_unparsed_and_non_ownership(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        owner = _seed_instrument(ebull_test_conn, 300, "BRKB", _OWNER_CIK)
        # Unparsed ownership accession — allowed (discovery must proceed).
        assert _upsert_filing(ebull_test_conn, str(owner), "sec", self._search_result("0000000828-26-000006")) is True
        # Non-ownership form — guard not applicable.
        assert (
            _upsert_filing(
                ebull_test_conn,
                str(owner),
                "sec",
                self._search_result("0000000828-26-000007", filing_type="8-K"),
            )
            is True
        )
        ebull_test_conn.commit()
