"""Form 4 3y ingest retention cap — #1233 §4.3 PR4.

Pins the contracts:

1. ``INSIDER_FORM4_RETENTION_YEARS == 3`` is the single source of truth.
2. ``form4_retention_cutoff`` is calendar-year arithmetic, Feb 29 anchors
   to Feb 28 in non-leap target years, returns a ``date`` (not datetime).
3. ``form4_within_retention`` is INCLUSIVE at the boundary; lower bound
   only (future-dated rows accepted).
4. Every Form 4 writer chokepoint honours the cap:
   - Legacy universe SELECT (``ingest_insider_transactions``)
   - Legacy backfill outer aggregate (``ingest_insider_transactions_backfill``)
   - Legacy backfill inner per-instrument SELECT (PR4 gap fix)
   - Legacy per-instrument selector (``ingest_insider_transactions_for_instrument``,
     PR4 gap fix)
   - Manifest-worker pre-fetch gate (``insider_345._parse_form4``)
   - Bulk dataset ingester transactions loop (``sec_insider_dataset_ingest``)
   - Bulk dataset ingester holdings loop (Form 4 / 4-A only; Form 3
     baselines untouched per PR10 scope)
5. ``filed_at IS NULL`` on a manifest row tombstones cleanly (matches the
   existing missing-metadata pattern in ``_parse_form4``).
6. ``refresh_insiders_current`` continues to aggregate pre-existing
   observations alongside post-cap rows without regression (steady-state
   cumulative-rollup invariant per spec §4.3 amendment).

Existing rows are not deleted by the cap (#1233 §6.3 is the only purge
event). These tests assert *insert* behaviour only.
"""

from __future__ import annotations

import csv
import io
import zipfile
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import psycopg
import pytest

from app.services.insider_transactions import (
    INSIDER_FORM4_RETENTION_YEARS,
    form4_retention_cutoff,
    form4_within_retention,
    ingest_insider_transactions,
    ingest_insider_transactions_backfill,
    ingest_insider_transactions_for_instrument,
)
from app.services.ownership_observations import refresh_insiders_current
from app.services.sec_insider_dataset_ingest import ingest_insider_dataset_archive
from app.services.sec_manifest import record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

# ---------------------------------------------------------------------------
# Pure helper contracts
# ---------------------------------------------------------------------------


class TestRetentionConstantAndCutoff:
    def test_constant_is_3_years(self) -> None:
        assert INSIDER_FORM4_RETENTION_YEARS == 3

    def test_cutoff_is_now_minus_3_calendar_years(self) -> None:
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        assert form4_retention_cutoff(now=ref) == date(2023, 5, 20)

    def test_cutoff_feb_29_anchors_to_feb_28_in_non_leap_target(self) -> None:
        # 2024 leap; 2024 - 3 = 2021 (non-leap) → cutoff = 2021-02-28.
        ref = datetime(2024, 2, 29, 12, 0, 0, tzinfo=UTC)
        assert form4_retention_cutoff(now=ref) == date(2021, 2, 28)

    def test_cutoff_returns_date_not_datetime(self) -> None:
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        result = form4_retention_cutoff(now=ref)
        assert isinstance(result, date)
        assert not isinstance(result, datetime)


class TestForm4WithinRetention:
    def test_at_boundary_accepted(self) -> None:
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        cutoff = form4_retention_cutoff(now=ref)
        assert form4_within_retention(cutoff, now=ref) is True

    def test_one_day_before_boundary_rejected(self) -> None:
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        cutoff = form4_retention_cutoff(now=ref)
        assert form4_within_retention(cutoff - timedelta(days=1), now=ref) is False

    def test_one_day_after_boundary_accepted(self) -> None:
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        cutoff = form4_retention_cutoff(now=ref)
        assert form4_within_retention(cutoff + timedelta(days=1), now=ref) is True

    def test_future_filing_accepted(self) -> None:
        """Cap is a lower bound only; future-dated rows (operator clock
        skew, weird provider) survive."""
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        assert form4_within_retention(ref.date() + timedelta(days=1), now=ref) is True

    def test_ancient_filing_rejected(self) -> None:
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        assert form4_within_retention(date(2010, 6, 1), now=ref) is False


# ---------------------------------------------------------------------------
# Helpers shared by every writer-level test
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.integration


class _StubFetcher:
    def __init__(self, by_url: dict[str, str | None]) -> None:
        self._by_url = by_url
        self.calls: list[str] = []

    def fetch_document_text(self, absolute_url: str) -> str | None:
        self.calls.append(absolute_url)
        return self._by_url.get(absolute_url)


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
            "VALUES (%s, %s, %s, TRUE) RETURNING instrument_id",
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
    filing_date: str,
    filing_type: str = "4",
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


# Minimal valid Form 4 XML — parser shape detail doesn't matter for cap
# tests; what matters is whether the fetcher gets called.
_FORM_4_MINIMAL = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>2025-06-15</periodOfReport>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001000099</rptOwnerCik>
      <rptOwnerName>Test Insider</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isOfficer>1</isOfficer>
      <officerTitle>CFO</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2025-06-15</value></transactionDate>
      <transactionCoding>
        <transactionCode>P</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>100</value></transactionShares>
        <transactionPricePerShare><value>200.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""


# ---------------------------------------------------------------------------
# Legacy filing_events path — universe + backfill + per-instrument
# ---------------------------------------------------------------------------


class TestLegacyUniversePath:
    def test_universe_select_skips_pre_3y(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        iid = _seed_instrument(ebull_test_conn, iid=701, symbol="OLDU")
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="UNIV-OLD-1",
            url="https://www.sec.gov/Archives/old.xml",
            # 5y ago → outside 3y cap, would have been INSIDE the
            # legacy 5y floor. Pins the 5→3 tightening.
            filing_date=(date.today() - timedelta(days=365 * 5)).isoformat(),
        )
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="UNIV-RECENT-1",
            url="https://www.sec.gov/Archives/recent.xml",
            filing_date=date.today().isoformat(),
        )

        recent_xml = _FORM_4_MINIMAL.replace("2025-06-15", date.today().isoformat())
        fetcher = _StubFetcher(
            {
                "https://www.sec.gov/Archives/old.xml": _FORM_4_MINIMAL,
                "https://www.sec.gov/Archives/recent.xml": recent_xml,
            }
        )

        result = ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        assert fetcher.calls == ["https://www.sec.gov/Archives/recent.xml"]
        assert result.filings_scanned == 1


class TestLegacyBackfillOuterAndInner:
    def test_backfill_outer_aggregate_skips_pre_3y(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        iid = _seed_instrument(ebull_test_conn, iid=702, symbol="OLDB")
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="BACK-OLD-1",
            url="https://www.sec.gov/Archives/back-old.xml",
            filing_date="2010-06-01",
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/back-old.xml": _FORM_4_MINIMAL})
        totals = ingest_insider_transactions_backfill(
            ebull_test_conn,
            cast("object", fetcher),  # type: ignore[arg-type]
            instruments_per_tick=5,
            per_instrument_limit=50,
        )
        assert totals["instruments_processed"] == 0
        assert fetcher.calls == []

    def test_backfill_inner_select_skips_pre_3y_gap_fix(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """PR4 gap fix: outer aggregate picks an instrument by count of
        inside-cap unfetched filings, then the inner per-instrument
        SELECT also honours the cap. Pre-PR4 the inner SELECT was
        floor-free, so an instrument with one fresh filing + many
        ancient filings burned per_instrument_limit slots on ancient
        work."""
        iid = _seed_instrument(ebull_test_conn, iid=703, symbol="MIX")
        # 1 fresh filing inside the 3y cap.
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="MIX-RECENT-1",
            url="https://www.sec.gov/Archives/mix-recent.xml",
            filing_date=date.today().isoformat(),
        )
        # 3 pre-3y filings that would have been pulled oldest-first
        # if the inner SELECT lacked the floor predicate.
        for i, anc in enumerate(["2010-01-01", "2011-01-01", "2012-01-01"]):
            _seed_form_4(
                ebull_test_conn,
                instrument_id=iid,
                accession=f"MIX-ANC-{i}",
                url=f"https://www.sec.gov/Archives/mix-anc-{i}.xml",
                filing_date=anc,
            )

        recent_xml = _FORM_4_MINIMAL.replace("2025-06-15", date.today().isoformat())
        fetcher = _StubFetcher(
            {
                "https://www.sec.gov/Archives/mix-recent.xml": recent_xml,
                "https://www.sec.gov/Archives/mix-anc-0.xml": _FORM_4_MINIMAL,
                "https://www.sec.gov/Archives/mix-anc-1.xml": _FORM_4_MINIMAL,
                "https://www.sec.gov/Archives/mix-anc-2.xml": _FORM_4_MINIMAL,
            }
        )
        totals = ingest_insider_transactions_backfill(
            ebull_test_conn,
            cast("object", fetcher),  # type: ignore[arg-type]
            instruments_per_tick=5,
            per_instrument_limit=50,
        )
        # ONE instrument processed, ONE fetch — the fresh filing only.
        # If the inner SELECT lacked the predicate the fetcher would
        # have been called 4 times (3 ancient oldest-first + 1 fresh).
        assert totals["instruments_processed"] == 1
        assert fetcher.calls == ["https://www.sec.gov/Archives/mix-recent.xml"]


class TestLegacyForInstrument:
    def test_for_instrument_inner_select_skips_pre_3y_gap_fix(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """PR4 gap fix for the targeted per-instrument selector. Pre-PR4
        this entry point had no floor at all."""
        iid = _seed_instrument(ebull_test_conn, iid=704, symbol="TARG")
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="TARG-OLD-1",
            url="https://www.sec.gov/Archives/targ-old.xml",
            filing_date="2010-06-01",
        )
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="TARG-RECENT-1",
            url="https://www.sec.gov/Archives/targ-recent.xml",
            filing_date=date.today().isoformat(),
        )

        recent_xml = _FORM_4_MINIMAL.replace("2025-06-15", date.today().isoformat())
        fetcher = _StubFetcher(
            {
                "https://www.sec.gov/Archives/targ-old.xml": _FORM_4_MINIMAL,
                "https://www.sec.gov/Archives/targ-recent.xml": recent_xml,
            }
        )

        result = ingest_insider_transactions_for_instrument(
            ebull_test_conn,
            cast("object", fetcher),  # type: ignore[arg-type]
            instrument_id=iid,
        )

        assert fetcher.calls == ["https://www.sec.gov/Archives/targ-recent.xml"]
        assert result.filings_scanned == 1


# ---------------------------------------------------------------------------
# Manifest-worker pre-fetch gate
# ---------------------------------------------------------------------------


def _seed_manifest_pending(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    filed_at: datetime,
    form: str = "4",
) -> None:
    record_manifest_entry(
        conn,
        accession,
        cik="0000320193",
        form=form,
        source="sec_form4",  # type: ignore[arg-type]
        subject_type="issuer",
        subject_id=str(instrument_id),
        instrument_id=instrument_id,
        filed_at=filed_at,
        primary_document_url=("https://www.sec.gov/Archives/edgar/data/320193/000032019326000099/primary_doc.xml"),
    )


class TestManifestWorkerGate:
    @pytest.fixture(autouse=True)
    def _reset_registry(self):
        from app.jobs.sec_manifest_worker import clear_registered_parsers
        from app.services.manifest_parsers import register_all_parsers

        clear_registered_parsers()
        register_all_parsers()
        yield
        clear_registered_parsers()
        register_all_parsers()

    def test_pre_3y_row_tombstoned_before_fetch(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app.jobs.sec_manifest_worker import run_manifest_worker
        from app.providers.implementations import sec_edgar
        from app.services.sec_manifest import get_manifest_row

        iid = _seed_instrument(ebull_test_conn, iid=801, symbol="MANO")
        _seed_manifest_pending(
            ebull_test_conn,
            accession="0000320193-15-000099",
            instrument_id=iid,
            filed_at=datetime(2015, 6, 15, tzinfo=UTC),  # pre-3y
        )
        ebull_test_conn.commit()

        call_log: list[str] = []
        monkeypatch.setattr(
            sec_edgar.SecFilingsProvider,
            "fetch_document_text",
            lambda self, url: (call_log.append(url), "should-not-be-called")[1],
        )

        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()

        assert call_log == []  # pre-fetch gate fired; no network
        assert stats.tombstoned == 1
        row = get_manifest_row(ebull_test_conn, "0000320193-15-000099")
        assert row is not None
        assert row.ingest_status == "tombstoned"
        assert row.error == "retention floor"

    def test_inside_3y_row_proceeds_to_fetch(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app.jobs.sec_manifest_worker import run_manifest_worker
        from app.providers.implementations import sec_edgar

        iid = _seed_instrument(ebull_test_conn, iid=802, symbol="MANI")
        _seed_manifest_pending(
            ebull_test_conn,
            accession="0000320193-26-000099",
            instrument_id=iid,
            filed_at=datetime.now(tz=UTC),  # current
        )
        ebull_test_conn.commit()

        call_log: list[str] = []

        def _fake_fetch(self: Any, url: str) -> str:
            call_log.append(url)
            return _FORM_4_MINIMAL.replace("2025-06-15", date.today().isoformat())

        monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _fake_fetch)

        run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()

        assert len(call_log) == 1  # fetched once

    def test_filed_at_none_tombstones_per_existing_pattern(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Defensive: the DB column is NOT NULL but the parser-side type
        is `Any`. A `None` here mirrors the existing
        ``missing instrument_id`` / ``missing primary_document_url``
        tombstone path — terminal, no retry, no fetch."""
        from app.services.manifest_parsers.insider_345 import _parse_form4

        class _FakeRow:
            accession_number = "FAKE-NULL-FILED-1"
            instrument_id = 12345
            primary_document_url = "https://example.com/x.xml"
            filed_at = None  # the gap under test

        outcome = _parse_form4(ebull_test_conn, _FakeRow())
        assert outcome.status == "tombstoned"
        assert outcome.error == "missing filed_at"


# ---------------------------------------------------------------------------
# Bulk dataset path — Form 4 only; Form 3 + Form 5 untouched
# ---------------------------------------------------------------------------


def _to_tsv(rows: list[dict[str, str]]) -> str:
    if not rows:
        return ""
    fieldnames = sorted({k for row in rows for k in row.keys()})
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, delimiter="\t")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buf.getvalue()


def _build_dataset_zip(
    *,
    submissions: list[dict[str, str]],
    owners: list[dict[str, str]],
    holdings: list[dict[str, str]] | None = None,
    transactions: list[dict[str, str]] | None = None,
) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        zf.writestr("SUBMISSION.tsv", _to_tsv(submissions))
        zf.writestr("REPORTINGOWNER.tsv", _to_tsv(owners))
        zf.writestr("NONDERIV_HOLDING.tsv", _to_tsv(holdings or []))
        zf.writestr("NONDERIV_TRANS.tsv", _to_tsv(transactions or []))
    return out.getvalue()


def _seed_universe_with_cik(conn: psycopg.Connection[tuple], *, iid: int, symbol: str, cik_padded: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', TRUE)",
            (iid, symbol, f"{symbol} Inc."),
        )
        cur.execute(
            "INSERT INTO external_identifiers "
            "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cik', %s, TRUE)",
            (iid, cik_padded),
        )
    conn.commit()
    return iid


def _bulk_payload(
    *,
    accession: str,
    cik_unpadded: str,
    document_type: str,
    filing_date: str,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    """Build (submissions, owners, transactions) for one accession.

    Holdings list is empty by default — exercise the NONDERIV_TRANS
    write loop. Use ``_bulk_holdings_payload`` for the holdings-only
    branch (Form 3 initial-holdings statements + Form 4 amendments
    that land in the holdings table)."""
    submissions = [
        {
            "ACCESSION_NUMBER": accession,
            "ISSUERCIK": cik_unpadded,
            "DOCUMENT_TYPE": document_type,
            "FILING_DATE": filing_date,
            "PERIOD_OF_REPORT": filing_date,
        }
    ]
    owners = [
        {
            "ACCESSION_NUMBER": accession,
            "RPTOWNERCIK": "1000099",
            "RPTOWNERNAME": "Test Insider",
            "RPTOWNER_RELATIONSHIP": "Officer",
        }
    ]
    transactions = [
        {
            "ACCESSION_NUMBER": accession,
            "NONDERIV_TRANS_SK": "1",
            "TRANS_DATE": filing_date,
            "SHRS_OWND_FOLWNG_TRANS": "100",
        }
    ]
    return submissions, owners, transactions


def _bulk_holdings_payload(
    *,
    accession: str,
    cik_unpadded: str,
    document_type: str,
    filing_date: str,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    """Build (submissions, owners, holdings) — exercises the secondary
    write path (NONDERIV_HOLDING loop). Used for the Form 3 baseline
    case + the Form 4 holdings-only edge."""
    submissions = [
        {
            "ACCESSION_NUMBER": accession,
            "ISSUERCIK": cik_unpadded,
            "DOCUMENT_TYPE": document_type,
            "FILING_DATE": filing_date,
            "PERIOD_OF_REPORT": filing_date,
        }
    ]
    owners = [
        {
            "ACCESSION_NUMBER": accession,
            "RPTOWNERCIK": "1000100",
            "RPTOWNERNAME": "Test Holder",
            "RPTOWNER_RELATIONSHIP": "Director",
        }
    ]
    holdings = [
        {
            "ACCESSION_NUMBER": accession,
            "NONDERIV_HOLDING_SK": "1",
            "SHRS_OWND_FOLWNG_TRANS": "5000",
        }
    ]
    return submissions, owners, holdings


class TestBulkDatasetForm4Only:
    def test_pre_3y_form4_skipped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        tmp_path: Path,
    ) -> None:
        iid = _seed_universe_with_cik(ebull_test_conn, iid=901, symbol="BULK4OLD", cik_padded="0000320193")
        del iid
        subs, owners, trans = _bulk_payload(
            accession="0000320193-10-000001",
            cik_unpadded="320193",
            document_type="4",
            filing_date="2010-06-15",
        )
        archive = tmp_path / "bulk-pre3y.zip"
        archive.write_bytes(_build_dataset_zip(submissions=subs, owners=owners, transactions=trans))

        result = ingest_insider_dataset_archive(conn=ebull_test_conn, archive_path=archive, ingest_run_id=uuid4())

        assert result.rows_skipped_retention == 1
        assert result.rows_written == 0

    def test_recent_form4_retained(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        tmp_path: Path,
    ) -> None:
        iid = _seed_universe_with_cik(ebull_test_conn, iid=902, symbol="BULK4NEW", cik_padded="0000320194")
        del iid
        subs, owners, trans = _bulk_payload(
            accession="0000320194-26-000001",
            cik_unpadded="320194",
            document_type="4",
            filing_date=date.today().isoformat(),
        )
        archive = tmp_path / "bulk-fresh.zip"
        archive.write_bytes(_build_dataset_zip(submissions=subs, owners=owners, transactions=trans))

        result = ingest_insider_dataset_archive(conn=ebull_test_conn, archive_path=archive, ingest_run_id=uuid4())
        assert result.rows_skipped_retention == 0
        assert result.rows_written > 0

    def test_pre_3y_form5_retained(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        tmp_path: Path,
    ) -> None:
        """Form 5 (annual catch-up) is outside PR4 scope — PR10 owns its
        latest-only cap. Confirm the Form 4 cap does NOT touch Form 5
        rows even though both map to source='form4' via
        ``_map_form_to_source``."""
        iid = _seed_universe_with_cik(ebull_test_conn, iid=903, symbol="BULK5OLD", cik_padded="0000320195")
        del iid
        subs, owners, trans = _bulk_payload(
            accession="0000320195-10-000001",
            cik_unpadded="320195",
            document_type="5",
            filing_date="2010-06-15",
        )
        archive = tmp_path / "bulk-form5.zip"
        archive.write_bytes(_build_dataset_zip(submissions=subs, owners=owners, transactions=trans))

        result = ingest_insider_dataset_archive(conn=ebull_test_conn, archive_path=archive, ingest_run_id=uuid4())
        # Pre-3y Form 5 row IS retained (PR4 out of scope for Form 5).
        assert result.rows_skipped_retention == 0
        assert result.rows_written > 0

    def test_pre_3y_form4_skipped_via_holdings_loop(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        tmp_path: Path,
    ) -> None:
        """Bulk archive ``NONDERIV_HOLDING`` write loop must also honour
        the cap. Form 4 amendments / re-statements without transaction
        rows land here. Covers the second loop branch (Codex 2 MED #2
        coverage gap)."""
        iid = _seed_universe_with_cik(ebull_test_conn, iid=905, symbol="BULK4HOLDOLD", cik_padded="0000320197")
        del iid
        subs, owners, holdings = _bulk_holdings_payload(
            accession="0000320197-10-000001",
            cik_unpadded="320197",
            document_type="4",
            filing_date="2010-06-15",
        )
        archive = tmp_path / "bulk-holdings-old.zip"
        archive.write_bytes(_build_dataset_zip(submissions=subs, owners=owners, holdings=holdings))

        result = ingest_insider_dataset_archive(conn=ebull_test_conn, archive_path=archive, ingest_run_id=uuid4())

        assert result.rows_skipped_retention == 1
        assert result.rows_written == 0

    def test_pre_3y_form3_retained_via_holdings_loop(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        tmp_path: Path,
    ) -> None:
        """Pre-3y Form 3 (initial-holdings statement) lands via the
        holdings loop and MUST be retained — Form 3 is PR10 scope, NOT
        PR4. Confirms the form-code check at the holdings-loop gate
        only fires on ``4*``."""
        iid = _seed_universe_with_cik(ebull_test_conn, iid=906, symbol="BULK3OLD", cik_padded="0000320198")
        del iid
        subs, owners, holdings = _bulk_holdings_payload(
            accession="0000320198-10-000001",
            cik_unpadded="320198",
            document_type="3",
            filing_date="2010-06-15",
        )
        archive = tmp_path / "bulk-form3-old.zip"
        archive.write_bytes(_build_dataset_zip(submissions=subs, owners=owners, holdings=holdings))

        result = ingest_insider_dataset_archive(conn=ebull_test_conn, archive_path=archive, ingest_run_id=uuid4())

        # Pre-3y Form 3 holdings row IS retained — Form 3 is PR10's
        # latest-only cap, not PR4's 3y rolling cap.
        assert result.rows_skipped_retention == 0
        assert result.rows_written > 0

    def test_future_dated_form4_accepted(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        tmp_path: Path,
    ) -> None:
        """Cap is lower bound only — future-dated Form 4 (clock skew,
        weird provider) passes through."""
        iid = _seed_universe_with_cik(ebull_test_conn, iid=904, symbol="BULK4FUT", cik_padded="0000320196")
        del iid
        future = (date.today() + timedelta(days=2)).isoformat()
        subs, owners, trans = _bulk_payload(
            accession="0000320196-26-000002",
            cik_unpadded="320196",
            document_type="4",
            filing_date=future,
        )
        archive = tmp_path / "bulk-future.zip"
        archive.write_bytes(_build_dataset_zip(submissions=subs, owners=owners, transactions=trans))

        result = ingest_insider_dataset_archive(conn=ebull_test_conn, archive_path=archive, ingest_run_id=uuid4())
        assert result.rows_skipped_retention == 0
        assert result.rows_written > 0


# ---------------------------------------------------------------------------
# Cumulative-rollup invariant (steady-state)
# ---------------------------------------------------------------------------


class TestRefreshInsidersCurrentSteadyState:
    def test_refresh_aggregates_pre_existing_and_post_cap_observations(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Spec §4.3 amendment: ``refresh_insiders_current`` continues
        to aggregate pre-existing observations alongside post-cap rows
        without regression. Existing rows survive the cap (#1233 §6.3
        — only the operator-driven pre-wipe purges)."""
        from app.services.ownership_observations import record_insider_observation

        iid = _seed_instrument(ebull_test_conn, iid=999, symbol="ROLL")
        run_id = uuid4()

        # Pre-cap observation (pretend it landed before PR4 shipped).
        # PR4 does NOT delete this row.
        record_insider_observation(
            ebull_test_conn,
            instrument_id=iid,
            holder_cik="0001000777",
            holder_name="Ancient Owner",
            ownership_nature="direct",
            source="form4",
            source_document_id="0000320193-10-000001:NDT:1",
            source_accession="0000320193-10-000001",
            source_field=None,
            source_url=None,
            filed_at=datetime(2010, 6, 15, tzinfo=UTC),
            period_start=None,
            period_end=date(2010, 6, 15),
            ingest_run_id=run_id,
            shares=Decimal("1000"),
        )

        # Post-cap observation (fresh).
        record_insider_observation(
            ebull_test_conn,
            instrument_id=iid,
            holder_cik="0001000888",
            holder_name="Fresh Owner",
            ownership_nature="direct",
            source="form4",
            source_document_id="0000320193-26-000001:NDT:1",
            source_accession="0000320193-26-000001",
            source_field=None,
            source_url=None,
            filed_at=datetime.now(tz=UTC),
            period_start=None,
            period_end=date.today(),
            ingest_run_id=run_id,
            shares=Decimal("2500"),
        )
        ebull_test_conn.commit()

        refresh_insiders_current(ebull_test_conn, instrument_id=iid)
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT holder_cik, shares FROM ownership_insiders_current "
                "WHERE instrument_id = %s ORDER BY holder_cik",
                (iid,),
            )
            rows = cur.fetchall()

        holders = {r[0]: r[1] for r in rows}
        # Both observations contribute — pre-existing pre-cap row is
        # NOT silently dropped by the recompute.
        assert "0001000777" in holders
        assert "0001000888" in holders
