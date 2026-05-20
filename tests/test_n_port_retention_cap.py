"""N-PORT 8-quarter (24-month) ingest retention cap — #1233 §4.6 PR7.

Mirror of the PR6 ``test_thirteen_f_retention_cap.py`` for N-PORT.
Pins the contracts:

1. ``NPORT_RETENTION_QUARTERS == 8`` is the single source of truth.
2. ``n_port_retention_cutoff`` is anchored to calendar month-ends:
   exactly 24 consecutive completed month-ends survive, which by the
   mod-3 congruence-class argument is exactly 8 fiscal-Q snapshots
   per fund regardless of fiscal-year alignment.
3. ``n_port_retention_cutoff`` requires a tz-aware ``now``; naive
   ``datetime`` raises ``ValueError``. Non-UTC tz normalises via
   ``.astimezone(UTC).date()``.
4. ``n_port_within_retention`` is INCLUSIVE at the boundary; None
   ``period_of_report`` returns False (defensive).
5. ``parse_submissions_index`` enforces the intrinsic floor — caller
   ``min_period_of_report=None`` no longer means full history; the
   24-month cap is the default. Caller floor RAISES the floor (more
   recent wins) but never lowers it.
6. Bulk dataset ``ingest_nport_dataset_archive`` per-row gate skips
   pre-cap rows with a ``rows_skipped_retention`` counter; boundary
   equality (``period_end == cutoff``) survives.
7. ``list_nport_filer_ciks`` cohort accessor filters by
   ``last_seen_filed_at`` when provided; ``None`` returns the full
   cohort (mirror of #1010's safety-net for re-emerging filers).

Existing rows are not deleted by the cap (#1233 §6.3 is the only
purge event). These tests assert *insert/admit* behaviour only.
"""

from __future__ import annotations

import csv
import io
import json
import zipfile
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import psycopg
import pytest

from app.services.n_port_ingest import (
    NPORT_RETENTION_QUARTERS,
    n_port_retention_cutoff,
    n_port_within_retention,
    parse_submissions_index,
)
from app.services.sec_nport_dataset_ingest import (
    NPortIngestResult,
    ingest_nport_dataset_archive,
)
from app.services.sec_nport_filer_directory import list_nport_filer_ciks
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

# ---------------------------------------------------------------------------
# Pure helper contracts
# ---------------------------------------------------------------------------


class TestRetentionConstantAndCutoff:
    def test_constant_is_8_quarters(self) -> None:
        assert NPORT_RETENTION_QUARTERS == 8

    def test_mid_month_anchors_to_month_24_back(self) -> None:
        # 2026-05-15 → target month = 2026-05 - 24 months = 2024-05.
        # Cutoff = 2024-05-31.
        ref = datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC)
        assert n_port_retention_cutoff(now=ref) == date(2024, 5, 31)

    def test_first_of_month(self) -> None:
        # 2026-06-01 → target month = 2024-06. Cutoff = 2024-06-30.
        ref = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
        assert n_port_retention_cutoff(now=ref) == date(2024, 6, 30)

    def test_last_of_month(self) -> None:
        # 2026-05-31 → target month = 2024-05. Cutoff = 2024-05-31.
        ref = datetime(2026, 5, 31, 23, 59, 59, tzinfo=UTC)
        assert n_port_retention_cutoff(now=ref) == date(2024, 5, 31)

    def test_cutoff_rolls_forward_at_month_boundary(self) -> None:
        # 2026-05-31 23:59 UTC → cutoff 2024-05-31.
        # 2026-06-01 00:00 UTC → cutoff 2024-06-30.
        before_roll = datetime(2026, 5, 31, 23, 59, 59, tzinfo=UTC)
        after_roll = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
        assert n_port_retention_cutoff(now=before_roll) == date(2024, 5, 31)
        assert n_port_retention_cutoff(now=after_roll) == date(2024, 6, 30)

    def test_year_wrap_january(self) -> None:
        # today=2026-01-15 → target month = 2026-01 - 24 = 2024-01.
        # Cutoff = 2024-01-31. (Not "previous year minus 1" — the
        # earlier plan draft had this wrong; Codex 1a BLOCKING 1 fix.)
        ref = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
        assert n_port_retention_cutoff(now=ref) == date(2024, 1, 31)

    def test_year_wrap_march_admits_march_two_years_back(self) -> None:
        # today.month=3 → target month=3 (24 months back), NOT February.
        # With the §5.1 algorithm Mar 2024 → cutoff 2022-03-31, Mar 2025
        # → cutoff 2023-03-31. (Codex 1a BLOCKING 1 fix.)
        ref_2024 = datetime(2024, 3, 15, 12, 0, 0, tzinfo=UTC)
        ref_2025 = datetime(2025, 3, 15, 12, 0, 0, tzinfo=UTC)
        assert n_port_retention_cutoff(now=ref_2024) == date(2022, 3, 31)
        assert n_port_retention_cutoff(now=ref_2025) == date(2023, 3, 31)

    def test_february_target_handles_leap_year(self) -> None:
        # Only ``today.month == 2`` targets a Feb cutoff. Walk back
        # 24 calendar months → target year/month is exactly (today.year
        # - 2, 2). Cutoff = Feb-29 in a leap year, Feb-28 otherwise.
        ref_2026 = datetime(2026, 2, 15, 12, 0, 0, tzinfo=UTC)
        ref_2025 = datetime(2025, 2, 15, 12, 0, 0, tzinfo=UTC)
        assert n_port_retention_cutoff(now=ref_2026) == date(2024, 2, 29)
        assert n_port_retention_cutoff(now=ref_2025) == date(2023, 2, 28)

    def test_admits_exactly_24_consecutive_month_ends(self) -> None:
        # Pin a reference now; assert every calendar month-end from
        # ``cutoff`` to ``latest_completed`` is admitted and the count
        # is exactly 24 (no off-by-one).
        ref = datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC)
        cutoff = n_port_retention_cutoff(now=ref)
        # latest_completed = month-end of today.month - 1 = 2026-04-30.
        latest_completed = date(2026, 4, 30)

        admitted: list[date] = []
        # Walk every month-end from cutoff forward, stepping by months.
        y, m = cutoff.year, cutoff.month
        while True:
            month_end = date(y + (1 if m == 12 else 0), 1 if m == 12 else m + 1, 1) - timedelta(days=1)
            if month_end > latest_completed:
                break
            admitted.append(month_end)
            if m == 12:
                y += 1
                m = 1
            else:
                m += 1

        assert len(admitted) == 24
        for d in admitted:
            assert n_port_within_retention(d, now=ref) is True

    def test_rejects_month_end_one_step_before_cutoff(self) -> None:
        # One month-end BEFORE the cutoff must be rejected.
        ref = datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC)
        cutoff = n_port_retention_cutoff(now=ref)  # 2024-05-31
        # Month-end immediately before cutoff = 2024-04-30.
        one_before = date(2024, 4, 30)
        assert one_before < cutoff
        assert n_port_within_retention(one_before, now=ref) is False

    def test_cutoff_returns_date_not_datetime(self) -> None:
        ref = datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC)
        result = n_port_retention_cutoff(now=ref)
        assert isinstance(result, date)
        assert not isinstance(result, datetime)

    def test_naive_datetime_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="tz-aware"):
            n_port_retention_cutoff(now=datetime(2026, 5, 20, 12, 0, 0))

    def test_non_utc_tz_normalises_to_utc_date(self) -> None:
        # 2026-05-31 23:00 in UTC-04 = 2026-06-01 03:00 UTC.
        # UTC date is 2026-06-01 (June) → cutoff = 2024-06-30. A local-
        # TZ ``.date()`` would read 2026-05-31 (May) → cutoff = 2024-05-31.
        # The UTC normalisation wins.
        tz_minus_04 = timezone(timedelta(hours=-4))
        edge_case = datetime(2026, 5, 31, 23, 0, 0, tzinfo=tz_minus_04)
        assert n_port_retention_cutoff(now=edge_case) == date(2024, 6, 30)


class TestNPortWithinRetention:
    def test_at_boundary_accepted(self) -> None:
        ref = datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC)
        cutoff = n_port_retention_cutoff(now=ref)
        assert n_port_within_retention(cutoff, now=ref) is True

    def test_one_day_before_boundary_rejected(self) -> None:
        ref = datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC)
        cutoff = n_port_retention_cutoff(now=ref)
        assert n_port_within_retention(cutoff - timedelta(days=1), now=ref) is False

    def test_one_day_after_boundary_accepted(self) -> None:
        ref = datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC)
        cutoff = n_port_retention_cutoff(now=ref)
        assert n_port_within_retention(cutoff + timedelta(days=1), now=ref) is True

    def test_none_period_rejected(self) -> None:
        # Defensive: an accession we couldn't tag with a month end is
        # unsafe to admit.
        assert n_port_within_retention(None) is False


# ---------------------------------------------------------------------------
# parse_submissions_index intrinsic floor
# ---------------------------------------------------------------------------


def _submissions_payload(rows: list[tuple[str, str, str, str]]) -> str:
    """Build a minimal SEC submissions JSON payload.

    Each row: (accession_number, form, filing_date, report_date).
    """
    return json.dumps(
        {
            "filings": {
                "recent": {
                    "accessionNumber": [r[0] for r in rows],
                    "form": [r[1] for r in rows],
                    "filingDate": [r[2] for r in rows],
                    "reportDate": [r[3] for r in rows],
                }
            }
        }
    )


class TestParseSubmissionsIndexIntrinsicFloor:
    """Spec §4.6: caller's ``None`` falls back to the intrinsic cap.

    Each scenario fixes a reference ``now`` via patch so the in-cap /
    pre-cap classification is deterministic.
    """

    @pytest.fixture
    def fixed_now(self) -> datetime:
        return datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC)

    def _patch_now(self, fixed_now: datetime):
        # Patch the bound datetime symbol inside ``n_port_ingest`` so the
        # intrinsic cutoff resolves deterministically.
        return patch("app.services.n_port_ingest.datetime", wraps=datetime)

    def test_caller_none_falls_back_to_intrinsic_cap(self, fixed_now: datetime) -> None:
        # Cutoff at fixed_now = 2024-05-31. 2023-12-31 is pre-cap;
        # 2024-09-30 is in-cap.
        payload = _submissions_payload(
            [
                ("0001000000-23-000001", "NPORT-P", "2024-02-29", "2023-12-31"),
                ("0001000000-24-000001", "NPORT-P", "2024-11-29", "2024-09-30"),
            ]
        )
        with patch("app.services.n_port_ingest.datetime") as mock_dt:
            mock_dt.now.return_value = fixed_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            refs = parse_submissions_index(payload)
        accessions = [r.accession_number for r in refs]
        assert "0001000000-23-000001" not in accessions
        assert "0001000000-24-000001" in accessions

    def test_caller_floor_more_recent_overrides_cap(self, fixed_now: datetime) -> None:
        # Caller floor = 2025-01-01 > intrinsic cap 2024-05-31.
        # 2024-12-31 is in-cap but pre-caller-floor → skipped.
        payload = _submissions_payload(
            [
                ("0001000000-24-099999", "NPORT-P", "2025-02-28", "2024-12-31"),
                ("0001000000-25-000001", "NPORT-P", "2025-05-29", "2025-03-31"),
            ]
        )
        with patch("app.services.n_port_ingest.datetime") as mock_dt:
            mock_dt.now.return_value = fixed_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            refs = parse_submissions_index(payload, min_period_of_report=date(2025, 1, 1))
        accessions = [r.accession_number for r in refs]
        assert "0001000000-24-099999" not in accessions
        assert "0001000000-25-000001" in accessions

    def test_caller_floor_more_permissive_loses_to_cap(self, fixed_now: datetime) -> None:
        # Caller floor = 2020-01-01 < intrinsic cap 2024-05-31.
        # 2023-09-30 is post-caller-floor but pre-cap → skipped.
        payload = _submissions_payload(
            [
                ("0001000000-23-000099", "NPORT-P", "2023-11-29", "2023-09-30"),
                ("0001000000-25-000002", "NPORT-P", "2025-05-29", "2025-03-31"),
            ]
        )
        with patch("app.services.n_port_ingest.datetime") as mock_dt:
            mock_dt.now.return_value = fixed_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            refs = parse_submissions_index(payload, min_period_of_report=date(2020, 1, 1))
        accessions = [r.accession_number for r in refs]
        assert "0001000000-23-000099" not in accessions
        assert "0001000000-25-000002" in accessions

    def test_boundary_period_accepted(self, fixed_now: datetime) -> None:
        # period_of_report == cutoff is admitted (inclusive boundary).
        # Cutoff at fixed_now = 2024-05-31.
        payload = _submissions_payload([("0001000000-24-000777", "NPORT-P", "2024-07-29", "2024-05-31")])
        with patch("app.services.n_port_ingest.datetime") as mock_dt:
            mock_dt.now.return_value = fixed_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            refs = parse_submissions_index(payload)
        assert [r.accession_number for r in refs] == ["0001000000-24-000777"]

    def test_null_report_date_leaks_past_index_filter(self, fixed_now: datetime) -> None:
        # ``period is None`` short-circuits the comparison — leaks past
        # ``parse_submissions_index``. ``_ingest_single_accession``
        # defensive post-parse gate is the safety net.
        payload = _submissions_payload([("0001000000-19-000001", "NPORT-P", "2019-04-30", "")])
        with patch("app.services.n_port_ingest.datetime") as mock_dt:
            mock_dt.now.return_value = fixed_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            refs = parse_submissions_index(payload)
        assert len(refs) == 1
        assert refs[0].period_of_report is None


# ---------------------------------------------------------------------------
# Bulk dataset per-row gate
# ---------------------------------------------------------------------------


def _build_dataset_zip_bytes(
    *,
    submissions: list[dict[str, str]],
    registrants: list[dict[str, str]],
    fund_info: list[dict[str, str]],
    holdings: list[dict[str, str]],
) -> bytes:
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

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        zf.writestr("SUBMISSION.tsv", _to_tsv(submissions))
        zf.writestr("REGISTRANT.tsv", _to_tsv(registrants))
        zf.writestr("FUND_REPORTED_INFO.tsv", _to_tsv(fund_info))
        zf.writestr("FUND_REPORTED_HOLDING.tsv", _to_tsv(holdings))
    return out.getvalue()


_NEXT_IID: list[int] = [15000]


def _seed_universe_with_cusip(
    conn: psycopg.Connection[tuple],
    *,
    symbol: str,
    cusip: str,
) -> int:
    _NEXT_IID[0] += 1
    iid = _NEXT_IID[0]
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', TRUE)",
            (iid, symbol, f"{symbol} Inc."),
        )
        cur.execute(
            "INSERT INTO external_identifiers "
            "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cusip', %s, TRUE)",
            (iid, cusip.upper()),
        )
    conn.commit()
    return iid


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestBulkDatasetRetentionGate:
    def _archive(
        self,
        tmp_path: Path,
        *,
        accession: str,
        report_date: str,
        cusip: str = "037833100",
    ) -> Path:
        payload = _build_dataset_zip_bytes(
            submissions=[
                {
                    "ACCESSION_NUMBER": accession,
                    "FILING_DATE": "2025-11-29",
                    "SUB_TYPE": "NPORT-P",
                    "REPORT_DATE": report_date,
                },
            ],
            registrants=[
                {"ACCESSION_NUMBER": accession, "CIK": "1234567", "REGISTRANT_NAME": "Big Fund Trust"},
            ],
            fund_info=[
                {
                    "ACCESSION_NUMBER": accession,
                    "SERIES_ID": "S000004310",
                    "SERIES_NAME": "Big Fund Equity Series",
                },
            ],
            holdings=[
                {
                    "ACCESSION_NUMBER": accession,
                    "HOLDING_ID": "1",
                    "ISSUER_CUSIP": cusip,
                    "BALANCE": "500000",
                    "UNIT": "NS",
                    "CURRENCY_CODE": "USD",
                    "CURRENCY_VALUE": "75000000",
                    "PAYOFF_PROFILE": "Long",
                    "ASSET_CAT": "EC",
                },
            ],
        )
        out = tmp_path / f"nport_{accession}.zip"
        out.write_bytes(payload)
        return out

    def test_pre_cap_row_skipped_with_counter(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # Pin cutoff via patched datetime: ref = 2026-05-15 → cutoff
        # = 2024-05-31. report_date = 2022-12-31 is pre-cap.
        _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        archive = self._archive(tmp_path, accession="0001234567-22-000001", report_date="2022-12-31")
        ref = datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC)
        with patch("app.services.n_port_ingest.datetime") as mock_dt:
            mock_dt.now.return_value = ref
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            result = ingest_nport_dataset_archive(
                conn=ebull_test_conn,
                archive_path=archive,
                ingest_run_id=uuid4(),
            )
        ebull_test_conn.commit()
        assert isinstance(result, NPortIngestResult)
        assert result.rows_written == 0
        assert result.rows_skipped_retention == 1

    def test_in_cap_row_admitted(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # report_date = 2025-09-30 is in-cap at ref 2026-05-15.
        _seed_universe_with_cusip(ebull_test_conn, symbol="MSFT", cusip="594918104")
        archive = self._archive(
            tmp_path,
            accession="0001234567-25-000010",
            report_date="2025-09-30",
            cusip="594918104",
        )
        ref = datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC)
        with patch("app.services.n_port_ingest.datetime") as mock_dt:
            mock_dt.now.return_value = ref
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            result = ingest_nport_dataset_archive(
                conn=ebull_test_conn,
                archive_path=archive,
                ingest_run_id=uuid4(),
            )
        ebull_test_conn.commit()
        assert result.rows_written == 1
        assert result.rows_skipped_retention == 0

    def test_boundary_period_equality_admitted(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # report_date == cutoff is admitted (inclusive boundary).
        # ref=2026-05-15 → cutoff 2024-05-31.
        _seed_universe_with_cusip(ebull_test_conn, symbol="GME", cusip="36467W109")
        archive = self._archive(
            tmp_path,
            accession="0001234567-24-000050",
            report_date="2024-05-31",
            cusip="36467W109",
        )
        ref = datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC)
        with patch("app.services.n_port_ingest.datetime") as mock_dt:
            mock_dt.now.return_value = ref
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            result = ingest_nport_dataset_archive(
                conn=ebull_test_conn,
                archive_path=archive,
                ingest_run_id=uuid4(),
            )
        ebull_test_conn.commit()
        assert result.rows_written == 1
        assert result.rows_skipped_retention == 0


# ---------------------------------------------------------------------------
# Cohort accessor (#1010 mirror)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestListNportFilerCiks:
    def _seed_filer(
        self,
        conn: psycopg.Connection[tuple],
        *,
        cik: str,
        name: str,
        last_seen_filed_at: datetime | None,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sec_nport_filer_directory
                    (cik, fund_trust_name, last_seen_period_end, last_seen_filed_at)
                VALUES (%s, %s, NULL, %s)
                ON CONFLICT (cik) DO UPDATE SET
                    fund_trust_name = EXCLUDED.fund_trust_name,
                    last_seen_filed_at = EXCLUDED.last_seen_filed_at
                """,
                (cik, name, last_seen_filed_at),
            )
        conn.commit()

    def test_full_cohort_returned_when_filter_none(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        # Clear + seed 3 rows; ensure all 3 returned in
        # ``last_seen_filed_at DESC NULLS LAST, cik`` order.
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM sec_nport_filer_directory")
        ebull_test_conn.commit()

        now = datetime(2026, 5, 15, tzinfo=UTC)
        self._seed_filer(
            ebull_test_conn,
            cik="0000000001",
            name="Old Trust",
            last_seen_filed_at=now - timedelta(days=400),
        )
        self._seed_filer(
            ebull_test_conn,
            cik="0000000002",
            name="Fresh Trust",
            last_seen_filed_at=now - timedelta(days=10),
        )
        self._seed_filer(
            ebull_test_conn,
            cik="0000000003",
            name="Null Trust",
            last_seen_filed_at=None,
        )

        ciks = list_nport_filer_ciks(ebull_test_conn, min_last_seen_filed_at=None)
        assert ciks == ["0000000002", "0000000001", "0000000003"]

    def test_recency_filter_excludes_pre_cutoff_and_nulls(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM sec_nport_filer_directory")
        ebull_test_conn.commit()

        now = datetime(2026, 5, 15, tzinfo=UTC)
        self._seed_filer(
            ebull_test_conn,
            cik="0000000010",
            name="Old Trust",
            last_seen_filed_at=now - timedelta(days=400),
        )
        self._seed_filer(
            ebull_test_conn,
            cik="0000000011",
            name="Fresh Trust",
            last_seen_filed_at=now - timedelta(days=200),
        )
        self._seed_filer(
            ebull_test_conn,
            cik="0000000012",
            name="Null Trust",
            last_seen_filed_at=None,
        )

        cutoff = now - timedelta(days=380)
        ciks = list_nport_filer_ciks(ebull_test_conn, min_last_seen_filed_at=cutoff)
        # 0000000011 alone (Fresh Trust at -200d is within 380d).
        # NULL row excluded; Old Trust at -400d excluded.
        assert ciks == ["0000000011"]

    def test_recency_filter_inclusive_at_boundary(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM sec_nport_filer_directory")
        ebull_test_conn.commit()

        now = datetime(2026, 5, 15, tzinfo=UTC)
        cutoff = now - timedelta(days=380)
        # Trust filed exactly AT the cutoff timestamp.
        self._seed_filer(
            ebull_test_conn,
            cik="0000000020",
            name="Boundary Trust",
            last_seen_filed_at=cutoff,
        )
        ciks = list_nport_filer_ciks(ebull_test_conn, min_last_seen_filed_at=cutoff)
        assert ciks == ["0000000020"]
