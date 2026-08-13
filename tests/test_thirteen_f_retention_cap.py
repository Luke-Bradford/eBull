"""13F-HR 8-quarter ingest retention cap — #1233 §4.5 PR6.

Pins the contracts:

1. ``THIRTEEN_F_HR_RETENTION_QUARTERS == 8`` is the single source of truth.
2. ``thirteen_f_retention_cutoff`` is anchored to calendar quarter ends.
   Cutoff = quarter-end exactly ``RETENTION_QUARTERS - 1`` quarters before
   the most-recent COMPLETED quarter end. Inclusive boundary. Always
   yields exactly 8 quarter-ends, never 9.
3. ``thirteen_f_retention_cutoff`` requires a tz-aware ``now``; naive
   ``datetime`` raises ``ValueError``. Non-UTC tz normalises via
   ``.astimezone(UTC).date()``.
4. ``thirteen_f_within_retention`` is INCLUSIVE at the boundary; None
   ``period_of_report`` returns False (defensive).
5. ``parse_submissions_index`` enforces the intrinsic floor — caller's
   ``min_period_of_report=None`` no longer means full history; the 8q
   cap is the default. Caller floor RAISES the floor (more recent
   wins) but never lowers it.
6. ``_ingest_single_accession`` defensive post-parse gate catches
   accessions whose submissions JSON ``reportDate`` was NULL/malformed.
7. Manifest-worker ``_parse_13f_hr`` post-parse gate tombstones pre-cap
   accessions BEFORE the infotable fetch.
8. Bulk dataset ``ingest_13f_dataset_archive`` per-row gate skips pre-
   cap rows with a ``rows_skipped_retention`` counter; boundary
   equality (``period_end == cutoff``) survives.
9. Rewash ``_apply_13f_infotable`` rescue branch gates by
   ``period_of_report`` (happy path uncapped per PR5 precedent).
10. ``ownership_observations_sync.sync_institutions`` SQL predicate
    blocks pre-cap rows even if ``institutional_holdings`` still
    carries them.

Existing rows are not deleted by the cap (#1233 §6.3 is the only
purge event). These tests assert *insert/admit* behaviour only.
"""

from __future__ import annotations

import csv
import io
import json
import zipfile
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import psycopg
import pytest

from app.services.institutional_holdings import (
    THIRTEEN_F_HR_RETENTION_QUARTERS,
    AccessionRef,
    _ingest_single_accession,
    parse_submissions_index,
    thirteen_f_retention_cutoff,
    thirteen_f_within_retention,
)
from app.services.ownership_observations_sync import sync_institutions
from app.services.sec_13f_dataset_ingest import ingest_13f_dataset_archive
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

# ---------------------------------------------------------------------------
# Pure helper contracts
# ---------------------------------------------------------------------------


class TestRetentionConstantAndCutoff:
    def test_constant_is_8_quarters(self) -> None:
        assert THIRTEEN_F_HR_RETENTION_QUARTERS == 8

    def test_q2_in_progress_anchors_to_q1_completed(self) -> None:
        # 2026-05-20 is inside Q2 2026. Latest completed quarter end =
        # Q1 2026 = 2026-03-31. 7 quarters back = Q2 2024 = 2024-06-30.
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        assert thirteen_f_retention_cutoff(now=ref) == date(2024, 6, 30)

    def test_cutoff_does_not_drift_within_a_quarter(self) -> None:
        # First day of Q2 and last day of Q2 must yield the SAME cutoff —
        # the bug Codex 1a §1 caught with a floating today-760d window.
        first_day_q2 = datetime(2026, 4, 1, 0, 0, 0, tzinfo=UTC)
        last_day_q2 = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
        assert thirteen_f_retention_cutoff(now=first_day_q2) == date(2024, 6, 30)
        assert thirteen_f_retention_cutoff(now=last_day_q2) == date(2024, 6, 30)

    def test_cutoff_rolls_forward_at_quarter_boundary(self) -> None:
        # 2026-06-30 is still inside Q2 (latest completed = Q1, cutoff = 2024-06-30).
        # 2026-07-01 is the first day of Q3 (latest completed = Q2, cutoff = 2024-09-30).
        before_roll = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
        after_roll = datetime(2026, 7, 1, 0, 0, 0, tzinfo=UTC)
        assert thirteen_f_retention_cutoff(now=before_roll) == date(2024, 6, 30)
        assert thirteen_f_retention_cutoff(now=after_roll) == date(2024, 9, 30)

    def test_q1_anchors_to_prior_year_q4(self) -> None:
        # 2026-01-15 inside Q1 2026 → latest completed = Q4 2025 (2025-12-31).
        # 7 back = Q1 2024 = 2024-03-31.
        ref = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
        assert thirteen_f_retention_cutoff(now=ref) == date(2024, 3, 31)

    def test_cutoff_returns_date_not_datetime(self) -> None:
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        result = thirteen_f_retention_cutoff(now=ref)
        assert isinstance(result, date)
        assert not isinstance(result, datetime)

    def test_naive_datetime_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="tz-aware"):
            thirteen_f_retention_cutoff(now=datetime(2026, 5, 20, 12, 0, 0))

    def test_non_utc_tz_normalises_to_utc_date(self) -> None:
        # 2026-04-01 03:00 in UTC-05 = 2026-04-01 08:00 UTC. Still inside
        # Q2 UTC → cutoff = 2024-06-30. Local-TZ date would also be
        # 2026-04-01, so flip to a case where the UTC date differs.
        #
        # 2026-06-30 23:00 in UTC-04 = 2026-07-01 03:00 UTC. UTC date is
        # 2026-07-01 = first day of Q3 → cutoff = 2024-09-30. Local-TZ
        # date would be 2026-06-30 (still Q2) → cutoff = 2024-06-30.
        # The UTC normalisation wins.
        tz_minus_04 = timezone(timedelta(hours=-4))
        edge_case = datetime(2026, 6, 30, 23, 0, 0, tzinfo=tz_minus_04)
        assert thirteen_f_retention_cutoff(now=edge_case) == date(2024, 9, 30)


class TestThirteenFWithinRetention:
    def test_at_boundary_accepted(self) -> None:
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        cutoff = thirteen_f_retention_cutoff(now=ref)
        assert thirteen_f_within_retention(cutoff, now=ref) is True

    def test_one_day_before_boundary_rejected(self) -> None:
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        cutoff = thirteen_f_retention_cutoff(now=ref)
        assert thirteen_f_within_retention(cutoff - timedelta(days=1), now=ref) is False

    def test_one_day_after_boundary_accepted(self) -> None:
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        cutoff = thirteen_f_retention_cutoff(now=ref)
        assert thirteen_f_within_retention(cutoff + timedelta(days=1), now=ref) is True

    def test_none_period_rejected(self) -> None:
        # Defensive: an accession we couldn't tag with a quarter end is
        # unsafe to admit.
        assert thirteen_f_within_retention(None) is False


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
    """Spec §4.5: caller's ``None`` falls back to the intrinsic cap.

    Each scenario fixes a reference ``now`` via patch so the in-cap /
    pre-cap classification is deterministic.
    """

    @pytest.fixture
    def fixed_now(self) -> datetime:
        return datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)

    def test_caller_none_falls_back_to_intrinsic_cap(self, fixed_now: datetime) -> None:
        # Cutoff at fixed_now = 2024-06-30. 2024-03-31 is pre-cap;
        # 2024-09-30 is in-cap.
        payload = _submissions_payload(
            [
                ("0001000000-20-000001", "13F-HR", "2024-04-15", "2024-03-31"),
                ("0001000000-24-000001", "13F-HR", "2024-10-31", "2024-09-30"),
            ]
        )
        with patch("app.services.institutional_holdings.datetime") as mock_dt:
            mock_dt.now.return_value = fixed_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            refs = parse_submissions_index(payload)
        accessions = [r.accession_number for r in refs]
        assert "0001000000-20-000001" not in accessions
        assert "0001000000-24-000001" in accessions

    def test_caller_floor_more_recent_overrides_cap(self, fixed_now: datetime) -> None:
        # Caller floor = 2025-01-01 > intrinsic cap 2024-06-30.
        # 2024-12-31 is in-cap but pre-caller-floor → skipped.
        payload = _submissions_payload(
            [
                ("0001000000-24-099999", "13F-HR", "2025-01-15", "2024-12-31"),
                ("0001000000-25-000001", "13F-HR", "2025-04-15", "2025-03-31"),
            ]
        )
        with patch("app.services.institutional_holdings.datetime") as mock_dt:
            mock_dt.now.return_value = fixed_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            refs = parse_submissions_index(payload, min_period_of_report=date(2025, 1, 1))
        accessions = [r.accession_number for r in refs]
        assert "0001000000-24-099999" not in accessions
        assert "0001000000-25-000001" in accessions

    def test_caller_floor_more_permissive_loses_to_cap(self, fixed_now: datetime) -> None:
        # Caller floor = 2020-01-01 < intrinsic cap 2024-06-30.
        # 2023-12-31 is post-caller-floor but pre-cap → skipped.
        payload = _submissions_payload(
            [
                ("0001000000-23-000001", "13F-HR", "2024-01-31", "2023-12-31"),
                ("0001000000-25-000002", "13F-HR", "2025-04-15", "2025-03-31"),
            ]
        )
        with patch("app.services.institutional_holdings.datetime") as mock_dt:
            mock_dt.now.return_value = fixed_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            refs = parse_submissions_index(payload, min_period_of_report=date(2020, 1, 1))
        accessions = [r.accession_number for r in refs]
        assert "0001000000-23-000001" not in accessions
        assert "0001000000-25-000002" in accessions

    def test_boundary_period_accepted(self, fixed_now: datetime) -> None:
        # period_of_report == cutoff is admitted (inclusive boundary).
        payload = _submissions_payload([("0001000000-24-000777", "13F-HR", "2024-07-15", "2024-06-30")])
        with patch("app.services.institutional_holdings.datetime") as mock_dt:
            mock_dt.now.return_value = fixed_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            refs = parse_submissions_index(payload)
        assert [r.accession_number for r in refs] == ["0001000000-24-000777"]

    def test_null_report_date_leaks_past_index_filter(self, fixed_now: datetime) -> None:
        # ``period is None`` short-circuits the comparison — leaks past
        # ``parse_submissions_index``. ``_ingest_single_accession``
        # defensive post-parse gate is the safety net (covered below).
        payload = _submissions_payload([("0001000000-19-000001", "13F-HR", "2019-04-15", "")])
        with patch("app.services.institutional_holdings.datetime") as mock_dt:
            mock_dt.now.return_value = fixed_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            refs = parse_submissions_index(payload)
        # The accession is admitted (period_of_report is None).
        assert len(refs) == 1
        assert refs[0].period_of_report is None


# ---------------------------------------------------------------------------
# _ingest_single_accession defensive post-parse gate (Codex 1a §2 fix)
# ---------------------------------------------------------------------------


class _StubFetcher:
    """Minimal SecArchiveFetcher stub for the defensive-gate test."""

    def __init__(self, responses: dict[str, str | None]) -> None:
        self._responses = responses

    def fetch_document_text(self, absolute_url: str) -> str | None:
        # Map any URL containing the accession back to the keyed response.
        for key, value in self._responses.items():
            if key in absolute_url:
                return value
        return None


class TestIngestSingleAccessionDefensiveGate:
    def test_pre_cap_period_skips_infotable_fetch(self) -> None:
        # Build a primary_doc.xml whose period_of_report parses to a
        # pre-cap quarter end. The fetcher records calls so we can
        # assert the infotable URL was never requested.
        accession = "0001000000-19-000001"
        cik = "0001234567"
        primary_xml = (
            '<?xml version="1.0"?>'
            '<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler">'
            "<headerData><filerInfo>"
            "<filer><credentials><cik>1234567</cik><ccc>xxxxxxxx</ccc></credentials></filer>"
            "<periodOfReport>03-31-2019</periodOfReport>"
            "</filerInfo></headerData>"
            "<formData>"
            "<coverPage>"
            "<reportCalendarOrQuarter>03-31-2019</reportCalendarOrQuarter>"
            "<filingManager><name>Test Filer</name>"
            '<address xmlns="http://www.sec.gov/edgar/common">'
            "<street1>1 Test St</street1><city>NYC</city><stateOrCountry>NY</stateOrCountry><zipCode>10001</zipCode>"
            "</address>"
            "</filingManager>"
            "<reportType>13F HOLDINGS REPORT</reportType>"
            "<form13FFileNumber>028-00000</form13FFileNumber>"
            "</coverPage>"
            "<signatureBlock>"
            "<name>Jane Doe</name>"
            "<title>Authorised Signer</title>"
            "<phone>555-0100</phone>"
            "<city>NYC</city>"
            "<stateOrCountry>NY</stateOrCountry>"
            "<signatureDate>04-15-2019</signatureDate>"
            "</signatureBlock>"
            "</formData></edgarSubmission>"
        )
        index_json = json.dumps(
            {
                "directory": {
                    "item": [
                        {"name": "primary_doc.xml"},
                        {"name": "infotable.xml"},
                    ]
                }
            }
        )
        infotable_calls: list[str] = []

        class RecordingFetcher:
            def fetch_document_text(self, absolute_url: str) -> str | None:
                if absolute_url.endswith("index.json"):
                    return index_json
                if absolute_url.endswith("primary_doc.xml"):
                    return primary_xml
                if absolute_url.endswith("infotable.xml") or "infotable" in absolute_url:
                    infotable_calls.append(absolute_url)
                    return None
                return None

        ref = AccessionRef(
            accession_number=accession,
            filing_type="13F-HR",
            period_of_report=None,  # NULL reportDate leaked past index filter
            filed_at=datetime(2019, 4, 15, tzinfo=UTC),
        )
        # Stub conn — only used for raw_filings.store_raw / commit.
        # A minimal mock that swallows execute + commit suffices because
        # the defensive gate returns BEFORE the infotable fetch.
        import unittest.mock as mock

        conn = mock.MagicMock()
        outcome = _ingest_single_accession(
            conn,
            RecordingFetcher(),
            filer_cik=cik,
            ref=ref,
        )
        assert outcome.status == "failed"
        assert outcome.error == "retention floor"
        # CRITICAL: infotable.xml must NOT have been fetched.
        assert infotable_calls == []
        # Codex 2 LOW — defensive gate surfaces the parser-derived
        # period_of_report so the caller's ingest-log row carries an
        # audit-grade date despite ``ref.period_of_report=None``.
        assert outcome.period_of_report_override == date(2019, 3, 31)


# ---------------------------------------------------------------------------
# Bulk dataset gate
# ---------------------------------------------------------------------------


def _write_13f_dataset_zip(
    path: Path,
    rows: Iterable[dict[str, str]],
) -> None:
    """Build a synthetic Form 13F Data Set ZIP with 1 row per accession
    in SUBMISSION + COVERPAGE + INFOTABLE.

    Each input row carries:
      - accession: ``ACCESSION_NUMBER``
      - cik
      - filing_date (DD-MMM-YYYY)
      - period_end (DD-MMM-YYYY)
      - cusip
      - value
      - shprn
    """

    with zipfile.ZipFile(path, "w") as zf:
        sub_buf = io.StringIO()
        sub_w = csv.DictWriter(
            sub_buf,
            fieldnames=["ACCESSION_NUMBER", "CIK", "FILING_DATE"],
            delimiter="\t",
        )
        sub_w.writeheader()
        for r in rows:
            sub_w.writerow(
                {
                    "ACCESSION_NUMBER": r["accession"],
                    "CIK": r["cik"],
                    "FILING_DATE": r["filing_date"],
                }
            )
        zf.writestr("SUBMISSION.tsv", sub_buf.getvalue())

        cover_buf = io.StringIO()
        cover_w = csv.DictWriter(
            cover_buf,
            fieldnames=["ACCESSION_NUMBER", "FILINGMANAGER_NAME", "REPORTCALENDARORQUARTER"],
            delimiter="\t",
        )
        cover_w.writeheader()
        for r in rows:
            cover_w.writerow(
                {
                    "ACCESSION_NUMBER": r["accession"],
                    "FILINGMANAGER_NAME": "Test Filer",
                    "REPORTCALENDARORQUARTER": r["period_end"],
                }
            )
        zf.writestr("COVERPAGE.tsv", cover_buf.getvalue())

        info_buf = io.StringIO()
        info_w = csv.DictWriter(
            info_buf,
            fieldnames=[
                "ACCESSION_NUMBER",
                "CUSIP",
                "VALUE",
                "SSHPRNAMT",
                "SSHPRNAMTTYPE",
                "PUTCALL",
                "VOTING_AUTH_SOLE",
                "VOTING_AUTH_SHARED",
                "VOTING_AUTH_NONE",
            ],
            delimiter="\t",
        )
        info_w.writeheader()
        for r in rows:
            info_w.writerow(
                {
                    "ACCESSION_NUMBER": r["accession"],
                    "CUSIP": r["cusip"],
                    "VALUE": r["value"],
                    "SSHPRNAMT": r["shprn"],
                    "SSHPRNAMTTYPE": "SH",
                    "PUTCALL": "",
                    "VOTING_AUTH_SOLE": "100",
                    "VOTING_AUTH_SHARED": "0",
                    "VOTING_AUTH_NONE": "0",
                }
            )
        zf.writestr("INFOTABLE.tsv", info_buf.getvalue())


def _seed_instrument_and_cusip(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    cusip: str,
) -> None:
    """Minimal instrument + external_identifiers row for the bulk path."""
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name,
            currency, is_tradable, country
        ) VALUES (
            %(iid)s, %(sym)s, %(name)s, 'USD', TRUE, 'US'
        )
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        {"iid": iid, "sym": f"TEST{iid:03d}", "name": f"Test Instrument {iid}"},
    )
    conn.execute(
        """
        INSERT INTO external_identifiers (
            instrument_id, provider, identifier_type, identifier_value, is_primary
        ) VALUES (%(iid)s, 'sec', 'cusip', %(cusip)s, TRUE)
        ON CONFLICT DO NOTHING
        """,
        {"iid": iid, "cusip": cusip},
    )


class TestBulkDatasetGate:
    @pytest.fixture
    def archive_path(self, tmp_path: Path) -> Path:
        return tmp_path / "form13f_test.zip"

    def test_boundary_in_cap_and_pre_cap_split(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        archive_path: Path,
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument_and_cusip(conn, iid=900_001, cusip="000900001A0")
        _seed_instrument_and_cusip(conn, iid=900_002, cusip="000900002B0")
        _seed_instrument_and_cusip(conn, iid=900_003, cusip="000900003C0")

        # Three rows: pre-cap, boundary (==cutoff), in-cap. Cutoff at
        # 2026-05-20 ref = 2024-06-30. We force ``now`` via patch.
        rows = [
            {
                "accession": "0001000000-20-000001",
                "cik": "1234567",
                "filing_date": "15-APR-2020",
                "period_end": "31-MAR-2020",  # pre-cap
                "cusip": "000900001A0",
                "value": "10000",
                "shprn": "1000",
            },
            {
                "accession": "0001000000-24-000001",
                "cik": "1234567",
                "filing_date": "15-JUL-2024",
                "period_end": "30-JUN-2024",  # boundary == cutoff
                "cusip": "000900002B0",
                "value": "20000",
                "shprn": "2000",
            },
            {
                "accession": "0001000000-25-000001",
                "cik": "1234567",
                "filing_date": "15-APR-2025",
                "period_end": "31-MAR-2025",  # in-cap
                "cusip": "000900003C0",
                "value": "30000",
                "shprn": "3000",
            },
        ]
        _write_13f_dataset_zip(archive_path, rows)

        fixed_now = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        with patch("app.services.institutional_holdings.datetime") as mock_dt:
            mock_dt.now.return_value = fixed_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            result = ingest_13f_dataset_archive(
                conn=conn,
                archive_path=archive_path,
                ingest_run_id=uuid4(),
            )

        # Pre-cap skipped; boundary + in-cap survived.
        assert result.rows_written == 2
        assert result.rows_skipped_retention == 1
        # No current-refresh side effect for the skipped accession.
        assert 900_001 not in result.touched_instrument_ids
        assert 900_002 in result.touched_instrument_ids
        assert 900_003 in result.touched_instrument_ids


# ---------------------------------------------------------------------------
# sync_institutions cap predicate
# ---------------------------------------------------------------------------


def _seed_filer_and_holding(
    conn: psycopg.Connection[tuple],
    *,
    filer_id: int,
    cik: str,
    instrument_id: int,
    accession: str,
    period_of_report: date,
    filed_at: datetime,
) -> None:
    conn.execute(
        """
        INSERT INTO institutional_filers (filer_id, cik, name, filer_type)
        VALUES (%(fid)s, %(cik)s, %(name)s, 'INV')
        ON CONFLICT (filer_id) DO NOTHING
        """,
        {"fid": filer_id, "cik": cik, "name": f"Test Filer {filer_id}"},
    )
    conn.execute(
        """
        INSERT INTO institutional_holdings (
            filer_id, instrument_id, accession_number, period_of_report,
            shares, market_value_usd, voting_authority, is_put_call, filed_at
        ) VALUES (
            %(fid)s, %(iid)s, %(acc)s, %(period)s,
            1000, 50000, 'SOLE', NULL, %(filed_at)s
        )
        ON CONFLICT DO NOTHING
        """,
        {
            "fid": filer_id,
            "iid": instrument_id,
            "acc": accession,
            "period": period_of_report,
            "filed_at": filed_at,
        },
    )


class TestSyncInstitutionsCapPredicate:
    def test_pre_cap_rows_not_repopulated(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        # Two holdings under one filer — one pre-cap, one in-cap.
        _seed_instrument_and_cusip(conn, iid=910_001, cusip="000910001A0")
        _seed_instrument_and_cusip(conn, iid=910_002, cusip="000910002B0")
        _seed_filer_and_holding(
            conn,
            filer_id=99_001,
            cik="0009999999",
            instrument_id=910_001,
            accession="0009999999-20-000001",
            period_of_report=date(2020, 3, 31),
            filed_at=datetime(2020, 4, 15, tzinfo=UTC),
        )
        _seed_filer_and_holding(
            conn,
            filer_id=99_001,
            cik="0009999999",
            instrument_id=910_002,
            accession="0009999999-25-000001",
            period_of_report=date(2025, 3, 31),
            filed_at=datetime(2025, 4, 15, tzinfo=UTC),
        )
        conn.commit()

        fixed_now = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        with patch("app.services.institutional_holdings.datetime") as mock_dt:
            mock_dt.now.return_value = fixed_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            summary = sync_institutions(conn)
        conn.commit()

        # Only the in-cap row produced an observation row.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT instrument_id, period_end
                FROM ownership_institutions_observations
                WHERE filer_cik = '0009999999'
                ORDER BY instrument_id
                """
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 910_002
        assert rows[0][1] == date(2025, 3, 31)
        assert summary.observations_recorded == 1
