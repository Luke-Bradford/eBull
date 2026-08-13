"""Integration tests for `app/jobs/sec_master_idx_quarterly_sweep.py` (G12).

Spec: docs/superpowers/specs/2026-05-17-g12-master-idx-quarterly-walker.md §6.2.
Plan: docs/superpowers/plans/2026-05-17-g12-master-idx-quarterly-walker-plan.md §T7.

Uses the per-worker ``ebull_test_db`` fixture (NOT the dev DB) and
explicit FK-respecting seed helpers per the plan T7 contract.
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg
import pytest

from app.jobs.sec_atom_fast_lane import ResolvedSubject
from app.jobs.sec_master_idx_quarterly_sweep import (
    MasterIdxSweepStats,
    QuarterStats,
    _current_calendar_quarter,
    _previous_calendar_quarter,
    _quarters_to_walk,
    build_preloaded_subject_resolver,
    run_master_idx_quarterly_sweep,
)
from app.providers.implementations.sec_daily_index import HttpGet
from app.services.sec_manifest import (
    get_manifest_row,
    record_manifest_entry,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

pytestmark = pytest.mark.integration


# -- Fake http_get -----------------------------------------------------------


def _quarter_url(year: int, quarter: int) -> str:
    return f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"


def _make_dispatch_http_get(
    by_url: dict[str, tuple[int, bytes] | Exception],
) -> HttpGet:
    """Build a fake ``http_get`` that dispatches on URL.

    ``by_url`` values can be a (status, body) tuple OR an Exception
    instance to be raised on access (simulates a network failure).
    """

    def _impl(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        outcome = by_url.get(url)
        if outcome is None:
            raise AssertionError(f"unexpected fake_get URL: {url}")
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return _impl


# -- Seed helpers ------------------------------------------------------------


def _seed_issuer(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    cik: str,
    symbol: str,
    company_name: str | None = None,
) -> None:
    """Seed an issuer row in instruments + instrument_sec_profile.

    FK chain: instruments(instrument_id) PK ← instrument_sec_profile(instrument_id).
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
            "VALUES (%s, %s, %s, TRUE) ON CONFLICT (instrument_id) DO NOTHING",
            (instrument_id, symbol, company_name or symbol),
        )
        cur.execute(
            "INSERT INTO instrument_sec_profile (instrument_id, cik) "
            "VALUES (%s, %s) ON CONFLICT (instrument_id) DO NOTHING",
            (instrument_id, cik),
        )


def _seed_institutional_filer(conn: psycopg.Connection[tuple], *, cik: str, name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO institutional_filers (cik, name) VALUES (%s, %s) ON CONFLICT (cik) DO NOTHING",
            (cik, name),
        )


def _seed_blockholder_filer(conn: psycopg.Connection[tuple], *, cik: str, name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO blockholder_filers (cik, name) VALUES (%s, %s) ON CONFLICT (cik) DO NOTHING",
            (cik, name),
        )


# -- Resolver helpers --------------------------------------------------------


def _static_resolver(mapping: dict[str, ResolvedSubject]):
    def _resolve(conn, cik):
        return mapping.get(cik)

    return _resolve


# -- Canned bodies -----------------------------------------------------------

_AAPL_8K_2026_Q2 = b"""\
Description:           Master Index of EDGAR Dissemination Feed
Last Data Received:    June 30, 2026

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
320193|Apple Inc.|8-K|2026-05-15|edgar/data/320193/0000320193-26-000042.txt
1318605|Tesla Inc|8-K|2026-05-20|edgar/data/1318605/0001318605-26-000033.txt
"""

_AAPL_8K_2026_Q1 = b"""\
Description:           Master Index of EDGAR Dissemination Feed
Last Data Received:    March 31, 2026

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
320193|Apple Inc.|8-K|2026-02-10|edgar/data/320193/0000320193-26-000001.txt
"""

_AAPL_S1_2026_Q2 = b"""\
Description:           Master Index of EDGAR Dissemination Feed
Last Data Received:    June 30, 2026

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
320193|Apple Inc.|S-1|2026-05-15|edgar/data/320193/0000320193-26-000099.txt
"""

# #1415 — a filing-metadata form (8-K → sec_8k) AND a bulk-dataset
# ownership form (13F-HR → sec_13f_hr) in the same quarter, for the
# source_allowlist filter test.
_MIXED_8K_AND_13FHR_Q2 = b"""\
Description:           Master Index of EDGAR Dissemination Feed
Last Data Received:    June 30, 2026

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
320193|Apple Inc.|8-K|2026-05-15|edgar/data/320193/0000320193-26-000042.txt
1067983|Berkshire Hathaway Inc|13F-HR|2026-05-15|edgar/data/1067983/0001067983-26-000002.txt
"""

# Header-only (zero rows) valid index — used as the prev-quarter body so
# it neither fails nor adds rows.
_EMPTY_IDX = b"""\
Description:           Master Index of EDGAR Dissemination Feed
Last Data Received:    March 31, 2026

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
"""


# -- Tests: pure-function helpers -------------------------------------------


class TestQuartersToWalk:
    def test_mid_year(self) -> None:
        assert _quarters_to_walk(datetime(2026, 5, 17, tzinfo=UTC)) == [
            (2026, 2),
            (2026, 1),
        ]

    def test_jan_rollover(self) -> None:
        assert _quarters_to_walk(datetime(2026, 1, 5, tzinfo=UTC)) == [
            (2026, 1),
            (2025, 4),
        ]

    @pytest.mark.parametrize(
        ("now_dt", "expected"),
        [
            (datetime(2026, 1, 1, tzinfo=UTC), [(2026, 1), (2025, 4)]),
            (datetime(2026, 3, 31, tzinfo=UTC), [(2026, 1), (2025, 4)]),
            (datetime(2026, 4, 1, tzinfo=UTC), [(2026, 2), (2026, 1)]),
            (datetime(2026, 6, 30, tzinfo=UTC), [(2026, 2), (2026, 1)]),
            (datetime(2026, 7, 1, tzinfo=UTC), [(2026, 3), (2026, 2)]),
            (datetime(2026, 9, 30, tzinfo=UTC), [(2026, 3), (2026, 2)]),
            (datetime(2026, 10, 1, tzinfo=UTC), [(2026, 4), (2026, 3)]),
            (datetime(2026, 12, 31, tzinfo=UTC), [(2026, 4), (2026, 3)]),
        ],
    )
    def test_quarter_boundaries(self, now_dt: datetime, expected: list[tuple[int, int]]) -> None:
        assert _quarters_to_walk(now_dt) == expected

    def test_previous_quarter_q1_rolls_to_prior_year_q4(self) -> None:
        assert _previous_calendar_quarter(2026, 1) == (2025, 4)

    def test_current_calendar_quarter_pure(self) -> None:
        assert _current_calendar_quarter(datetime(2026, 5, 17, tzinfo=UTC)) == (
            2026,
            2,
        )


# -- Tests: full-loop walker -------------------------------------------------


class TestRunMasterIdxQuarterlySweep:
    def test_happy_path_walks_two_quarters_and_upserts_in_universe_rows(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_issuer(ebull_test_conn, instrument_id=7, cik="0000320193", symbol="AAPL")
        ebull_test_conn.commit()

        now = datetime(2026, 5, 17, tzinfo=UTC)  # CQ=Q2, CQ-1=Q1
        resolver = _static_resolver(
            {
                "0000320193": ResolvedSubject(subject_type="issuer", subject_id="7", instrument_id=7),
            }
        )
        stats = run_master_idx_quarterly_sweep(
            ebull_test_conn,
            http_get=_make_dispatch_http_get(
                {
                    _quarter_url(2026, 2): (200, _AAPL_8K_2026_Q2),
                    _quarter_url(2026, 1): (200, _AAPL_8K_2026_Q1),
                }
            ),
            now=now,
            subject_resolver=resolver,
        )

        # Two quarters walked, both succeed
        assert len(stats.quarters) == 2
        assert stats.failed_quarters == 0
        # Q2 has 2 rows (AAPL + TSLA), only AAPL in-universe
        q2 = stats.quarters[0]
        assert q2.year == 2026 and q2.quarter == 2
        assert q2.index_rows == 2
        assert q2.matched_in_universe == 1
        assert q2.upserted == 1
        assert q2.skipped_unknown_subject == 1
        # Q1 has 1 row (AAPL), in-universe
        q1 = stats.quarters[1]
        assert q1.year == 2026 and q1.quarter == 1
        assert q1.upserted == 1
        # Total
        assert stats.total_upserted == 2

    def test_source_allowlist_filters_ownership_sources(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """#1415 — the bootstrap gap-close passes a filing-metadata
        ``source_allowlist`` so it advances ONLY filing-metadata watermarks,
        never the bulk-dataset ownership sources (13F-HR / N-PORT / Form-3/4/5
        — their observations come from the quarterly bulk datasets, NOT the
        manifest worker, so a discovery-side watermark advance would push the
        steady-state cursor ahead of loaded data: silent gap, spec §4.3).

        A 13F-HR row alongside an 8-K must be SKIPPED (no manifest row, no
        ``data_freshness_index`` row) when ``source_allowlist={"sec_8k"}``.
        """
        _seed_issuer(ebull_test_conn, instrument_id=7, cik="0000320193", symbol="AAPL")
        _seed_institutional_filer(ebull_test_conn, cik="0001067983", name="Berkshire Hathaway Inc")
        ebull_test_conn.commit()

        now = datetime(2026, 5, 17, tzinfo=UTC)  # CQ=Q2, CQ-1=Q1
        resolver = _static_resolver(
            {
                "0000320193": ResolvedSubject(subject_type="issuer", subject_id="7", instrument_id=7),
                "0001067983": ResolvedSubject(
                    subject_type="institutional_filer", subject_id="0001067983", instrument_id=None
                ),
            }
        )
        stats = run_master_idx_quarterly_sweep(
            ebull_test_conn,
            http_get=_make_dispatch_http_get(
                {
                    _quarter_url(2026, 2): (200, _MIXED_8K_AND_13FHR_Q2),
                    _quarter_url(2026, 1): (200, _EMPTY_IDX),
                }
            ),
            now=now,
            subject_resolver=resolver,
            source_allowlist=frozenset({"sec_8k"}),
        )
        ebull_test_conn.commit()

        # Only the 8-K upserted; the 13F-HR row is filtered by the allowlist.
        assert stats.failed_quarters == 0
        assert stats.total_upserted == 1
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM sec_filing_manifest WHERE source = 'sec_8k'")
            assert cur.fetchone()[0] == 1  # type: ignore[index]
            cur.execute("SELECT count(*) FROM sec_filing_manifest WHERE source = 'sec_13f_hr'")
            assert cur.fetchone()[0] == 0, "13F-HR must NOT be seeded by the guarded gap-close"  # type: ignore[index]
            cur.execute("SELECT count(*) FROM data_freshness_index WHERE source = 'sec_13f_hr'")
            assert cur.fetchone()[0] == 0, "ownership-source watermark must NOT advance from gap-close discovery"  # type: ignore[index]

    def test_unmapped_form_skipped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_issuer(ebull_test_conn, instrument_id=7, cik="0000320193", symbol="AAPL")
        ebull_test_conn.commit()

        resolver = _static_resolver(
            {
                "0000320193": ResolvedSubject(subject_type="issuer", subject_id="7", instrument_id=7),
            }
        )
        # S-1 has no ManifestSource → skipped before resolver fires
        stats = run_master_idx_quarterly_sweep(
            ebull_test_conn,
            http_get=_make_dispatch_http_get({_quarter_url(2024, 1): (200, _AAPL_S1_2026_Q2)}),
            quarters=[(2024, 1)],
            subject_resolver=resolver,
        )
        assert stats.total_upserted == 0
        assert stats.quarters[0].skipped_unmapped_form == 1

    def test_current_quarter_404_does_not_abort_previous_quarter(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_issuer(ebull_test_conn, instrument_id=7, cik="0000320193", symbol="AAPL")
        ebull_test_conn.commit()

        now = datetime(2026, 5, 17, tzinfo=UTC)  # CQ=Q2
        resolver = _static_resolver(
            {
                "0000320193": ResolvedSubject(subject_type="issuer", subject_id="7", instrument_id=7),
            }
        )
        stats = run_master_idx_quarterly_sweep(
            ebull_test_conn,
            http_get=_make_dispatch_http_get(
                {
                    _quarter_url(2026, 2): (404, b""),  # current quarter — allowed
                    _quarter_url(2026, 1): (200, _AAPL_8K_2026_Q1),
                }
            ),
            now=now,
            subject_resolver=resolver,
        )
        cq = stats.quarters[0]
        cq_minus_1 = stats.quarters[1]
        assert cq.year == 2026 and cq.quarter == 2
        assert cq.failed is False
        assert cq.index_rows == 0
        assert cq_minus_1.upserted == 1
        assert stats.total_upserted == 1

    def test_previous_quarter_404_is_treated_as_failure(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Pin: asymmetric 404 contract (Codex 1a r1 HIGH-2)."""
        _seed_issuer(ebull_test_conn, instrument_id=7, cik="0000320193", symbol="AAPL")
        ebull_test_conn.commit()

        now = datetime(2026, 5, 17, tzinfo=UTC)  # CQ=Q2, CQ-1=Q1
        resolver = _static_resolver(
            {
                "0000320193": ResolvedSubject(subject_type="issuer", subject_id="7", instrument_id=7),
            }
        )
        stats = run_master_idx_quarterly_sweep(
            ebull_test_conn,
            http_get=_make_dispatch_http_get(
                {
                    _quarter_url(2026, 2): (200, _AAPL_8K_2026_Q2),
                    _quarter_url(2026, 1): (404, b""),  # previous quarter — strict
                }
            ),
            now=now,
            subject_resolver=resolver,
        )
        cq = stats.quarters[0]
        cq_minus_1 = stats.quarters[1]
        assert cq.upserted == 1
        assert cq_minus_1.failed is True
        assert cq_minus_1.error_detail is not None
        assert "status=404" in cq_minus_1.error_detail

    def test_previous_quarter_failure_does_not_abort_current_quarter(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_issuer(ebull_test_conn, instrument_id=7, cik="0000320193", symbol="AAPL")
        ebull_test_conn.commit()

        now = datetime(2026, 5, 17, tzinfo=UTC)
        resolver = _static_resolver(
            {
                "0000320193": ResolvedSubject(subject_type="issuer", subject_id="7", instrument_id=7),
            }
        )
        stats = run_master_idx_quarterly_sweep(
            ebull_test_conn,
            http_get=_make_dispatch_http_get(
                {
                    _quarter_url(2026, 2): (200, _AAPL_8K_2026_Q2),
                    _quarter_url(2026, 1): RuntimeError("network down"),
                }
            ),
            now=now,
            subject_resolver=resolver,
        )
        assert stats.quarters[0].upserted == 1
        assert stats.quarters[1].failed is True
        assert "network down" in (stats.quarters[1].error_detail or "")

    def test_quarter_failure_rolls_back_partial_writes_in_that_quarter(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Pin: per-quarter txn isolation (Codex 1a r1 HIGH-1 + 1b r1 MED-1).

        Mid-loop synthetic OperationalError on the third row's resolver
        call → conn.rollback() discards the previous two UPSERTs in
        BOTH sec_filing_manifest AND data_freshness_index.
        """
        _seed_issuer(ebull_test_conn, instrument_id=7, cik="0000320193", symbol="AAPL")
        _seed_issuer(ebull_test_conn, instrument_id=8, cik="0001067983", symbol="BRK")
        _seed_issuer(ebull_test_conn, instrument_id=9, cik="0000789019", symbol="MSFT")
        ebull_test_conn.commit()

        # Resolver maps 320193 + 1067983 in-universe; on third call (789019)
        # raise to simulate a mid-loop psycopg failure.
        call_count = {"n": 0}

        def _failing_resolver(conn, cik):
            call_count["n"] += 1
            if call_count["n"] == 3:
                raise psycopg.errors.OperationalError("synthetic connection failure")
            mapping = {
                "0000320193": ResolvedSubject(subject_type="issuer", subject_id="7", instrument_id=7),
                "0001067983": ResolvedSubject(subject_type="issuer", subject_id="8", instrument_id=8),
            }
            return mapping.get(cik)

        body = b"""\
Description:           Master Index
Last Data Received:    March 31, 2024

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
320193|Apple Inc.|8-K|2024-01-15|edgar/data/320193/0000320193-24-000001.txt
1067983|Berkshire|8-K|2024-02-10|edgar/data/1067983/0001067983-24-000001.txt
789019|MSFT|8-K|2024-03-15|edgar/data/789019/0000789019-24-000001.txt
"""

        stats = run_master_idx_quarterly_sweep(
            ebull_test_conn,
            http_get=_make_dispatch_http_get({_quarter_url(2024, 1): (200, body)}),
            quarters=[(2024, 1)],
            subject_resolver=_failing_resolver,
        )

        assert len(stats.quarters) == 1
        assert stats.quarters[0].failed is True
        assert stats.quarters[0].upserted == 0  # rolled back
        # Verify cross-table rollback: NO sec_filing_manifest rows AND NO
        # data_freshness_index rows for any of the three accessions.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM sec_filing_manifest WHERE accession_number IN (%s, %s, %s)",
                (
                    "0000320193-24-000001",
                    "0001067983-24-000001",
                    "0000789019-24-000001",
                ),
            )
            manifest_row = cur.fetchone()
            assert manifest_row is not None and manifest_row[0] == 0
            cur.execute("SELECT COUNT(*) FROM data_freshness_index WHERE subject_id IN ('7', '8', '9')")
            freshness_row = cur.fetchone()
            assert freshness_row is not None and freshness_row[0] == 0

    def test_successful_quarter_commits_before_next_quarter(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Pin: per-quarter commit boundary (Codex 1b r1 MED-1).

        First quarter succeeds + commits; second quarter raises mid-loop.
        First quarter's UPSERT survives in BOTH sec_filing_manifest AND
        data_freshness_index, observable through a fresh psycopg cursor.
        """
        _seed_issuer(ebull_test_conn, instrument_id=7, cik="0000320193", symbol="AAPL")
        ebull_test_conn.commit()

        now = datetime(2026, 5, 17, tzinfo=UTC)
        resolver = _static_resolver(
            {
                "0000320193": ResolvedSubject(subject_type="issuer", subject_id="7", instrument_id=7),
            }
        )
        stats = run_master_idx_quarterly_sweep(
            ebull_test_conn,
            http_get=_make_dispatch_http_get(
                {
                    _quarter_url(2026, 2): (200, _AAPL_8K_2026_Q2),
                    _quarter_url(2026, 1): RuntimeError("explode"),
                }
            ),
            now=now,
            subject_resolver=resolver,
        )
        assert stats.quarters[0].upserted == 1
        assert stats.quarters[1].failed is True

        # Fresh cursor sees the durably-committed first-quarter row in
        # both tables.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM sec_filing_manifest WHERE accession_number = %s",
                ("0000320193-26-000042",),
            )
            assert cur.fetchone() is not None
            cur.execute(
                "SELECT 1 FROM data_freshness_index WHERE subject_id = %s AND source = %s",
                ("7", "sec_8k"),
            )
            assert cur.fetchone() is not None

    def test_explicit_quarters_kwarg_overrides_default_window(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_issuer(ebull_test_conn, instrument_id=7, cik="0000320193", symbol="AAPL")
        ebull_test_conn.commit()

        now = datetime(2026, 5, 17, tzinfo=UTC)  # would default to [Q2, Q1]
        resolver = _static_resolver(
            {
                "0000320193": ResolvedSubject(subject_type="issuer", subject_id="7", instrument_id=7),
            }
        )
        # Only 2024 Q1 URL is fetched; if walker fell through to Q2/Q1
        # 2026 the dispatch table's AssertionError would fire.
        stats = run_master_idx_quarterly_sweep(
            ebull_test_conn,
            http_get=_make_dispatch_http_get({_quarter_url(2024, 1): (200, _AAPL_8K_2026_Q1)}),
            now=now,
            quarters=[(2024, 1)],
            subject_resolver=resolver,
        )
        assert len(stats.quarters) == 1
        assert stats.quarters[0] == QuarterStats(
            year=2024,
            quarter=1,
            index_rows=1,
            matched_in_universe=1,
            upserted=1,
            skipped_unmapped_form=0,
            skipped_unknown_subject=0,
            failed=False,
            error_detail=None,
        )

    def test_idempotency_re_walk_preserves_in_flight_status(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Pin: ON CONFLICT preserves ingest_status.

        Reads post-UPSERT ingest_status directly from DB (NOT Python
        arithmetic on a captured timestamp) — complies with the
        time-monotonicity prevention-log entry.
        """
        _seed_issuer(ebull_test_conn, instrument_id=7, cik="0000320193", symbol="AAPL")
        ebull_test_conn.commit()

        accession = "0000320193-26-000042"
        # Pre-seed the row, then move it to parsed.
        record_manifest_entry(
            ebull_test_conn,
            accession,
            cik="0000320193",
            form="8-K",
            source="sec_8k",
            subject_type="issuer",
            subject_id="7",
            instrument_id=7,
            filed_at=datetime(2026, 5, 15, tzinfo=UTC),
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE sec_filing_manifest SET ingest_status = 'parsed' WHERE accession_number = %s",
                (accession,),
            )
        ebull_test_conn.commit()

        resolver = _static_resolver(
            {
                "0000320193": ResolvedSubject(subject_type="issuer", subject_id="7", instrument_id=7),
            }
        )
        run_master_idx_quarterly_sweep(
            ebull_test_conn,
            http_get=_make_dispatch_http_get({_quarter_url(2026, 2): (200, _AAPL_8K_2026_Q2)}),
            quarters=[(2026, 2)],
            subject_resolver=resolver,
        )
        # Re-read from DB (no Python arithmetic). The UPSERT fired but
        # ON CONFLICT preserves ingest_status = 'parsed'.
        row = get_manifest_row(ebull_test_conn, accession)
        assert row is not None
        assert row.ingest_status == "parsed"

    def test_stats_total_upserted_aggregates_across_quarters(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_issuer(ebull_test_conn, instrument_id=7, cik="0000320193", symbol="AAPL")
        _seed_issuer(ebull_test_conn, instrument_id=9, cik="0000789019", symbol="MSFT")
        ebull_test_conn.commit()

        body_a = b"""\
Description:           Master Index
Last Data Received:    March 31, 2024

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
320193|Apple Inc.|8-K|2024-01-15|edgar/data/320193/0000320193-24-100001.txt
789019|MSFT|10-K|2024-03-15|edgar/data/789019/0000789019-24-100002.txt
"""
        body_b = b"""\
Description:           Master Index
Last Data Received:    March 31, 2024

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
320193|Apple Inc.|8-K|2024-01-15|edgar/data/320193/0000320193-24-200001.txt
789019|MSFT|10-K|2024-03-15|edgar/data/789019/0000789019-24-200002.txt
"""
        now = datetime(2026, 5, 17, tzinfo=UTC)
        resolver = _static_resolver(
            {
                "0000320193": ResolvedSubject(subject_type="issuer", subject_id="7", instrument_id=7),
                "0000789019": ResolvedSubject(subject_type="issuer", subject_id="9", instrument_id=9),
            }
        )
        stats = run_master_idx_quarterly_sweep(
            ebull_test_conn,
            http_get=_make_dispatch_http_get(
                {
                    _quarter_url(2026, 2): (200, body_a),
                    _quarter_url(2026, 1): (200, body_b),
                }
            ),
            now=now,
            subject_resolver=resolver,
        )
        assert stats.total_upserted == 4
        assert stats.failed_quarters == 0


class TestStatsContract:
    def test_master_idx_sweep_stats_total_upserted_property(self) -> None:
        stats = MasterIdxSweepStats(
            quarters=[
                QuarterStats(year=2024, quarter=1, upserted=3),
                QuarterStats(year=2024, quarter=2, upserted=5, failed=False),
            ]
        )
        assert stats.total_upserted == 8
        assert stats.failed_quarters == 0

    def test_failed_quarters_property(self) -> None:
        stats = MasterIdxSweepStats(
            quarters=[
                QuarterStats(year=2024, quarter=1, upserted=3),
                QuarterStats(year=2024, quarter=2, upserted=0, failed=True, error_detail="boom"),
            ]
        )
        assert stats.failed_quarters == 1
        assert stats.total_upserted == 3


# -- Tests: preloaded resolver priority + cohort coverage --------------------


class TestPreloadedResolver:
    def test_priority_issuer_over_institutional_filer(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Seed CIK as BOTH issuer AND institutional_filer.
        _seed_issuer(ebull_test_conn, instrument_id=42, cik="0000320193", symbol="AAPL")
        _seed_institutional_filer(ebull_test_conn, cik="0000320193", name="Apple Inc.")
        ebull_test_conn.commit()

        resolver = build_preloaded_subject_resolver(ebull_test_conn)
        result = resolver(ebull_test_conn, "0000320193")
        assert result is not None
        assert result.subject_type == "issuer"
        assert result.instrument_id == 42

    def test_priority_institutional_over_blockholder(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Pin: institutional > blockholder priority (Codex 1b r1 HIGH-2)."""
        _seed_institutional_filer(ebull_test_conn, cik="0001067983", name="Berkshire")
        _seed_blockholder_filer(ebull_test_conn, cik="0001067983", name="Berkshire (blockholder)")
        ebull_test_conn.commit()

        resolver = build_preloaded_subject_resolver(ebull_test_conn)
        result = resolver(ebull_test_conn, "0001067983")
        assert result is not None
        assert result.subject_type == "institutional_filer"
        assert result.instrument_id is None

    def test_blockholder_only_cik_resolves(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_blockholder_filer(ebull_test_conn, cik="0001234567", name="Activist Inc.")
        ebull_test_conn.commit()

        resolver = build_preloaded_subject_resolver(ebull_test_conn)
        result = resolver(ebull_test_conn, "0001234567")
        assert result is not None
        assert result.subject_type == "blockholder_filer"

    def test_unknown_cik_returns_none(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        resolver = build_preloaded_subject_resolver(ebull_test_conn)
        assert resolver(ebull_test_conn, "9999999999") is None
