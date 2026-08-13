"""Tests for the SEC fundamentals observability service (#414, #418).

Uses the isolated ``ebull_test`` database via the shared
``ebull_test_conn`` fixture — destructive writes against the dev DB
are rejected by ``tests/smoke/test_no_settings_url_in_destructive_paths.py``
and by the ``_assert_test_db`` backstop in the fixture module.
``percentile_cont`` is a real Postgres aggregate, so a mocked cursor
would only test SQL string shape and miss actual execution.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from app.services.fundamentals_observability import (
    get_cik_timing_summary,
    get_seed_progress,
)
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _test_db_available(),
        reason="ebull_test DB unavailable",
    ),
]


def _insert_run(conn: psycopg.Connection[tuple], *, status: str = "success") -> int:
    row = conn.execute(
        """
        INSERT INTO data_ingestion_runs (source, endpoint, instrument_count, status)
        VALUES ('sec_edgar', '/api/xbrl/companyfacts', 1, %s)
        RETURNING ingestion_run_id
        """,
        (status,),
    ).fetchone()
    assert row is not None
    conn.commit()
    return int(row[0])


def _insert_timing(
    conn: psycopg.Connection[tuple],
    *,
    run_id: int | None,
    cik: str,
    mode: str,
    seconds: float,
    facts: int = 0,
    outcome: str = "success",
) -> None:
    now = datetime.now(tz=UTC)
    conn.execute(
        """
        INSERT INTO cik_upsert_timing (
            ingestion_run_id, cik, mode, outcome,
            facts_upserted, seconds, started_at, finished_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            cik,
            mode,
            outcome,
            facts,
            seconds,
            now - timedelta(seconds=seconds),
            now,
        ),
    )


class TestCikTimingSummary:
    def test_empty_table_returns_null_run_id(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        summary = get_cik_timing_summary(ebull_test_conn)
        assert summary.ingestion_run_id is None
        assert summary.modes == []
        assert summary.slowest == []

    def test_returns_percentiles_for_latest_run_only(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        older_run = _insert_run(ebull_test_conn)
        newer_run = _insert_run(ebull_test_conn)

        with ebull_test_conn.transaction():
            _insert_timing(ebull_test_conn, run_id=older_run, cik="1", mode="seed", seconds=99.0)
            for i, s in enumerate([0.5, 1.0, 1.5, 2.0, 9.0]):
                _insert_timing(
                    ebull_test_conn,
                    run_id=newer_run,
                    cik=f"{i}",
                    mode="seed",
                    seconds=s,
                    facts=10,
                )

        summary = get_cik_timing_summary(ebull_test_conn)

        assert summary.ingestion_run_id == newer_run
        assert len(summary.modes) == 1
        seed = summary.modes[0]
        assert seed.mode == "seed"
        assert seed.count == 5
        # p50 of [0.5, 1.0, 1.5, 2.0, 9.0] = 1.5
        assert seed.p50_seconds == pytest.approx(1.5)
        # p95 of same = 7.6 (interpolated)
        assert seed.p95_seconds == pytest.approx(7.6, rel=0.01)
        assert seed.max_seconds == pytest.approx(9.0)
        assert seed.facts_upserted_total == 50

    def test_slowest_capped_at_five(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        run = _insert_run(ebull_test_conn)
        with ebull_test_conn.transaction():
            for i in range(12):
                _insert_timing(
                    ebull_test_conn,
                    run_id=run,
                    cik=f"{i:04d}",
                    mode="seed",
                    seconds=float(i),
                )

        summary = get_cik_timing_summary(ebull_test_conn)
        assert len(summary.slowest) == 5
        # Descending by seconds.
        assert summary.slowest[0].seconds == pytest.approx(11.0)
        assert summary.slowest[-1].seconds == pytest.approx(7.0)

    def test_mode_split_produces_two_rows(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        run = _insert_run(ebull_test_conn)
        with ebull_test_conn.transaction():
            _insert_timing(ebull_test_conn, run_id=run, cik="1", mode="seed", seconds=5.0)
            _insert_timing(ebull_test_conn, run_id=run, cik="2", mode="refresh", seconds=0.1)

        summary = get_cik_timing_summary(ebull_test_conn)
        modes = {m.mode: m for m in summary.modes}
        assert set(modes.keys()) == {"seed", "refresh"}
        assert modes["seed"].count == 1
        assert modes["refresh"].count == 1


class TestSeedProgress:
    def test_returns_zero_seeded_when_no_watermarks(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        summary = get_seed_progress(ebull_test_conn)
        sources = {s.source: s for s in summary.sources}
        assert "sec.submissions" in sources
        assert sources["sec.submissions"].seeded == 0
        # Universe is truncated by the fixture → total == 0 here. Just
        # assert the contract: total is a non-negative int.
        assert sources["sec.submissions"].total >= 0
        assert summary.ingest_paused is False

    def test_ingest_paused_flag_tracks_layer_enabled_row(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        with ebull_test_conn.transaction():
            ebull_test_conn.execute(
                """
                INSERT INTO layer_enabled (layer_name, is_enabled, updated_at)
                VALUES ('fundamentals_ingest', FALSE, now())
                ON CONFLICT (layer_name) DO UPDATE SET is_enabled = EXCLUDED.is_enabled
                """
            )
        summary = get_seed_progress(ebull_test_conn)
        assert summary.ingest_paused is True

    def test_absent_layer_row_counts_as_not_paused(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        summary = get_seed_progress(ebull_test_conn)
        assert summary.ingest_paused is False

    def test_seeded_count_restricted_to_tradable_cohort(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Counter-case surfaced by Codex review: an orphan watermark
        for a de-listed / untracked CIK must NOT count toward
        ``seeded`` — otherwise the progress bar can exceed 100%.
        """
        with ebull_test_conn.transaction():
            # Tradable CIK + watermark: should count in both numerator
            # and denominator.
            ebull_test_conn.execute(
                "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
                "VALUES (1, 'FOO', 'Foo Inc', TRUE)"
            )
            ebull_test_conn.execute(
                "INSERT INTO external_identifiers "
                "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
                "VALUES (1, 'sec', 'cik', '0000000001', TRUE)"
            )
            ebull_test_conn.execute(
                "INSERT INTO external_data_watermarks (source, key, watermark) "
                "VALUES ('sec.submissions', '0000000001', 'acc-1')"
            )
            # Orphan watermark (CIK not in external_identifiers at all).
            # Should count in neither.
            ebull_test_conn.execute(
                "INSERT INTO external_data_watermarks (source, key, watermark) "
                "VALUES ('sec.submissions', '0000000099', 'acc-99')"
            )

        summary = get_seed_progress(ebull_test_conn)
        src = next(s for s in summary.sources if s.source == "sec.submissions")
        assert src.seeded == 1
        assert src.total == 1
        # Invariant: seeded can never exceed total.
        assert src.seeded <= src.total

    def test_latest_run_surfaces_most_recent_sec_edgar_run(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        older = _insert_run(ebull_test_conn, status="success")
        newer = _insert_run(ebull_test_conn, status="running")

        summary = get_seed_progress(ebull_test_conn)
        assert summary.latest_run is not None
        assert summary.latest_run.ingestion_run_id == newer
        assert summary.latest_run.status == "running"
        assert newer > older  # sanity
