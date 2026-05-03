"""Integration tests for the ingest-status service + operator API
(#793, Batch 4 of #788).

Exercises the grouped-provider rollup, queue counts, group-state
fold, recent-failures surface, and the enqueue-backfill helper.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import psycopg.rows
import pytest

from app.services import ingest_status
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_run(
    conn: psycopg.Connection[tuple],
    *,
    source: str,
    started_at: datetime,
    status: str,
    rows_upserted: int = 0,
    error: str | None = None,
) -> None:
    finished_at = started_at + timedelta(seconds=30) if status != "running" else None
    conn.execute(
        """
        INSERT INTO data_ingestion_runs (
            source, started_at, finished_at, status, rows_upserted, error
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (source, started_at, finished_at, status, rows_upserted, error),
    )


def _seed_queue_row(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    pipeline_name: str,
    status: str = "pending",
    priority: int = 100,
) -> None:
    conn.execute(
        """
        INSERT INTO ingest_backfill_queue (
            instrument_id, pipeline_name, priority, status
        ) VALUES (%s, %s, %s, %s)
        ON CONFLICT (instrument_id, pipeline_name) DO UPDATE
        SET status = EXCLUDED.status, priority = EXCLUDED.priority
        """,
        (instrument_id, pipeline_name, priority, status),
    )


# ---------------------------------------------------------------------------
# Provider grouping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("sec_edgar", "sec_fundamentals"),
        ("sec_edgar_xbrl", "sec_fundamentals"),
        ("sec.companyfacts", "sec_fundamentals"),
        ("sec.submissions", "sec_fundamentals"),
        ("sec_edgar_13f", "sec_ownership"),
        ("sec_edgar_13dg", "sec_ownership"),
        ("sec_edgar_13d", "sec_ownership"),
        ("sec_edgar_form4", "sec_ownership"),
        ("sec_edgar_form3", "sec_ownership"),
        ("sec_edgar_def14a", "sec_ownership"),
        ("sec_edgar_ncen", "sec_ownership"),
        ("sec_edgar_nport", "sec_ownership"),
        ("etoro_candles", "etoro"),
        ("etoro", "etoro"),
        ("finra_short_interest", "fundamentals_other"),
        ("companies_house", "fundamentals_other"),
        ("unknown_provider", "other"),
        ("", "other"),
    ],
)
def test_group_for_source(source: str, expected: str) -> None:
    assert ingest_status.group_for_source(source) == expected


# ---------------------------------------------------------------------------
# Grouped status rollup
# ---------------------------------------------------------------------------


class TestIngestStatus:
    def test_empty_db_returns_never_run_for_each_canonical_group(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        report = ingest_status.get_ingest_status(ebull_test_conn)
        keys = [g.key for g in report.groups]
        # ``other`` is hidden when empty; the four canonical groups
        # always render.
        assert "sec_fundamentals" in keys
        assert "sec_ownership" in keys
        assert "etoro" in keys
        assert "fundamentals_other" in keys
        assert "other" not in keys
        for g in report.groups:
            assert g.state == "never_run"
            assert g.sources == ()

    def test_groups_recent_successful_run_as_green(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_run(
            conn,
            source="sec_edgar_13f",
            started_at=datetime.now(tz=UTC) - timedelta(hours=1),
            status="success",
            rows_upserted=500,
        )
        conn.commit()
        report = ingest_status.get_ingest_status(conn)
        ownership = next(g for g in report.groups if g.key == "sec_ownership")
        assert ownership.state == "green"
        assert len(ownership.sources) == 1
        assert ownership.sources[0].source == "sec_edgar_13f"
        assert ownership.sources[0].rows_upserted_total == 500

    def test_failed_recent_run_promotes_amber(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """One success + one recent failure → amber (recovery in progress)."""
        conn = ebull_test_conn
        _seed_run(
            conn,
            source="sec_edgar",
            started_at=datetime.now(tz=UTC) - timedelta(hours=2),
            status="success",
        )
        _seed_run(
            conn,
            source="sec_edgar",
            started_at=datetime.now(tz=UTC) - timedelta(minutes=30),
            status="failed",
            error="timeout fetching companyfacts",
        )
        conn.commit()
        report = ingest_status.get_ingest_status(conn)
        sec_fund = next(g for g in report.groups if g.key == "sec_fundamentals")
        assert sec_fund.state == "amber"

    def test_many_failures_promotes_red(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """> 3 failures in 24h → red."""
        conn = ebull_test_conn
        now = datetime.now(tz=UTC)
        _seed_run(conn, source="etoro_candles", started_at=now - timedelta(hours=12), status="success")
        for i in range(4):
            _seed_run(
                conn,
                source="etoro_candles",
                started_at=now - timedelta(hours=i + 1),
                status="failed",
                error=f"flap {i}",
            )
        conn.commit()
        report = ingest_status.get_ingest_status(conn)
        etoro = next(g for g in report.groups if g.key == "etoro")
        assert etoro.state == "red"

    def test_stale_success_over_7d_promotes_amber(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Last success > 7 days ago, no recent activity → amber."""
        conn = ebull_test_conn
        _seed_run(
            conn,
            source="sec.companyfacts",
            started_at=datetime.now(tz=UTC) - timedelta(days=10),
            status="success",
        )
        conn.commit()
        report = ingest_status.get_ingest_status(conn)
        sec_fund = next(g for g in report.groups if g.key == "sec_fundamentals")
        assert sec_fund.state == "amber"

    def test_failed_only_source_is_red_not_never_run(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Codex pre-push review (Batch 4 of #788) caught this: a
        provider that has only ever failed must surface as ``red``,
        not ``never_run``. The page exists to answer "why is data
        missing?"; hiding active failures behind a "Never run"
        badge defeats the purpose."""
        conn = ebull_test_conn
        _seed_run(
            conn,
            source="sec_edgar",
            started_at=datetime.now(tz=UTC) - timedelta(hours=1),
            status="failed",
            error="cold-start failure",
        )
        conn.commit()
        report = ingest_status.get_ingest_status(conn)
        sec_fund = next(g for g in report.groups if g.key == "sec_fundamentals")
        assert sec_fund.state == "red"

    def test_other_group_emerges_when_unmapped_source_runs(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A new source not in the curated map surfaces under ``other``."""
        conn = ebull_test_conn
        _seed_run(
            conn,
            source="my_custom_pipeline",
            started_at=datetime.now(tz=UTC) - timedelta(hours=1),
            status="success",
        )
        conn.commit()
        report = ingest_status.get_ingest_status(conn)
        keys = [g.key for g in report.groups]
        assert "other" in keys
        other = next(g for g in report.groups if g.key == "other")
        assert other.sources[0].source == "my_custom_pipeline"

    def test_queue_backlog_counts_fold_into_groups(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=793_001, symbol="QBQ")
        _seed_queue_row(
            conn,
            instrument_id=793_001,
            pipeline_name="sec_edgar_form3",
            status="pending",
        )
        _seed_queue_row(
            conn,
            instrument_id=793_001,
            pipeline_name="sec_edgar_13f",
            status="failed",
        )
        conn.commit()
        report = ingest_status.get_ingest_status(conn)
        ownership = next(g for g in report.groups if g.key == "sec_ownership")
        assert ownership.backlog_pending == 1
        assert ownership.backlog_failed == 1
        assert ownership.state == "red"  # queue has failed rows
        assert report.queue_total == 1
        assert report.queue_failed == 1


# ---------------------------------------------------------------------------
# Recent failures
# ---------------------------------------------------------------------------


class TestRecentFailures:
    def test_failures_returned_newest_first_and_bounded_by_limit(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        now = datetime.now(tz=UTC)
        for i in range(5):
            _seed_run(
                conn,
                source="sec_edgar",
                started_at=now - timedelta(hours=i + 1),
                status="failed",
                error=f"err {i}",
            )
        conn.commit()
        failures = ingest_status.get_recent_failures(conn, limit=3)
        assert len(failures) == 3
        # Newest first.
        assert failures[0].error == "err 0"
        assert failures[2].error == "err 2"

    def test_only_recent_failures_returned(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Failures > 7 days old are excluded."""
        conn = ebull_test_conn
        _seed_run(
            conn,
            source="sec_edgar",
            started_at=datetime.now(tz=UTC) - timedelta(days=10),
            status="failed",
            error="ancient",
        )
        _seed_run(
            conn,
            source="sec_edgar",
            started_at=datetime.now(tz=UTC) - timedelta(days=1),
            status="failed",
            error="recent",
        )
        conn.commit()
        failures = ingest_status.get_recent_failures(conn, limit=10)
        assert [f.error for f in failures] == ["recent"]


# ---------------------------------------------------------------------------
# Enqueue helper
# ---------------------------------------------------------------------------


class TestEnqueueBackfill:
    def test_enqueue_inserts_pending_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=793_010, symbol="ENQ")
        conn.commit()
        ingest_status.enqueue_backfill(
            conn,
            instrument_id=793_010,
            pipeline_name="sec_edgar_form3",
            priority=10,
            triggered_by="operator",
        )
        conn.commit()
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT status, priority, triggered_by FROM ingest_backfill_queue "
                "WHERE instrument_id = %s AND pipeline_name = %s",
                (793_010, "sec_edgar_form3"),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["status"] == "pending"
        assert row["priority"] == 10
        assert row["triggered_by"] == "operator"

    def test_enqueue_idempotent_refreshes_existing_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Re-queueing a (instrument, pipeline) row should refresh
        priority + queued_at + clear last_error rather than insert a
        duplicate."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=793_011, symbol="IDEM")
        conn.commit()
        # Seed a stale failed row.
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingest_backfill_queue (
                    instrument_id, pipeline_name, priority, status,
                    last_error, attempts
                ) VALUES (%s, 'sec_edgar_13f', 50, 'failed', 'prior fail', 2)
                """,
                (793_011,),
            )
        conn.commit()
        ingest_status.enqueue_backfill(
            conn,
            instrument_id=793_011,
            pipeline_name="sec_edgar_13f",
            priority=10,
            triggered_by="operator",
        )
        conn.commit()
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT status, priority, last_error FROM ingest_backfill_queue "
                "WHERE instrument_id = %s AND pipeline_name = %s",
                (793_011, "sec_edgar_13f"),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["status"] == "pending"
        assert row["priority"] == 10
        assert row["last_error"] is None  # cleared on re-queue

    def test_enqueue_refresh_does_not_duplicate(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=793_012, symbol="DUP")
        conn.commit()
        for _ in range(3):
            ingest_status.enqueue_backfill(
                conn,
                instrument_id=793_012,
                pipeline_name="sec_edgar_form3",
            )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM ingest_backfill_queue "
                "WHERE instrument_id = %s AND pipeline_name = 'sec_edgar_form3'",
                (793_012,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1
