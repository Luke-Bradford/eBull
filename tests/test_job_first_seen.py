"""#1508 C6 — never-started bound on the persisted ``job_first_seen`` anchor.

DB-backed against the worker ``ebull_test`` template (migration 185 creates
``job_first_seen``; the template is rebuilt on migration-hash change).

Proves the persisted anchor drives the ``never_started`` verdict signal:

* a never-run daily job whose persisted first-seen is more than one cadence +
  grace in the past reads ``never_started=True`` (broken-from-day-one), and
* a recent first-seen (or no anchor row at all) reads ``never_started=False``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from app.services.processes import scheduled_adapter
from app.services.processes.scheduled_adapter import _job_first_seen
from app.workers.scheduler import JOB_RAW_DATA_RETENTION_SWEEP

pytestmark = pytest.mark.db


def _ensure_kill_switch_off(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        INSERT INTO kill_switch (id, is_active, activated_at, activated_by, reason)
        VALUES (TRUE, FALSE, NULL, NULL, NULL)
        ON CONFLICT (id) DO UPDATE
        SET is_active = FALSE, activated_at = NULL, activated_by = NULL, reason = NULL
        """
    )


def _set_first_seen(conn: psycopg.Connection[tuple], *, job_name: str, first_seen: datetime) -> None:
    conn.execute(
        """
        INSERT INTO job_first_seen (job_name, first_seen) VALUES (%(n)s, %(t)s)
        ON CONFLICT (job_name) DO UPDATE SET first_seen = EXCLUDED.first_seen
        """,
        {"n": job_name, "t": first_seen},
    )


def test_job_first_seen_helper_round_trips(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """The probe returns the persisted timestamp, and None when absent."""
    assert _job_first_seen(ebull_test_conn, job_name=JOB_RAW_DATA_RETENTION_SWEEP) is None
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    _set_first_seen(ebull_test_conn, job_name=JOB_RAW_DATA_RETENTION_SWEEP, first_seen=anchor)
    ebull_test_conn.commit()
    assert _job_first_seen(ebull_test_conn, job_name=JOB_RAW_DATA_RETENTION_SWEEP) == anchor


def test_old_first_seen_never_run_reads_never_started(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Never-run daily job, anchor well over a day in the past → never_started."""
    _ensure_kill_switch_off(ebull_test_conn)
    _set_first_seen(
        ebull_test_conn,
        job_name=JOB_RAW_DATA_RETENTION_SWEEP,
        first_seen=datetime.now(UTC) - timedelta(days=7),
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RAW_DATA_RETENTION_SWEEP)
    assert row is not None
    assert row.status == "pending_first_run"  # genuinely never ran
    assert row.never_started is True


def test_recent_first_seen_never_run_is_not_never_started(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Anchor inside the first cadence + grace window stays awaiting-first-slot."""
    _ensure_kill_switch_off(ebull_test_conn)
    _set_first_seen(
        ebull_test_conn,
        job_name=JOB_RAW_DATA_RETENTION_SWEEP,
        first_seen=datetime.now(UTC) - timedelta(minutes=1),
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RAW_DATA_RETENTION_SWEEP)
    assert row is not None
    assert row.status == "pending_first_run"
    assert row.never_started is False


def test_no_anchor_row_is_not_never_started(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """A job seen for the first time this boot has no persisted anchor → False."""
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RAW_DATA_RETENTION_SWEEP)
    assert row is not None
    assert row.never_started is False
