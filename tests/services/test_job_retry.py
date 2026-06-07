"""Job retry sweeper (#1509 / T3 of #1508) — re-fires due ``next_retry_at``.

DB-backed tests pin: due → audited manual-queue request + next_retry_at advanced
(not cleared); an async-rejected request is re-dispatched (retry not lost); not-due
skipped; in-flight request / running row defers; a superseded (newer terminal) row
is cleared, not dispatched; an ineligible job is cleared, never dispatched.

Spec: ``docs/specs/ops/2026-06-07-job-retry-backoff.md``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg

from app.services.job_retry import sweep_due_retries
from app.services.sync_orchestrator.dispatcher import publish_manual_job_request_with_conn
from app.workers.scheduler import JOB_CUSIP_EXTID_SWEEP

JOB = JOB_CUSIP_EXTID_SWEEP
ELIGIBLE = frozenset({JOB})
_NOW = datetime(2026, 6, 7, 12, 0, tzinfo=UTC)


def _seed_failure(
    conn: psycopg.Connection[tuple],
    *,
    job: str,
    next_retry_at: datetime | None,
    started_at: datetime,
    attempt: int = 1,
) -> int:
    row = conn.execute(
        """
        INSERT INTO job_runs (job_name, started_at, finished_at, status, attempt, next_retry_at)
        VALUES (%s, %s, %s, 'failure', %s, %s)
        RETURNING run_id
        """,
        (job, started_at, started_at, attempt, next_retry_at),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _next_retry_at(conn: psycopg.Connection[tuple], run_id: int) -> datetime | None:
    row = conn.execute("SELECT next_retry_at FROM job_runs WHERE run_id = %s", (run_id,)).fetchone()
    assert row is not None
    return row[0]


def _request_count(conn: psycopg.Connection[tuple], job: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM pending_job_requests WHERE job_name = %s AND request_kind = 'manual_job'",
        (job,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_due_retry_reenqueued_audited_and_advanced(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.autocommit = True
    run_id = _seed_failure(
        conn, job=JOB, next_retry_at=_NOW - timedelta(minutes=1), started_at=_NOW - timedelta(minutes=10), attempt=2
    )

    assert sweep_due_retries(conn, eligible_job_names=ELIGIBLE, now=_NOW) == [JOB]

    req = conn.execute(
        """
        SELECT requested_by, process_id, mode
          FROM pending_job_requests
         WHERE job_name = %s AND request_kind = 'manual_job'
        """,
        (JOB,),
    ).fetchall()
    assert req == [("system:retry_backoff", JOB, "iterate")]

    audit = conn.execute(
        """
        SELECT pass_fail, evidence_json->>'job_name', evidence_json->>'attempt'
          FROM decision_audit WHERE stage = 'retry_backoff'
        """
    ).fetchall()
    assert audit == [("RETRY", JOB, "2")]

    # Advanced, NOT cleared (Codex ckpt-2): the row stays a durable backstop in
    # case the queued request is rejected async with no new terminal run.
    advanced = _next_retry_at(conn, run_id)
    assert advanced is not None
    assert advanced > _NOW


def test_async_rejected_request_is_redispatched_not_lost(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """The Codex ckpt-2 case: the queued request is rejected AFTER commit (gate /
    prereq / fence) with no new terminal run. The retry must NOT be lost — once the
    recheck window elapses and the request is no longer active, it re-dispatches."""
    conn = ebull_test_conn
    conn.autocommit = True
    _seed_failure(conn, job=JOB, next_retry_at=_NOW - timedelta(minutes=1), started_at=_NOW - timedelta(minutes=10))

    assert sweep_due_retries(conn, eligible_job_names=ELIGIBLE, now=_NOW) == [JOB]
    # Simulate the listener rejecting the request asynchronously (terminal,
    # non-active) — no new job_runs terminal row was produced.
    conn.execute(
        "UPDATE pending_job_requests SET status = 'rejected' WHERE job_name = %s AND request_kind = 'manual_job'",
        (JOB,),
    )

    # After the recheck window the row is due again, no active request, still the
    # latest terminal → re-dispatched (retry preserved).
    later = _NOW + timedelta(seconds=901)
    assert sweep_due_retries(conn, eligible_job_names=ELIGIBLE, now=later) == [JOB]
    assert _request_count(conn, JOB) == 2  # original + re-dispatch


def test_not_yet_due_is_skipped(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.autocommit = True
    _seed_failure(conn, job=JOB, next_retry_at=_NOW + timedelta(minutes=5), started_at=_NOW)
    assert sweep_due_retries(conn, eligible_job_names=ELIGIBLE, now=_NOW) == []
    assert _request_count(conn, JOB) == 0


def test_active_request_defers_without_clearing(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.autocommit = True
    run_id = _seed_failure(
        conn, job=JOB, next_retry_at=_NOW - timedelta(minutes=1), started_at=_NOW - timedelta(minutes=5)
    )
    publish_manual_job_request_with_conn(conn, JOB, requested_by="operator-test")

    assert sweep_due_retries(conn, eligible_job_names=ELIGIBLE, now=_NOW) == []
    # Deferred WITHOUT clearing — if the in-flight attempt fails, the retry
    # is not lost.
    assert _next_retry_at(conn, run_id) is not None
    assert _request_count(conn, JOB) == 1  # no duplicate


def test_running_row_defers(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.autocommit = True
    run_id = _seed_failure(
        conn, job=JOB, next_retry_at=_NOW - timedelta(minutes=1), started_at=_NOW - timedelta(minutes=20)
    )
    conn.execute("INSERT INTO job_runs (job_name, started_at, status) VALUES (%s, %s, 'running')", (JOB, _NOW))

    assert sweep_due_retries(conn, eligible_job_names=ELIGIBLE, now=_NOW) == []
    assert _next_retry_at(conn, run_id) is not None
    assert _request_count(conn, JOB) == 0


def test_superseded_by_newer_terminal_is_cleared(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.autocommit = True
    stale = _seed_failure(
        conn, job=JOB, next_retry_at=_NOW - timedelta(minutes=2), started_at=_NOW - timedelta(minutes=30)
    )
    # A newer SUCCESS terminal makes the old failure no longer latest.
    conn.execute(
        "INSERT INTO job_runs (job_name, started_at, finished_at, status) VALUES (%s, %s, %s, 'success')",
        (JOB, _NOW - timedelta(minutes=1), _NOW - timedelta(minutes=1)),
    )

    assert sweep_due_retries(conn, eligible_job_names=ELIGIBLE, now=_NOW) == []
    assert _next_retry_at(conn, stale) is None  # cleared, not re-fired
    assert _request_count(conn, JOB) == 0


def test_ineligible_job_cleared_not_dispatched(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.autocommit = True
    run_id = _seed_failure(
        conn,
        job="orphan_unregistered_job",
        next_retry_at=_NOW - timedelta(minutes=1),
        started_at=_NOW - timedelta(minutes=5),
    )

    assert sweep_due_retries(conn, eligible_job_names=ELIGIBLE, now=_NOW) == []
    assert _next_retry_at(conn, run_id) is None  # cleared so it is not re-swept
    assert _request_count(conn, "orphan_unregistered_job") == 0
