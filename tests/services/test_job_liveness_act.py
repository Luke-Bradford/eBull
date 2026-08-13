"""Liveness watchdog actuator (#1510 / T4 of epic #1508).

DB-backed tests pin: a stalled job → one audited manual-queue kick
(``requested_by='system:liveness_kick'`` + ``decision_audit`` row); an in-flight
request / running row defers (no double-dispatch); a natural fire between detect
and act drops the candidate (in-tx stall recheck); a recent kick within the
cooldown surfaces ``blocked`` instead of re-storming; an ineligible job is never
dispatched.

Spec: ``docs/specs/ops/2026-06-07-liveness-watchdog-acts.md``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
from psycopg.types.json import Jsonb

from app.services.job_liveness import StalledJob
from app.services.job_liveness_act import act_on_stalled_jobs
from app.services.sync_orchestrator.dispatcher import publish_manual_job_request_with_conn
from app.workers.scheduler import JOB_CUSIP_EXTID_SWEEP, Cadence

JOB = JOB_CUSIP_EXTID_SWEEP
CADENCE = Cadence.daily(hour=3, minute=0)
ELIGIBLE = {JOB: CADENCE}
_NOW = datetime(2026, 6, 7, 12, 0, tzinfo=UTC)


def _stalled(job: str = JOB) -> StalledJob:
    return StalledJob(job_name=job, window_seconds=3 * 86400.0, last_fire_at=_NOW - timedelta(days=10))


def _seed_old_terminal(conn: psycopg.Connection[tuple], *, job: str = JOB, age_days: int = 10) -> None:
    """A lifetime terminal row OLD enough to sit outside the K=3 daily window —
    so find_stalled_jobs sees lifetime>=1, recent==0, not running ⇒ stalled."""
    at = _NOW - timedelta(days=age_days)
    conn.execute(
        "INSERT INTO job_runs (job_name, started_at, finished_at, status) VALUES (%s, %s, %s, 'success')",
        (job, at, at),
    )


def _request_count(conn: psycopg.Connection[tuple], job: str = JOB) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM pending_job_requests WHERE job_name = %s AND request_kind = 'manual_job'",
        (job,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _seed_kick_audit(conn: psycopg.Connection[tuple], *, job: str = JOB, when: datetime) -> None:
    conn.execute(
        """
        INSERT INTO decision_audit (decision_time, stage, pass_fail, explanation, evidence_json)
        VALUES (%s, 'liveness_kick', 'KICK', 'seeded', %s)
        """,
        (when, Jsonb({"job_name": job})),
    )


def test_stalled_job_kicked_audited(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.autocommit = True
    _seed_old_terminal(conn)

    result = act_on_stalled_jobs(conn, stalled=[_stalled()], eligible=ELIGIBLE, now=_NOW)
    assert result.kicked == [JOB]
    assert result.blocked == []

    req = conn.execute(
        "SELECT requested_by, process_id, mode FROM pending_job_requests "
        "WHERE job_name = %s AND request_kind = 'manual_job'",
        (JOB,),
    ).fetchall()
    assert req == [("system:liveness_kick", JOB, "iterate")]

    audit = conn.execute(
        "SELECT pass_fail, evidence_json->>'job_name' FROM decision_audit WHERE stage = 'liveness_kick'"
    ).fetchall()
    assert audit == [("KICK", JOB)]


def test_in_flight_request_defers(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.autocommit = True
    _seed_old_terminal(conn)
    publish_manual_job_request_with_conn(conn, JOB, requested_by="operator-test")

    result = act_on_stalled_jobs(conn, stalled=[_stalled()], eligible=ELIGIBLE, now=_NOW)
    assert result.kicked == []
    assert _request_count(conn) == 1  # no duplicate


def test_running_row_defers(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.autocommit = True
    _seed_old_terminal(conn)
    conn.execute("INSERT INTO job_runs (job_name, started_at, status) VALUES (%s, %s, 'running')", (JOB, _NOW))

    result = act_on_stalled_jobs(conn, stalled=[_stalled()], eligible=ELIGIBLE, now=_NOW)
    # A live running row means the job is NOT actually stalled (the in-tx recheck
    # sees has_active_running) — defer, never kick.
    assert result.kicked == []
    assert _request_count(conn) == 0


def test_recent_fire_drops_candidate(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Detect→act race (Codex ckpt-1 #3): a natural fire landed in the gap, so the
    in-tx recheck finds a recent row and the stale kick is not dispatched."""
    conn = ebull_test_conn
    conn.autocommit = True
    _seed_old_terminal(conn)
    # A fresh terminal inside the K-window — the job fired again after detection.
    fresh = _NOW - timedelta(hours=1)
    conn.execute(
        "INSERT INTO job_runs (job_name, started_at, finished_at, status) VALUES (%s, %s, %s, 'success')",
        (JOB, fresh, fresh),
    )

    result = act_on_stalled_jobs(conn, stalled=[_stalled()], eligible=ELIGIBLE, now=_NOW)
    assert result.kicked == []
    assert _request_count(conn) == 0


def test_cooldown_blocks_second_kick(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """A prior liveness_kick within max(cadence, 6h) means the kick did not take
    (gate-rejected / dead scheduler) → blocked, not re-stormed."""
    conn = ebull_test_conn
    conn.autocommit = True
    _seed_old_terminal(conn)
    _seed_kick_audit(conn, when=_NOW - timedelta(hours=1))  # within the 24h daily cooldown

    result = act_on_stalled_jobs(conn, stalled=[_stalled()], eligible=ELIGIBLE, now=_NOW)
    assert result.kicked == []
    assert result.blocked == [JOB]
    assert _request_count(conn) == 0  # no new request


def test_kick_outside_cooldown_redispatches(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.autocommit = True
    _seed_old_terminal(conn)
    _seed_kick_audit(conn, when=_NOW - timedelta(days=2))  # older than the 24h daily cooldown

    result = act_on_stalled_jobs(conn, stalled=[_stalled()], eligible=ELIGIBLE, now=_NOW)
    assert result.kicked == [JOB]
    assert _request_count(conn) == 1


def test_ineligible_job_never_dispatched(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.autocommit = True
    _seed_old_terminal(conn, job="some_orchestrator_job")

    result = act_on_stalled_jobs(conn, stalled=[_stalled("some_orchestrator_job")], eligible=ELIGIBLE, now=_NOW)
    assert result.kicked == []
    assert result.blocked == []
    assert _request_count(conn, "some_orchestrator_job") == 0
