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
from psycopg.types.json import Jsonb

from app.services.job_retry import sweep_due_retries
from app.services.ops_monitor import LANE_BUSY_SKIP_PREFIX, MISFIRE_SKIP_PREFIX, RETRY_MAX_ATTEMPTS
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


# ---------------------------------------------------------------------------
# #2603 — a fire LOST to a busy lane is re-armed on a ``skipped`` row
# ---------------------------------------------------------------------------


def _seed_skip(
    conn: psycopg.Connection[tuple],
    *,
    job: str,
    reason: str,
    next_retry_at: datetime | None,
    started_at: datetime,
    attempt: int = 1,
) -> int:
    row = conn.execute(
        """
        INSERT INTO job_runs (job_name, started_at, finished_at, status, error_msg, attempt, next_retry_at)
        VALUES (%s, %s, %s, 'skipped', %s, %s, %s)
        RETURNING run_id
        """,
        (job, started_at, started_at, reason, attempt, next_retry_at),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _attempt(conn: psycopg.Connection[tuple], run_id: int) -> int:
    row = conn.execute("SELECT attempt FROM job_runs WHERE run_id = %s", (run_id,)).fetchone()
    assert row is not None
    return int(row[0])


def test_armed_lane_busy_skip_is_redispatched(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """The #2603 case: a ``skipped`` row carrying ``next_retry_at`` re-fires.

    Before this change both ``_select_due`` and ``_refire_one`` filtered
    ``status = 'failure'``, so an armed skip was invisible to the sweeper and
    the lost fire was simply gone until the next cadence.
    """
    conn = ebull_test_conn
    conn.autocommit = True
    run_id = _seed_skip(
        conn,
        job=JOB,
        reason=LANE_BUSY_SKIP_PREFIX + "lane stayed busy through the retry window",
        next_retry_at=_NOW - timedelta(minutes=1),
        started_at=_NOW - timedelta(minutes=10),
    )

    assert sweep_due_retries(conn, eligible_job_names=ELIGIBLE, now=_NOW) == [JOB]
    assert _request_count(conn, JOB) == 1

    advanced = _next_retry_at(conn, run_id)
    assert advanced is not None and advanced > _NOW


def test_lane_busy_audit_does_not_claim_a_transient_failure(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The audit line names the real cause, and the evidence carries the status.

    It previously asserted "after a transient failure" unconditionally, which is
    false for a re-armed skip — an audit that names the wrong cause is worse
    than a vague one.
    """
    conn = ebull_test_conn
    conn.autocommit = True
    _seed_skip(
        conn,
        job=JOB,
        reason=LANE_BUSY_SKIP_PREFIX + "lane stayed busy",
        next_retry_at=_NOW - timedelta(minutes=1),
        started_at=_NOW - timedelta(minutes=10),
    )

    assert sweep_due_retries(conn, eligible_job_names=ELIGIBLE, now=_NOW) == [JOB]

    row = conn.execute(
        """
        SELECT explanation, evidence_json->>'source_status'
          FROM decision_audit WHERE stage = 'retry_backoff'
        """
    ).fetchone()
    assert row is not None
    explanation, source_status = row
    assert "transient failure" not in explanation
    assert "lost fire (lane busy)" in explanation
    assert source_status == "skipped"


def test_misfire_audit_is_not_captioned_as_a_lane_collision(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ Codex ckpt-3 on PR #3219: the cause was hard-coded, not read.

    ``cause`` was ``"a transient failure" if status == "failure" else "a lost
    fire (lane busy)"``, so admitting misfires to the re-arm path (#2603) would
    have captioned every one of them as a lane collision it had nothing to do
    with. Same defect shape as the sentence it replaced — a fixed string that
    was true for the only case that existed when it was written.
    """
    conn = ebull_test_conn
    conn.autocommit = True
    _seed_skip(
        conn,
        job=JOB,
        reason=MISFIRE_SKIP_PREFIX + "fire due ...; worker reached it 180.8s late, past misfire_grace_time",
        next_retry_at=_NOW - timedelta(minutes=1),
        started_at=_NOW - timedelta(minutes=10),
    )

    assert sweep_due_retries(conn, eligible_job_names=ELIGIBLE, now=_NOW) == [JOB]

    row = conn.execute("SELECT explanation FROM decision_audit WHERE stage = 'retry_backoff'").fetchone()
    assert row is not None
    assert "lost fire (misfire)" in row[0]
    assert "lane busy" not in row[0]
    assert "transient failure" not in row[0]


def test_an_unrecognised_skip_reason_degrades_to_the_vague_cause(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Unknown prefix ⇒ "a lost fire", never a guess at which kind."""
    conn = ebull_test_conn
    conn.autocommit = True
    _seed_skip(
        conn,
        job=JOB,
        reason="something_new: a reason class that does not exist yet",
        next_retry_at=_NOW - timedelta(minutes=1),
        started_at=_NOW - timedelta(minutes=10),
    )

    assert sweep_due_retries(conn, eligible_job_names=ELIGIBLE, now=_NOW) == [JOB]

    row = conn.execute("SELECT explanation FROM decision_audit WHERE stage = 'retry_backoff'").fetchone()
    assert row is not None
    assert "after a lost fire (run" in row[0]
    assert "misfire" not in row[0]
    assert "lane busy" not in row[0]
    assert "transient failure" not in row[0]


def test_unarmed_skip_is_never_swept(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """A ``prereq_missing`` skip carries no ``next_retry_at`` and must stay inert.

    Arming happens only at the lane-busy emission site, so this is the
    structural guarantee that a legitimate no-op skip never re-fires.
    """
    conn = ebull_test_conn
    conn.autocommit = True
    run_id = _seed_skip(
        conn,
        job=JOB,
        reason="prereq_missing: nothing to do",
        next_retry_at=None,
        started_at=_NOW - timedelta(minutes=10),
    )

    assert sweep_due_retries(conn, eligible_job_names=ELIGIBLE, now=_NOW) == []
    assert _next_retry_at(conn, run_id) is None
    assert _request_count(conn, JOB) == 0


def test_redispatch_is_bounded_by_the_attempt_cap(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """⚠⚠ The pre-existing unbounded-redispatch defect, fixed in #2603.

    ``_refire_one`` read ``attempt`` only for the audit string: it never checked
    the cap and never incremented, so bounding came entirely from
    ``record_job_finish`` — which runs only on a new terminal FAILURE. A row
    whose request is rejected asynchronously produces no new terminal, so it
    re-dispatched every recheck window forever. Here the request is rejected
    each round, so nothing but the cap can stop it.
    """
    conn = ebull_test_conn
    conn.autocommit = True
    run_id = _seed_failure(
        conn,
        job=JOB,
        next_retry_at=_NOW - timedelta(minutes=1),
        started_at=_NOW - timedelta(minutes=10),
        attempt=1,
    )

    now = _NOW
    dispatches = 0
    for _ in range(RETRY_MAX_ATTEMPTS + 3):
        if sweep_due_retries(conn, eligible_job_names=ELIGIBLE, now=now) == [JOB]:
            dispatches += 1
        # Async rejection: no new terminal row, so only the cap can terminate.
        conn.execute(
            "UPDATE pending_job_requests SET status = 'rejected' WHERE job_name = %s AND request_kind = 'manual_job'",
            (JOB,),
        )
        now = now + timedelta(seconds=901)

    assert dispatches == RETRY_MAX_ATTEMPTS
    assert _next_retry_at(conn, run_id) is None  # cleared once exhausted
    # ⚠ The streak counter is NOT the dispatch counter. ``attempt`` means this
    # run's position in the consecutive-failure streak and is rendered to the
    # operator as "attempt N", so re-dispatching must leave it exactly as the
    # failing run recorded it. The dispatch count lives in ``decision_audit``.
    assert _attempt(conn, run_id) == 1


def test_dispatch_cap_counts_legacy_audit_rows_too(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Pre-#2603 dispatches recorded ``failed_run_id``, not ``source_run_id``.

    Counting only the new key would restart the cap from zero for any row that
    already carried history, handing it a full extra set of dispatches. Found
    by Codex ckpt-3; latent rather than live (no armed run on dev currently has
    legacy audit rows), which is exactly why it needs a test.
    """
    conn = ebull_test_conn
    conn.autocommit = True
    run_id = _seed_failure(
        conn,
        job=JOB,
        next_retry_at=_NOW - timedelta(minutes=1),
        started_at=_NOW - timedelta(minutes=10),
    )
    # Exhaust the cap entirely with OLD-key audit rows.
    for _ in range(RETRY_MAX_ATTEMPTS):
        conn.execute(
            """
            INSERT INTO decision_audit (decision_time, stage, pass_fail, explanation, evidence_json)
            VALUES (NOW(), 'retry_backoff', 'RETRY', 'legacy', %s)
            """,
            (Jsonb({"job_name": JOB, "attempt": 1, "failed_run_id": run_id}),),
        )

    assert sweep_due_retries(conn, eligible_job_names=ELIGIBLE, now=_NOW) == []
    assert _next_retry_at(conn, run_id) is None  # exhausted → cleared
    assert _request_count(conn, JOB) == 0  # and never dispatched
