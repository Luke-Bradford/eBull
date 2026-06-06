"""Job-level retry sweeper (#1509 / T3 of epic #1508).

Re-fires transiently-failed scheduled jobs whose ``job_runs.next_retry_at``
is due, through the **audited manual-queue path** (the same mechanism
``post_bootstrap_activation`` uses). ``record_job_finish``
(``app/services/ops_monitor.py``) sets ``next_retry_at`` on a failed row when
the failure is transient (``REMEDIES[category].self_heal``) and attempts are
not exhausted; this module turns that timestamp into an actual re-dispatch.

Spec: ``docs/specs/ops/2026-06-07-job-retry-backoff.md``.

Design constraints (Codex ckpt-1 + ckpt-2):

  * **Advance, never clear, on dispatch.** The manual queue can reject a
    request ASYNCHRONOUSLY after its INSERT commits (bootstrap gate, per-job
    prerequisite, full-wash fence). A committed clear would lose the retry —
    there is no new terminal run to restamp it. Instead the failed row's
    ``next_retry_at`` is pushed forward by ``_DISPATCH_RECHECK_SECONDS``: a
    genuine new run supersedes it (latest-terminal check clears the stale
    row), an async rejection is simply re-dispatched once the window elapses.
  * **Atomic recheck+publish+advance.** Each candidate's FOR UPDATE recheck,
    in-flight guard, publish, audit, and reschedule share ONE
    ``conn.transaction()`` so a *synchronous* publish failure (e.g. a
    full-wash fence ``UniqueViolation``) rolls everything back and the row is
    retried next sweep, unchanged.
  * **In-flight request is the dedup.** A live ``pending_job_requests`` row
    (or a ``running`` ``job_runs`` row) makes the candidate defer, so two
    sweeps — or a natural cadence fire — can never double-dispatch.
  * **Eligibility-bounded.** Only jobs in the caller-supplied registry set
    (``SCHEDULED_JOBS`` minus the sync-runs-tracked orchestrator jobs) are
    ever dispatched; a stray ``next_retry_at`` on any other name is cleared,
    never re-fired.
  * **#1484 caveat** (never retry into a held rate-limit) is handled upstream
    by the longer ``RATE_LIMITED`` backoff base in ``record_job_finish`` —
    the window has passed by the time the row is due here.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)

_REQUESTED_BY = "system:retry_backoff"

# After a dispatch we do NOT clear ``next_retry_at`` — we push it forward by this
# window. Rationale (Codex ckpt-2 HIGH): the manual queue can reject a request
# ASYNCHRONOUSLY *after* its INSERT has committed (bootstrap gate, per-job
# prerequisite, full-wash fence — see app/jobs/listener.py / runtime.py). A
# committed clear would then lose the retry, because no new terminal run exists
# to restamp it. Advancing keeps the failed row as its own durable backstop: a
# genuine new run supersedes it (latest-terminal branch clears the stale row),
# while an async rejection (no new terminal) is simply re-dispatched once this
# window elapses — no loss, and bounded to one request per window (not per sweep).
_DISPATCH_RECHECK_SECONDS: int = 900  # 15m


def sweep_due_retries(
    conn: psycopg.Connection[Any],
    *,
    eligible_job_names: frozenset[str],
    now: datetime,
) -> list[str]:
    """Re-enqueue every job with a due ``next_retry_at``. Returns names re-fired.

    ``conn`` MUST be autocommit so each candidate's ``conn.transaction()``
    issues a real ``BEGIN``/``COMMIT`` (the post_bootstrap_activation
    contract), not a savepoint.
    """
    assert conn.autocommit, "sweep_due_retries requires an autocommit connection"
    refired: list[str] = []
    for run_id, job_name, attempt in _select_due(conn, now=now):
        if job_name not in eligible_job_names:
            # Stray next_retry_at on an unregistered / sync-runs-tracked job —
            # should not happen (those never flow through record_job_finish),
            # but clear it so it is not re-selected forever. Never dispatch.
            _clear(conn, run_id)
            logger.debug("jobs_retry_sweeper: cleared stray next_retry_at on %r (run %s)", job_name, run_id)
            continue
        try:
            if _refire_one(conn, run_id=run_id, job_name=job_name, attempt=attempt, now=now):
                refired.append(job_name)
        except Exception:
            logger.exception(
                "jobs_retry_sweeper: re-enqueue failed for %r (run %s) — leaving next_retry_at set for next sweep",
                job_name,
                run_id,
            )
    if refired:
        logger.info(
            "jobs_retry_sweeper: re-enqueued %d job(s) via audited manual queue: %s",
            len(refired),
            ", ".join(refired),
        )
    return refired


def _select_due(conn: psycopg.Connection[Any], *, now: datetime) -> list[tuple[int, str, int]]:
    """Due retry rows off the partial ``job_runs_due_retry_idx`` index.

    ``next_retry_at`` is only ever set on a ``status='failure'`` row, so the
    status predicate is a cheap guard, not the access path. Deterministic
    order so the latest failure per job is handled first.
    """
    rows = conn.execute(
        """
        SELECT run_id, job_name, attempt
          FROM job_runs
         WHERE next_retry_at IS NOT NULL
           AND next_retry_at <= %(now)s
           AND status = 'failure'
         ORDER BY job_name, started_at DESC, run_id DESC
        """,
        {"now": now},
    ).fetchall()
    return [(int(r[0]), str(r[1]), int(r[2])) for r in rows]


def _refire_one(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
    job_name: str,
    attempt: int,
    now: datetime,
) -> bool:
    """Recheck + re-enqueue one due candidate atomically. Returns True if fired."""
    from app.services.sync_orchestrator.dispatcher import publish_manual_job_request_with_conn

    with conn.transaction():
        locked = conn.execute(
            "SELECT next_retry_at, status FROM job_runs WHERE run_id = %(id)s FOR UPDATE",
            {"id": run_id},
        ).fetchone()
        # Superseded between SELECT-due and lock (a concurrent sweep cleared it,
        # or the status changed): nothing to do.
        if locked is None or locked[0] is None or locked[1] != "failure" or locked[0] > now:
            return False
        # A newer terminal run exists ⇒ this failure is stale; clear + skip.
        if not _is_latest_terminal(conn, job_name=job_name, run_id=run_id):
            _clear(conn, run_id)
            return False
        # Already recovering — a live run or queued request will produce a fresh
        # terminal that supersedes this row. Defer WITHOUT clearing so the retry
        # is not lost if that in-flight attempt itself fails.
        if _has_running_run(conn, job_name) or _has_active_request(conn, job_name):
            return False
        publish_manual_job_request_with_conn(
            conn,
            job_name,
            requested_by=_REQUESTED_BY,
            process_id=job_name,
            mode="iterate",
        )
        _write_retry_audit(conn, job_name=job_name, attempt=attempt, run_id=run_id)
        # Advance, do NOT clear: the request may still be rejected async after
        # this commit (gate/prereq/fence) with no new terminal to restamp the
        # retry. Pushing next_retry_at forward keeps the row a durable backstop
        # while the in-flight request defers the next few sweeps; a real run
        # supersedes it via the latest-terminal check.
        _reschedule(conn, run_id, now + timedelta(seconds=_DISPATCH_RECHECK_SECONDS))
        return True


def _is_latest_terminal(conn: psycopg.Connection[Any], *, job_name: str, run_id: int) -> bool:
    row = conn.execute(
        """
        SELECT run_id FROM job_runs
         WHERE job_name = %(job)s
           AND status IN ('success', 'failure', 'skipped', 'cancelled')
         ORDER BY started_at DESC, run_id DESC
         LIMIT 1
        """,
        {"job": job_name},
    ).fetchone()
    return row is not None and int(row[0]) == run_id


def _has_running_run(conn: psycopg.Connection[Any], job_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM job_runs WHERE job_name = %(job)s AND status = 'running' LIMIT 1",
        {"job": job_name},
    ).fetchone()
    return row is not None


def _has_active_request(conn: psycopg.Connection[Any], job_name: str) -> bool:
    """True when a manual_job request for ``job_name`` is already in flight."""
    row = conn.execute(
        """
        SELECT 1
          FROM pending_job_requests
         WHERE job_name = %(job_name)s
           AND request_kind = 'manual_job'
           AND status IN ('pending', 'claimed', 'dispatched')
         LIMIT 1
        """,
        {"job_name": job_name},
    ).fetchone()
    return row is not None


def _clear(conn: psycopg.Connection[Any], run_id: int) -> None:
    conn.execute(
        "UPDATE job_runs SET next_retry_at = NULL WHERE run_id = %(id)s",
        {"id": run_id},
    )


def _reschedule(conn: psycopg.Connection[Any], run_id: int, next_at: datetime) -> None:
    conn.execute(
        "UPDATE job_runs SET next_retry_at = %(next)s WHERE run_id = %(id)s",
        {"next": next_at, "id": run_id},
    )


def _write_retry_audit(
    conn: psycopg.Connection[Any],
    *,
    job_name: str,
    attempt: int,
    run_id: int,
) -> None:
    """Record the audited retry in ``decision_audit`` (mirrors the kick audit)."""
    explanation = (
        f"retry/backoff: re-enqueued job {job_name!r} (attempt {attempt}) after a transient failure (run {run_id})"
    )
    conn.execute(
        """
        INSERT INTO decision_audit
            (decision_time, stage, pass_fail, explanation, evidence_json)
        VALUES
            (NOW(), 'retry_backoff', 'RETRY', %(expl)s, %(evidence)s)
        """,
        {
            "expl": explanation,
            "evidence": Jsonb({"job_name": job_name, "attempt": attempt, "failed_run_id": run_id}),
        },
    )


__all__ = ["sweep_due_retries"]
