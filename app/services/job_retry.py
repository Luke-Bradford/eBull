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
from typing import Any, Final

import psycopg
from psycopg.types.json import Jsonb

from app.services.ops_monitor import RETRY_MAX_ATTEMPTS, TERMINAL_STATUS_SQL

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

#: Terminal statuses a ``next_retry_at`` may legitimately sit on.
#:
#: ``failure`` is the original (#1509). ``skipped`` was added by #2603 for a
#: fire lost to a busy lane; arming happens only at that one emission site and
#: only for a job carrying ``rearm_on_lost_fire``, so no other skip reason —
#: ``prereq_missing``, a session-window guard, "no pending X" — can ever reach
#: this set. Rendered as a SQL array literal so the tuple here is the single
#: source of truth for both the query and the ``_refire_one`` recheck.
_REARMABLE_STATUSES: Final[tuple[str, ...]] = ("failure", "skipped")
_REARMABLE_STATUS_SQL: Final[str] = "ARRAY['" + "','".join(_REARMABLE_STATUSES) + "']"


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

    ⚠ This docstring used to read *"``next_retry_at`` is only ever set on a
    ``status='failure'`` row"*. That stopped being true in #2603: a ``skipped``
    row that lost its fire needs re-dispatching for exactly the same reason a
    transient failure does — work was due and did not happen. Two writers stamp
    one, both gated on the same per-job ``rearm_on_lost_fire`` flag:

    * ``app.jobs.runtime._record_lane_busy_skip`` — the lane was held;
    * ``app.jobs.runtime.JobRuntime._arm_missed_fire`` — APScheduler discarded
      the fire past ``misfire_grace_time``.

    ⚠ The misfire row's ``started_at`` is BACKDATED to the slot it lost, so
    ``_is_latest_terminal`` below can clear it before it ever dispatches if any
    newer terminal row has appeared. For a newer SUCCESS that is correct — the
    slot has been superseded. For a newer non-work ``skipped`` row it is a
    silent drop, which degrades to the pre-#2603 behaviour (no recovery at all)
    and is therefore a safe floor rather than a regression.

    The status predicate stays a cheap guard rather than the access path: the
    index (``sql/183``) is ``ON job_runs (next_retry_at) WHERE next_retry_at IS
    NOT NULL`` with no status column, so widening the set costs no migration
    and no plan change. Deterministic order so the latest row per job is
    handled first.
    """
    rows = conn.execute(
        f"""
        SELECT run_id, job_name, attempt
          FROM job_runs
         WHERE next_retry_at IS NOT NULL
           AND next_retry_at <= %(now)s
           AND status = ANY({_REARMABLE_STATUS_SQL})
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
        if locked is None or locked[0] is None or locked[1] not in _REARMABLE_STATUSES or locked[0] > now:
            return False
        # ⚠⚠ #2603 — bound the re-dispatch HERE, because nothing else does.
        # This function never checked ``RETRY_MAX_ATTEMPTS``: the cap was
        # enforced entirely by ``record_job_finish``, which runs only when a new
        # terminal FAILURE is recorded. A row whose request is rejected
        # asynchronously (bootstrap gate, per-job prerequisite, full-wash fence)
        # produces no new terminal at all, so it was re-dispatched every
        # ``_DISPATCH_RECHECK_SECONDS`` forever. Reachable before #2603 and more
        # often after it, so it is fixed rather than inherited.
        #
        # ⚠ Counted from ``decision_audit``, NOT from ``job_runs.attempt``.
        # ``attempt`` is the consecutive-failure streak position and is rendered
        # to the operator as "attempt N"; overwriting it with a dispatch count
        # would corrupt a different, published quantity.
        if _dispatch_count(conn, run_id) >= RETRY_MAX_ATTEMPTS:
            _clear(conn, run_id)
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
        _write_retry_audit(conn, job_name=job_name, attempt=attempt, run_id=run_id, status=str(locked[1]))
        # Advance, do NOT clear: the request may still be rejected async after
        # this commit (gate/prereq/fence) with no new terminal to restamp the
        # retry. Pushing next_retry_at forward keeps the row a durable backstop
        # while the in-flight request defers the next few sweeps; a real run
        # supersedes it via the latest-terminal check.
        _reschedule(conn, run_id, now + timedelta(seconds=_DISPATCH_RECHECK_SECONDS))
        return True


def _is_latest_terminal(conn: psycopg.Connection[Any], *, job_name: str, run_id: int) -> bool:
    row = conn.execute(
        f"""
        SELECT run_id FROM job_runs
         WHERE job_name = %(job)s
           AND status IN {TERMINAL_STATUS_SQL}
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
    """Push this row's next dispatch out.

    ⚠ Deliberately does NOT touch ``attempt``. That column means "this run's
    position in the consecutive-failure streak" (``ops_monitor._retry_plan``)
    and is operator-visible — ``app/api/processes.py`` renders it as
    "attempt N" on a retrying row. Incrementing it per dispatch would make the
    first failed run read attempt 2 before any second run existed. The dispatch
    count lives in ``decision_audit`` instead; see ``_dispatch_count``.
    """
    conn.execute(
        "UPDATE job_runs SET next_retry_at = %(next)s WHERE run_id = %(id)s",
        {"next": next_at, "id": run_id},
    )


def _dispatch_count(conn: psycopg.Connection[Any], run_id: int) -> int:
    """How many times this row has already been re-dispatched.

    ⚠ #2603 — read from ``decision_audit`` rather than stored on ``job_runs``,
    because every dispatch already writes exactly one audit row
    (``_write_retry_audit``): the audit IS the dispatch record, so counting it
    needs no new column. The alternative — bumping ``job_runs.attempt`` — would
    have corrupted an operator-visible failure-streak counter to store a
    different quantity.

    ⚠⚠ The count matches what was actually dispatched only because the publish
    and this counter's audit INSERT happen inside the SAME
    ``conn.transaction()`` in ``_refire_one``. A crash between them rolls back
    both, so there is no state where a dispatch is visible but uncounted. If
    the audit write is ever moved out of that transaction — or made
    best-effort — the cap silently stops bounding anything, which is the defect
    this function exists to fix.

    ⚠ That atomicity is the only guarantee claimed here. It says nothing about
    RETENTION: nothing prunes ``decision_audit`` today, but an archival job
    added later would lower the count and hand a row fresh dispatches. Any such
    job must either exclude ``stage='retry_backoff'`` rows whose source run is
    still armed, or this cap needs its own column.

    ⚠ BOTH evidence keys are matched. Dispatches before this change recorded
    ``failed_run_id``; the key became ``source_run_id`` when a re-armed skip
    made "failed" untrue. Matching only the new key would restart the count
    from zero for any row carrying pre-change history (167 such audit rows
    exist on dev; 0 of them currently sit under an armed run, so this is a
    latent gap being closed rather than a live one).
    """
    row = conn.execute(
        """
        SELECT COUNT(*) FROM decision_audit
         WHERE stage = 'retry_backoff'
           AND COALESCE(evidence_json->>'source_run_id', evidence_json->>'failed_run_id') = %(id)s::text
        """,
        {"id": run_id},
    ).fetchone()
    return 0 if row is None else int(row[0])


def _write_retry_audit(
    conn: psycopg.Connection[Any],
    *,
    job_name: str,
    attempt: int,
    run_id: int,
    status: str,
) -> None:
    """Record the audited retry in ``decision_audit`` (mirrors the kick audit).

    ⚠ #2603 — the cause is taken from the row's ``status`` rather than asserted.
    This sentence read "after a transient failure" unconditionally, which is now
    false for a ``skipped`` row re-armed after losing its fire to a busy lane.
    An audit line that names the wrong cause is worse than a vague one.
    """
    cause = "a transient failure" if status == "failure" else "a lost fire (lane busy)"
    explanation = f"retry/backoff: re-enqueued job {job_name!r} (attempt {attempt}) after {cause} (run {run_id})"
    conn.execute(
        """
        INSERT INTO decision_audit
            (decision_time, stage, pass_fail, explanation, evidence_json)
        VALUES
            (NOW(), 'retry_backoff', 'RETRY', %(expl)s, %(evidence)s)
        """,
        {
            "expl": explanation,
            # ``source_run_id`` replaces the old ``failed_run_id`` key, which
            # asserted a failure that a re-armed skip row did not have. The
            # status travels beside it so a reader never has to infer the cause.
            "evidence": Jsonb(
                {"job_name": job_name, "attempt": attempt, "source_run_id": run_id, "source_status": status}
            ),
        },
    )


__all__ = ["sweep_due_retries"]
