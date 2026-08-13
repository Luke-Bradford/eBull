"""Liveness watchdog actuator (#1510 / T4 of epic #1508).

Re-fires **stalled** scheduled jobs — those :func:`app.services.job_liveness.find_stalled_jobs`
flagged (zero ``job_runs`` rows over K cadence cycles despite firing before, not
running) — through the **audited manual-queue path**, the same mechanism
``job_retry`` (#1509) and ``post_bootstrap_activation`` (#1511) use. The
``jobs_liveness_watchdog`` (#1507) only LOGGED stalls; this turns a detected
stall into one bounded, audited re-enqueue.

Spec: ``docs/specs/ops/2026-06-07-liveness-watchdog-acts.md``.

Why a stalled job is safe to kick (cause-awareness, load-bearing): the detector
counts ANY ``job_runs`` status as a fire, so a job blocked by the bootstrap gate
or a per-job prerequisite writes a ``skipped`` row every scheduled fire and is
NEVER in the stalled set. A job that IS stalled has genuinely stopped firing —
re-enqueue is the right remedy, not a re-entry into a held limit (#1484). The
kick itself flows through the universal bootstrap-state gate (prevention-log
1341), so a kick that should not run is rejected downstream — the issue's
"blocked by gate -> Needs-attention, not a retry storm" path, for free.

Storm bound (two guards, both mirrored from ``job_retry``):

  * **In-tx stall recheck + in-flight dedup.** Detection runs before this loop,
    so a natural fire could land in the gap; inside each candidate's transaction
    we re-assert the stall AND skip if a live request / running row already
    exists. Two watchdog fires cannot double-dispatch.
  * **Cooldown via ``decision_audit``.** A ``liveness_kick`` audit newer than
    ``max(cadence_period, 6h)`` means the prior kick did not take (gate-rejected
    / dead scheduler) — do NOT re-kick; the row stays attention. Bounds
    re-dispatch to once per cadence/6h, not once per 15-min watchdog fire.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import psycopg
from psycopg.types.json import Jsonb

from app.services.job_liveness import cadence_period, find_stalled_jobs

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from app.services.job_liveness import StalledJob
    from app.workers.scheduler import Cadence

logger = logging.getLogger(__name__)

_REQUESTED_BY = "system:liveness_kick"

# Floor on the per-job cooldown between re-kicks. A daily job's cadence (24h)
# already self-bounds; a 5-min job's cadence does not, so this floor stops a
# wedged high-frequency job from being kicked every 15-min watchdog tick.
_COOLDOWN_FLOOR = timedelta(hours=6)


@dataclass(frozen=True)
class ActResult:
    """Outcome of one actuator pass."""

    kicked: list[str]
    blocked: list[str]


def act_on_stalled_jobs(
    conn: psycopg.Connection[Any],
    *,
    stalled: Sequence[StalledJob],
    eligible: Mapping[str, Cadence],
    now: datetime,
) -> ActResult:
    """Re-enqueue each eligible stalled job once. Returns names kicked / blocked.

    ``conn`` MUST be autocommit so each candidate's ``conn.transaction()`` issues
    a real ``BEGIN``/``COMMIT`` (the audited-manual-queue contract), not a
    savepoint. ``eligible`` is ``{job_name: cadence}`` for ``SCHEDULED_JOBS``
    minus the sync-runs-tracked orchestrator jobs — the cadence drives the
    cooldown floor and the in-tx stall recheck.
    """
    assert conn.autocommit, "act_on_stalled_jobs requires an autocommit connection"
    kicked: list[str] = []
    blocked: list[str] = []
    for job in stalled:
        cadence = eligible.get(job.job_name)
        if cadence is None:
            # Not eligible (orchestrator_* / unregistered) — never dispatch.
            continue
        try:
            outcome = _act_one(conn, job_name=job.job_name, cadence=cadence, now=now)
        except Exception:
            logger.exception(
                "jobs_liveness_watchdog: re-enqueue failed for %r — leaving stalled for next pass",
                job.job_name,
            )
            continue
        if outcome == "kicked":
            kicked.append(job.job_name)
        elif outcome == "blocked":
            blocked.append(job.job_name)
    if kicked:
        logger.info(
            "jobs_liveness_watchdog: re-enqueued %d stalled job(s) via audited manual queue: %s",
            len(kicked),
            ", ".join(kicked),
        )
    if blocked:
        logger.warning(
            "jobs_liveness_watchdog: %d stalled job(s) blocked (kick did not take within cooldown): %s",
            len(blocked),
            ", ".join(blocked),
        )
    return ActResult(kicked=kicked, blocked=blocked)


def _act_one(
    conn: psycopg.Connection[Any],
    *,
    job_name: str,
    cadence: Cadence,
    now: datetime,
) -> str:
    """Recheck + re-enqueue one candidate atomically.

    Returns ``"kicked"`` / ``"blocked"`` / ``"skip"``.
    """
    from app.services.sync_orchestrator.dispatcher import publish_manual_job_request_with_conn

    with conn.transaction():
        # Per-job xact advisory lock (Codex ckpt-2): unlike job_retry there is no
        # durable failed row to lock FOR UPDATE, so without this two concurrent
        # actors (a second watchdog run, an operator kick, the retry sweeper)
        # could both pass the recheck + dedup before either INSERT commits and
        # double-enqueue. Process-level single-instancing is not DB isolation
        # (prevention-log 410). Namespaced key so it cannot collide with the
        # process_stop prelude lock that keys on the bare process_id.
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s)::bigint)",
            (f"liveness_kick:{job_name}",),
        )
        # In-tx stall recheck (Codex ckpt-1 #3): a natural fire may have landed
        # between find_stalled_jobs and here. Re-run the exact same predicate for
        # this one job; if it fired, it is no longer stalled -> skip.
        if not find_stalled_jobs(conn, [(job_name, cadence)], now):
            return "skip"
        # In-flight dedup: a live request / running row will produce a fresh
        # terminal -> defer without acting.
        if _has_running_run(conn, job_name) or _has_active_request(conn, job_name):
            return "skip"
        # Cooldown: a recent kick that did not clear the stall means the kick is
        # not taking (gate-rejected / dead scheduler). Do not storm — surface
        # blocked so the operator sees a genuinely-stuck job.
        cooldown_start = now - max(cadence_period(cadence), _COOLDOWN_FLOOR)
        if _kicked_since(conn, job_name, cooldown_start):
            return "blocked"
        publish_manual_job_request_with_conn(
            conn,
            job_name,
            requested_by=_REQUESTED_BY,
            process_id=job_name,
            mode="iterate",
        )
        _write_liveness_audit(conn, job_name=job_name, now=now)
        return "kicked"


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


def _kicked_since(conn: psycopg.Connection[Any], job_name: str, since: datetime) -> bool:
    """True if a ``liveness_kick`` audit for ``job_name`` exists at/after ``since``."""
    row = conn.execute(
        """
        SELECT 1
          FROM decision_audit
         WHERE stage = 'liveness_kick'
           AND decision_time >= %(since)s
           AND evidence_json ->> 'job_name' = %(job)s
         LIMIT 1
        """,
        {"since": since, "job": job_name},
    ).fetchone()
    return row is not None


def _write_liveness_audit(conn: psycopg.Connection[Any], *, job_name: str, now: datetime) -> None:
    """Record the audited re-enqueue in ``decision_audit`` (mirrors the retry audit)."""
    explanation = f"liveness: re-enqueued stalled job {job_name!r} (no fires in K cadence cycles)"
    conn.execute(
        """
        INSERT INTO decision_audit
            (decision_time, stage, pass_fail, explanation, evidence_json)
        VALUES
            (NOW(), 'liveness_kick', 'KICK', %(expl)s, %(evidence)s)
        """,
        {
            "expl": explanation,
            "evidence": Jsonb({"job_name": job_name}),
        },
    )


__all__ = ["ActResult", "act_on_stalled_jobs"]
