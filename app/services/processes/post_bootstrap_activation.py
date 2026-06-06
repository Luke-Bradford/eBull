"""Post-bootstrap auto-current activation (#1511 / T5 of #1508).

Called once from ``bootstrap_state.finalize_run`` when a run transitions
``running → complete`` (parts b + c of the spec
``docs/specs/ops/2026-06-06-post-bootstrap-auto-current.md``). It enqueues,
through the **audited manual-queue path**, the jobs that should run the moment
the universal bootstrap gate opens:

  * (c) **catch-up-trap recovery** — jobs with ``catch_up_on_boot=True`` whose
    latest terminal ``job_runs`` row is a gate skip (``status='skipped'``,
    ``error_msg='bootstrap_not_complete'``). Their boot catch-up was evaluated
    while bootstrap was still running, wrote a ``skipped`` row, and is never
    re-evaluated until the next process restart (prevention-log 1339-1343).
  * (b) **genuine-gap kick** — never-run jobs whose ``data_freshness_index``
    source is NOT bootstrap-covered (so their operator-visible data is a real
    gap), gated to ``prerequisite is None`` (empty-DB-safe, per settled-decision
    #1181). Self-populating: ∅ on the current registry (every registered
    freshness source is bootstrap-covered), fires only if a genuinely uncovered
    never-run job is added later.

Design constraints (Codex ckpt-1):

  * **No new universal-gate carve-out.** The gate is already ``complete`` by the
    time this runs, so the manual-queue dispatch passes it without an
    ``override_bootstrap_gate`` flag — the carve-out allow-list stays 2,
    test-pinned (#1064/#1181).
  * **Abort-safe / best-effort.** Runs AFTER the completion is committed; each
    candidate enqueues + audits inside its OWN ``conn.transaction()`` so a
    failure rolls back only that candidate (psycopg3 aborts the whole tx on the
    first error). The caller wraps the top-level call in try/except, so nothing
    here can roll the completion back. The durable backstop is the now-open gate
    + normal cadence (the two catch-up jobs are daily) + the next boot catch-up;
    re-enqueue bodies are idempotent, so a duplicate at the next scheduled fire
    is harmless.
"""

from __future__ import annotations

import logging
from typing import Any, Literal

import psycopg
from psycopg.types.json import Jsonb

from app.services.processes.bootstrap_coverage import BOOTSTRAP_COVERED_FRESHNESS_SOURCES

logger = logging.getLogger(__name__)

# Matches ``bootstrap_gate._GATE_REASON_BOOTSTRAP_NOT_COMPLETE`` — the
# ``error_msg`` ``record_job_skip`` writes when the universal gate blocks a
# catch-up fire. A catch-up job sitting on exactly this skip is gate-trapped
# (NOT skipped for a benign per-job prerequisite — prevention 249).
_GATE_SKIP_REASON: Literal["bootstrap_not_complete"] = "bootstrap_not_complete"

_REQUESTED_BY = "system:post_bootstrap_activation"

_ActivationReason = Literal["catch_up_trap_recovery", "genuine_gap_kick"]


def activate_post_bootstrap(conn: psycopg.Connection[Any], *, run_id: int) -> list[str]:
    """Enqueue catch-up-trapped + genuine-gap jobs via the audited manual queue.

    Returns the list of job names actually enqueued (excludes candidates that
    already had an active request in flight, and any whose enqueue failed). The
    caller (``finalize_run``) treats this as best-effort and never lets it
    propagate past completion.
    """
    candidates = _select_candidates(conn)
    if not candidates:
        return []
    enqueued: list[str] = []
    for job_name, reason in candidates:
        try:
            if _enqueue_one(conn, job_name=job_name, reason=reason, run_id=run_id):
                enqueued.append(job_name)
        except Exception:
            logger.exception(
                "post_bootstrap_activation: enqueue failed for %r — skipping (job recovers via gate+cadence)",
                job_name,
            )
    if enqueued:
        logger.info(
            "post_bootstrap_activation: run %s enqueued %d job(s) via audited manual queue: %s",
            run_id,
            len(enqueued),
            ", ".join(enqueued),
        )
    return enqueued


def _select_candidates(conn: psycopg.Connection[Any]) -> list[tuple[str, _ActivationReason]]:
    """Resolve the (job_name, reason) candidate set against the live registry.

    Read-only; runs in its own transaction so it leaves no dangling implicit
    transaction on the shared connection.
    """
    # Lazy imports — keep this module (reachable from low-level bootstrap_state)
    # free of an import cycle through the scheduler / watermark registries.
    from app.services.processes.watermarks import freshness_source_for
    from app.workers.scheduler import SCHEDULED_JOBS

    with conn.transaction():
        latest = _latest_terminal_by_job(conn)

    candidates: list[tuple[str, _ActivationReason]] = []
    for job in SCHEDULED_JOBS:
        terminal = latest.get(job.name)
        if _is_catch_up_trap(job, terminal):
            candidates.append((job.name, "catch_up_trap_recovery"))
        elif _is_genuine_gap(job, terminal, freshness_source_for(job.name)):
            candidates.append((job.name, "genuine_gap_kick"))
    return candidates


def _is_catch_up_trap(job: Any, terminal: tuple[str, str | None] | None) -> bool:
    """(c) A NON-exempt ``catch_up_on_boot`` job whose latest terminal run is the
    universal gate skip — its boot catch-up fired while bootstrap was still
    running and is never re-evaluated until a restart (prevention 1339-1343).

    Exempt jobs (the carve-out allow-list) BYPASS the gate, so a gate-skip
    ``job_runs`` row on one is a stale artifact, NOT a trap. Notably
    ``orchestrator_high_frequency_sync`` writes ``sync_runs`` (not ``job_runs``)
    and already runs every 5 min via its exemption — its lingering boot-era
    ``skipped/bootstrap_not_complete`` row must NOT trigger a redundant
    (double-firing) kick. Verified on dev: without this guard the selection
    wrongly picked ``orchestrator_high_frequency_sync``."""
    return (
        bool(job.catch_up_on_boot)
        and not getattr(job, "exempt_from_universal_bootstrap_gate", False)
        and terminal == ("skipped", _GATE_SKIP_REASON)
    )


def _is_genuine_gap(
    job: Any,
    terminal: tuple[str, str | None] | None,
    source: str | None,
) -> bool:
    """(b) A never-run job whose freshness source is a genuine, bootstrap-
    UNcovered gap, and which is empty-DB-safe to run once (no ``catch_up`` — that
    path is (c)/boot; ``prerequisite is None`` per settled-decision #1181)."""
    return (
        not job.catch_up_on_boot
        and terminal is None
        and job.prerequisite is None
        and source is not None
        and source not in BOOTSTRAP_COVERED_FRESHNESS_SOURCES
    )


def _latest_terminal_by_job(
    conn: psycopg.Connection[Any],
) -> dict[str, tuple[str, str | None]]:
    """Latest terminal ``(status, error_msg)`` per job_name (never-run jobs absent)."""
    rows = conn.execute(
        """
        SELECT DISTINCT ON (job_name) job_name, status, error_msg
          FROM job_runs
         WHERE status IN ('success', 'failure', 'skipped', 'cancelled')
         ORDER BY job_name, started_at DESC
        """
    ).fetchall()
    return {row[0]: (row[1], row[2]) for row in rows}


def _enqueue_one(
    conn: psycopg.Connection[Any],
    *,
    job_name: str,
    reason: _ActivationReason,
    run_id: int,
) -> bool:
    """Enqueue one candidate + write its audit row in a single transaction.

    Returns True when a request was published, False when skipped because an
    active manual request already exists for the job (double-fire guard).
    """
    from app.services.sync_orchestrator.dispatcher import publish_manual_job_request_with_conn

    with conn.transaction():
        if _has_active_request(conn, job_name):
            return False
        publish_manual_job_request_with_conn(conn, job_name, requested_by=_REQUESTED_BY)
        _write_activation_audit(conn, job_name=job_name, reason=reason, run_id=run_id)
        return True


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


def _write_activation_audit(
    conn: psycopg.Connection[Any],
    *,
    job_name: str,
    reason: _ActivationReason,
    run_id: int,
) -> None:
    """Record the audited kick in ``decision_audit`` (mirrors the gate-override audit)."""
    explanation = (
        f"post-bootstrap auto-current: enqueued job {job_name!r} ({reason}) "
        f"after bootstrap run {run_id} reached 'complete'"
    )
    conn.execute(
        """
        INSERT INTO decision_audit
            (decision_time, stage, pass_fail, explanation, evidence_json)
        VALUES
            (NOW(), 'post_bootstrap_activation', 'KICK', %(expl)s, %(evidence)s)
        """,
        {
            "expl": explanation,
            "evidence": Jsonb({"job_name": job_name, "reason": reason, "run_id": run_id}),
        },
    )


__all__ = ["activate_post_bootstrap"]
