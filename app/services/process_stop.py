"""Cooperative-cancel signal infrastructure for the admin control hub.

Issue #1065 (umbrella #1064).
Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §Cancel semantics — cooperative + §Full-wash execution fence.

Cooperative cancel writes a row into ``process_stop_requests``
(sql/135) targeting a specific ``(target_run_kind, target_run_id)``
tuple — the API handler resolves the active run with
``SELECT ... FOR UPDATE`` first, so the row id is pinned at insert
time and a stop signal cannot wrongly cancel a later run.

Workers poll the table at well-defined checkpoints (between bootstrap
stages, between SEC manifest accessions, between sync orchestrator
layers). Mid-stage work runs to completion; the watermark advance +
ON CONFLICT idempotency on ``sec_filing_manifest`` /
``data_freshness_index`` mean Iterate later resumes correctly.

This module also owns the start-of-work prelude lock used by
full-wash, Iterate, and scheduled paths
(``acquire_prelude_lock``) — a transaction-scoped advisory lock that
serialises the fence-check + active-marker-publish step. Without it,
a scheduled run starting in the gap between full-wash COMMIT and
worker-start could read stale watermark state. Lock key is
deterministic from ``process_id`` so all paths converge on the same
key.

Boot recovery:

* ``reap_orphaned_stop_requests`` sweeps stop rows abandoned by a
  jobs-process restart (>6h old, never observed). Frees the
  partial-unique slot for future cancels.
* ``reap_stuck_full_wash_fences`` sweeps queue-row fences that stayed
  ``dispatched`` >6h (worst-case bootstrap full-wash is ~2h).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

import psycopg
import psycopg.rows

logger = logging.getLogger(__name__)


StopMode = Literal["cooperative", "terminate"]
TargetRunKind = Literal["bootstrap_run", "job_run", "sync_run"]
Mechanism = Literal["bootstrap", "scheduled_job", "ingest_sweep"]


class NoActiveRunError(Exception):
    """Raised by request_stop when no run is currently 'running' for the process."""


class StopAlreadyPendingError(Exception):
    """Raised by request_stop when an active stop row already exists for the
    target run (partial-unique index hit)."""


@dataclass(frozen=True, slots=True)
class StopRequest:
    """Worker-facing view of an outstanding stop request."""

    id: int
    process_id: str
    mechanism: Mechanism
    target_run_kind: TargetRunKind
    target_run_id: int
    mode: StopMode
    requested_at: datetime
    requested_by_operator_id: UUID | None
    observed_at: datetime | None
    completed_at: datetime | None


def acquire_prelude_lock(conn: psycopg.Connection[Any], process_id: str) -> None:
    """Acquire the per-process advisory lock for a start-of-work prelude.

    Tx-scoped (``pg_advisory_xact_lock``) — released automatically at the
    enclosing transaction's COMMIT or ROLLBACK. All paths that mutate this
    process's state (full-wash trigger, Iterate trigger, scheduled-run
    prelude) acquire the same key, so the fence-check + active-marker
    publish step happens atomically against any concurrent path.

    Lock key: ``hashtext(process_id)::bigint``. Deterministic + collision-
    free at our process count (collision risk is hash-collision-only).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s)::bigint)",
            (process_id,),
        )


def request_stop(
    conn: psycopg.Connection[Any],
    *,
    process_id: str,
    mechanism: Mechanism,
    target_run_kind: TargetRunKind,
    target_run_id: int,
    mode: StopMode,
    requested_by_operator_id: UUID | None,
) -> int:
    """Insert a cooperative-cancel signal for an already-locked active run.

    The CALLER is responsible for resolving + ``SELECT ... FOR UPDATE``
    locking the active run row in the mechanism-specific table
    (bootstrap_runs / job_runs / sync_runs) BEFORE invoking this helper.
    The lock guarantees ``target_run_id`` is the run that is genuinely
    in flight; without it, the run could finish between resolution and
    insert, leaving an orphan stop row.

    Raises:
        StopAlreadyPendingError: if the partial-unique index
            ``process_stop_requests_active_unq`` rejects the insert
            (a previous active stop request exists for this run).
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO process_stop_requests (
                    process_id,
                    mechanism,
                    target_run_kind,
                    target_run_id,
                    mode,
                    requested_by_operator_id
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    process_id,
                    mechanism,
                    target_run_kind,
                    target_run_id,
                    mode,
                    requested_by_operator_id,
                ),
            )
            row = cur.fetchone()
            assert row is not None  # INSERT ... RETURNING always yields a row
            return int(row[0])
    except psycopg.errors.UniqueViolation as exc:
        raise StopAlreadyPendingError(f"active stop already pending for {target_run_kind} {target_run_id}") from exc


def is_stop_requested(
    conn: psycopg.Connection[Any],
    *,
    target_run_kind: TargetRunKind,
    target_run_id: int,
) -> StopRequest | None:
    """Worker poll at a cancel checkpoint.

    Returns the most recent unobserved stop request for the EXACT run
    the worker owns, or None. Pinning on ``target_run_id`` guarantees a
    stop signal for a future run cannot wrongly cancel the current one.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT id, process_id, mechanism, target_run_kind, target_run_id,
                   mode, requested_at, requested_by_operator_id,
                   observed_at, completed_at
              FROM process_stop_requests
             WHERE target_run_kind = %s
               AND target_run_id = %s
               AND completed_at IS NULL
             ORDER BY requested_at DESC
             LIMIT 1
            """,
            (target_run_kind, target_run_id),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return StopRequest(
            id=row["id"],
            process_id=row["process_id"],
            mechanism=row["mechanism"],
            target_run_kind=row["target_run_kind"],
            target_run_id=row["target_run_id"],
            mode=row["mode"],
            requested_at=row["requested_at"],
            requested_by_operator_id=row["requested_by_operator_id"],
            observed_at=row["observed_at"],
            completed_at=row["completed_at"],
        )


def mark_observed(conn: psycopg.Connection[Any], stop_request_id: int) -> None:
    """Record that the worker has SEEN the stop signal.

    Idempotent: calling twice does not advance ``observed_at``.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE process_stop_requests
               SET observed_at = COALESCE(observed_at, now())
             WHERE id = %s
            """,
            (stop_request_id,),
        )


def mark_completed(conn: psycopg.Connection[Any], stop_request_id: int) -> None:
    """Record that the worker has finished cleanly in response to the stop.

    Frees the partial-unique active-stop slot, allowing future cancels
    against the same run kind / id (e.g. after Iterate restarts the run).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE process_stop_requests
               SET completed_at = COALESCE(completed_at, now()),
                   observed_at = COALESCE(observed_at, now())
             WHERE id = %s
            """,
            (stop_request_id,),
        )


def reap_orphaned_stop_requests(conn: psycopg.Connection[Any], *, max_age_hours: int = 6) -> int:
    """Boot-recovery sweep: free abandoned stop rows.

    A jobs-process restart between cancel-insert and worker-observe
    leaves the stop row in ``observed_at IS NULL AND completed_at IS NULL``.
    The partial-unique index would block future cancels against the same
    run id forever. Sweep transitions:

        SET completed_at = now()      -- frees the partial-unique slot
        -- observed_at left NULL: sentinel "abandoned, never observed"

    Per Codex round 2 R2-W2: ``observed_at = NULL`` after ``completed_at``
    is set is the audit-visible "abandoned" sentinel. Returns the count
    of swept rows for caller logging.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE process_stop_requests
               SET completed_at = now()
             WHERE completed_at IS NULL
               AND observed_at IS NULL                  -- abandoned sentinel
               AND requested_at < now() - (%s::int * INTERVAL '1 hour')
            """,
            (int(max_age_hours),),
        )
        return cur.rowcount


def reap_stuck_full_wash_fences(conn: psycopg.Connection[Any], *, max_age_hours: int = 6) -> int:
    """Boot-recovery sweep: free stuck full-wash fence rows.

    Fence rows in ``status='dispatched'`` for >max_age_hours are presumed
    abandoned (worst-case bootstrap full-wash is ~2h). Transition them
    to ``rejected`` (verified at sql/084:23 — ``failed`` is NOT in the
    pending_job_requests CHECK set, Codex round 3 R3-B2). Returns count
    of swept rows for caller logging.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pending_job_requests
               SET status = 'rejected',
                   error_msg = 'dispatched full_wash fence stuck '
                               || %s::text
                               || 'h, freed by boot-recovery'
             WHERE mode = 'full_wash'
               AND status = 'dispatched'
               AND requested_at < now() - (%s::int * INTERVAL '1 hour')
            """,
            (int(max_age_hours), int(max_age_hours)),
        )
        return cur.rowcount


def boot_recovery_sweep(conn: psycopg.Connection[Any]) -> tuple[int, int]:
    """Run both boot-recovery sweeps and commit. Called from jobs startup.

    Returns ``(orphaned_stop_count, stuck_fence_count)`` for caller
    logging. Idempotent — safe to invoke multiple times.
    """
    orphaned = reap_orphaned_stop_requests(conn)
    stuck = reap_stuck_full_wash_fences(conn)
    conn.commit()
    if orphaned or stuck:
        logger.info(
            "process_stop boot-recovery: orphaned_stop=%d stuck_fence=%d",
            orphaned,
            stuck,
        )
    return orphaned, stuck


__all__ = [
    "Mechanism",
    "NoActiveRunError",
    "StopAlreadyPendingError",
    "StopMode",
    "StopRequest",
    "TargetRunKind",
    "acquire_prelude_lock",
    "boot_recovery_sweep",
    "is_stop_requested",
    "mark_completed",
    "mark_observed",
    "reap_orphaned_stop_requests",
    "reap_stuck_full_wash_fences",
    "request_stop",
]
