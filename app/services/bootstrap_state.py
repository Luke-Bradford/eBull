"""First-install bootstrap state — DB persistence layer.

Source of truth for the bootstrap orchestrator's run history and the
singleton scheduler-gate state. Spec:
``docs/superpowers/specs/2026-05-07-first-install-bootstrap.md``.

Three tables (sql/129_bootstrap_state.sql):

  - ``bootstrap_runs``    — one row per "Run bootstrap" click.
  - ``bootstrap_stages``  — one row per stage in a run (18 stages today).
  - ``bootstrap_state``   — singleton row (id=1) with the canonical
                            ``_bootstrap_complete`` gate status.

This module owns all reads / writes against those tables. Callers
(API endpoints, the orchestrator service, the scheduler prerequisite)
must go through these helpers rather than touching the tables
directly so the state machine stays consistent.

Concurrency contract: ``start_run`` takes ``SELECT ... FOR UPDATE`` on
the ``bootstrap_state`` singleton row before deciding whether to
create a new run, so two concurrent ``POST /system/bootstrap/run``
handlers cannot both succeed. The partial unique index on
``bootstrap_runs(status='running')`` is defense-in-depth.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

import psycopg

logger = logging.getLogger(__name__)


BootstrapStatus = Literal["pending", "running", "complete", "partial_error"]
RunStatus = Literal["running", "complete", "partial_error"]
StageStatus = Literal["pending", "running", "success", "error", "skipped", "blocked"]
Lane = Literal["init", "etoro", "sec", "sec_rate", "sec_bulk_download", "db"]


class BootstrapAlreadyRunning(RuntimeError):
    """Raised by ``start_run`` when a run is already in flight.

    The API layer maps this to 409 Conflict. The exception carries the
    in-flight ``run_id`` so the API response can point the operator at
    the existing run.
    """

    def __init__(self, run_id: int) -> None:
        super().__init__(f"bootstrap run {run_id} is already running")
        self.run_id = run_id


@dataclass(frozen=True)
class StageSpec:
    """Static definition of a stage. Lives in code, not DB.

    The orchestrator service builds the canonical ordered list of
    18 specs (1 init + 1 eToro + 16 SEC) and passes it to ``start_run``,
    which materialises one ``bootstrap_stages`` row per spec.
    """

    stage_key: str
    stage_order: int
    lane: Lane
    job_name: str


@dataclass(frozen=True)
class StageRow:
    """Snapshot of a single ``bootstrap_stages`` row."""

    id: int
    bootstrap_run_id: int
    stage_key: str
    stage_order: int
    lane: Lane
    job_name: str
    status: StageStatus
    started_at: datetime | None
    completed_at: datetime | None
    rows_processed: int | None
    expected_units: int | None
    units_done: int | None
    last_error: str | None
    attempt_count: int


@dataclass(frozen=True)
class RunSnapshot:
    """Latest run + its stages, read in a single transaction."""

    run_id: int
    run_status: RunStatus
    triggered_at: datetime
    completed_at: datetime | None
    stages: Sequence[StageRow] = field(default_factory=tuple)


@dataclass(frozen=True)
class BootstrapState:
    """Singleton bootstrap_state row."""

    status: BootstrapStatus
    last_run_id: int | None
    last_completed_at: datetime | None


def read_state(conn: psycopg.Connection[Any]) -> BootstrapState:
    """Return the singleton bootstrap_state row.

    Raises ``RuntimeError`` if the migration has not seeded the row.
    The migration's ``INSERT ... ON CONFLICT DO NOTHING`` makes that
    case represent migration corruption rather than first-time-run.
    """
    row = conn.execute("SELECT status, last_run_id, last_completed_at FROM bootstrap_state WHERE id = 1").fetchone()
    if row is None:
        raise RuntimeError("bootstrap_state singleton row missing; sql/129_bootstrap_state.sql may not have run")
    return BootstrapState(
        status=row[0],
        last_run_id=row[1],
        last_completed_at=row[2],
    )


def read_latest_run_with_stages(
    conn: psycopg.Connection[Any],
) -> RunSnapshot | None:
    """Return the latest bootstrap_runs row + its stages, or None.

    Reads happen inside a single transaction so a stage transition
    landing mid-fetch cannot produce an inconsistent snapshot — the
    contract this respects is the prevention-log entry "Multi-query
    read handlers must use a single snapshot".
    """
    with conn.transaction():
        run_row = conn.execute(
            """
            SELECT id, status, triggered_at, completed_at
              FROM bootstrap_runs
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()
        if run_row is None:
            return None
        run_id, run_status, triggered_at, completed_at = run_row

        stage_rows = conn.execute(
            """
            SELECT id, bootstrap_run_id, stage_key, stage_order, lane, job_name,
                   status, started_at, completed_at, rows_processed,
                   expected_units, units_done, last_error, attempt_count
              FROM bootstrap_stages
             WHERE bootstrap_run_id = %(run_id)s
             ORDER BY stage_order ASC, id ASC
            """,
            {"run_id": run_id},
        ).fetchall()

    stages = tuple(
        StageRow(
            id=row[0],
            bootstrap_run_id=row[1],
            stage_key=row[2],
            stage_order=row[3],
            lane=row[4],
            job_name=row[5],
            status=row[6],
            started_at=row[7],
            completed_at=row[8],
            rows_processed=row[9],
            expected_units=row[10],
            units_done=row[11],
            last_error=row[12],
            attempt_count=row[13],
        )
        for row in stage_rows
    )

    return RunSnapshot(
        run_id=run_id,
        run_status=run_status,
        triggered_at=triggered_at,
        completed_at=completed_at,
        stages=stages,
    )


def start_run(
    conn: psycopg.Connection[Any],
    *,
    operator_id: str | None,
    stage_specs: Sequence[StageSpec],
) -> int:
    """Create a new bootstrap run + seed pending stage rows.

    Single-flight contract: takes ``SELECT ... FOR UPDATE`` on the
    bootstrap_state singleton row, raises ``BootstrapAlreadyRunning``
    if status is already ``running``, otherwise inserts a new
    ``bootstrap_runs`` row and one ``bootstrap_stages`` row per spec,
    then flips ``bootstrap_state.status`` to ``running``.

    Returns the new ``bootstrap_runs.id``.

    All work happens inside one transaction so a partial commit cannot
    leave a half-seeded run.
    """
    if not stage_specs:
        raise ValueError("stage_specs must be non-empty")

    with conn.transaction():
        state_row = conn.execute("SELECT status, last_run_id FROM bootstrap_state WHERE id = 1 FOR UPDATE").fetchone()
        if state_row is None:
            raise RuntimeError("bootstrap_state singleton row missing; sql/129_bootstrap_state.sql may not have run")
        current_status, last_run_id = state_row
        if current_status == "running":
            raise BootstrapAlreadyRunning(run_id=last_run_id or 0)

        run_row = conn.execute(
            """
            INSERT INTO bootstrap_runs (triggered_by_operator_id, status)
            VALUES (%(operator_id)s, 'running')
            RETURNING id
            """,
            {"operator_id": operator_id},
        ).fetchone()
        if run_row is None:
            raise RuntimeError("INSERT INTO bootstrap_runs returned no row")
        run_id = run_row[0]

        for spec in stage_specs:
            conn.execute(
                """
                INSERT INTO bootstrap_stages
                       (bootstrap_run_id, stage_key, stage_order, lane, job_name, status)
                VALUES (%(run_id)s, %(stage_key)s, %(stage_order)s, %(lane)s, %(job_name)s, 'pending')
                """,
                {
                    "run_id": run_id,
                    "stage_key": spec.stage_key,
                    "stage_order": spec.stage_order,
                    "lane": spec.lane,
                    "job_name": spec.job_name,
                },
            )

        conn.execute(
            """
            UPDATE bootstrap_state
               SET status      = 'running',
                   last_run_id = %(run_id)s
             WHERE id = 1
            """,
            {"run_id": run_id},
        )

    return run_id


def mark_stage_running(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
    stage_key: str,
) -> None:
    """Transition a stage from pending to running.

    Increments ``attempt_count``, clears stale ``last_error`` /
    timestamps. Idempotency: re-running this on an already-running
    stage row leaves attempt_count un-incremented (UPDATE matches
    only ``status='pending'``).
    """
    conn.execute(
        """
        UPDATE bootstrap_stages
           SET status        = 'running',
               started_at    = now(),
               completed_at  = NULL,
               last_error    = NULL,
               attempt_count = attempt_count + 1
         WHERE bootstrap_run_id = %(run_id)s
           AND stage_key        = %(stage_key)s
           AND status           = 'pending'
        """,
        {"run_id": run_id, "stage_key": stage_key},
    )


def mark_stage_success(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
    stage_key: str,
    rows_processed: int | None = None,
) -> None:
    """Transition a stage to success on invoker exit."""
    conn.execute(
        """
        UPDATE bootstrap_stages
           SET status         = 'success',
               completed_at   = now(),
               rows_processed = %(rows_processed)s,
               last_error     = NULL
         WHERE bootstrap_run_id = %(run_id)s
           AND stage_key        = %(stage_key)s
        """,
        {
            "run_id": run_id,
            "stage_key": stage_key,
            "rows_processed": rows_processed,
        },
    )


def mark_stage_error(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
    stage_key: str,
    error_message: str,
) -> None:
    """Record a stage error. Lane proceeds; orchestrator finalises later.

    ``error_message`` is truncated to 1000 chars to keep DB rows
    bounded; full forensic detail lives in the underlying
    ``job_runs`` row that the invoker's own ``_tracked_job`` writes.
    """
    conn.execute(
        """
        UPDATE bootstrap_stages
           SET status       = 'error',
               completed_at = now(),
               last_error   = %(error_message)s
         WHERE bootstrap_run_id = %(run_id)s
           AND stage_key        = %(stage_key)s
        """,
        {
            "run_id": run_id,
            "stage_key": stage_key,
            "error_message": error_message[:1000],
        },
    )


def mark_stage_blocked(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
    stage_key: str,
    reason: str,
) -> None:
    """Mark a stage as ``blocked`` — orchestrator never invoked it
    because an upstream `requires` stage finished `error` or `blocked`.

    Distinct from `error` (which means the invoker raised). The
    operator panel renders both with red styling but a different
    sublabel: blocked = "Skipped — upstream failure".
    """
    conn.execute(
        """
        UPDATE bootstrap_stages
           SET status       = 'blocked',
               completed_at = now(),
               last_error   = %(reason)s
         WHERE bootstrap_run_id = %(run_id)s
           AND stage_key        = %(stage_key)s
        """,
        {
            "run_id": run_id,
            "stage_key": stage_key,
            "reason": reason[:1000],
        },
    )


def finalize_run(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
) -> RunStatus:
    """Compute terminal run status from per-stage outcomes.

    Called by the orchestrator after both lane threads have joined.
    All stages in error → ``partial_error``; otherwise ``complete``.
    Updates the run row, the bootstrap_state singleton, and
    ``last_completed_at`` in one transaction.

    Returns the chosen terminal status.
    """
    with conn.transaction():
        # Count both `error` and `blocked` — both are unsuccessful
        # outcomes. `blocked` = upstream failure propagation; the
        # operator must still see the run as `partial_error`.
        error_count_row = conn.execute(
            """
            SELECT COUNT(*) FROM bootstrap_stages
             WHERE bootstrap_run_id = %(run_id)s
               AND status IN ('error', 'blocked')
            """,
            {"run_id": run_id},
        ).fetchone()
        error_count = error_count_row[0] if error_count_row is not None else 0
        terminal: RunStatus = "partial_error" if error_count > 0 else "complete"

        conn.execute(
            """
            UPDATE bootstrap_runs
               SET status       = %(status)s,
                   completed_at = now()
             WHERE id = %(run_id)s
            """,
            {"status": terminal, "run_id": run_id},
        )
        conn.execute(
            """
            UPDATE bootstrap_state
               SET status            = %(status)s,
                   last_run_id       = %(run_id)s,
                   last_completed_at = now()
             WHERE id = 1
            """,
            {"status": terminal, "run_id": run_id},
        )

    return terminal


def reset_failed_stages_for_retry(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
) -> int:
    """Reset failed + later-numbered same-lane stages for retry.

    Spec §"retry-failed dependency-aware reset". Returns the number
    of rows reset to ``pending``.

    Algorithm (one transaction, ``SELECT ... FOR UPDATE`` on the
    singleton up front to prevent a concurrent ``start_run`` from
    transitioning state to ``running`` between the API's earlier
    status read and this reset — TOCTOU avoidance):

    1. Lock the bootstrap_state singleton row (``FOR UPDATE``).
    2. Raise ``BootstrapAlreadyRunning`` if status flipped to
       ``running`` since the API's pre-check; the API maps this to
       a 409 response.
    3. Find all failed (error) stages on the latest run.
    4. For each lane that has at least one failed stage, find the
       smallest ``stage_order`` of a failed stage in that lane.
    5. Reset every stage in that lane with ``stage_order >=`` the
       smallest-failed-order to ``pending``, regardless of current
       status. The orchestrator's per-stage pre-check skips stages
       already in ``success`` so only re-running re-enqueued
       pending stages happens.
    6. Flip bootstrap_state.status back to ``running``.

    If no failed stages exist, returns 0 and leaves state untouched.
    """
    with conn.transaction():
        state_row = conn.execute("SELECT status, last_run_id FROM bootstrap_state WHERE id = 1 FOR UPDATE").fetchone()
        if state_row is None:
            raise RuntimeError("bootstrap_state singleton row missing")
        current_status, current_last_run_id = state_row
        if current_status == "running":
            raise BootstrapAlreadyRunning(run_id=current_last_run_id or 0)

        failed_rows = conn.execute(
            """
            SELECT lane, MIN(stage_order)
              FROM bootstrap_stages
             WHERE bootstrap_run_id = %(run_id)s
               AND status IN ('error', 'blocked')
             GROUP BY lane
            """,
            {"run_id": run_id},
        ).fetchall()
        if not failed_rows:
            return 0

        total_reset = 0
        for lane, min_order in failed_rows:
            cursor = conn.execute(
                """
                UPDATE bootstrap_stages
                   SET status       = 'pending',
                       started_at   = NULL,
                       completed_at = NULL,
                       last_error   = NULL
                 WHERE bootstrap_run_id = %(run_id)s
                   AND lane             = %(lane)s
                   AND stage_order      >= %(min_order)s
                """,
                {"run_id": run_id, "lane": lane, "min_order": min_order},
            )
            if cursor.rowcount and cursor.rowcount > 0:
                total_reset += cursor.rowcount

        conn.execute(
            """
            UPDATE bootstrap_runs
               SET status       = 'running',
                   completed_at = NULL
             WHERE id = %(run_id)s
            """,
            {"run_id": run_id},
        )
        conn.execute(
            """
            UPDATE bootstrap_state
               SET status      = 'running',
                   last_run_id = %(run_id)s
             WHERE id = 1
            """,
            {"run_id": run_id},
        )

    return total_reset


def force_mark_complete(
    conn: psycopg.Connection[Any],
) -> None:
    """Operator escape hatch: flip bootstrap_state.status to complete.

    Used when the operator has manually fixed the cause of a stage
    failure and wants to release the scheduler gate without re-running
    heavy stages. Does not touch any run / stage row — those keep
    their accurate forensic history. Audit-logging of the call is the
    caller's responsibility.

    Concurrency contract: takes ``SELECT ... FOR UPDATE`` on the
    bootstrap_state singleton up front so a concurrent ``start_run``
    cannot transition state to ``running`` between the API's
    pre-check and this write. Raises ``BootstrapAlreadyRunning`` if
    state is ``running`` at lock-acquisition time.
    """
    with conn.transaction():
        state_row = conn.execute("SELECT status, last_run_id FROM bootstrap_state WHERE id = 1 FOR UPDATE").fetchone()
        if state_row is None:
            raise RuntimeError("bootstrap_state singleton row missing")
        current_status, current_last_run_id = state_row
        if current_status == "running":
            raise BootstrapAlreadyRunning(run_id=current_last_run_id or 0)

        conn.execute(
            """
            UPDATE bootstrap_state
               SET status            = 'complete',
                   last_completed_at = now()
             WHERE id = 1
            """
        )


def reap_orphaned_running(
    conn: psycopg.Connection[Any],
) -> bool:
    """Boot-recovery sweep for crash-orphaned runs.

    Runs once at jobs-process startup. If
    ``bootstrap_state.status='running'`` on cold start, no live thread
    is executing this run. Sweep:

      - Latest run's stages with ``status='running'`` → ``error``,
        last_error='jobs process restarted mid-run'.
      - Latest run's stages with ``status='pending'`` → ``error``,
        last_error='orchestrator did not dispatch before restart'.
      - Latest ``bootstrap_runs`` row → ``partial_error``.
      - ``bootstrap_state`` → ``partial_error``.

    All in one transaction. Idempotent on a state that is not
    ``running``; returns True if a sweep occurred, False otherwise.
    """
    with conn.transaction():
        state = conn.execute("SELECT status, last_run_id FROM bootstrap_state WHERE id = 1 FOR UPDATE").fetchone()
        if state is None or state[0] != "running":
            return False
        last_run_id = state[1]
        if last_run_id is None:
            conn.execute("UPDATE bootstrap_state SET status = 'partial_error' WHERE id = 1")
            return True

        conn.execute(
            """
            UPDATE bootstrap_stages
               SET status       = 'error',
                   completed_at = now(),
                   last_error   = 'jobs process restarted mid-run'
             WHERE bootstrap_run_id = %(run_id)s
               AND status           = 'running'
            """,
            {"run_id": last_run_id},
        )
        conn.execute(
            """
            UPDATE bootstrap_stages
               SET status       = 'error',
                   completed_at = now(),
                   last_error   = 'orchestrator did not dispatch before restart'
             WHERE bootstrap_run_id = %(run_id)s
               AND status           = 'pending'
            """,
            {"run_id": last_run_id},
        )
        conn.execute(
            """
            UPDATE bootstrap_runs
               SET status       = 'partial_error',
                   completed_at = now()
             WHERE id = %(run_id)s
            """,
            {"run_id": last_run_id},
        )
        conn.execute(
            """
            UPDATE bootstrap_state
               SET status            = 'partial_error',
                   last_completed_at = now()
             WHERE id = 1
            """
        )

    return True


__all__ = [
    "BootstrapAlreadyRunning",
    "BootstrapState",
    "BootstrapStatus",
    "Lane",
    "RunSnapshot",
    "RunStatus",
    "StageRow",
    "StageSpec",
    "StageStatus",
    "finalize_run",
    "force_mark_complete",
    "mark_stage_error",
    "mark_stage_running",
    "mark_stage_success",
    "read_latest_run_with_stages",
    "read_state",
    "reap_orphaned_running",
    "reset_failed_stages_for_retry",
    "start_run",
]
