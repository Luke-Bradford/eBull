"""First-install bootstrap state — DB persistence layer.

Source of truth for the bootstrap orchestrator's run history and the
singleton scheduler-gate state. Spec:
``docs/superpowers/specs/2026-05-07-first-install-bootstrap.md``.

Three tables (sql/129_bootstrap_state.sql):

  - ``bootstrap_runs``    — one row per "Run bootstrap" click.
  - ``bootstrap_stages``  — one row per stage in a run (26 stages today;
                            catalogue lives in
                            ``app/services/bootstrap_orchestrator.py::_BOOTSTRAP_STAGE_SPECS``).
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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

import psycopg

from app.services.process_stop import (
    StopAlreadyPendingError,
    request_stop,
)

logger = logging.getLogger(__name__)


BootstrapStatus = Literal["pending", "running", "complete", "partial_error", "cancelled"]
RunStatus = Literal["running", "complete", "partial_error", "cancelled"]
StageStatus = Literal["pending", "running", "success", "error", "skipped", "blocked", "cancelled"]
Lane = Literal[
    "init",
    "etoro",
    "sec",
    "sec_rate",
    "sec_bulk_download",
    "db",
    "db_filings",
    "db_fundamentals_raw",
    "db_ownership_inst",
    "db_ownership_insider",
    "db_ownership_funds",
]
"""Row-shape Literal for ``bootstrap_stages.lane`` reads. Mirrors
``app/jobs/sources.py::Lane`` plus the legacy ``"sec"`` catch-all
preserved for pre-#1020 rows. New family lanes added by #1141 /
Task E of #1136 audit (see
``docs/superpowers/specs/2026-05-13-db-lane-family-split.md``)."""


class BootstrapAlreadyRunning(RuntimeError):
    """Raised by ``start_run`` when a run is already in flight.

    The API layer maps this to 409 Conflict. The exception carries the
    in-flight ``run_id`` so the API response can point the operator at
    the existing run.
    """

    def __init__(self, run_id: int) -> None:
        super().__init__(f"bootstrap run {run_id} is already running")
        self.run_id = run_id


class BootstrapNotRunning(RuntimeError):
    """Raised by ``cancel_run`` when no bootstrap run is currently in flight.

    The API layer maps this to 409 Conflict — there is nothing to cancel.
    """


class BootstrapNoPriorRun(RuntimeError):
    """Raised by ``reset_failed_stages_for_retry`` when the singleton's
    ``last_run_id`` is NULL — no prior bootstrap run exists to retry.

    Maps to 404. Precedes the status-check inside the helper so a fresh
    install (`status='pending', last_run_id IS NULL`) returns the same
    operator message as the wipe-then-mark-partial edge case. Issue #1139.
    """


class BootstrapNotResettable(RuntimeError):
    """Raised by ``reset_failed_stages_for_retry`` when the singleton's
    ``status`` is not in the resettable set (``partial_error`` or
    ``cancelled``) but a prior run does exist.

    Maps to 409 ``bootstrap_not_resettable``. Carries the current status
    so the API response detail can name it. Issue #1139.
    """

    def __init__(self, status: str) -> None:
        super().__init__(f"bootstrap state {status!r} is not resettable")
        self.status = status


class BootstrapStageCancelled(RuntimeError):
    """Raised by a long-running stage invoker when it observes the
    bootstrap-run cancel signal at one of its checkpoints.

    Issue #1064 PR3d. Stages with multi-minute loops (the SEC drain,
    the 13F sweep) poll ``bootstrap_cancel_requested()`` periodically;
    when the operator clicks Cancel the helper returns True and the
    invoker raises this exception to bail out cooperatively. The
    bootstrap orchestrator's ``_run_one_stage`` catches it, marks the
    stage as ``cancelled`` (PR3c #1093), and the next dispatcher
    iteration's run-level checkpoint terminalises the entire run.

    The exception carries the stage_key (when known) so the operator
    audit log can name the stage that observed the cancel; default
    empty string for stages that don't thread the key through.
    """

    def __init__(self, message: str = "stage cancelled by operator", stage_key: str = "") -> None:
        super().__init__(message)
        self.stage_key = stage_key


@dataclass(frozen=True)
class StageSpec:
    """Static definition of a stage. Lives in code, not DB.

    The orchestrator service builds the canonical ordered list of
    24 specs (1 init + 1 eToro + 1 sec_bulk_download + 7 db + 14 sec_rate;
    see ``app/services/bootstrap_orchestrator.py::_BOOTSTRAP_STAGE_SPECS``)
    and passes it to ``start_run``, which materialises one
    ``bootstrap_stages`` row per spec.
    """

    stage_key: str
    stage_order: int
    lane: Lane
    job_name: str
    # PR1a #1064 — params dict the bootstrap dispatcher passes to the
    # registered invoker. Default empty mapping = "use the invoker's
    # registry-default params" (PR1b's materialise_scheduled_params
    # path). Stages 14, 15, 21 will populate this in PR1c when the
    # bespoke wrappers collapse — at that point the bootstrap-only
    # param overrides (e.g. ``min_period_of_report`` for the bounded
    # 13F sweep, ``filing_types`` for the seed) live here as data,
    # not in a separate code path. See
    # ``docs/wiki/job-registry-audit.md`` §4 for the per-wrapper
    # collapse plan. Mapping rather than dict to signal read-only
    # consumption — dispatcher must not mutate.
    params: Mapping[str, Any] = field(default_factory=dict)


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
    operator_id: UUID | None,
    stage_specs: Sequence[StageSpec],
) -> int:
    """Create a new bootstrap run + seed pending stage rows.

    Single-flight contract: takes ``SELECT ... FOR UPDATE`` on the
    bootstrap_state singleton row, raises ``BootstrapAlreadyRunning``
    if status is already ``running``, otherwise inserts a new
    ``bootstrap_runs`` row and one ``bootstrap_stages`` row per spec,
    then flips ``bootstrap_state.status`` to ``running``.

    Returns the new ``bootstrap_runs.id``.

    ``operator_id`` populates ``bootstrap_runs.triggered_by_operator_id``
    for audit. ``None`` is correct for service-token initiated runs;
    only operator-session callers populate the column. The retry path
    must NOT overwrite this column on the existing row (#1139 — that
    would corrupt original-run audit); only fresh runs created here
    take a value.

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
    """Transition a stage to success on invoker exit.

    ``status='running'`` predicate (Codex pre-push round 1 WARNING
    W3): defense in depth against a late stage update racing with
    ``mark_run_cancelled`` having already swept the stage to
    ``error``. The dispatcher's wait()/checkpoint ordering already
    avoids the race in the normal flow, but pinning the helper to
    "only advance from running" keeps the invariant local.
    """
    conn.execute(
        """
        UPDATE bootstrap_stages
           SET status         = 'success',
               completed_at   = now(),
               rows_processed = %(rows_processed)s,
               last_error     = NULL
         WHERE bootstrap_run_id = %(run_id)s
           AND stage_key        = %(stage_key)s
           AND status           = 'running'
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

    ``status='running'`` predicate (Codex pre-push round 1 W3):
    avoids overwriting a cancellation sweep with a late error
    transition (defense in depth — dispatcher wait() ordering
    already prevents the race in the normal flow).
    """
    conn.execute(
        """
        UPDATE bootstrap_stages
           SET status       = 'error',
               completed_at = now(),
               last_error   = %(error_message)s
         WHERE bootstrap_run_id = %(run_id)s
           AND stage_key        = %(stage_key)s
           AND status           = 'running'
        """,
        {
            "run_id": run_id,
            "stage_key": stage_key,
            "error_message": error_message[:1000],
        },
    )


def mark_stage_skipped(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
    stage_key: str,
    reason: str,
) -> None:
    """Mark a stage as ``skipped`` — operator-policy bypass.

    Distinct from ``blocked`` (which means upstream failure forced
    the skip). ``skipped`` is the right state for intentional bypass
    paths like the slow-connection fallback (#1041) where Phase C
    is bypassed in favour of the legacy chain. ``finalize_run`` does
    NOT count ``skipped`` as a failure, so the run still reaches
    ``complete`` when only skips remain.

    ``status IN ('running', 'pending')`` predicate (Codex pre-push
    round 1 W3): allow the existing two callsites — the bypass path
    skips a still-pending stage; the lane runner skips a stage it
    just transitioned to running. Refuses to overwrite terminal
    states (success / error / cancelled-via-sweep).
    """
    conn.execute(
        """
        UPDATE bootstrap_stages
           SET status       = 'skipped',
               completed_at = now(),
               last_error   = %(reason)s
         WHERE bootstrap_run_id = %(run_id)s
           AND stage_key        = %(stage_key)s
           AND status           IN ('running', 'pending')
        """,
        {
            "run_id": run_id,
            "stage_key": stage_key,
            "reason": reason[:1000],
        },
    )


def mark_stage_cancelled(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
    stage_key: str,
    reason: str,
) -> None:
    """Mark a stage as ``cancelled`` — operator clicked Cancel mid-stage.

    Issue #1064 PR3d (#1093 schema migration). Distinct from ``error``
    (genuine failure) and ``skipped`` (operator-policy bypass) — this
    fires when the stage's invoker observes ``cancel_requested_at``
    at one of its long-loop checkpoints and raises
    ``BootstrapStageCancelled``. The orchestrator's
    ``_run_one_stage`` catches the exception and calls this helper.

    Refuses to overwrite terminal states; allows the same
    ``running``/``pending`` transition window as ``mark_stage_skipped``.
    """
    conn.execute(
        """
        UPDATE bootstrap_stages
           SET status       = 'cancelled',
               completed_at = now(),
               last_error   = %(reason)s
         WHERE bootstrap_run_id = %(run_id)s
           AND stage_key        = %(stage_key)s
           AND status           IN ('running', 'pending')
        """,
        {
            "run_id": run_id,
            "stage_key": stage_key,
            "reason": reason[:1000],
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

    Cooperative-cancel handling (Codex pre-push round 1 BLOCKING B2 —
    closes the "no checkpoint between dispatcher loop-exit and
    finalize_run" race): we lock the run row ``FOR UPDATE`` at the
    start of the tx and then check ``cancel_requested_at``. If set,
    the operator clicked Cancel any time before this commit — even
    after the dispatcher's last checkpoint observed nothing — and we
    terminalise as ``cancelled`` here, sweeping any remaining running/
    pending stages to ``error`` so a follow-up Iterate retries them.

    Lock-order discipline: this function locks ``bootstrap_runs``
    BEFORE writing to ``bootstrap_state`` — the same order
    ``cancel_run`` uses — so the two paths cannot deadlock.

    The ``status='running'`` guard on the UPDATEs preserves any prior
    terminal state set by ``mark_run_cancelled`` from a dispatcher
    checkpoint that landed slightly earlier.
    """
    with conn.transaction():
        # Lock the run row first. cancel_run also locks runs first;
        # finalize_run holding state-then-runs would deadlock against
        # a concurrent cancel.
        run_meta = conn.execute(
            """
            SELECT status, cancel_requested_at IS NOT NULL
              FROM bootstrap_runs
             WHERE id = %(run_id)s
             FOR UPDATE
            """,
            {"run_id": run_id},
        ).fetchone()
        if run_meta is None:
            raise RuntimeError(f"finalize_run: bootstrap_runs row {run_id} disappeared")
        current_status, cancel_pending = run_meta

        # Already terminal (e.g. mark_run_cancelled fired from a
        # dispatcher checkpoint). Return what the row says.
        if current_status != "running":
            return current_status

        # Cancel was requested but never observed by a checkpoint —
        # honour it here. Sweep stages so retry-failed has work to
        # reset. mark_run_cancelled idempotently transitions the run
        # + state under our held row lock.
        if cancel_pending:
            mark_run_cancelled(
                conn,
                run_id=run_id,
                notes_line="cancelled by operator before finalize",
            )
            return "cancelled"

        # Count both `error` and `blocked` — both are unsuccessful
        # outcomes. `blocked` = upstream failure propagation; the
        # operator must still see the run as `partial_error`.
        error_count_row = conn.execute(
            """
            SELECT COUNT(*) FROM bootstrap_stages
             WHERE bootstrap_run_id = %(run_id)s
               AND status IN ('error', 'blocked', 'cancelled')
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
             WHERE id     = %(run_id)s
               AND status = 'running'
            """,
            {"status": terminal, "run_id": run_id},
        )
        conn.execute(
            """
            UPDATE bootstrap_state
               SET status            = %(status)s,
                   last_run_id       = %(run_id)s,
                   last_completed_at = now()
             WHERE id     = 1
               AND status = 'running'
            """,
            {"status": terminal, "run_id": run_id},
        )

    return terminal


def reset_failed_stages_for_retry(
    conn: psycopg.Connection[Any],
) -> tuple[int, int]:
    """Reset failed + later-numbered same-lane stages for retry.

    Derives the target ``run_id`` from ``bootstrap_state.last_run_id``
    under the same ``FOR UPDATE`` lock that gates status — callers
    pass no ``run_id`` argument, so the stale-id race (#1139) is
    structurally impossible.

    Returns ``(run_id, reset_count)`` on success. ``reset_count == 0``
    means the singleton was in a resettable status but the latest run
    had no failed stages — the helper does NOT flip state in that
    case (nothing to retry); the API maps to 404.

    Raises (precedence — first match wins inside the lock):
      * ``BootstrapNoPriorRun``     — singleton.last_run_id IS NULL
                                      (any status). API → 404.
      * ``BootstrapAlreadyRunning`` — singleton.status == 'running'.
                                      API → 409 ``bootstrap_running``.
      * ``BootstrapNotResettable``  — singleton.status in
                                      {pending, complete} (i.e. not
                                      in {partial_error, cancelled}).
                                      API → 409 ``bootstrap_not_resettable``.

    No-prior-run precedence means a fresh install
    (``pending + last_run_id NULL``) returns the same 404 as the
    wipe-then-mark-partial edge case — preserves the original
    /retry-failed contract.
    """
    with conn.transaction():
        state_row = conn.execute("SELECT status, last_run_id FROM bootstrap_state WHERE id = 1 FOR UPDATE").fetchone()
        if state_row is None:
            raise RuntimeError("bootstrap_state singleton row missing")
        current_status, current_last_run_id = state_row
        if current_last_run_id is None:
            raise BootstrapNoPriorRun()
        if current_status == "running":
            raise BootstrapAlreadyRunning(run_id=current_last_run_id)
        if current_status not in ("partial_error", "cancelled"):
            raise BootstrapNotResettable(status=current_status)

        run_id = int(current_last_run_id)

        failed_rows = conn.execute(
            """
            SELECT lane, MIN(stage_order)
              FROM bootstrap_stages
             WHERE bootstrap_run_id = %(run_id)s
               AND status IN ('error', 'blocked', 'cancelled')
             GROUP BY lane
            """,
            {"run_id": run_id},
        ).fetchall()
        if not failed_rows:
            return (run_id, 0)

        total_reset = 0
        for lane, min_order in failed_rows:
            cursor = conn.execute(
                """
                UPDATE bootstrap_stages
                   SET status         = 'pending',
                       started_at     = NULL,
                       completed_at   = NULL,
                       last_error     = NULL,
                       -- #1140 Task C: reset rows_processed alongside
                       -- the rest of the stage row so the operator
                       -- timeline / bootstrap_adapter aggregate don't
                       -- show stale counts from the prior failed pass.
                       rows_processed = NULL
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

    return (run_id, total_reset)


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


def cancel_run(
    conn: psycopg.Connection[Any],
    *,
    requested_by_operator_id: UUID | None,
    mode: Literal["cooperative", "terminate"] = "cooperative",
) -> int:
    """Cancel the in-flight bootstrap run.

    Spec §Cancel semantics — cooperative + §PR2.

    Issue #1092 (PR3b #1064): ``mode`` plumbed through from the FE
    cancel modal selection. Pre-fix the helper hardcoded
    ``mode='cooperative'`` regardless of operator choice. ``terminate``
    in v1 still writes the same stop row — the worker observes it at
    the next checkpoint and acts cooperatively. The operator-visible
    distinction lives in ``process_stop_requests.mode`` so post-mortem
    auditing can tell what the operator asked for vs what the worker
    did. Genuine terminate (forcibly kill stuck worker) requires a
    jobs-process restart per the cancel runbook.

    One-transaction flow. Lock-order discipline (Codex pre-push round
    1 BLOCKING B1): we acquire the bootstrap_runs row lock FIRST and
    do NOT touch the bootstrap_state singleton lock — same order the
    finalize_run UPDATEs use (runs first, then state). Locking state
    first here would deadlock against a finalize_run racing in
    parallel.

    Identifying the active run without the singleton: the partial
    unique index ``bootstrap_runs_one_running_idx`` guarantees at most
    one row with ``status='running'``. We resolve via that predicate
    rather than dereferencing ``bootstrap_state.last_run_id`` so the
    cancel path never depends on the singleton being in sync.

    Steps:

    1. ``SELECT id FROM bootstrap_runs WHERE status='running'
       FOR UPDATE`` — pins the active run; the partial-unique index
       guarantees ≤1 row. No row → BootstrapNotRunning (API → 409).
    2. ``request_stop`` writes the ``process_stop_requests`` row with
       ``target_run_kind='bootstrap_run'`` and the locked run id.
       Internally wraps the INSERT in a SAVEPOINT so a
       ``UniqueViolation`` (active stop already pending) rolls back
       cleanly without poisoning the outer transaction.
    3. UPDATE ``bootstrap_runs.cancel_requested_at = now()`` for the
       worker's fast-path observation.

    Returns the cancelled run id.

    Raises:
        BootstrapNotRunning: nothing to cancel.
        StopAlreadyPendingError: an active stop is already pending
            for this run (operator double-clicked).
    """
    with conn.transaction():
        run_row = conn.execute(
            """
            SELECT id FROM bootstrap_runs
             WHERE status = 'running'
             FOR UPDATE
            """,
        ).fetchone()
        if run_row is None:
            raise BootstrapNotRunning("no running bootstrap_runs row to cancel")
        run_id: int = run_row[0]

        # Insert the stop signal. ``request_stop`` raises
        # StopAlreadyPendingError on partial-unique violation; the
        # exception escapes the inner SAVEPOINT cleanly so the outer
        # tx stays usable.
        request_stop(
            conn,
            process_id="bootstrap",
            mechanism="bootstrap",
            target_run_kind="bootstrap_run",
            target_run_id=run_id,
            mode=mode,
            requested_by_operator_id=requested_by_operator_id,
        )

        update_cur = conn.execute(
            """
            UPDATE bootstrap_runs
               SET cancel_requested_at = now()
             WHERE id = %(run_id)s
            """,
            {"run_id": run_id},
        )
        # Single-row UPDATE: if rowcount is 0 the run row vanished
        # between our FOR UPDATE and now (impossible — we hold the
        # lock — but guard against silent no-ops per prevention-log
        # "UPDATE-by-PK helpers must assert rowcount").
        if update_cur.rowcount != 1:
            raise RuntimeError(
                f"cancel_run: expected 1 bootstrap_runs row for run_id={run_id}, got rowcount={update_cur.rowcount}"
            )

    return run_id


def mark_run_cancelled(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
    notes_line: str = "cancelled by operator",
) -> None:
    """Transition the bootstrap run to the terminal ``cancelled`` state.

    Called from the orchestrator after observing the stop signal at a
    cancel checkpoint, AND from boot recovery on a jobs restart, AND
    from finalize_run when cancel_requested_at is non-null.

    Pre-state contract (Codex pre-push round 1 WARNING W2 —
    UPDATE-by-PK rowcount): we read the current status under
    ``FOR UPDATE`` first.

    * ``running``           → transition to ``cancelled`` (the work).
    * ``cancelled``         → no-op (true idempotency for the
                              dispatcher → finalize_run double-call).
    * ``complete`` /
      ``partial_error``     → raise; cancelling a finalised run is a
                              programming error, not a benign no-op,
                              and silently masking it would let the
                              singleton state drift from the run row.
    * row missing           → raise.
    """
    with conn.transaction():
        run_meta = conn.execute(
            """
            SELECT status FROM bootstrap_runs
             WHERE id = %(run_id)s
             FOR UPDATE
            """,
            {"run_id": run_id},
        ).fetchone()
        if run_meta is None:
            raise RuntimeError(f"mark_run_cancelled: bootstrap_runs row {run_id} not found")
        current_status = run_meta[0]
        if current_status == "cancelled":
            return
        if current_status != "running":
            raise RuntimeError(
                f"mark_run_cancelled: bootstrap_runs row {run_id} is in terminal "
                f"state {current_status!r}; cannot cancel a finalised run"
            )

        # PR3c #1093: operator cancel-mid-run sweeps running + pending
        # stages to ``cancelled`` (not ``error``) so the Timeline can
        # tone gray rather than red. Genuine error stages stay red. The
        # ``cancelled by operator`` reason still lands in ``last_error``
        # for audit clarity.
        conn.execute(
            """
            UPDATE bootstrap_stages
               SET status       = 'cancelled',
                   completed_at = now(),
                   last_error   = COALESCE(last_error, %(reason)s)
             WHERE bootstrap_run_id = %(run_id)s
               AND status           = 'running'
            """,
            {"run_id": run_id, "reason": notes_line},
        )
        conn.execute(
            """
            UPDATE bootstrap_stages
               SET status       = 'cancelled',
                   completed_at = now(),
                   last_error   = %(reason)s
             WHERE bootstrap_run_id = %(run_id)s
               AND status           = 'pending'
            """,
            {"run_id": run_id, "reason": notes_line},
        )
        conn.execute(
            """
            UPDATE bootstrap_runs
               SET status       = 'cancelled',
                   completed_at = now(),
                   notes        = TRIM(BOTH E'\n' FROM
                                       COALESCE(notes, '') || E'\n' || %(reason)s)
             WHERE id     = %(run_id)s
               AND status = 'running'
            """,
            {"run_id": run_id, "reason": notes_line},
        )
        conn.execute(
            """
            UPDATE bootstrap_state
               SET status            = 'cancelled',
                   last_completed_at = now()
             WHERE id          = 1
               AND last_run_id = %(run_id)s
               AND status      = 'running'
            """,
            {"run_id": run_id},
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
      - Latest ``bootstrap_runs`` row →
          * ``cancelled`` if ``cancel_requested_at IS NOT NULL`` (an
            operator cancel that the worker never observed before
            jobs restarted — Codex round 2 R2-B3 + spec §sql/136
            "boot recovery handles cancelled");
          * ``partial_error`` otherwise.
      - ``bootstrap_state`` → matching terminal status.

    All in one transaction. Idempotent on a state that is not
    ``running``; returns True if a sweep occurred, False otherwise.

    PR3c #1093: when the boot-recovery sweep observes
    ``cancel_requested_at IS NOT NULL`` (operator clicked Cancel
    before the jobs process died), running + pending stages are swept
    to ``cancelled`` rather than ``error`` so the Timeline tones them
    gray (operator-driven termination) instead of red (genuine
    failure).
    """
    with conn.transaction():
        state = conn.execute("SELECT status, last_run_id FROM bootstrap_state WHERE id = 1 FOR UPDATE").fetchone()
        if state is None or state[0] != "running":
            return False
        last_run_id = state[1]
        if last_run_id is None:
            conn.execute("UPDATE bootstrap_state SET status = 'partial_error' WHERE id = 1")
            return True

        # Distinguish operator-cancel-then-restart from generic crash.
        run_meta = conn.execute(
            """
            SELECT cancel_requested_at IS NOT NULL
              FROM bootstrap_runs
             WHERE id = %(run_id)s
            """,
            {"run_id": last_run_id},
        ).fetchone()
        cancel_requested = bool(run_meta[0]) if run_meta is not None else False

        # PR3c #1093: cancel-driven boot recovery writes ``cancelled``;
        # generic crash recovery still writes ``error`` (server died,
        # not operator intent). Branch on cancel_requested up-front so
        # both UPDATEs use the matching status value.
        terminal_status = "cancelled" if cancel_requested else "error"
        running_reason = (
            "cancelled by operator before jobs restart" if cancel_requested else "jobs process restarted mid-run"
        )
        pending_reason = (
            "cancelled by operator before jobs restart"
            if cancel_requested
            else "orchestrator did not dispatch before restart"
        )

        conn.execute(
            """
            UPDATE bootstrap_stages
               SET status       = %(status)s,
                   completed_at = now(),
                   last_error   = %(reason)s
             WHERE bootstrap_run_id = %(run_id)s
               AND status           = 'running'
            """,
            {"run_id": last_run_id, "status": terminal_status, "reason": running_reason},
        )
        conn.execute(
            """
            UPDATE bootstrap_stages
               SET status       = %(status)s,
                   completed_at = now(),
                   last_error   = %(reason)s
             WHERE bootstrap_run_id = %(run_id)s
               AND status           = 'pending'
            """,
            {"run_id": last_run_id, "status": terminal_status, "reason": pending_reason},
        )

        if cancel_requested:
            # Operator clicked Cancel; jobs restarted before the
            # worker observed. Honour the cancel intent rather than
            # masking it as partial_error.
            conn.execute(
                """
                UPDATE bootstrap_runs
                   SET status       = 'cancelled',
                       completed_at = now(),
                       notes        = TRIM(BOTH E'\n' FROM
                                           COALESCE(notes, '') || E'\n' ||
                                           'terminated by operator before jobs restart')
                 WHERE id = %(run_id)s
                """,
                {"run_id": last_run_id},
            )
            conn.execute(
                """
                UPDATE bootstrap_state
                   SET status            = 'cancelled',
                       last_completed_at = now()
                 WHERE id = 1
                """
            )
        else:
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
    "BootstrapNoPriorRun",
    "BootstrapNotResettable",
    "BootstrapNotRunning",
    "BootstrapStageCancelled",
    "BootstrapState",
    "BootstrapStatus",
    "Lane",
    "RunSnapshot",
    "RunStatus",
    "StageRow",
    "StageSpec",
    "StageStatus",
    "StopAlreadyPendingError",
    "cancel_run",
    "finalize_run",
    "force_mark_complete",
    "mark_run_cancelled",
    "mark_stage_blocked",
    "mark_stage_cancelled",
    "mark_stage_error",
    "mark_stage_running",
    "mark_stage_skipped",
    "mark_stage_success",
    "read_latest_run_with_stages",
    "read_state",
    "reap_orphaned_running",
    "reset_failed_stages_for_retry",
    "start_run",
]
