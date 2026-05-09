"""Bootstrap → ProcessRow adapter for the admin control hub.

Issue #1071 (umbrella #1064).
Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §Adapter map / bootstrap.

Translates the four bootstrap source tables into a single
``ProcessRow`` with ``mechanism='bootstrap'``, ``process_id='bootstrap'``,
``lane='setup'``. The 17 underlying stages are NOT separate process
rows — they live in the bootstrap row's drill-in (PR7).

Auto-hide-on-retry rule (spec §Auto-hide-on-retry rule): bootstrap is
stage-based, so the only failure-with-retry path is
``state='partial_error'`` followed by a retry-failed click which flips
state back to ``running``. The adapter therefore does NOT need to peek
at ``pending_job_requests`` for bootstrap — when a retry is in flight,
``bootstrap_state.status='running'`` and ``last_n_errors`` is naturally
empty (we only surface errors when status='failed').

PR3 leaves ``ProcessRow.watermark = None``. PR4 wires a stage-index
cursor.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg
import psycopg.rows

from app.services.processes import (
    ActiveRunSummary,
    ErrorClassSummary,
    ProcessRow,
    ProcessRunSummary,
    ProcessStatus,
    RunStatus,
)

logger = logging.getLogger(__name__)


_PROCESS_ID = "bootstrap"
_DISPLAY_NAME = "First-install bootstrap"


# Maps the bootstrap_state.status enum into the ProcessRow status
# vocabulary (spec §Status semantics). 'partial_error' becomes the
# operator-facing 'failed'; 'pending' becomes 'pending_first_run' so
# the FE can render a different colour while the operator has not yet
# clicked Run.
_STATE_TO_PROCESS_STATUS: dict[str, ProcessStatus] = {
    "pending": "pending_first_run",
    "running": "running",
    "complete": "ok",
    "partial_error": "failed",
    "cancelled": "cancelled",
}


# bootstrap_runs.status maps onto ProcessRunSummary.status. 'partial_error'
# does not exist on ProcessRunSummary — we surface it as 'partial' so the
# downstream FE has a single vocabulary.
_RUN_STATUS_TO_SUMMARY: dict[str, RunStatus] = {
    "complete": "success",
    "partial_error": "partial",
    "cancelled": "cancelled",
}


def _read_state(conn: psycopg.Connection[Any]) -> dict[str, Any] | None:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT status, last_run_id, last_completed_at FROM bootstrap_state WHERE id = 1")
        return cur.fetchone()


def _read_active_run(conn: psycopg.Connection[Any]) -> dict[str, Any] | None:
    """Return the in-flight bootstrap_runs row, if any.

    The partial-unique index ``bootstrap_runs_one_running_idx`` guarantees
    at most one row matches ``status='running'``, so ``LIMIT 1`` is
    defence-in-depth, not a sort-required guard.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT id, triggered_at, cancel_requested_at
              FROM bootstrap_runs
             WHERE status = 'running'
             LIMIT 1
            """
        )
        return cur.fetchone()


def _read_latest_terminal_run(
    conn: psycopg.Connection[Any],
) -> dict[str, Any] | None:
    """Latest finalised bootstrap_runs row.

    Used for ``ProcessRow.last_run`` and to source the ``last_n_errors``
    grouped error list (auto-hide-on-retry rule reads from THIS row, not
    a 7-day window — spec §Auto-hide-on-retry rule).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT id, status, triggered_at, completed_at
              FROM bootstrap_runs
             WHERE status IN ('complete', 'partial_error', 'cancelled')
             ORDER BY id DESC
             LIMIT 1
            """
        )
        return cur.fetchone()


def _read_stage_aggregates(conn: psycopg.Connection[Any], *, run_id: int) -> dict[str, Any]:
    """Aggregate progress + processed counts across a run's stages."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT
                COUNT(*)                                                  AS total_stages,
                COUNT(*) FILTER (
                    WHERE status IN ('success', 'error', 'skipped', 'blocked', 'cancelled')
                )                                                         AS finished_stages,
                COALESCE(SUM(rows_processed), 0)                          AS rows_processed,
                COALESCE(SUM(processed_count), 0)                         AS processed_count
              FROM bootstrap_stages
             WHERE bootstrap_run_id = %(run_id)s
            """,
            {"run_id": run_id},
        )
        row = cur.fetchone()
    if row is None:
        # COUNT/SUM aggregates always return one row even on an empty
        # table; absence implies driver-level corruption. Raise a typed
        # error so it surfaces honestly rather than crashing on
        # ``row[0]`` later (prevention-log "assert as runtime guard"
        # forbidden in service code).
        raise RuntimeError(f"_read_stage_aggregates: aggregate SELECT returned no row for run_id={run_id}")
    return row


def _read_failed_stages(conn: psycopg.Connection[Any], *, run_id: int) -> list[dict[str, Any]]:
    """Per-stage error rows for the latest run, grouped by stage_key.

    bootstrap_stages.last_error is free-text — there is no producer-side
    ``error_class`` like ``job_runs.error_classes`` for scheduled jobs.
    We surface one ``ErrorClassSummary`` per failed stage with
    ``error_class = stage_key`` so operators get per-stage drill granularity
    without inventing a fake taxonomy.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT stage_key, lane, last_error, completed_at
              FROM bootstrap_stages
             WHERE bootstrap_run_id = %(run_id)s
               AND status IN ('error', 'blocked')
               AND completed_at IS NOT NULL
             ORDER BY stage_order ASC
            """,
            {"run_id": run_id},
        )
        return list(cur.fetchall())


def _aggregate_run_skip_reasons(conn: psycopg.Connection[Any], *, run_id: int) -> dict[str, int]:
    """Sum the per-archive ``rows_skipped`` JSONB across one run.

    bootstrap_archive_results.rows_skipped looks like
    ``{"unresolved_cusip": 42, "unresolved_cik": 3}``. Aggregate keys
    across every archive row of the run so the per-run summary mirrors
    the same shape job_runs uses.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT key, SUM(value::bigint)::bigint
              FROM bootstrap_archive_results,
                   LATERAL jsonb_each_text(rows_skipped) AS skips(key, value)
             WHERE bootstrap_run_id = %(run_id)s
             GROUP BY key
            """,
            {"run_id": run_id},
        )
        rows = cur.fetchall()
    return {key: int(total) for key, total in rows}


def _has_pending_full_wash_fence(conn: psycopg.Connection[Any]) -> bool:
    """True if a sql/138 full-wash fence row exists for this process.

    Used to disable Iterate + Full-wash buttons while a fence is held.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM pending_job_requests
             WHERE process_id = %(pid)s
               AND mode       = 'full_wash'
               AND status     IN ('pending', 'claimed', 'dispatched')
             LIMIT 1
            """,
            {"pid": _PROCESS_ID},
        )
        return cur.fetchone() is not None


def _build_active_run(active_row: dict[str, Any], aggregates: dict[str, Any]) -> ActiveRunSummary:
    total_stages = int(aggregates["total_stages"])
    finished_stages = int(aggregates["finished_stages"])
    rows_processed = int(aggregates["rows_processed"]) or None
    processed_count = int(aggregates["processed_count"])
    # Prefer producer-side ``processed_count`` (sql/140) when populated; fall
    # back to legacy ``rows_processed`` so the operator always sees motion.
    rows_processed_so_far = processed_count if processed_count > 0 else rows_processed
    progress_units_total = total_stages if total_stages > 0 else None
    progress_units_done = finished_stages if total_stages > 0 else None
    return ActiveRunSummary(
        run_id=int(active_row["id"]),
        started_at=active_row["triggered_at"],
        rows_processed_so_far=rows_processed_so_far,
        progress_units_done=progress_units_done,
        progress_units_total=progress_units_total,
        expected_p95_seconds=None,  # PR8: rolling p95
        is_cancelling=active_row["cancel_requested_at"] is not None,
        is_stale=False,  # PR8
    )


def _build_last_run(
    terminal_row: dict[str, Any],
    aggregates: dict[str, Any],
    skip_reasons: dict[str, int],
    failed_stages: list[dict[str, Any]],
) -> ProcessRunSummary:
    started_at = terminal_row["triggered_at"]
    finished_at = terminal_row["completed_at"]
    duration = (finished_at - started_at).total_seconds() if finished_at else 0.0
    rows_processed = int(aggregates["rows_processed"]) or None
    summary_status = _RUN_STATUS_TO_SUMMARY.get(terminal_row["status"], "partial")
    return ProcessRunSummary(
        run_id=int(terminal_row["id"]),
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration,
        rows_processed=rows_processed,
        rows_skipped_by_reason=skip_reasons,
        rows_errored=len(failed_stages),
        status=summary_status,
        # bootstrap_runs has no operator id on cancellation; the originating
        # operator lives in process_stop_requests.requested_by_operator_id
        # which PR4 surfaces. Leaving None here is honest for PR3.
        cancelled_by_operator_id=None,
    )


def _build_error_summaries(
    failed_stages: list[dict[str, Any]],
) -> tuple[ErrorClassSummary, ...]:
    return tuple(
        ErrorClassSummary(
            error_class=row["stage_key"],
            count=1,
            last_seen_at=row["completed_at"],
            sample_message=(row["last_error"] or "")[:500],
            sample_subject=row["lane"],
        )
        for row in failed_stages
    )


def get_row(conn: psycopg.Connection[Any]) -> ProcessRow | None:
    """Return the bootstrap process row, or ``None`` if state is missing.

    Caller MUST be inside a ``snapshot_read(conn)`` block (REPEATABLE
    READ) so ``bootstrap_state`` and ``bootstrap_runs`` reads see the
    same snapshot — without it a concurrent state transition could
    surface as a row with ``status='ok'`` and a still-running
    ``active_run``.
    """
    state = _read_state(conn)
    if state is None:
        # Migration 129 seeds the singleton; absence means corruption.
        # Caller treats None as "skip this row" rather than crashing the
        # whole snapshot.
        logger.warning("bootstrap_adapter: bootstrap_state singleton row missing")
        return None

    state_status: str = state["status"]
    process_status = _STATE_TO_PROCESS_STATUS.get(state_status, "failed")

    active_row = _read_active_run(conn) if state_status == "running" else None
    terminal_row = _read_latest_terminal_run(conn)

    aggregates_target_run_id: int | None = None
    if active_row is not None:
        aggregates_target_run_id = int(active_row["id"])
    elif terminal_row is not None:
        aggregates_target_run_id = int(terminal_row["id"])

    aggregates: dict[str, Any]
    if aggregates_target_run_id is not None:
        aggregates = _read_stage_aggregates(conn, run_id=aggregates_target_run_id)
    else:
        aggregates = {
            "total_stages": 0,
            "finished_stages": 0,
            "rows_processed": 0,
            "processed_count": 0,
        }

    active_run = _build_active_run(active_row, aggregates) if active_row is not None else None

    last_run: ProcessRunSummary | None = None
    failed_stages: list[dict[str, Any]] = []
    if terminal_row is not None:
        skip_reasons = _aggregate_run_skip_reasons(conn, run_id=int(terminal_row["id"]))
        failed_stages = _read_failed_stages(conn, run_id=int(terminal_row["id"]))
        last_run = _build_last_run(terminal_row, aggregates, skip_reasons, failed_stages)

    fence_held = _has_pending_full_wash_fence(conn)
    last_n_errors: tuple[ErrorClassSummary, ...] = ()
    if process_status == "failed":
        last_n_errors = _build_error_summaries(failed_stages)

    return ProcessRow(
        process_id=_PROCESS_ID,
        display_name=_DISPLAY_NAME,
        lane="setup",
        mechanism="bootstrap",
        status=process_status,
        last_run=last_run,
        active_run=active_run,
        cadence_human="on demand",
        cadence_cron=None,
        next_fire_at=None,
        watermark=None,  # PR4 wires a stage_index cursor
        # Iterate = retry-failed; only meaningful when something failed
        # OR was cancelled mid-flight (resume from the failed stage).
        can_iterate=(state_status in ("partial_error", "cancelled")) and not fence_held,
        # Full-wash = wipe + start_run from scratch; legal except mid-run.
        can_full_wash=(state_status != "running") and not fence_held,
        can_cancel=(state_status == "running"),
        last_n_errors=last_n_errors,
    )


def list_rows(conn: psycopg.Connection[Any]) -> list[ProcessRow]:
    """Return the (single-element) list of bootstrap process rows.

    The list shape mirrors the other adapters so the handler can call
    every adapter the same way.
    """
    row = get_row(conn)
    return [row] if row is not None else []


__all__ = ["get_row", "list_rows"]
