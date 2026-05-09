"""Admin control hub: ``/system/processes`` API surface.

Issue #1071 (umbrella #1064).
Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §API surface.

Read endpoints render the unified ``ProcessRow`` envelope by composing
the per-mechanism adapters under one ``snapshot_read(conn)``
(REPEATABLE READ) so the cross-adapter snapshot is consistent.

Trigger + cancel endpoints implement the preconditions matrix
(spec §Trigger preconditions matrix) — 409 with a structured
``{"reason": ...}`` body on every prereq failure so the FE renders the
disabled-button tooltip without inventing the cause.

Watermark surfacing + per-mechanism watermark reset on full_wash are
deferred to PR4. PR3 inserts the durable fence row (sql/138) on
full_wash so future iterates / scheduled fires correctly self-skip;
PR4 wires the actual watermark reset semantics.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

import psycopg
import psycopg.errors
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, status
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.db.snapshot import snapshot_read
from app.jobs.runtime import VALID_JOB_NAMES
from app.services.bootstrap_orchestrator import JOB_BOOTSTRAP_ORCHESTRATOR
from app.services.bootstrap_state import (
    BootstrapNotRunning,
)
from app.services.bootstrap_state import (
    cancel_run as bootstrap_cancel_run,
)
from app.services.ops_monitor import get_kill_switch_status
from app.services.process_stop import (
    NoActiveRunError,
    StopAlreadyPendingError,
    acquire_prelude_lock,
    request_stop,
)
from app.services.processes import (
    ActiveRunSummary,
    ErrorClassSummary,
    ProcessLane,
    ProcessMechanism,
    ProcessRow,
    ProcessRunSummary,
    ProcessSnapshot,
    ProcessStatus,
    ProcessWatermark,
    RunStatus,
    bootstrap_adapter,
    ingest_sweep_adapter,
    scheduled_adapter,
)
from app.services.sync_orchestrator.dispatcher import NOTIFY_CHANNEL

logger = logging.getLogger(__name__)


router = APIRouter(
    prefix="/system/processes",
    tags=["system", "processes"],
    dependencies=[Depends(require_session_or_service_token)],
)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class ErrorClassSummaryResponse(BaseModel):
    error_class: str
    count: int
    last_seen_at: datetime
    sample_message: str
    sample_subject: str | None


class ProcessRunSummaryResponse(BaseModel):
    run_id: int
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    rows_processed: int | None
    rows_skipped_by_reason: dict[str, int]
    rows_errored: int
    status: RunStatus
    cancelled_by_operator_id: UUID | None


class ActiveRunSummaryResponse(BaseModel):
    run_id: int
    started_at: datetime
    rows_processed_so_far: int | None
    progress_units_done: int | None
    progress_units_total: int | None
    expected_p95_seconds: float | None
    is_cancelling: bool
    is_stale: bool


class ProcessWatermarkResponse(BaseModel):
    cursor_kind: Literal[
        "filed_at",
        "accession",
        "instrument_offset",
        "stage_index",
        "epoch",
        "atom_etag",
    ]
    cursor_value: str
    human: str
    last_advanced_at: datetime


class ProcessRowResponse(BaseModel):
    process_id: str
    display_name: str
    lane: ProcessLane
    mechanism: ProcessMechanism
    status: ProcessStatus
    last_run: ProcessRunSummaryResponse | None
    active_run: ActiveRunSummaryResponse | None
    cadence_human: str
    cadence_cron: str | None
    next_fire_at: datetime | None
    watermark: ProcessWatermarkResponse | None
    can_iterate: bool
    can_full_wash: bool
    can_cancel: bool
    last_n_errors: list[ErrorClassSummaryResponse]


class ProcessListResponse(BaseModel):
    """List response wrapper.

    ``partial=True`` flips on when at least one adapter raised — the FE
    renders a banner ("ingest sweep telemetry unavailable") while still
    showing the lanes that succeeded. Spec §Failure-mode invariants.
    """

    rows: list[ProcessRowResponse]
    partial: bool


class TriggerRequest(BaseModel):
    mode: Literal["iterate", "full_wash"]


class TriggerResponse(BaseModel):
    request_id: int | None
    mode: Literal["iterate", "full_wash"]


class CancelRequest(BaseModel):
    mode: Literal["cooperative", "terminate"]


class CancelResponse(BaseModel):
    target_run_kind: Literal["bootstrap_run", "job_run", "sync_run"]
    target_run_id: int


# ---------------------------------------------------------------------------
# Envelope -> Pydantic conversion
# ---------------------------------------------------------------------------


def _convert_run(summary: ProcessRunSummary) -> ProcessRunSummaryResponse:
    return ProcessRunSummaryResponse(
        run_id=summary.run_id,
        started_at=summary.started_at,
        finished_at=summary.finished_at,
        duration_seconds=summary.duration_seconds,
        rows_processed=summary.rows_processed,
        rows_skipped_by_reason=dict(summary.rows_skipped_by_reason),
        rows_errored=summary.rows_errored,
        status=summary.status,
        cancelled_by_operator_id=summary.cancelled_by_operator_id,
    )


def _convert_active_run(active: ActiveRunSummary) -> ActiveRunSummaryResponse:
    return ActiveRunSummaryResponse(
        run_id=active.run_id,
        started_at=active.started_at,
        rows_processed_so_far=active.rows_processed_so_far,
        progress_units_done=active.progress_units_done,
        progress_units_total=active.progress_units_total,
        expected_p95_seconds=active.expected_p95_seconds,
        is_cancelling=active.is_cancelling,
        is_stale=active.is_stale,
    )


def _convert_watermark(wm: ProcessWatermark) -> ProcessWatermarkResponse:
    return ProcessWatermarkResponse(
        cursor_kind=wm.cursor_kind,
        cursor_value=wm.cursor_value,
        human=wm.human,
        last_advanced_at=wm.last_advanced_at,
    )


def _convert_error(err: ErrorClassSummary) -> ErrorClassSummaryResponse:
    return ErrorClassSummaryResponse(
        error_class=err.error_class,
        count=err.count,
        last_seen_at=err.last_seen_at,
        sample_message=err.sample_message,
        sample_subject=err.sample_subject,
    )


def _convert_row(row: ProcessRow) -> ProcessRowResponse:
    return ProcessRowResponse(
        process_id=row.process_id,
        display_name=row.display_name,
        lane=row.lane,
        mechanism=row.mechanism,
        status=row.status,
        last_run=_convert_run(row.last_run) if row.last_run is not None else None,
        active_run=(_convert_active_run(row.active_run) if row.active_run is not None else None),
        cadence_human=row.cadence_human,
        cadence_cron=row.cadence_cron,
        next_fire_at=row.next_fire_at,
        watermark=(_convert_watermark(row.watermark) if row.watermark is not None else None),
        can_iterate=row.can_iterate,
        can_full_wash=row.can_full_wash,
        can_cancel=row.can_cancel,
        last_n_errors=[_convert_error(e) for e in row.last_n_errors],
    )


# ---------------------------------------------------------------------------
# Snapshot read
# ---------------------------------------------------------------------------


def _gather_snapshot(conn: psycopg.Connection[Any]) -> ProcessSnapshot:
    """Compose every adapter's rows under one REPEATABLE READ snapshot.

    A failure in any single adapter does NOT 500 the page — that
    mechanism's rows are omitted and ``partial=True`` flags the
    envelope so the FE renders a banner. Spec §Failure-mode invariants.
    Adapter exception text is logged at error level only (never
    surfaced to the operator response — prevention-log #86).
    """
    rows: list[ProcessRow] = []
    partial = False
    with snapshot_read(conn):
        for adapter_name, adapter in (
            ("bootstrap", bootstrap_adapter),
            ("scheduled_job", scheduled_adapter),
            ("ingest_sweep", ingest_sweep_adapter),
        ):
            try:
                rows.extend(adapter.list_rows(conn))
            except Exception:
                logger.exception(
                    "processes: %s adapter raised; omitting from snapshot",
                    adapter_name,
                )
                partial = True
    return ProcessSnapshot(rows=tuple(rows), partial=partial)


# ---------------------------------------------------------------------------
# Process resolution
# ---------------------------------------------------------------------------


def _resolve_mechanism(process_id: str) -> ProcessMechanism | None:
    """Return the mechanism that owns ``process_id``, or None if unknown.

    process_id == 'bootstrap' → bootstrap mechanism.
    process_id == any name in ``VALID_JOB_NAMES`` → scheduled_job.
    Anything else (PR6: ingest sweeps) → None for now.
    """
    if process_id == "bootstrap":
        return "bootstrap"
    if process_id in VALID_JOB_NAMES:
        return "scheduled_job"
    return None


def _adapter_for(mechanism: ProcessMechanism) -> Any:
    if mechanism == "bootstrap":
        return bootstrap_adapter
    if mechanism == "scheduled_job":
        return scheduled_adapter
    return ingest_sweep_adapter


# ---------------------------------------------------------------------------
# Read endpoints
# ---------------------------------------------------------------------------


@router.get("", response_model=ProcessListResponse)
def list_processes(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> ProcessListResponse:
    snapshot = _gather_snapshot(conn)
    return ProcessListResponse(rows=[_convert_row(r) for r in snapshot.rows], partial=snapshot.partial)


@router.get("/{process_id}", response_model=ProcessRowResponse)
def get_process(
    process_id: str = Path(..., min_length=1),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> ProcessRowResponse:
    mechanism = _resolve_mechanism(process_id)
    if mechanism is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown process: {process_id!r}",
        )
    adapter = _adapter_for(mechanism)
    with snapshot_read(conn):
        if mechanism == "bootstrap":
            row = adapter.get_row(conn)
        else:
            row = adapter.get_row(conn, process_id=process_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"process {process_id!r} unavailable",
        )
    return _convert_row(row)


@router.get("/{process_id}/runs", response_model=list[ProcessRunSummaryResponse])
def list_process_runs(
    process_id: str = Path(..., min_length=1),
    days: int = Query(default=7, ge=1, le=90),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> list[ProcessRunSummaryResponse]:
    mechanism = _resolve_mechanism(process_id)
    if mechanism is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown process: {process_id!r}",
        )
    if mechanism == "bootstrap":
        # Bootstrap History tab is wired in PR7's drawer; PR3 returns empty
        # so the endpoint contract holds and the FE doesn't 404.
        return []
    adapter = _adapter_for(mechanism)
    runs = adapter.list_runs(conn, process_id=process_id, days=days)
    return [_convert_run(r) for r in runs]


@router.get(
    "/{process_id}/runs/{run_id}/errors",
    response_model=list[ErrorClassSummaryResponse],
)
def list_process_run_errors(
    process_id: str = Path(..., min_length=1),
    run_id: int = Path(..., ge=1),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> list[ErrorClassSummaryResponse]:
    mechanism = _resolve_mechanism(process_id)
    if mechanism is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown process: {process_id!r}",
        )
    if mechanism == "bootstrap":
        # Bootstrap per-stage errors live on the row's drill-in (PR7).
        return []
    adapter = _adapter_for(mechanism)
    errors = adapter.list_run_errors(conn, process_id=process_id, run_id=run_id)
    return [_convert_error(e) for e in errors]


# ---------------------------------------------------------------------------
# Trigger / cancel endpoints
# ---------------------------------------------------------------------------


def _conflict(reason: str, *, advice: str | None = None) -> HTTPException:
    """Build a 409 with a structured ``detail = {reason, advice?}`` body.

    FastAPI serialises ``HTTPException.detail`` verbatim to the response
    body when it is a dict, so the FE can read ``error.detail.reason``
    without parsing strings (prevention-log #86 — no driver text in
    response bodies).
    """
    body: dict[str, str] = {"reason": reason}
    if advice is not None:
        body["advice"] = advice
    return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=body)


def _identify_requestor(request: Request) -> tuple[str | None, UUID | None]:
    """Return (string-form requested_by, UUID operator_id-or-None).

    Mirrors the helper in ``app/api/jobs.py`` but additionally extracts a
    typed UUID for the cancel path (process_stop_requests has a UUID FK
    on ``operators.operator_id``). Service-token paths return ``None``
    for the UUID — the audit row records the actor via ``requested_by``
    string only.
    """
    operator_id = getattr(request.state, "operator_id", None)
    if operator_id is not None:
        try:
            uuid_val = UUID(str(operator_id))
        except ValueError:
            uuid_val = None
        return f"operator:{operator_id}", uuid_val
    if getattr(request.state, "service_token", False):
        return "service-token", None
    return "unknown", None


def _kill_switch_active(conn: psycopg.Connection[Any]) -> bool:
    return bool(get_kill_switch_status(conn).get("is_active", True))


def _has_pending_full_wash_fence(conn: psycopg.Connection[Any], *, process_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM pending_job_requests
             WHERE process_id = %s
               AND mode       = 'full_wash'
               AND status     IN ('pending', 'claimed', 'dispatched')
             LIMIT 1
            """,
            (process_id,),
        )
        return cur.fetchone() is not None


def _has_inflight_manual_job(conn: psycopg.Connection[Any], *, job_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM pending_job_requests
             WHERE request_kind = 'manual_job'
               AND job_name     = %s
               AND status       IN ('pending', 'claimed', 'dispatched')
             LIMIT 1
            """,
            (job_name,),
        )
        return cur.fetchone() is not None


def _bootstrap_state_status(conn: psycopg.Connection[Any]) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM bootstrap_state WHERE id = 1")
        row = cur.fetchone()
    if row is None:
        return None
    return str(row[0])


def _check_bootstrap_preconditions(conn: psycopg.Connection[Any], *, mode: Literal["iterate", "full_wash"]) -> None:
    """Raise 409 if bootstrap trigger preconditions are not met.

    Caller MUST already hold the per-process advisory lock so the
    fence-check + INSERT in the trigger handler are atomic against any
    concurrent prelude / trigger.
    """
    if _kill_switch_active(conn):
        raise _conflict("kill_switch_active", advice="deactivate kill switch")
    state = _bootstrap_state_status(conn)
    if state is None:
        raise _conflict(
            "bootstrap_state_missing",
            advice="run sql/129 migration",
        )
    if state == "running":
        raise _conflict("bootstrap_already_running", advice="cancel first or wait for completion")
    # Fence check FIRST so an iterate-with-fence-active trigger reports
    # the spec-aligned ``full_wash_already_pending`` reason (which the
    # FE renders as "wait for the active full-wash"), rather than the
    # less-specific ``iterate_already_pending`` it would get from the
    # inflight check that follows. PR #1072 review WARNING.
    if _has_pending_full_wash_fence(conn, process_id="bootstrap"):
        raise _conflict("full_wash_already_pending", advice="wait for the active full-wash")
    if _has_inflight_manual_job(conn, job_name=JOB_BOOTSTRAP_ORCHESTRATOR):
        # A queued bootstrap trigger is awaiting dispatch. Without this
        # check, the second trigger could land while bootstrap_state is
        # still its pre-run value (the orchestrator hasn't transitioned
        # yet), the listener would dispatch both, and the orchestrator
        # would reject the second with BootstrapAlreadyRunning — wasted
        # round-trip.
        raise _conflict(
            "iterate_already_pending",
            advice="a manual bootstrap trigger is already pending",
        )
    if mode == "iterate" and state not in ("partial_error", "cancelled"):
        # Iterate = retry-failed; only meaningful from a failed/cancelled
        # state. From 'pending' / 'complete' there is nothing to resume.
        raise _conflict(
            "bootstrap_not_resumable",
            advice=(f"iterate is retry-failed; current state is {state!r}, no failed stages to resume"),
        )


def _check_scheduled_job_preconditions(conn: psycopg.Connection[Any], *, job_name: str) -> None:
    """Raise 409 if scheduled-job trigger preconditions are not met.

    The bootstrap-gate (``_bootstrap_complete``) check is left to the
    job's own prerequisite at fire time — the trigger endpoint only
    enforces the universal preconditions (kill_switch, dedup, fence).
    Trying to short-circuit a gated job here would duplicate the
    job-level prerequisite and risk drift (e.g. a future pause flag).

    Caller MUST already hold the per-process advisory lock so two
    concurrent iterate POSTs for the same job_name serialise — the
    second one's ``_has_inflight_manual_job`` check sees the first's
    committed row and 409s. (sql/138 only has a partial UNIQUE for
    ``mode='full_wash'``; iterate dedup relies entirely on this lock.)
    """
    if _kill_switch_active(conn):
        raise _conflict("kill_switch_active", advice="deactivate kill switch")
    # Fence check FIRST per PR #1072 review WARNING — full-wash fence
    # blocks iterate too, and the spec-aligned reason is
    # ``full_wash_already_pending`` (which the FE renders as a different
    # tooltip than plain dedup).
    if _has_pending_full_wash_fence(conn, process_id=job_name):
        raise _conflict("full_wash_already_pending", advice="wait for the active full-wash")
    if _has_inflight_manual_job(conn, job_name=job_name):
        raise _conflict(
            "iterate_already_pending",
            advice="a manual run for this job is already in flight",
        )


def _publish_within_tx(
    conn: psycopg.Connection[Any],
    *,
    job_name: str,
    requested_by: str | None,
    process_id: str,
    mode: Literal["iterate", "full_wash"],
) -> int:
    """INSERT a manual_job request inside the caller's transaction.

    Replaces ``publish_manual_job_request`` (which opens its own conn
    and commits autonomously) for the trigger handler so the fence-check
    + INSERT happen atomically under the per-process advisory lock.
    NOTIFY happens after the surrounding tx commits — Postgres buffers
    NOTIFY payloads until commit so calling ``pg_notify`` inside the tx
    is safe and the listener wakes up at the right moment.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pending_job_requests
                (request_kind, job_name, requested_by, process_id, mode)
            VALUES ('manual_job', %s, %s, %s, %s)
            RETURNING request_id
            """,
            (job_name, requested_by, process_id, mode),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("_publish_within_tx: INSERT...RETURNING produced no row")
        request_id = int(row[0])
        cur.execute("SELECT pg_notify(%s, %s)", (NOTIFY_CHANNEL, str(request_id)))
    return request_id


@router.post("/{process_id}/trigger", response_model=TriggerResponse)
def trigger_process(
    body: TriggerRequest,
    request: Request,
    process_id: str = Path(..., min_length=1),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> TriggerResponse:
    """Iterate / Full-wash trigger.

    Mode-specific dispatch:

    * **bootstrap, iterate** — caller must have a partial_error /
      cancelled state to retry. PR3 enqueues the orchestrator; the
      orchestrator internally calls ``reset_failed_stages_for_retry``
      via the existing ``/system/bootstrap/retry-failed`` code path
      (PR4 may switch to a thinner direct-call once watermark wiring
      lands).
    * **bootstrap, full_wash** — same as the existing ``POST
      /system/bootstrap/run`` shim: enqueues the orchestrator with
      ``mode='full_wash'`` so the durable fence row gates concurrent
      iterates / scheduled fires. Watermark reset (wipe stages → pending)
      is handled by the orchestrator itself.
    * **scheduled_job, iterate** — INSERTs ``pending_job_requests``
      with ``mode='iterate'``. The listener picks it up and dispatches
      via the manual-trigger executor.
    * **scheduled_job, full_wash** — same shape, ``mode='full_wash'``.
      The fence column makes any racing iterate / scheduled fire
      self-skip until the worker completes.

    The watermark reset step that the spec lists for full_wash on SEC
    sources is deferred to PR4 (watermark resolver). PR3 ships the fence
    row + dispatch; PR4 wires the per-mechanism reset.
    """
    mechanism = _resolve_mechanism(process_id)
    if mechanism is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown process: {process_id!r}",
        )
    if mechanism == "ingest_sweep":
        # Stub adapter — no triggers wired in PR3.
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown process: {process_id!r}",
        )

    requested_by, _ = _identify_requestor(request)
    if mechanism == "bootstrap":
        target_process_id = "bootstrap"
        target_job_name = JOB_BOOTSTRAP_ORCHESTRATOR
    else:
        target_process_id = process_id
        target_job_name = process_id

    # Atomic lock + re-check + INSERT (Codex round 7 — spec §Full-wash
    # execution fence). The advisory lock serialises every path that
    # mutates this process's state (full-wash trigger, iterate trigger,
    # scheduled prelude). Without this, two concurrent iterate POSTs
    # could both pass the dedup precheck and double-enqueue, or a
    # scheduled prelude could commit a 'running' job_runs row in the
    # gap between our precheck and INSERT.
    try:
        with conn.transaction():
            acquire_prelude_lock(conn, target_process_id)
            if mechanism == "bootstrap":
                _check_bootstrap_preconditions(conn, mode=body.mode)
            else:
                _check_scheduled_job_preconditions(conn, job_name=target_job_name)
            request_id = _publish_within_tx(
                conn,
                job_name=target_job_name,
                requested_by=requested_by,
                process_id=target_process_id,
                mode=body.mode,
            )
    except psycopg.errors.UniqueViolation as exc:
        # Caught by the partial UNIQUE on
        # ``pending_job_requests_active_full_wash_idx`` — a concurrent
        # full_wash beat us past the in-tx fence-check.
        raise _conflict("full_wash_already_pending", advice="wait for the active full-wash") from exc
    return TriggerResponse(request_id=request_id, mode=body.mode)


def _resolve_active_job_run(conn: psycopg.Connection[Any], *, job_name: str) -> int | None:
    """Lock + return the latest running job_runs row for a scheduled job.

    Caller MUST be inside a ``conn.transaction()`` so the row stays locked
    until the cancel insert + cancel_requested_at update commit.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id
              FROM job_runs
             WHERE job_name = %s
               AND status   = 'running'
             ORDER BY started_at DESC
             LIMIT 1
             FOR UPDATE
            """,
            (job_name,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return int(row[0])


@router.post("/{process_id}/cancel", response_model=CancelResponse)
def cancel_process(
    body: CancelRequest,
    request: Request,
    process_id: str = Path(..., min_length=1),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> CancelResponse:
    """Cooperative / terminate cancel by mechanism.

    Bootstrap routes through the existing
    ``app.services.bootstrap_state.cancel_run`` helper which already
    implements the atomic ``SELECT ... FOR UPDATE`` + stop-row insert
    pattern (PR2 wiring).

    Scheduled jobs lock the latest ``status='running'`` ``job_runs`` row
    in one transaction and call ``request_stop`` with
    ``target_run_kind='job_run'``.

    ``terminate`` mode in v1 is honest — it writes the same stop row as
    cooperative but with ``mode='terminate'``. The worker observes it
    at the next checkpoint and treats it as cooperative; if the worker
    is genuinely stuck, the operator restarts the jobs process and
    boot-recovery sweeps. Spec §Cancel — terminate (escape hatch).
    """
    mechanism = _resolve_mechanism(process_id)
    if mechanism is None or mechanism == "ingest_sweep":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown process: {process_id!r}",
        )

    _, operator_uuid = _identify_requestor(request)

    if mechanism == "bootstrap":
        try:
            run_id = bootstrap_cancel_run(conn, requested_by_operator_id=operator_uuid)
        except BootstrapNotRunning as exc:
            raise _conflict("no_active_run") from exc
        except StopAlreadyPendingError as exc:
            raise _conflict("stop_already_pending") from exc
        # bootstrap_state.cancel_run wraps everything in conn.transaction()
        # which auto-commits on clean exit; ``mode`` is currently always
        # 'cooperative' in the helper. PR3's body validates 'terminate'
        # input but the bootstrap cancel helper does not yet distinguish
        # — PR2 only landed cooperative. The bootstrap cancel runbook
        # (PR10) documents the operator path: cooperative cancel +
        # restart jobs is the equivalent of terminate.
        return CancelResponse(target_run_kind="bootstrap_run", target_run_id=run_id)

    # mechanism == 'scheduled_job'
    with conn.transaction():
        run_id = _resolve_active_job_run(conn, job_name=process_id)
        if run_id is None:
            raise _conflict("no_active_run")
        try:
            request_stop(
                conn,
                process_id=process_id,
                mechanism="scheduled_job",
                target_run_kind="job_run",
                target_run_id=run_id,
                mode=body.mode,
                requested_by_operator_id=operator_uuid,
            )
        except NoActiveRunError as exc:
            raise _conflict("no_active_run") from exc
        except StopAlreadyPendingError as exc:
            raise _conflict("stop_already_pending") from exc
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE job_runs
                   SET cancel_requested_at = now()
                 WHERE run_id = %s
                """,
                (run_id,),
            )
            # Single-row UPDATE-by-PK invariant (prevention-log
            # "Single-row UPDATE silent no-op on missing row" #145):
            # we just held the row under SELECT FOR UPDATE in the same
            # tx, so a rowcount=0 is impossible barring driver-level
            # corruption. Raise rather than silently mismatching the
            # cancel response with reality.
            if cur.rowcount != 1:
                raise RuntimeError(f"cancel: expected 1 job_runs row for run_id={run_id}, got rowcount={cur.rowcount}")
    return CancelResponse(target_run_kind="job_run", target_run_id=run_id)


__all__ = [
    "ActiveRunSummaryResponse",
    "CancelRequest",
    "CancelResponse",
    "ErrorClassSummaryResponse",
    "ProcessListResponse",
    "ProcessRowResponse",
    "ProcessRunSummaryResponse",
    "ProcessWatermarkResponse",
    "TriggerRequest",
    "TriggerResponse",
    "router",
]
