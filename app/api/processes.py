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
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, status
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.db.snapshot import snapshot_read
from app.jobs.runtime import VALID_JOB_NAMES
from app.services.bootstrap_orchestrator import JOB_BOOTSTRAP_ORCHESTRATOR, get_bootstrap_stage_specs
from app.services.bootstrap_state import (
    BootstrapAlreadyRunning,
    BootstrapNotRunning,
)
from app.services.bootstrap_state import (
    cancel_run as bootstrap_cancel_run,
)
from app.services.bootstrap_state import (
    reset_failed_stages_for_retry as bootstrap_reset_failed_stages,
)
from app.services.bootstrap_state import (
    start_run as bootstrap_start_run,
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
from app.services.processes.ingest_sweep_adapter import is_sweep
from app.services.processes.watermarks import (
    acquire_shared_source_locks,
    atom_etag_target_for,
    freshness_source_for,
    jobs_sharing_freshness_source,
    jobs_sharing_manifest_source,
    manifest_source_for,
)
from app.services.sync_orchestrator.dispatcher import NOTIFY_CHANNEL

JOB_ORCHESTRATOR_FULL_SYNC = "orchestrator_full_sync"
BOOTSTRAP_PROCESS_ID = "bootstrap"

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


class OrchestratorDagSyncRunResponse(BaseModel):
    """Latest ``sync_runs`` row surfaced on the orchestrator drill-in DAG tab.

    Issue #1078 (umbrella #1064) — admin control hub PR6.
    """

    sync_run_id: int
    scope: str
    scope_detail: str | None
    trigger: str
    started_at: datetime
    finished_at: datetime | None
    status: Literal["running", "complete", "partial", "failed", "cancelled"]
    layers_planned: int
    layers_done: int
    layers_failed: int
    layers_skipped: int
    error_category: str | None
    cancel_requested_at: datetime | None


class OrchestratorDagLayerResponse(BaseModel):
    """One row from ``sync_layer_progress`` joined to the ``LAYERS`` registry.

    ``display_name`` + ``tier`` come from
    ``app/services/sync_orchestrator/registry.py::LAYERS``; layer rows
    that are NOT in the static registry (defensive for future drift)
    fall back to ``display_name=name`` and ``tier=None``.
    """

    name: str
    display_name: str
    tier: int | None
    status: Literal["pending", "running", "complete", "failed", "skipped", "partial", "cancelled"]
    started_at: datetime | None
    finished_at: datetime | None
    items_total: int | None
    items_done: int | None
    row_count: int | None
    error_category: str | None
    skip_reason: str | None
    error_message: str | None


class OrchestratorDagResponse(BaseModel):
    """DAG drill-in payload for ``orchestrator_full_sync``."""

    sync_run: OrchestratorDagSyncRunResponse | None
    layers: list[OrchestratorDagLayerResponse]


class BootstrapTimelineArchiveResponse(BaseModel):
    """One ``bootstrap_archive_results`` row.

    Issue #1080 (umbrella #1064) — admin control hub PR7.
    """

    archive_name: str
    rows_written: int
    rows_skipped_by_reason: dict[str, int]
    completed_at: datetime


class BootstrapTimelineStageResponse(BaseModel):
    """One ``bootstrap_stages`` row joined to ``get_bootstrap_stage_specs()``.

    ``display_name`` + ``stage_order`` (when known) come from the spec
    catalogue; rows whose ``stage_key`` is NOT in the current catalogue
    (e.g. legacy run from a prior catalogue) fall back to a humanised
    ``stage_key`` and the DB ``stage_order`` value. Mirrors PR6's
    ``LAYERS`` registry-fallback shape.
    """

    stage_key: str
    display_name: str
    stage_order: int
    lane: str
    job_name: str
    status: Literal["pending", "running", "success", "error", "skipped", "blocked"]
    started_at: datetime | None
    completed_at: datetime | None
    last_error: str | None
    rows_processed: int | None
    processed_count: int
    target_count: int | None
    archives: list[BootstrapTimelineArchiveResponse]


class BootstrapTimelineRunResponse(BaseModel):
    """Latest ``bootstrap_runs`` row surfaced in the timeline drill-in."""

    run_id: int
    status: Literal["running", "complete", "partial_error", "cancelled"]
    triggered_at: datetime
    completed_at: datetime | None
    cancel_requested_at: datetime | None


class BootstrapTimelineResponse(BaseModel):
    """Timeline drill-in payload for ``bootstrap``."""

    run: BootstrapTimelineRunResponse | None
    stages: list[BootstrapTimelineStageResponse]


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
    process_id == any sweep registered in ingest_sweep_adapter → ingest_sweep.
    """
    if process_id == "bootstrap":
        return "bootstrap"
    if process_id in VALID_JOB_NAMES:
        return "scheduled_job"
    if is_sweep(process_id):
        return "ingest_sweep"
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


@router.get("/{process_id}/dag", response_model=OrchestratorDagResponse)
def get_orchestrator_dag(
    process_id: str = Path(..., min_length=1),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> OrchestratorDagResponse:
    """DAG drill-in for ``orchestrator_full_sync`` — restricted endpoint.

    Issue #1078 (umbrella #1064) — admin control hub PR6.
    Spec §"Sync-orchestrator surface": orchestrator surfaces as ONE
    scheduled_job row + a custom drill-in tab rendering the 10-LAYERS
    DAG state for the latest run.

    Returns ``{sync_run: null, layers: []}`` when no sync run exists
    yet (e.g. fresh install pre-first-trigger). Returns 404 for any
    process_id other than ``orchestrator_full_sync`` — the endpoint
    is intentionally restricted because no other process has the
    multi-layer DAG drill-in shape in v1.
    """
    if process_id != JOB_ORCHESTRATOR_FULL_SYNC:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DAG drill-in not available for {process_id!r}",
        )

    # Lazy import keeps the registry import out of the API process's
    # cold-start path (the orchestrator package pulls in
    # planner / executor / adapters).
    from app.services.sync_orchestrator.registry import LAYERS

    with snapshot_read(conn):
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT sync_run_id, scope, scope_detail, trigger,
                       started_at, finished_at, status,
                       layers_planned, layers_done, layers_failed, layers_skipped,
                       error_category, cancel_requested_at
                  FROM sync_runs
                 ORDER BY started_at DESC
                 LIMIT 1
                """
            )
            sync_run_row = cur.fetchone()

        if sync_run_row is None:
            return OrchestratorDagResponse(sync_run=None, layers=[])

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT layer_name, status, started_at, finished_at,
                       items_total, items_done, row_count,
                       error_category, skip_reason, error_message
                  FROM sync_layer_progress
                 WHERE sync_run_id = %s
                 ORDER BY layer_name
                """,
                (int(sync_run_row["sync_run_id"]),),
            )
            layer_rows = cur.fetchall()

    sync_run_payload = OrchestratorDagSyncRunResponse(
        sync_run_id=int(sync_run_row["sync_run_id"]),
        scope=str(sync_run_row["scope"]),
        scope_detail=sync_run_row.get("scope_detail"),
        trigger=str(sync_run_row["trigger"]),
        started_at=sync_run_row["started_at"],
        finished_at=sync_run_row.get("finished_at"),
        status=sync_run_row["status"],
        layers_planned=int(sync_run_row["layers_planned"]),
        layers_done=int(sync_run_row["layers_done"]),
        layers_failed=int(sync_run_row["layers_failed"]),
        layers_skipped=int(sync_run_row["layers_skipped"]),
        error_category=sync_run_row.get("error_category"),
        cancel_requested_at=sync_run_row.get("cancel_requested_at"),
    )

    layers_payload: list[OrchestratorDagLayerResponse] = []
    for row in layer_rows:
        layer_name = str(row["layer_name"])
        registry_entry = LAYERS.get(layer_name)
        if registry_entry is not None:
            display_name = registry_entry.display_name
            tier: int | None = registry_entry.tier
        else:
            # Defensive: layer rows that don't match the static registry
            # (e.g. a future composite emit) fall back rather than 500.
            display_name = layer_name
            tier = None
        layers_payload.append(
            OrchestratorDagLayerResponse(
                name=layer_name,
                display_name=display_name,
                tier=tier,
                status=row["status"],
                started_at=row.get("started_at"),
                finished_at=row.get("finished_at"),
                items_total=row.get("items_total"),
                items_done=row.get("items_done"),
                row_count=row.get("row_count"),
                error_category=row.get("error_category"),
                skip_reason=row.get("skip_reason"),
                error_message=row.get("error_message"),
            )
        )

    return OrchestratorDagResponse(sync_run=sync_run_payload, layers=layers_payload)


def _humanise_stage_key(stage_key: str) -> str:
    """Fallback display name for stage_keys not in the current catalogue.

    Used only when a legacy ``bootstrap_stages`` row carries a
    ``stage_key`` that ``get_bootstrap_stage_specs()`` no longer lists
    (catalogue evolution over the install's lifetime). Mirrors PR6's
    ``LAYERS`` defensive fallback at line 526.
    """
    return stage_key.replace("_", " ").strip().title() or stage_key


@router.get("/{process_id}/timeline", response_model=BootstrapTimelineResponse)
def get_bootstrap_timeline(
    process_id: str = Path(..., min_length=1),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> BootstrapTimelineResponse:
    """Bootstrap timeline drill-in — restricted endpoint.

    Issue #1080 (umbrella #1064) — admin control hub PR7.
    Spec §"PR7 — Bootstrap timeline drawer + decommission BootstrapPanel".

    Returns ``{run: null, stages: []}`` (200, NOT 404) when no
    ``bootstrap_runs`` row exists yet (fresh install pre-first-trigger).
    Returns 404 for any process_id other than ``"bootstrap"`` — the
    endpoint is intentionally restricted because no other process has
    the parallel-lane stage tree drill-in shape.
    """
    if process_id != BOOTSTRAP_PROCESS_ID:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Bootstrap timeline not available for {process_id!r}",
        )

    spec_by_key = {spec.stage_key: spec for spec in get_bootstrap_stage_specs()}

    with snapshot_read(conn):
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT id, status, triggered_at, completed_at, cancel_requested_at
                  FROM bootstrap_runs
                 ORDER BY id DESC
                 LIMIT 1
                """
            )
            run_row = cur.fetchone()

        if run_row is None:
            return BootstrapTimelineResponse(run=None, stages=[])

        run_id = int(run_row["id"])

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT stage_key, stage_order, lane, job_name, status,
                       started_at, completed_at, last_error,
                       rows_processed, processed_count, target_count
                  FROM bootstrap_stages
                 WHERE bootstrap_run_id = %s
                 ORDER BY stage_order ASC, stage_key ASC
                """,
                (run_id,),
            )
            stage_rows = cur.fetchall()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT stage_key, archive_name, rows_written, rows_skipped,
                       completed_at
                  FROM bootstrap_archive_results
                 WHERE bootstrap_run_id = %s
                 ORDER BY stage_key ASC, archive_name ASC
                """,
                (run_id,),
            )
            archive_rows = cur.fetchall()

    archives_by_stage: dict[str, list[BootstrapTimelineArchiveResponse]] = {}
    for row in archive_rows:
        # ``rows_skipped`` is JSONB DEFAULT '{}' (sql/130); psycopg3 +
        # ``dict_row`` decodes it directly to a Python dict. Cast values
        # to int so the operator-facing payload is unambiguous (the
        # producer-side `_aggregate_run_skip_reasons` query already
        # casts via ``::bigint`` — keep parity here).
        rows_skipped_raw = row.get("rows_skipped") or {}
        rows_skipped: dict[str, int] = {str(key): int(value) for key, value in rows_skipped_raw.items()}
        stage_key = str(row["stage_key"])
        archives_by_stage.setdefault(stage_key, []).append(
            BootstrapTimelineArchiveResponse(
                archive_name=str(row["archive_name"]),
                rows_written=int(row["rows_written"]),
                rows_skipped_by_reason=rows_skipped,
                completed_at=row["completed_at"],
            )
        )

    stage_payload: list[BootstrapTimelineStageResponse] = []
    for row in stage_rows:
        stage_key = str(row["stage_key"])
        display_name = _humanise_stage_key(stage_key)
        spec = spec_by_key.get(stage_key)
        if spec is not None:
            stage_order = int(spec.stage_order)
            job_name = str(spec.job_name)
        else:
            # Defensive: stage rows that don't match the current catalogue
            # (legacy install on a prior version of the spec list) fall
            # back to the DB columns rather than 500. The DB carries the
            # historical truth — the catalogue is the deployable contract.
            stage_order = int(row["stage_order"])
            job_name = str(row["job_name"])

        stage_payload.append(
            BootstrapTimelineStageResponse(
                stage_key=stage_key,
                display_name=display_name,
                stage_order=stage_order,
                lane=str(row["lane"]),
                job_name=job_name,
                status=row["status"],
                started_at=row.get("started_at"),
                completed_at=row.get("completed_at"),
                last_error=row.get("last_error"),
                rows_processed=row.get("rows_processed"),
                processed_count=int(row.get("processed_count") or 0),
                target_count=row.get("target_count"),
                archives=archives_by_stage.get(stage_key, []),
            )
        )

    run_payload = BootstrapTimelineRunResponse(
        run_id=run_id,
        status=run_row["status"],
        triggered_at=run_row["triggered_at"],
        completed_at=run_row.get("completed_at"),
        cancel_requested_at=run_row.get("cancel_requested_at"),
    )

    return BootstrapTimelineResponse(run=run_payload, stages=stage_payload)


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


def _has_active_job_run(conn: psycopg.Connection[Any], *, job_name: str) -> bool:
    """True if a ``job_runs`` row is currently ``status='running'``.

    The advisory lock serialises preludes only — once a worker has
    started the body of its run, the lock is released. Any trigger
    that arrives during that window must still see the active run and
    refuse rather than (a) double-enqueue an iterate or (b) reset
    watermarks under the running worker's feet (Codex pre-push BLOCKING).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM job_runs
             WHERE job_name = %s
               AND status   = 'running'
             LIMIT 1
            """,
            (job_name,),
        )
        return cur.fetchone() is not None


def _check_scheduled_job_preconditions(conn: psycopg.Connection[Any], *, job_name: str) -> None:
    """Raise 409 if scheduled-job trigger preconditions are not met.

    The bootstrap-gate (``_bootstrap_complete``) check is left to the
    job's own prerequisite at fire time — the trigger endpoint only
    enforces the universal preconditions (kill_switch, dedup, fence,
    active-run).
    Trying to short-circuit a gated job here would duplicate the
    job-level prerequisite and risk drift (e.g. a future pause flag).

    Caller MUST already hold the per-process advisory lock so two
    concurrent iterate POSTs for the same job_name serialise — the
    second one's ``_has_inflight_manual_job`` check sees the first's
    committed row and 409s. (sql/138 only has a partial UNIQUE for
    ``mode='full_wash'``; iterate dedup relies entirely on this lock.)

    Active-run gate (Codex pre-push BLOCKING + spec §"Full-wash
    execution fence" step 4): a scheduled fire that has already
    advanced past the prelude into its body holds no lock and is
    invisible to ``pending_job_requests`` (its trigger row is already
    transitioned to ``completed``/``rejected``). A trigger landing
    while that body is in flight must be refused with
    ``active_run_in_progress`` — full-wash MUST NOT mutate watermark
    state under the running worker, and a second iterate would
    double-enqueue.
    """
    if _kill_switch_active(conn):
        raise _conflict("kill_switch_active", advice="deactivate kill switch")
    # Fence check FIRST per PR #1072 review WARNING — full-wash fence
    # blocks iterate too, and the spec-aligned reason is
    # ``full_wash_already_pending`` (which the FE renders as a different
    # tooltip than plain dedup).
    if _has_pending_full_wash_fence(conn, process_id=job_name):
        raise _conflict("full_wash_already_pending", advice="wait for the active full-wash")
    if _has_active_job_run(conn, job_name=job_name):
        raise _conflict("active_run_in_progress", advice="cancel the active run first")
    if _has_inflight_manual_job(conn, job_name=job_name):
        raise _conflict(
            "iterate_already_pending",
            advice="a manual run for this job is already in flight",
        )


def _check_full_wash_shared_source_clear(
    conn: psycopg.Connection[Any],
    *,
    job_name: str,
) -> None:
    """Refuse a scheduled-job full-wash while a sibling sharing the
    same freshness/manifest source is mid-run.

    Codex pre-push BLOCKING: a full-wash on ``daily_financial_facts``
    resets ``data_freshness_index`` rows for ``sec_xbrl_facts``, which
    is also consumed by ``fundamentals_sync`` and
    ``sec_business_summary_ingest``. The per-job advisory lock + the
    per-job ``_has_active_job_run`` are scoped to ``job_name`` and
    cannot see a sibling running under a different name. Walk the
    registry, take ``_has_active_job_run`` for every sibling sharing
    the same scheduler source, and 409 if any is running.
    """
    siblings: set[str] = set()
    fresh = freshness_source_for(job_name)
    if fresh is not None:
        siblings.update(jobs_sharing_freshness_source(fresh))
    manifest = manifest_source_for(job_name)
    if manifest is not None:
        siblings.update(jobs_sharing_manifest_source(manifest))
    siblings.discard(job_name)
    for sibling in sorted(siblings):
        if _has_active_job_run(conn, job_name=sibling):
            raise _conflict(
                "shared_source_active_run",
                advice=(
                    f"sibling job {sibling!r} consumes the same scheduler source; cancel that run before full-wash"
                ),
            )
        # Codex review BLOCKING: also reject concurrent full-wash POSTs
        # across siblings. The partial UNIQUE on
        # ``pending_job_requests_active_full_wash_idx`` only dedupes by
        # ``process_id``; a sibling's fence row carries a different
        # ``process_id`` and wouldn't hit the index. Probe each sibling's
        # fence row explicitly so two full-washes targeting the same
        # scheduler source cannot race past this gate. (Runtime-prelude
        # sibling-fence enforcement — needed when an APScheduler fire of
        # `fundamentals_sync` arrives during a queued
        # `daily_financial_facts` full-wash — is filed as a follow-up
        # under #1064 since it requires changes to
        # ``app/jobs/runtime.py`` outside PR4's watermark scope.)
        if _has_pending_full_wash_fence(conn, process_id=sibling):
            raise _conflict(
                "shared_source_full_wash_pending",
                advice=(f"sibling job {sibling!r} already has an active full-wash; wait for it to complete"),
            )


def _apply_bootstrap_iterate_reset(conn: psycopg.Connection[Any]) -> None:
    """Bootstrap iterate: reset failed stages + flip state to 'running'.

    Reuses ``bootstrap_state.reset_failed_stages_for_retry`` — the
    same helper the legacy ``POST /system/bootstrap/retry-failed``
    endpoint uses. The helper takes ``SELECT ... FOR UPDATE`` on the
    bootstrap_state singleton internally, so a concurrent ``start_run``
    racing in between the precondition gate and this call surfaces as
    ``BootstrapAlreadyRunning`` and we map it to 409.
    """
    state_row = conn.execute("SELECT last_run_id FROM bootstrap_state WHERE id = 1").fetchone()
    if state_row is None:
        raise _conflict("bootstrap_state_missing", advice="run sql/129 migration")
    last_run_id = state_row[0]
    if last_run_id is None:
        raise _conflict(
            "bootstrap_not_resumable",
            advice="no prior bootstrap run to retry",
        )
    try:
        bootstrap_reset_failed_stages(conn, run_id=int(last_run_id))
    except BootstrapAlreadyRunning as exc:
        raise _conflict("bootstrap_already_running") from exc


def _apply_bootstrap_full_wash_reset(
    conn: psycopg.Connection[Any],
    *,
    operator_uuid: UUID | None,
) -> None:
    """Bootstrap full-wash: create a fresh bootstrap_runs + flip state.

    Codex review BLOCKING: PR3 enqueued the orchestrator after a
    naive ``UPDATE bootstrap_stages`` reset; the orchestrator's
    invoker (``run_bootstrap_orchestrator``) no-ops unless
    ``bootstrap_runs.status == 'running'``. ``start_run`` is the same
    helper the legacy ``POST /system/bootstrap/run`` endpoint calls;
    it inserts a new ``bootstrap_runs`` row, seeds 17 pending
    ``bootstrap_stages`` rows, and flips the singleton state to
    ``running``, all in one transaction.

    The precondition gate already rejected the trigger if state was
    'running'; ``start_run``'s internal ``FOR UPDATE`` is the
    defence-in-depth against a concurrent ``start_run`` slipping in.
    """
    operator_id = str(operator_uuid) if operator_uuid is not None else None
    try:
        bootstrap_start_run(
            conn,
            operator_id=operator_id,
            stage_specs=get_bootstrap_stage_specs(),
        )
    except BootstrapAlreadyRunning as exc:
        raise _conflict("bootstrap_already_running") from exc


def _apply_scheduled_full_wash_reset(
    conn: psycopg.Connection[Any],
    *,
    process_id: str,
) -> None:
    """Reset scheduled-job watermark to mechanism-specific minimum.

    Spec §"Full-wash semantics" step 5 + §"Full-wash execution fence"
    step 5. Caller MUST be inside ``conn.transaction()`` and MUST hold
    the per-process advisory lock so the reset + the durable fence
    INSERT are atomic against any racing scheduled prelude / iterate
    trigger.

    Reset shape:

    * Freshness source — full epoch reset of ``data_freshness_index``
      (clears ``last_known_filed_at``, ``last_known_filing_id``,
      ``expected_next_at``, ``next_recheck_at``, ``state_reason``,
      ``new_filings_since`` and flips ``state`` to ``unknown``).
      Codex pre-push BLOCKING: clearing only ``last_known_filed_at``
      leaves the prior filing_id pointer + future poll cadence, so
      the next poll may not run immediately AND ``check_freshness``
      would skip historical filings against the stale filing_id.
    * Manifest source — flips all rows for the source to ``pending``,
      clears ``last_attempted_at`` / ``next_retry_at``. ON CONFLICT
      idempotency on the per-source ingester is the de-dupe layer.
    * Atom ETag target — DELETEs the watermark row. A missing row is
      the "no prior state, do full backfill" signal consumed by
      ``app/services/watermarks.py::get_watermark``.
    * No registered source — no-op. Full-wash on a no-watermark job
      is just "rerun"; the queue INSERT alone is sufficient.

    Bootstrap full-wash is handled separately in ``trigger_process``
    via ``bootstrap_start_run`` so the orchestrator picks up a
    fresh ``bootstrap_runs`` row + ``bootstrap_state.status='running'``
    on dispatch (Codex review BLOCKING — the orchestrator no-ops
    unless ``bootstrap_runs.status='running'``).
    """
    freshness_source = freshness_source_for(process_id)
    if freshness_source is not None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE data_freshness_index
                   SET last_known_filed_at  = NULL,
                       last_known_filing_id = NULL,
                       expected_next_at     = NULL,
                       next_recheck_at      = NULL,
                       state                = 'unknown',
                       state_reason         = NULL,
                       new_filings_since    = 0
                 WHERE source = %s
                """,
                (freshness_source,),
            )
    manifest_source = manifest_source_for(process_id)
    if manifest_source is not None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sec_filing_manifest
                   SET ingest_status   = 'pending',
                       last_attempted_at = NULL,
                       next_retry_at   = NULL
                 WHERE source = %s
                """,
                (manifest_source,),
            )
    atom_target = atom_etag_target_for(process_id)
    if atom_target is not None:
        atom_source, atom_key = atom_target
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM external_data_watermarks WHERE source = %s AND key = %s",
                (atom_source, atom_key),
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
        # #1078 — admin control hub PR6. Sweeps are READ-ONLY surfaces:
        # the operator triggers / cancels via the underlying scheduled
        # job (e.g. ``sec_filing_documents_ingest`` for the Form 4
        # sweep). Source-level iterate / full_wash deferred to v2.
        raise _conflict(
            "trigger_not_supported",
            advice="trigger via the underlying scheduled job",
        )

    requested_by, operator_uuid = _identify_requestor(request)
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
            # Codex pre-push round 7 BLOCKING: per-process advisory lock
            # only serialises operations under the same ``process_id``.
            # Multi-job shared scheduler sources (e.g. XBRL) need a
            # source-keyed lock so a sibling prelude waits for the
            # full-wash trigger's fence INSERT to commit before it can
            # query and proceed.
            if mechanism == "scheduled_job":
                acquire_shared_source_locks(conn, process_id=target_process_id)
            if mechanism == "bootstrap":
                _check_bootstrap_preconditions(conn, mode=body.mode)
                # Bootstrap iterate / full_wash both need to flip the
                # underlying state machine BEFORE the orchestrator is
                # dispatched — the invoker no-ops unless the latest
                # bootstrap_runs.status == 'running' (orchestrator
                # `read_latest_run_with_stages` short-circuit). Codex
                # review BLOCKING: PR3 enqueued without flipping state,
                # so the orchestrator quietly returned without doing
                # the requested work. start_run / reset_failed_stages
                # both flip bootstrap_state.status + bootstrap_runs.
                if body.mode == "iterate":
                    _apply_bootstrap_iterate_reset(conn)
                else:
                    _apply_bootstrap_full_wash_reset(conn, operator_uuid=operator_uuid)
            else:
                _check_scheduled_job_preconditions(conn, job_name=target_job_name)
                if body.mode == "full_wash":
                    # Refuse if any sibling job sharing the freshness
                    # or manifest source is mid-run — full-wash mutates
                    # source-scoped scheduler rows, not job-scoped
                    # ones, so a sibling holds no per-job advisory lock
                    # under our key (Codex review BLOCKING).
                    _check_full_wash_shared_source_clear(conn, job_name=target_job_name)
                    _apply_scheduled_full_wash_reset(conn, process_id=target_process_id)
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


def _resolve_active_sync_run(conn: psycopg.Connection[Any]) -> int | None:
    """Lock + return the latest running sync_runs row.

    Issue #1078 (umbrella #1064) — admin control hub PR6.
    Spec §"Cancel — cooperative" — orchestrator_full_sync cancel writes
    ``target_run_kind='sync_run'`` against the locked sync_run_id.
    Caller MUST be inside ``conn.transaction()`` so the row stays locked
    until the cancel insert + cancel_requested_at update commit.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sync_run_id
              FROM sync_runs
             WHERE status = 'running'
             ORDER BY started_at DESC
             LIMIT 1
             FOR UPDATE
            """
        )
        row = cur.fetchone()
    if row is None:
        return None
    return int(row[0])


def _cancel_orchestrator_full_sync(
    conn: psycopg.Connection[Any],
    *,
    body: CancelRequest,
    operator_uuid: UUID | None,
) -> CancelResponse:
    """Cancel handler for the ``orchestrator_full_sync`` scheduled job.

    Issue #1078 (umbrella #1064). The orchestrator writes ``sync_runs``,
    not ``job_runs``, so the cancel signal must target
    ``target_run_kind='sync_run'``. The cancel checkpoint inside
    ``_run_layers_loop`` polls ``process_stop_requests`` keyed on
    ``(target_run_kind='sync_run', target_run_id=<sync_run_id>)``.

    ``mechanism`` on ``process_stop_requests`` is ``'scheduled_job'``
    because the orchestrator surfaces as one ``mechanism="scheduled_job"``
    row in the Processes table (spec §"Sync-orchestrator surface").
    """
    with conn.transaction():
        sync_run_id = _resolve_active_sync_run(conn)
        if sync_run_id is None:
            raise _conflict("no_active_run")
        try:
            request_stop(
                conn,
                process_id=JOB_ORCHESTRATOR_FULL_SYNC,
                mechanism="scheduled_job",
                target_run_kind="sync_run",
                target_run_id=sync_run_id,
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
                UPDATE sync_runs
                   SET cancel_requested_at = now()
                 WHERE sync_run_id = %s
                """,
                (sync_run_id,),
            )
            # Single-row UPDATE-by-PK invariant (prevention-log #145):
            # the row was locked under SELECT FOR UPDATE in this same tx.
            if cur.rowcount != 1:
                raise RuntimeError(
                    f"cancel: expected 1 sync_runs row for sync_run_id={sync_run_id}, got rowcount={cur.rowcount}"
                )
    return CancelResponse(target_run_kind="sync_run", target_run_id=sync_run_id)


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
    if mechanism is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown process: {process_id!r}",
        )
    if mechanism == "ingest_sweep":
        # #1078 — admin control hub PR6. Sweeps have no in-flight
        # state of their own; cancel the underlying scheduled job
        # instead.
        raise _conflict(
            "cancel_not_supported",
            advice="cancel via the underlying scheduled job",
        )

    _, operator_uuid = _identify_requestor(request)

    if mechanism == "scheduled_job" and process_id == JOB_ORCHESTRATOR_FULL_SYNC:
        return _cancel_orchestrator_full_sync(conn, body=body, operator_uuid=operator_uuid)

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
