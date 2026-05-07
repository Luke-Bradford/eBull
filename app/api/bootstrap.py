"""First-install bootstrap API.

Four endpoints under ``/system/bootstrap/*``:

* ``GET /status`` — single-snapshot read of ``bootstrap_state`` +
  the latest run + its 17 stages.
* ``POST /run`` — single-flight; creates a new run, seeds 17 pending
  stages, queues the orchestrator via the existing ``manual_job``
  path. 409 if a run is already in flight.
* ``POST /retry-failed`` — for ``partial_error`` only. Resets failed
  stages plus all later-numbered same-lane stages, re-queues the
  orchestrator.
* ``POST /mark-complete`` — operator escape hatch. Forces
  ``bootstrap_state.status='complete'``. 409 while a run is in flight.

All endpoints share the existing operator-session auth.

Spec: docs/superpowers/specs/2026-05-07-first-install-bootstrap.md
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Literal

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.services.bootstrap_orchestrator import (
    JOB_BOOTSTRAP_ORCHESTRATOR,
    get_bootstrap_stage_specs,
)
from app.services.bootstrap_state import (
    BootstrapAlreadyRunning,
    force_mark_complete,
    read_latest_run_with_stages,
    read_state,
    reset_failed_stages_for_retry,
    start_run,
)
from app.services.sync_orchestrator.dispatcher import publish_manual_job_request

logger = logging.getLogger(__name__)


router = APIRouter(
    prefix="/system/bootstrap",
    tags=["system", "bootstrap"],
    dependencies=[Depends(require_session_or_service_token)],
)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


BootstrapApiStatus = Literal["pending", "running", "complete", "partial_error"]
LaneApi = Literal["init", "etoro", "sec"]
StageApiStatus = Literal["pending", "running", "success", "error", "skipped"]


class BootstrapStageResponse(BaseModel):
    stage_key: str
    stage_order: int
    lane: LaneApi
    job_name: str
    status: StageApiStatus
    started_at: datetime | None
    completed_at: datetime | None
    rows_processed: int | None
    expected_units: int | None
    units_done: int | None
    last_error: str | None
    attempt_count: int


class BootstrapStatusResponse(BaseModel):
    status: BootstrapApiStatus
    current_run_id: int | None
    last_completed_at: datetime | None
    stages: list[BootstrapStageResponse]


class BootstrapRunQueuedResponse(BaseModel):
    run_id: int
    request_id: int


class BootstrapConflictResponse(BaseModel):
    detail: str
    current_run_id: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _identify_requestor(request: Request) -> str:
    """Operator session identity (mirrors app/api/jobs.py)."""
    op = getattr(request.state, "operator_id", None)
    if op:
        return f"operator:{op}"
    return "operator:anonymous"


def _build_status_response(conn: psycopg.Connection[object]) -> BootstrapStatusResponse:
    """Single-transaction snapshot of bootstrap state + latest run."""
    state = read_state(conn)
    snap = read_latest_run_with_stages(conn)
    if snap is None:
        return BootstrapStatusResponse(
            status=state.status,
            current_run_id=None,
            last_completed_at=state.last_completed_at,
            stages=[],
        )

    stages = [
        BootstrapStageResponse(
            stage_key=stage.stage_key,
            stage_order=stage.stage_order,
            lane=stage.lane,
            job_name=stage.job_name,
            status=stage.status,
            started_at=stage.started_at,
            completed_at=stage.completed_at,
            rows_processed=stage.rows_processed,
            expected_units=stage.expected_units,
            units_done=stage.units_done,
            last_error=stage.last_error,
            attempt_count=stage.attempt_count,
        )
        for stage in snap.stages
    ]
    return BootstrapStatusResponse(
        status=state.status,
        current_run_id=snap.run_id,
        last_completed_at=state.last_completed_at,
        stages=stages,
    )


# ---------------------------------------------------------------------------
# GET /status
# ---------------------------------------------------------------------------


@router.get("/status", response_model=BootstrapStatusResponse)
def get_bootstrap_status(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> BootstrapStatusResponse:
    """Single-snapshot read of bootstrap state + latest run.

    Reads happen inside one ``conn.transaction()`` so a stage
    transitioning mid-fetch cannot produce an internally-inconsistent
    payload (prevention-log: "Multi-query read handlers must use a
    single snapshot").
    """
    return _build_status_response(conn)


# ---------------------------------------------------------------------------
# POST /run
# ---------------------------------------------------------------------------


@router.post(
    "/run",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=BootstrapRunQueuedResponse,
)
def run_bootstrap(
    request: Request,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> BootstrapRunQueuedResponse:
    """Trigger a fresh bootstrap run.

    Single-flight via ``SELECT ... FOR UPDATE`` on the
    ``bootstrap_state`` singleton row inside ``start_run``. Defense
    in depth via the partial unique index on
    ``bootstrap_runs(status='running')``.

    On success: creates a new ``bootstrap_runs`` row, seeds 17
    pending ``bootstrap_stages`` rows, flips the singleton state to
    ``running``, then publishes a ``manual_job`` queue row pointing
    at the ``bootstrap_orchestrator`` invoker. Returns 202 with the
    new ``run_id`` and the queue ``request_id``.
    """
    try:
        run_id = start_run(
            conn,
            operator_id=None,
            stage_specs=get_bootstrap_stage_specs(),
        )
    except BootstrapAlreadyRunning as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "bootstrap_already_running",
                "current_run_id": exc.run_id,
            },
        ) from exc

    requested_by = _identify_requestor(request)
    request_id = publish_manual_job_request(JOB_BOOTSTRAP_ORCHESTRATOR, requested_by=requested_by)
    logger.info(
        "bootstrap: queued run_id=%d request_id=%d requested_by=%s",
        run_id,
        request_id,
        requested_by,
    )
    return BootstrapRunQueuedResponse(run_id=run_id, request_id=request_id)


# ---------------------------------------------------------------------------
# POST /retry-failed
# ---------------------------------------------------------------------------


@router.post(
    "/retry-failed",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=BootstrapRunQueuedResponse,
)
def retry_failed(
    request: Request,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> BootstrapRunQueuedResponse:
    """Re-run failed stages (and their downstream same-lane peers).

    For a ``partial_error`` state. Reuses the latest
    ``bootstrap_runs.id`` rather than creating a new run.

    409 if a run is already in flight. 404 if there is no prior run
    or the latest run has no failed stages to retry.
    """
    state = read_state(conn)
    if state.status == "running":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "bootstrap_running",
                "current_run_id": state.last_run_id,
            },
        )
    if state.last_run_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="no prior bootstrap run to retry",
        )

    reset_count = reset_failed_stages_for_retry(conn, run_id=state.last_run_id)
    if reset_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="latest run has no failed stages to retry",
        )

    requested_by = _identify_requestor(request)
    request_id = publish_manual_job_request(JOB_BOOTSTRAP_ORCHESTRATOR, requested_by=requested_by)
    logger.info(
        "bootstrap: retry-failed run_id=%d reset_count=%d request_id=%d requested_by=%s",
        state.last_run_id,
        reset_count,
        request_id,
        requested_by,
    )
    return BootstrapRunQueuedResponse(run_id=state.last_run_id, request_id=request_id)


# ---------------------------------------------------------------------------
# POST /mark-complete
# ---------------------------------------------------------------------------


class BootstrapMarkCompleteResponse(BaseModel):
    status: BootstrapApiStatus


@router.post("/mark-complete", response_model=BootstrapMarkCompleteResponse)
def mark_complete(
    request: Request,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> BootstrapMarkCompleteResponse:
    """Operator escape hatch: force ``bootstrap_state.status='complete'``.

    Used when the operator has manually fixed the cause of a stage
    failure and wants to release the scheduler gate without
    re-running heavy stages.

    409 while a run is in flight: releasing the gate while
    orchestrator threads are still mutating data would let nightly
    jobs run against half-populated tables — exactly the case the
    gate exists to prevent.
    """
    state = read_state(conn)
    if state.status == "running":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "bootstrap_running",
                "current_run_id": state.last_run_id,
            },
        )
    force_mark_complete(conn)
    requested_by = _identify_requestor(request)
    logger.warning(
        "bootstrap: mark-complete forced by %s (prior status=%s)",
        requested_by,
        state.status,
    )
    return BootstrapMarkCompleteResponse(status="complete")
