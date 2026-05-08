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
LaneApi = Literal["init", "etoro", "sec", "sec_rate", "sec_bulk_download", "db"]
StageApiStatus = Literal["pending", "running", "success", "error", "skipped", "blocked"]


class BootstrapArchiveResultResponse(BaseModel):
    """Per-archive ingest outcome from ``bootstrap_archive_results``.

    Surfaced under the parent stage so the operator panel can render
    per-archive progress (which form13f / insider / nport quarter
    landed) + skipped counts (unresolved_cusip, unresolved_cik, etc).
    """

    archive_name: str
    rows_written: int
    rows_skipped: dict[str, int] = {}
    completed_at: datetime | None


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
    # #1046 archive-level progress: per-archive rows from
    # bootstrap_archive_results filtered to this stage_key. Empty list
    # for B-stages and one-shot lifecycle stages that don't track
    # per-archive outcomes.
    archive_results: list[BootstrapArchiveResultResponse] = []


class BulkManifestResponse(BaseModel):
    """Snapshot of ``<bulk>/.run_manifest.json`` (#1046).

    ``mode`` is "bulk" when A3 downloaded all archives, "fallback"
    when A3 measured bandwidth below threshold and bypassed Phase C
    in favour of the legacy chain, or null when no manifest exists
    (e.g. before A3 has run, or after a wipe).
    """

    present: bool
    mode: Literal["bulk", "fallback"] | None = None
    bootstrap_run_id: int | None = None
    archive_count: int = 0


class BootstrapStatusResponse(BaseModel):
    status: BootstrapApiStatus
    current_run_id: int | None
    last_completed_at: datetime | None
    stages: list[BootstrapStageResponse]
    bulk_manifest: BulkManifestResponse | None = None


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
    """Build the status response within a single transaction.

    Groups the ``bootstrap_state`` + latest-run + stages reads under
    one ``with conn.transaction():`` so the FastAPI dependency chain
    does not interleave commits between them. **Note** the connection
    runs at the default Postgres isolation level (READ COMMITTED), so
    each statement still observes its own snapshot — a writer that
    commits between statement 1 and statement 2 *is* visible to
    statement 2. The wrapping here is therefore *transaction
    grouping*, not cross-statement snapshot isolation.

    For v1 this is an acceptable trade-off: the orchestrator's stage
    transitions are short single-row UPDATEs, so the read-window
    inconsistency surface is small and the operator-facing impact is
    "the run progress UI briefly shows stage N as running and stage
    N+1 already started" — never a destructive read. If true
    cross-statement isolation is needed later, switch this
    connection to ``ISOLATION_LEVEL_REPEATABLE_READ`` for the
    duration of the read transaction.
    """
    with conn.transaction():
        state = read_state(conn)
        snap = read_latest_run_with_stages(conn)
        # #1046: per-archive results grouped by stage_key.
        archive_results_by_stage: dict[str, list[BootstrapArchiveResultResponse]] = {}
        archive_rows: list[tuple[str, str, int, object, datetime | None]] = []
        if snap is not None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT stage_key, archive_name, rows_written,
                           rows_skipped, completed_at
                    FROM bootstrap_archive_results
                    WHERE bootstrap_run_id = %s
                      AND archive_name <> '__job__'
                    ORDER BY stage_key, archive_name
                    """,
                    (snap.run_id,),
                )
                for row in cur.fetchall():
                    # psycopg returns each row as a tuple; the typed
                    # row factory is `object` here so cast for pyright.
                    row_tuple = row if isinstance(row, tuple) else tuple(row)  # type: ignore[arg-type]
                    archive_rows.append(
                        (
                            str(row_tuple[0]),
                            str(row_tuple[1]),
                            int(row_tuple[2] or 0),
                            row_tuple[3],
                            row_tuple[4],
                        )
                    )
            for stage_key, archive_name, rows_written, rows_skipped, completed_at in archive_rows:
                # rows_skipped is JSONB; psycopg returns it as dict.
                skipped_dict: dict[str, int] = {}
                if isinstance(rows_skipped, dict):
                    skipped_dict = {str(k): int(v) for k, v in rows_skipped.items() if isinstance(v, int)}
                archive_results_by_stage.setdefault(stage_key, []).append(
                    BootstrapArchiveResultResponse(
                        archive_name=archive_name,
                        rows_written=int(rows_written or 0),
                        rows_skipped=skipped_dict,
                        completed_at=completed_at,
                    )
                )
    if snap is None:
        return BootstrapStatusResponse(
            status=state.status,
            current_run_id=None,
            last_completed_at=state.last_completed_at,
            stages=[],
            bulk_manifest=_read_bulk_manifest_response(),
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
            archive_results=archive_results_by_stage.get(stage.stage_key, []),
        )
        for stage in snap.stages
    ]
    return BootstrapStatusResponse(
        status=state.status,
        current_run_id=snap.run_id,
        last_completed_at=state.last_completed_at,
        stages=stages,
        bulk_manifest=_read_bulk_manifest_response(),
    )


def _read_bulk_manifest_response() -> BulkManifestResponse:
    """Read ``<bulk>/.run_manifest.json`` and project it for the API.

    Operator-facing — answers "is the bulk path on disk and what mode
    did it land in?". Errors are swallowed (e.g. before A3 has run
    the directory may not exist) and surface as ``present=False``.
    """
    try:
        from app.security.master_key import resolve_data_dir
        from app.services.sec_bulk_download import read_run_manifest

        manifest = read_run_manifest(resolve_data_dir() / "sec" / "bulk")
    except Exception as exc:  # noqa: BLE001 — UI-side enrichment must not fail status
        # Log at warning so corrupt/malformed manifests are visible to
        # operators without breaking the status payload. Codex pre-push
        # P3 for #1046.
        logger.warning("bulk manifest read failed: %s", exc)
        return BulkManifestResponse(present=False)
    if manifest is None:
        return BulkManifestResponse(present=False)
    raw_mode = manifest.get("mode")
    mode: Literal["bulk", "fallback"] | None
    if raw_mode in ("bulk", "fallback"):
        mode = raw_mode  # type: ignore[assignment]
    else:
        mode = None
    archives = manifest.get("archives", [])
    archive_count = len(archives) if isinstance(archives, list) else 0
    raw_run_id = manifest.get("bootstrap_run_id")
    run_id_int: int | None
    try:
        run_id_int = int(raw_run_id) if raw_run_id is not None else None
    # Bind exception so ruff format on Python 3.14 keeps the tuple
    # parens (PEP 758 unparenthesised except handlers strip them
    # otherwise). Match the established workaround in
    # app/services/sec_bulk_download.py:_zip_round_trip.
    except (TypeError, ValueError) as _exc:
        del _exc
        run_id_int = None
    return BulkManifestResponse(
        present=True,
        mode=mode,
        bootstrap_run_id=run_id_int,
        archive_count=archive_count,
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

    try:
        reset_count = reset_failed_stages_for_retry(conn, run_id=state.last_run_id)
    except BootstrapAlreadyRunning as exc:
        # Concurrent ``start_run`` flipped state to ``running`` between
        # our pre-check above and the FOR UPDATE acquisition inside
        # ``reset_failed_stages_for_retry``. Treat as 409 — same
        # contract as the /run handler.
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "bootstrap_running",
                "current_run_id": exc.run_id,
            },
        ) from exc
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
    try:
        force_mark_complete(conn)
    except BootstrapAlreadyRunning as exc:
        # Concurrent ``start_run`` flipped state to ``running`` between
        # our pre-check and the FOR UPDATE acquisition.
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "bootstrap_running",
                "current_run_id": exc.run_id,
            },
        ) from exc
    requested_by = _identify_requestor(request)
    logger.warning(
        "bootstrap: mark-complete forced by %s (prior status=%s)",
        requested_by,
        state.status,
    )
    return BootstrapMarkCompleteResponse(status="complete")
