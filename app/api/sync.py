"""Sync orchestrator HTTP endpoints.

Phase 1: POST /sync returns 503 while ORCHESTRATOR_ENABLED=false.
GET endpoints work for inspection regardless of the flag.

Auth: same dependency as /jobs — require_session_or_service_token.
"""

from __future__ import annotations

from typing import Any, Literal

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.config import settings
from app.db import get_conn
from app.services.sync_orchestrator import (
    ExecutionPlan,
    SyncAlreadyRunning,
    SyncScope,
    submit_sync,
)

router = APIRouter(
    prefix="/sync",
    tags=["sync"],
    dependencies=[Depends(require_session_or_service_token)],
)


class SyncRequest(BaseModel):
    scope: Literal["full", "layer", "high_frequency", "job"] = "full"
    layer: str | None = None
    job: str | None = None


def _scope_from(body: SyncRequest) -> SyncScope:
    if body.scope == "full":
        return SyncScope.full()
    if body.scope == "high_frequency":
        return SyncScope.high_frequency()
    if body.scope == "layer":
        if not body.layer:
            raise HTTPException(status_code=422, detail="layer required when scope='layer'")
        return SyncScope.layer(body.layer)
    if body.scope == "job":
        if not body.job:
            raise HTTPException(status_code=422, detail="job required when scope='job'")
        return SyncScope.job(body.job, force=True)
    raise HTTPException(status_code=422, detail=f"unknown scope {body.scope!r}")


def _plan_to_json(plan: ExecutionPlan) -> dict[str, Any]:
    return {
        "layers_to_refresh": [
            {
                "name": lp.name,
                "emits": list(lp.emits),
                "reason": lp.reason,
                "dependencies": list(lp.dependencies),
                "is_blocking": lp.is_blocking,
                "estimated_items": lp.estimated_items,
            }
            for lp in plan.layers_to_refresh
        ],
        "layers_skipped": [{"name": s.name, "reason": s.reason} for s in plan.layers_skipped],
    }


@router.post("", status_code=status.HTTP_202_ACCEPTED)
def post_sync(body: SyncRequest) -> Any:
    if not settings.orchestrator_enabled:
        raise HTTPException(status_code=503, detail="sync orchestrator disabled (Phase 1)")
    try:
        sync_run_id, plan = submit_sync(_scope_from(body), trigger="manual")
    except SyncAlreadyRunning as exc:
        # Use JSONResponse to get a top-level body per spec §4.4 instead
        # of FastAPI's HTTPException "detail" wrapper.
        return JSONResponse(
            status_code=409,
            content={
                "error": "sync_already_running",
                "sync_run_id": exc.active_sync_run_id,
            },
        )
    return {"sync_run_id": sync_run_id, "plan": _plan_to_json(plan)}


@router.get("/status")
def get_sync_status(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> dict[str, Any]:
    """Current running sync (if any) + active layer row."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT sync_run_id, scope, trigger, started_at,
                   layers_planned, layers_done, layers_failed, layers_skipped
            FROM sync_runs
            WHERE status = 'running'
            ORDER BY started_at DESC
            LIMIT 1
            """,
        )
        row = cur.fetchone()
    if row is None:
        return {"is_running": False, "current_run": None, "active_layer": None}
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT layer_name, started_at, items_total, items_done
            FROM sync_layer_progress
            WHERE sync_run_id = %s AND status = 'running'
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (row["sync_run_id"],),
        )
        active = cur.fetchone()
    return {
        "is_running": True,
        "current_run": {
            "sync_run_id": row["sync_run_id"],
            "scope": row["scope"],
            "trigger": row["trigger"],
            "started_at": row["started_at"].isoformat(),
            "layers_planned": row["layers_planned"],
            "layers_done": row["layers_done"],
            "layers_failed": row["layers_failed"],
            "layers_skipped": row["layers_skipped"],
        },
        "active_layer": None
        if active is None
        else {
            "name": active["layer_name"],
            "started_at": active["started_at"].isoformat() if active["started_at"] else None,
            "items_total": active["items_total"],
            "items_done": active["items_done"],
        },
    }


@router.get("/layers")
def get_sync_layers(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> dict[str, Any]:
    """All 15 layers with freshness + last successful run."""
    from app.services.sync_orchestrator import LAYERS
    from app.services.sync_orchestrator.registry import JOB_TO_LAYERS

    # Collision guard: JOB_TO_LAYERS maps N jobs → M layer names. If two
    # jobs ever claimed the same layer, last-writer-wins would silently
    # hide the conflict and send a wrong job_name into freshness lookups.
    layer_to_job: dict[str, str] = {}
    for job, emits in JOB_TO_LAYERS.items():
        for emit in emits:
            if emit in layer_to_job:
                raise RuntimeError(
                    f"layer {emit!r} emitted by both {layer_to_job[emit]!r} "
                    f"and {job!r}; JOB_TO_LAYERS must have disjoint emits"
                )
            layer_to_job[emit] = job

    out: list[dict[str, Any]] = []
    for name, layer in LAYERS.items():
        # Per-layer isolation: one broken predicate should not 500 the
        # whole endpoint. Operators need the dashboard most when things
        # are red; masking a partial failure would hide which layer broke.
        try:
            fresh, detail = layer.is_fresh(conn)
            predicate_error: str | None = None
        except Exception as exc:
            fresh = False
            detail = f"freshness predicate error: {type(exc).__name__}"
            predicate_error = type(exc).__name__
        job_name = layer_to_job[name]
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT started_at, finished_at
                FROM job_runs
                WHERE job_name = %s AND status = 'success'
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (job_name,),
            )
            last = cur.fetchone()
        last_success_at = last["finished_at"] if last else None
        last_start = last["started_at"] if last else None
        out.append(
            {
                "name": name,
                "display_name": layer.display_name,
                "tier": layer.tier,
                "is_fresh": fresh,
                "freshness_detail": detail,
                "last_success_at": last_success_at.isoformat() if last_success_at else None,
                "last_duration_seconds": (
                    int((last_success_at - last_start).total_seconds()) if last_success_at and last_start else None
                ),
                "last_error_category": predicate_error,
                "consecutive_failures": 0,
                "dependencies": list(layer.dependencies),
                "is_blocking": layer.is_blocking,
            }
        )
    return {"layers": out}


@router.get("/runs")
def get_sync_runs(
    conn: psycopg.Connection[object] = Depends(get_conn),
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Recent sync runs, newest first."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT sync_run_id, scope, scope_detail, trigger, started_at,
                   finished_at, status, layers_planned, layers_done,
                   layers_failed, layers_skipped
            FROM sync_runs
            ORDER BY started_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return {
        "runs": [
            {
                "sync_run_id": r["sync_run_id"],
                "scope": r["scope"],
                "scope_detail": r["scope_detail"],
                "trigger": r["trigger"],
                "started_at": r["started_at"].isoformat(),
                "finished_at": r["finished_at"].isoformat() if r["finished_at"] else None,
                "status": r["status"],
                "layers_planned": r["layers_planned"],
                "layers_done": r["layers_done"],
                "layers_failed": r["layers_failed"],
                "layers_skipped": r["layers_skipped"],
            }
            for r in rows
        ]
    }
