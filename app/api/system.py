"""System status and job overview endpoints (issue #57).

Endpoints:
  - GET /system/status — unified operator visibility:
      * data freshness per layer (ok / stale / empty / error)
      * job health per registered scheduled job
      * kill switch state
      * a single ``overall_status`` derived from the worst component
  - GET /system/jobs — declared schedule overview:
      * one row per ``ScheduledJob`` in ``app.workers.scheduler.SCHEDULED_JOBS``
      * each row carries the declared cadence + computed ``next_run_time``
        plus the most recent ``job_runs`` row for that job

Distinct from ``/health`` (liveness) and from the deprecated ``/health/data``
(which served the same purpose pre-#57). The frontend admin page (#64) polls
``/system/status``; the scheduled-job table on the same page polls
``/system/jobs``.

Auth: both endpoints require operator auth via ``require_auth`` (issue #58),
mounted on the router so individual handlers cannot accidentally be exposed
without it. The status payload reveals data-pipeline gaps that an attacker
could use to time abuse, so it must not be public.

Fail-closed posture (prevention-log #70):
  - On any service-level exception, raise ``HTTPException(status_code=503)``.
  - Per-layer failures inside ``check_all_layers`` are already surfaced as
    ``status="error"`` rows so a single broken layer does not 503 the whole
    response.

``next_run_time`` is derived from the *declared* cadence in the registry, not
from a live scheduler — APScheduler is not yet wired (#13). When it lands,
``compute_next_run`` should be replaced with the live scheduler's next-fire
time so reality and intent reconcile at one source of truth.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Literal

import psycopg
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.api.auth import require_auth
from app.db import get_conn
from app.services.ops_monitor import (
    JobHealth,
    LayerHealth,
    LayerStatus,
    check_all_layers,
    check_job_health,
    get_kill_switch_status,
)
from app.workers.scheduler import (
    SCHEDULED_JOBS,
    ScheduledJob,
    compute_next_run,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/system",
    tags=["system"],
    dependencies=[Depends(require_auth)],
)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


OverallStatus = Literal["ok", "degraded", "down"]


class LayerHealthResponse(BaseModel):
    layer: str
    status: LayerStatus
    latest: datetime | None
    max_age_seconds: float | None
    age_seconds: float | None
    detail: str


class KillSwitchStateResponse(BaseModel):
    active: bool
    activated_at: datetime | None
    activated_by: str | None
    reason: str | None


class JobHealthResponse(BaseModel):
    name: str
    last_status: Literal["running", "success", "failure"] | None
    last_started_at: datetime | None
    last_finished_at: datetime | None
    detail: str


class SystemStatusResponse(BaseModel):
    checked_at: datetime
    overall_status: OverallStatus
    layers: list[LayerHealthResponse]
    jobs: list[JobHealthResponse]
    kill_switch: KillSwitchStateResponse


class JobOverviewResponse(BaseModel):
    name: str
    description: str
    cadence: str
    cadence_kind: Literal["hourly", "daily", "weekly"]
    next_run_time: datetime
    next_run_time_source: Literal["declared"]  # see module docstring
    last_status: Literal["running", "success", "failure"] | None
    last_started_at: datetime | None
    last_finished_at: datetime | None
    detail: str


class JobsListResponse(BaseModel):
    checked_at: datetime
    jobs: list[JobOverviewResponse]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _layer_to_response(lh: LayerHealth) -> LayerHealthResponse:
    return LayerHealthResponse(
        layer=lh.layer,
        status=lh.status,
        latest=lh.latest,
        max_age_seconds=lh.max_age.total_seconds() if lh.max_age is not None else None,
        age_seconds=lh.age.total_seconds() if lh.age is not None else None,
        detail=lh.detail,
    )


def _job_health_to_response(name: str, jh: JobHealth) -> JobHealthResponse:
    return JobHealthResponse(
        name=name,
        last_status=jh.last_status,
        last_started_at=jh.last_started_at,
        last_finished_at=jh.last_finished_at,
        detail=jh.detail,
    )


def _derive_overall_status(
    layers: list[LayerHealth],
    jobs: list[JobHealth],
    kill_switch_active: bool,
) -> OverallStatus:
    """Worst-of(components).

    - kill switch active   → "down"  (system is intentionally halted)
    - any layer "error"    → "down"  (infra fault)
    - any job "failure"    → "down"
    - any layer "stale"/"empty" → "degraded"
    - any job "running" or unknown → "degraded"
    - otherwise → "ok"
    """
    if kill_switch_active:
        return "down"
    if any(layer.status == "error" for layer in layers):
        return "down"
    if any(job.last_status == "failure" for job in jobs):
        return "down"
    if any(layer.status in ("stale", "empty") for layer in layers):
        return "degraded"
    if any(job.last_status != "success" for job in jobs):
        return "degraded"
    return "ok"


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


def _build_jobs_overview(
    conn: psycopg.Connection[object],
    registry: list[ScheduledJob],
    now: datetime,
) -> list[JobOverviewResponse]:
    overviews: list[JobOverviewResponse] = []
    for job in registry:
        health = check_job_health(conn, job.name)
        overviews.append(
            JobOverviewResponse(
                name=job.name,
                description=job.description,
                cadence=job.cadence.label,
                cadence_kind=job.cadence.kind,
                next_run_time=compute_next_run(job.cadence, now),
                next_run_time_source="declared",
                last_status=health.last_status,
                last_started_at=health.last_started_at,
                last_finished_at=health.last_finished_at,
                detail=health.detail,
            )
        )
    return overviews


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


@router.get("/status", response_model=SystemStatusResponse)
def get_system_status(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> SystemStatusResponse:
    """Unified per-layer freshness + job health + kill switch.

    Errors at the report-build level (e.g. DB unreachable) raise 503; per-layer
    errors are surfaced as ``status="error"`` rows in the layers list and do
    not fail the whole endpoint.
    """
    now = _utcnow()
    try:
        layers = check_all_layers(conn, now=now)
        jobs = [check_job_health(conn, job.name) for job in SCHEDULED_JOBS]
        ks = get_kill_switch_status(conn)
    except Exception as exc:
        logger.exception("get_system_status: failed to build report")
        raise HTTPException(status_code=503, detail=f"system status unavailable: {exc}") from exc

    overall = _derive_overall_status(layers, jobs, bool(ks["is_active"]))

    return SystemStatusResponse(
        checked_at=now,
        overall_status=overall,
        layers=[_layer_to_response(layer) for layer in layers],
        jobs=[_job_health_to_response(job.name, health) for job, health in zip(SCHEDULED_JOBS, jobs, strict=True)],
        kill_switch=KillSwitchStateResponse(
            active=bool(ks["is_active"]),
            activated_at=ks.get("activated_at"),
            activated_by=ks.get("activated_by"),
            reason=ks.get("reason"),
        ),
    )


@router.get("/jobs", response_model=JobsListResponse)
def get_jobs(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> JobsListResponse:
    """Declared scheduled jobs with computed next-run time and last result.

    ``next_run_time`` is derived from the declared cadence (see module
    docstring); ``next_run_time_source`` is always ``"declared"`` until
    APScheduler is wired (#13).
    """
    now = _utcnow()
    try:
        overviews = _build_jobs_overview(conn, SCHEDULED_JOBS, now)
    except Exception as exc:
        logger.exception("get_jobs: failed to build overview")
        raise HTTPException(status_code=503, detail=f"job overview unavailable: {exc}") from exc

    return JobsListResponse(checked_at=now, jobs=overviews)
