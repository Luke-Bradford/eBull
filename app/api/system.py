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

Distinct from ``/health`` (liveness) and from the retired ``/health/data``
(removed in #342; served the same purpose pre-#57). The frontend admin page (#64) polls
``/system/status``; the scheduled-job table on the same page polls
``/system/jobs``.

Auth: both endpoints require operator auth via ``require_session_or_service_token`` (issue #98),
mounted on the router so individual handlers cannot accidentally be exposed
without it. The status payload reveals data-pipeline gaps that an attacker
could use to time abuse, so it must not be public.

Fail-closed posture (prevention-log #70):
  - On any service-level exception, raise ``HTTPException(status_code=503)``.
  - Per-layer failures inside ``check_all_layers`` are already surfaced as
    ``status="error"`` rows so a single broken layer does not 503 the whole
    response.

``next_run_time`` is sourced from the live APScheduler scheduler when the
runtime is available, falling back to the declared cadence computation
(``compute_next_run``) when the runtime is absent (e.g. in tests).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Literal

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
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
    dependencies=[Depends(require_session_or_service_token)],
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
    last_status: Literal["running", "success", "failure", "skipped"] | None
    last_started_at: datetime | None
    last_finished_at: datetime | None
    detail: str


class CredentialHealthSummary(BaseModel):
    """Operator-level credential health for the admin Problems banner (#979 / #974/E).

    State values mirror ``app.services.credential_health.CredentialHealth``:
      * ``valid``     — operator's credential pair validated; orchestrator
                        runs credential-using layers freely.
      * ``untested``  — saved but not yet probed; orchestrator will skip
                        until validate-stored confirms.
      * ``rejected``  — provider returned 401/403; orchestrator
                        PREREQ_SKIPs all credential-using layers; admin
                        UI surfaces a single banner instead of N
                        cascading AUTH_EXPIRED rows.
      * ``missing``   — no credential rows; first-run state.
    """

    state: Literal["valid", "untested", "rejected", "missing"]
    last_recovered_at: datetime | None = None
    last_error: str | None = None


class SystemStatusResponse(BaseModel):
    checked_at: datetime
    overall_status: OverallStatus
    layers: list[LayerHealthResponse]
    jobs: list[JobHealthResponse]
    kill_switch: KillSwitchStateResponse
    credential_health: CredentialHealthSummary


class JobOverviewResponse(BaseModel):
    name: str
    description: str
    cadence: str
    cadence_kind: Literal["every_n_minutes", "hourly", "daily", "weekly", "monthly"]
    next_run_time: datetime
    # `next_run_time_source` retained for frontend compat; always
    # "declared" since #719 — the API no longer hosts APScheduler so
    # there is no live-fire-time source to compete with the cadence
    # computation. The frontend can drop the discriminator at its leisure.
    next_run_time_source: Literal["live", "declared"]
    last_status: Literal["running", "success", "failure", "skipped"] | None
    last_started_at: datetime | None
    last_finished_at: datetime | None
    detail: str


class JobsProcessSubsystemHealth(BaseModel):
    """Per-subsystem heartbeat health for the jobs process (#719)."""

    subsystem: str
    last_beat_at: datetime | None
    age_seconds: float | None
    is_stale: bool


class JobsProcessHealthResponse(BaseModel):
    """Aggregate jobs-process health derived from the heartbeat table.

    `state` is `healthy` only when every expected subsystem has beaten
    within `STALE_THRESHOLD_SECONDS`. A single stale subsystem
    downgrades to `degraded`; every subsystem stale (or no rows at all)
    is `down`. Frontend renders the aggregate with a per-subsystem
    drilldown.
    """

    state: Literal["healthy", "degraded", "down"]
    subsystems: list[JobsProcessSubsystemHealth]


class JobsListResponse(BaseModel):
    checked_at: datetime
    jobs: list[JobOverviewResponse]
    jobs_process: JobsProcessHealthResponse


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
    - any job currently "running" → "degraded"
    - otherwise → "ok"

    Jobs with ``last_status is None`` (no runs ever recorded) are deliberately
    NOT treated as degraded on their own — a fresh deploy would otherwise
    always report "degraded" purely because no jobs have fired yet, and the
    per-job ``detail`` already surfaces "no runs recorded" so the operator
    can see exactly what is missing. A fresh deploy will still report
    "degraded" via the empty data layers, which is the more meaningful
    signal anyway.
    """
    if kill_switch_active:
        return "down"
    if any(layer.status == "error" for layer in layers):
        return "down"
    if any(job.last_status == "failure" for job in jobs):
        return "down"
    if any(layer.status in ("stale", "empty") for layer in layers):
        return "degraded"
    if any(job.last_status == "running" for job in jobs):
        return "degraded"
    return "ok"


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


def _build_jobs_overview(
    conn: psycopg.Connection[object],
    registry: list[ScheduledJob],
    now: datetime,
) -> list[JobOverviewResponse]:
    """Build the per-job overview view.

    Since #719, next-run-time is always computed from the declared
    cadence — the API no longer hosts APScheduler, so there is no live
    fire-time to query. `compute_next_run(cadence, now)` returns the
    next future occurrence, which is the same value APScheduler would
    schedule against. The `_source="declared"` discriminator is kept
    for frontend compat.
    """
    overviews: list[JobOverviewResponse] = []
    for job in registry:
        health = check_job_health(conn, job.name)
        next_run = compute_next_run(job.cadence, now)
        overviews.append(
            JobOverviewResponse(
                name=job.name,
                description=job.description,
                cadence=job.cadence.label,
                cadence_kind=job.cadence.kind,
                next_run_time=next_run,
                next_run_time_source="declared",
                last_status=health.last_status,
                last_started_at=health.last_started_at,
                last_finished_at=health.last_finished_at,
                detail=health.detail,
            )
        )
    return overviews


# Stale threshold for a heartbeat row. Each subsystem writes every 10s;
# 60s gives 6 missed beats of head-room before the API flags `degraded`.
_HEARTBEAT_STALE_THRESHOLD_S: float = 60.0


def _build_jobs_process_health(
    conn: psycopg.Connection[object],
    now: datetime,
) -> JobsProcessHealthResponse:
    """Aggregate per-subsystem heartbeat into a process-level state.

    Returns `down` when the heartbeat table has no rows (jobs process
    has never run, or every subsystem is stale). Returns `healthy`
    only when every row's age is below the stale threshold. Anything
    in between is `degraded`.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT subsystem, last_beat_at
            FROM job_runtime_heartbeat
            ORDER BY subsystem
            """
        )
        rows = cur.fetchall()

    subsystems: list[JobsProcessSubsystemHealth] = []
    any_fresh = False
    any_stale = False
    for row in rows:
        last_beat: datetime = row["last_beat_at"]
        age = (now - last_beat).total_seconds()
        is_stale = age > _HEARTBEAT_STALE_THRESHOLD_S
        if is_stale:
            any_stale = True
        else:
            any_fresh = True
        subsystems.append(
            JobsProcessSubsystemHealth(
                subsystem=str(row["subsystem"]),
                last_beat_at=last_beat,
                age_seconds=age,
                is_stale=is_stale,
            )
        )

    if not subsystems or not any_fresh:
        state: Literal["healthy", "degraded", "down"] = "down"
    elif any_stale:
        state = "degraded"
    else:
        state = "healthy"

    return JobsProcessHealthResponse(state=state, subsystems=subsystems)


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
        # Log the full exception server-side; the HTTP detail is a fixed
        # string so we never leak internal schema, table names, or driver
        # error text to a bearer-token holder.
        logger.exception("get_system_status: failed to build report")
        raise HTTPException(status_code=503, detail="system status unavailable") from exc

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
        credential_health=_build_credential_health_summary(conn),
    )


def _build_credential_health_summary(conn: psycopg.Connection[object]) -> CredentialHealthSummary:
    """Resolve operator-level credential health for the admin banner.

    Per-operator scope (single-operator v1). Falls back to MISSING
    when no operator yet, so the admin UI shows the "save credentials
    in Settings" path on a fresh install rather than misreporting
    VALID. Codex r3.2 + #979 banner contract.
    """
    from app.services.credential_health import (
        get_last_recovered_at,
        get_operator_credential_health,
    )
    from app.services.operators import (
        AmbiguousOperatorError,
        NoOperatorError,
        sole_operator_id,
    )

    try:
        op_id = sole_operator_id(conn)
    except NoOperatorError:
        return CredentialHealthSummary(state="missing")
    except AmbiguousOperatorError:
        return CredentialHealthSummary(state="missing")

    try:
        health = get_operator_credential_health(conn, operator_id=op_id, environment="demo")
        recovered_at = get_last_recovered_at(conn, operator_id=op_id)
    except Exception:
        logger.exception("credential_health summary lookup failed; reporting missing")
        return CredentialHealthSummary(state="missing")

    state_value = health.value
    if state_value not in ("valid", "untested", "rejected", "missing"):
        # CredentialHealth enum values are pinned to the four literals
        # above; any deviation is a registry change that hasn't yet
        # been propagated to this response model. Fail-safe to missing.
        logger.error("unexpected CredentialHealth value: %s", state_value)
        return CredentialHealthSummary(state="missing")
    return CredentialHealthSummary(
        state=state_value,  # type: ignore[arg-type]
        last_recovered_at=recovered_at,
        last_error=None,
    )


@router.get("/jobs", response_model=JobsListResponse)
def get_jobs(
    request: Request,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> JobsListResponse:
    """Declared scheduled jobs + per-subsystem jobs-process health (#719).

    Next-run-time is computed from the declared cadence
    (``compute_next_run(cadence, now)``) — same value APScheduler
    schedules against. The ``jobs_process`` block exposes the
    multi-subsystem heartbeat table so a stale subsystem
    (manual_listener, queue_drainer, scheduler, main) is visible to
    the operator without log grepping.
    """
    # ``request`` is intentionally unused — kept in the signature for
    # backwards compat with existing FastAPI dependency wiring; the
    # runtime lookup off ``app.state.job_runtime`` is gone in #719.
    _ = request
    now = _utcnow()
    try:
        overviews = _build_jobs_overview(conn, SCHEDULED_JOBS, now)
        jobs_process = _build_jobs_process_health(conn, now)
    except Exception as exc:
        logger.exception("get_jobs: failed to build overview")
        raise HTTPException(status_code=503, detail="job overview unavailable") from exc

    return JobsListResponse(checked_at=now, jobs=overviews, jobs_process=jobs_process)
