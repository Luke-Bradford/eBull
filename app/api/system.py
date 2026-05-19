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
from app.api.bootstrap import BootstrapApiStatus, LaneApi, StageApiStatus
from app.db import get_conn
from app.services.bootstrap_state import (
    compute_retryable_view,
    read_run_with_stages,
    read_state,
)
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
    # Operator-facing label populated from
    # ``ScheduledJob.display_name`` (registry single-source-of-truth at
    # ``app/workers/scheduler.py::SCHEDULED_JOBS``). FE renders this in
    # the legacy "Background tasks" JobsTable + ProblemsPanel failure
    # copy in place of the raw ``name`` slug. ``None`` falls back to
    # ``name`` at render time, matching the scheduled_adapter contract.
    display_name: str | None
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
                display_name=job.display_name,
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

    Per-operator scope (single-operator v1). Walks every environment
    the operator has active credential rows for and returns the
    worst-of state — REJECTED in any environment surfaces as
    REJECTED so a live-environment rejection is not masked by a
    valid demo pair (review #985 BLOCKING).

    Falls back to MISSING when no operator yet so the admin UI shows
    the "save credentials in Settings" path on a fresh install rather
    than misreporting VALID.
    """
    from app.services.credential_health import (
        CredentialHealth,
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
        environments = _operator_environments(conn, op_id)
        if not environments:
            return CredentialHealthSummary(state="missing")

        worst_health: CredentialHealth | None = None
        for env in environments:
            env_health = get_operator_credential_health(conn, operator_id=op_id, environment=env)
            if worst_health is None or _is_worse(env_health, worst_health):
                worst_health = env_health

        recovered_at = get_last_recovered_at(conn, operator_id=op_id)
        last_error = _latest_credential_error(conn, op_id) if worst_health == CredentialHealth.REJECTED else None
    except Exception:
        logger.exception("credential_health summary lookup failed; reporting missing")
        return CredentialHealthSummary(state="missing")

    if worst_health is None:
        return CredentialHealthSummary(state="missing")

    state_value = worst_health.value
    if state_value not in ("valid", "untested", "rejected", "missing"):
        logger.error("unexpected CredentialHealth value: %s", state_value)
        return CredentialHealthSummary(state="missing")
    return CredentialHealthSummary(
        state=state_value,  # type: ignore[arg-type]
        last_recovered_at=recovered_at,
        last_error=last_error,
    )


def _operator_environments(conn: psycopg.Connection[object], operator_id: object) -> list[str]:
    """Return distinct active-credential environments for the operator."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT environment
              FROM broker_credentials
             WHERE operator_id = %s
               AND revoked_at IS NULL
             ORDER BY environment
            """,
            (operator_id,),
        )
        return [str(row["environment"]) for row in cur.fetchall()]


def _is_worse(a: object, b: object) -> bool:
    """Worst-of precedence on CredentialHealth (REJECTED > MISSING > UNTESTED > VALID)."""
    from app.services.credential_health import CredentialHealth

    rank = {
        CredentialHealth.VALID: 0,
        CredentialHealth.UNTESTED: 1,
        CredentialHealth.MISSING: 2,
        CredentialHealth.REJECTED: 3,
    }
    return rank.get(a, 0) > rank.get(b, 0)  # type: ignore[arg-type]


def _latest_credential_error(conn: psycopg.Connection[object], operator_id: object) -> str | None:
    """Return the most recent ``last_health_error`` across the operator's
    rejected rows, for the admin banner's contextual error display
    (review #985 WARNING — the response model declared the field but
    the original implementation always returned None)."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT last_health_error
              FROM broker_credentials
             WHERE operator_id = %s
               AND revoked_at IS NULL
               AND health_state = 'rejected'
               AND last_health_error IS NOT NULL
             ORDER BY health_state_updated_at DESC
             LIMIT 1
            """,
            (operator_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    value = row["last_health_error"]
    return str(value) if value is not None else None


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


# ---------------------------------------------------------------------------
# Postgres health (#1208 Phase 4 Sub 4)
# ---------------------------------------------------------------------------
#
# Surfaces the five operator-visible signals that Phases 1-3 of #1208
# added enforcement for but no live readout: dev DB size + leaked
# test-DB count + WAL pressure + last checkpoint + autovacuum top-10
# + financial_facts_raw_default growth alarm.
#
# Implementation lives in `app/services/postgres_health.py` — that
# module opens its own autocommit conn so per-metric failures stay
# isolated (Phase 4 spec §4.2 / Codex 1a BLOCKING #1).


class AutovacuumTableLagSchema(BaseModel):
    relname: str
    last_autovacuum: datetime | None
    last_analyze: datetime | None
    n_dead_tup: int
    n_live_tup: int
    dead_fraction: float | None


class PostgresHealthResponse(BaseModel):
    db_size_bytes: int | None
    db_size_pretty: str | None
    db_size_warn_threshold_bytes: int
    db_size_breached_warn: bool | None
    leaked_test_db_count: int | None
    leaked_test_db_names: list[str] | None
    wal_dir_bytes: int | None
    wal_dir_pretty: str | None
    wal_since_checkpoint_bytes: int | None
    wal_warn_threshold_bytes: int
    wal_breached_warn: bool | None
    last_checkpoint_at: datetime | None
    autovacuum_top10: list[AutovacuumTableLagSchema] | None
    financial_facts_raw_default_rows: int | None
    financial_facts_raw_default_warn_threshold: int
    financial_facts_raw_default_breached_warn: bool | None
    metric_errors: list[str]
    collected_at: datetime


# ---------------------------------------------------------------------------
# Bootstrap status overview (#1136 Phase A.3 audit endpoint)
# ---------------------------------------------------------------------------
#
# Lean operator readout mirroring the ``/system/postgres-health`` shape:
# per-stage ``(status, last_error, retryable, attempt_count,
# completed_at)`` plus a top-level summary and ``retry_available`` /
# ``retry_blocked_reason`` pair. Lets the operator drive T9-POST drain
# without log-grepping or replaying ``reset_failed_stages_for_retry``
# precedence by hand.
#
# Distinct from ``GET /system/bootstrap/status`` (rich admin-page
# payload — archive_results, bulk_manifest, full stage params); this
# is the audit-shape sibling. Both endpoints read the same tables.


class BootstrapStatusSummary(BaseModel):
    total: int
    pending: int
    running: int
    success: int
    error: int
    blocked: int
    skipped: int
    cancelled: int


class BootstrapStatusStageOverview(BaseModel):
    stage_key: str
    stage_order: int
    lane: LaneApi
    status: StageApiStatus
    last_error: str | None
    attempt_count: int
    completed_at: datetime | None
    retryable: bool


class BootstrapStatusOverview(BaseModel):
    state_status: BootstrapApiStatus
    current_run_id: int | None
    last_completed_at: datetime | None
    summary: BootstrapStatusSummary
    retry_available: bool
    retry_blocked_reason: (
        Literal[
            "bootstrap_running",
            "no_prior_run",
            "state_not_resettable",
            "no_failed_stages",
        ]
        | None
    )
    stages: list[BootstrapStatusStageOverview]
    collected_at: datetime


@router.get("/bootstrap-status", response_model=BootstrapStatusOverview)
def get_bootstrap_status_overview(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> BootstrapStatusOverview:
    """Lean operator readout of bootstrap state + per-stage retryability.

    Spec: ``docs/superpowers/specs/2026-05-19-1136-bootstrap-state-audit.md``.

    Failure posture mirrors ``/system/postgres-health``:
      * DB unreachable → 503.
      * No prior run → 200 with ``state_status='pending'``, empty
        stages, ``retry_blocked_reason='no_prior_run'``.

    Run identification pins on ``bootstrap_state.last_run_id`` (NOT
    ``ORDER BY bootstrap_runs.id DESC``) so the readout reflects what
    ``/retry-failed`` would target — the two can diverge transiently
    during ``start_run`` or after a post-restart sweep that re-seeded
    a row without touching the singleton.
    """
    now = _utcnow()
    try:
        with conn.transaction():
            state = read_state(conn)
            if state.last_run_id is None:
                snapshot = None
            else:
                snapshot = read_run_with_stages(conn, run_id=state.last_run_id)
    except psycopg.Error as exc:
        logger.exception("bootstrap-status: DB-level failure")
        raise HTTPException(status_code=503, detail="bootstrap status unavailable") from exc

    view = compute_retryable_view(state, snapshot)

    summary = BootstrapStatusSummary(
        total=0, pending=0, running=0, success=0, error=0, blocked=0, skipped=0, cancelled=0
    )
    stage_overviews: list[BootstrapStatusStageOverview] = []
    if snapshot is not None:
        for stage in snapshot.stages:
            stage_overviews.append(
                BootstrapStatusStageOverview(
                    stage_key=stage.stage_key,
                    stage_order=stage.stage_order,
                    lane=stage.lane,
                    status=stage.status,
                    last_error=stage.last_error,
                    attempt_count=stage.attempt_count,
                    completed_at=stage.completed_at,
                    retryable=view.stage_retryable.get(stage.stage_key, False),
                )
            )
        counter_map: dict[str, int] = {
            "pending": 0,
            "running": 0,
            "success": 0,
            "error": 0,
            "blocked": 0,
            "skipped": 0,
            "cancelled": 0,
        }
        for stage in snapshot.stages:
            if stage.status in counter_map:
                counter_map[stage.status] += 1
        summary = BootstrapStatusSummary(
            total=len(snapshot.stages),
            pending=counter_map["pending"],
            running=counter_map["running"],
            success=counter_map["success"],
            error=counter_map["error"],
            blocked=counter_map["blocked"],
            skipped=counter_map["skipped"],
            cancelled=counter_map["cancelled"],
        )

    return BootstrapStatusOverview(
        state_status=state.status,
        current_run_id=state.last_run_id,
        last_completed_at=state.last_completed_at,
        summary=summary,
        retry_available=view.retry_available,
        retry_blocked_reason=view.retry_blocked_reason,
        stages=stage_overviews,
        collected_at=now,
    )


@router.get("/postgres-health", response_model=PostgresHealthResponse)
def get_postgres_health() -> PostgresHealthResponse:
    """Live Postgres health snapshot for the operator dashboard.

    Returns 200 with a partial payload if some metric queries fail —
    the `metric_errors` field lists which probes raised, and the
    corresponding `*_breached_warn` flags are `null` rather than
    `false` so a silent collection failure can't masquerade as "all
    clear" (Phase 4 spec §4.3).

    Raises 503 only when the underlying `psycopg.connect()` fails —
    i.e. the dev DB is itself unreachable. Matches the fail-closed
    posture documented in the module docstring.
    """
    # Local import so the system router file stays light on global
    # state (the service module touches `settings.database_url` at
    # call time, not at import time).
    from app.services.postgres_health import (
        PostgresHealthSnapshot,
        collect_postgres_health,
    )

    try:
        snapshot: PostgresHealthSnapshot = collect_postgres_health()
    except psycopg.Error as exc:
        logger.exception("postgres-health: connection-level failure")
        raise HTTPException(status_code=503, detail="postgres health unavailable") from exc

    return PostgresHealthResponse(
        db_size_bytes=snapshot.db_size_bytes,
        db_size_pretty=snapshot.db_size_pretty,
        db_size_warn_threshold_bytes=snapshot.db_size_warn_threshold_bytes,
        db_size_breached_warn=snapshot.db_size_breached_warn,
        leaked_test_db_count=snapshot.leaked_test_db_count,
        leaked_test_db_names=snapshot.leaked_test_db_names,
        wal_dir_bytes=snapshot.wal_dir_bytes,
        wal_dir_pretty=snapshot.wal_dir_pretty,
        wal_since_checkpoint_bytes=snapshot.wal_since_checkpoint_bytes,
        wal_warn_threshold_bytes=snapshot.wal_warn_threshold_bytes,
        wal_breached_warn=snapshot.wal_breached_warn,
        last_checkpoint_at=snapshot.last_checkpoint_at,
        autovacuum_top10=(
            None
            if snapshot.autovacuum_top10 is None
            else [
                AutovacuumTableLagSchema(
                    relname=lag.relname,
                    last_autovacuum=lag.last_autovacuum,
                    last_analyze=lag.last_analyze,
                    n_dead_tup=lag.n_dead_tup,
                    n_live_tup=lag.n_live_tup,
                    dead_fraction=lag.dead_fraction,
                )
                for lag in snapshot.autovacuum_top10
            ]
        ),
        financial_facts_raw_default_rows=snapshot.financial_facts_raw_default_rows,
        financial_facts_raw_default_warn_threshold=(snapshot.financial_facts_raw_default_warn_threshold),
        financial_facts_raw_default_breached_warn=(snapshot.financial_facts_raw_default_breached_warn),
        metric_errors=snapshot.metric_errors,
        collected_at=snapshot.collected_at,
    )
