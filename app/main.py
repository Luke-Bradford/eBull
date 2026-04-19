from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from app.api.attribution import router as attribution_router
from app.api.audit import router as audit_router
from app.api.auth import require_session_or_service_token
from app.api.auth_bootstrap import router as auth_bootstrap_router
from app.api.auth_session import router as auth_session_router
from app.api.auth_setup import router as auth_setup_router
from app.api.broker_credentials import router as broker_credentials_router
from app.api.budget import router as budget_router
from app.api.config import KillSwitchRequest, KillSwitchResponse, post_kill_switch
from app.api.config import router as config_router
from app.api.copy_trading import router as copy_trading_router
from app.api.coverage import router as coverage_router
from app.api.filings import router as filings_router
from app.api.instruments import router as instruments_router
from app.api.jobs import router as jobs_router
from app.api.news import router as news_router
from app.api.operators import router as operators_router
from app.api.orders import router as orders_router
from app.api.portfolio import router as portfolio_router
from app.api.recommendations import router as recommendations_router
from app.api.reports import router as reports_router
from app.api.scores import router as scores_router
from app.api.sync import router as sync_router
from app.api.system import router as system_router
from app.api.theses import router as theses_router
from app.config import settings
from app.db import get_conn
from app.db.migrations import migration_status, run_migrations
from app.jobs.runtime import JobRuntime, shutdown_runtime, start_runtime
from app.security import master_key
from app.security.secrets_crypto import set_active_key as set_broker_encryption_key
from app.services.coverage import override_tier
from app.services.operator_setup import ensure_startup_token, operators_empty
from app.services.ops_monitor import get_system_health
from app.services.sync_orchestrator.layer_state import compute_layer_states_from_db
from app.services.sync_orchestrator.layer_types import LayerState
from app.services.sync_orchestrator.reaper import reap_orphaned_syncs
from app.workers.scheduler import SCHEDULED_JOBS

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logger.info("Running pending migrations...")
    applied = await asyncio.to_thread(run_migrations)
    if applied:
        logger.info("Applied %d migration(s): %s", len(applied), applied)
    else:
        logger.info("No pending migrations.")

    # Open the connection pool after migrations so the schema is up to date.
    pool = ConnectionPool(settings.database_url, min_size=1, max_size=10)
    pool.wait()
    logger.info("Connection pool opened (min=1, max=10).")
    app.state.db_pool = pool

    # First-run bootstrap token (issue #106 / ADR 0002).
    with pool.connection() as conn:
        ensure_startup_token(operators_empty=operators_empty(conn))

        # Master-key bootstrap (#114 / ADR-0003). Must run AFTER the
        # pool is open (we need the connection to verify ciphertext)
        # and BEFORE yield (so the boot state is fixed before the
        # first request lands). Never raises on a missing file -- a
        # missing file with existing credentials puts the app in
        # recovery_required mode and the frontend routes to /recover.
        # Does raise on EBULL_SECRETS_KEY mismatch with existing
        # ciphertext (fail-loud, ADR-0003 §6).
        boot = master_key.bootstrap(conn)
    app.state.boot_state = boot.state
    app.state.needs_setup = boot.needs_setup
    app.state.recovery_required = boot.recovery_required
    app.state.broker_key_loaded = boot.broker_encryption_key is not None
    if boot.broker_encryption_key is not None:
        set_broker_encryption_key(boot.broker_encryption_key)
    logger.info(
        "Master-key bootstrap: state=%s needs_setup=%s recovery_required=%s",
        boot.state,
        boot.needs_setup,
        boot.recovery_required,
    )

    # Sync orchestrator: reap orphaned runs BEFORE the scheduler starts
    # so the partial-unique-index gate is clear for the first new sync.
    # Crash here must not block boot — log loud, continue.
    #
    # reap_all=True: orchestrator runs in-process, so any `status='running'`
    # row at boot is from a prior dead process — there is no live sync to
    # preserve. The age-based predicate would miss rows started within the
    # same clock tick when timeout collapses to zero; reap_all bypasses the
    # age check entirely. If the orchestrator ever becomes multi-process,
    # this must switch to an activity-based liveness check.
    try:
        reaped = reap_orphaned_syncs(reap_all=True)
        if reaped:
            logger.info(
                "orchestrator reaper: transitioned %d orphaned sync_runs row(s)",
                reaped,
            )
    except Exception:
        logger.exception("orchestrator reaper failed — continuing startup")

    # Start the in-process job runtime (#13). All SCHEDULED_JOBS are
    # registered with APScheduler; catch-up fires overdue jobs at boot.
    job_runtime: JobRuntime | None
    try:
        job_runtime = start_runtime()
    except Exception:
        # Runtime startup failure must not block the app from booting --
        # the operator can still log in, see system status, and diagnose.
        # We log loud and continue with no runtime; manual trigger
        # endpoints will return 503.
        logger.exception("Job runtime failed to start; continuing without scheduler")
        job_runtime = None
    app.state.job_runtime = job_runtime

    # Register the executor for submit_sync() — only when job_runtime
    # started successfully. If None, submit_sync raises RuntimeError on
    # call; POST /sync still returns 503 via ORCHESTRATOR_ENABLED flag.
    if job_runtime is not None:
        try:
            from app.services.sync_orchestrator import set_executor

            set_executor(job_runtime._manual_executor)
        except Exception:
            logger.exception("failed to register orchestrator executor")

    yield

    # Shut the runtime down BEFORE closing the pool so any in-flight
    # job can still write to job_runs as part of its cleanup. The
    # scheduler.shutdown(wait=True) inside shutdown_runtime() blocks
    # until worker threads return.
    shutdown_runtime(job_runtime)
    app.state.job_runtime = None

    pool.close()
    logger.info("Connection pool closed.")


app = FastAPI(title="eBull", version="0.1.0", lifespan=lifespan)
app.include_router(attribution_router)
app.include_router(auth_setup_router)
app.include_router(auth_bootstrap_router)
app.include_router(auth_session_router)
app.include_router(operators_router)
app.include_router(audit_router)
app.include_router(budget_router)
app.include_router(broker_credentials_router)
app.include_router(config_router)
app.include_router(copy_trading_router)
app.include_router(coverage_router)
app.include_router(filings_router)
app.include_router(instruments_router)
app.include_router(jobs_router)
app.include_router(news_router)
app.include_router(orders_router)
app.include_router(portfolio_router)
app.include_router(recommendations_router)
app.include_router(reports_router)
app.include_router(scores_router)
app.include_router(sync_router)
app.include_router(system_router)
app.include_router(theses_router)


@app.get("/health")
def health(request: Request) -> JSONResponse:
    """Liveness + layer-state rollup.

    200 when every layer is HEALTHY / DEGRADED / RUNNING / RETRYING /
    CASCADE_WAITING / DISABLED (self-healing or operator-gated states).
    503 when ANY layer is ACTION_NEEDED or SECRET_MISSING — external
    monitoring should alert.
    Falls through to 503 with system_state="error" when the state
    machine itself cannot be evaluated (DB pool exhausted, DB down,
    etc.). Acquires the connection inline rather than via
    `Depends(get_conn)` so pool checkout failures map to the same
    503 JSON shape instead of FastAPI's default 500 HTML.

    Trading-mode flags intentionally NOT returned here.  They live in
    runtime_config (DB-backed) and are exposed via /config — surfacing the
    env-backed values would be misleading and stale (issue #56).
    """
    base: dict[str, object] = {
        "env": settings.app_env,
        "etoro_env": settings.etoro_env,
    }
    try:
        with request.app.state.db_pool.connection() as conn:
            states = compute_layer_states_from_db(conn)
    except Exception:
        logger.exception("/health: compute_layer_states_from_db failed")
        return JSONResponse(
            {"status": "error", "system_state": "error", **base},
            status_code=503,
        )
    needs_attention = any(s in {LayerState.ACTION_NEEDED, LayerState.SECRET_MISSING} for s in states.values())
    return JSONResponse(
        {
            "status": "ok",
            "system_state": "needs_attention" if needs_attention else "ok",
            **base,
        },
        status_code=503 if needs_attention else 200,
    )


@app.get("/health/db")
def health_db(conn: psycopg.Connection[object] = Depends(get_conn)) -> dict:
    """Returns migration history and list of public tables in the database."""
    try:
        migrations = migration_status(conn)
    except Exception as exc:
        return {"db_reachable": False, "db_error": str(exc), "tables": [], "migrations": []}

    try:
        tables = [
            row[0]  # type: ignore[index]  # TupleRow from default row factory
            for row in conn.execute(
                """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
                ORDER BY tablename
                """
            )
        ]
        db_ok = True
        db_error = None
    except Exception as exc:
        tables = []
        db_ok = False
        db_error = str(exc)

    return {
        "db_reachable": db_ok,
        "db_error": db_error,
        "tables": tables,
        "migrations": migrations,
    }


# ---------------------------------------------------------------------------
# Coverage tier override
# ---------------------------------------------------------------------------


class TierOverrideRequest(BaseModel):
    instrument_id: int
    new_tier: int = Field(ge=1, le=3)
    rationale: str = Field(min_length=1)


@app.post("/coverage/override", dependencies=[Depends(require_session_or_service_token)])
def coverage_override(
    body: TierOverrideRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> dict:
    """Manually override an instrument's coverage tier."""
    try:
        change = override_tier(
            conn=conn,
            instrument_id=body.instrument_id,
            new_tier=body.new_tier,
            rationale=body.rationale,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "instrument_id": change.instrument_id,
        "old_tier": change.old_tier,
        "new_tier": change.new_tier,
        "change_type": change.change_type,
        "rationale": change.rationale,
    }


# ---------------------------------------------------------------------------
# Data health
# ---------------------------------------------------------------------------

# Deprecated: superseded by GET /system/status (issue #57). The new router
# carries the canonical operator visibility surface (per-layer freshness, job
# health, kill switch). This route is kept temporarily so any existing
# operator scripts continue to work; remove once the admin page (#64) ships.


@app.get("/health/data", dependencies=[Depends(require_session_or_service_token)], deprecated=True, tags=["system"])
def health_data(conn: psycopg.Connection[object] = Depends(get_conn)) -> dict:
    """Deprecated alias for ``GET /system/status``.

    Job names are sourced from ``SCHEDULED_JOBS`` so this endpoint cannot
    drift from the registry — both /health/data and /system/status report on
    the same job set.
    """
    try:
        report = get_system_health(conn, job_names=[job.name for job in SCHEDULED_JOBS])
    except Exception as exc:
        # Fixed-string detail; full exception text goes to logger only.
        # See review-prevention-log entry on 5xx HTTPException leaks.
        logger.exception("/health/data: failed to build system health report")
        raise HTTPException(status_code=503, detail="health data unavailable") from exc

    return {
        "checked_at": report.checked_at.isoformat(),
        "kill_switch": {
            "active": report.kill_switch_active,
            "detail": report.kill_switch_detail,
        },
        "layers": [
            {
                "layer": lh.layer,
                "status": lh.status,
                "latest": lh.latest.isoformat() if lh.latest else None,
                "max_age_seconds": lh.max_age.total_seconds() if lh.max_age else None,
                "age_seconds": lh.age.total_seconds() if lh.age else None,
                "detail": lh.detail,
            }
            for lh in report.layers
        ],
        "jobs": [
            {
                "job_name": jh.job_name,
                "last_status": jh.last_status,
                "last_started_at": jh.last_started_at.isoformat() if jh.last_started_at else None,
                "last_finished_at": jh.last_finished_at.isoformat() if jh.last_finished_at else None,
                "detail": jh.detail,
            }
            for jh in report.jobs
        ],
    }


# ---------------------------------------------------------------------------
# Kill switch — deprecated alias
# ---------------------------------------------------------------------------
#
# The canonical route is POST /config/kill-switch (issue #56).  This alias is
# kept temporarily so any existing operator scripts do not break in this
# release.  It delegates to the same handler, so behaviour and audit writes
# are identical.  Remove once the settings UI (#65) lands.


@app.post(
    "/kill-switch",
    dependencies=[Depends(require_session_or_service_token)],
    deprecated=True,
    tags=["config"],
)
def set_kill_switch_deprecated(
    body: KillSwitchRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> KillSwitchResponse:
    """Deprecated alias for POST /config/kill-switch."""
    return post_kill_switch(body, conn)
