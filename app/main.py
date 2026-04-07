from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import psycopg
from fastapi import Depends, FastAPI, HTTPException
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from app.api.audit import router as audit_router
from app.api.auth import require_session_or_service_token
from app.api.auth_session import router as auth_session_router
from app.api.auth_setup import router as auth_setup_router
from app.api.config import KillSwitchRequest, KillSwitchResponse, post_kill_switch
from app.api.config import router as config_router
from app.api.filings import router as filings_router
from app.api.instruments import router as instruments_router
from app.api.news import router as news_router
from app.api.operators import router as operators_router
from app.api.portfolio import router as portfolio_router
from app.api.recommendations import router as recommendations_router
from app.api.scores import router as scores_router
from app.api.system import router as system_router
from app.api.theses import router as theses_router
from app.config import settings
from app.db import get_conn
from app.db.migrations import migration_status, run_migrations
from app.services.coverage import override_tier
from app.services.operator_setup import ensure_startup_token, operators_empty
from app.services.ops_monitor import get_system_health
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

    # First-run bootstrap token (issue #106 / ADR 0002). On a non-loopback
    # bind with no env-configured token and an empty operators table, this
    # generates a fresh token and prints it to the log exactly once. On
    # loopback / configured-token / non-empty cases it is a no-op.
    with pool.connection() as conn:
        ensure_startup_token(operators_empty=operators_empty(conn))

    yield

    pool.close()
    logger.info("Connection pool closed.")


app = FastAPI(title="eBull", version="0.1.0", lifespan=lifespan)
app.include_router(auth_setup_router)
app.include_router(auth_session_router)
app.include_router(operators_router)
app.include_router(audit_router)
app.include_router(config_router)
app.include_router(filings_router)
app.include_router(instruments_router)
app.include_router(news_router)
app.include_router(portfolio_router)
app.include_router(recommendations_router)
app.include_router(scores_router)
app.include_router(system_router)
app.include_router(theses_router)


@app.get("/health")
def health() -> dict:
    # Trading-mode flags intentionally NOT returned here.  They live in
    # runtime_config (DB-backed) and are exposed via /config — surfacing the
    # env-backed values would be misleading and stale (issue #56).
    return {
        "status": "ok",
        "env": settings.app_env,
        "etoro_env": settings.etoro_env,
    }


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
