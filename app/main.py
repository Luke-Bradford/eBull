from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import psycopg
from fastapi import Depends, FastAPI, HTTPException
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field, model_validator

from app.api.audit import router as audit_router
from app.api.auth import require_auth
from app.api.filings import router as filings_router
from app.api.instruments import router as instruments_router
from app.api.news import router as news_router
from app.api.portfolio import router as portfolio_router
from app.api.recommendations import router as recommendations_router
from app.api.scores import router as scores_router
from app.api.theses import router as theses_router
from app.config import settings
from app.db import get_conn
from app.db.migrations import migration_status, run_migrations
from app.services.coverage import override_tier
from app.services.ops_monitor import (
    activate_kill_switch,
    deactivate_kill_switch,
    get_system_health,
)

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

    yield

    pool.close()
    logger.info("Connection pool closed.")


app = FastAPI(title="eBull", version="0.1.0", lifespan=lifespan)
app.include_router(audit_router)
app.include_router(filings_router)
app.include_router(instruments_router)
app.include_router(news_router)
app.include_router(portfolio_router)
app.include_router(recommendations_router)
app.include_router(scores_router)
app.include_router(theses_router)


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "env": settings.app_env,
        "etoro_env": settings.etoro_env,
        "auto_trading_enabled": settings.enable_auto_trading,
        "live_trading_enabled": settings.enable_live_trading,
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


@app.post("/coverage/override", dependencies=[Depends(require_auth)])
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

# Job names that the scheduler uses — listed here so the health endpoint
# can report on each without coupling to the scheduler module.
# Keep in sync with app/workers/scheduler.py job function names.
_KNOWN_JOBS: list[str] = [
    "nightly_universe_sync",
    "hourly_market_refresh",
    "daily_cik_refresh",
    "daily_research_refresh",
    "daily_news_refresh",
    "daily_thesis_refresh",
    "morning_candidate_review",
    "weekly_coverage_review",
    "daily_tax_reconciliation",
]


@app.get("/health/data", dependencies=[Depends(require_auth)])
def health_data(conn: psycopg.Connection[object] = Depends(get_conn)) -> dict:
    """Per-layer staleness status, job health, and kill switch state."""
    try:
        report = get_system_health(conn, job_names=_KNOWN_JOBS)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

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
# Kill switch
# ---------------------------------------------------------------------------


class KillSwitchRequest(BaseModel):
    active: bool
    reason: str = ""
    activated_by: str = ""

    @model_validator(mode="after")
    def reason_required_when_active(self) -> KillSwitchRequest:
        if self.active and not self.reason.strip():
            raise ValueError("reason is required when activating the kill switch")
        return self


@app.post("/kill-switch", dependencies=[Depends(require_auth)])
def set_kill_switch(
    body: KillSwitchRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> dict:
    """Activate or deactivate the system-wide kill switch."""
    try:
        if body.active:
            activate_kill_switch(conn, reason=body.reason, activated_by=body.activated_by)
        else:
            deactivate_kill_switch(conn)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {"active": body.active, "reason": body.reason}
