import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI

from app.config import settings
from app.db.migrations import migration_status, run_migrations

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
    yield


app = FastAPI(title="trader-os", version="0.1.0", lifespan=lifespan)


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
def health_db() -> dict:
    """Returns migration history and list of public tables in the database."""
    migrations = migration_status()

    try:
        with psycopg.connect(settings.database_url) as conn:
            tables = [
                row[0]
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
