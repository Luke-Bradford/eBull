"""Database connection pool and FastAPI dependency.

Pool lifecycle:
  - Created in the FastAPI lifespan and stored on ``app.state.db_pool``.
  - Closed when the app shuts down.

Usage in route handlers::

    from app.db import get_conn

    @router.get("/example")
    def example(conn: psycopg.Connection = Depends(get_conn)):
        rows = conn.execute("SELECT ...").fetchall()
"""

from __future__ import annotations

import logging
from collections.abc import Generator

import psycopg
from fastapi import HTTPException, Request
from psycopg_pool import PoolTimeout

logger = logging.getLogger(__name__)


def get_conn(request: Request) -> Generator[psycopg.Connection[object]]:
    """FastAPI dependency that checks out a connection from the pool.

    The connection is returned to the pool when the request completes.

    A ``PoolTimeout`` (the pool's hardened checkout cap fired — the pool
    is saturated or every conn is wedged) maps to a 503 so routes
    surface the failure cleanly instead of the asyncio loop blocking
    on the await or FastAPI defaulting to a 500. See #717.
    """
    try:
        with request.app.state.db_pool.connection() as conn:
            yield conn
    except PoolTimeout:
        logger.warning("db pool checkout timed out — returning 503")
        raise HTTPException(status_code=503, detail="database temporarily unavailable") from None
