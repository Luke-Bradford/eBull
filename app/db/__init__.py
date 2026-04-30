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

import contextlib
import logging
from collections.abc import Generator

import psycopg
from fastapi import HTTPException, Request
from psycopg_pool import PoolTimeout

logger = logging.getLogger(__name__)


def get_conn(request: Request) -> Generator[psycopg.Connection[object]]:
    """FastAPI dependency that checks out a connection from the pool.

    The connection is returned to the pool when the request completes.

    A ``PoolTimeout`` raised by the pool's checkout (the hardened
    timeout fired — pool saturated or every conn wedged) maps to a
    503 so routes surface the failure cleanly instead of FastAPI
    defaulting to a 500. See #717.

    Scope the catch to ``enter_context`` only — a route handler that
    somehow raises ``PoolTimeout`` inside its body must propagate
    untouched (FastAPI / route exception handlers see the original
    exception). PR #718 round 1 review caught the wider catch
    swallowing handler-side `generator.throw(PoolTimeout(...))`.
    """
    pool = request.app.state.db_pool
    with contextlib.ExitStack() as stack:
        try:
            conn = stack.enter_context(pool.connection())
        except PoolTimeout:
            logger.warning("db pool checkout timed out — returning 503")
            raise HTTPException(status_code=503, detail="database temporarily unavailable") from None
        yield conn
