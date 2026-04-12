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

from collections.abc import Generator

import psycopg
from fastapi import Request


def get_conn(request: Request) -> Generator[psycopg.Connection[object]]:
    """FastAPI dependency that checks out a connection from the pool.

    The connection is returned to the pool when the request completes.
    """
    with request.app.state.db_pool.connection() as conn:
        yield conn
