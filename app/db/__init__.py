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

# Errors that mean "no usable connection right now" → 503, not 500.
# Hoisted to a module constant (rather than an inline `except (A, B):`)
# because ruff 0.15.9's formatter strips the parens off a bindless
# parenthesised except-tuple, producing invalid Python 3
# (`except A, B:`); `except _NAME:` sidesteps that. Do not inline.
_DB_UNAVAILABLE_ERRORS = (PoolTimeout, psycopg.OperationalError)


def get_conn(request: Request) -> Generator[psycopg.Connection[object]]:
    """FastAPI dependency that checks out a connection from the pool.

    The connection is returned to the pool when the request completes.

    Any failure to hand out a usable connection maps to a **503**, so
    routes surface the failure cleanly instead of FastAPI defaulting to
    a 500 (or an ``AttributeError`` when the pool was never created):

      * ``PoolTimeout`` — the hardened checkout timeout fired (pool
        saturated or every conn wedged). See #717.
      * ``psycopg.OperationalError`` — the DB is unreachable / in
        recovery, so opening a pooled connection fails. This is the
        common dev-PG-down case (#1325 / #1217): without it, every
        ``/system/*`` diagnostic endpoint 500s exactly when the
        operator needs the 503 "service unavailable" signal to tell
        "DB is down" apart from "auth is wrong".
      * Pool missing / ``None`` — lifespan never created it (startup
        failed) or it was torn down. Treat as unavailable, not a 500.

    The ``detail`` is a fixed phrase — never the exception text
    (prevention-log #86: no driver/SQL error leakage into response
    bodies). Full text goes to the server log only.

    Scope the catch to ``enter_context`` only — a route handler that
    somehow raises ``PoolTimeout`` / ``OperationalError`` inside its
    body must propagate untouched (FastAPI / route exception handlers
    see the original exception). PR #718 round 1 review caught the
    wider catch swallowing handler-side `generator.throw(...)`.
    """
    pool = getattr(request.app.state, "db_pool", None)
    if pool is None:
        logger.warning("db pool absent (lifespan not started or torn down) — returning 503")
        raise HTTPException(status_code=503, detail="database temporarily unavailable")
    with contextlib.ExitStack() as stack:
        try:
            conn = stack.enter_context(pool.connection())
        except _DB_UNAVAILABLE_ERRORS:
            logger.warning("db connection unavailable — returning 503", exc_info=True)
            raise HTTPException(status_code=503, detail="database temporarily unavailable") from None
        yield conn
