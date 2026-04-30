"""Hardened ``psycopg_pool.ConnectionPool`` factory.

Extracted from ``app/main.py`` (#719) so both the FastAPI process and the
out-of-process jobs runtime can share a single source of truth for the
defences that #717 introduced. Adding a third pool elsewhere with a raw
``ConnectionPool(...)`` call is the regression shape the prevention-log
entry guards against.
"""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg_pool import ConnectionPool

from app.config import settings

# Pool hardening (#717). The dev stack went unresponsive after ~6h of
# uptime when a Docker port-forwarder socket silently died — the pool
# kept handing out the half-open conn and every request then blocked
# on a TCP read that would never complete. Defence is layered:
#
#   1. TCP keepalives (libpq-level) so the OS detects dead peers in
#      ~60s rather than the default ~2h.
#   2. ``check=ConnectionPool.check_connection`` runs SELECT 1 on every
#      checkout (~1ms) — catches conns the OS hasn't yet flagged dead.
#   3. ``max_idle`` / ``max_lifetime`` proactively recycle conns so
#      one bad conn cannot wedge the pool for the rest of uptime.
#   4. ``timeout`` caps how long ``pool.connection()`` will wait, so a
#      saturated or wedged pool surfaces as a 503 instead of hanging
#      the asyncio event loop forever.
_POOL_CONNECTION_KWARGS: dict[str, int] = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 3,
}


def open_pool(name: str, *, min_size: int, max_size: int) -> ConnectionPool[psycopg.Connection[Any]]:
    """Open a hardened psycopg ConnectionPool. Caller owns the pool's lifetime.

    The settings module's ``database_url`` is read at call time, so a
    test that monkey-patches it before invoking ``open_pool`` gets a
    pool against the override URL — no caching, no closure capture.
    """
    pool: ConnectionPool[psycopg.Connection[Any]] = ConnectionPool(
        settings.database_url,
        min_size=min_size,
        max_size=max_size,
        kwargs=_POOL_CONNECTION_KWARGS,
        check=ConnectionPool.check_connection,
        max_idle=600.0,
        max_lifetime=1800.0,
        timeout=15.0,
        name=name,
    )
    pool.wait()
    return pool
