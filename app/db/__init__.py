"""Database connection pool.

Usage (async context):
    async with get_conn() as conn:
        rows = await conn.execute("SELECT ...")
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import psycopg
from psycopg import AsyncConnection

from app.config import settings


@asynccontextmanager
async def get_conn() -> AsyncIterator[AsyncConnection]:  # type: ignore[type-arg]
    # TODO: replace with a connection pool (e.g. psycopg_pool.AsyncConnectionPool)
    # before any production use — this opens a new connection on every call.
    async with await psycopg.AsyncConnection.connect(settings.database_url) as conn:
        yield conn
