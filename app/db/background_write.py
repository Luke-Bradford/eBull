"""Process-global background-write connection seam (#1472 PR4c).

The jobs-process ``BackgroundConnectionPool`` (``app/jobs/background_pool.py``)
is a singleton, but the background WRITERS that borrow from it — the
sync-orchestrator audit / progress writers in
``app/services/sync_orchestrator/executor.py`` — live in modules also reachable
from the API, tests, and one-off CLI scripts where no bg pool exists.

This seam lives in ``app.db`` (a leaf package: imports only ``app.config`` +
``psycopg``) ON PURPOSE. The writers are under ``app.services`` and ``app.jobs``
already imports ``app.services`` (``runtime`` → ``executor``); if the seam lived
in ``app.jobs.background_pool`` then ``executor`` importing it would close a
``services → jobs → services`` import cycle. Keeping it in ``app.db`` lets every
writer import it without perturbing module-load order.

The jobs ``serve()`` registers the open pool here at boot (before any sync work
can dispatch) and clears it (``None``) in the shutdown finally BEFORE closing
the pool. ``background_write_connection`` borrows from the registered pool when
set, else falls back to a fresh raw ``autocommit=True`` connection (the pre-PR4c
behaviour) so API / test / CLI callers are unchanged.
"""

from __future__ import annotations

import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import psycopg
from psycopg_pool import PoolClosed

from app.config import settings

if TYPE_CHECKING:
    from app.jobs.background_pool import BackgroundConnectionPool


class BackgroundPoolClosed(RuntimeError):
    """Raised by ``BackgroundConnectionPool.connection`` when the pool is
    closed. Lives in this leaf module (not ``app.jobs.background_pool``) so
    ``background_write_connection`` can catch it without importing ``app.jobs``
    at runtime (that would close the services→jobs→services cycle). The pool
    class imports it from here."""


_GLOBAL_BACKGROUND_POOL: BackgroundConnectionPool | None = None
_GLOBAL_LOCK = threading.Lock()


def set_background_pool(pool: BackgroundConnectionPool | None) -> None:
    """Register (or clear) the jobs-process background pool.

    ``serve()`` sets it immediately after opening the pool — before any sync
    work can dispatch — and clears it (``None``) in the shutdown finally BEFORE
    closing the pool, so a late write degrades to the raw fallback instead of
    borrowing a closing pool. Lock-guarded; assignment is atomic but the lock
    pairs the set/get for clarity + future-proofing."""
    global _GLOBAL_BACKGROUND_POOL
    with _GLOBAL_LOCK:
        _GLOBAL_BACKGROUND_POOL = pool


def get_background_pool() -> BackgroundConnectionPool | None:
    with _GLOBAL_LOCK:
        return _GLOBAL_BACKGROUND_POOL


@contextmanager
def background_write_connection() -> Iterator[psycopg.Connection[Any]]:
    """Yield an autocommit connection for a short background WRITE.

    Borrows from the registered jobs-process ``BackgroundConnectionPool`` when
    set (bounded, self-healing), else opens a fresh raw ``autocommit=True``
    connection (API / tests / CLI). BOTH paths are ``autocommit=True``, so a
    writer's ``with conn.transaction()`` issues the same real BEGIN/COMMIT
    either way — the pool and fallback are semantically identical.

    Shutdown robustness (Codex PR4c-ckpt-2): if the registered pool is CLOSED
    when the borrow is attempted — a sync still finalizing during jobs shutdown
    after ``sync_executor.shutdown(wait=False)`` returns, racing
    ``background_pool.close()`` — the write degrades to the raw fallback instead
    of raising. ONLY a closed pool triggers the fallback; a ``PoolTimeout`` from
    normal saturation/ill-health is NOT caught, so the bounding guarantee holds
    during normal operation (and the pool's own hard-recreate still fires).
    """
    pool = get_background_pool()
    if pool is not None:
        try:
            cm = pool.connection()
            conn = cm.__enter__()
        except BackgroundPoolClosed, PoolClosed:
            pass  # pool closed (shutdown race) → fall through to raw
        else:
            try:
                yield conn
            finally:
                cm.__exit__(*sys.exc_info())
            return
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        yield conn
