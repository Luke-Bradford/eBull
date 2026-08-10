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
def background_write_connection(*, autocommit: bool = True) -> Iterator[psycopg.Connection[Any]]:
    """Yield a bounded connection for a short background write.

    Borrows from the registered jobs-process ``BackgroundConnectionPool`` when
    set (bounded, self-healing), else opens a fresh raw ``autocommit=True``
    connection (API / tests / CLI). The default remains ``autocommit=True``,
    so a writer's ``with conn.transaction()`` issues a real BEGIN/COMMIT.
    ``autocommit=False`` supports existing helpers that own their commit and
    need several statements to stay atomic; pooled state is restored before
    return.

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
        yielded = False
        try:
            with pool.connection() as conn:
                previous_autocommit = conn.autocommit
                if previous_autocommit != autocommit:
                    conn.autocommit = autocommit
                try:
                    yielded = True
                    yield conn
                finally:
                    # A caller using the transactional mode may raise before
                    # its own commit.  Return only an idle, autocommit-restored
                    # connection to this shared pool.
                    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                        conn.rollback()
                    conn.autocommit = previous_autocommit
            return
        except BackgroundPoolClosed, PoolClosed:
            # A closed pool raises at CHECKOUT (before yield) — fall through to
            # the raw fallback (shutdown race). If we had already yielded, the
            # error came from the caller's WORK, not checkout, so re-raise it.
            if yielded:
                raise
    with psycopg.connect(
        settings.database_url,
        autocommit=autocommit,
        application_name="ebull-background-write-fallback",
    ) as conn:
        yield conn
