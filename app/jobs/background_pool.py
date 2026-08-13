"""Bounded, self-healing background connection pool for the jobs process (#1472 PR4b).

The jobs process opens many short-lived raw ``psycopg.connect`` per cadence
boundary for high-frequency background WRITES (progress / result / audit
rows, bookkeeping). At a ``:00``/``:05`` boundary that per-fire churn is the
connection herd that was the #1472 RCA. This pool gives those writers a
single bounded place to borrow from, capping the jobs process's concurrent
footprint regardless of cadence concurrency (settled #719: build it via
``open_pool``, not a raw ``ConnectionPool``).

It is a thin SELF-HEALING wrapper over ``open_pool(autocommit=True)``:

- Pooled connections are ``autocommit=True`` so a borrowing writer's
  ``with conn.transaction()`` issues a real BEGIN/COMMIT (on a non-autocommit
  pooled conn it would be a SAVEPOINT — psycopg3 quirk — silently breaking
  durability / rollback isolation).
- ``check=check_connection`` (inherited from ``open_pool``) discards a dead
  conn on checkout, but that only heals one conn at a time. After a Postgres
  restart the whole pool can wedge; ``hard-recreate`` rebuilds the underlying
  pool after ``recreate_after`` consecutive CHECKOUT failures.

Concurrency model (Codex PR4b-ckpt-1b): multiple subsystem threads share the
pool. Failures + recreates are tracked by POOL GENERATION under a lock so a
failure on a stale generation can neither double-recreate nor reset the new
pool's counter. The new pool is built OUTSIDE the lock (``open_pool`` waits on
``min_size``, which can block), then swapped in under the lock; the retired
pool is closed OUTSIDE the lock so its ``close()`` drains in-flight borrows
without holding up new checkouts.

This module is PURE INFRASTRUCTURE in PR4b — no writer borrows from it yet.
PR4c sweeps the eligible raw-connect background writers onto it (the pool is
the contract that must exist first, so the sweep is consistent).
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, Final

import psycopg
from psycopg_pool import ConnectionPool, PoolTimeout

from app.db.background_write import BackgroundPoolClosed
from app.db.pool import BACKGROUND_POOL_MAX_SIZE, open_pool

logger = logging.getLogger(__name__)

BACKGROUND_POOL_APPLICATION_NAME: Final[str] = "ebull-jobs-background-pool"
"""``application_name`` stamped on this pool's connections (PR3 convention)
so its footprint is attributable in ``pg_stat_activity``."""

_DEFAULT_RECREATE_AFTER_CONSECUTIVE_FAILURES: Final[int] = 3
"""Consecutive CHECKOUT failures on the current generation before a
hard-recreate. >1 so a single transient blip does not churn the pool."""

_POOL_NAME: Final[str] = "jobs_background_pool"


class BackgroundConnectionPool:
    """Self-healing bounded pool for jobs-process background writes (#1472 PR4b)."""

    def __init__(
        self,
        *,
        max_size: int = BACKGROUND_POOL_MAX_SIZE,
        recreate_after: int = _DEFAULT_RECREATE_AFTER_CONSECUTIVE_FAILURES,
    ) -> None:
        self._max_size = max_size
        self._recreate_after = recreate_after
        self._lock = threading.Lock()
        self._closed = False
        self._recreating = False
        self._generation = 0
        self._consecutive_checkout_failures = 0
        self._last_recreate_at: datetime | None = None
        self._metrics: dict[str, int] = {
            "checkouts": 0,
            "pool_timeouts": 0,
            "operational_errors": 0,
            "hard_recreates": 0,
        }
        self._pool = self._build_pool()

    def _build_pool(self) -> ConnectionPool[psycopg.Connection[Any]]:
        return open_pool(
            _POOL_NAME,
            min_size=1,
            max_size=self._max_size,
            autocommit=True,
            application_name=BACKGROUND_POOL_APPLICATION_NAME,
        )

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection[Any]]:
        """Borrow a connection for a short background write.

        On a CHECKOUT failure (``PoolTimeout`` / ``OperationalError`` before a
        conn is handed out) the failure is counted against the current pool
        generation and may trigger a hard-recreate. A failure raised by the
        caller's WORK (after the conn is handed out) is NOT a pool-health
        signal — it propagates without touching the recreate accounting.

        Saturation vs ill-health (Codex PR4b-ckpt-2 MEDIUM): a ``PoolTimeout``
        normally means "pool full for ``timeout`` seconds". For THIS pool that
        is an ill-health signal, not legitimate saturation: every borrower does
        a single sub-millisecond upsert, so ``max_size`` short writes cannot
        keep all conns busy for the 15s checkout timeout — a timeout that long
        means the conns are wedged (e.g. a Postgres restart left half-open
        sockets that ``check_connection`` cannot replace fast enough), which is
        exactly what the hard-recreate heals. Boundedness holds across a
        recreate: a CHECKOUT failure means the borrower never acquired a conn,
        so at recreate time the OLD pool has at most ``min_size`` idle conns
        outstanding; its ``close()`` drains those before dropping them, so the
        transient old+new overlap stays small and bounded.
        """
        with self._lock:
            if self._closed:
                # BackgroundPoolClosed (a RuntimeError subclass) so the
                # background-write seam can catch the shutdown race precisely
                # and fall back to a raw connection (#1472 PR4c-ckpt-2).
                raise BackgroundPoolClosed("BackgroundConnectionPool is closed")
            pool = self._pool
            gen = self._generation

        checked_out = False
        try:
            with pool.connection() as conn:
                checked_out = True
                with self._lock:
                    self._metrics["checkouts"] += 1
                    if gen == self._generation:
                        self._consecutive_checkout_failures = 0
                yield conn
        except PoolTimeout:
            if not checked_out:
                self._on_checkout_failure(gen, "pool_timeouts")
            raise
        except psycopg.OperationalError:
            if not checked_out:
                self._on_checkout_failure(gen, "operational_errors")
            raise

    def _on_checkout_failure(self, gen: int, metric_key: str) -> None:
        old_pool: ConnectionPool[psycopg.Connection[Any]] | None = None
        with self._lock:
            self._metrics[metric_key] += 1
            # Ignore failures from a stale generation (another thread already
            # recreated), while a recreate is in flight, or after close.
            if self._closed or gen != self._generation or self._recreating:
                return
            self._consecutive_checkout_failures += 1
            if self._consecutive_checkout_failures < self._recreate_after:
                return
            # Claim the recreate so concurrent failers don't double-build.
            self._recreating = True
            old_pool = self._pool

        # Build the replacement OUTSIDE the lock — open_pool waits on min_size,
        # which can block (and raise) while Postgres is still unreachable.
        try:
            new_pool = self._build_pool()
        except Exception:
            logger.exception("background pool: hard-recreate could not open a new pool; keeping the old one")
            with self._lock:
                self._recreating = False
                # Leave the consecutive counter at the threshold so the NEXT
                # checkout failure retries the recreate rather than waiting for
                # another full streak.
                self._consecutive_checkout_failures = self._recreate_after
            return

        with self._lock:
            if self._closed:
                # Shutdown won the race while we were building outside the lock
                # (Codex PR4b-ckpt-2 HIGH). ``close()`` already closed the old
                # pool; discard the freshly built one rather than swap it onto a
                # closed wrapper (which would leak an open pool).
                self._recreating = False
                try:
                    new_pool.close()
                except Exception:
                    logger.exception("background pool: discarding post-close recreate raised")
                return
            self._pool = new_pool
            self._generation += 1
            generation = self._generation
            self._consecutive_checkout_failures = 0
            self._recreating = False
            self._metrics["hard_recreates"] += 1
            self._last_recreate_at = datetime.now(UTC)

        logger.warning(
            "background pool: hard-recreated after %d consecutive checkout failures (generation=%d)",
            self._recreate_after,
            generation,
        )
        # Close the retired pool OUTSIDE the lock; close() drains outstanding
        # borrows so in-flight short writes finish before its conns drop.
        if old_pool is not None:
            try:
                old_pool.close()
            except Exception:
                logger.exception("background pool: retired-pool close raised")

    def metrics(self) -> dict[str, Any]:
        """Snapshot of counters for operator surfacing / tests."""
        with self._lock:
            snapshot: dict[str, Any] = dict(self._metrics)
            snapshot["generation"] = self._generation
            snapshot["last_recreate_at"] = (
                self._last_recreate_at.isoformat() if self._last_recreate_at is not None else None
            )
            return snapshot

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            pool = self._pool
        try:
            pool.close()
        except Exception:
            logger.exception("background pool: close raised")
