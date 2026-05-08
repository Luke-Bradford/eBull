"""Pipelined SEC EDGAR fetcher (#1026).

Phase D of the bulk-datasets-first first-install bootstrap (#1020).
Wraps an ``httpx.AsyncClient`` with a fixed-concurrency pool plus a
shared async rate limiter so per-filing body fetches (DEF 14A,
10-K, 8-K) can issue multiple requests in flight while honouring
SEC's per-IP 10 req/s ceiling (real-world target ~7 req/s, see the
spec's facts table).

Pipelining a 4-way concurrent fetcher at 7 req/s vs sequential at
7 req/s is ~30% faster wall-clock when fetch latency dominates the
inter-request floor (typical for SEC HTML/PDF bodies at 200–500 ms
RTT). Same total request count, same rate ceiling.

Result yield order is COMPLETION ORDER, not request order. Each
``FetchTask`` carries an opaque ``key`` that the caller uses to
associate the result with the original request — caller code must
not rely on positional ordering.

Spec: docs/superpowers/specs/2026-05-08-bulk-datasets-first-bootstrap.md
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from typing import Final

import httpx

from app.providers.implementations.sec_edgar import (
    _PROCESS_RATE_LIMIT_CLOCK,
    _PROCESS_RATE_LIMIT_LOCK,
)

logger = logging.getLogger(__name__)


# Real-world ceiling per the datamule "hidden cumulative rate limit"
# article + SEC's published 10 req/s fair-use policy. 7 leaves
# headroom for retransmits.
DEFAULT_TARGET_RPS: Final[float] = 7.0
DEFAULT_CONCURRENCY: Final[int] = 4
DEFAULT_TIMEOUT_S: Final[float] = 30.0


@dataclass(frozen=True)
class FetchTask:
    """One pending fetch.

    ``key`` is opaque to the fetcher — callers use it to associate
    the result back to its originating context (filing accession,
    instrument id, etc) since results yield in completion order.
    """

    key: object
    url: str
    headers: dict[str, str] | None = None


@dataclass
class FetchResult:
    """Outcome of one fetch.

    On success ``response`` is the ``httpx.Response``; on failure
    ``error`` is a string and ``response`` is ``None``.
    """

    key: object
    response: httpx.Response | None
    error: str | None = None


class _AsyncRateLimiter:
    """Coordinates a shared inter-request floor across coroutines AND
    across the existing synchronous ``ResilientClient`` SEC clients.

    Mirrors the synchronous ``ResilientClient`` shared-clock pattern
    used by the existing per-filing path (#168 / #537 / prevention
    log "Multiple ResilientClient instances sharing a rate limit must
    share throttle state").

    The clock is a one-element ``list[float]`` (next-allowed-time)
    shared with the synchronous SEC clients via
    ``_PROCESS_RATE_LIMIT_CLOCK``; the companion ``threading.Lock``
    ``_PROCESS_RATE_LIMIT_LOCK`` makes the read-modify-write atomic
    across both async coroutines and sync threads. Pipelined fetcher
    acquires the threading lock briefly inside the async context —
    no coroutine sleeps while holding it, so concurrent coroutines
    queue at the lock instead of bursting past the budget.

    Codex pre-push round 1 (PR1026): without sharing the clock, two
    pipelined fetchers can issue 14 req/s combined, and one fetcher
    plus existing sync SEC traffic can exceed the per-IP budget.
    """

    def __init__(
        self,
        target_rps: float,
        *,
        shared_clock: list[float] | None = None,
        shared_lock: threading.Lock | None = None,
    ) -> None:
        if target_rps <= 0:
            raise ValueError("target_rps must be > 0")
        self._min_interval = 1.0 / target_rps
        self._clock = shared_clock if shared_clock is not None else [0.0]
        self._lock = shared_lock if shared_lock is not None else threading.Lock()

    async def acquire(self) -> None:
        """Block until the rate limiter authorises one request.

        Stores the same semantics as the synchronous
        ``ResilientClient._throttle_and_stamp``
        (``app/providers/resilient_client.py:135``):
        ``self._clock[0]`` is the LAST-REQUEST TIMESTAMP, and the
        floor is ``last + min_interval``. Mixed sync + async traffic
        on the same shared clock therefore observes a single coherent
        floor — Codex pre-push round 2.

        Two-phase: (1) under the threading lock, compute the wait
        duration and stamp the new last-request timestamp at
        ``now + wait`` (i.e. when this request will actually fire);
        (2) release the lock and ``await asyncio.sleep`` outside it
        so other coroutines on the same event loop can still queue.
        """
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._clock[0]
            wait = max(0.0, self._min_interval - elapsed)
            # Stamp the projected fire-time — both sync and async
            # readers will observe this as the last-request timestamp.
            self._clock[0] = now + wait
        if wait > 0:
            await asyncio.sleep(wait)


class PipelinedSecFetcher:
    """Fixed-concurrency rate-limited wrapper around ``httpx.AsyncClient``.

    Concurrency cap (semaphore) and rate ceiling (async lock with
    next-allowed-time stamp) are independent guarantees:
    - Concurrency keeps at most N requests in flight simultaneously
      (TCP socket budget + memory budget for buffered responses).
    - Rate ceiling keeps the requests-per-second below SEC's fair-use
      policy.

    A 4-way pool at 7 req/s is the spec default. Both knobs can be
    raised or lowered by the caller for testing.
    """

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        target_rps: float = DEFAULT_TARGET_RPS,
        concurrency: int = DEFAULT_CONCURRENCY,
        shared_clock: list[float] | None = None,
        shared_lock: threading.Lock | None = None,
    ) -> None:
        """Initialise the fetcher.

        Process-wide rate budget is shared with the synchronous SEC
        ``ResilientClient`` instances by default
        (``_PROCESS_RATE_LIMIT_CLOCK`` / ``_PROCESS_RATE_LIMIT_LOCK``).
        Pass explicit ``shared_clock``/``shared_lock`` for tests that
        need an isolated budget, or pass empty lists / ``None`` for
        the default shared one.
        """
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self._client = client
        self._sem = asyncio.Semaphore(concurrency)
        self._rate_limiter = _AsyncRateLimiter(
            target_rps,
            shared_clock=shared_clock if shared_clock is not None else _PROCESS_RATE_LIMIT_CLOCK,
            shared_lock=shared_lock if shared_lock is not None else _PROCESS_RATE_LIMIT_LOCK,
        )

    async def fetch_one(self, task: FetchTask) -> FetchResult:
        """Single bounded fetch — useful for callers that don't need
        a generator interface."""
        async with self._sem:
            await self._rate_limiter.acquire()
            try:
                response = await self._client.get(task.url, headers=task.headers)
                return FetchResult(key=task.key, response=response)
            except (httpx.HTTPError, OSError) as exc:
                return FetchResult(key=task.key, response=None, error=str(exc))

    async def fetch_many(self, tasks: Iterable[FetchTask]) -> AsyncIterator[FetchResult]:
        """Yield ``FetchResult`` instances in COMPLETION ORDER.

        Caller iterates with ``async for result in fetcher.fetch_many(...)``
        and uses ``result.key`` to associate to the originating
        context. Each task acquires the semaphore + rate limiter
        independently so completion-order can interleave arbitrarily.
        """
        # Materialise into a list so we can enumerate without
        # exhausting a one-shot iterable.
        task_list = list(tasks)
        if not task_list:
            return

        # Spawn coroutines first, then drain via ``as_completed``.
        # ``asyncio.as_completed`` returns an iterator of futures
        # that resolve in completion order — which is the contract
        # we promise.
        coros = [self.fetch_one(t) for t in task_list]
        for coro in asyncio.as_completed(coros):
            yield await coro
