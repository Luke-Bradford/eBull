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
from typing import TYPE_CHECKING, Final

import httpx

from app.providers.implementations.sec_edgar import (
    SubmissionsPageResult,
)

if TYPE_CHECKING:
    from app.providers.rate_gate import RateGate

logger = logging.getLogger(__name__)


# Real-world ceiling per the datamule "hidden cumulative rate limit"
# article + SEC's published 10 req/s fair-use policy. 7 leaves
# headroom for retransmits.
DEFAULT_TARGET_RPS: Final[float] = 7.0
DEFAULT_CONCURRENCY: Final[int] = 4
DEFAULT_TIMEOUT_S: Final[float] = 30.0


# #1341 — caller-managed chunk size for the chunk-and-drain pattern
# in `walk_files_pages`. Bounded peak heap at ~150-200 MB per chunk
# (≈50 MB raw JSON + Python overhead). Tunable via module constant
# without invoker shape change.
DEFAULT_PREFETCH_CHUNK_SIZE: Final[int] = 1000


# Sentinel host check — `SubmissionsPageResult` is keyed by submissions
# page-name (e.g. `CIKxxxxxxxxxx-submissions-001.json`); URL is
# constructed below.
_SEC_SUBMISSIONS_URL_PREFIX: Final[str] = "https://data.sec.gov/submissions/"


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
        gate: RateGate | None = None,
        shared_clock: list[float] | None = None,
        shared_lock: threading.Lock | None = None,
    ) -> None:
        if target_rps <= 0:
            raise ValueError("target_rps must be > 0")
        # #1484: default to the process-global cross-process gate. An explicit
        # shared_clock (tests) keeps the legacy in-process floor for isolation;
        # target_rps is advisory on the gate path (the gate's floor governs).
        if gate is None and shared_clock is None:
            from app.providers.sec_rate_gate_holder import get_sec_rate_gate

            gate = get_sec_rate_gate()
        self._gate = gate
        self._min_interval = 1.0 / target_rps
        self._clock = shared_clock if shared_clock is not None else [0.0]
        self._lock = shared_lock if shared_lock is not None else threading.Lock()

    async def acquire(self) -> None:
        """Block until the rate limiter authorises one request.

        When a gate is present (#1484), delegates entirely to
        ``gate.acquire_async()`` — the gate's own floor governs.

        Otherwise uses the legacy in-process two-phase floor:
        ``self._clock[0]`` is the LAST-REQUEST TIMESTAMP and the
        floor is ``last + min_interval``. Mixed sync + async traffic
        on the same shared clock therefore observes a single coherent
        floor — Codex pre-push round 2.

        Two-phase: (1) under the threading lock, compute the wait
        duration and stamp the new last-request timestamp at
        ``now + wait`` (i.e. when this request will actually fire);
        (2) release the lock and ``await asyncio.sleep`` outside it
        so other coroutines on the same event loop can still queue.
        """
        if self._gate is not None:
            await self._gate.acquire_async()
            return
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
            shared_clock=shared_clock,  # None -> _AsyncRateLimiter uses the cross-process gate
            shared_lock=shared_lock,
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


# ---------------------------------------------------------------------------
# Sync wrapper for D-stage services (#1045)
# ---------------------------------------------------------------------------


def prefetch_document_texts(
    urls: list[str],
    *,
    user_agent: str,
    target_rps: float = DEFAULT_TARGET_RPS,
    concurrency: int = DEFAULT_CONCURRENCY,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> dict[str, str | None]:
    """Bulk-fetch SEC document bodies via the pipelined fetcher.

    Sync wrapper that builds an event loop, runs ``PipelinedSecFetcher.fetch_many``
    against ``urls``, and returns a ``{url: body_or_None}`` dict.

    Bodies returned as decoded text (``response.text``). 404/410 responses
    map to ``None`` (filing withdrawn — same semantics as
    ``SecFilingsProvider.fetch_document_text``). Transport errors,
    429, 5xx, and other non-permanent failures are OMITTED from the
    returned dict so ``_CachedDocFetcher``'s cache-miss path falls
    through to the underlying sync provider's retry / quarantine
    contract.

    Designed for D-stage services (sec_def14a / sec_business_summary /
    sec_8k_events) that previously fetched per-filing serially. Hand
    the candidate URL list here; iterate the result dict in the
    existing per-filing loop without changing parsing logic.

    Acquires the same shared SEC rate clock as the synchronous
    ``ResilientClient`` SEC traffic, so concurrent jobs can co-exist
    safely under the per-IP 7 req/s ceiling.
    """
    if not urls:
        return {}
    deduped = list(dict.fromkeys(urls))
    headers = {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
    }

    async def _run() -> dict[str, str | None]:
        out: dict[str, str | None] = {}
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            fetcher = PipelinedSecFetcher(
                client=client,
                target_rps=target_rps,
                concurrency=concurrency,
            )
            tasks = [FetchTask(key=u, url=u, headers=headers) for u in deduped]
            async for result in fetcher.fetch_many(tasks):
                key = str(result.key)
                # Failure-mode parity with sync fetch_document_text:
                # ONLY cache None for permanent 404/410. Transport
                # errors, 429, 5xx, 4xx are OMITTED so the cache-miss
                # path falls through to the underlying sync provider,
                # preserving its retry/quarantine contract. Codex
                # pre-push MED for #1045.
                if result.error is not None or result.response is None:
                    continue
                resp = result.response
                if resp.status_code in (404, 410):
                    out[key] = None
                    continue
                if 200 <= resp.status_code < 300:
                    out[key] = resp.text
                # Else: omit from cache.
        return out

    return asyncio.run(_run())


class _CachedDocFetcher:
    """Wraps a sync ``SecFilingsProvider`` with a prefetch cache.

    D-stage ingest loops call ``fetcher.fetch_document_text(url)``
    serially. When a bootstrap entrypoint pre-fetches the cohort
    URLs via ``prefetch_document_texts`` ahead of the loop, this
    wrapper serves cached bodies on hit and falls back to the
    underlying sync fetcher on miss (e.g. URL added between prefetch
    and ingest).

    Bookkeeping for telemetry: ``cache_hits`` / ``cache_misses``
    counters expose how much of the cohort the prefetch covered.
    """

    def __init__(self, underlying: object, cache: dict[str, str | None]) -> None:
        self._underlying = underlying
        self._cache = cache
        self.cache_hits = 0
        self.cache_misses = 0

    def fetch_document_text(self, absolute_url: str) -> str | None:
        if absolute_url in self._cache:
            self.cache_hits += 1
            return self._cache[absolute_url]
        self.cache_misses += 1
        # type: ignore[misc, attr-defined] — duck-typed fallback.
        return self._underlying.fetch_document_text(absolute_url)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# #1341 — secondary-submissions conditional prefetch for S14
# (`sec_submissions_files_walk`) bootstrap path.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConditionalFetchTask:
    """Per-page prefetch task carrying its own If-Modified-Since header.

    Spec: docs/proposals/etl/1341-s14-pipelined-fetch.md §3.1.
    """

    page_name: str
    if_modified_since: str | None


def prefetch_submissions_pages_conditional(
    tasks: list[ConditionalFetchTask],
    *,
    user_agent: str,
    target_rps: float = DEFAULT_TARGET_RPS,
    concurrency: int = DEFAULT_CONCURRENCY,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> dict[str, SubmissionsPageResult | None]:
    """Bulk-fetch ONE CHUNK of SEC secondary submissions pages.

    Caller (S14 walker) is responsible for chunking the full cohort
    via the chunk-and-drain pattern at the walker; this function
    fetches every task in `tasks` and returns one cache dict. The
    caller drains the dict and drops it before invoking again for
    the next chunk so peak heap stays bounded.

    Returns ``{page_name: SubmissionsPageResult | None}``:

    * key absent — fetch failed (transport / 429 / 5xx / malformed
      body). Caller's cache-miss fallthrough hits the sync provider,
      which owns the retry/quarantine contract.
    * value ``None`` — 404; page absent.
    * value ``SubmissionsPageResult(payload=None, last_modified=ims,
      not_modified=True)`` — 304.
    * value ``SubmissionsPageResult(payload=<dict>, last_modified=lm,
      not_modified=False)`` — 200.

    Per-task failures isolated via ``try/except``; one bad page never
    aborts the chunk.

    Mirrors ``prefetch_document_texts`` lifecycle: shared
    ``_PROCESS_RATE_LIMIT_CLOCK`` so concurrent sync SEC traffic
    co-exists under the 7 req/s ceiling.
    """
    if not tasks:
        return {}
    # Dedupe by page_name (defensive — sidecar PK guarantees
    # uniqueness, but a chunking caller could in theory pass dups).
    deduped: dict[str, ConditionalFetchTask] = {}
    for task in tasks:
        deduped.setdefault(task.page_name, task)
    work = list(deduped.values())

    base_headers = {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
    }

    async def _run() -> dict[str, SubmissionsPageResult | None]:
        out: dict[str, SubmissionsPageResult | None] = {}
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            fetcher = PipelinedSecFetcher(
                client=client,
                target_rps=target_rps,
                concurrency=concurrency,
            )
            fetch_tasks = [
                FetchTask(
                    key=task.page_name,
                    url=f"{_SEC_SUBMISSIONS_URL_PREFIX}{task.page_name}",
                    headers={
                        **base_headers,
                        **({"If-Modified-Since": task.if_modified_since} if task.if_modified_since else {}),
                    },
                )
                for task in work
            ]
            ims_lookup = {task.page_name: task.if_modified_since for task in work}
            async for result in fetcher.fetch_many(fetch_tasks):
                page_name = str(result.key)
                # Per-task failure isolation — omit from cache; loop
                # fallthrough hits the sync provider's retry path.
                if result.error is not None or result.response is None:
                    continue
                resp = result.response
                try:
                    if resp.status_code == 304:
                        out[page_name] = SubmissionsPageResult(
                            payload=None,
                            last_modified=ims_lookup.get(page_name),
                            not_modified=True,
                        )
                        continue
                    if resp.status_code == 404:
                        out[page_name] = None
                        continue
                    if 200 <= resp.status_code < 300:
                        payload = resp.json()  # type: ignore[no-untyped-call]
                        if not isinstance(payload, dict):
                            # Malformed body — caller falls through.
                            continue
                        out[page_name] = SubmissionsPageResult(
                            payload=payload,
                            last_modified=resp.headers.get("Last-Modified"),
                            not_modified=False,
                        )
                        continue
                    # 429 / 5xx / other 4xx — OMIT from cache; loop
                    # fallthrough hits the sync provider's retry path.
                except (ValueError, OSError) as exc:
                    # ValueError covers json.JSONDecodeError (subclass).
                    logger.debug(
                        "prefetch_submissions_pages_conditional: malformed body for %s: %s",
                        page_name,
                        exc,
                    )
                    continue
        return out

    return asyncio.run(_run())


class _CachedSubmissionsPageFetcher:
    """Wraps a sync ``SecFilingsProvider`` for the bootstrap S14 path.

    Mirror of ``_CachedDocFetcher`` but for
    ``fetch_submissions_page_conditional`` rather than
    ``fetch_document_text``. Cache lookups honour the per-page
    If-Modified-Since the caller passes; cache misses fall through
    to the underlying provider's sync ResilientClient.

    Cache contract:

    * page_name in cache, value None → 404; return None.
    * page_name in cache, value SubmissionsPageResult → return it.
    * page_name NOT in cache → fall through to underlying provider.

    Telemetry:

    * ``cache_hits`` — page_name in cache (any value, including None).
    * ``cache_misses`` — page_name NOT in cache (caller's per-CIK
      loop visit went to the sync provider).
    """

    def __init__(
        self,
        underlying: object,
        cache: dict[str, SubmissionsPageResult | None],
    ) -> None:
        self._underlying = underlying
        self._cache = cache
        self.cache_hits = 0
        self.cache_misses = 0

    def fetch_submissions_page_conditional(
        self,
        page_name: str,
        *,
        if_modified_since: str | None = None,
    ) -> SubmissionsPageResult | None:
        if page_name in self._cache:
            self.cache_hits += 1
            return self._cache[page_name]
        self.cache_misses += 1
        return self._underlying.fetch_submissions_page_conditional(  # type: ignore[attr-defined,no-any-return]
            page_name, if_modified_since=if_modified_since
        )
