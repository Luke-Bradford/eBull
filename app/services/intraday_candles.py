"""Intraday candle fetcher with in-process TTL cache.

Wraps a ``MarketDataProvider`` so chart consumers can fetch sub-day
candles on demand without DB persistence. Daily candles continue to
flow through ``app/services/market_data.py`` and ``price_daily`` —
this module is for the timeframes the daily refresh job does not
cover (#600).

Design contract:

  * No DB persistence. Bars live only in this process's cache.
  * Cache key is ``(instrument_id, interval, count)``. Two pages
    asking for the same window within TTL share one provider call.
  * TTL is interval-aware: shorter for sub-hour intervals (a 1min
    chart should refresh fast); longer for daily/weekly/monthly
    (those barely change intra-session).
  * Eviction is lazy on lookup; we don't run a background sweeper.
    The cache is bounded by ``_MAX_ENTRIES`` so a misbehaving caller
    cannot OOM the worker.

Rate-limit surfacing: the wrapper does NOT translate eToro 429
responses on its own — callers (the API layer) catch
``httpx.HTTPStatusError`` and map to a 503 with ``Retry-After``
themselves. This keeps the cache layer pure.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass

from app.providers.market_data import IntradayBar, IntradayInterval, MarketDataProvider

# Hard cap on cached entries. Each entry is one chart-window worth
# of bars (≤1000 IntradayBar dataclass instances ≈ a few hundred KB).
# 256 entries ≈ ~100MB worst case — comfortably below worker memory.
_MAX_ENTRIES = 256

# TTL by interval. Sub-15-min intervals refresh fast so a stale tick
# isn't visible for long; longer intraday intervals (≥30min) tolerate a
# few minutes of staleness because the next bar is itself minutes away.
_TTL_SHORT_S = 30.0
_TTL_LONG_S = 180.0

_SHORT_TTL_INTERVALS: frozenset[IntradayInterval] = frozenset(
    {"OneMinute", "FiveMinutes", "TenMinutes", "FifteenMinutes"}
)


def _ttl_for(interval: IntradayInterval) -> float:
    return _TTL_SHORT_S if interval in _SHORT_TTL_INTERVALS else _TTL_LONG_S


@dataclass(frozen=True)
class _CacheEntry:
    bars: tuple[IntradayBar, ...]
    expires_at: float


class IntradayCandleCache:
    """LRU + TTL cache for intraday candle windows.

    Thread-safe: a lock guards the OrderedDict against concurrent
    mutation from multiple uvicorn worker threads. Lookup is
    O(1) on the dict; LRU promotion uses ``move_to_end``.
    """

    def __init__(self, max_entries: int = _MAX_ENTRIES) -> None:
        self._max_entries = max_entries
        self._lock = threading.Lock()
        self._entries: OrderedDict[tuple[int, IntradayInterval, int], _CacheEntry] = OrderedDict()
        # Singleflight: per-key inflight Event so concurrent misses
        # collapse to one provider call. Reader threads block on the
        # Event until the leader's `put` lands, then re-read the
        # cache. Mirrors the pattern used elsewhere for fan-out
        # protection.
        self._inflight: dict[tuple[int, IntradayInterval, int], threading.Event] = {}

    def get(self, instrument_id: int, interval: IntradayInterval, count: int) -> tuple[IntradayBar, ...] | None:
        """Return cached bars if fresh, else None."""
        key = (instrument_id, interval, count)
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expires_at <= time.monotonic():
                # Expired: drop and miss. Do NOT return stale bars
                # even with a "is-stale" flag — callers are chart
                # consumers and would have to re-implement freshness
                # logic themselves.
                self._entries.pop(key, None)
                return None
            self._entries.move_to_end(key)
            return entry.bars

    def put(
        self,
        instrument_id: int,
        interval: IntradayInterval,
        count: int,
        bars: list[IntradayBar],
    ) -> None:
        key = (instrument_id, interval, count)
        ttl = _ttl_for(interval)
        entry = _CacheEntry(
            bars=tuple(bars),
            expires_at=time.monotonic() + ttl,
        )
        with self._lock:
            self._entries[key] = entry
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)

    def clear(self) -> None:
        """Reset the cache. Used by tests."""
        with self._lock:
            self._entries.clear()
            # Wake any waiters so a test that resets between cases
            # doesn't deadlock a thread mid-singleflight.
            for event in self._inflight.values():
                event.set()
            self._inflight.clear()

    def claim_or_wait(
        self,
        instrument_id: int,
        interval: IntradayInterval,
        count: int,
        timeout_s: float = 30.0,
    ) -> bool:
        """Coordinate a singleflight fetch for ``(id, interval, count)``.

        Returns True if the caller is the **leader** — it must perform
        the provider fetch and call ``release_inflight`` once done.
        Returns False if another thread is already fetching this key;
        the caller blocks up to ``timeout_s`` waiting for that leader
        to finish, then returns and the caller re-reads the cache.

        On timeout we still return False — the leader may be wedged,
        but a follower's only safe option is to fall through and let
        its own request go to the provider. Better a duplicate fetch
        than a stuck request.
        """
        key = (instrument_id, interval, count)
        with self._lock:
            existing = self._inflight.get(key)
            if existing is None:
                # Leader: install our event, others will wait on it.
                self._inflight[key] = threading.Event()
                return True
        existing.wait(timeout=timeout_s)
        return False

    def release_inflight(self, instrument_id: int, interval: IntradayInterval, count: int) -> None:
        """Signal followers that the leader's fetch is complete.

        Always paired with a successful ``claim_or_wait`` returning
        True. Idempotent — a second release on a missing event is a
        no-op so callers can release in `finally` without checking.
        """
        key = (instrument_id, interval, count)
        with self._lock:
            event = self._inflight.pop(key, None)
        if event is not None:
            event.set()


# Process-global cache instance. Stored on ``app.state`` would be
# cleaner for tests, but every other in-process service in eBull
# (quote_stream.QuoteBus etc.) already uses module-globals plus a
# `clear` hook for test isolation.
_GLOBAL_CACHE = IntradayCandleCache()


def get_intraday_cache() -> IntradayCandleCache:
    return _GLOBAL_CACHE


def fetch_intraday_candles(
    provider: MarketDataProvider,
    *,
    instrument_id: int,
    interval: IntradayInterval,
    count: int,
    cache: IntradayCandleCache | None = None,
) -> list[IntradayBar]:
    """Fetch intraday candles via cache, falling through to provider.

    Cache hit returns the cached tuple (converted back to a list so
    callers retain the previous mutable-list contract).

    Cache miss enters a singleflight: the first miss for a key fetches
    from the provider, stores the result, and wakes any waiting
    followers. Followers re-check the cache after waking and fall
    through to a fresh fetch only if the leader's fetch failed (so a
    transient leader failure does not cascade into stuck followers).

    Provider failures propagate to whichever thread owns the leader
    slot. The cache only stores successful fetches.
    """
    cache = cache or _GLOBAL_CACHE
    cached = cache.get(instrument_id, interval, count)
    if cached is not None:
        return list(cached)

    is_leader = cache.claim_or_wait(instrument_id, interval, count)
    if not is_leader:
        # Follower: leader has already finished (or timed out). Re-read.
        cached = cache.get(instrument_id, interval, count)
        if cached is not None:
            return list(cached)
        # Leader's fetch failed or timed out. Fall through to our own
        # fetch — duplicate work in the rare wedged-leader case is
        # better than returning a misleading empty list.
        is_leader = cache.claim_or_wait(instrument_id, interval, count)
        if not is_leader:
            # Yet another thread became leader between our re-read and
            # our re-claim. Re-read once more; if still missing, accept
            # the duplicate-fetch fallback by going to provider directly.
            cached = cache.get(instrument_id, interval, count)
            if cached is not None:
                return list(cached)
            return provider.get_intraday_candles(instrument_id, interval, count)

    try:
        bars = provider.get_intraday_candles(instrument_id, interval, count)
        cache.put(instrument_id, interval, count, bars)
        return bars
    finally:
        cache.release_inflight(instrument_id, interval, count)
