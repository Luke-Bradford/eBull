"""Tests for the in-process intraday candle cache (#600)."""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from app.providers.market_data import IntradayBar
from app.services.intraday_candles import (
    IntradayCandleCache,
    fetch_intraday_candles,
)


def _bar(ts_iso: str, close: str = "100.0") -> IntradayBar:
    return IntradayBar(
        timestamp=datetime.fromisoformat(ts_iso).replace(tzinfo=UTC),
        open=Decimal(close),
        high=Decimal(close),
        low=Decimal(close),
        close=Decimal(close),
        volume=1000,
    )


class TestIntradayCandleCache:
    def test_get_returns_none_on_miss(self) -> None:
        cache = IntradayCandleCache()
        assert cache.get(1, "OneMinute", 100) is None

    def test_put_then_get_returns_stored_bars(self) -> None:
        cache = IntradayCandleCache()
        bars = [_bar("2026-04-27T14:30:00")]
        cache.put(1, "OneMinute", 100, bars)
        result = cache.get(1, "OneMinute", 100)
        assert result is not None
        assert len(result) == 1
        assert result[0].close == Decimal("100.0")

    def test_cache_keyed_on_all_three_dimensions(self) -> None:
        cache = IntradayCandleCache()
        cache.put(1, "OneMinute", 100, [_bar("2026-04-27T14:30:00", "1")])
        cache.put(2, "OneMinute", 100, [_bar("2026-04-27T14:30:00", "2")])
        cache.put(1, "FiveMinutes", 100, [_bar("2026-04-27T14:30:00", "3")])
        cache.put(1, "OneMinute", 200, [_bar("2026-04-27T14:30:00", "4")])

        assert cache.get(1, "OneMinute", 100)[0].close == Decimal("1")  # type: ignore[index]
        assert cache.get(2, "OneMinute", 100)[0].close == Decimal("2")  # type: ignore[index]
        assert cache.get(1, "FiveMinutes", 100)[0].close == Decimal("3")  # type: ignore[index]
        assert cache.get(1, "OneMinute", 200)[0].close == Decimal("4")  # type: ignore[index]

    def test_expired_entry_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Put a bar at t=0, advance clock past the 30s TTL, get returns None.
        clock = [1000.0]
        monkeypatch.setattr(time, "monotonic", lambda: clock[0])

        cache = IntradayCandleCache()
        cache.put(1, "OneMinute", 100, [_bar("2026-04-27T14:30:00")])
        assert cache.get(1, "OneMinute", 100) is not None

        clock[0] += 30.1
        assert cache.get(1, "OneMinute", 100) is None

    def test_long_ttl_for_longer_intraday_interval(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Hourly+ uses long-TTL (180s) — verify a 60s skip does not expire it.
        clock = [1000.0]
        monkeypatch.setattr(time, "monotonic", lambda: clock[0])

        cache = IntradayCandleCache()
        cache.put(1, "OneHour", 100, [_bar("2026-04-27T14:30:00")])
        clock[0] += 60.0
        assert cache.get(1, "OneHour", 100) is not None

    def test_lru_evicts_oldest_when_over_capacity(self) -> None:
        cache = IntradayCandleCache(max_entries=2)
        cache.put(1, "OneMinute", 100, [_bar("2026-04-27T14:30:00", "1")])
        cache.put(2, "OneMinute", 100, [_bar("2026-04-27T14:30:00", "2")])
        cache.put(3, "OneMinute", 100, [_bar("2026-04-27T14:30:00", "3")])

        # Oldest (instrument 1) evicted; newer two remain.
        assert cache.get(1, "OneMinute", 100) is None
        assert cache.get(2, "OneMinute", 100) is not None
        assert cache.get(3, "OneMinute", 100) is not None

    def test_get_promotes_lru_recency(self) -> None:
        cache = IntradayCandleCache(max_entries=2)
        cache.put(1, "OneMinute", 100, [_bar("2026-04-27T14:30:00", "1")])
        cache.put(2, "OneMinute", 100, [_bar("2026-04-27T14:30:00", "2")])
        # Touch entry 1 — promote to most-recent.
        assert cache.get(1, "OneMinute", 100) is not None
        # Insert a third — entry 2 evicts (it is now LRU), not entry 1.
        cache.put(3, "OneMinute", 100, [_bar("2026-04-27T14:30:00", "3")])
        assert cache.get(1, "OneMinute", 100) is not None
        assert cache.get(2, "OneMinute", 100) is None
        assert cache.get(3, "OneMinute", 100) is not None


class TestFetchIntradayCandles:
    def test_first_call_invokes_provider_subsequent_call_hits_cache(self) -> None:
        cache = IntradayCandleCache()
        bars = [_bar("2026-04-27T14:30:00")]
        provider = MagicMock()
        provider.get_intraday_candles.return_value = bars

        result1 = fetch_intraday_candles(provider, instrument_id=1, interval="OneMinute", count=100, cache=cache)
        result2 = fetch_intraday_candles(provider, instrument_id=1, interval="OneMinute", count=100, cache=cache)

        assert result1 == bars
        assert result2 == bars
        provider.get_intraday_candles.assert_called_once_with(1, "OneMinute", 100)

    def test_provider_error_does_not_populate_cache(self) -> None:
        cache = IntradayCandleCache()
        provider = MagicMock()
        provider.get_intraday_candles.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError):
            fetch_intraday_candles(provider, instrument_id=1, interval="OneMinute", count=100, cache=cache)
        # Subsequent call retries the provider; failed fetch is not cached.
        provider.get_intraday_candles.side_effect = None
        provider.get_intraday_candles.return_value = [_bar("2026-04-27T14:30:00")]
        result = fetch_intraday_candles(provider, instrument_id=1, interval="OneMinute", count=100, cache=cache)
        assert len(result) == 1
        assert provider.get_intraday_candles.call_count == 2

    def test_singleflight_collapses_concurrent_misses_to_one_provider_call(self) -> None:
        """Two threads racing on the same key must hit the provider once."""
        cache = IntradayCandleCache()
        provider_started = threading.Event()
        leader_can_finish = threading.Event()
        provider_calls = 0
        provider_lock = threading.Lock()

        def slow_fetch(*_args: object, **_kwargs: object) -> list[IntradayBar]:
            nonlocal provider_calls
            with provider_lock:
                provider_calls += 1
            provider_started.set()
            # Block the leader until the follower has had a chance to
            # enter the singleflight wait. Bounded wait so a buggy test
            # cannot hang indefinitely.
            leader_can_finish.wait(timeout=5.0)
            return [_bar("2026-04-27T14:30:00")]

        provider = MagicMock()
        provider.get_intraday_candles.side_effect = slow_fetch

        results: list[list[IntradayBar]] = []

        def call() -> None:
            results.append(
                fetch_intraday_candles(provider, instrument_id=1, interval="OneMinute", count=100, cache=cache)
            )

        leader = threading.Thread(target=call)
        leader.start()
        # Wait for the leader to enter the provider so the follower
        # cannot win the leadership election.
        assert provider_started.wait(timeout=5.0)
        follower = threading.Thread(target=call)
        follower.start()
        # Give the follower a beat to enter `claim_or_wait` and block.
        time.sleep(0.05)
        leader_can_finish.set()
        leader.join(timeout=5.0)
        follower.join(timeout=5.0)

        assert provider_calls == 1
        assert len(results) == 2
        assert all(len(r) == 1 for r in results)
