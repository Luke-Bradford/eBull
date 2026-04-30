"""Unit tests for ``app.providers.concurrent_fetch.fetch_document_texts`` (#726)."""

from __future__ import annotations

import threading
import time

import pytest

from app.providers.concurrent_fetch import fetch_document_texts


class _Fetcher:
    """Captures call ordering + concurrency for assertions."""

    def __init__(self, by_url: dict[str, str | None | type[Exception]]) -> None:
        self._by = by_url
        self.calls: list[str] = []
        self.live: int = 0
        self.peak_live: int = 0
        self._lock = threading.Lock()

    def fetch_document_text(self, absolute_url: str) -> str | None:
        with self._lock:
            self.calls.append(absolute_url)
            self.live += 1
            self.peak_live = max(self.peak_live, self.live)
        try:
            time.sleep(0.05)  # simulate SEC response time
            outcome = self._by[absolute_url]
            if isinstance(outcome, type) and issubclass(outcome, Exception):
                raise outcome("simulated fetch error")
            return outcome
        finally:
            with self._lock:
                self.live -= 1


class TestConcurrentFetch:
    def test_returns_body_per_url(self) -> None:
        fetcher = _Fetcher({"u1": "body1", "u2": "body2", "u3": "body3"})
        result = fetch_document_texts(fetcher, ["u1", "u2", "u3"], max_workers=4)
        assert result == {"u1": "body1", "u2": "body2", "u3": "body3"}

    def test_concurrency_actually_overlaps(self) -> None:
        """Peak-live counter > 1 proves multiple fetches were in flight
        simultaneously. Without concurrency the peak would always be 1."""
        fetcher = _Fetcher({f"u{i}": f"body{i}" for i in range(8)})
        fetch_document_texts(fetcher, [f"u{i}" for i in range(8)], max_workers=4)
        assert fetcher.peak_live > 1
        assert fetcher.peak_live <= 4  # bounded by max_workers

    def test_per_future_exception_becomes_none(self) -> None:
        """One bad URL must not crash the batch — surfaces as None
        in the result map. Caller treats None identical to a 404."""
        fetcher = _Fetcher(
            {
                "good": "ok",
                "bad": RuntimeError,
                "good2": "ok2",
            }
        )
        result = fetch_document_texts(fetcher, ["good", "bad", "good2"], max_workers=2)
        assert result == {"good": "ok", "bad": None, "good2": "ok2"}

    def test_empty_input_returns_empty(self) -> None:
        fetcher = _Fetcher({})
        assert fetch_document_texts(fetcher, []) == {}

    def test_duplicate_urls_dedup_before_fetch(self) -> None:
        """Sending the same URL twice must not double-fetch — the
        rate-budget cost has to match the unique URL count."""
        fetcher = _Fetcher({"u1": "body1"})
        result = fetch_document_texts(fetcher, ["u1", "u1", "u1"], max_workers=4)
        assert result == {"u1": "body1"}
        assert fetcher.calls.count("u1") == 1

    def test_workers_capped_to_unique_url_count(self) -> None:
        """Asking for more workers than URLs must not over-allocate."""
        fetcher = _Fetcher({"u1": "body1", "u2": "body2"})
        fetch_document_texts(fetcher, ["u1", "u2"], max_workers=16)
        assert fetcher.peak_live <= 2

    def test_filters_falsy_urls(self) -> None:
        """Empty-string / falsy URLs in the input are dropped before
        fetch. Caller probably has a None primary_document_url that
        leaked through; we don't want to hit SEC with an empty path."""
        fetcher = _Fetcher({"u1": "body1"})
        result = fetch_document_texts(fetcher, ["u1", "", "u1"], max_workers=4)
        assert result == {"u1": "body1"}


class TestRateLimitSafetyUnderConcurrency:
    """ResilientClient throttle must remain atomic under concurrent
    callers. A regression here lets concurrent fetchers burst past the
    rate-limit floor → SEC UA throttling → cascading 4xx/5xx
    tombstones across every ingest path."""

    @pytest.mark.parametrize("workers", [4, 8, 16])
    def test_throttle_lock_serialises_request_stamping(self, workers: int) -> None:
        from concurrent.futures import ThreadPoolExecutor

        from app.providers.resilient_client import ResilientClient

        # Build a ResilientClient with no real httpx underneath — we
        # only exercise ``_throttle_and_stamp`` directly. The lock
        # protects the read-modify-write of ``_last_request_at[0]``.
        clock: list[float] = [0.0]
        lock = threading.Lock()
        # min_interval=0 path still locks for a deterministic stamp
        # write; min_interval>0 path tests the throttle branch.
        rc = ResilientClient.__new__(ResilientClient)
        rc._min_interval = 0.02  # 20 ms floor — easy to detect violation
        rc._last_request_at = clock
        rc._throttle_lock = lock

        stamps: list[float] = []
        stamps_lock = threading.Lock()

        def fire() -> None:
            rc._throttle_and_stamp()  # pyright: ignore[reportPrivateUsage]
            with stamps_lock:
                stamps.append(time.monotonic())

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for _ in range(40):
                pool.submit(fire)

        stamps.sort()
        # No two consecutive successful fires can be within the floor.
        # Allow 2 ms slack for OS sleep imprecision.
        slack = 0.002
        for prev, cur in zip(stamps, stamps[1:], strict=False):
            assert cur - prev >= rc._min_interval - slack, (
                f"throttle violation: {cur - prev:.4f}s < {rc._min_interval}s floor"
            )
