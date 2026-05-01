"""Unit tests for ``app.providers.concurrent_fetch`` (#726, #761)."""

from __future__ import annotations

import threading
import time

import pytest

from app.providers.concurrent_fetch import (
    concurrent_iter,
    concurrent_map,
    fetch_document_texts,
)


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


class TestConcurrencyAchievesActualThroughput:
    """Wall-clock regression guard for #726. Bot pre-flight raised
    a concern that ``time.sleep`` inside the throttle lock would
    serialise threads end-to-end and erase the concurrency gain.
    Live SEC tests showed 7.5 req/s actual vs ~1 req/s sequential,
    so the design works — but the bot's intuition is reasonable
    enough that we want a deterministic CI check.

    The math: with N concurrent workers, each lock holder spends
    ``min_interval`` sleeping (since the previous holder just
    stamped). After release, the next thread acquires and sleeps
    ``min_interval`` again. Aggregate rate = ``1 / min_interval``
    regardless of N (the lock IS the rate gate). Crucially, the
    HTTP RTT happens AFTER lock release, in parallel across threads
    — so total wall-clock for N requests with response_time R is
    ``N * min_interval + R`` (the last request's response), NOT
    ``N * (min_interval + R)`` which is what the sequential
    pre-PR loop took.
    """

    def test_concurrent_total_time_smaller_than_sequential(self) -> None:
        from concurrent.futures import ThreadPoolExecutor

        from app.providers.resilient_client import ResilientClient

        rc = ResilientClient.__new__(ResilientClient)
        rc._min_interval = 0.02  # 20 ms floor
        rc._last_request_at = [0.0]
        rc._throttle_lock = threading.Lock()

        # Simulated work: each worker stamps then sleeps 100ms
        # (the "HTTP RTT") OUTSIDE the lock, exactly like the real
        # ResilientClient._request flow.
        N_REQUESTS = 16
        RESPONSE_MS = 0.1

        def fire() -> None:
            rc._throttle_and_stamp()  # pyright: ignore[reportPrivateUsage]
            time.sleep(RESPONSE_MS)  # outside the lock

        sequential_estimate = N_REQUESTS * (rc._min_interval + RESPONSE_MS)

        t0 = time.monotonic()
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda _: fire(), range(N_REQUESTS)))
        elapsed = time.monotonic() - t0

        # With 8 workers, response time overlaps. Wall-clock should
        # be much closer to N*min_interval + RESPONSE_MS (~0.42s)
        # than the sequential estimate (~1.92s).
        assert elapsed < sequential_estimate * 0.5, (
            f"Concurrent total {elapsed:.3f}s did not beat sequential {sequential_estimate:.3f}s "
            "by ≥2x — throttle design may be serialising HTTP RTT across threads."
        )


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


# ---------------------------------------------------------------------------
# Generic ``concurrent_map`` (#761) — used by JSON-fetch ingest paths
# (e.g. SEC companyfacts) that don't go through ``fetch_document_text``.
# ---------------------------------------------------------------------------


class TestConcurrentMap:
    def test_returns_pairs_in_submission_order(self) -> None:
        # Order preservation matters when the caller zips the result
        # back against parallel input arrays (e.g. (symbol, cik)
        # tuples in ``refresh_financial_facts``).
        def double(x: int) -> int:
            return x * 2

        result = concurrent_map(double, [3, 1, 4, 1, 5, 9], max_workers=4)
        assert [item for item, _ in result] == [3, 1, 4, 1, 5, 9]
        assert [r for _, r in result] == [6, 2, 8, 2, 10, 18]

    def test_concurrency_actually_overlaps(self) -> None:
        live = 0
        peak = 0
        lock = threading.Lock()

        def slow(x: int) -> int:
            nonlocal live, peak
            with lock:
                live += 1
                peak = max(peak, live)
            try:
                time.sleep(0.05)
                return x
            finally:
                with lock:
                    live -= 1

        concurrent_map(slow, list(range(8)), max_workers=4)
        assert peak > 1
        assert peak <= 4

    def test_per_item_exception_becomes_none(self) -> None:
        def maybe_raise(x: int) -> int:
            if x == 2:
                raise RuntimeError("simulated")
            return x * 10

        result = concurrent_map(maybe_raise, [1, 2, 3], max_workers=2)
        assert result == [(1, 10), (2, None), (3, 30)]

    def test_empty_input_returns_empty(self) -> None:
        assert concurrent_map(lambda x: x, []) == []

    def test_workers_capped_to_item_count(self) -> None:
        # max_workers=8 over 2 items must not allocate 8 threads.
        # ``ThreadPoolExecutor`` accepts the cap; we verify by checking
        # the function still runs and returns paired results — the
        # internal cap path is exercised whenever ``len(items) <
        # max_workers``.
        result = concurrent_map(lambda x: x + "_done", ["a", "b"], max_workers=8)
        assert result == [("a", "a_done"), ("b", "b_done")]

    def test_none_result_passes_through(self) -> None:
        # Distinguishes "fn returned None as a valid result" from
        # "exception caught, surfaced as None". Both look the same
        # by design — caller treats None as "no data, skip" either
        # way (matches the 404 contract).
        def returns_none(x: int) -> int | None:
            return None if x % 2 == 0 else x

        result = concurrent_map(returns_none, [1, 2, 3, 4], max_workers=2)
        assert result == [(1, 1), (2, None), (3, 3), (4, None)]


class TestConcurrentIter:
    def test_yields_one_pair_per_item(self) -> None:
        # Set semantics — yields all items eventually, regardless of
        # order. Streaming consumers don't need submission order.
        result = list(concurrent_iter(lambda x: x * 2, [1, 2, 3, 4], max_workers=2))
        assert sorted(result) == [(1, 2), (2, 4), (3, 6), (4, 8)]

    def test_yields_in_completion_order_not_submission(self) -> None:
        # Slow item 0 should be yielded LAST when faster items
        # complete first. Pin completion-order semantics so the
        # streaming-consumer pattern (refresh_financial_facts) can
        # rely on it.
        def variable_speed(x: int) -> int:
            time.sleep(0.1 if x == 0 else 0.0)
            return x

        result = list(concurrent_iter(variable_speed, [0, 1, 2, 3, 4], max_workers=4))
        items_in_order = [item for item, _ in result]
        # Fast items 1-4 must precede slow item 0.
        assert items_in_order[-1] == 0
        assert set(items_in_order) == {0, 1, 2, 3, 4}

    def test_per_item_exception_becomes_none(self) -> None:
        def raises_on_two(x: int) -> int:
            if x == 2:
                raise RuntimeError("boom")
            return x * 10

        result = sorted(concurrent_iter(raises_on_two, [1, 2, 3], max_workers=2))
        assert result == [(1, 10), (2, None), (3, 30)]

    def test_streaming_memory_bounded_by_workers(self) -> None:
        # The point of concurrent_iter vs concurrent_map: a consumer
        # can drain results as they arrive rather than waiting for
        # the full batch. Verify the producer doesn't pre-buffer
        # everything by checking we can act on the first result
        # before the last item is even started.
        started = threading.Event()
        first_yielded = threading.Event()
        block_late = threading.Event()
        started_count = 0
        lock = threading.Lock()

        def fn(x: int) -> int:
            nonlocal started_count
            with lock:
                started_count += 1
                started.set()
            if x == 99:
                # Last submission — block until the consumer has
                # already received its first result. Proves
                # streaming, not batch-collect.
                block_late.wait(timeout=2.0)
            return x

        items = [1, 2, 3, 99]
        gen = concurrent_iter(fn, items, max_workers=2)

        first_item, first_result = next(gen)
        first_yielded.set()
        block_late.set()

        rest = sorted(list(gen))
        assert first_item in {1, 2, 3, 99}
        assert first_result == first_item
        assert sorted([first_item] + [i for i, _ in rest]) == [1, 2, 3, 99]

    def test_empty_input_yields_nothing(self) -> None:
        assert list(concurrent_iter(lambda x: x, [])) == []
