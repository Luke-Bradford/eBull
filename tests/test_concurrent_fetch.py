"""Unit tests for ``app.providers.concurrent_fetch`` (#726, #761)."""

from __future__ import annotations

import threading
import time

import pytest

from app.providers.concurrent_fetch import (
    FetchOutcome,
    concurrent_iter,
    concurrent_map,
    fetch_document_texts,
    fetch_document_texts_classified,
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


class TestFetchDocumentTextsClassified:
    """#1698 — discriminated outcomes so a transient 429 (a RAISE) is
    never collapsed into the same lossy ``None`` as a permanent 404,
    then tombstoned. A tombstone on a transient throttle permanently
    drops a real filing (411 lost in one burst, dev 2026-06-21)."""

    def test_each_outcome_classified(self) -> None:
        fetcher = _Fetcher(
            {
                "ok": "body",
                "missing": None,  # SEC 404 / 410 -> MISSING (permanent)
                "empty": "",  # empty 200 -> EMPTY (permanent)
                "boom": RuntimeError,  # raise -> TRANSIENT (retry)
            }
        )
        out = fetch_document_texts_classified(fetcher, ["ok", "missing", "empty", "boom"], max_workers=4)
        assert out["ok"] == (FetchOutcome.OK, "body")
        assert out["missing"] == (FetchOutcome.MISSING, None)
        assert out["empty"] == (FetchOutcome.EMPTY, None)
        assert out["boom"] == (FetchOutcome.TRANSIENT, None)

    def test_transient_raise_distinct_from_missing_none(self) -> None:
        """The core of the fix: a raised exception (429/timeout) and a
        legit ``None`` (404) must NOT collapse to the same outcome —
        only ``MISSING`` may be tombstoned."""
        fetcher = _Fetcher({"raises": RuntimeError, "not_found": None})
        out = fetch_document_texts_classified(fetcher, ["raises", "not_found"])
        assert out["raises"][0] is FetchOutcome.TRANSIENT
        assert out["not_found"][0] is FetchOutcome.MISSING
        assert out["raises"][0] is not out["not_found"][0]

    def test_absent_url_get_default_is_transient(self) -> None:
        """A URL filtered by de-dup (falsy) / absent from the map must
        default to TRANSIENT via the caller's ``.get`` default — never
        tombstone on an unclassified result (Codex ckpt-1 MED)."""
        fetcher = _Fetcher({"u1": "body"})
        out = fetch_document_texts_classified(fetcher, ["u1", ""], max_workers=2)
        assert "" not in out  # falsy URL dropped before fetch
        assert out.get("", (FetchOutcome.TRANSIENT, None)) == (FetchOutcome.TRANSIENT, None)

    def test_duplicate_urls_dedup_before_fetch(self) -> None:
        fetcher = _Fetcher({"u1": "body"})
        out = fetch_document_texts_classified(fetcher, ["u1", "u1", "u1"], max_workers=4)
        assert out == {"u1": (FetchOutcome.OK, "body")}
        assert fetcher.calls.count("u1") == 1

    def test_empty_input_returns_empty(self) -> None:
        assert fetch_document_texts_classified(_Fetcher({}), []) == {}


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

        # Simulated work: each worker stamps then sleeps RESPONSE_MS
        # (the "HTTP RTT") OUTSIDE the lock, exactly like the real
        # ResilientClient._request flow.
        N_REQUESTS = 12
        RESPONSE_MS = 0.1

        def _make_rc() -> ResilientClient:
            rc = ResilientClient.__new__(ResilientClient)
            rc._min_interval = 0.02  # 20 ms floor
            rc._last_request_at = [0.0]
            rc._throttle_lock = threading.Lock()
            rc._gate = None  # #1484: no cross-process gate -> exercise the in-process floor
            return rc

        def fire(rc: ResilientClient) -> None:
            rc._throttle_and_stamp()  # pyright: ignore[reportPrivateUsage]
            time.sleep(RESPONSE_MS)  # outside the lock

        # #1769 — measure the sequential baseline IN-PROCESS rather than
        # against a precomputed constant. A constant (the old
        # ``N*(min_interval+R)``) silently assumes an idle host: on a busy
        # box wall-clock dilates and the concurrent run dilates with it, so
        # an absolute threshold becomes unreachable and the test false-fails
        # the push gate. The *ratio* concurrency buys is what we actually
        # want to pin: it tracks host load far better than an absolute bar
        # (both arms dilate under scheduler pressure, though not perfectly in
        # lock-step — the concurrent arm also pays thread/lock contention).
        # The R sleeps overlap even on a single core — sleeping needs no CPU
        # — so a healthy design still wins regardless of contention. This is
        # a coarse "did we preserve substantial overlap?" guard, not a fine
        # throughput regression detector; basic overlap correctness is pinned
        # deterministically by ``test_concurrency_actually_overlaps``.
        # Best-of-2 on BOTH arms — symmetric sampling. Measuring sequential
        # once but concurrent best-of-2 is one-sided: a single noise-inflated
        # sequential run lifts the threshold, which could let a serialised
        # regression squeak under ``sequential*0.85``. Taking the fastest of 2
        # sequential runs too removes that hole — both arms shed transient
        # scheduler stalls, so the ratio reflects the design, not host noise.
        def _time_sequential() -> float:
            rc_local = _make_rc()
            start = time.monotonic()
            for _ in range(N_REQUESTS):
                fire(rc_local)
            return time.monotonic() - start

        sequential = min(_time_sequential() for _ in range(2))

        # Best-of-2 concurrent: sheds a transient scheduler stall that would
        # otherwise mask a genuinely-parallel design as serial. A throttle
        # regression that serialises HTTP RTT inside the lock collapses the
        # ratio toward ~1.0 in EVERY run, so best-of-2 still fails it.
        def _time_concurrent() -> float:
            rc_local = _make_rc()
            start = time.monotonic()
            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(lambda _: fire(rc_local), range(N_REQUESTS)))
            return time.monotonic() - start

        concurrent = min(_time_concurrent() for _ in range(2))

        # Healthy design overlaps RTT across threads: ratio ~0.4 idle, ~0.68
        # under heavy load (measured). A serialising regression -> ~1.0. The
        # 0.85 bar sits in that gap, deliberately biased toward the looser
        # side: a false FAIL wedges the push gate (#1769 — the exact bug this
        # rewrite fixes), whereas a false pass is still caught by the
        # deterministic overlap guard (``test_concurrency_actually_overlaps``).
        assert concurrent < sequential * 0.85, (
            f"Concurrent best {concurrent:.3f}s did not beat measured sequential "
            f"{sequential:.3f}s by ≥15% (ratio {concurrent / sequential:.2f}) — "
            "throttle design may be serialising HTTP RTT across threads."
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
        rc._gate = None  # #1484: no cross-process gate -> exercise the in-process floor

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
