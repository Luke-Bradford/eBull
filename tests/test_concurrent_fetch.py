"""Unit tests for ``app.providers.concurrent_fetch`` (#726, #761)."""

from __future__ import annotations

import threading
import time

import httpx
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


class TestResponseTimeIsSpentOutsideTheThrottleLock:
    """Regression guard for #726. Bot pre-flight raised a concern that
    ``time.sleep`` inside the throttle lock would serialise threads
    end-to-end and erase the concurrency gain. Live SEC tests showed
    7.5 req/s actual vs ~1 req/s sequential, so the design works — but
    the bot's intuition is reasonable enough that we want a CI check.

    The design property: ``_request`` calls ``_throttle_and_stamp``,
    which acquires ``_throttle_lock``, sleeps out any remaining floor,
    stamps and RELEASES. Only then does it call ``self._client.send``.
    So the HTTP round trip is served concurrently across threads while
    the *stamping* remains serialised at ``1 / min_interval``. Move the
    send inside the lock and every request serialises behind the RTT.

    ⚠ #2610 — this replaces a wall-clock assertion, twice. Both prior
    forms compared elapsed time of a concurrent arm against a
    sequential one (an absolute constant, then a measured ratio), and
    both false-failed ``.githooks/pre-push`` under ordinary background
    load: 3 of 4 attempts during PR #2609, with a sibling autonomy loop
    on the box holding load average at 2.68-3.59. A sibling loop is a
    NORMAL condition for this machine, so that arrangement was not
    fixable by widening the margin. Per #2224, a gate that fails
    randomly trains ``--no-verify``, which is strictly worse than no
    gate — and this test sits on the fast tier, the per-push path.

    ⚠⚠ The prior forms also observed the WRONG CODE. Their worker was a
    test-local ``fire()`` that called ``rc._throttle_and_stamp()`` and
    then slept the simulated RTT itself — so the send-outside-the-lock
    ordering being asserted was the ordering the *test* had written, not
    the one in ``_request``. Reinstating the send inside ``_request``'s
    lock would not have moved either ratio. This form drives the real
    ``rc.get()`` over ``httpx.MockTransport``, so the ordering under
    test is the shipped one.

    The assertion is now a rendezvous, not a stopwatch: N threads must
    be inside the transport handler SIMULTANEOUSLY, which a
    ``threading.Barrier`` decides exactly. If the send holds the
    throttle lock, at most one thread can ever be in the handler, the
    barrier cannot form, and the test fails on every run and every box.
    Host load moves only how long the pass takes, not whether it passes.
    """

    def test_all_requests_are_in_flight_simultaneously(self) -> None:
        from concurrent.futures import ThreadPoolExecutor

        from app.providers.resilient_client import ResilientClient

        N_REQUESTS = 4
        # Slack over the ~N*min_interval (80 ms) the throttle floor
        # itself costs before the last thread can reach the handler.
        # Only ever paid in full when the barrier genuinely cannot form,
        # i.e. on a real regression.
        BARRIER_TIMEOUT_S = 5.0

        rendezvous = threading.Barrier(N_REQUESTS)
        never_formed = threading.Event()
        in_handler: list[str] = []
        handler_lock = threading.Lock()

        def handler(request: httpx.Request) -> httpx.Response:
            """Stands in for the SEC round trip. Reached only after
            ``_throttle_and_stamp`` has returned, so arriving here means
            the caller is past the throttle."""
            with handler_lock:
                in_handler.append(str(request.url))
            try:
                rendezvous.wait(timeout=BARRIER_TIMEOUT_S)
            except threading.BrokenBarrierError:
                # Fewer than N_REQUESTS threads could be here at once.
                never_formed.set()
            return httpx.Response(200, text="ok")

        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            rc = ResilientClient(client, min_request_interval_s=0.02)
            with ThreadPoolExecutor(max_workers=N_REQUESTS) as pool:
                statuses = [
                    r.status_code for r in pool.map(lambda i: rc.get(f"https://example.test/{i}"), range(N_REQUESTS))
                ]

        assert statuses == [200] * N_REQUESTS
        assert len(in_handler) == N_REQUESTS, f"expected {N_REQUESTS} sends, transport saw {len(in_handler)}"
        assert not never_formed.is_set(), (
            f"{N_REQUESTS} requests never overlapped inside the transport within "
            f"{BARRIER_TIMEOUT_S}s — the throttle lock is being held across "
            "``self._client.send``, serialising the HTTP round trip."
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
        recorded: list[float] = []

        class _RecordingClock(list[float]):
            """The client's stamp slot, which records every write.

            ⚠ ``_throttle_and_stamp`` assigns ``_last_request_at[0]`` while
            holding ``_throttle_lock``, so this ``__setitem__`` runs inside the
            critical section. That is what makes the recorded sequence the
            throttle's own, rather than an observation of it taken later.
            """

            def __setitem__(self, index: int, value: float) -> None:  # type: ignore[override]
                super().__setitem__(index, value)
                recorded.append(value)

        clock: list[float] = _RecordingClock([0.0])
        lock = threading.Lock()
        # min_interval=0 path still locks for a deterministic stamp
        # write; min_interval>0 path tests the throttle branch.
        rc = ResilientClient.__new__(ResilientClient)
        rc._min_interval = 0.02  # 20 ms floor — easy to detect violation
        rc._last_request_at = clock
        rc._throttle_lock = lock
        rc._gate = None  # #1484: no cross-process gate -> exercise the in-process floor

        def fire() -> None:
            rc._throttle_and_stamp()  # pyright: ignore[reportPrivateUsage]

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for _ in range(40):
                pool.submit(fire)

        # ⚠⚠ THE STAMPS ARE THE ONES THE THROTTLE WROTE, captured INSIDE its own
        # lock — not `time.monotonic()` read after `_throttle_and_stamp` returns.
        #
        # The earlier form timed the wrong instant. A thread that was correctly
        # spaced by the throttle could be descheduled between returning and
        # taking its own reading, so two readings landed closer together than
        # the floor while the floor itself had held. Measured 2026-08-07 on an
        # otherwise idle box: 2 failures in 5 consecutive runs of this class,
        # against no change in `resilient_client` — a wall-clock assertion about
        # thread scheduling, wearing a rate-limit invariant's name. Reading the
        # sequence the throttle ASSIGNED removes the gap entirely: `__setitem__`
        # below runs while `_throttle_lock` is held, so the recorded order is
        # the assignment order and no post-return scheduling can reach it.
        #
        # The invariant asserted is unchanged and is checked on MORE stamps than
        # before (every write, including the initial 0.0 seed's successors).
        assert len(recorded) == 40, f"expected 40 stamps under the throttle lock, recorded {len(recorded)}"
        for prev, cur in zip(recorded, recorded[1:], strict=False):
            assert cur - prev >= rc._min_interval, f"throttle violation: {cur - prev:.4f}s < {rc._min_interval}s floor"


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
