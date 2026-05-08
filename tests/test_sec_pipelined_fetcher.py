"""Tests for the pipelined SEC EDGAR fetcher (#1026)."""

from __future__ import annotations

import asyncio
import threading
import time

import httpx
import pytest

from app.services.sec_pipelined_fetcher import (
    DEFAULT_CONCURRENCY,
    DEFAULT_TARGET_RPS,
    FetchTask,
    PipelinedSecFetcher,
    _AsyncRateLimiter,
)


def _isolated_budget() -> tuple[list[float], threading.Lock]:
    """Return a fresh (clock, lock) pair for tests that need predictable timing.

    Without this the fetcher shares the process-wide SEC budget with
    every other test in the suite, making timing-sensitive assertions
    fragile under xdist.
    """
    return [0.0], threading.Lock()


# ---------------------------------------------------------------------------
# AsyncRateLimiter unit tests
# ---------------------------------------------------------------------------


class TestAsyncRateLimiter:
    @pytest.mark.asyncio
    async def test_first_acquire_is_immediate(self) -> None:
        rl = _AsyncRateLimiter(target_rps=10)
        started = time.monotonic()
        await rl.acquire()
        elapsed = time.monotonic() - started
        assert elapsed < 0.05

    @pytest.mark.asyncio
    async def test_back_to_back_acquires_observe_min_interval(self) -> None:
        rl = _AsyncRateLimiter(target_rps=10)  # 100 ms floor
        started = time.monotonic()
        await rl.acquire()
        await rl.acquire()
        await rl.acquire()
        elapsed = time.monotonic() - started
        # 3 acquires over a 100 ms floor must take at least ~200 ms.
        assert elapsed >= 0.18

    @pytest.mark.asyncio
    async def test_zero_or_negative_rps_rejected(self) -> None:
        with pytest.raises(ValueError):
            _AsyncRateLimiter(target_rps=0)
        with pytest.raises(ValueError):
            _AsyncRateLimiter(target_rps=-1)


# ---------------------------------------------------------------------------
# PipelinedSecFetcher integration tests
# ---------------------------------------------------------------------------


def _record_handler(latency_s: float = 0.05, in_flight: list[int] | None = None):
    """Return a MockTransport handler that records concurrent in-flight counts."""
    if in_flight is None:
        in_flight = [0, 0]  # current, peak

    def handler(request: httpx.Request) -> httpx.Response:
        in_flight[0] += 1
        in_flight[1] = max(in_flight[1], in_flight[0])
        time.sleep(latency_s)
        in_flight[0] -= 1
        path = request.url.path
        return httpx.Response(200, content=path.encode("utf-8"))

    return handler, in_flight


class TestPipelinedSecFetcher:
    @pytest.mark.asyncio
    async def test_results_yield_in_completion_order_with_correct_keys(self) -> None:
        # Each URL responds after a deterministic delay; with
        # concurrency=3 all three start near-simultaneously and
        # complete in increasing-delay order.
        delays = {"a": 0.15, "b": 0.05, "c": 0.10}

        async def async_handler(request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(delays.get(request.url.path.strip("/"), 0))
            return httpx.Response(200)

        transport = httpx.MockTransport(async_handler)
        async with httpx.AsyncClient(transport=transport, base_url="https://test") as client:
            clock, lock = _isolated_budget()
            fetcher = PipelinedSecFetcher(
                client=client,
                target_rps=1000,
                concurrency=3,
                shared_clock=clock,
                shared_lock=lock,
            )
            tasks = [FetchTask(key=k, url=f"https://test/{k}") for k in ("a", "b", "c")]
            keys_in_order: list[object] = []
            async for result in fetcher.fetch_many(tasks):
                assert result.error is None
                keys_in_order.append(result.key)
        # Completion order: shortest delay first.
        assert keys_in_order == ["b", "c", "a"]

    @pytest.mark.asyncio
    async def test_concurrency_cap_honoured(self) -> None:
        in_flight = [0, 0]

        async def async_handler(request: httpx.Request) -> httpx.Response:
            in_flight[0] += 1
            in_flight[1] = max(in_flight[1], in_flight[0])
            await asyncio.sleep(0.05)
            in_flight[0] -= 1
            return httpx.Response(200)

        transport = httpx.MockTransport(async_handler)
        async with httpx.AsyncClient(transport=transport, base_url="https://test") as client:
            clock, lock = _isolated_budget()
            fetcher = PipelinedSecFetcher(
                client=client,
                target_rps=1000,
                concurrency=2,
                shared_clock=clock,
                shared_lock=lock,
            )
            tasks = [FetchTask(key=i, url=f"https://test/{i}") for i in range(8)]
            async for _ in fetcher.fetch_many(tasks):
                pass
        # With concurrency=2, peak in-flight must never exceed 2.
        assert in_flight[1] <= 2

    @pytest.mark.asyncio
    async def test_rate_ceiling_honoured_under_load(self) -> None:
        # 5 req/s ceiling = 200 ms floor; 6 requests over MockTransport
        # with negligible latency must take ~1.0s (5 floors between
        # 6 acquires).
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200)

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport, base_url="https://test") as client:
            clock, lock = _isolated_budget()
            fetcher = PipelinedSecFetcher(
                client=client,
                target_rps=5,
                concurrency=4,
                shared_clock=clock,
                shared_lock=lock,
            )
            tasks = [FetchTask(key=i, url=f"https://test/{i}") for i in range(6)]
            started = time.monotonic()
            async for _ in fetcher.fetch_many(tasks):
                pass
            elapsed = time.monotonic() - started
        assert elapsed >= 0.95  # 5 floors × 200 ms

    @pytest.mark.asyncio
    async def test_empty_task_list_yields_nothing(self) -> None:
        transport = httpx.MockTransport(lambda r: httpx.Response(200))
        async with httpx.AsyncClient(transport=transport) as client:
            fetcher = PipelinedSecFetcher(client=client)
            yielded = [r async for r in fetcher.fetch_many([])]
        assert yielded == []

    @pytest.mark.asyncio
    async def test_http_error_surfaces_on_result_error_field(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("simulated")

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport, base_url="https://test") as client:
            clock, lock = _isolated_budget()
            fetcher = PipelinedSecFetcher(
                client=client,
                target_rps=1000,
                concurrency=1,
                shared_clock=clock,
                shared_lock=lock,
            )
            results = [r async for r in fetcher.fetch_many([FetchTask(key="x", url="https://test/x")])]
        assert len(results) == 1
        assert results[0].response is None
        assert results[0].error is not None

    def test_concurrency_zero_rejected(self) -> None:
        async def _make() -> None:
            async with httpx.AsyncClient() as client:
                with pytest.raises(ValueError):
                    PipelinedSecFetcher(client=client, concurrency=0)

        asyncio.run(_make())

    def test_default_constants_match_spec(self) -> None:
        assert DEFAULT_TARGET_RPS == 7.0
        assert DEFAULT_CONCURRENCY == 4

    @pytest.mark.asyncio
    async def test_async_floor_observes_sync_stamp(self) -> None:
        # Mixed sync+async sharing of the same clock: simulate a sync
        # ResilientClient firing "right now" by stamping clock[0] =
        # monotonic(); the async fetcher must wait min_interval before
        # firing. Codex pre-push round 2: the prior implementation
        # treated clock[0] as "next allowed time" and bypassed the
        # floor when it was actually a last-request stamp.
        clock, lock = _isolated_budget()
        # Sync client just fired:
        clock[0] = time.monotonic()
        rl = _AsyncRateLimiter(target_rps=10, shared_clock=clock, shared_lock=lock)
        started = time.monotonic()
        await rl.acquire()
        elapsed = time.monotonic() - started
        # 10 req/s = 100 ms floor; the async caller should wait ~100 ms.
        assert elapsed >= 0.08, f"expected ~100 ms wait, got {elapsed:.3f} s"

    @pytest.mark.asyncio
    async def test_two_fetchers_share_rate_budget_via_shared_clock(self) -> None:
        # Two fetchers configured against the SAME (clock, lock)
        # tuple must serialise their requests against the budget —
        # combined throughput stays at target_rps, not 2 × target_rps.
        # Codex pre-push round 1: this is the prevention-log
        # "Multiple ResilientClient instances sharing a rate limit
        # must share throttle state" case applied to async clients.
        clock, lock = _isolated_budget()
        transport = httpx.MockTransport(lambda r: httpx.Response(200))
        async with httpx.AsyncClient(transport=transport, base_url="https://test") as client:
            # 5 req/s shared budget; two fetchers each issuing 3 requests.
            f1 = PipelinedSecFetcher(client=client, target_rps=5, concurrency=3, shared_clock=clock, shared_lock=lock)
            f2 = PipelinedSecFetcher(client=client, target_rps=5, concurrency=3, shared_clock=clock, shared_lock=lock)
            t1 = [FetchTask(key=f"a{i}", url=f"https://test/a{i}") for i in range(3)]
            t2 = [FetchTask(key=f"b{i}", url=f"https://test/b{i}") for i in range(3)]

            async def _drain(fetcher: PipelinedSecFetcher, tasks: list[FetchTask]) -> None:
                async for _ in fetcher.fetch_many(tasks):
                    pass

            started = time.monotonic()
            await asyncio.gather(_drain(f1, t1), _drain(f2, t2))
            elapsed = time.monotonic() - started
        # 6 requests at 5 req/s shared = 5 floors × 200 ms ≈ 1.0 s.
        # If the budget were NOT shared, two fetchers at 3 req each
        # would interleave at 10 req/s combined and finish in ~0.4 s.
        assert elapsed >= 0.95
