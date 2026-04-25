"""Tests for QuoteBus + SSE quotes endpoint (#274 Slice 3).

QuoteBus tests cover the pub/sub fan-out invariants directly. The
SSE endpoint test exercises the full FastAPI route via TestClient
to confirm the StreamingResponse emits ``data:`` frames in the
right order.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from app.api.sse_quotes import (
    _DisplayContext,
    _event_stream,
    _format_tick,
    _parse_instrument_ids,
)
from app.api.sse_quotes import router as sse_router
from app.services.etoro_websocket import QuoteUpdate
from app.services.quote_stream import QuoteBus


def _empty_ctx(display_ccy: str = "USD") -> _DisplayContext:
    """Trivial display context: no instruments → no conversion path,
    so _format_tick emits the native triple with display=null. Used
    by tests that don't care about the Slice 4 conversion."""
    return _DisplayContext(display_ccy=display_ccy, instrument_ccys={}, rates={})


def _make_update(instrument_id: int, bid: str = "100", ask: str = "101") -> QuoteUpdate:
    return QuoteUpdate(
        instrument_id=instrument_id,
        bid=Decimal(bid),
        ask=Decimal(ask),
        last=None,
        quoted_at=datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC),
    )


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestFormatTick:
    def test_envelope_shape(self) -> None:
        frame = _format_tick(_make_update(1001, bid="100.50", ask="100.70"), _empty_ctx())
        assert frame.endswith("\n\n")
        assert frame.startswith("data: ")
        body = json.loads(frame[len("data: ") :].strip())
        assert body == {
            "instrument_id": 1001,
            "native_currency": None,
            "bid": "100.50",
            "ask": "100.70",
            "last": None,
            "quoted_at": "2026-04-25T12:00:00+00:00",
            "display": None,
        }

    def test_decimal_precision_preserved(self) -> None:
        # Critical: lossy float conversion would defeat spread-pct
        # invariants downstream. ``str(Decimal)`` round-trips.
        frame = _format_tick(_make_update(1001, bid="186.4567", ask="186.4569"), _empty_ctx())
        body = json.loads(frame[len("data: ") :].strip())
        assert body["bid"] == "186.4567"
        assert body["ask"] == "186.4569"

    def test_display_block_emitted_with_known_ccy_and_rate(self) -> None:
        ctx = _DisplayContext(
            display_ccy="GBP",
            instrument_ccys={1001: "USD"},
            rates={("USD", "GBP"): Decimal("0.75")},
        )
        frame = _format_tick(_make_update(1001, bid="100", ask="200"), ctx)
        body = json.loads(frame[len("data: ") :].strip())
        assert body["native_currency"] == "USD"
        assert body["bid"] == "100"
        assert body["ask"] == "200"
        assert body["display"] == {
            "currency": "GBP",
            "bid": "75.00",
            "ask": "150.00",
            "last": None,
        }

    def test_display_block_null_when_no_rate(self) -> None:
        # Native ccy known but no FX pair available — payload still
        # delivered (raw triple) with display=null so UI knows to
        # fall back.
        ctx = _DisplayContext(
            display_ccy="JPY",
            instrument_ccys={1001: "USD"},
            rates={},  # No USD↔JPY rate
        )
        frame = _format_tick(_make_update(1001), ctx)
        body = json.loads(frame[len("data: ") :].strip())
        assert body["native_currency"] == "USD"
        assert body["display"] is None

    def test_same_currency_pass_through(self) -> None:
        # Native = display, no FX lookup needed. Display block carries
        # the raw triple unchanged.
        ctx = _DisplayContext(
            display_ccy="USD",
            instrument_ccys={1001: "USD"},
            rates={},
        )
        frame = _format_tick(_make_update(1001, bid="50", ask="51"), ctx)
        body = json.loads(frame[len("data: ") :].strip())
        assert body["display"] == {
            "currency": "USD",
            "bid": "50",
            "ask": "51",
            "last": None,
        }


class TestParseInstrumentIds:
    def test_canonical_csv(self) -> None:
        assert _parse_instrument_ids("1,2,3") == frozenset({1, 2, 3})

    def test_dedupes_and_strips_whitespace(self) -> None:
        assert _parse_instrument_ids(" 1 , 2 , 1 ") == frozenset({1, 2})

    def test_drops_non_numeric_tokens(self) -> None:
        assert _parse_instrument_ids("1,foo,2") == frozenset({1, 2})

    def test_empty_returns_empty_set(self) -> None:
        assert _parse_instrument_ids("") == frozenset()


# ---------------------------------------------------------------------------
# QuoteBus pub/sub
# ---------------------------------------------------------------------------


class TestQuoteBusFanOut:
    async def test_subscriber_only_receives_filtered_instruments(self) -> None:
        bus = QuoteBus()
        async with bus.subscribe(frozenset({1001, 1002})) as queue:
            bus.publish(_make_update(1001))
            bus.publish(_make_update(9999))  # filtered out
            bus.publish(_make_update(1002))

            received = [queue.get_nowait().instrument_id, queue.get_nowait().instrument_id]
            assert sorted(received) == [1001, 1002]
            assert queue.empty()

    async def test_two_subscribers_each_get_independent_copies(self) -> None:
        bus = QuoteBus()
        async with bus.subscribe(frozenset({1001})) as q1, bus.subscribe(frozenset({1001})) as q2:
            bus.publish(_make_update(1001))
            assert q1.get_nowait().instrument_id == 1001
            assert q2.get_nowait().instrument_id == 1001

    async def test_unsubscribe_stops_delivery(self) -> None:
        bus = QuoteBus()
        async with bus.subscribe(frozenset({1001})) as queue:
            bus.publish(_make_update(1001))
            queue.get_nowait()
        # After context exit, publish is a no-op for this subscriber.
        bus.publish(_make_update(1001))
        # No queue to assert against — invariant is "no error, no growth"
        # which we verify by inspecting the internal set.
        assert len(bus._subscribers) == 0  # noqa: SLF001

    async def test_full_queue_drops_without_blocking(self) -> None:
        """Slow consumer must not block the publisher. The drop counter
        captures the loss; subsequent publishes still deliver to other
        subscribers."""
        bus = QuoteBus()
        async with bus.subscribe(frozenset({1001})) as slow:
            # Fill the slow subscriber's queue to capacity.
            for _ in range(slow.maxsize):
                bus.publish(_make_update(1001))
            # Next publishes should drop, not raise / block.
            for _ in range(5):
                bus.publish(_make_update(1001))
            # Internal: drop counter > 0 on the slow subscriber.
            sub = next(iter(bus._subscribers))  # noqa: SLF001
            assert sub.drops == 5

    async def test_empty_filter_receives_nothing(self) -> None:
        bus = QuoteBus()
        async with bus.subscribe(frozenset()) as queue:
            bus.publish(_make_update(1001))
            assert queue.empty()

    async def test_slow_subscriber_does_not_starve_healthy_one(self) -> None:
        """Critical multi-subscriber invariant: a saturated
        subscriber's queue must drop without blocking the publisher
        loop, so a healthy subscriber alongside it still receives
        every tick. Without this, one stale browser tab could
        freeze the entire SSE fan-out."""
        bus = QuoteBus()
        async with (
            bus.subscribe(frozenset({1001})) as slow,
            bus.subscribe(frozenset({1001})) as healthy,
        ):
            # Saturate the slow one.
            for _ in range(slow.maxsize):
                bus.publish(_make_update(1001))
            # Drain the healthy one so it has room.
            for _ in range(healthy.maxsize):
                healthy.get_nowait()
            assert healthy.empty()
            # Publish 5 more ticks. The slow queue stays full and
            # drops; the healthy queue must receive all 5.
            for _ in range(5):
                bus.publish(_make_update(1001))
            received = []
            for _ in range(5):
                received.append(healthy.get_nowait().instrument_id)
            assert received == [1001] * 5
            slow_sub = next(s for s in bus._subscribers if s.queue is slow)  # noqa: SLF001
            assert slow_sub.drops == 5


# ---------------------------------------------------------------------------
# SSE endpoint — full FastAPI route exercise
# ---------------------------------------------------------------------------


class TestSseQuotesRoute:
    def _build_app(self, bus: QuoteBus | None) -> FastAPI:
        # Bypass auth dependency so we can drive the route directly.
        app = FastAPI()

        from app.api.auth import require_session_or_service_token

        async def _no_auth() -> None:
            return None

        app.dependency_overrides[require_session_or_service_token] = _no_auth
        app.include_router(sse_router)
        if bus is not None:
            app.state.quote_bus = bus
        return app

    def test_503_when_bus_not_installed(self) -> None:
        app = self._build_app(bus=None)
        with TestClient(app) as client:
            resp = client.get("/sse/quotes?ids=1001")
            assert resp.status_code == 503

    def test_router_carries_auth_dependency(self) -> None:
        """Pin the auth gate as a structural property of the router.
        Regression guard: a future refactor that drops the
        router-level dependency would silently expose live operator
        price data. We assert against ``router.dependencies`` rather
        than driving an unauthed request because the dependency
        chain pulls in db state that isn't set up in this minimal
        test app — and that incidental coupling would itself be a
        flaky test."""
        from app.api.auth import require_session_or_service_token

        dep_callables = [d.dependency for d in sse_router.dependencies]
        assert require_session_or_service_token in dep_callables


class TestEventStreamGenerator:
    """Drive the SSE generator directly, no TestClient. The endpoint
    is an infinite stream — TestClient's context-exit won't unblock
    the server-side generator because ``request.is_disconnected()``
    is only checked between yields. Driving the generator as a plain
    async iterator with a fake Request gives us deterministic frame-
    by-frame assertions without timing flakiness."""

    class _FakeRequest:
        def __init__(self) -> None:
            self._disconnected = False

        def disconnect(self) -> None:
            self._disconnected = True

        async def is_disconnected(self) -> bool:
            return self._disconnected

    async def test_initial_open_frame_then_tick_then_disconnect(self) -> None:
        bus = QuoteBus()
        req = self._FakeRequest()

        gen = _event_stream(req, bus, frozenset({1001}), _empty_ctx(), None)  # type: ignore[arg-type]

        # First yield: open comment.
        first = await gen.__anext__()
        assert first.startswith(": stream open at ")

        # Publish a tick — generator should pick it up on next iteration.
        bus.publish(_make_update(1001, bid="100", ask="101"))
        second = await gen.__anext__()
        assert second.startswith("data: ")
        payload = json.loads(second[len("data: ") :].strip())
        assert payload["instrument_id"] == 1001
        assert payload["bid"] == "100"

        # Mark disconnected; next pull should terminate the generator.
        req.disconnect()
        # Publish another tick to wake the queue.get await.
        bus.publish(_make_update(1001, bid="102", ask="103"))
        # Generator may yield the third tick OR terminate; consume one
        # more then assert StopAsyncIteration on the following call.
        try:
            await gen.__anext__()
        except StopAsyncIteration:
            return
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    async def test_heartbeat_emitted_when_no_ticks(self) -> None:
        """When no tick arrives within the heartbeat window the
        generator must emit a comment frame so proxies / load
        balancers don't idle-kill the connection. Shrink the window
        to keep the test fast."""
        from app.api import sse_quotes

        original = sse_quotes._HEARTBEAT_INTERVAL_S
        sse_quotes._HEARTBEAT_INTERVAL_S = 0.05
        bus = QuoteBus()
        req = self._FakeRequest()
        gen = sse_quotes._event_stream(req, bus, frozenset({1001}), _empty_ctx(), None)  # type: ignore[arg-type]
        try:
            # Open frame.
            first = await gen.__anext__()
            assert first.startswith(": stream open at ")

            # No publish — generator should hit the timeout and emit
            # a heartbeat comment.
            second = await asyncio.wait_for(gen.__anext__(), timeout=1.0)
            assert second.startswith(":") and "heartbeat" in second
        finally:
            sse_quotes._HEARTBEAT_INTERVAL_S = original
            await gen.aclose()

    async def test_filtered_instrument_does_not_yield(self) -> None:
        bus = QuoteBus()
        req = self._FakeRequest()
        gen = _event_stream(req, bus, frozenset({1001}), _empty_ctx(), None)  # type: ignore[arg-type]

        # Drain initial open frame.
        await gen.__anext__()

        # Publish a tick for an instrument we did NOT subscribe to.
        bus.publish(_make_update(9999))

        # Generator should be parked in queue.get — assert by racing
        # with a short timeout.
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(gen.__anext__(), timeout=0.1)

        # Cleanup: close the generator so the queue subscription tears
        # down without leaking the open Subscriber.
        await gen.aclose()

    async def test_cancel_during_add_still_calls_remove(self) -> None:
        """Regression for Codex high finding on #485: if the SSE
        request is cancelled DURING ``add_instruments``, the ref
        count has already been committed (pure-Python under a
        lock, no await in the critical section). The generator's
        finally must still run ``remove_instruments`` or the ref
        leaks forever. The fix placed the add INSIDE the try; this
        test verifies the invariant."""
        bus = QuoteBus()
        req = self._FakeRequest()

        class _SlowSubscriber:
            def __init__(self) -> None:
                self.added: list[list[int]] = []
                self.removed: list[list[int]] = []

            async def add_instruments(self, ids: list[int]) -> None:
                self.added.append(ids)
                # Simulate the commit-then-await pattern of the
                # real implementation: state is captured before the
                # await, so any cancel here still leaves refs
                # committed.
                await asyncio.sleep(10)

            async def remove_instruments(self, ids: list[int]) -> None:
                self.removed.append(ids)

        ws_sub = _SlowSubscriber()
        gen = _event_stream(req, bus, frozenset({1001}), _empty_ctx(), ws_sub)  # type: ignore[arg-type]

        # Start the generator + cancel mid-add.
        next_task = asyncio.create_task(gen.__anext__())
        await asyncio.sleep(0.02)  # let add_instruments begin its sleep
        next_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await next_task
        # Generator close drives the finally.
        await gen.aclose()

        assert ws_sub.added == [[1001]]
        assert ws_sub.removed == [[1001]]

    async def test_ws_subscriber_add_on_open_remove_on_close(self) -> None:
        """#485 wiring: SSE stream must register the requested
        instrument IDs with the WS subscriber on open and release
        them on close/disconnect. Otherwise opening a page for a
        ticker outside held+watchlist receives no ticks (the
        eToro-side Subscribe frame is never sent)."""
        bus = QuoteBus()
        req = self._FakeRequest()

        class _RecordingSubscriber:
            def __init__(self) -> None:
                self.added: list[list[int]] = []
                self.removed: list[list[int]] = []

            async def add_instruments(self, ids: list[int]) -> None:
                self.added.append(ids)

            async def remove_instruments(self, ids: list[int]) -> None:
                self.removed.append(ids)

        ws_sub = _RecordingSubscriber()
        gen = _event_stream(req, bus, frozenset({1001, 1002}), _empty_ctx(), ws_sub)  # type: ignore[arg-type]

        # Open frame triggers registration.
        await gen.__anext__()
        assert ws_sub.added == [[1001, 1002]]
        assert ws_sub.removed == []

        # Simulate disconnect path.
        await gen.aclose()
        assert ws_sub.removed == [[1001, 1002]]


# ---------------------------------------------------------------------------
# WS subscriber → bus integration
# ---------------------------------------------------------------------------


class TestEtoroWsBusIntegration:
    """Confirms the WS listen loop publishes to the bus before the
    DB upsert. Critical because Slice 3's whole point is sub-second
    UI delivery — if the publish happened only after the DB commit,
    every tick would be gated on a Postgres round-trip."""

    async def test_listen_publishes_then_upserts(self) -> None:
        from typing import Any

        from app.services.etoro_websocket import EtoroWebSocketSubscriber

        bus = QuoteBus()
        upsert_calls: list[QuoteUpdate] = []
        order: list[str] = []

        sentinel: Any = object()
        sub = EtoroWebSocketSubscriber(
            api_key="API",
            user_key="USR",
            env="demo",
            pool=sentinel,
            bus=bus,
            watched_ids_provider=lambda: [],
            reconcile_runner=lambda: None,
        )

        original_publish = bus.publish

        def trace_publish(update: QuoteUpdate) -> None:
            order.append("publish")
            original_publish(update)

        bus.publish = trace_publish  # type: ignore[method-assign]

        def fake_upsert(update: QuoteUpdate) -> None:
            order.append("upsert")
            upsert_calls.append(update)

        sub._sync_upsert = fake_upsert  # type: ignore[method-assign]

        rate = json.dumps(
            {
                "type": "Trading.Instrument.Rate",
                "data": {
                    "InstrumentID": 1001,
                    "Bid": "100",
                    "Ask": "101",
                    "Date": "2026-04-25T12:00:00Z",
                },
            }
        )

        class FakeWs:
            def __init__(self, frames: list[str]) -> None:
                self._frames = frames

            def __aiter__(self) -> FakeWs:
                return self

            async def __anext__(self) -> str:
                if not self._frames:
                    raise StopAsyncIteration
                return self._frames.pop(0)

        async with bus.subscribe(frozenset({1001})) as queue:
            await sub._listen(FakeWs([rate]))  # type: ignore[arg-type]
            # Bus saw the tick.
            received = await asyncio.wait_for(queue.get(), timeout=1.0)
            assert received.instrument_id == 1001

        # Publish strictly preceded upsert (Slice 3 latency goal).
        assert order == ["publish", "upsert"]
        assert len(upsert_calls) == 1
