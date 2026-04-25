"""Tests for the eToro WebSocket subscriber (#274 Slices 1+2).

Pure helpers (auth-message build, subscribe-message build, rate-
message parser, spread-pct compute, private-event classifier) are
unit-tested; the DB upsert is integration-tested against
``ebull_test``. The connect/listen loop is not exercised end-to-end
— that requires a real WS server or a heavyweight fixture — but the
debounce dispatch is exercised directly on
``EtoroWebSocketSubscriber._schedule_reconcile`` with a stub runner
so the collapse-to-one-call invariant is covered.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import threading
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services import etoro_websocket
from app.services.etoro_websocket import (
    EtoroWebSocketSubscriber,
    QuoteUpdate,
    _await_auth_envelope,
    _compute_spread_pct,
    _is_auth_success,
    _looks_like_json_envelope,
    build_auth_message,
    build_private_subscribe_message,
    build_subscribe_message,
    fetch_watched_instrument_ids,
    is_private_event,
    parse_rate_message,
    upsert_quote,
)

# ---------------------------------------------------------------------------
# Pure helpers — no DB
# ---------------------------------------------------------------------------


class TestBuildAuthMessage:
    def test_envelope_shape(self) -> None:
        msg = json.loads(build_auth_message("API", "USR"))
        assert msg["operation"] == "Authenticate"
        assert msg["data"] == {"apiKey": "API", "userKey": "USR"}
        assert "id" in msg

    def test_id_is_unique_per_call(self) -> None:
        ids = {json.loads(build_auth_message("a", "u"))["id"] for _ in range(5)}
        assert len(ids) == 5


class TestBuildSubscribeMessage:
    def test_topics_built_correctly(self) -> None:
        raw = build_subscribe_message([1001, 1002, 1003])
        assert raw is not None
        msg = json.loads(raw)
        assert msg["operation"] == "Subscribe"
        assert msg["data"]["topics"] == [
            "instrument:1001",
            "instrument:1002",
            "instrument:1003",
        ]
        # snapshot=True so we get the latest tick on (re)connect.
        assert msg["data"]["snapshot"] is True

    def test_empty_list_returns_none(self) -> None:
        """No-op subscribe must not be sent — eToro may reject empty
        topics, and we have nothing to listen for."""
        assert build_subscribe_message([]) is None


class TestParseRateMessage:
    def test_canonical_rate_push(self) -> None:
        raw = json.dumps(
            {
                "type": "Trading.Instrument.Rate",
                "data": {
                    "InstrumentID": 1001,
                    "Bid": "186.50",
                    "Ask": "186.70",
                    "LastExecution": "186.60",
                    "Date": "2026-04-24T14:30:00Z",
                    "PriceRateID": "abc",
                },
            }
        )
        update = parse_rate_message(raw)
        assert update is not None
        assert update.instrument_id == 1001
        assert update.bid == Decimal("186.50")
        assert update.ask == Decimal("186.70")
        assert update.last == Decimal("186.60")
        assert update.quoted_at == datetime(2026, 4, 24, 14, 30, 0, tzinfo=UTC)

    def test_missing_last_execution_passes_through(self) -> None:
        raw = json.dumps(
            {
                "type": "Trading.Instrument.Rate",
                "data": {
                    "InstrumentID": 1001,
                    "Bid": "186.50",
                    "Ask": "186.70",
                    "Date": "2026-04-24T14:30:00Z",
                },
            }
        )
        update = parse_rate_message(raw)
        assert update is not None
        assert update.last is None

    def test_non_rate_message_returns_none(self) -> None:
        assert parse_rate_message(json.dumps({"type": "Trading.OrderForCloseMultiple.Update", "data": {}})) is None

    def test_malformed_json_returns_none(self) -> None:
        assert parse_rate_message("not json") is None
        assert parse_rate_message("") is None

    def test_missing_required_field_returns_none(self) -> None:
        # No InstrumentID
        raw = json.dumps(
            {"type": "Trading.Instrument.Rate", "data": {"Bid": "1", "Ask": "2", "Date": "2026-04-24T14:30:00Z"}}
        )
        assert parse_rate_message(raw) is None


class TestSpreadPct:
    def test_canonical_spread(self) -> None:
        # bid 100, ask 101 → spread = 1; mid = 100.5; pct = 1/100.5 * 100
        spread = _compute_spread_pct(Decimal("100"), Decimal("101"))
        assert spread is not None
        assert abs(spread - Decimal("0.99502487562189")) < Decimal("0.0001")

    def test_zero_or_negative_returns_none(self) -> None:
        assert _compute_spread_pct(Decimal("0"), Decimal("100")) is None
        assert _compute_spread_pct(Decimal("100"), Decimal("0")) is None
        assert _compute_spread_pct(Decimal("-1"), Decimal("100")) is None


class TestIsAuthSuccess:
    def test_success_envelope(self) -> None:
        assert _is_auth_success(json.dumps({"success": True})) is True

    def test_failure_envelope(self) -> None:
        assert _is_auth_success(json.dumps({"success": False, "errorCode": "InvalidKey"})) is False

    def test_missing_field(self) -> None:
        assert _is_auth_success(json.dumps({"id": "x"})) is False

    def test_malformed_returns_false(self) -> None:
        assert _is_auth_success("not json") is False


# ---------------------------------------------------------------------------
# Auth-handshake noise drain (#474)
# ---------------------------------------------------------------------------


class TestLooksLikeJsonEnvelope:
    def test_canonical_json_text(self) -> None:
        assert _looks_like_json_envelope('{"success": true}') is True

    def test_json_bytes(self) -> None:
        assert _looks_like_json_envelope(b'{"success": true}') is True

    def test_leading_null_byte_str(self) -> None:
        assert _looks_like_json_envelope('\x00{"success": true}') is True

    def test_leading_null_byte_bytes(self) -> None:
        assert _looks_like_json_envelope(b'\x00{"success": true}') is True

    def test_pure_null_byte_is_noise(self) -> None:
        assert _looks_like_json_envelope(b"\x00") is False

    def test_empty_is_noise(self) -> None:
        assert _looks_like_json_envelope("") is False

    def test_whitespace_only_is_noise(self) -> None:
        assert _looks_like_json_envelope("   ") is False

    def test_array_envelope_is_noise(self) -> None:
        # eToro auth ack is always a JSON object envelope. Arrays /
        # other shapes — if they ever arrived — are still drained.
        assert _looks_like_json_envelope("[]") is False

    def test_null_byte_then_whitespace_then_json(self) -> None:
        # Regression for review WARNING: a two-pass strip
        # (``.lstrip().lstrip("\\x00")``) misses this shape because
        # the leading null blocks the whitespace strip on pass one.
        # Single-pass strip across both classes handles it.
        assert _looks_like_json_envelope(b'\x00 {"success": true}') is True
        assert _looks_like_json_envelope(b'\x00\x00 {"success": true}') is True
        assert _looks_like_json_envelope(' \x00{"success": true}') is True


class TestAwaitAuthEnvelope:
    """Integration coverage of the drain loop. Stubs the WS recv()
    side via a tiny fake so the test runs without a real socket."""

    class _FakeWs:
        def __init__(self, frames: list[str | bytes]) -> None:
            self._frames = frames

        async def recv(self) -> str | bytes:
            if not self._frames:
                # Mimic a real ws stalling forever — the deadline in
                # _await_auth_envelope's wait_for cuts in instead.
                await asyncio.sleep(3600)
                raise AssertionError("unreachable")
            return self._frames.pop(0)

    async def test_returns_first_json_envelope_after_noise(self) -> None:
        fake = self._FakeWs([b"\x00", '{"success": true}'])
        result = await _await_auth_envelope(fake, timeout_s=2.0)  # type: ignore[arg-type]
        assert result == '{"success": true}'

    async def test_returns_canonical_envelope_immediately(self) -> None:
        fake = self._FakeWs(['{"success": true}'])
        result = await _await_auth_envelope(fake, timeout_s=2.0)  # type: ignore[arg-type]
        assert result == '{"success": true}'

    async def test_drains_multiple_noise_frames(self) -> None:
        fake = self._FakeWs([b"\x00", b"\x00\x00", "  ", '{"success": true}'])
        result = await _await_auth_envelope(fake, timeout_s=2.0)  # type: ignore[arg-type]
        assert result == '{"success": true}'

    async def test_timeout_when_no_envelope_arrives(self) -> None:
        # Empty frame queue → fake will await forever → deadline must
        # fire and raise TimeoutError, not loop forever.
        fake = self._FakeWs([])
        with pytest.raises(TimeoutError):
            await _await_auth_envelope(fake, timeout_s=0.1)  # type: ignore[arg-type]

    async def test_timeout_after_noise_burst(self) -> None:
        # Several noise frames followed by stalling — must still
        # surface the timeout instead of draining forever.
        fake = self._FakeWs([b"\x00", b"\x00", b"\x00"])
        with pytest.raises(TimeoutError):
            await _await_auth_envelope(fake, timeout_s=0.1)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Integration — DB upsert + watched-IDs query
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestUpsertQuote:
    def _seed_instrument(self, conn: psycopg.Connection[tuple], iid: int = 1001) -> None:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO instruments (instrument_id, symbol, company_name) VALUES (%s, %s, %s)",
                (iid, "AAPL", "Apple Inc."),
            )
        conn.commit()

    def test_first_upsert_inserts(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        self._seed_instrument(ebull_test_conn)
        upsert_quote(
            ebull_test_conn,
            QuoteUpdate(
                instrument_id=1001,
                bid=Decimal("100"),
                ask=Decimal("101"),
                last=Decimal("100.5"),
                quoted_at=datetime(2026, 4, 24, 14, 30, 0, tzinfo=UTC),
            ),
        )
        ebull_test_conn.commit()
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT bid, ask, last, spread_pct FROM quotes WHERE instrument_id = 1001")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == Decimal("100")
        assert row[1] == Decimal("101")
        assert row[2] == Decimal("100.5")
        assert row[3] is not None  # spread computed

    def test_newer_tick_overwrites(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        self._seed_instrument(ebull_test_conn, iid=1002)
        upsert_quote(
            ebull_test_conn,
            QuoteUpdate(
                instrument_id=1002,
                bid=Decimal("100"),
                ask=Decimal("101"),
                last=None,
                quoted_at=datetime(2026, 4, 24, 14, 30, 0, tzinfo=UTC),
            ),
        )
        upsert_quote(
            ebull_test_conn,
            QuoteUpdate(
                instrument_id=1002,
                bid=Decimal("105"),
                ask=Decimal("106"),
                last=None,
                quoted_at=datetime(2026, 4, 24, 14, 31, 0, tzinfo=UTC),
            ),
        )
        ebull_test_conn.commit()
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT bid FROM quotes WHERE instrument_id = 1002")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == Decimal("105")

    def test_older_tick_does_not_overwrite(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Out-of-order arrival across reconnects must not regress
        the stored tick."""
        self._seed_instrument(ebull_test_conn, iid=1003)
        upsert_quote(
            ebull_test_conn,
            QuoteUpdate(
                instrument_id=1003,
                bid=Decimal("100"),
                ask=Decimal("101"),
                last=None,
                quoted_at=datetime(2026, 4, 24, 14, 31, 0, tzinfo=UTC),
            ),
        )
        # Older tick arrives second.
        upsert_quote(
            ebull_test_conn,
            QuoteUpdate(
                instrument_id=1003,
                bid=Decimal("90"),
                ask=Decimal("91"),
                last=None,
                quoted_at=datetime(2026, 4, 24, 14, 30, 0, tzinfo=UTC),
            ),
        )
        ebull_test_conn.commit()
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT bid FROM quotes WHERE instrument_id = 1003")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == Decimal("100")  # newer tick survived


@pytest.mark.integration
class TestFetchWatchedInstrumentIds:
    def test_returns_held_and_watchlist_union(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO instruments (instrument_id, symbol, company_name) "
                "VALUES (1001, 'AAPL', 'Apple'), "
                "(1002, 'MSFT', 'Microsoft'), "
                "(1003, 'NVDA', 'Nvidia'), "
                "(1004, 'GOOG', 'Google')"
            )
            # Held = 1001, 1002. Watchlist = 1002, 1003. Result should
            # be the union {1001, 1002, 1003}; 1004 (neither) stays out.
            cur.execute(
                """
                INSERT INTO broker_positions
                    (position_id, instrument_id, is_buy, units, amount,
                     initial_amount_in_dollars, open_rate, open_conversion_rate,
                     open_date_time, raw_payload)
                VALUES
                    (1001, 1001, TRUE, 1, 100, 100, 100, 1, NOW(), '{}'::jsonb),
                    (1002, 1002, TRUE, 2, 200, 200, 100, 1, NOW(), '{}'::jsonb)
                """
            )
            cur.execute(
                "INSERT INTO operators (operator_id, username, password_hash) "
                "VALUES ('00000000-0000-0000-0000-000000000001', 'op', 'x')"
            )
            cur.execute(
                "INSERT INTO watchlist (instrument_id, operator_id, added_at) "
                "VALUES (1002, '00000000-0000-0000-0000-000000000001', NOW()), "
                "(1003, '00000000-0000-0000-0000-000000000001', NOW())"
            )
        ebull_test_conn.commit()

        ids = fetch_watched_instrument_ids(ebull_test_conn)
        assert sorted(ids) == [1001, 1002, 1003]


# ---------------------------------------------------------------------------
# Slice 2 — private channel + reconcile debounce
# ---------------------------------------------------------------------------


class TestBuildPrivateSubscribeMessage:
    def test_envelope_shape(self) -> None:
        msg = json.loads(build_private_subscribe_message())
        assert msg["operation"] == "Subscribe"
        assert msg["data"]["topics"] == ["private"]
        # snapshot=False — REST reconcile owns the snapshot, the WS
        # private channel is forward-only.
        assert msg["data"]["snapshot"] is False
        assert "id" in msg


class TestIsPrivateEvent:
    @pytest.mark.parametrize(
        "msg_type",
        [
            "Trading.OrderForCloseMultiple.Update",
            "Trading.OrderForOpenMultiple.Update",
            "Trading.PositionUpdate",
            "Trading.CreditUpdate",
        ],
    )
    def test_known_private_types(self, msg_type: str) -> None:
        raw = json.dumps({"type": msg_type, "data": {}})
        assert is_private_event(raw) is True

    def test_rate_push_is_not_private(self) -> None:
        raw = json.dumps({"type": "Trading.Instrument.Rate", "data": {}})
        assert is_private_event(raw) is False

    def test_unknown_type_is_not_private(self) -> None:
        raw = json.dumps({"type": "Heartbeat", "data": {}})
        assert is_private_event(raw) is False

    def test_malformed_returns_false(self) -> None:
        assert is_private_event("not json") is False
        assert is_private_event("[]") is False
        assert is_private_event(json.dumps({"type": 42})) is False


class TestReconcileDebounce:
    """The reconcile worker must collapse a burst of private events
    into exactly one reconcile call. Critical because a multi-leg
    eToro trade emits several order/position events within a few
    hundred ms; without debounce we'd hammer the REST endpoint and
    burn the 60-GET/min budget on a single user action.

    The worker pattern (single dedicated coroutine + Event signal)
    also guarantees serial execution: if a new event arrives *while*
    a reconcile is in flight, the worker completes the current
    reconcile before starting the next, so two ``sync_portfolio``
    calls never race against the same DB.
    """

    def _make_subscriber(self, runner: Any) -> EtoroWebSocketSubscriber:
        # Pool is never touched in this path: ``watched_ids_provider``
        # short-circuits ``_default_watched_ids`` and ``runner``
        # short-circuits ``_default_reconcile_runner``. Constructor
        # only stores the reference, so an inert sentinel is safe.
        sentinel: Any = object()
        return EtoroWebSocketSubscriber(
            api_key="API",
            user_key="USR",
            env="demo",
            pool=sentinel,
            watched_ids_provider=lambda: [],
            reconcile_runner=runner,
        )

    async def _start_worker(self, sub: EtoroWebSocketSubscriber) -> asyncio.Task[None]:
        """Spin up just the reconcile worker without booting the WS
        listen loop. Returns the task so the test can cancel it on
        teardown."""
        task = asyncio.create_task(sub._reconcile_worker())
        # Yield once so the worker reaches its first ``event.wait()``
        # before any test schedules an event — otherwise the very
        # first set() can be lost between create_task and the worker
        # actually awaiting.
        await asyncio.sleep(0)
        return task

    async def _stop_worker(self, task: asyncio.Task[None]) -> None:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def test_burst_collapses_to_single_call(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(etoro_websocket, "_RECONCILE_DEBOUNCE_S", 0.05)

        calls = 0
        done = asyncio.Event()

        def runner() -> None:
            nonlocal calls
            calls += 1
            done.set()

        sub = self._make_subscriber(runner)
        worker = await self._start_worker(sub)
        try:
            sub._schedule_reconcile()
            sub._schedule_reconcile()
            sub._schedule_reconcile()
            await asyncio.wait_for(done.wait(), timeout=2.0)
            # Wait one more debounce window to confirm no second
            # reconcile fires from the burst.
            await asyncio.sleep(0.15)
            assert calls == 1
        finally:
            await self._stop_worker(worker)

    async def test_event_during_reconcile_triggers_followup(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """If a new private event arrives while a reconcile is in
        flight, the worker must finish the current reconcile then
        run a second one. Previously a cancel-and-replace timer
        could let two reconciles run concurrently because
        ``Task.cancel()`` doesn't kill an in-progress
        ``asyncio.to_thread`` worker."""
        monkeypatch.setattr(etoro_websocket, "_RECONCILE_DEBOUNCE_S", 0.05)

        in_flight = threading.Event()
        release = threading.Event()
        order: list[str] = []
        calls = 0

        def runner() -> None:
            nonlocal calls
            calls += 1
            n = calls
            order.append(f"start-{n}")
            in_flight.set()
            # Block the first reconcile so the test can land a second
            # event while it's running. The second reconcile is
            # released immediately because release is set after the
            # first call signals.
            release.wait(timeout=2.0)
            order.append(f"end-{n}")

        sub = self._make_subscriber(runner)
        worker = await self._start_worker(sub)
        try:
            sub._schedule_reconcile()
            # Wait for the first reconcile to actually be running
            # inside the worker thread.
            await asyncio.to_thread(in_flight.wait, 2.0)
            assert calls == 1

            # Schedule a second event during the in-flight reconcile.
            sub._schedule_reconcile()
            # Release the first reconcile; the worker should re-loop,
            # debounce again, then run reconcile #2.
            release.set()

            # Wait for the second reconcile to start AND end.
            for _ in range(200):
                if calls >= 2 and order.count("end-") >= 0 and "end-2" in order:
                    break
                await asyncio.sleep(0.02)
            assert calls == 2, f"order={order}"
            # Sequencing: end-1 must precede start-2 — proves serial
            # execution, no concurrent reconcile.
            assert order.index("end-1") < order.index("start-2")
        finally:
            release.set()
            await self._stop_worker(worker)

    async def test_runner_exception_does_not_kill_worker(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A failing reconcile must not propagate or kill the worker
        — the next event re-attempts. Logged as a warning in
        production."""
        monkeypatch.setattr(etoro_websocket, "_RECONCILE_DEBOUNCE_S", 0.05)

        calls = 0
        events = [asyncio.Event(), asyncio.Event()]

        def runner() -> None:
            nonlocal calls
            calls += 1
            events[calls - 1].set()
            if calls == 1:
                raise RuntimeError("broker exploded")

        sub = self._make_subscriber(runner)
        worker = await self._start_worker(sub)
        try:
            sub._schedule_reconcile()
            await asyncio.wait_for(events[0].wait(), timeout=2.0)
            assert calls == 1

            # Worker should still be alive — schedule a second event
            # and confirm it runs.
            sub._schedule_reconcile()
            await asyncio.wait_for(events[1].wait(), timeout=2.0)
            assert calls == 2
            assert not worker.done()
        finally:
            await self._stop_worker(worker)

    async def test_stop_waits_for_in_flight_reconcile_thread(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``stop()`` must not return while the reconcile thread is
        still running, otherwise the lifespan caller can close the
        DB pool out from under ``sync_portfolio``. Cancelling the
        worker coroutine cancels the *await* but does not kill the
        thread — so ``stop()`` has to wait on a thread-side barrier.
        """
        monkeypatch.setattr(etoro_websocket, "_RECONCILE_DEBOUNCE_S", 0.05)

        thread_done = threading.Event()
        in_flight = threading.Event()
        release = threading.Event()

        def runner() -> None:
            in_flight.set()
            release.wait(timeout=5.0)
            thread_done.set()

        sentinel: Any = object()
        sub = EtoroWebSocketSubscriber(
            api_key="API",
            user_key="USR",
            env="demo",
            pool=sentinel,
            watched_ids_provider=lambda: [],
            reconcile_runner=runner,
        )

        # Replace _run so start() doesn't try to open a real
        # WebSocket. The substitute hangs on the stop event so the
        # listen loop's Task surface mirrors production.
        async def fake_run() -> None:
            await sub._stop_event.wait()

        sub._run = fake_run  # type: ignore[method-assign]

        await sub.start()
        try:
            sub._schedule_reconcile()
            await asyncio.to_thread(in_flight.wait, 2.0)
            assert in_flight.is_set()

            # Kick stop() concurrently. It should block until the
            # thread completes — the release.set() unblocks the
            # runner shortly after.
            stop_task = asyncio.create_task(sub.stop())
            await asyncio.sleep(0.05)
            assert not stop_task.done(), "stop() returned while reconcile thread still running"

            release.set()
            await asyncio.wait_for(stop_task, timeout=2.0)
            assert thread_done.is_set()
        finally:
            release.set()
            if sub._task is not None or sub._reconcile_worker_task is not None:
                with contextlib.suppress(Exception):
                    await sub.stop()

    async def test_separate_windows_each_fire(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Two events separated by more than the debounce window
        produce two independent reconciles."""
        monkeypatch.setattr(etoro_websocket, "_RECONCILE_DEBOUNCE_S", 0.05)

        calls = 0
        events = [asyncio.Event(), asyncio.Event()]

        def runner() -> None:
            nonlocal calls
            calls += 1
            events[calls - 1].set()

        sub = self._make_subscriber(runner)
        worker = await self._start_worker(sub)
        try:
            sub._schedule_reconcile()
            await asyncio.wait_for(events[0].wait(), timeout=2.0)
            sub._schedule_reconcile()
            await asyncio.wait_for(events[1].wait(), timeout=2.0)
            assert calls == 2
        finally:
            await self._stop_worker(worker)


class TestListenResilience:
    """``_listen`` must keep consuming WS frames after a private-event
    reconcile dispatch and after a reconcile failure — a noisy
    private channel must not stall the rate path."""

    async def test_rate_frame_after_private_event_still_upserts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(etoro_websocket, "_RECONCILE_DEBOUNCE_S", 0.05)

        upsert_calls: list[QuoteUpdate] = []

        def runner() -> None:
            # Private path runs but does nothing observable here —
            # we only assert that a subsequent rate frame still
            # reaches the upsert path.
            return None

        sentinel: Any = object()
        sub = EtoroWebSocketSubscriber(
            api_key="API",
            user_key="USR",
            env="demo",
            pool=sentinel,
            watched_ids_provider=lambda: [],
            reconcile_runner=runner,
        )

        # Replace _sync_upsert so we don't need a real DB.
        def fake_upsert(update: QuoteUpdate) -> None:
            upsert_calls.append(update)

        sub._sync_upsert = fake_upsert  # type: ignore[method-assign]

        worker = asyncio.create_task(sub._reconcile_worker())
        await asyncio.sleep(0)
        try:
            private = json.dumps({"type": "Trading.PositionUpdate", "data": {}})
            rate = json.dumps(
                {
                    "type": "Trading.Instrument.Rate",
                    "data": {
                        "InstrumentID": 1001,
                        "Bid": "100",
                        "Ask": "101",
                        "Date": "2026-04-24T14:30:00Z",
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

            await sub._listen(FakeWs([private, rate]))  # type: ignore[arg-type]

            assert len(upsert_calls) == 1
            assert upsert_calls[0].instrument_id == 1001
        finally:
            worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker
