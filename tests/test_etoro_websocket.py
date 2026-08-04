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
import logging
import threading
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services import etoro_websocket
from app.services.etoro_websocket import (
    EtoroWebSocketSubscriber,
    OpAck,
    QuoteUpdate,
    RateStateStore,
    _await_auth_envelope,
    _compute_spread_pct,
    _is_auth_success,
    _looks_like_json_envelope,
    build_auth_message,
    build_private_subscribe_message,
    build_subscribe_frames,
    build_subscribe_message,
    build_unsubscribe_frames,
    build_unsubscribe_message,
    fetch_watched_instrument_ids,
    is_private_event,
    parse_op_acks,
    parse_rate_deltas,
    parse_rate_message,
    parse_rate_messages,
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

    def test_zero_last_execution_coerced_to_none(self) -> None:
        """#1429: eToro pushes LastExecution=0 for un-freshly-traded
        instruments (bid/ask present). A 0 last must never be persisted —
        coerce to None so the read-side derives a mark from bid/ask."""
        raw = json.dumps(
            {
                "type": "Trading.Instrument.Rate",
                "data": {
                    "InstrumentID": 1001,
                    "Bid": "697.16",
                    "Ask": "697.22",
                    "LastExecution": "0.00",
                    "Date": "2026-04-24T14:30:00Z",
                },
            }
        )
        update = parse_rate_message(raw)
        assert update is not None
        assert update.bid == Decimal("697.16")
        assert update.last is None  # NOT Decimal("0.00")

    def test_negative_last_execution_coerced_to_none(self) -> None:
        raw = json.dumps(
            {
                "type": "Trading.Instrument.Rate",
                "data": {
                    "InstrumentID": 1001,
                    "Bid": "10.00",
                    "Ask": "10.02",
                    "LastExecution": "-1.0",
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
        # No InstrumentID and no topic — parser cannot recover the id.
        raw = json.dumps(
            {"type": "Trading.Instrument.Rate", "data": {"Bid": "1", "Ask": "2", "Date": "2026-04-24T14:30:00Z"}}
        )
        assert parse_rate_message(raw) is None


class TestFrameChunking:
    """#2249 — a single Subscribe frame over the ref set is fatal above
    ~25 KiB, and the failure is SILENT: eToro drops the socket with 1006
    and an empty reason, so `ws.send()` succeeds and nothing surfaces."""

    def test_five_thousand_wide_ids_all_fit_under_the_limit(self) -> None:
        # Widest ids in the real universe are 6 digits (100236 etc.).
        ids = list(range(100_000, 105_000))
        frames = build_subscribe_frames(ids)

        assert len(frames) > 1, "5,000 wide ids must not fit in one frame"
        for f in frames:
            assert len(f.payload.encode("utf-8")) <= etoro_websocket._WS_FRAME_LIMIT_BYTES

        # Nothing dropped and nothing duplicated across the split.
        sent = [t for f in frames for t in json.loads(f.payload)["data"]["topics"]]
        assert sent == [f"instrument:{i}" for i in ids]

    def test_chunking_is_by_bytes_not_topic_count(self) -> None:
        """Same COUNT, different id widths → different frame counts.
        A count-based cap would give the same answer for both."""
        narrow = build_subscribe_frames(list(range(10, 3_010)))
        wide = build_subscribe_frames(list(range(100_000_000, 100_003_000)))
        assert len(wide) > len(narrow)

    def test_subscribe_frames_carry_snapshot_and_unsubscribe_does_not(self) -> None:
        sub = build_subscribe_frames([1, 2, 3])
        assert json.loads(sub[0].payload)["data"]["snapshot"] is True
        assert json.loads(sub[0].payload)["operation"] == "Subscribe"

        unsub = build_unsubscribe_frames([1, 2, 3])
        assert "snapshot" not in json.loads(unsub[0].payload)["data"]
        assert json.loads(unsub[0].payload)["operation"] == "Unsubscribe"

    def test_empty_input_produces_no_frames(self) -> None:
        assert build_subscribe_frames([]) == []
        assert build_unsubscribe_frames([]) == []

    def test_every_frame_has_a_distinct_id(self) -> None:
        frames = build_subscribe_frames(list(range(100_000, 105_000)))
        ids = [f.frame_id for f in frames]
        assert len(set(ids)) == len(ids)


class TestParseOpAcks:
    """#2249 — a missing ack is the ONLY signal that a frame was dropped."""

    def test_parses_success_ack(self) -> None:
        raw = json.dumps({"id": "abc", "success": True, "operation": "Subscribe"})
        assert parse_op_acks(raw) == [OpAck(frame_id="abc", operation="Subscribe", success=True, error_code=None)]

    def test_parses_rejection_with_error_code(self) -> None:
        raw = json.dumps(
            {
                "id": "abc",
                "success": False,
                "operation": "Subscribe",
                "errorCode": "SubscribeFailed",
            }
        )
        (ack,) = parse_op_acks(raw)
        assert ack.success is False
        assert ack.error_code == "SubscribeFailed"

    def test_parses_acks_inside_the_messages_envelope(self) -> None:
        raw = json.dumps(
            {
                "messages": [
                    {"id": "a", "success": True, "operation": "Subscribe"},
                    {"id": "b", "success": True, "operation": "Unsubscribe"},
                ]
            }
        )
        assert [a.frame_id for a in parse_op_acks(raw)] == ["a", "b"]

    def test_rate_frames_and_junk_are_not_acks(self) -> None:
        assert parse_op_acks(_rate_frame(1001, "2026-08-04T10:00:00Z", Bid="1", Ask="2")) == []
        assert parse_op_acks("not json") == []
        assert parse_op_acks(json.dumps({"id": "x", "operation": "Authenticate"})) == []


class TestAckCorrelation:
    """#2249 — the log used to read 'subscribed to N topics' immediately
    before every death, because the send succeeds locally."""

    def _sub(self) -> EtoroWebSocketSubscriber:
        sentinel: Any = object()
        return EtoroWebSocketSubscriber(
            api_key="API",
            user_key="USR",
            env="demo",
            pool=sentinel,
            watched_ids_provider=lambda: [],
            reconcile_runner=lambda: None,
        )

    def test_ack_clears_the_pending_entry(self) -> None:
        sub = self._sub()
        (frame,) = build_subscribe_frames([1001])
        sub._register_pending(frame)
        assert frame.frame_id in sub._pending_acks

        sub._resolve_acks(json.dumps({"id": frame.frame_id, "success": True, "operation": "Subscribe"}))
        assert sub._pending_acks == {}

    def test_unacked_frame_is_reported_and_then_forgotten(self, caplog: pytest.LogCaptureFixture) -> None:
        sub = self._sub()
        (frame,) = build_subscribe_frames([1001])
        sub._register_pending(frame)
        # Backdate past the timeout rather than sleeping.
        operation, count, sent_at = sub._pending_acks[frame.frame_id]
        sub._pending_acks[frame.frame_id] = (operation, count, sent_at - etoro_websocket._ACK_TIMEOUT_S - 1)

        with caplog.at_level(logging.WARNING):
            sub._reap_unacked()
        assert "NEVER ACKED" in caplog.text
        assert sub._pending_acks == {}, "reported once, then dropped so it does not repeat"

    async def test_reaper_fires_with_no_inbound_traffic(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Codex checkpoint-2 catch: the failure this detects is an
        oversize frame that gets the SOCKET DROPPED, so no further
        inbound message ever arrives. A reaper riding the receive loop
        would miss exactly the case it exists for — it must be timed."""
        monkeypatch.setattr(etoro_websocket, "_ACK_TIMEOUT_S", 0.02)
        sub = self._sub()
        (frame,) = build_subscribe_frames([1001])
        sub._register_pending(frame)

        reaper = asyncio.create_task(sub._ack_reaper_loop())
        try:
            with caplog.at_level(logging.WARNING):
                # No frames are fed to _listen at all — the reaper is
                # the only thing running.
                for _ in range(100):
                    await asyncio.sleep(0.01)
                    if not sub._pending_acks:
                        break
            assert sub._pending_acks == {}
            assert "NEVER ACKED" in caplog.text
        finally:
            sub._stop_event.set()
            reaper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reaper

    def test_reconnect_reports_pending_rather_than_clearing_silently(self, caplog: pytest.LogCaptureFixture) -> None:
        sub = self._sub()
        (frame,) = build_subscribe_frames([1001])
        sub._register_pending(frame)
        with caplog.at_level(logging.WARNING):
            sub._reap_unacked(reason="connection was re-established before the ack arrived")
        assert "NEVER ACKED" in caplog.text
        assert sub._pending_acks == {}

    async def test_ack_arriving_during_send_still_clears_the_entry(self) -> None:
        """Review round 3 — `ws.send` awaits, so the receive loop can
        resolve this very frame's ack before control returns. If the
        entry were registered AFTER the send, nothing would ever clear
        it and the reaper would report an acked frame as NEVER ACKED."""
        sub = self._sub()
        (frame,) = build_subscribe_frames([1001])

        class AckingWs:
            """Acks from inside `send` — the tightest possible race."""

            async def send(self, payload: str) -> None:
                await asyncio.sleep(0)  # yield, as a real send does
                sub._resolve_acks(json.dumps({"id": frame.frame_id, "success": True, "operation": "Subscribe"}))

        await sub._send_frames(AckingWs(), [frame])  # type: ignore[arg-type]
        assert sub._pending_acks == {}, "ack raced the registration and was lost"

    async def test_failed_send_is_deregistered(self) -> None:
        """Nothing will ack a frame that never reached the wire."""
        sub = self._sub()
        (frame,) = build_subscribe_frames([1001])

        class BrokenWs:
            async def send(self, payload: str) -> None:
                raise ConnectionError("socket gone")

        with pytest.raises(ConnectionError):
            await sub._send_frames(BrokenWs(), [frame])  # type: ignore[arg-type]
        assert sub._pending_acks == {}

    def test_fresh_frame_is_not_reported(self) -> None:
        sub = self._sub()
        (frame,) = build_subscribe_frames([1001])
        sub._register_pending(frame)
        sub._reap_unacked()
        assert frame.frame_id in sub._pending_acks

    def test_rejection_is_logged_without_tearing_down(self, caplog: pytest.LogCaptureFixture) -> None:
        """#2241: an over-cap rejection does NOT poison the session —
        already-subscribed topics keep serving. Log, do not reconnect."""
        sub = self._sub()
        (frame,) = build_subscribe_frames([1001])
        sub._register_pending(frame)
        with caplog.at_level(logging.WARNING):
            sub._resolve_acks(
                json.dumps(
                    {
                        "id": frame.frame_id,
                        "success": False,
                        "operation": "Subscribe",
                        "errorCode": "SubscribeFailed",
                    }
                )
            )
        assert "REJECTED" in caplog.text
        assert sub._pending_acks == {}


def _rate_frame(instrument_id: int, date: str, **fields: str) -> str:
    """One official-envelope rate frame carrying exactly ``fields``.

    Mirrors the live shape: the instrument is on the envelope
    ``topic``, never in the payload (#2243).
    """
    payload: dict[str, str] = {"Date": date, "PriceRateID": "x", **fields}
    return json.dumps(
        {
            "messages": [
                {
                    "topic": f"instrument:{instrument_id}",
                    "type": "Trading.Instrument.Rate",
                    "content": json.dumps(payload),
                }
            ]
        }
    )


class TestRateStateStore:
    """#2252 — eToro's rate push is a field-level sparse delta.

    Shapes below are the ones actually observed on the wire over
    180,666 messages (#2243): only 16.8% carry Bid+Ask together, and
    requiring both discarded 58.1% of price-CHANGING pushes.
    """

    def _apply(self, store: RateStateStore, raw: str) -> list[QuoteUpdate]:
        return [u for d in parse_rate_deltas(raw) if (u := store.apply(d)) is not None]

    def test_snapshot_then_partials_each_produce_a_tick(self) -> None:
        """The core defect: after a complete snapshot seeds state, a
        bid-only and an ask-only push must each emit a merged tick."""
        store = RateStateStore()

        seeded = self._apply(
            store, _rate_frame(1001, "2026-08-04T10:00:00Z", Bid="186.50", Ask="186.70", LastExecution="186.60")
        )
        assert [(u.bid, u.ask, u.last) for u in seeded] == [(Decimal("186.50"), Decimal("186.70"), Decimal("186.60"))]

        # Bid-only — pre-#2252 this was dropped entirely.
        bid_only = self._apply(store, _rate_frame(1001, "2026-08-04T10:00:01Z", Bid="187.00"))
        assert len(bid_only) == 1
        assert bid_only[0].bid == Decimal("187.00")
        assert bid_only[0].ask == Decimal("186.70")  # standing ask retained
        assert bid_only[0].last == Decimal("186.60")  # absent field unchanged
        assert bid_only[0].quoted_at == datetime(2026, 8, 4, 10, 0, 1, tzinfo=UTC)

        # Ask-only.
        ask_only = self._apply(store, _rate_frame(1001, "2026-08-04T10:00:02Z", Ask="187.40"))
        assert len(ask_only) == 1
        assert ask_only[0].bid == Decimal("187.00")
        assert ask_only[0].ask == Decimal("187.40")

        # LastExecution alone (1.6% of the wire).
        last_only = self._apply(store, _rate_frame(1001, "2026-08-04T10:00:03Z", LastExecution="187.20"))
        assert len(last_only) == 1
        assert last_only[0].last == Decimal("187.20")
        assert last_only[0].bid == Decimal("187.00")

    def test_heartbeat_emits_nothing(self) -> None:
        """59.8% of messages carry no price field. Emitting on one
        would advance quoted_at with no price behind it."""
        store = RateStateStore()
        self._apply(store, _rate_frame(1001, "2026-08-04T10:00:00Z", Bid="1.00", Ask="1.02"))
        assert self._apply(store, _rate_frame(1001, "2026-08-04T10:00:05Z")) == []

    def test_heartbeat_does_not_advance_the_ordering_watermark(self) -> None:
        """Codex checkpoint-2 catch: heartbeats are the majority of the
        wire, so if one set `quoted_at` the guard would reject the next
        genuine price delta stamped behind it."""
        store = RateStateStore()
        self._apply(store, _rate_frame(1001, "2026-08-04T10:00:00Z", Bid="10.00", Ask="10.02"))
        # Heartbeat well ahead of the price stream.
        assert self._apply(store, _rate_frame(1001, "2026-08-04T10:00:09Z")) == []
        # A real price delta stamped BEHIND the heartbeat but AHEAD of
        # the last price must still be merged.
        after = self._apply(store, _rate_frame(1001, "2026-08-04T10:00:04Z", Bid="10.50"))
        assert [(u.bid, u.ask) for u in after] == [(Decimal("10.50"), Decimal("10.02"))]

    def test_partial_before_any_snapshot_emits_nothing(self) -> None:
        """One side alone is not a quote — no fabricated counter-side."""
        store = RateStateStore()
        assert self._apply(store, _rate_frame(1001, "2026-08-04T10:00:00Z", Bid="186.50")) == []
        # ...and the ask completing it does emit.
        done = self._apply(store, _rate_frame(1001, "2026-08-04T10:00:01Z", Ask="186.70"))
        assert [(u.bid, u.ask) for u in done] == [(Decimal("186.50"), Decimal("186.70"))]

    def test_present_zero_last_clears_to_none_but_absent_last_retains(self) -> None:
        """#1429 regression guard, plus the presence-vs-value distinction:
        a present LastExecution<=0 clears to NULL; an absent one keeps
        the prior value."""
        store = RateStateStore()
        self._apply(
            store, _rate_frame(1001, "2026-08-04T10:00:00Z", Bid="697.16", Ask="697.22", LastExecution="697.20")
        )

        absent = self._apply(store, _rate_frame(1001, "2026-08-04T10:00:01Z", Bid="697.18"))
        assert absent[0].last == Decimal("697.20")  # retained, not nulled

        present_zero = self._apply(store, _rate_frame(1001, "2026-08-04T10:00:02Z", LastExecution="0.00"))
        assert present_zero[0].last is None  # NOT Decimal("0.00")

        present_negative = self._apply(store, _rate_frame(1001, "2026-08-04T10:00:03Z", LastExecution="-1.0"))
        assert present_negative[0].last is None

    def test_out_of_order_delta_is_ignored(self) -> None:
        """Against merged state an out-of-order push would corrupt every
        subsequent tick, not just lose one row."""
        store = RateStateStore()
        self._apply(store, _rate_frame(1001, "2026-08-04T10:00:05Z", Bid="10.00", Ask="10.02"))

        assert self._apply(store, _rate_frame(1001, "2026-08-04T10:00:01Z", Bid="9.00")) == []

        after = self._apply(store, _rate_frame(1001, "2026-08-04T10:00:06Z", Ask="10.04"))
        assert after[0].bid == Decimal("10.00")  # stale 9.00 never merged

    def test_state_is_per_instrument(self) -> None:
        store = RateStateStore()
        self._apply(store, _rate_frame(1001, "2026-08-04T10:00:00Z", Bid="1.00", Ask="1.02"))
        # A bid-only push for a DIFFERENT instrument must not borrow
        # 1001's ask.
        assert self._apply(store, _rate_frame(2002, "2026-08-04T10:00:01Z", Bid="50.00")) == []

    def test_forget_drops_state(self) -> None:
        store = RateStateStore()
        self._apply(store, _rate_frame(1001, "2026-08-04T10:00:00Z", Bid="1.00", Ask="1.02"))
        store.forget([1001])
        assert self._apply(store, _rate_frame(1001, "2026-08-04T10:00:01Z", Bid="1.01")) == []

    def test_stateless_api_still_sees_only_complete_pushes(self) -> None:
        """parse_rate_messages keeps its pre-#2252 contract."""
        assert parse_rate_messages(_rate_frame(1001, "2026-08-04T10:00:00Z", Bid="1.00")) == []
        assert len(parse_rate_messages(_rate_frame(1001, "2026-08-04T10:00:00Z", Bid="1.00", Ask="1.02"))) == 1

    def test_batched_frame_merges_deltas_in_order(self) -> None:
        """A single frame can carry several partials for one instrument;
        each must merge onto the result of the previous."""
        store = RateStateStore()
        self._apply(store, _rate_frame(1001, "2026-08-04T10:00:00Z", Bid="1.00", Ask="1.02"))
        raw = json.dumps(
            {
                "messages": [
                    {
                        "topic": "instrument:1001",
                        "type": "Trading.Instrument.Rate",
                        "content": json.dumps({"Date": "2026-08-04T10:00:01Z", "Bid": "1.10"}),
                    },
                    {
                        "topic": "instrument:1001",
                        "type": "Trading.Instrument.Rate",
                        "content": json.dumps({"Date": "2026-08-04T10:00:02Z", "Ask": "1.15"}),
                    },
                ]
            }
        )
        updates = self._apply(store, raw)
        assert [(u.bid, u.ask) for u in updates] == [
            (Decimal("1.10"), Decimal("1.02")),
            (Decimal("1.10"), Decimal("1.15")),
        ]


class TestParseRateMessageOfficialEnvelope:
    """Regression for #503 — the actual eToro WS frame shape per the
    official documentation
    (https://api-portal.etoro.com/api-reference/websocket/topics.md):

        {
          "messages": [
            {
              "topic": "instrument:100000",
              "content": "{\\"Ask\\":\\"...\\", ...}",
              "id": "...",
              "type": "Trading.Instrument.Rate"
            }
          ]
        }

    Pre-#503 the parser only recognised ``{type, data}`` at the top
    level, dropping every real frame on the floor. ``quotes`` table
    looked populated only because the (now-retired) Phase 2 of
    ``fx_rates_refresh`` was writing rows via REST. Post-#502 with
    Phase 2 gone, the WS path was the sole writer and Tier 3
    instruments (BTC, LRC) silently never updated. These tests pin
    the actual envelope shape so future drift fails loud."""

    def test_messages_envelope_with_string_encoded_content(self) -> None:
        """The documented eToro shape: ``messages: [...]`` outer wrap,
        ``content`` field carrying a JSON-encoded string."""
        inner_content = json.dumps(
            {
                "Ask": "84917.73",
                "Bid": "83232.21",
                "LastExecution": "84072.94",
                "Date": "2025-04-01T08:36:02.8305456Z",
                "PriceRateID": "106439224591",
            }
        )
        raw = json.dumps(
            {
                "messages": [
                    {
                        "topic": "instrument:100000",
                        "content": inner_content,
                        "id": "f1992278-2c4a-4b8f-92d6-8b99f5e1cb00",
                        "type": "Trading.Instrument.Rate",
                    }
                ]
            }
        )
        update = parse_rate_message(raw)
        assert update is not None
        assert update.instrument_id == 100000
        assert update.bid == Decimal("83232.21")
        assert update.ask == Decimal("84917.73")
        assert update.last == Decimal("84072.94")

    def test_messages_envelope_recovers_id_from_topic(self) -> None:
        """eToro's ``content`` does not carry ``InstrumentID`` — the
        parser must derive it from the message's ``topic`` field
        (``instrument:<id>``)."""
        inner_content = json.dumps(
            {
                "Ask": "100",
                "Bid": "99",
                "LastExecution": "99.5",
                "Date": "2026-04-25T10:00:00Z",
            }
        )
        raw = json.dumps(
            {
                "messages": [
                    {
                        "topic": "instrument:100050",
                        "content": inner_content,
                        "id": "x",
                        "type": "Trading.Instrument.Rate",
                    }
                ]
            }
        )
        update = parse_rate_message(raw)
        assert update is not None
        assert update.instrument_id == 100050

    def test_parse_rate_messages_returns_every_tick_in_a_batch(self) -> None:
        """A single WS frame can carry multiple rates. The listener
        must process every one — pre-#503 only the first matched."""
        msgs = []
        for iid, bid, ask in [(100000, "83232.21", "84917.73"), (100050, "0.10", "0.11")]:
            content = json.dumps(
                {
                    "Ask": ask,
                    "Bid": bid,
                    "LastExecution": bid,
                    "Date": "2026-04-25T10:00:00Z",
                }
            )
            msgs.append(
                {
                    "topic": f"instrument:{iid}",
                    "content": content,
                    "id": str(iid),
                    "type": "Trading.Instrument.Rate",
                }
            )
        raw = json.dumps({"messages": msgs})
        updates = parse_rate_messages(raw)
        assert len(updates) == 2
        assert {u.instrument_id for u in updates} == {100000, 100050}

    def test_messages_envelope_skips_non_rate_inner_messages(self) -> None:
        """A frame may interleave private events with rate ticks; the
        rate parser must skip the private ones, not abort."""
        rate_content = json.dumps(
            {
                "Ask": "100",
                "Bid": "99",
                "LastExecution": "99.5",
                "Date": "2026-04-25T10:00:00Z",
            }
        )
        raw = json.dumps(
            {
                "messages": [
                    {
                        "topic": "private",
                        "content": "{}",
                        "id": "p",
                        "type": "Trading.OrderForCloseMultiple.Update",
                    },
                    {
                        "topic": "instrument:100000",
                        "content": rate_content,
                        "id": "r",
                        "type": "Trading.Instrument.Rate",
                    },
                ]
            }
        )
        updates = parse_rate_messages(raw)
        assert len(updates) == 1
        assert updates[0].instrument_id == 100000


class TestMixedBatchFrame:
    """Prevention regression for the ``_listen`` ``continue`` bug
    (#504 review BLOCKING). A single ``messages: [...]`` frame may
    carry one private-event inner message AND one or more rate-tick
    inner messages. Pre-fix, ``_listen`` saw ``is_private_event``
    return True and ``continue``d before reaching the rate parser,
    silently dropping every rate in that frame. The two assertions
    below pin the contract: BOTH ``is_private_event`` and
    ``parse_rate_messages`` must surface their respective inner
    messages on the same frame."""

    def test_mixed_frame_yields_private_event_and_rate_tick(self) -> None:
        rate_content = json.dumps(
            {
                "Ask": "100",
                "Bid": "99",
                "LastExecution": "99.5",
                "Date": "2026-04-25T10:00:00Z",
            }
        )
        raw = json.dumps(
            {
                "messages": [
                    {
                        "topic": "private",
                        "content": "{}",
                        "id": "p",
                        "type": "Trading.OrderForCloseMultiple.Update",
                    },
                    {
                        "topic": "instrument:100000",
                        "content": rate_content,
                        "id": "r",
                        "type": "Trading.Instrument.Rate",
                    },
                ]
            }
        )
        # Both predicates must report their finding from the same
        # frame — _listen must dispatch both paths.
        assert is_private_event(raw) is True
        updates = parse_rate_messages(raw)
        assert len(updates) == 1
        assert updates[0].instrument_id == 100000


class TestIsPrivateEventOfficialEnvelope:
    """Companion regression for the ``messages`` envelope on the
    private-channel path."""

    def test_messages_envelope_recognises_private_event(self) -> None:
        raw = json.dumps(
            {
                "messages": [
                    {
                        "topic": "private",
                        "content": "{}",
                        "id": "p",
                        "type": "Trading.OrderForCloseMultiple.Update",
                    }
                ]
            }
        )
        assert is_private_event(raw) is True

    def test_messages_envelope_skips_when_no_private_inner(self) -> None:
        raw = json.dumps(
            {
                "messages": [
                    {
                        "topic": "instrument:1",
                        "content": "{}",
                        "id": "r",
                        "type": "Trading.Instrument.Rate",
                    }
                ]
            }
        )
        assert is_private_event(raw) is False


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
                "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
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
                "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
                "VALUES (1001, 'AAPL', 'Apple', TRUE), "
                "(1002, 'MSFT', 'Microsoft', TRUE), "
                "(1003, 'NVDA', 'Nvidia', TRUE), "
                "(1004, 'GOOG', 'Google', TRUE)"
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
        # Post-#2252 ``_listen`` admits only subscribed ids, so the
        # frame under test needs a ref. Unchanged in intent: this test
        # asserts a rate frame still lands AFTER a private event.
        sub._topic_refs[1001] = 1

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

    async def test_listen_ignores_frames_for_unsubscribed_instruments(self) -> None:
        """#2252 review round 2 — ``_rate_state`` must not be repopulated
        by a frame for an id we no longer hold a ref for.

        ``remove_instruments`` forgets BEFORE the wire Unsubscribe
        lands, so a dropped / rejected / cancelled Unsubscribe would
        otherwise let already-buffered frames refill the store
        indefinitely. This is what bounds it, not ``forget``.
        """
        upsert_calls: list[QuoteUpdate] = []
        sentinel: Any = object()
        sub = EtoroWebSocketSubscriber(
            api_key="API",
            user_key="USR",
            env="demo",
            pool=sentinel,
            watched_ids_provider=lambda: [],
            reconcile_runner=lambda: None,
        )
        sub._sync_upsert = upsert_calls.append  # type: ignore[method-assign]
        sub._topic_refs[1001] = 1  # 2002 deliberately absent

        class FakeWs:
            def __init__(self, frames: list[str]) -> None:
                self._frames = frames

            def __aiter__(self) -> FakeWs:
                return self

            async def __anext__(self) -> str:
                if not self._frames:
                    raise StopAsyncIteration
                return self._frames.pop(0)

        ws = FakeWs(
            [
                _rate_frame(1001, "2026-08-04T10:00:00Z", Bid="100", Ask="101"),
                _rate_frame(2002, "2026-08-04T10:00:00Z", Bid="200", Ask="201"),
            ]
        )
        await sub._listen(ws)  # type: ignore[arg-type]

        assert [u.instrument_id for u in upsert_calls] == [1001]
        assert 2002 not in sub._rate_state._state
        assert set(sub._rate_state._state) <= set(sub._topic_refs)


# ---------------------------------------------------------------------------
# Dynamic page-view subscribe / unsubscribe (#485)
# ---------------------------------------------------------------------------


class TestBuildUnsubscribeMessage:
    def test_envelope_shape(self) -> None:
        raw = build_unsubscribe_message([1001, 1002])
        assert raw is not None
        msg = json.loads(raw)
        assert msg["operation"] == "Unsubscribe"
        assert msg["data"]["topics"] == ["instrument:1001", "instrument:1002"]
        # No ``snapshot`` field — unsubscribe envelope per eToro
        # docs is topics-only.
        assert "snapshot" not in msg["data"]
        assert "id" in msg

    def test_empty_returns_none(self) -> None:
        assert build_unsubscribe_message([]) is None


class _DynFakeWs:
    """Minimal stand-in for ``ClientConnection`` used to capture
    dynamic frames sent by the subscriber outside of ``_listen``."""

    def __init__(self) -> None:
        self.sent: list[str] = []
        self.fail_next_send: bool = False

    async def send(self, payload: str) -> None:
        if self.fail_next_send:
            self.fail_next_send = False
            raise OSError("simulated send failure")
        self.sent.append(payload)


def _make_dyn_subscriber() -> EtoroWebSocketSubscriber:
    sentinel: Any = object()
    return EtoroWebSocketSubscriber(
        api_key="API",
        user_key="USR",
        env="demo",
        pool=sentinel,
        watched_ids_provider=lambda: [],
        reconcile_runner=lambda: None,
    )


class TestRefCountedAddInstruments:
    async def test_first_add_sends_subscribe_frame(self) -> None:
        sub = _make_dyn_subscriber()
        fake = _DynFakeWs()
        sub._ws = fake  # type: ignore[assignment]

        await sub.add_instruments([1001, 1002])

        assert len(fake.sent) == 1
        msg = json.loads(fake.sent[0])
        assert msg["operation"] == "Subscribe"
        assert sorted(msg["data"]["topics"]) == ["instrument:1001", "instrument:1002"]
        assert sub._topic_refs == {1001: 1, 1002: 1}

    async def test_second_add_for_same_id_bumps_ref_without_frame(self) -> None:
        sub = _make_dyn_subscriber()
        fake = _DynFakeWs()
        sub._ws = fake  # type: ignore[assignment]

        await sub.add_instruments([1001])
        await sub.add_instruments([1001])

        assert len(fake.sent) == 1  # only initial
        assert sub._topic_refs == {1001: 2}

    async def test_add_with_no_live_ws_updates_refs_only(self) -> None:
        """During a reconnect window ``_ws`` is None. Add path still
        tracks refs so the next connect re-subscribes from
        accumulated state."""
        sub = _make_dyn_subscriber()
        sub._ws = None

        await sub.add_instruments([1001])

        assert sub._topic_refs == {1001: 1}

    async def test_send_failure_preserves_ref_counts(self) -> None:
        sub = _make_dyn_subscriber()
        fake = _DynFakeWs()
        fake.fail_next_send = True
        sub._ws = fake  # type: ignore[assignment]

        await sub.add_instruments([1001])

        assert sub._topic_refs == {1001: 1}
        assert fake.sent == []


class TestRefCountedRemoveInstruments:
    async def test_decrement_to_zero_sends_unsubscribe(self) -> None:
        sub = _make_dyn_subscriber()
        fake = _DynFakeWs()
        sub._ws = fake  # type: ignore[assignment]

        await sub.add_instruments([1001])
        await sub.remove_instruments([1001])

        assert len(fake.sent) == 2
        unsub = json.loads(fake.sent[1])
        assert unsub["operation"] == "Unsubscribe"
        assert unsub["data"]["topics"] == ["instrument:1001"]
        assert sub._topic_refs == {}

    async def test_decrement_above_zero_does_not_unsubscribe(self) -> None:
        sub = _make_dyn_subscriber()
        fake = _DynFakeWs()
        sub._ws = fake  # type: ignore[assignment]

        await sub.add_instruments([1001])
        await sub.add_instruments([1001])
        await sub.remove_instruments([1001])

        assert len(fake.sent) == 1
        assert sub._topic_refs == {1001: 1}

    async def test_multi_tab_share_keeps_topic_alive_until_last_close(self) -> None:
        """Two tabs viewing the same instrument share the topic. The
        first close drops the refcount from 2→1 (no Unsubscribe); the
        topic stays alive while the second tab is still viewing. Only
        the second close (1→0) sends Unsubscribe to eToro."""
        sub = _make_dyn_subscriber()
        fake = _DynFakeWs()
        sub._ws = fake  # type: ignore[assignment]

        # Tab A opens.
        await sub.add_instruments([1001])
        # Tab B opens (same instrument).
        await sub.add_instruments([1001])
        # Tab A closes — refcount 2→1, no Unsubscribe.
        await sub.remove_instruments([1001])

        # Only the initial Subscribe frame went out; no Unsubscribe.
        assert len(fake.sent) == 1
        assert json.loads(fake.sent[0])["operation"] == "Subscribe"
        assert sub._topic_refs == {1001: 1}

    async def test_remove_unknown_id_is_noop(self) -> None:
        sub = _make_dyn_subscriber()
        fake = _DynFakeWs()
        sub._ws = fake  # type: ignore[assignment]

        await sub.remove_instruments([9999])

        assert fake.sent == []
        assert sub._topic_refs == {}


class TestReconnectSubscribesFromTopicRefs:
    """After reconnect, the Subscribe frame covers ``_topic_refs.keys()``.
    Source-reconcile and page-view callers populated the dict prior to
    the connect; the connect path simply replays the current set in
    one batch so SSE streams that survived the outage keep receiving
    ticks."""

    async def test_connect_subscribes_current_topic_refs(self) -> None:
        from unittest.mock import AsyncMock, MagicMock

        sub = _make_dyn_subscriber()
        # Refs accumulated before reconnect: 1001/1002 from source
        # worker (held + watchlist), 2001/2002 from page-view path.
        sub._topic_refs = {1001: 1, 1002: 1, 2001: 1, 2002: 1}

        fake_ws = MagicMock()
        fake_ws.send = AsyncMock()
        fake_ws.recv = AsyncMock(return_value='{"success": true}')
        fake_ws.__aenter__ = AsyncMock(return_value=fake_ws)
        fake_ws.__aexit__ = AsyncMock(return_value=False)

        async def fake_listen(ws: Any) -> None:
            return

        sub._listen = fake_listen  # type: ignore[method-assign]

        import app.services.etoro_websocket as ws_mod

        original_connect = ws_mod.websockets.connect
        ws_mod.websockets.connect = MagicMock(return_value=fake_ws)  # type: ignore[assignment]
        try:
            await sub._connect_and_listen()
        finally:
            ws_mod.websockets.connect = original_connect  # type: ignore[assignment]

        sent_frames = [json.loads(c.args[0]) for c in fake_ws.send.call_args_list]
        ops = [f["operation"] for f in sent_frames]
        assert ops[:3] == ["Authenticate", "Subscribe", "Subscribe"]
        instrument_topics = sorted(sent_frames[1]["data"]["topics"])
        assert instrument_topics == [
            "instrument:1001",
            "instrument:1002",
            "instrument:2001",
            "instrument:2002",
        ]


class TestWireOrdering:
    """Wire-ordering invariants for add/remove sends. Held under the
    topic lock so that a concurrent remove cannot queue an
    Unsubscribe(T) frame that overtakes an in-flight Subscribe(T)."""

    async def test_concurrent_remove_blocks_until_add_send_completes(self) -> None:
        """``add_instruments`` and ``remove_instruments`` must hold
        the topic lock through the wire send so that a concurrent
        remove cannot queue an Unsubscribe(T) frame that overtakes
        an in-flight Subscribe(T). Without this, eToro can end up
        unsubscribed from a topic ``_topic_refs`` still considers
        live — a held-position feed teardown.

        Regression test for the wire-ordering race Codex flagged on
        PR for #490."""
        sub = _make_dyn_subscriber()

        sent_order: list[str] = []
        block_subscribe = asyncio.Event()

        class GatedWs:
            async def send(self, payload: str) -> None:
                op = json.loads(payload)["operation"]
                if op == "Subscribe":
                    # Hold the Subscribe in flight until the test
                    # sets the gate, so a concurrent remove can race
                    # against us if locking is wrong.
                    await block_subscribe.wait()
                sent_order.append(op)

        sub._ws = GatedWs()  # type: ignore[assignment]

        # add_2001 will trigger a 0→1 transition → Subscribe send →
        # blocks on the gate while still holding the topic lock.
        async def add_2001() -> None:
            await sub.add_instruments([2001])

        # remove_2001 attempts to acquire the same lock; if locking
        # is correct it cannot enter until add_2001 releases.
        async def remove_2001() -> None:
            await asyncio.sleep(0.05)  # let add reach the gate first
            await sub.remove_instruments([2001])

        add_task = asyncio.create_task(add_2001())
        remove_task = asyncio.create_task(remove_2001())

        # Yield enough for add to reach the gated send and remove to
        # block on the lock.
        await asyncio.sleep(0.1)
        assert not add_task.done()
        assert not remove_task.done()

        # Release the gate — Subscribe flushes, add releases the
        # lock, remove acquires + sends Unsubscribe.
        block_subscribe.set()
        await asyncio.gather(add_task, remove_task)

        # Wire order must be Subscribe THEN Unsubscribe; reversed
        # would tear down the feed.
        assert sent_order == ["Subscribe", "Unsubscribe"]
