"""Tests for the eToro WebSocket subscriber (#274 Slice 1).

Pure helpers (auth-message build, subscribe-message build, rate-
message parser, spread-pct compute) are unit-tested; the DB upsert
is integration-tested against ``ebull_test``. The connect/listen
loop itself is not exercised — that requires a real WS server or a
heavyweight fixture and adds little safety beyond covering the
component pieces.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import psycopg
import pytest

from app.services.etoro_websocket import (
    QuoteUpdate,
    _compute_spread_pct,
    _is_auth_success,
    build_auth_message,
    build_subscribe_message,
    fetch_watched_instrument_ids,
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
