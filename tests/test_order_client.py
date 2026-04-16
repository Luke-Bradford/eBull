"""
Tests for app.services.order_client.

Structure:
  - TestSyntheticFill         — _synthetic_fill pure logic
  - TestLoadApprovedRec       — _load_approved_recommendation validation
  - TestExecuteOrderDemoMode  — full execute_order in demo mode (no broker)
  - TestExecuteOrderLiveMode  — execute_order with mocked broker provider
  - TestExecuteOrderFailures  — error paths: rejected, failed, missing broker

Mock DB approach mirrors test_execution_guard.py:
  - _make_cursor(rows) builds a context-manager cursor mock
  - _make_conn(cursors) builds a connection mock
  - conn.transaction() is a no-op context manager

Cursor call order inside execute_order (demo BUY with suggested_size_pct):
  1. _load_approved_recommendation  — fetchone
  2. _load_cash                     — fetchone
  3. _load_latest_quote_price       — fetchone
  4. _persist_order                 — fetchone (INSERT RETURNING)
  5. _persist_fill                  — fetchone (INSERT RETURNING)
  6. conn.execute x5                — position upsert, broker_positions, cash_ledger, rec status, audit

Cursor call order inside execute_order (demo EXIT):
  1. _load_approved_recommendation  — fetchone
  2. _load_position_units           — fetchone
  3. _load_latest_quote_price       — fetchone
  4. _persist_order                 — fetchone (INSERT RETURNING)
  5. _persist_fill                  — fetchone (INSERT RETURNING)
  6. conn.execute x4                — position update, cash_ledger, rec status, audit
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.providers.broker import BrokerOrderResult, OrderParams
from app.services.order_client import (
    _load_approved_recommendation,
    _load_latest_quote_price,
    _load_position_units,
    _persist_broker_position,
    _synthetic_fill,
    _update_position_buy,
    execute_order,
)
from app.services.runtime_config import RuntimeConfig, RuntimeConfigCorrupt

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)

_RUNTIME_DEMO = RuntimeConfig(
    enable_auto_trading=True,
    enable_live_trading=False,
    display_currency="USD",
    updated_at=_NOW,
    updated_by="test",
    reason="test",
)

_RUNTIME_LIVE = RuntimeConfig(
    enable_auto_trading=True,
    enable_live_trading=True,
    display_currency="USD",
    updated_at=_NOW,
    updated_by="test",
    reason="test",
)


@pytest.fixture(autouse=True)
def _patch_runtime_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default every test in this file to demo mode.

    Tests that need live mode override via monkeypatch within the test body.
    Tests that need a corrupt-config failure raise RuntimeConfigCorrupt.
    """
    monkeypatch.setattr(
        "app.services.order_client.get_runtime_config",
        lambda _conn: _RUNTIME_DEMO,
    )


def _make_cursor(rows: list[dict[str, Any]]) -> MagicMock:
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.fetchone.return_value = rows[0] if rows else None
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    return cur


def _make_conn(cursor_sequence: list[MagicMock]) -> MagicMock:
    """
    Build a fake psycopg connection.
    conn.cursor() calls consume cursor_sequence in order.
    conn.execute() is a no-op mock.
    conn.transaction() is a no-op context manager.
    """
    conn = MagicMock()
    conn.cursor.side_effect = cursor_sequence
    conn.execute.return_value = MagicMock()
    conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
    conn.transaction.return_value.__exit__ = MagicMock(return_value=False)
    return conn


def _rec_cursor(
    action: str = "BUY",
    instrument_id: int = 1,
    recommendation_id: int = 42,
    target_entry: float | None = 100.0,
    suggested_size_pct: float | None = 0.05,
    model_version: str | None = "v1-balanced",
    status: str = "approved",
    stop_loss_rate: float | None = None,
    take_profit_rate: float | None = None,
) -> MagicMock:
    return _make_cursor(
        [
            {
                "recommendation_id": recommendation_id,
                "instrument_id": instrument_id,
                "action": action,
                "target_entry": target_entry,
                "suggested_size_pct": suggested_size_pct,
                "model_version": model_version,
                "status": status,
                "stop_loss_rate": stop_loss_rate,
                "take_profit_rate": take_profit_rate,
            }
        ]
    )


def _cash_cursor(balance: float | None = 10_000.0) -> MagicMock:
    return _make_cursor([{"balance": balance}])


def _quote_cursor(last: float | None = 150.0) -> MagicMock:
    return _make_cursor([{"last": last}] if last is not None else [])


def _position_cursor(current_units: float = 10.0) -> MagicMock:
    return _make_cursor([{"current_units": current_units}])


def _order_returning_cursor(order_id: int = 1) -> MagicMock:
    return _make_cursor([{"order_id": order_id}])


def _fill_returning_cursor(fill_id: int = 1) -> MagicMock:
    return _make_cursor([{"fill_id": fill_id}])


# ---------------------------------------------------------------------------
# TestSyntheticFill
# ---------------------------------------------------------------------------


class TestSyntheticFill:
    def test_buy_with_amount(self) -> None:
        result = _synthetic_fill(
            instrument_id=1,
            action="BUY",
            quote_price=Decimal("100"),
            requested_amount=Decimal("500"),
            requested_units=None,
        )
        assert result.status == "filled"
        assert result.filled_price == Decimal("100")
        assert result.filled_units == Decimal("5.000000")
        assert result.broker_order_ref == "DEMO-1-BUY"
        assert result.raw_payload["demo"] is True
        assert result.fees == Decimal("0")

    def test_buy_with_units(self) -> None:
        result = _synthetic_fill(
            instrument_id=1,
            action="BUY",
            quote_price=Decimal("50"),
            requested_amount=None,
            requested_units=Decimal("10"),
        )
        assert result.status == "filled"
        assert result.filled_price == Decimal("50")
        assert result.filled_units == Decimal("10")

    def test_exit_with_units(self) -> None:
        result = _synthetic_fill(
            instrument_id=1,
            action="EXIT",
            quote_price=Decimal("200"),
            requested_amount=None,
            requested_units=Decimal("5"),
        )
        assert result.status == "filled"
        assert result.filled_price == Decimal("200")
        assert result.filled_units == Decimal("5")

    def test_no_quote_price_uses_zero(self) -> None:
        result = _synthetic_fill(
            instrument_id=1,
            action="BUY",
            quote_price=None,
            requested_amount=Decimal("500"),
            requested_units=None,
        )
        assert result.filled_price == Decimal("0")
        assert result.filled_units == Decimal("0")
        assert "no quote available" in result.raw_payload["note"]

    def test_zero_amount_and_no_units(self) -> None:
        result = _synthetic_fill(
            instrument_id=1,
            action="BUY",
            quote_price=Decimal("100"),
            requested_amount=None,
            requested_units=None,
        )
        assert result.filled_units == Decimal("0")


# ---------------------------------------------------------------------------
# TestSyntheticFillSpreadCost
# ---------------------------------------------------------------------------


class TestSyntheticFillSpreadCost:
    def test_buy_fills_at_ask_with_spread_fee(self) -> None:
        result = _synthetic_fill(
            instrument_id=123,
            action="BUY",
            quote_price=Decimal("100.00"),
            requested_amount=Decimal("1000"),
            requested_units=None,
            bid=Decimal("99.80"),
            ask=Decimal("100.20"),
        )
        # BUY fills at ask
        assert result.filled_price == Decimal("100.20")
        # units = 1000 / 100.20
        expected_units = (Decimal("1000") / Decimal("100.20")).quantize(Decimal("0.000001"))
        assert result.filled_units == expected_units
        # Spread cost = (ask - bid) / 2 * units
        spread_per_unit = (Decimal("100.20") - Decimal("99.80")) / 2
        expected_fees = (spread_per_unit * expected_units).quantize(Decimal("0.000001"))
        assert result.fees == expected_fees

    def test_exit_fills_at_bid_with_spread_fee(self) -> None:
        result = _synthetic_fill(
            instrument_id=123,
            action="EXIT",
            quote_price=Decimal("100.00"),
            requested_amount=None,
            requested_units=Decimal("10"),
            bid=Decimal("99.80"),
            ask=Decimal("100.20"),
        )
        assert result.filled_price == Decimal("99.80")
        spread_per_unit = (Decimal("100.20") - Decimal("99.80")) / 2
        expected_fees = (spread_per_unit * Decimal("10")).quantize(Decimal("0.000001"))
        assert result.fees == expected_fees

    def test_no_bid_ask_falls_back_to_zero_fees(self) -> None:
        result = _synthetic_fill(
            instrument_id=123,
            action="BUY",
            quote_price=Decimal("100.00"),
            requested_amount=Decimal("1000"),
            requested_units=None,
            bid=None,
            ask=None,
        )
        assert result.fees == Decimal("0")
        assert result.filled_price == Decimal("100.00")


# ---------------------------------------------------------------------------
# TestLoadApprovedRec
# ---------------------------------------------------------------------------


class TestLoadApprovedRec:
    def test_not_found_raises(self) -> None:
        conn = _make_conn([_make_cursor([])])
        with pytest.raises(ValueError, match="not found"):
            _load_approved_recommendation(conn, 999)

    def test_not_approved_raises(self) -> None:
        conn = _make_conn([_rec_cursor(status="proposed")])
        with pytest.raises(ValueError, match="expected 'approved'"):
            _load_approved_recommendation(conn, 42)

    def test_approved_returns_row(self) -> None:
        conn = _make_conn([_rec_cursor(status="approved")])
        row = _load_approved_recommendation(conn, 42)
        assert row["recommendation_id"] == 42
        assert row["action"] == "BUY"
        assert row["status"] == "approved"


# ---------------------------------------------------------------------------
# TestLoadHelpers
# ---------------------------------------------------------------------------


class TestLoadHelpers:
    def test_quote_price_returns_decimal(self) -> None:
        conn = _make_conn([_quote_cursor(last=150.50)])
        price = _load_latest_quote_price(conn, 1)
        assert price == Decimal("150.5")

    def test_quote_price_none_when_no_rows(self) -> None:
        conn = _make_conn([_make_cursor([])])
        price = _load_latest_quote_price(conn, 1)
        assert price is None

    def test_quote_price_none_when_last_is_null(self) -> None:
        conn = _make_conn([_make_cursor([{"last": None}])])
        price = _load_latest_quote_price(conn, 1)
        assert price is None

    def test_position_units_returns_decimal(self) -> None:
        conn = _make_conn([_position_cursor(current_units=10.5)])
        units = _load_position_units(conn, 1)
        assert units == Decimal("10.5")

    def test_position_units_zero_when_no_position(self) -> None:
        conn = _make_conn([_make_cursor([])])
        units = _load_position_units(conn, 1)
        assert units == Decimal("0")


# ---------------------------------------------------------------------------
# TestExecuteOrderDemoMode
# ---------------------------------------------------------------------------


class TestExecuteOrderDemoMode:
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_buy_produces_fill_and_order(self, _mock_now: MagicMock) -> None:
        """Demo BUY: synthetic fill, order row, fill row, position upsert, cash, audit."""
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            _quote_cursor(last=100.0),
            # Inside transaction:
            _order_returning_cursor(order_id=7),
            _fill_returning_cursor(fill_id=3),
        ]
        conn = _make_conn(cursors)
        result = execute_order(
            conn,
            recommendation_id=42,
            decision_id=10,
        )
        assert result.outcome == "filled"
        assert result.order_id == 7
        assert result.fill_id == 3
        assert result.broker_order_ref == "DEMO-1-BUY"
        assert "order filled" in result.explanation

        # conn.execute: position upsert, broker_positions, cash_ledger, rec status, audit = 5
        assert conn.execute.call_count == 5

    @patch("app.services.order_client._maybe_trigger_attribution")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_exit_produces_fill(self, _mock_now: MagicMock, _mock_attr: MagicMock) -> None:
        """Demo EXIT: loads position units, synthetic fill at quote price."""
        cursors = [
            _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
            _position_cursor(current_units=5.0),
            _quote_cursor(last=200.0),
            # Inside transaction:
            _order_returning_cursor(order_id=8),
            _fill_returning_cursor(fill_id=4),
            # Post-fill: read current_units for attribution check
            _make_cursor([{"current_units": 0}]),
        ]
        conn = _make_conn(cursors)
        result = execute_order(
            conn,
            recommendation_id=42,
            decision_id=10,
        )
        assert result.outcome == "filled"
        assert result.order_id == 8
        assert result.fill_id == 4

        # conn.execute: position update, cash_ledger, rec status, audit = 4
        assert conn.execute.call_count == 4

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_buy_no_quote_produces_failed_no_fill(self, _mock_now: MagicMock) -> None:
        """Demo BUY with no quote: zero-unit fill is not persisted."""
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            _make_cursor([]),  # no quote
            # Inside transaction: order persisted, no fill (zero units)
            _order_returning_cursor(order_id=9),
        ]
        conn = _make_conn(cursors)
        result = execute_order(
            conn,
            recommendation_id=42,
            decision_id=10,
        )
        assert result.outcome == "failed"
        assert result.fill_id is None
        assert result.order_id == 9
        assert "zero units" in result.explanation

        # conn.execute: rec status update, audit = 2 (no fill/position/cash)
        assert conn.execute.call_count == 2

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_mode_never_calls_broker(self, _mock_now: MagicMock) -> None:
        """Demo mode must never invoke the broker provider."""
        broker = MagicMock()
        cursors = [
            _rec_cursor(action="BUY"),
            _cash_cursor(balance=10_000.0),
            _quote_cursor(last=100.0),
            _order_returning_cursor(order_id=1),
            _fill_returning_cursor(fill_id=1),
        ]
        conn = _make_conn(cursors)
        execute_order(
            conn,
            recommendation_id=42,
            decision_id=10,
            broker=broker,
        )
        broker.place_order.assert_not_called()
        broker.close_position.assert_not_called()
        broker.get_order_status.assert_not_called()

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_buy_writes_broker_positions_row(self, _mock_now: MagicMock) -> None:
        """BUY/ADD fills must INSERT into broker_positions so EXIT can resolve."""
        cursors = [
            _rec_cursor(action="BUY", stop_loss_rate=90.0, take_profit_rate=120.0),
            _cash_cursor(balance=10_000.0),
            _quote_cursor(last=100.0),
            _order_returning_cursor(order_id=7),
            _fill_returning_cursor(fill_id=3),
        ]
        conn = _make_conn(cursors)
        result = execute_order(
            conn,
            recommendation_id=42,
            decision_id=10,
        )
        assert result.outcome == "filled"

        # Find the broker_positions INSERT among conn.execute calls
        bp_calls = [c for c in conn.execute.call_args_list if "broker_positions" in str(c)]
        assert len(bp_calls) == 1, f"Expected 1 broker_positions INSERT, got {len(bp_calls)}"

        # Verify params passed through execute_order (not just SQL shape)
        params = bp_calls[0].args[1]
        assert params["pid"] == 7  # position_id = order_id
        assert params["iid"] == 1  # instrument_id from rec
        assert params["sl"] == Decimal("90")
        assert params["tp"] == Decimal("120")
        assert params["no_sl"] is False
        assert params["no_tp"] is False

    @patch("app.services.order_client._maybe_trigger_attribution")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_exit_does_not_write_broker_positions(self, _mock_now: MagicMock, _mock_attr: MagicMock) -> None:
        """EXIT fills must NOT insert into broker_positions (the row already exists)."""
        cursors = [
            _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
            _position_cursor(current_units=5.0),
            _quote_cursor(last=200.0),
            _order_returning_cursor(order_id=8),
            _fill_returning_cursor(fill_id=4),
            # Post-fill: read current_units for attribution check
            _make_cursor([{"current_units": 0}]),
        ]
        conn = _make_conn(cursors)
        result = execute_order(
            conn,
            recommendation_id=42,
            decision_id=10,
        )
        assert result.outcome == "filled"
        bp_calls = [c for c in conn.execute.call_args_list if "broker_positions" in str(c)]
        assert len(bp_calls) == 0

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_buy_writes_execution_audit(self, _mock_now: MagicMock) -> None:
        """Every code path must write a decision_audit row for execution outcome."""
        cursors = [
            _rec_cursor(action="BUY"),
            _cash_cursor(balance=10_000.0),
            _quote_cursor(last=100.0),
            _order_returning_cursor(order_id=1),
            _fill_returning_cursor(fill_id=1),
        ]
        conn = _make_conn(cursors)
        execute_order(
            conn,
            recommendation_id=42,
            decision_id=10,
        )
        # Find the decision_audit INSERT among conn.execute calls
        audit_calls = [c for c in conn.execute.call_args_list if "decision_audit" in str(c)]
        assert len(audit_calls) == 1


# ---------------------------------------------------------------------------
# TestExecuteOrderLiveMode
# ---------------------------------------------------------------------------


class TestExecuteOrderLiveMode:
    @pytest.fixture(autouse=True)
    def _force_live(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Override the file-level demo default for every test in this class.
        monkeypatch.setattr(
            "app.services.order_client.get_runtime_config",
            lambda _conn: _RUNTIME_LIVE,
        )

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_live_buy_calls_broker_place_order(self, _mock_now: MagicMock) -> None:
        broker = MagicMock()
        broker.place_order.return_value = BrokerOrderResult(
            broker_order_ref="ORD-123",
            status="filled",
            filled_price=Decimal("100"),
            filled_units=Decimal("5"),
            fees=Decimal("1.50"),
            raw_payload={"orderId": "ORD-123", "status": "filled"},
        )
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            # broker called (no cursor)
            _order_returning_cursor(order_id=10),
            _fill_returning_cursor(fill_id=6),
        ]
        conn = _make_conn(cursors)
        result = execute_order(
            conn,
            recommendation_id=42,
            decision_id=10,
            broker=broker,
        )
        assert result.outcome == "filled"
        assert result.broker_order_ref == "ORD-123"
        broker.place_order.assert_called_once()

    @patch("app.services.order_client._maybe_trigger_attribution")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_live_exit_calls_broker_close_position(self, _mock_now: MagicMock, _mock_attr: MagicMock) -> None:
        broker = MagicMock()
        broker.close_position.return_value = BrokerOrderResult(
            broker_order_ref="ORD-456",
            status="filled",
            filled_price=Decimal("200"),
            filled_units=Decimal("5"),
            fees=Decimal("0"),
            raw_payload={"orderId": "ORD-456", "status": "filled"},
        )
        cursors = [
            _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
            _position_cursor(current_units=5.0),
            # _load_position_id_for_exit resolves instrument_id → position_id
            _make_cursor([{"position_id": 98765}]),
            # broker called (no cursor)
            _order_returning_cursor(order_id=11),
            _fill_returning_cursor(fill_id=7),
            # Post-fill: read current_units for attribution check
            _make_cursor([{"current_units": 0}]),
        ]
        conn = _make_conn(cursors)
        result = execute_order(
            conn,
            recommendation_id=42,
            decision_id=10,
            broker=broker,
        )
        assert result.outcome == "filled"
        broker.close_position.assert_called_once_with(98765)

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_live_exit_no_broker_positions_row_fails(self, _mock_now: MagicMock) -> None:
        """EXIT with no broker_positions row returns failed (pre-024 positions)."""
        broker = MagicMock()
        cursors = [
            _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
            _position_cursor(current_units=5.0),
            # _load_position_id_for_exit returns None — no broker_positions row
            _make_cursor([]),
            # broker NOT called — broker_result is constructed inline as failed
            _order_returning_cursor(order_id=12),
        ]
        conn = _make_conn(cursors)
        result = execute_order(
            conn,
            recommendation_id=42,
            decision_id=10,
            broker=broker,
        )
        assert result.outcome == "failed"
        broker.close_position.assert_not_called()

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_live_mode_no_broker_raises(self, _mock_now: MagicMock) -> None:
        cursors = [
            _rec_cursor(action="BUY"),
            _cash_cursor(balance=10_000.0),
        ]
        conn = _make_conn(cursors)
        with pytest.raises(ValueError, match="no broker provider supplied"):
            execute_order(
                conn,
                recommendation_id=42,
                decision_id=10,
                broker=None,
            )


# ---------------------------------------------------------------------------
# TestExecuteOrderFailures
# ---------------------------------------------------------------------------


class TestExecuteOrderFailures:
    @pytest.fixture(autouse=True)
    def _force_live(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # All tests in this class exercise broker error paths, which are
        # only reachable in live mode.  The two not-found / not-approved
        # tests fail before the runtime read, so this override is harmless
        # for them.
        monkeypatch.setattr(
            "app.services.order_client.get_runtime_config",
            lambda _conn: _RUNTIME_LIVE,
        )

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_broker_failed_persists_order_with_failed_status(self, _mock_now: MagicMock) -> None:
        """Failed broker call still persists an order row and audit row."""
        broker = MagicMock()
        broker.place_order.return_value = BrokerOrderResult(
            broker_order_ref=None,
            status="failed",
            filled_price=None,
            filled_units=None,
            fees=Decimal("0"),
            raw_payload={"error": "insufficient funds"},
        )
        cursors = [
            _rec_cursor(action="BUY"),
            _cash_cursor(balance=10_000.0),
            _order_returning_cursor(order_id=12),
        ]
        conn = _make_conn(cursors)
        result = execute_order(
            conn,
            recommendation_id=42,
            decision_id=10,
            broker=broker,
        )
        assert result.outcome == "failed"
        assert result.fill_id is None
        assert result.order_id == 12
        assert "failed" in result.explanation

        # conn.execute: rec status update + audit = 2 (no fill/position/cash)
        assert conn.execute.call_count == 2

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_broker_pending_persists_order_with_pending_status(self, _mock_now: MagicMock) -> None:
        """Pending broker response still persists an order row and audit row."""
        broker = MagicMock()
        broker.place_order.return_value = BrokerOrderResult(
            broker_order_ref="ORD-789",
            status="pending",
            filled_price=None,
            filled_units=None,
            fees=Decimal("0"),
            raw_payload={"orderId": "ORD-789", "status": "pending"},
        )
        cursors = [
            _rec_cursor(action="BUY"),
            _cash_cursor(balance=10_000.0),
            _order_returning_cursor(order_id=13),
        ]
        conn = _make_conn(cursors)
        result = execute_order(
            conn,
            recommendation_id=42,
            decision_id=10,
            broker=broker,
        )
        assert result.outcome == "pending"
        assert result.fill_id is None
        assert result.broker_order_ref == "ORD-789"
        assert "pending" in result.explanation

    def test_recommendation_not_found_raises(self) -> None:
        conn = _make_conn([_make_cursor([])])
        with pytest.raises(ValueError, match="not found"):
            execute_order(
                conn,
                recommendation_id=999,
                decision_id=10,
            )

    def test_recommendation_not_approved_raises(self) -> None:
        conn = _make_conn([_rec_cursor(status="proposed")])
        with pytest.raises(ValueError, match="expected 'approved'"):
            execute_order(
                conn,
                recommendation_id=42,
                decision_id=10,
            )

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_broker_rejected_persists_order_row(self, _mock_now: MagicMock) -> None:
        """Rejected broker response persists order with rejected status."""
        broker = MagicMock()
        broker.place_order.return_value = BrokerOrderResult(
            broker_order_ref="ORD-REJ",
            status="rejected",
            filled_price=None,
            filled_units=None,
            fees=Decimal("0"),
            raw_payload={"orderId": "ORD-REJ", "status": "rejected", "reason": "market closed"},
        )
        cursors = [
            _rec_cursor(action="BUY"),
            _cash_cursor(balance=10_000.0),
            _order_returning_cursor(order_id=14),
        ]
        conn = _make_conn(cursors)
        result = execute_order(
            conn,
            recommendation_id=42,
            decision_id=10,
            broker=broker,
        )
        assert result.outcome == "failed"
        assert result.fill_id is None
        assert result.order_id == 14

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_failed_order_still_writes_audit(self, _mock_now: MagicMock) -> None:
        """Even a failed order must produce a decision_audit row."""
        broker = MagicMock()
        broker.place_order.return_value = BrokerOrderResult(
            broker_order_ref=None,
            status="failed",
            filled_price=None,
            filled_units=None,
            fees=Decimal("0"),
            raw_payload={"error": "timeout"},
        )
        cursors = [
            _rec_cursor(action="BUY"),
            _cash_cursor(balance=10_000.0),
            _order_returning_cursor(order_id=15),
        ]
        conn = _make_conn(cursors)
        execute_order(
            conn,
            recommendation_id=42,
            decision_id=10,
            broker=broker,
        )
        audit_calls = [c for c in conn.execute.call_args_list if "decision_audit" in str(c)]
        assert len(audit_calls) == 1


# ---------------------------------------------------------------------------
# TestExecuteOrderRuntimeConfigCorrupt
# ---------------------------------------------------------------------------


class TestExecuteOrderRuntimeConfigCorrupt:
    """RuntimeConfigCorrupt must propagate from execute_order — never silently
    fall through to demo or live mode.  The execution_guard fails closed on
    the same condition; the order client is the second line of defence.
    """

    def test_corrupt_runtime_config_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def _raise(_conn: object) -> RuntimeConfig:
            raise RuntimeConfigCorrupt("singleton missing")

        monkeypatch.setattr("app.services.order_client.get_runtime_config", _raise)

        cursors = [
            _rec_cursor(action="BUY"),
            _cash_cursor(balance=10_000.0),
        ]
        conn = _make_conn(cursors)
        with pytest.raises(RuntimeConfigCorrupt):
            execute_order(conn, recommendation_id=42, decision_id=10)

        # No order should have been persisted, no audit row written.
        conn.transaction.assert_not_called()


# ---------------------------------------------------------------------------
# TestUpdatePositionBuySource
# ---------------------------------------------------------------------------


class TestUpdatePositionBuySource:
    """Verify _update_position_buy writes ``source='ebull'`` and resets on reopen.

    Issue #180 — the positions ``source`` column identifies who currently
    manages the open units.  Every eBull-originated BUY must insert
    ``'ebull'``.  On reopen (ON CONFLICT into a closed row), source must
    flip to ``'ebull'`` too; on ADD into an already-open position, the
    existing source must be preserved so an ebull ADD into a
    broker_sync-owned position doesn't claim ownership of the original
    external open.
    """

    def test_insert_emits_source_literal_and_reopen_reset_clause(self) -> None:
        """INSERT carries the 'ebull' literal AND the reset CASE WHEN.

        With a mocked connection, ``_update_position_buy`` captures a
        single SQL string per call regardless of whether Postgres would
        take the INSERT or the ON CONFLICT branch at runtime — the
        branch decision is made by the planner, not by us.  So the
        unit-level guarantee we can assert here is SQL *shape*: a
        single captured string must contain both the hard-coded VALUES
        literal and the reset CASE WHEN, evaluated together from one
        call.

        End-to-end verification that Postgres actually routes closed
        rows through the reset arm is tracked in the DB integration
        test backlog (#186) — unreachable from a mocked connection.
        """
        conn = _make_conn([])
        _update_position_buy(
            conn,
            instrument_id=42,
            filled_price=Decimal("100"),
            filled_units=Decimal("5"),
            now=_NOW,
        )

        assert conn.execute.call_count == 1
        sql = conn.execute.call_args_list[0].args[0]
        normalised = re.sub(r"\s+", " ", sql)

        # Hard-coded VALUES literal — no parameter placeholder.
        assert "INSERT INTO positions" in normalised
        assert "'ebull'" in normalised
        # Reset CASE WHEN: pre-update row fully closed → overwrite
        # source; otherwise preserve.  Postgres evaluates CASE against
        # the pre-update row, so SET-list ordering is irrelevant.
        assert "positions.current_units <= 0" in normalised
        assert "EXCLUDED.source" in normalised
        assert "ELSE positions.source" in normalised


# ---------------------------------------------------------------------------
# TestPersistBrokerPosition
# ---------------------------------------------------------------------------


class TestPersistBrokerPosition:
    """Verify _persist_broker_position emits the correct INSERT."""

    def test_inserts_with_source_ebull_and_sl_tp(self) -> None:
        conn = _make_conn([])
        _persist_broker_position(
            conn,
            order_id=7,
            instrument_id=42,
            filled_price=Decimal("100"),
            filled_units=Decimal("5"),
            fees=Decimal("1.50"),
            order_params=OrderParams(
                stop_loss_rate=Decimal("90"),
                take_profit_rate=Decimal("120"),
            ),
            raw_payload={"demo": True},
            now=_NOW,
        )
        assert conn.execute.call_count == 1
        sql = conn.execute.call_args_list[0].args[0]
        normalised = re.sub(r"\s+", " ", sql)
        assert "INSERT INTO broker_positions" in normalised
        assert "'ebull'" in normalised
        assert "ON CONFLICT (position_id) DO UPDATE" in normalised
        # ON CONFLICT must update raw_payload to prevent silent payload loss
        assert "raw_payload = EXCLUDED.raw_payload" in normalised

        params = conn.execute.call_args_list[0].args[1]
        assert params["pid"] == 7
        assert params["iid"] == 42
        assert params["units"] == Decimal("5")
        # amount = price * units = 500
        assert params["amount"] == Decimal("500")
        assert params["sl"] == Decimal("90")
        assert params["tp"] == Decimal("120")
        assert params["no_sl"] is False
        assert params["no_tp"] is False

    def test_inserts_without_order_params(self) -> None:
        conn = _make_conn([])
        _persist_broker_position(
            conn,
            order_id=8,
            instrument_id=99,
            filled_price=Decimal("50"),
            filled_units=Decimal("10"),
            fees=Decimal("0"),
            order_params=None,
            raw_payload={"demo": True},
            now=_NOW,
        )
        params = conn.execute.call_args_list[0].args[1]
        assert params["sl"] is None
        assert params["tp"] is None
        assert params["no_sl"] is True
        assert params["no_tp"] is True
        assert params["leverage"] == 1
        assert params["tsl"] is False
