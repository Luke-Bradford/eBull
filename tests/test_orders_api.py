"""Tests for app.api.orders — POST /portfolio/orders and POST /portfolio/positions/{id}/close.

Test strategy:
  Mock DB via FastAPI dependency override (same pattern as test_api_portfolio).
  The ``get_conn`` dependency is replaced with a mock connection that returns
  ``dict_row``-style dicts.

  ``get_runtime_config`` is patched at the module level to return demo mode
  by default (enable_live_trading=False).

Structure:
  - TestPlaceOrder — validation, kill switch, happy-path demo BUY, broker_positions write
  - TestClosePosition — 404, kill switch, happy-path demo close
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app
from app.services.runtime_config import RuntimeConfig

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 14, 12, 0, 0, tzinfo=UTC)

_DEFAULT_CONFIG = RuntimeConfig(
    enable_auto_trading=False,
    enable_live_trading=False,
    display_currency="USD",
    updated_at=_NOW,
    updated_by="test",
    reason="test",
)

_KILL_SWITCH_OFF = [{"is_active": False, "reason": None}]
_KILL_SWITCH_ON = [{"is_active": True, "reason": "manual halt"}]
_QUOTE_ROW = [{"last": 150.0}]
_NO_ROWS: list[dict[str, Any]] = []


def _mock_conn(cursor_results: list[list[dict[str, Any]]]) -> MagicMock:
    """Build a mock psycopg.Connection.

    ``cursor_results`` is a list of result sets, one per ``cur.execute()`` call.
    Each cursor().execute() pops the next result set from the iterator.
    """
    cur = MagicMock()
    result_iter = iter(cursor_results)

    def _on_execute(*_args: Any, **_kwargs: Any) -> None:
        rows = next(result_iter)
        cur.fetchone.return_value = rows[0] if rows else None
        cur.fetchall.return_value = rows

    cur.execute.side_effect = _on_execute
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cur

    tx = MagicMock()
    tx.__enter__ = MagicMock(return_value=tx)
    tx.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value = tx

    return conn


def _with_conn(cursor_results: list[list[dict[str, Any]]]) -> MagicMock:
    conn = _mock_conn(cursor_results)

    def _override() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _override
    return conn


def _cleanup() -> None:
    app.dependency_overrides[get_conn] = _fallback_conn


def _fallback_conn() -> Iterator[MagicMock]:
    yield _mock_conn([])


app.dependency_overrides.setdefault(get_conn, _fallback_conn)

client = TestClient(app)


# ---------------------------------------------------------------------------
# TestPlaceOrder
# ---------------------------------------------------------------------------


class TestPlaceOrder:
    """POST /portfolio/orders — place a manual BUY/ADD order."""

    def setup_method(self) -> None:
        self._patch_config = patch(
            "app.api.orders.get_runtime_config",
            return_value=_DEFAULT_CONFIG,
        )
        self._patch_config.start()

    def teardown_method(self) -> None:
        self._patch_config.stop()
        _cleanup()

    def test_rejects_missing_instrument_id(self) -> None:
        """422 when instrument_id is not provided."""
        _with_conn([])
        resp = client.post("/portfolio/orders", json={"action": "BUY", "amount": 100})
        assert resp.status_code == 422

    def test_rejects_missing_action(self) -> None:
        """422 when action is not provided."""
        _with_conn([])
        resp = client.post("/portfolio/orders", json={"instrument_id": 1, "amount": 100})
        assert resp.status_code == 422

    def test_rejects_both_amount_and_units(self) -> None:
        """400 when both amount and units are provided."""
        _with_conn([_KILL_SWITCH_OFF])
        resp = client.post(
            "/portfolio/orders",
            json={"instrument_id": 1, "action": "BUY", "amount": 100, "units": 5},
        )
        assert resp.status_code == 400
        assert "not both" in resp.json()["detail"]

    def test_rejects_neither_amount_nor_units(self) -> None:
        """400 when neither amount nor units are provided."""
        _with_conn([_KILL_SWITCH_OFF])
        resp = client.post(
            "/portfolio/orders",
            json={"instrument_id": 1, "action": "BUY"},
        )
        assert resp.status_code == 400
        assert "amount or units" in resp.json()["detail"]

    def test_demo_buy_order_returns_synthetic_fill(self) -> None:
        """200 — demo BUY with amount returns a synthetic fill."""
        # Cursor calls:
        #   1. kill switch check
        #   2. quote price lookup
        #   3. INSERT orders RETURNING order_id
        #   4. INSERT fills RETURNING fill_id
        order_row = [{"order_id": 42}]
        fill_row = [{"fill_id": 7}]
        _with_conn([_KILL_SWITCH_OFF, _QUOTE_ROW, order_row, fill_row])

        resp = client.post(
            "/portfolio/orders",
            json={"instrument_id": 1, "action": "BUY", "amount": 300},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["order_id"] == 42
        assert body["status"] == "filled"
        assert body["filled_price"] == 150.0
        # 300 / 150 = 2.0 units
        assert body["filled_units"] == 2.0
        assert body["fees"] == 0.0
        assert body["broker_order_ref"] is not None

    def test_kill_switch_blocks_order(self) -> None:
        """403 when the kill switch is active."""
        _with_conn([_KILL_SWITCH_ON])
        resp = client.post(
            "/portfolio/orders",
            json={"instrument_id": 1, "action": "BUY", "amount": 100},
        )
        assert resp.status_code == 403
        assert "kill switch" in resp.json()["detail"].lower()

    def test_amount_buy_rejects_when_no_quote(self) -> None:
        """Amount-based BUY with no quote fails closed (422)."""
        no_quote: list[dict[str, Any]] = []
        _with_conn([_KILL_SWITCH_OFF, no_quote])
        resp = client.post(
            "/portfolio/orders",
            json={"instrument_id": 999, "action": "BUY", "amount": 500},
        )
        assert resp.status_code == 422
        assert "no quote" in resp.json()["detail"].lower()

    def test_buy_fill_writes_to_broker_positions(self) -> None:
        """Verify that a BUY fill includes an INSERT into broker_positions."""
        order_row = [{"order_id": 99}]
        fill_row = [{"fill_id": 10}]
        conn = _with_conn([_KILL_SWITCH_OFF, _QUOTE_ROW, order_row, fill_row])

        resp = client.post(
            "/portfolio/orders",
            json={
                "instrument_id": 5,
                "action": "BUY",
                "amount": 750,
                "stop_loss_rate": 140.0,
                "take_profit_rate": 200.0,
            },
        )
        assert resp.status_code == 200

        # Collect all SQL executed via conn.execute() (non-cursor calls)
        sql_calls = [str(call.args[0]) for call in conn.execute.call_args_list]
        broker_positions_inserts = [s for s in sql_calls if "broker_positions" in s]
        assert len(broker_positions_inserts) >= 1, "Expected at least one INSERT into broker_positions"
        assert "INSERT INTO broker_positions" in broker_positions_inserts[0]


# ---------------------------------------------------------------------------
# TestClosePosition
# ---------------------------------------------------------------------------


class TestClosePosition:
    """POST /portfolio/positions/{position_id}/close — close a broker position."""

    def setup_method(self) -> None:
        self._patch_config = patch(
            "app.api.orders.get_runtime_config",
            return_value=_DEFAULT_CONFIG,
        )
        self._patch_config.start()

    def teardown_method(self) -> None:
        self._patch_config.stop()
        _cleanup()

    def test_404_for_unknown_position(self) -> None:
        """404 when position_id does not exist in broker_positions."""
        # Cursor calls:
        #   1. kill switch check
        #   2. broker_positions lookup => empty
        _with_conn([_KILL_SWITCH_OFF, _NO_ROWS])
        resp = client.post("/portfolio/positions/9999/close")
        assert resp.status_code == 404
        assert "9999" in resp.json()["detail"]

    def test_demo_close_returns_filled(self) -> None:
        """200 — demo close returns a filled synthetic response with correct units."""
        pos_row = [{"instrument_id": 5, "units": 10.0, "amount": 1500.0, "open_rate": 150.0}]
        order_row = [{"order_id": 77}]
        fill_row = [{"fill_id": 15}]
        # Cursor calls:
        #   1. kill switch check
        #   2. broker_positions lookup
        #   3. INSERT orders RETURNING order_id
        #   4. INSERT fills RETURNING fill_id
        _with_conn([_KILL_SWITCH_OFF, pos_row, order_row, fill_row])

        resp = client.post("/portfolio/positions/500/close")
        assert resp.status_code == 200
        body = resp.json()
        assert body["order_id"] == 77
        assert body["status"] == "filled"
        assert body["filled_units"] == 10.0
        assert body["filled_price"] == 150.0

    def test_partial_close_rejects_excess_units(self) -> None:
        """400 when units_to_deduct exceeds position units."""
        pos_row = [{"instrument_id": 5, "units": 10.0, "amount": 1500.0, "open_rate": 150.0}]
        _with_conn([_KILL_SWITCH_OFF, pos_row])
        resp = client.post(
            "/portfolio/positions/500/close",
            json={"units_to_deduct": 999.0},
        )
        assert resp.status_code == 400
        assert "exceeds" in resp.json()["detail"]

    def test_close_updates_broker_positions_units(self) -> None:
        """Closing a position should UPDATE broker_positions to deduct units."""
        pos_row = [{"instrument_id": 5, "units": 10.0, "amount": 1500.0, "open_rate": 150.0}]
        order_row = [{"order_id": 78}]
        fill_row = [{"fill_id": 16}]
        conn = _with_conn([_KILL_SWITCH_OFF, pos_row, order_row, fill_row])

        resp = client.post("/portfolio/positions/500/close")
        assert resp.status_code == 200

        sql_calls = [str(call.args[0]) for call in conn.execute.call_args_list]
        bp_updates = [s for s in sql_calls if "broker_positions" in s and "UPDATE" in s]
        assert len(bp_updates) >= 1, f"Expected UPDATE broker_positions, got: {sql_calls}"

    def test_kill_switch_blocks_close(self) -> None:
        """403 when the kill switch is active."""
        _with_conn([_KILL_SWITCH_ON])
        resp = client.post("/portfolio/positions/500/close")
        assert resp.status_code == 403
        assert "kill switch" in resp.json()["detail"].lower()
