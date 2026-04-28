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

from collections.abc import Iterator, Sequence
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


def _mock_conn(cursor_results: Sequence[Sequence[Any]]) -> MagicMock:
    """Build a mock psycopg.Connection.

    ``cursor_results`` is a list of result sets, one per ``cur.execute()`` call.
    Each cursor().execute() pops the next result set from the iterator.
    Rows may be either dict-like (for ``row_factory=dict_row``) or tuples
    (for the default tuple-row cursor used by the EXIT FOR UPDATE re-read).
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

    # ``conn.execute(...)`` returns a cursor whose ``rowcount`` callers
    # use to detect silent zero-row UPDATEs (#245). Default to 1 so the
    # happy-path EXIT branch passes the rowcount checks; per-test
    # overrides (``conn.execute.return_value.rowcount = 0``) drive the
    # zero-row regressions.
    exec_cursor = MagicMock()
    exec_cursor.rowcount = 1
    conn.execute.return_value = exec_cursor

    tx = MagicMock()
    tx.__enter__ = MagicMock(return_value=tx)
    tx.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value = tx

    return conn


def _with_conn(cursor_results: Sequence[Sequence[Any]]) -> MagicMock:
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

    def test_rejects_non_positive_amount(self) -> None:
        """400 when amount is zero or negative."""
        _with_conn([_KILL_SWITCH_OFF])
        resp = client.post(
            "/portfolio/orders",
            json={"instrument_id": 1, "action": "BUY", "amount": 0},
        )
        assert resp.status_code == 400
        assert "positive" in resp.json()["detail"]

    def test_rejects_negative_units(self) -> None:
        """400 when units is negative."""
        _with_conn([_KILL_SWITCH_OFF])
        resp = client.post(
            "/portfolio/orders",
            json={"instrument_id": 1, "action": "BUY", "units": -5},
        )
        assert resp.status_code == 400
        assert "positive" in resp.json()["detail"]

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

    def test_units_buy_rejects_when_no_quote(self) -> None:
        """Units-based BUY with no quote also fails closed (422)."""
        no_quote: list[dict[str, Any]] = []
        _with_conn([_KILL_SWITCH_OFF, no_quote])
        resp = client.post(
            "/portfolio/orders",
            json={"instrument_id": 999, "action": "BUY", "units": 10},
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

        # #227: synthetic position_id is the negation of order_id so
        # the synthetic-id namespace is partitioned from real
        # broker-assigned position_ids. order_id was 99 above.
        bp_call = next(
            c
            for c in conn.execute.call_args_list
            if "broker_positions" in str(c.args[0]) and "INSERT" in str(c.args[0])
        )
        assert bp_call.args[1]["pid"] == -99
        assert bp_call.args[1]["pid"] < 0


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
        """200 — demo close fills at current quote, not open_rate."""
        pos_row = [{"instrument_id": 5, "units": 10.0, "amount": 1500.0, "open_rate": 150.0}]
        close_quote = [{"last": 160.0}]
        order_row = [{"order_id": 77}]
        fill_row = [{"fill_id": 15}]
        locked_row = [(10.0,)]
        # Cursor calls:
        #   1. kill switch check
        #   2. broker_positions lookup
        #   3. quote price lookup
        #   4. INSERT orders RETURNING order_id
        #   5. INSERT fills RETURNING fill_id
        #   6. SELECT broker_positions FOR UPDATE (#245 race-safe re-read)
        _with_conn([_KILL_SWITCH_OFF, pos_row, close_quote, order_row, fill_row, locked_row])

        resp = client.post("/portfolio/positions/500/close")
        assert resp.status_code == 200
        body = resp.json()
        assert body["order_id"] == 77
        assert body["status"] == "filled"
        assert body["filled_units"] == 10.0
        assert body["filled_price"] == 160.0  # current quote, not open_rate

    def test_demo_close_falls_back_to_open_rate_when_no_quote(self) -> None:
        """200 — when no quote available, falls back to open_rate."""
        pos_row = [{"instrument_id": 5, "units": 10.0, "amount": 1500.0, "open_rate": 150.0}]
        order_row = [{"order_id": 80}]
        fill_row = [{"fill_id": 18}]
        locked_row = [(10.0,)]
        # Cursor calls: kill switch, broker_positions, quote (empty), order, fill, FOR UPDATE
        _with_conn([_KILL_SWITCH_OFF, pos_row, _NO_ROWS, order_row, fill_row, locked_row])

        resp = client.post("/portfolio/positions/500/close")
        assert resp.status_code == 200
        body = resp.json()
        assert body["filled_price"] == 150.0  # fallback to open_rate

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

    def test_partial_close_rejects_zero_units(self) -> None:
        """400 when units_to_deduct is zero."""
        pos_row = [{"instrument_id": 5, "units": 10.0, "amount": 1500.0, "open_rate": 150.0}]
        _with_conn([_KILL_SWITCH_OFF, pos_row])
        resp = client.post(
            "/portfolio/positions/500/close",
            json={"units_to_deduct": 0},
        )
        assert resp.status_code == 400
        assert "positive" in resp.json()["detail"]

    def test_partial_close_rejects_negative_units(self) -> None:
        """400 when units_to_deduct is negative."""
        pos_row = [{"instrument_id": 5, "units": 10.0, "amount": 1500.0, "open_rate": 150.0}]
        _with_conn([_KILL_SWITCH_OFF, pos_row])
        resp = client.post(
            "/portfolio/positions/500/close",
            json={"units_to_deduct": -3.0},
        )
        assert resp.status_code == 400
        assert "positive" in resp.json()["detail"]

    def test_close_updates_broker_positions_units(self) -> None:
        """Closing a position should UPDATE broker_positions to deduct units."""
        pos_row = [{"instrument_id": 5, "units": 10.0, "amount": 1500.0, "open_rate": 150.0}]
        order_row = [{"order_id": 78}]
        fill_row = [{"fill_id": 16}]
        locked_row = [(10.0,)]
        conn = _with_conn([_KILL_SWITCH_OFF, pos_row, _QUOTE_ROW, order_row, fill_row, locked_row])

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

    def test_close_409_when_locked_units_below_request(self) -> None:
        """#245 regression — stale outer read, fewer locked units inside tx.

        Outer SELECT shows ``units = 10`` so the endpoint accepts the
        full close. Inside the transaction, the FOR UPDATE re-read
        returns ``units = 2`` (another close consumed 8). The endpoint
        must raise 409, the transaction must exit via the exception
        path (so the orders/fills cursor inserts roll back), and no
        cash_ledger or decision_audit ``conn.execute`` write may run.
        """
        pos_row = [{"instrument_id": 5, "units": 10.0, "amount": 1500.0, "open_rate": 150.0}]
        order_row = [{"order_id": 90}]
        fill_row = [{"fill_id": 25}]
        # FOR UPDATE re-read: locked units shrunk below requested deduction.
        locked_row = [(2.0,)]
        conn = _with_conn([_KILL_SWITCH_OFF, pos_row, _QUOTE_ROW, order_row, fill_row, locked_row])

        resp = client.post("/portfolio/positions/500/close")
        assert resp.status_code == 409
        assert "another close" in resp.json()["detail"]

        # The orders/fills INSERTs use ``conn.cursor()`` (not
        # ``conn.execute``); the meaningful rollback evidence is the
        # transaction context manager exiting with an exception. Assert
        # ``conn.transaction().__exit__`` was called with a non-None
        # ``exc_type`` — that is what triggers psycopg's tx ROLLBACK.
        tx_exits = conn.transaction.return_value.__exit__.call_args_list
        assert any(call.args[0] is not None for call in tx_exits), (
            f"Expected tx __exit__ to receive an exception (rollback path); actual call_args={tx_exits!r}"
        )

        sql_calls = [str(call.args[0]) for call in conn.execute.call_args_list]
        assert not any("cash_ledger" in s for s in sql_calls), "Race-loser must not record a cash_ledger entry."
        assert not any("decision_audit" in s for s in sql_calls), "Race-loser must not record a decision_audit entry."

    def test_close_409_when_locked_row_disappeared(self) -> None:
        """#245 regression — broker_position deleted between outer read and tx.

        The endpoint's outer SELECT found the row, but by the time the
        FOR UPDATE re-read fires the row is gone (e.g. another close
        zeroed it and a downstream prune removed zero-unit rows). The
        endpoint must raise 409 rather than silently committing the
        order/fill side effects.
        """
        pos_row = [{"instrument_id": 5, "units": 10.0, "amount": 1500.0, "open_rate": 150.0}]
        order_row = [{"order_id": 91}]
        fill_row = [{"fill_id": 26}]
        locked_missing: list[Any] = []
        conn = _with_conn([_KILL_SWITCH_OFF, pos_row, _QUOTE_ROW, order_row, fill_row, locked_missing])

        resp = client.post("/portfolio/positions/500/close")
        assert resp.status_code == 409
        assert "no longer exists" in resp.json()["detail"]

        sql_calls = [str(call.args[0]) for call in conn.execute.call_args_list]
        assert not any("cash_ledger" in s for s in sql_calls)

    def test_close_409_when_broker_positions_rowcount_zero(self) -> None:
        """#245 belt-and-braces — locked re-read shows valid units but the
        ``broker_positions`` UPDATE still matches zero rows (e.g. an
        ``ON DELETE CASCADE`` removed the row between the SELECT FOR
        UPDATE and the UPDATE). Without the rowcount check the endpoint
        would commit cash + audit side effects against a deleted broker
        position.
        """
        pos_row = [{"instrument_id": 5, "units": 10.0, "amount": 1500.0, "open_rate": 150.0}]
        order_row = [{"order_id": 93}]
        fill_row = [{"fill_id": 28}]
        locked_row = [(10.0,)]
        conn = _with_conn([_KILL_SWITCH_OFF, pos_row, _QUOTE_ROW, order_row, fill_row, locked_row])

        # Per-call side_effect: positions UPDATE returns rowcount=1, the
        # subsequent broker_positions UPDATE returns rowcount=0; later
        # writes (cash_ledger, decision_audit) never run because the 409
        # raises first.
        rowcounts = iter([1, 0])

        def _exec_side_effect(*_args: Any, **_kwargs: Any) -> MagicMock:
            cur = MagicMock()
            cur.rowcount = next(rowcounts, 1)
            return cur

        conn.execute.side_effect = _exec_side_effect

        resp = client.post("/portfolio/positions/500/close")
        assert resp.status_code == 409
        assert "deduction matched zero" in resp.json()["detail"]

    def test_close_409_when_positions_aggregate_rowcount_zero(self) -> None:
        """#245 regression — broker_positions locked + valid, but aggregate
        ``positions`` row missing or already drained. The aggregate UPDATE
        matches zero rows; without the rowcount check the endpoint would
        have committed orders/fills/cash without moving realized_pnl.
        """
        pos_row = [{"instrument_id": 5, "units": 10.0, "amount": 1500.0, "open_rate": 150.0}]
        order_row = [{"order_id": 92}]
        fill_row = [{"fill_id": 27}]
        locked_row = [(10.0,)]
        conn = _with_conn([_KILL_SWITCH_OFF, pos_row, _QUOTE_ROW, order_row, fill_row, locked_row])
        # Force the aggregate-positions UPDATE to match zero rows.
        conn.execute.return_value.rowcount = 0

        resp = client.post("/portfolio/positions/500/close")
        assert resp.status_code == 409
        assert "positions row" in resp.json()["detail"]
