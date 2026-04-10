"""Tests for app.services.portfolio_sync — broker→local reconciliation."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

from app.providers.broker import BrokerPortfolio, BrokerPosition
from app.services.portfolio_sync import PortfolioSyncResult, sync_portfolio

_NOW = datetime(2026, 4, 10, 5, 30, tzinfo=UTC)


def _pos(
    instrument_id: int = 1,
    units: Decimal = Decimal("10"),
    open_price: Decimal = Decimal("100"),
    current_price: Decimal = Decimal("110"),
) -> BrokerPosition:
    return BrokerPosition(
        instrument_id=instrument_id,
        units=units,
        open_price=open_price,
        current_price=current_price,
        raw_payload={},
    )


def _portfolio(
    positions: list[BrokerPosition] | None = None,
    available_cash: Decimal = Decimal("5000"),
) -> BrokerPortfolio:
    return BrokerPortfolio(
        positions=positions or [],
        available_cash=available_cash,
        raw_payload={},
    )


def _mock_conn(
    local_positions: list[tuple[int, Decimal]] | None = None,
    local_cash: Decimal = Decimal("0"),
) -> MagicMock:
    """Build a mock connection with canned SELECT/cursor results.

    The service uses two patterns:
    - ``conn.cursor(row_factory=...)`` for SELECTs (returns dict rows)
    - ``conn.execute(...)`` for writes (UPDATE/INSERT)

    Cursor SQL dispatch matches on substrings.  Priority:
    - 'FROM positions' → local position rows as dicts
    - 'FROM cash_ledger' → local cash sum as dict
    - Everything else → default MagicMock

    Note: substring matching cannot detect structural SQL errors like
    wrong column order or missing WHERE clauses.
    """
    conn = MagicMock()
    raw_positions = local_positions or []

    # Dict-style rows matching dict_row factory output.
    position_rows = [{"instrument_id": iid, "current_units": units} for iid, units in raw_positions]

    def _cursor_execute(sql: str, params: dict[str, Any] | None = None) -> MagicMock:
        result = MagicMock()
        stripped = sql.strip()
        if "FROM positions" in stripped:
            result.fetchall.return_value = position_rows
        elif "FROM cash_ledger" in stripped:
            result.fetchone.return_value = {"total": local_cash}
        return result

    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = _cursor_execute
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = mock_cursor

    return conn


class TestExistingPositionUpdate:
    """Broker position already exists locally → UPDATE."""

    def test_updates_units_and_pnl(self) -> None:
        pos = _pos(instrument_id=42, units=Decimal("5"), open_price=Decimal("100"), current_price=Decimal("120"))
        conn = _mock_conn(
            local_positions=[(42, Decimal("5"))],
            local_cash=Decimal("5000"),
        )
        result = sync_portfolio(conn, _portfolio([pos], Decimal("5000")), now=_NOW)

        assert result.positions_updated == 1
        assert result.positions_opened_externally == 0
        assert result.positions_closed_externally == 0

    def test_passes_correct_params_to_update(self) -> None:
        pos = _pos(instrument_id=42, units=Decimal("5"), open_price=Decimal("100"), current_price=Decimal("120"))
        conn = _mock_conn(local_positions=[(42, Decimal("5"))], local_cash=Decimal("5000"))
        sync_portfolio(conn, _portfolio([pos], Decimal("5000")), now=_NOW)

        # Find the UPDATE call
        update_calls = [
            c for c in conn.execute.call_args_list if isinstance(c.args[0], str) and "UPDATE positions SET" in c.args[0]
        ]
        assert len(update_calls) == 1
        params = update_calls[0].args[1]
        assert params["iid"] == 42
        assert params["units"] == Decimal("5")
        # unrealized = (120 - 100) * 5 = 100
        assert params["upnl"] == Decimal("100")
        assert params["now"] == _NOW


class TestExternallyOpenedPosition:
    """Broker has a position not in local DB → INSERT."""

    def test_counts_as_opened_externally(self) -> None:
        pos = _pos(instrument_id=99)
        conn = _mock_conn(local_positions=[], local_cash=Decimal("0"))
        result = sync_portfolio(conn, _portfolio([pos]), now=_NOW)

        assert result.positions_opened_externally == 1
        assert result.positions_updated == 0

    def test_inserts_with_correct_cost_basis(self) -> None:
        pos = _pos(instrument_id=99, units=Decimal("3"), open_price=Decimal("50"), current_price=Decimal("55"))
        conn = _mock_conn(local_positions=[], local_cash=Decimal("0"))
        sync_portfolio(conn, _portfolio([pos]), now=_NOW)

        insert_calls = [
            c
            for c in conn.execute.call_args_list
            if isinstance(c.args[0], str) and "INSERT INTO positions" in c.args[0]
        ]
        assert len(insert_calls) == 1
        params = insert_calls[0].args[1]
        assert params["iid"] == 99
        assert params["units"] == Decimal("3")
        assert params["price"] == Decimal("50")
        # cost_basis = open_price * units = 150
        assert params["cost"] == Decimal("150")
        # unrealized = (55 - 50) * 3 = 15
        assert params["upnl"] == Decimal("15")


class TestExternallyClosedPosition:
    """Local position absent from broker → zero out."""

    def test_zeros_out_missing_position(self) -> None:
        conn = _mock_conn(
            local_positions=[(7, Decimal("10"))],
            local_cash=Decimal("0"),
        )
        # Empty broker portfolio — position 7 was closed externally.
        result = sync_portfolio(conn, _portfolio([]), now=_NOW)

        assert result.positions_closed_externally == 1
        assert result.positions_updated == 0

    def test_sends_zero_units_in_update(self) -> None:
        conn = _mock_conn(
            local_positions=[(7, Decimal("10"))],
            local_cash=Decimal("0"),
        )
        sync_portfolio(conn, _portfolio([]), now=_NOW)

        update_calls = [
            c
            for c in conn.execute.call_args_list
            if isinstance(c.args[0], str) and "UPDATE positions SET" in c.args[0] and "current_units  = 0" in c.args[0]
        ]
        assert len(update_calls) == 1
        assert update_calls[0].args[1]["iid"] == 7


class TestCashReconciliation:
    """Cash sync records a broker_sync event for non-trivial deltas."""

    def test_records_positive_delta(self) -> None:
        conn = _mock_conn(local_positions=[], local_cash=Decimal("1000"))
        result = sync_portfolio(conn, _portfolio([], Decimal("1500")), now=_NOW)

        assert result.cash_delta == Decimal("500")
        assert result.broker_cash == Decimal("1500")
        assert result.local_cash == Decimal("1000")

        insert_calls = [
            c
            for c in conn.execute.call_args_list
            if isinstance(c.args[0], str) and "INSERT INTO cash_ledger" in c.args[0]
        ]
        assert len(insert_calls) == 1
        params = insert_calls[0].args[1]
        assert params["amount"] == Decimal("500")
        assert params["time"] == _NOW

    def test_records_negative_delta(self) -> None:
        conn = _mock_conn(local_positions=[], local_cash=Decimal("2000"))
        result = sync_portfolio(conn, _portfolio([], Decimal("1800")), now=_NOW)

        assert result.cash_delta == Decimal("-200")

    def test_skips_insert_within_tolerance(self) -> None:
        conn = _mock_conn(local_positions=[], local_cash=Decimal("1000"))
        result = sync_portfolio(conn, _portfolio([], Decimal("1000.005")), now=_NOW)

        assert result.cash_delta == Decimal("0.005")
        # No cash_ledger INSERT because delta < 0.01 tolerance.
        insert_calls = [
            c
            for c in conn.execute.call_args_list
            if isinstance(c.args[0], str) and "INSERT INTO cash_ledger" in c.args[0]
        ]
        assert len(insert_calls) == 0

    def test_exact_tolerance_boundary_skips(self) -> None:
        """Delta exactly at tolerance (0.01) is NOT greater than tolerance → skip."""
        conn = _mock_conn(local_positions=[], local_cash=Decimal("1000"))
        sync_portfolio(conn, _portfolio([], Decimal("1000.01")), now=_NOW)

        insert_calls = [
            c
            for c in conn.execute.call_args_list
            if isinstance(c.args[0], str) and "INSERT INTO cash_ledger" in c.args[0]
        ]
        assert len(insert_calls) == 0

    def test_just_above_tolerance_records(self) -> None:
        """Delta just above tolerance triggers the insert."""
        conn = _mock_conn(local_positions=[], local_cash=Decimal("1000"))
        sync_portfolio(conn, _portfolio([], Decimal("1000.011")), now=_NOW)

        insert_calls = [
            c
            for c in conn.execute.call_args_list
            if isinstance(c.args[0], str) and "INSERT INTO cash_ledger" in c.args[0]
        ]
        assert len(insert_calls) == 1


class TestEmptyPortfolio:
    """Broker returns zero positions and zero cash."""

    def test_empty_broker_no_local_state(self) -> None:
        conn = _mock_conn(local_positions=[], local_cash=Decimal("0"))
        result = sync_portfolio(conn, _portfolio([], Decimal("0")), now=_NOW)

        assert result == PortfolioSyncResult(
            positions_updated=0,
            positions_opened_externally=0,
            positions_closed_externally=0,
            cash_delta=Decimal("0"),
            broker_cash=Decimal("0"),
            local_cash=Decimal("0"),
        )


class TestMixedScenario:
    """Multiple positions: one updated, one new, one closed."""

    def test_all_three_reconciliation_paths(self) -> None:
        broker_positions = [
            _pos(instrument_id=1, units=Decimal("10")),  # exists locally → update
            _pos(instrument_id=2, units=Decimal("5")),  # new → opened externally
        ]
        local = [(1, Decimal("10")), (3, Decimal("8"))]  # 3 missing from broker → closed

        conn = _mock_conn(local_positions=local, local_cash=Decimal("0"))
        result = sync_portfolio(conn, _portfolio(broker_positions), now=_NOW)

        assert result.positions_updated == 1
        assert result.positions_opened_externally == 1
        assert result.positions_closed_externally == 1
