"""Tests for app.services.portfolio_sync — broker→local reconciliation."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

from app.providers.broker import BrokerPortfolio, BrokerPosition
from app.services.portfolio_sync import (
    PortfolioSyncResult,
    _aggregate_by_instrument,
    sync_portfolio,
)

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
        # avg_cost/cost_basis must NOT appear — local cost basis is authoritative.
        assert "avg_cost" not in params
        assert "cost_basis" not in params


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

    def test_open_date_defaults_to_now_without_raw_field(self) -> None:
        pos = _pos(instrument_id=99)
        conn = _mock_conn(local_positions=[], local_cash=Decimal("0"))
        sync_portfolio(conn, _portfolio([pos]), now=_NOW)

        insert_calls = [
            c
            for c in conn.execute.call_args_list
            if isinstance(c.args[0], str) and "INSERT INTO positions" in c.args[0]
        ]
        assert insert_calls[0].args[1]["date"] == _NOW.date()

    def test_open_date_extracted_from_raw_payload(self) -> None:
        pos = BrokerPosition(
            instrument_id=99,
            units=Decimal("5"),
            open_price=Decimal("100"),
            current_price=Decimal("110"),
            raw_payload={"openDateTime": "2026-03-15T10:30:00Z"},
        )
        conn = _mock_conn(local_positions=[], local_cash=Decimal("0"))
        sync_portfolio(conn, _portfolio([pos]), now=_NOW)

        insert_calls = [
            c
            for c in conn.execute.call_args_list
            if isinstance(c.args[0], str) and "INSERT INTO positions" in c.args[0]
        ]
        from datetime import date

        assert insert_calls[0].args[1]["date"] == date(2026, 3, 15)


class TestExternallyClosedPosition:
    """Local position absent from broker → zero out.

    Tests use a non-empty broker portfolio (containing at least one
    unrelated position) so the whole-portfolio-empty guard does not
    fire.  See ``TestEmptyBrokerGuard`` for that path.
    """

    def test_zeros_out_missing_position(self) -> None:
        # Local has two open positions; broker only reports one of them.
        # This isolates the close path — no insert side-effect from a
        # broker-only instrument.
        conn = _mock_conn(
            local_positions=[(7, Decimal("10")), (8, Decimal("1"))],
            local_cash=Decimal("0"),
        )
        broker_pos = _pos(instrument_id=8, units=Decimal("1"))
        result = sync_portfolio(conn, _portfolio([broker_pos]), now=_NOW)

        assert result.positions_closed_externally == 1
        assert result.positions_opened_externally == 0

    def test_sends_zero_units_in_update(self) -> None:
        conn = _mock_conn(
            local_positions=[(7, Decimal("10")), (8, Decimal("1"))],
            local_cash=Decimal("0"),
        )
        broker_pos = _pos(instrument_id=8, units=Decimal("1"))
        sync_portfolio(conn, _portfolio([broker_pos]), now=_NOW)

        update_calls = [
            c
            for c in conn.execute.call_args_list
            if isinstance(c.args[0], str) and "UPDATE positions SET" in c.args[0] and "current_units  = 0" in c.args[0]
        ]
        assert len(update_calls) == 1
        assert update_calls[0].args[1]["iid"] == 7


class TestEmptyBrokerGuard:
    """Empty broker + non-empty local state → refuse to zero out.

    A legitimate "user liquidated everything in one cycle" scenario is
    indistinguishable from an upstream API failure returning HTTP 200
    with an empty body.  To prevent silent data loss on the positions
    table, the sync raises — the tracked-job wrapper records the failure
    in ``job_runs`` so operators are alerted.
    """

    def test_raises_when_broker_empty_but_local_has_positions(self) -> None:
        import pytest

        conn = _mock_conn(
            local_positions=[(7, Decimal("10"))],
            local_cash=Decimal("0"),
        )
        with pytest.raises(RuntimeError, match="empty positions"):
            sync_portfolio(conn, _portfolio([]), now=_NOW)

    def test_does_not_zero_any_positions_when_raising(self) -> None:
        import pytest

        conn = _mock_conn(
            local_positions=[(7, Decimal("10")), (8, Decimal("5"))],
            local_cash=Decimal("0"),
        )
        with pytest.raises(RuntimeError):
            sync_portfolio(conn, _portfolio([]), now=_NOW)

        # No zero-out UPDATEs must have been issued.
        zero_updates = [
            c for c in conn.execute.call_args_list if isinstance(c.args[0], str) and "current_units  = 0" in c.args[0]
        ]
        assert zero_updates == []

    def test_empty_broker_with_empty_local_does_not_raise(self) -> None:
        """Boundary: fully empty on both sides is a valid no-op."""
        conn = _mock_conn(local_positions=[], local_cash=Decimal("0"))
        result = sync_portfolio(conn, _portfolio([], Decimal("0")), now=_NOW)
        assert result.positions_closed_externally == 0
        assert result.positions_opened_externally == 0
        assert result.positions_updated == 0
        assert result.cash_delta == Decimal("0")


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


class TestAggregateByInstrument:
    """Multiple broker positions for the same instrument are aggregated."""

    def test_single_position_passes_through(self) -> None:
        bp = _pos(instrument_id=42, units=Decimal("10"), open_price=Decimal("100"), current_price=Decimal("110"))
        agg = _aggregate_by_instrument([bp])
        assert 42 in agg
        assert agg[42].units == Decimal("10")
        assert agg[42].avg_open_price == Decimal("100")
        # PnL: (110-100)*10 = 100
        assert agg[42].unrealized_pnl == Decimal("100")

    def test_two_positions_same_instrument_are_summed(self) -> None:
        p1 = _pos(
            instrument_id=42,
            units=Decimal("10"),
            open_price=Decimal("100"),
            current_price=Decimal("120"),
        )
        p2 = _pos(
            instrument_id=42,
            units=Decimal("5"),
            open_price=Decimal("80"),
            current_price=Decimal("120"),
        )
        agg = _aggregate_by_instrument([p1, p2])

        assert len(agg) == 1
        a = agg[42]
        assert a.units == Decimal("15")
        # weighted avg: (100*10 + 80*5) / 15 = 1400/15 ≈ 93.333...
        expected_avg = (Decimal("100") * Decimal("10") + Decimal("80") * Decimal("5")) / Decimal("15")
        assert a.avg_open_price == expected_avg
        # PnL: (120-100)*10 + (120-80)*5 = 200 + 200 = 400
        assert a.unrealized_pnl == Decimal("400")
        assert len(a.raw_payloads) == 2

    def test_different_instruments_stay_separate(self) -> None:
        p1 = _pos(instrument_id=1)
        p2 = _pos(instrument_id=2)
        agg = _aggregate_by_instrument([p1, p2])
        assert len(agg) == 2
        assert 1 in agg
        assert 2 in agg

    def test_earliest_open_date_is_selected(self) -> None:
        p1 = BrokerPosition(
            instrument_id=42,
            units=Decimal("10"),
            open_price=Decimal("100"),
            current_price=Decimal("110"),
            raw_payload={"openDateTime": "2026-03-20T10:00:00Z"},
        )
        p2 = BrokerPosition(
            instrument_id=42,
            units=Decimal("5"),
            open_price=Decimal("80"),
            current_price=Decimal("110"),
            raw_payload={"openDateTime": "2026-03-10T08:00:00Z"},
        )
        agg = _aggregate_by_instrument([p1, p2])

        # Earliest date should win.
        assert agg[42].earliest_open_date_raw == "2026-03-10T08:00:00Z"

    def test_zero_units_returns_zero_avg_price(self) -> None:
        """Guard against division-by-zero from bad broker data."""
        p = _pos(
            instrument_id=42,
            units=Decimal("0"),
            open_price=Decimal("100"),
            current_price=Decimal("110"),
        )
        agg = _aggregate_by_instrument([p])
        assert agg[42].avg_open_price == Decimal(0)


class TestMultiPositionSync:
    """End-to-end: multiple broker positions for one instrument sync correctly."""

    def test_duplicate_instrument_updates_once_with_aggregated_values(self) -> None:
        """Two broker positions for instrument 42 should produce a single UPDATE."""
        p1 = _pos(
            instrument_id=42,
            units=Decimal("10"),
            open_price=Decimal("100"),
            current_price=Decimal("120"),
        )
        p2 = _pos(
            instrument_id=42,
            units=Decimal("5"),
            open_price=Decimal("80"),
            current_price=Decimal("120"),
        )
        conn = _mock_conn(local_positions=[(42, Decimal("10"))], local_cash=Decimal("0"))
        result = sync_portfolio(conn, _portfolio([p1, p2]), now=_NOW)

        assert result.positions_updated == 1
        assert result.positions_opened_externally == 0

        update_calls = [
            c for c in conn.execute.call_args_list if isinstance(c.args[0], str) and "UPDATE positions SET" in c.args[0]
        ]
        assert len(update_calls) == 1
        params = update_calls[0].args[1]
        assert params["units"] == Decimal("15")
        # PnL: (120-100)*10 + (120-80)*5 = 200 + 200 = 400
        assert params["upnl"] == Decimal("400")
        # avg_cost/cost_basis must NOT be overwritten from broker data.
        assert "avg_cost" not in params

    def test_duplicate_instrument_inserts_once_with_aggregated_values(self) -> None:
        """Two broker positions for a new instrument should produce a single INSERT."""
        p1 = _pos(
            instrument_id=99,
            units=Decimal("10"),
            open_price=Decimal("100"),
            current_price=Decimal("120"),
        )
        p2 = _pos(
            instrument_id=99,
            units=Decimal("5"),
            open_price=Decimal("80"),
            current_price=Decimal("120"),
        )
        conn = _mock_conn(local_positions=[], local_cash=Decimal("0"))
        result = sync_portfolio(conn, _portfolio([p1, p2]), now=_NOW)

        assert result.positions_opened_externally == 1

        insert_calls = [
            c
            for c in conn.execute.call_args_list
            if isinstance(c.args[0], str) and "INSERT INTO positions" in c.args[0]
        ]
        assert len(insert_calls) == 1
        params = insert_calls[0].args[1]
        assert params["units"] == Decimal("15")
        expected_avg = (Decimal("100") * Decimal("10") + Decimal("80") * Decimal("5")) / Decimal("15")
        assert params["price"] == expected_avg


class TestReopenedPositionOpenDate:
    """ON CONFLICT should update open_date for reopened positions."""

    def test_upsert_includes_open_date_in_conflict_clause(self) -> None:
        """Verify the INSERT's ON CONFLICT SET includes open_date."""
        pos = _pos(instrument_id=99)
        conn = _mock_conn(local_positions=[], local_cash=Decimal("0"))
        sync_portfolio(conn, _portfolio([pos]), now=_NOW)

        insert_calls = [
            c
            for c in conn.execute.call_args_list
            if isinstance(c.args[0], str) and "INSERT INTO positions" in c.args[0]
        ]
        sql = insert_calls[0].args[0]
        assert "open_date      = EXCLUDED.open_date" in sql
