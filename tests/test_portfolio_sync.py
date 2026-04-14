"""Tests for app.services.portfolio_sync — broker→local reconciliation."""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.providers.broker import BrokerPortfolio, BrokerPosition
from app.services.portfolio_sync import (
    PortfolioSyncResult,
    _aggregate_by_instrument,
    _upsert_broker_positions,
    sync_portfolio,
)
from tests.fixtures.copy_mirrors import _NOW


def _is_zero_out_update(sql_arg: Any) -> bool:
    """True if ``sql_arg`` is the zero-out UPDATE SQL.

    Whitespace-tolerant: the production SQL aligns column names with
    extra spaces (``current_units  = 0``), but the test must not break
    if that alignment ever changes.  We normalise runs of whitespace
    to a single space before substring-matching.
    """
    if not isinstance(sql_arg, str):
        return False
    normalised = re.sub(r"\s+", " ", sql_arg)
    return "UPDATE positions SET" in normalised and "current_units = 0" in normalised


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
    - 'FROM copy_mirrors' → {"n": 0} (no mirror state in these tests)
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
        elif "FROM copy_mirrors" in stripped:
            # Pre-write mirror guard queries copy_mirrors for an
            # active count. These tests have no mirror state, so
            # 0 lets the guard fall through cleanly.
            result.fetchone.return_value = {"n": 0}
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

        update_calls = [c for c in conn.execute.call_args_list if _is_zero_out_update(c.args[0])]
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
        conn = _mock_conn(
            local_positions=[(7, Decimal("10"))],
            local_cash=Decimal("0"),
        )
        # Match on the distinctive "refusing to zero" phrase from the
        # guard's error message rather than generic substrings, so an
        # accidental RuntimeError raised elsewhere in the call stack
        # would not satisfy this assertion.
        with pytest.raises(RuntimeError, match="refusing to zero"):
            sync_portfolio(conn, _portfolio([]), now=_NOW)

    def test_guard_raises_before_any_write(self) -> None:
        """Strongest form of the guard test: raise happens before any write.

        The production code's ``conn.execute(...)`` path is used only
        for writes (UPDATE/INSERT); reads go through
        ``conn.cursor(...)``.  So if the guard fires *before* the
        zeroing loop, ``conn.execute.call_args_list`` must be empty at
        the point of raising.  This catches a broken guard that moves
        to *after* the zeroing loop (or partway through it) because
        any UPDATE issued before the raise would leave a recorded call.
        """
        conn = _mock_conn(
            local_positions=[(7, Decimal("10")), (8, Decimal("5"))],
            local_cash=Decimal("0"),
        )
        with pytest.raises(RuntimeError):
            sync_portfolio(conn, _portfolio([]), now=_NOW)

        # Zero writes must have occurred. Stronger than "no zero-out
        # updates" — catches any write attempted before the raise.
        assert conn.execute.call_args_list == []

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


class TestPositionSource:
    """Verify INSERT path writes source='broker_sync' and handles close/reopen.

    Issue #180 — positions rows carry a ``source`` column identifying who
    currently manages the open units.  The sync path (broker-discovered)
    must always insert ``'broker_sync'`` and, on reopen (the ON CONFLICT
    path where the existing row has zero units), reset source so the
    new opener is reflected.
    """

    def test_insert_emits_source_literal_and_reopen_reset_clause(self) -> None:
        """INSERT carries the 'broker_sync' literal AND the reset CASE WHEN.

        With a mocked connection, ``sync_portfolio`` captures a single
        SQL string per call regardless of whether Postgres would take
        the INSERT or the ON CONFLICT branch at runtime — the branch
        decision is made by the planner, not by us.  So the unit-level
        guarantee we can assert here is SQL *shape*: a single captured
        string must contain both the hard-coded VALUES literal and the
        reset CASE WHEN, evaluated together from one call.

        End-to-end verification that Postgres actually routes closed
        rows through the reset arm is tracked in the DB integration
        test backlog (#186) — unreachable from a mocked connection.
        """
        pos = _pos(instrument_id=99)
        conn = _mock_conn(local_positions=[], local_cash=Decimal("0"))
        sync_portfolio(conn, _portfolio([pos]), now=_NOW)

        insert_calls = [
            c
            for c in conn.execute.call_args_list
            if isinstance(c.args[0], str) and "INSERT INTO positions" in c.args[0]
        ]
        assert len(insert_calls) == 1
        sql = insert_calls[0].args[0]
        normalised = re.sub(r"\s+", " ", sql)

        # Hard-coded VALUES literal — no parameter placeholder.
        assert "'broker_sync'" in normalised
        # Reset CASE WHEN: when the pre-update row is fully closed,
        # overwrite source with EXCLUDED; otherwise preserve the
        # existing source.  Postgres evaluates CASE against the
        # pre-update row, so SET-list ordering is irrelevant.
        assert "positions.current_units <= 0" in normalised
        assert "EXCLUDED.source" in normalised
        assert "ELSE positions.source" in normalised

    def test_update_path_does_not_overwrite_source(self) -> None:
        """Existing open position — UPDATE must NOT touch source.

        When the broker reports units for a position we already have
        open locally, we update units and unrealized_pnl only.  The
        source column must not appear in the UPDATE SQL at all — adding
        it would silently flip ebull-owned positions to broker_sync on
        every sync cycle.
        """
        pos = _pos(instrument_id=42, units=Decimal("5"), open_price=Decimal("100"), current_price=Decimal("120"))
        conn = _mock_conn(local_positions=[(42, Decimal("5"))], local_cash=Decimal("5000"))
        sync_portfolio(conn, _portfolio([pos], Decimal("5000")), now=_NOW)

        update_calls = [
            c for c in conn.execute.call_args_list if isinstance(c.args[0], str) and "UPDATE positions SET" in c.args[0]
        ]
        assert len(update_calls) == 1
        sql = update_calls[0].args[0]
        # The update path updates only units/pnl/updated_at.  "source ="
        # must not appear, else we would clobber ebull ownership.
        assert "source" not in sql.lower()


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


def test_portfolio_sync_result_has_mirror_counters() -> None:
    """Spec §2.3 result extension — mirrors_upserted, mirrors_closed,
    mirror_positions_upserted are part of the return contract."""
    result = PortfolioSyncResult(
        positions_updated=0,
        positions_opened_externally=0,
        positions_closed_externally=0,
        cash_delta=Decimal("0"),
        broker_cash=Decimal("0"),
        local_cash=Decimal("0"),
        mirrors_upserted=2,
        mirrors_closed=1,
        mirror_positions_upserted=6,
    )
    assert result.mirrors_upserted == 2
    assert result.mirrors_closed == 1
    assert result.mirror_positions_upserted == 6


def test_portfolio_sync_result_has_broker_position_counters() -> None:
    """Migration 024 extension — broker_positions_upserted and
    broker_positions_deleted are part of the return contract."""
    result = PortfolioSyncResult(
        positions_updated=0,
        positions_opened_externally=0,
        positions_closed_externally=0,
        cash_delta=Decimal("0"),
        broker_cash=Decimal("0"),
        local_cash=Decimal("0"),
        broker_positions_upserted=5,
        broker_positions_deleted=2,
    )
    assert result.broker_positions_upserted == 5
    assert result.broker_positions_deleted == 2


# ---------------------------------------------------------------------------
# broker_positions (migration 024) — per-position tracking
# ---------------------------------------------------------------------------


def _detailed_pos(
    position_id: int = 5001,
    instrument_id: int = 42,
    units: Decimal = Decimal("10"),
    open_price: Decimal = Decimal("100"),
    current_price: Decimal = Decimal("110"),
    stop_loss_rate: Decimal | None = None,
    take_profit_rate: Decimal | None = None,
) -> BrokerPosition:
    """BrokerPosition with per-position fields populated (for broker_positions tests)."""
    return BrokerPosition(
        instrument_id=instrument_id,
        units=units,
        open_price=open_price,
        current_price=current_price,
        raw_payload={"positionID": position_id, "instrumentID": instrument_id},
        position_id=position_id,
        is_buy=True,
        amount=open_price * units,
        initial_amount_in_dollars=open_price * units,
        stop_loss_rate=stop_loss_rate,
        take_profit_rate=take_profit_rate,
    )


def _is_broker_positions_upsert(sql_arg: Any) -> bool:
    if not isinstance(sql_arg, str):
        return False
    normalised = re.sub(r"\s+", " ", sql_arg)
    return "INSERT INTO broker_positions" in normalised


def _is_broker_positions_delete(sql_arg: Any) -> bool:
    if not isinstance(sql_arg, str):
        return False
    normalised = re.sub(r"\s+", " ", sql_arg)
    return "DELETE FROM broker_positions" in normalised


class TestUpsertBrokerPositions:
    """_upsert_broker_positions writes individual eToro positions to
    the broker_positions table (migration 024)."""

    def test_upserts_position_with_id(self) -> None:
        conn = MagicMock()
        # DELETE returns no rows (no disappeared positions)
        conn.execute.return_value = iter([])

        bp = _detailed_pos(position_id=5001, instrument_id=42)
        upserted, deleted = _upsert_broker_positions(conn, [bp], _NOW)

        assert upserted == 1
        assert deleted == 0
        upsert_calls = [c for c in conn.execute.call_args_list if _is_broker_positions_upsert(c.args[0])]
        assert len(upsert_calls) == 1
        params = upsert_calls[0].args[1]
        assert params["position_id"] == 5001
        assert params["instrument_id"] == 42

    def test_skips_position_without_id(self) -> None:
        """Legacy BrokerPosition fixtures (position_id=None) are skipped."""
        conn = MagicMock()
        bp = _pos(instrument_id=42)  # uses the old helper — no position_id
        upserted, deleted = _upsert_broker_positions(conn, [bp], _NOW)

        assert upserted == 0
        assert deleted == 0
        # No SQL should have been issued at all
        assert conn.execute.call_args_list == []

    def test_multiple_positions_upserted_individually(self) -> None:
        conn = MagicMock()
        conn.execute.return_value = iter([])

        positions = [
            _detailed_pos(position_id=5001, instrument_id=42),
            _detailed_pos(position_id=5002, instrument_id=42),
            _detailed_pos(position_id=5003, instrument_id=99),
        ]
        upserted, deleted = _upsert_broker_positions(conn, positions, _NOW)

        assert upserted == 3
        upsert_calls = [c for c in conn.execute.call_args_list if _is_broker_positions_upsert(c.args[0])]
        assert len(upsert_calls) == 3

    def test_sl_tp_passed_to_upsert(self) -> None:
        conn = MagicMock()
        conn.execute.return_value = iter([])

        bp = _detailed_pos(
            position_id=5001,
            stop_loss_rate=Decimal("90.00"),
            take_profit_rate=Decimal("150.00"),
        )
        _upsert_broker_positions(conn, [bp], _NOW)

        upsert_calls = [c for c in conn.execute.call_args_list if _is_broker_positions_upsert(c.args[0])]
        params = upsert_calls[0].args[1]
        assert params["stop_loss_rate"] == Decimal("90.00")
        assert params["take_profit_rate"] == Decimal("150.00")

    def test_delete_returns_disappeared_count(self) -> None:
        """Positions not in broker payload are deleted from broker_positions."""
        conn = MagicMock()

        # The DELETE path uses conn.cursor(row_factory=...) as a context
        # manager, then iterates over the cursor for RETURNING rows.
        mock_delete_cursor = MagicMock()
        mock_delete_cursor.__enter__ = MagicMock(return_value=mock_delete_cursor)
        mock_delete_cursor.__exit__ = MagicMock(return_value=False)
        mock_delete_cursor.__iter__ = MagicMock(return_value=iter([{"position_id": 9999, "instrument_id": 42}]))
        conn.cursor.return_value = mock_delete_cursor

        bp = _detailed_pos(position_id=5001, instrument_id=42)
        upserted, deleted = _upsert_broker_positions(conn, [bp], _NOW)

        assert upserted == 1
        assert deleted == 1

    def test_source_preserved_for_ebull_positions(self) -> None:
        """ON CONFLICT preserves source='ebull' — SQL shape check."""
        conn = MagicMock()
        conn.execute.return_value = iter([])

        bp = _detailed_pos(position_id=5001)
        _upsert_broker_positions(conn, [bp], _NOW)

        upsert_calls = [c for c in conn.execute.call_args_list if _is_broker_positions_upsert(c.args[0])]
        sql = upsert_calls[0].args[0]
        normalised = re.sub(r"\s+", " ", sql)
        # Must preserve 'ebull' source on conflict
        assert "broker_positions.source = 'ebull'" in normalised
        assert "broker_positions.source" in normalised


class TestBrokerPositionsInSyncPortfolio:
    """Integration: sync_portfolio calls _upsert_broker_positions alongside
    the existing positions reconciliation."""

    def test_sync_reports_broker_positions_upserted(self) -> None:
        bp = _detailed_pos(position_id=5001, instrument_id=42)
        conn = _mock_conn(local_positions=[(42, Decimal("10"))], local_cash=Decimal("0"))
        result = sync_portfolio(conn, _portfolio([bp]), now=_NOW)

        assert result.broker_positions_upserted == 1
        # Existing positions-table update still works
        assert result.positions_updated == 1

    def test_legacy_positions_without_id_still_sync(self) -> None:
        """Backwards-compat: BrokerPosition without position_id still
        updates the positions table (broker_positions upsert is skipped)."""
        bp = _pos(instrument_id=42)
        conn = _mock_conn(local_positions=[(42, Decimal("10"))], local_cash=Decimal("0"))
        result = sync_portfolio(conn, _portfolio([bp]), now=_NOW)

        assert result.positions_updated == 1
        assert result.broker_positions_upserted == 0
        assert result.broker_positions_deleted == 0
