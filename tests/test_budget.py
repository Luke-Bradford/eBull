"""
Tests for app.services.budget.

Covers:
  - get_budget_config: happy path, missing row -> BudgetConfigCorrupt
  - update_budget_config: updates config and writes audit, raises on missing
    singleton, raises when no fields provided, raises when no fields changed
  - record_capital_event: inserts event and returns it, rejects non-positive amount
  - list_capital_events: returns events ordered by time, returns empty list

Mock DB approach mirrors test_runtime_config.py.
"""

from __future__ import annotations

import unittest.mock
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.services.budget import (
    BudgetConfig,
    BudgetConfigCorrupt,
    BudgetState,
    CapitalEvent,
    _current_uk_tax_year,
    compute_budget_state,
    get_budget_config,
    list_capital_events,
    record_capital_event,
    update_budget_config,
)

_NOW = datetime(2026, 4, 15, 12, 0, 0, tzinfo=UTC)


def _make_cursor(
    rows: list[dict[str, Any]] | None = None,
    single: dict[str, Any] | None = None,
) -> MagicMock:
    cur = MagicMock()
    if single is not None:
        cur.execute.return_value.fetchone.return_value = single
        cur.fetchone.return_value = single
    if rows is not None:
        cur.execute.return_value.fetchall.return_value = rows
        cur.fetchall.return_value = rows
        cur.fetchone.return_value = rows[0] if rows else None
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    return cur


def _make_conn(cursors: list[MagicMock]) -> MagicMock:
    conn = MagicMock()
    cursor_iter = iter(cursors)
    conn.cursor.side_effect = lambda **kwargs: next(cursor_iter)
    conn.execute.return_value = MagicMock(rowcount=1)
    tx = MagicMock()
    tx.__enter__ = MagicMock(return_value=tx)
    tx.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value = tx
    return conn


def _config_row(
    buffer: str = "0.0500",
    scenario: str = "higher",
) -> dict[str, Any]:
    return {
        "cash_buffer_pct": Decimal(buffer),
        "cgt_scenario": scenario,
        "updated_at": _NOW,
        "updated_by": "seed",
        "reason": "initial seed",
    }


def _event_row(
    event_id: int = 1,
    event_type: str = "injection",
    amount: str = "5000.00",
    currency: str = "USD",
    source: str = "operator",
    note: str | None = "first deposit",
    created_by: str | None = "operator",
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "event_time": _NOW,
        "event_type": event_type,
        "amount": Decimal(amount),
        "currency": currency,
        "source": source,
        "note": note,
        "created_by": created_by,
    }


# ---------------------------------------------------------------------------
# TestGetBudgetConfig
# ---------------------------------------------------------------------------


class TestGetBudgetConfig:
    def test_returns_config_from_db(self) -> None:
        conn = _make_conn([_make_cursor(rows=[_config_row()])])
        cfg = get_budget_config(conn)
        assert isinstance(cfg, BudgetConfig)
        assert cfg.cash_buffer_pct == Decimal("0.0500")
        assert cfg.cgt_scenario == "higher"
        assert cfg.updated_by == "seed"
        assert cfg.reason == "initial seed"
        assert cfg.updated_at == _NOW

    def test_missing_row_raises_budget_config_corrupt(self) -> None:
        conn = _make_conn([_make_cursor(rows=[])])
        with pytest.raises(BudgetConfigCorrupt, match="missing"):
            get_budget_config(conn)

    def test_decimal_conversion_avoids_float_precision_loss(self) -> None:
        row = _config_row(buffer="0.1234")
        conn = _make_conn([_make_cursor(rows=[row])])
        cfg = get_budget_config(conn)
        assert cfg.cash_buffer_pct == Decimal("0.1234")
        assert isinstance(cfg.cash_buffer_pct, Decimal)


# ---------------------------------------------------------------------------
# TestUpdateBudgetConfig
# ---------------------------------------------------------------------------


class TestUpdateBudgetConfig:
    def test_raises_when_no_fields_provided(self) -> None:
        conn = _make_conn([])
        with pytest.raises(ValueError, match="at least one"):
            update_budget_config(conn, updated_by="op", reason="r")

    def test_raises_when_singleton_missing(self) -> None:
        conn = _make_conn([_make_cursor(rows=[])])
        with pytest.raises(BudgetConfigCorrupt, match="cannot update"):
            update_budget_config(
                conn,
                cash_buffer_pct=Decimal("0.10"),
                updated_by="op",
                reason="raise buffer",
            )

    def test_raises_when_no_fields_changed(self) -> None:
        conn = _make_conn([_make_cursor(rows=[_config_row()])])
        with pytest.raises(ValueError, match="no fields changed"):
            update_budget_config(
                conn,
                cash_buffer_pct=Decimal("0.0500"),
                updated_by="op",
                reason="noop",
            )

    def test_updates_cash_buffer_and_writes_audit(self) -> None:
        # Cursor 1: SELECT FOR UPDATE, Cursor 2: UPDATE RETURNING
        returning_row = {
            **_config_row(buffer="0.1000"),
            "updated_by": "op",
            "reason": "raise buffer",
        }
        cur_select = _make_cursor(rows=[_config_row(buffer="0.0500")])
        cur_update = _make_cursor(rows=[returning_row])
        # Make the execute return value have rowcount
        cur_update.execute.return_value = MagicMock(rowcount=1)

        conn = _make_conn([cur_select, cur_update])
        cfg = update_budget_config(
            conn,
            cash_buffer_pct=Decimal("0.1000"),
            updated_by="op",
            reason="raise buffer",
        )

        assert cfg.cash_buffer_pct == Decimal("0.1000")
        assert cfg.cgt_scenario == "higher"
        assert cfg.updated_by == "op"

        # One audit row for cash_buffer_pct
        assert conn.execute.call_count == 1
        sql, params = conn.execute.call_args[0]
        assert "budget_config_audit" in sql
        assert params["field"] == "cash_buffer_pct"
        assert params["old"] == "0.0500"
        assert params["new"] == "0.1000"

    def test_updates_cgt_scenario_and_writes_audit(self) -> None:
        returning_row = {
            **_config_row(scenario="basic"),
            "updated_by": "op",
            "reason": "switch to basic rate",
        }
        cur_select = _make_cursor(rows=[_config_row(scenario="higher")])
        cur_update = _make_cursor(rows=[returning_row])
        cur_update.execute.return_value = MagicMock(rowcount=1)

        conn = _make_conn([cur_select, cur_update])
        cfg = update_budget_config(
            conn,
            cgt_scenario="basic",
            updated_by="op",
            reason="switch to basic rate",
        )

        assert cfg.cgt_scenario == "basic"
        assert conn.execute.call_count == 1
        sql, params = conn.execute.call_args[0]
        assert params["field"] == "cgt_scenario"
        assert params["old"] == "higher"
        assert params["new"] == "basic"

    def test_updates_both_fields_writes_two_audit_rows(self) -> None:
        returning_row = {
            **_config_row(buffer="0.1000", scenario="basic"),
            "updated_by": "op",
            "reason": "full change",
        }
        cur_select = _make_cursor(rows=[_config_row(buffer="0.0500", scenario="higher")])
        cur_update = _make_cursor(rows=[returning_row])
        cur_update.execute.return_value = MagicMock(rowcount=1)

        conn = _make_conn([cur_select, cur_update])
        cfg = update_budget_config(
            conn,
            cash_buffer_pct=Decimal("0.1000"),
            cgt_scenario="basic",
            updated_by="op",
            reason="full change",
        )

        assert cfg.cash_buffer_pct == Decimal("0.1000")
        assert cfg.cgt_scenario == "basic"

        # Two audit rows (one per changed field)
        assert conn.execute.call_count == 2
        fields = {call[0][1]["field"] for call in conn.execute.call_args_list}
        assert fields == {"cash_buffer_pct", "cgt_scenario"}

    def test_atomic_via_transaction(self) -> None:
        returning_row = {**_config_row(buffer="0.1000"), "updated_by": "op", "reason": "r"}
        cur_select = _make_cursor(rows=[_config_row(buffer="0.0500")])
        cur_update = _make_cursor(rows=[returning_row])
        cur_update.execute.return_value = MagicMock(rowcount=1)

        conn = _make_conn([cur_select, cur_update])
        update_budget_config(
            conn,
            cash_buffer_pct=Decimal("0.1000"),
            updated_by="op",
            reason="r",
        )

        conn.transaction.assert_called_once()

    def test_raises_corrupt_when_update_affects_zero_rows(self) -> None:
        cur_select = _make_cursor(rows=[_config_row()])
        cur_update = MagicMock()
        cur_update.__enter__ = MagicMock(return_value=cur_update)
        cur_update.__exit__ = MagicMock(return_value=False)
        cur_update.execute.return_value = MagicMock(rowcount=0)

        conn = _make_conn([cur_select, cur_update])
        with pytest.raises(BudgetConfigCorrupt, match="0 rows"):
            update_budget_config(
                conn,
                cgt_scenario="basic",
                updated_by="op",
                reason="vanished",
            )


# ---------------------------------------------------------------------------
# TestRecordCapitalEvent
# ---------------------------------------------------------------------------


class TestRecordCapitalEvent:
    def test_inserts_event_and_returns_dataclass(self) -> None:
        row = _event_row()
        conn = _make_conn([_make_cursor(rows=[row])])
        event = record_capital_event(
            conn,
            event_type="injection",
            amount=Decimal("5000.00"),
            currency="USD",
            source="operator",
            note="first deposit",
            created_by="operator",
        )

        assert isinstance(event, CapitalEvent)
        assert event.event_id == 1
        assert event.event_type == "injection"
        assert event.amount == Decimal("5000.00")
        assert event.currency == "USD"
        assert event.source == "operator"
        assert event.note == "first deposit"
        assert event.created_by == "operator"
        assert event.event_time == _NOW

    def test_rejects_zero_amount(self) -> None:
        conn = _make_conn([])
        with pytest.raises(ValueError, match="amount must be positive"):
            record_capital_event(
                conn,
                event_type="injection",
                amount=Decimal("0"),
                currency="USD",
                source="operator",
                note=None,
                created_by="op",
            )

    def test_rejects_negative_amount(self) -> None:
        conn = _make_conn([])
        with pytest.raises(ValueError, match="amount must be positive"):
            record_capital_event(
                conn,
                event_type="withdrawal",
                amount=Decimal("-100"),
                currency="USD",
                source="operator",
                note=None,
                created_by="op",
            )

    def test_handles_null_note_and_created_by(self) -> None:
        row = _event_row(note=None, created_by=None)
        conn = _make_conn([_make_cursor(rows=[row])])
        event = record_capital_event(
            conn,
            event_type="tax_provision",
            amount=Decimal("100.00"),
            currency="GBP",
            source="system",
            note=None,
            created_by=None,
        )
        assert event.note is None
        assert event.created_by is None

    def test_raises_runtime_error_when_returning_produces_none(self) -> None:
        cur = _make_cursor(rows=[])
        # Override fetchone to return None (simulating RETURNING producing nothing)
        cur.fetchone.return_value = None
        conn = _make_conn([cur])
        with pytest.raises(RuntimeError, match="RETURNING produced no row"):
            record_capital_event(
                conn,
                event_type="injection",
                amount=Decimal("100"),
                currency="USD",
                source="operator",
                note=None,
                created_by="op",
            )


# ---------------------------------------------------------------------------
# TestListCapitalEvents
# ---------------------------------------------------------------------------


class TestListCapitalEvents:
    def test_returns_events_ordered_by_time(self) -> None:
        rows = [
            _event_row(event_id=2, amount="3000.00", note="second"),
            _event_row(event_id=1, amount="5000.00", note="first"),
        ]
        conn = _make_conn([_make_cursor(rows=rows)])
        events = list_capital_events(conn)

        assert len(events) == 2
        assert events[0].event_id == 2
        assert events[1].event_id == 1
        assert all(isinstance(e, CapitalEvent) for e in events)

    def test_returns_empty_list_when_no_events(self) -> None:
        conn = _make_conn([_make_cursor(rows=[])])
        events = list_capital_events(conn)
        assert events == []

    def test_passes_limit_and_offset_to_query(self) -> None:
        cur = _make_cursor(rows=[])
        conn = _make_conn([cur])
        list_capital_events(conn, limit=10, offset=20)

        # Verify the cursor received limit and offset in the params dict.
        sql, params = cur.execute.call_args[0]
        assert "LIMIT" in sql
        assert "OFFSET" in sql
        assert params["limit"] == 10
        assert params["offset"] == 20

    def test_decimal_conversion_on_returned_events(self) -> None:
        row = _event_row(amount="1234.567890")
        conn = _make_conn([_make_cursor(rows=[row])])
        events = list_capital_events(conn)
        assert events[0].amount == Decimal("1234.567890")
        assert isinstance(events[0].amount, Decimal)


# ---------------------------------------------------------------------------
# TestComputeBudgetState
# ---------------------------------------------------------------------------


def _budget_config_row(
    buffer: str = "0.0500",
    scenario: str = "higher",
) -> dict[str, Any]:
    return {
        "cash_buffer_pct": Decimal(buffer),
        "cgt_scenario": scenario,
        "updated_at": _NOW,
        "updated_by": "seed",
        "reason": "initial seed",
    }


def _budget_conn(
    config_row: dict[str, Any] | None = None,
    cash_balance: Decimal | None = Decimal("10000"),
    deployed: Decimal = Decimal("5000"),
    mirror_equity: float = 2000.0,
    total_gains: Decimal = Decimal("3500"),
    net_gain: Decimal = Decimal("3500"),
    gbp_usd_rate: Decimal | None = Decimal("1.25"),
) -> tuple[MagicMock, float]:
    """Build a mock connection for compute_budget_state.

    _load_mirror_equity is now imported from portfolio.py and must be patched
    by callers via ``unittest.mock.patch("app.services.budget._load_mirror_equity")``.
    This helper returns (conn, mirror_equity_float) so callers can supply the
    patch return_value.

    Cursor order (5 cursors — mirror_equity consumed by patched function):
      0: budget_config (get_budget_config)
      1: cash_balance (_load_cash_balance)
      2: deployed_capital (_load_deployed_capital)
      3: tax estimates (_load_tax_estimates)
      4: gbp_usd rate (_load_gbp_usd_rate)
    """
    if config_row is None:
        config_row = _budget_config_row()

    cur_config = _make_cursor(single=config_row)
    cur_cash = _make_cursor(single={"balance": cash_balance})
    cur_deployed = _make_cursor(single={"deployed": deployed})
    cur_tax = _make_cursor(
        single={"total_gains": total_gains, "net_gain": net_gain},
    )
    if gbp_usd_rate is not None:
        cur_fx = _make_cursor(single={"rate": gbp_usd_rate})
    else:
        # Simulate no row found — fetchone returns None
        cur_fx = _make_cursor()
        cur_fx.fetchone.return_value = None

    return _make_conn([cur_config, cur_cash, cur_deployed, cur_tax, cur_fx]), mirror_equity


class TestComputeBudgetState:
    def test_full_budget_computation(self) -> None:
        """Full happy path: cash=10000, deployed=5000, mirrors=2000,
        higher CGT with gains=3500 net=3500 → taxable=500 → est=120 GBP,
        rate=1.25 → tax_usd=150, buffer=5% of 17000=850,
        available=10000-150-850=9000.
        """
        conn, mirror_val = _budget_conn()
        with (
            unittest.mock.patch(
                "app.services.budget._current_uk_tax_year",
                return_value="2025/26",
            ),
            unittest.mock.patch(
                "app.services.budget._load_mirror_equity",
                return_value=mirror_val,
            ),
        ):
            state = compute_budget_state(conn)

        assert isinstance(state, BudgetState)
        assert state.cash_balance == Decimal("10000")
        assert state.deployed_capital == Decimal("5000")
        assert state.mirror_equity == Decimal("2000")
        assert state.working_budget == Decimal("17000")

        # Tax: total_gains=3500, net_gain=3500, ANNUAL_EXEMPT=3000
        # taxable_net = 3500 - 3000 = 500
        # higher_est = 500 * 0.24 = 120.00
        assert state.estimated_tax_gbp == Decimal("120.00")
        # tax_usd = 120.00 * 1.25 = 150.0000
        assert state.estimated_tax_usd == Decimal("150.0000")
        assert state.gbp_usd_rate == Decimal("1.25")

        # buffer = 17000 * 0.05 = 850.00
        assert state.cash_buffer_reserve == Decimal("850.0000")

        # available = 10000 - 150.0000 - 850.0000 = 9000.0000
        assert state.available_for_deployment == Decimal("9000.0000")
        assert state.cash_buffer_pct == Decimal("0.0500")
        assert state.cgt_scenario == "higher"
        assert state.tax_year == "2025/26"

    def test_unknown_cash_returns_none_available(self) -> None:
        """When cash_ledger SUM is NULL, cash_balance=None,
        working_budget=None, available=None.
        """
        conn, mirror_val = _budget_conn(cash_balance=None)
        with (
            unittest.mock.patch(
                "app.services.budget._current_uk_tax_year",
                return_value="2025/26",
            ),
            unittest.mock.patch(
                "app.services.budget._load_mirror_equity",
                return_value=mirror_val,
            ),
        ):
            state = compute_budget_state(conn)

        assert state.cash_balance is None
        assert state.working_budget is None
        assert state.available_for_deployment is None
        assert state.cash_buffer_reserve == Decimal("0")

    def test_missing_gbp_usd_rate_fails_closed_when_tax_owed(self) -> None:
        """When GBP→USD rate is missing AND there is a non-zero GBP
        tax estimate, ``compute_budget_state`` raises FxRateUnavailable
        rather than silently degrading ``tax_usd = 0``. Previously the
        zero would have let an order through with no tax provision —
        an execution-safety hole (Codex round 2 finding 2 on PR #500;
        #502 PR C)."""
        from app.services.budget import FxRateUnavailable

        conn, mirror_val = _budget_conn(gbp_usd_rate=None)
        with (
            unittest.mock.patch(
                "app.services.budget._current_uk_tax_year",
                return_value="2025/26",
            ),
            unittest.mock.patch(
                "app.services.budget._load_mirror_equity",
                return_value=mirror_val,
            ),
            pytest.raises(FxRateUnavailable),
        ):
            compute_budget_state(conn)

    def test_missing_gbp_usd_rate_when_zero_tax_owed_returns_zero(self) -> None:
        """When GBP→USD rate is missing AND the GBP tax estimate is
        already zero, the computation succeeds with ``tax_usd = 0``
        — there is no figure to convert. Common in non-UK operator
        configs."""
        conn, mirror_val = _budget_conn(
            gbp_usd_rate=None,
            total_gains=Decimal("0"),
            net_gain=Decimal("0"),
        )
        with (
            unittest.mock.patch(
                "app.services.budget._current_uk_tax_year",
                return_value="2025/26",
            ),
            unittest.mock.patch(
                "app.services.budget._load_mirror_equity",
                return_value=mirror_val,
            ),
        ):
            state = compute_budget_state(conn)

        assert state.gbp_usd_rate is None
        assert state.estimated_tax_gbp == Decimal("0")
        assert state.estimated_tax_usd == Decimal("0")

    def test_negative_available_when_over_reserved(self) -> None:
        """When tax + buffer > cash, available is negative."""
        # cash=100, deployed=50000, mirrors=0, gains=50000, net=50000
        # working_budget = 100 + 50000 + 0 = 50100
        # taxable_net = 50000 - 3000 = 47000
        # higher_est = 47000 * 0.24 = 11280
        # tax_usd = 11280 * 1.25 = 14100
        # buffer = 50100 * 0.05 = 2505
        # available = 100 - 14100 - 2505 = -16505
        conn, mirror_val = _budget_conn(
            cash_balance=Decimal("100"),
            deployed=Decimal("50000"),
            mirror_equity=0.0,
            total_gains=Decimal("50000"),
            net_gain=Decimal("50000"),
        )
        with (
            unittest.mock.patch(
                "app.services.budget._current_uk_tax_year",
                return_value="2025/26",
            ),
            unittest.mock.patch(
                "app.services.budget._load_mirror_equity",
                return_value=mirror_val,
            ),
        ):
            state = compute_budget_state(conn)

        assert state.available_for_deployment is not None
        assert state.available_for_deployment < Decimal("0")

    def test_basic_cgt_scenario_uses_basic_estimate(self) -> None:
        """When cgt_scenario='basic', the basic rate (18%) is used."""
        conn, mirror_val = _budget_conn(
            config_row=_budget_config_row(scenario="basic"),
        )
        with (
            unittest.mock.patch(
                "app.services.budget._current_uk_tax_year",
                return_value="2025/26",
            ),
            unittest.mock.patch(
                "app.services.budget._load_mirror_equity",
                return_value=mirror_val,
            ),
        ):
            state = compute_budget_state(conn)

        assert state.cgt_scenario == "basic"
        # basic_est = 500 * 0.18 = 90.00
        assert state.estimated_tax_gbp == Decimal("90.00")


class TestCurrentUkTaxYear:
    def test_mid_april_returns_current_start_year(self) -> None:
        """2026-04-15 is past 6 April → in the 2026/27 tax year."""
        with unittest.mock.patch(
            "app.services.budget.datetime",
        ) as mock_dt:
            mock_dt.now.return_value = datetime(2026, 4, 15, tzinfo=UTC)
            result = _current_uk_tax_year()
        assert result == "2026/27"

    def test_april_5_belongs_to_previous_year(self) -> None:
        """2025-04-05 is still in the 2024/25 tax year."""
        with unittest.mock.patch(
            "app.services.budget.datetime",
        ) as mock_dt:
            mock_dt.now.return_value = datetime(2025, 4, 5, tzinfo=UTC)
            result = _current_uk_tax_year()
        assert result == "2024/25"

    def test_april_6_starts_new_year(self) -> None:
        """2025-04-06 is in the 2025/26 tax year."""
        with unittest.mock.patch(
            "app.services.budget.datetime",
        ) as mock_dt:
            mock_dt.now.return_value = datetime(2025, 4, 6, tzinfo=UTC)
            result = _current_uk_tax_year()
        assert result == "2025/26"

    def test_january_belongs_to_previous_start_year(self) -> None:
        """2026-01-15 is in the 2025/26 tax year."""
        with unittest.mock.patch(
            "app.services.budget.datetime",
        ) as mock_dt:
            mock_dt.now.return_value = datetime(2026, 1, 15, tzinfo=UTC)
            result = _current_uk_tax_year()
        assert result == "2025/26"

    def test_century_boundary_format(self) -> None:
        """2099-12-31 should produce '2099/00'."""
        with unittest.mock.patch(
            "app.services.budget.datetime",
        ) as mock_dt:
            mock_dt.now.return_value = datetime(2099, 12, 31, tzinfo=UTC)
            result = _current_uk_tax_year()
        assert result == "2099/00"
