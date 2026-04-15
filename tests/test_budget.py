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

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.services.budget import (
    BudgetConfig,
    BudgetConfigCorrupt,
    CapitalEvent,
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
