"""#2842 slice 5c-i — the pot's exits against real Postgres (spec §7.4, ``sql/451``).

One executed-book entry is decided (5a), submitted (5b) and filled through the real reconciliation with a stub broker.
Then: its protection is repaired by the shared manager; a later rebalance stamps it for exit and the manager closes it
(`rerank_exit`) from 15:00 UTC on the exit session, past a pending edit and around an untradable name; a wind-down
stamps it from the event; and `completed` waits for a flat executed book.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

import psycopg
import pytest

from app.providers.broker import (
    BrokerOrderDetail,
    BrokerPortfolio,
    BrokerPosition,
    BrokerPositionCloseSubmission,
    BrokerPositionEditSubmission,
    BrokerPositionExecution,
)
from app.services import ranking_pot_executor as px
from app.services import ranking_pot_exits as exits
from app.services import ranking_pot_rebalance as rb
from app.services.ranking_pot_policy import RANKING_POT_POLICY_HASH
from app.services.ranking_pot_step import run_step_job, wind_down_session
from app.services.strategy_order_reconciliation import reconcile_strategy_order
from app.services.strategy_position_manager import manage_owned_position
from tests.test_ai_trial_executor_db import _REQUEST_ID, _enable_trading
from tests.test_ranking_pot_exec_db import _decide_month, _next_month_session, _universes
from tests.test_ranking_pot_executor_db import _at, _deploy_pot, _market, _pot_broker, _signal_of
from tests.test_ranking_pot_job_db import _first_window
from tests.test_ranking_pot_schema_db import _frozen, _move

Conn = psycopg.Connection[Any]
_POSITION_ID = 28420001
_EDIT_ID = UUID("28420000-0000-4000-8000-0000000028e1")


def _position(**changes: Any) -> BrokerPosition:
    # The 5b fixture's sent levels: 3 × ATR(2) stop and 2R target off the 100 ask.
    base = BrokerPosition(
        instrument_id=2842,
        units=Decimal("0.4"),
        open_price=Decimal("100"),
        current_price=Decimal("101"),
        raw_payload={},
        position_id=_POSITION_ID,
        open_date_time=None,
        stop_loss_rate=Decimal("94"),
        take_profit_rate=Decimal("112"),
        is_no_stop_loss=False,
        is_no_take_profit=False,
    )
    return replace(base, **changes)


def _portfolio(broker: Any, position: BrokerPosition) -> None:
    broker.get_portfolio.return_value = BrokerPortfolio(
        positions=(position,), available_cash=Decimal("500"), raw_payload={}
    )


def _opened(conn: Conn, monkeypatch: pytest.MonkeyPatch) -> tuple[int, date, int, Any]:
    """Declaration executing; 2842 entered at month 1, filled. Returns (declaration, last session d, trade, broker)."""
    decl_id = _frozen(conn)
    _move(conn, decl_id, "shadow_only", "executing", "supervisor")
    _deploy_pot(conn, decl_id)
    _enable_trading(conn)
    conn.autocommit = True
    d, _ = _first_window(conn)
    a1, r1 = _decide_month(conn, monkeypatch, d, _universes({2842: "0.9"}, {2842}), policy_hash=RANKING_POT_POLICY_HASH)
    assert r1.entries == 1
    t1 = rb.target_session(datetime.combine(d, time(23, 40), UTC))
    now = _at(t1)
    _market(conn, now, {d: Decimal(100)})
    conn.autocommit = False
    monkeypatch.setattr("app.services.strategy_order_reconciliation.uuid4", lambda: _REQUEST_ID)
    broker = _pot_broker(2842, now)
    result = px.execute_pot_signal(conn, broker=broker, signal_id=_signal_of(conn, a1, 2842), now=now)
    assert result.verdict == "submitted" and isinstance(result, px.PaperExecutionResult)
    assert result.strategy_trade_id is not None and result.order_id is not None
    broker.lookup_order.return_value = BrokerOrderDetail(
        broker_order_ref="3471001",  # the stub broker's placement ref
        reference_id=str(_REQUEST_ID),
        status="filled",
        broker_status="Filled",
        instrument_id=2842,
        position_executions=(
            BrokerPositionExecution(
                position_id=_POSITION_ID,
                state="open",
                remaining_units=Decimal("0.4"),
                opening_units=Decimal("0.4"),
                average_price=Decimal("100"),
                execution_time=now,
                fees=Decimal("0"),
                raw_payload={},
            ),
        ),
        last_update=now,
        raw_payload={},
    )
    assert reconcile_strategy_order(conn, broker=broker, order_id=result.order_id).state == "resolved"
    _portfolio(broker, _position())
    close = BrokerPositionCloseSubmission("2842999", _POSITION_ID, {"orderForClose": {"orderID": 2842999}})

    def _close_position(**kwargs: Any) -> BrokerPositionCloseSubmission:
        kwargs["persist_response"](close.raw_payload)
        return close

    broker.close_demo_strategy_position.side_effect = _close_position
    edit = BrokerPositionEditSubmission(_EDIT_ID, _POSITION_ID, _EDIT_ID, {"ok": True})

    def _edit(**kwargs: Any) -> BrokerPositionEditSubmission:
        kwargs["persist_response"](edit.raw_payload)
        return edit

    broker.edit_demo_strategy_position.side_effect = _edit
    return decl_id, d, result.strategy_trade_id, broker


def _manage(conn: Conn, broker: Any, trade_id: int, at: datetime) -> tuple[str, str]:
    conn.execute("UPDATE quotes SET quoted_at = %s WHERE instrument_id = 2842", (at,))
    conn.commit()
    result = manage_owned_position(
        conn, broker=broker, strategy_trade_id=trade_id, broker_position_id=_POSITION_ID, now=at
    )
    return result.state, result.reason_code


def _operations(conn: Conn) -> list[tuple[str, str, str, str | None]]:
    rows = conn.execute(
        "SELECT operation_type, trigger_code, status, last_error_code FROM strategy_position_operations "
        "ORDER BY position_operation_id"
    ).fetchall()
    conn.commit()
    return rows


def _rerank_exit(conn: Conn, monkeypatch: pytest.MonkeyPatch, d: date) -> date:
    """Month 2: 2842 leaves R, so the rebalance stamps it for exit at its target session."""
    conn.autocommit = True
    d2 = _next_month_session(d)
    a2, r2 = _decide_month(
        conn, monkeypatch, d2, _universes({2843: "0.8"}, {2843}), policy_hash=RANKING_POT_POLICY_HASH
    )
    conn.autocommit = False
    assert r2.exits == 1
    stamp = conn.execute("SELECT exit_session, attempt_id FROM ranking_pot_exec_exit_stamps").fetchone()
    conn.commit()
    assert stamp is not None and stamp[1] == a2
    return stamp[0]


def _halted(broker: Any) -> None:
    response = broker.check_instrument_eligibility.return_value
    broker.check_instrument_eligibility.return_value = replace(
        response, eligibilities=(replace(response.eligibilities[0], allow_close_position=False),)
    )


def test_a_pot_trade_is_repaired_and_closed_from_15_utc_on_its_exit_session(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _, d, trade_id, broker = _opened(conn, monkeypatch)
    exit_session = _rerank_exit(conn, monkeypatch, d)
    due = datetime.combine(exit_session, time(15), UTC)

    # Not yet due: a removed SL is repaired to the sent level (the shared signal arm).
    _portfolio(broker, _position(stop_loss_rate=None, is_no_stop_loss=True))
    assert _manage(conn, broker, trade_id, due - timedelta(minutes=1)) == ("submitted", "broker_edit_accepted")
    assert broker.edit_demo_strategy_position.call_args.kwargs["stop_loss_rate"] == Decimal("94")
    broker.close_demo_strategy_position.assert_not_called()

    # Due, with that edit accepted but never landed: the edit yields and the close is sent.
    assert _manage(conn, broker, trade_id, due) == ("submitted", "broker_close_accepted")
    assert broker.close_demo_strategy_position.call_args.kwargs["position_id"] == _POSITION_ID
    assert _operations(conn) == [
        ("fixed_exit_repair", "entry_exit_gap", "reconcile_required", "superseded_by_pot_exit"),
        ("close", "rerank_exit", "submitted", None),
    ]


def test_an_untradable_name_is_repaired_and_its_close_retried(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _, d, trade_id, broker = _opened(conn, monkeypatch)
    exit_session = _rerank_exit(conn, monkeypatch, d)
    due = datetime.combine(exit_session, time(15), UTC)
    _halted(broker)

    # Protected and halted: nothing to do but wait (the close is retried every cycle).
    assert _manage(conn, broker, trade_id, due) == ("no_change", "position_protected")
    # Unprotected and halted: the protection repair still runs.
    _portfolio(broker, _position(stop_loss_rate=None, is_no_stop_loss=True))
    assert _manage(conn, broker, trade_id, due + timedelta(minutes=5)) == ("submitted", "broker_edit_accepted")
    broker.close_demo_strategy_position.assert_not_called()
    # The pending edit is kept while the close cannot be sent.
    assert _manage(conn, broker, trade_id, due + timedelta(minutes=10)) == ("rejected", "broker_close_not_allowed")
    assert [op[:3] for op in _operations(conn)] == [("fixed_exit_repair", "entry_exit_gap", "submitted")]


def _held(conn: Conn) -> list[tuple[Any, ...]]:
    rows = conn.execute(
        "SELECT stop_loss_rate, take_profit_rate, is_no_stop_loss, is_no_take_profit, observed_at "
        "FROM ranking_pot_exec_level_observations ORDER BY observation_id"
    ).fetchall()
    conn.commit()
    return rows


def test_the_held_levels_are_recorded_change_only_before_any_repair(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    """§7.4 "The broker-held levels" (slice 5c-ii-c, r3-108)."""
    conn = ebull_test_conn
    _, d, trade_id, broker = _opened(conn, monkeypatch)
    t0 = datetime.combine(_next_month_session(d), time(14), UTC)

    # The first cycle records the level the broker holds; an unchanged cycle writes nothing.
    assert _manage(conn, broker, trade_id, t0) == ("no_change", "position_protected")
    assert _manage(conn, broker, trade_id, t0 + timedelta(minutes=5)) == ("no_change", "position_protected")
    assert _held(conn) == [(Decimal("94"), Decimal("112"), False, False, t0)]

    # A removed SL is recorded as held, then repaired: the row precedes the edit it causes.
    _portfolio(broker, _position(stop_loss_rate=None, is_no_stop_loss=True))
    t1 = t0 + timedelta(minutes=10)
    assert _manage(conn, broker, trade_id, t1) == ("submitted", "broker_edit_accepted")
    assert _held(conn)[1:] == [(None, Decimal("112"), True, False, t1)]
    first_edit = conn.execute("SELECT min(created_at) FROM strategy_position_operations").fetchone()
    recorded = conn.execute("SELECT max(recorded_at) FROM ranking_pot_exec_level_observations").fetchone()
    conn.commit()
    assert first_edit is not None and recorded is not None and recorded[0] <= first_edit[0]

    # Append-only, and bound to a position the trade owns.
    with pytest.raises(psycopg.errors.RaiseException):
        conn.execute("DELETE FROM ranking_pot_exec_level_observations")
    conn.rollback()
    with pytest.raises(psycopg.errors.RaiseException, match="is not owned by pot trade"):
        conn.execute(
            "INSERT INTO ranking_pot_exec_level_observations (strategy_trade_id, broker_position_id, observed_at, "
            "is_no_stop_loss, is_no_take_profit) VALUES (%s, 1, now(), FALSE, FALSE)",
            (trade_id,),
        )
    conn.rollback()


def test_a_failed_held_level_record_never_blocks_the_repair(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _, d, trade_id, broker = _opened(conn, monkeypatch)

    def _fail(*_: Any, **__: Any) -> bool:
        raise psycopg.errors.InternalError("recorder down")

    monkeypatch.setattr("app.services.strategy_position_manager.record_held_levels", _fail)
    _portfolio(broker, _position(stop_loss_rate=None, is_no_stop_loss=True))
    at = datetime.combine(_next_month_session(d), time(14), UTC)
    assert _manage(conn, broker, trade_id, at) == ("submitted", "broker_edit_accepted")
    assert broker.edit_demo_strategy_position.call_args.kwargs["stop_loss_rate"] == Decimal("94")
    assert _held(conn) == []


def test_a_wind_down_stamps_the_book_and_completed_waits_for_it_to_be_flat(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    decl_id, _, trade_id, broker = _opened(conn, monkeypatch)
    _move(conn, decl_id, "executing", "winding_down", "operator", "operator")
    event = conn.execute("SELECT event_id, at FROM ranking_pot_state_events ORDER BY event_id DESC LIMIT 1").fetchone()
    conn.commit()
    assert event is not None
    w = wind_down_session(event[1])

    assert exits.stamp_wind_down(conn, decl_id) == 1
    assert exits.stamp_wind_down(conn, decl_id) == 0
    stamp = conn.execute(
        "SELECT state_event_id, attempt_id, exit_session, reason FROM ranking_pot_exec_exit_stamps"
    ).fetchone()
    conn.commit()
    assert stamp == (event[0], None, w, "wind_down:operator")

    decl = rb.load_declaration(conn)
    assert decl is not None
    now = datetime.combine(w, time(15), UTC)
    assert exits.complete_if_flat(conn, decl, now=now) == (
        f"not completed: lifecycle {_lifecycle(conn)}: trade {trade_id} is open"
    )
    # No writer can complete a non-flat executed book: sql/451's trigger.
    with pytest.raises(psycopg.errors.RaiseException, match="executed_not_flat"):
        _move(conn, decl_id, "winding_down", "completed", "engine")

    # The manager closes it at W; once closed and released, only the books' fence remains (a decided snapshot
    # exists, and no step has applied the wind-down), which reads as a quiet "not yet".
    assert _manage(conn, broker, trade_id, now) == ("submitted", "broker_close_accepted")
    conn.execute("UPDATE strategy_trades SET status = 'closed' WHERE strategy_trade_id = %s", (trade_id,))
    conn.execute(
        "UPDATE strategy_position_ownership SET status = 'released', released_at = now(), release_reason = 'closed' "
        "WHERE strategy_trade_id = %s",
        (trade_id,),
    )
    conn.commit()
    assert exits.complete_if_flat(conn, decl, now=now) == "not completed: books_incomplete (0 checkpoints)"


def _lifecycle(conn: Conn) -> int:
    row = conn.execute("SELECT lifecycle_id FROM ranking_pot_exec_lifecycles").fetchone()
    conn.commit()
    assert row is not None
    return int(row[0])


def test_a_trial_winding_down_before_any_decision_completes_at_the_next_step_fire(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl_id = _frozen(conn)
    _move(conn, decl_id, "shadow_only", "winding_down", "operator", "operator")
    conn.autocommit = True
    result = run_step_job(conn)
    assert result.note == "completed"
    state = conn.execute(
        "SELECT to_state, actor FROM ranking_pot_state_events WHERE declaration_id = %s ORDER BY event_id DESC LIMIT 1",
        (decl_id,),
    ).fetchone()
    assert state == ("completed", "engine")
    assert rb.load_declaration(conn) is None
