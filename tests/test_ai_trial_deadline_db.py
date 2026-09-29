"""#3471 slice 2c-ii — the §8 per-trade deadline against real Postgres: stamped by the
reconciliation that opens a trial leg, enforced by the position manager, guarded by `sql/435`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from app.providers.broker import (
    BrokerOrderDetail,
    BrokerPortfolio,
    BrokerPosition,
    BrokerPositionCloseSubmission,
    BrokerPositionExecution,
)
from app.services.ai_trial_executor import execute_trial_signal
from app.services.strategy_order_reconciliation import reconcile_strategy_order
from app.services.strategy_position_manager import configure_position_manager, manage_owned_position
from tests.test_ai_trial_executor_db import _REQUEST_ID, _broker, _control_instrument, _enable_trading
from tests.test_ai_trial_intent_db import ARM_INSTRUMENT, NOW, _published_pair

Conn = psycopg.Connection[Any]
_POSITION_ID = 34710001
# NOW is Monday 2026-10-05 15:00 UTC; the fixture decision's horizon is 10 sessions, and
# Columbus Day is a NYSE session, so session 10 is Monday 2026-10-19.
_DEADLINE = date(2026, 10, 19)


def _position(*, open_at: datetime) -> BrokerPosition:
    return BrokerPosition(
        instrument_id=ARM_INSTRUMENT,
        units=Decimal("1.25"),
        open_price=Decimal("100"),
        current_price=Decimal("101"),
        raw_payload={},
        position_id=_POSITION_ID,
        open_date_time=open_at,
        # The arm decision's 8% / 16% levels off the 100 ask, so the position needs no repair.
        stop_loss_rate=Decimal("92"),
        take_profit_rate=Decimal("116"),
        is_no_stop_loss=False,
        is_no_take_profit=False,
    )


def _opened_arm_leg(
    conn: Conn, monkeypatch: pytest.MonkeyPatch, *, filled_at: datetime = NOW, pack: Mapping[str, Any] | None = None
) -> tuple[int, Any]:
    _, signals = _published_pair(conn, pack=pack)
    _enable_trading(conn)
    broker = _broker(ARM_INSTRUMENT)
    monkeypatch.setattr("app.services.strategy_order_reconciliation.uuid4", lambda: _REQUEST_ID)
    submitted = execute_trial_signal(conn, broker=broker, signal_id=signals["arm"], now=NOW)
    assert submitted.strategy_trade_id is not None and submitted.order_id is not None
    broker.lookup_order.return_value = BrokerOrderDetail(
        broker_order_ref="3471001",
        reference_id=str(_REQUEST_ID),
        status="filled",
        broker_status="Filled",
        instrument_id=ARM_INSTRUMENT,
        position_executions=(
            BrokerPositionExecution(
                position_id=_POSITION_ID,
                state="open",
                remaining_units=Decimal("1.25"),
                opening_units=Decimal("1.25"),
                average_price=Decimal("100"),
                execution_time=filled_at,
                fees=Decimal("0"),
                raw_payload={},
            ),
        ),
        last_update=filled_at,
        raw_payload={},
    )
    assert reconcile_strategy_order(conn, broker=broker, order_id=submitted.order_id).state == "resolved"
    broker.get_portfolio.return_value = BrokerPortfolio(
        positions=(_position(open_at=filled_at),), available_cash=Decimal("500"), raw_payload={}
    )
    close = BrokerPositionCloseSubmission("3471999", _POSITION_ID, {"orderForClose": {"orderID": 3471999}})

    def _close_position(**kwargs: Any) -> BrokerPositionCloseSubmission:
        kwargs["persist_response"](close.raw_payload)
        return close

    broker.close_demo_strategy_position.side_effect = _close_position
    return submitted.strategy_trade_id, broker


def _deadline(conn: Conn, trade_id: int) -> tuple[str, date | None]:
    row = conn.execute(
        "SELECT status, exit_deadline_session FROM strategy_trades WHERE strategy_trade_id = %s", (trade_id,)
    ).fetchone()
    conn.commit()
    assert row is not None
    return row[0], row[1]


def test_the_opening_reconciliation_stamps_the_deadline_from_the_fill_session(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    # An after-hours fill (23:30 New York) still belongs to Monday's session.
    trade_id, _ = _opened_arm_leg(conn, monkeypatch, filled_at=datetime(2026, 10, 6, 3, 30, tzinfo=UTC))
    assert _deadline(conn, trade_id) == ("open", _DEADLINE)


def test_the_manager_exits_a_trial_leg_from_15_utc_on_its_deadline_session(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, broker = _opened_arm_leg(conn, monkeypatch)
    due_at = datetime.combine(_DEADLINE, datetime.min.time(), tzinfo=UTC).replace(hour=15)

    early = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=_POSITION_ID,
        now=due_at - timedelta(seconds=1),
    )
    assert (early.state, early.reason_code) == ("no_change", "position_protected")
    broker.close_demo_strategy_position.assert_not_called()

    due = manage_owned_position(
        conn, broker=broker, strategy_trade_id=trade_id, broker_position_id=_POSITION_ID, now=due_at
    )
    assert due.state == "submitted"
    assert broker.close_demo_strategy_position.call_args.kwargs["position_id"] == _POSITION_ID
    assert conn.execute("SELECT operation_type, trigger_code FROM strategy_position_operations").fetchall() == [
        ("close", "exit_deadline")
    ]


def test_a_due_deadline_closes_even_when_the_broker_omits_the_open_time(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, broker = _opened_arm_leg(conn, monkeypatch)
    deployment = conn.execute(
        "SELECT fd.deployment_id FROM strategy_trades t JOIN strategy_funding_decisions fd "
        "ON fd.funding_decision_id = t.funding_decision_id WHERE t.strategy_trade_id = %s",
        (trade_id,),
    ).fetchone()
    assert deployment is not None
    configure_position_manager(
        conn,
        deployment_id=int(deployment[0]),
        max_position_age_seconds=40 * 86400,
        ratchet_variant_id=None,
        updated_by="test",
        reason="#3471 trial age cap",
    )
    conn.commit()
    broker.get_portfolio.return_value = BrokerPortfolio(
        positions=(replace(_position(open_at=NOW), open_date_time=None),), available_cash=Decimal("500"), raw_payload={}
    )
    # Before the deadline the age-out still needs the open time (Codex ckpt-2: only a DUE
    # deadline is independent of it).
    early = manage_owned_position(
        conn, broker=broker, strategy_trade_id=trade_id, broker_position_id=_POSITION_ID, now=NOW
    )
    assert (early.state, early.reason_code) == ("reconcile_required", "position_open_time_missing")
    conn.execute("UPDATE strategy_trades SET status = 'open' WHERE strategy_trade_id = %s", (trade_id,))
    conn.commit()

    due = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=_POSITION_ID,
        now=datetime(2026, 10, 19, 15, 0, tzinfo=UTC),
    )
    assert due.state == "submitted"
    assert conn.execute("SELECT trigger_code FROM strategy_position_operations").fetchall() == [("exit_deadline",)]


def test_a_halted_name_at_its_deadline_is_retried_not_closed(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, broker = _opened_arm_leg(conn, monkeypatch)
    eligibility = broker.check_instrument_eligibility.return_value
    halted = eligibility.eligibilities[0].__class__(
        **{**eligibility.eligibilities[0].__dict__, "allow_close_position": False}
    )
    broker.check_instrument_eligibility.return_value = eligibility.__class__(
        **{**eligibility.__dict__, "eligibilities": (halted,)}
    )
    overdue = datetime(2026, 10, 21, 2, 0, tzinfo=UTC)
    result = manage_owned_position(
        conn, broker=broker, strategy_trade_id=trade_id, broker_position_id=_POSITION_ID, now=overdue
    )
    assert (result.state, result.reason_code) == ("rejected", "broker_close_not_allowed")
    broker.close_demo_strategy_position.assert_not_called()
    assert _deadline(conn, trade_id) == ("open", _DEADLINE)


def test_the_schema_keeps_the_deadline_trial_only_immutable_and_present_once_open(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, _ = _opened_arm_leg(conn, monkeypatch)

    for moved in (_DEADLINE + timedelta(days=1), None):
        with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
            conn.execute(
                "UPDATE strategy_trades SET exit_deadline_session = %s WHERE strategy_trade_id = %s",
                (moved, trade_id),
            )
        conn.rollback()

    # The control leg is linked while planned; opening it without a deadline is refused.
    signal = conn.execute("SELECT signal_id FROM ai_trial_leg_links WHERE leg = 'control'").fetchone()
    assert signal is not None
    conn.commit()
    monkeypatch.setattr("app.services.strategy_order_reconciliation.uuid4", uuid4)
    control = execute_trial_signal(
        conn, broker=_broker(_control_instrument(conn, signal[0])), signal_id=signal[0], now=NOW
    ).strategy_trade_id
    assert control is not None
    with pytest.raises(psycopg.errors.RaiseException, match="without an exit deadline"):
        conn.execute("UPDATE strategy_trades SET status = 'open' WHERE strategy_trade_id = %s", (control,))
    conn.rollback()

    # A trade with no trial link never carries one. The BEFORE trigger fires ahead of the
    # (AFTER) foreign-key check, so an unfunded id reaches it.
    with pytest.raises(psycopg.errors.RaiseException, match="not a trial leg"):
        conn.execute(
            "INSERT INTO strategy_trades (funding_decision_id, instrument_id, status, exit_deadline_session) "
            "VALUES (-1, %s, 'planned', %s)",
            (ARM_INSTRUMENT, _DEADLINE),
        )
    conn.rollback()


def test_a_trial_link_is_refused_on_a_trade_that_is_no_longer_planned(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, _ = _opened_arm_leg(conn, monkeypatch)
    row = conn.execute(
        "SELECT pair_id, leg, requested_amount FROM ai_trial_trade_links WHERE strategy_trade_id = %s", (trade_id,)
    ).fetchone()
    assert row is not None
    conn.commit()
    with pytest.raises(psycopg.errors.RaiseException, match="not planned"):
        conn.execute(
            "INSERT INTO ai_trial_trade_links (pair_id, leg, strategy_trade_id, requested_amount) "
            "VALUES (%s, 'control', %s, %s)",
            (row[0], trade_id, row[2]),
        )
    conn.rollback()
