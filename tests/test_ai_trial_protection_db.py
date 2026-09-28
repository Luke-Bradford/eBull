"""#3471 slice 2c-iii-a — spec §15 O10 against real Postgres: a demo-trial leg whose SL/TP repair
fails is closed; when the close is refused too, the trial halts. Includes the #3484 Codex P1: an
accepted edit that never lands no longer holds a trial leg's exits hostage.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

import psycopg
import pytest

from app.providers.broker import BrokerPortfolio, BrokerPositionEditSubmission, BrokerPositionMutationUncertain
from app.services.strategy_exit_protection import alerting_exit_protection, check_exit_protection
from app.services.strategy_position_manager import manage_owned_position
from tests.test_ai_trial_deadline_db import _POSITION_ID, _opened_arm_leg, _position
from tests.test_ai_trial_intent_db import ARM_INSTRUMENT, NOW

Conn = psycopg.Connection[Any]
_EDIT_OPERATION_ID = UUID("5a4f0e7e-3471-4c2c-9d00-3c3c3c3c3c3c")
# A day after the fill, well before the 2026-10-19 deadline.
_T0 = NOW + timedelta(days=1)


def _unprotected(broker: Any) -> None:
    broker.get_portfolio.return_value = BrokerPortfolio(
        positions=(replace(_position(open_at=NOW), stop_loss_rate=None, is_no_stop_loss=True),),
        available_cash=Decimal("500"),
        raw_payload={},
    )


def _eligibility(broker: Any, *, edit: bool = True, close: bool = True) -> None:
    response = broker.check_instrument_eligibility.return_value
    instrument = response.eligibilities[0]
    arm = replace(instrument.leverage_configs[0], allow_edit_stop_loss=edit)
    broker.check_instrument_eligibility.return_value = replace(
        response,
        eligibilities=(replace(instrument, allow_close_position=close, leverage_configs=(arm,)),),
    )


def _operations(conn: Conn) -> list[tuple[str, str, str, str | None]]:
    rows = conn.execute(
        "SELECT operation_type, trigger_code, status, last_error_code FROM strategy_position_operations "
        "ORDER BY position_operation_id"
    ).fetchall()
    conn.commit()
    return rows


def _trial_state(conn: Conn) -> list[tuple[str, str]]:
    rows = conn.execute("SELECT to_state, actor FROM ai_trial_state_events ORDER BY event_id").fetchall()
    conn.commit()
    return rows


def _manage(conn: Conn, broker: Any, now: datetime) -> tuple[str, str]:
    result = manage_owned_position(
        conn, broker=broker, strategy_trade_id=_trade_id(conn), broker_position_id=_POSITION_ID, now=now
    )
    return result.state, result.reason_code


def _trade_id(conn: Conn) -> int:
    row = conn.execute("SELECT strategy_trade_id FROM ai_trial_trade_links WHERE leg = 'arm'").fetchone()
    conn.commit()
    assert row is not None
    return int(row[0])


def test_a_refused_repair_closes_the_trial_leg(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    _, broker = _opened_arm_leg(conn, monkeypatch)
    _unprotected(broker)
    _eligibility(broker, edit=False)

    assert _manage(conn, broker, _T0) == ("submitted", "broker_close_accepted")
    assert _operations(conn) == [("close", "protection_failed", "submitted", None)]
    assert _trial_state(conn)[-1][0] == "active"


def test_a_refused_repair_and_a_refused_close_halt_the_trial_once(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _, broker = _opened_arm_leg(conn, monkeypatch)
    _unprotected(broker)
    _eligibility(broker, edit=False, close=False)

    assert _manage(conn, broker, _T0) == ("rejected", "broker_close_not_allowed")
    broker.close_demo_strategy_position.assert_not_called()
    assert _trial_state(conn)[-1] == ("halted_operator", "engine")
    # O10's operator alert: the unprotected position is `unrepairable` on /system/status.
    alerting = alerting_exit_protection(check_exit_protection(conn))
    conn.commit()
    assert [(entry.broker_position_id, entry.last_refusal_reason) for entry in alerting] == [
        (_POSITION_ID, "trial_protection_close_refused")
    ]
    halts = len(_trial_state(conn))
    # A second visit does not stack a halt on the halt.
    assert _manage(conn, broker, _T0 + timedelta(minutes=5)) == ("rejected", "broker_close_not_allowed")
    assert len(_trial_state(conn)) == halts


def _stuck_edit(conn: Conn, broker: Any, at: datetime) -> None:
    """An accepted repair edit whose levels never arrive at the broker."""
    conn.execute("UPDATE quotes SET quoted_at = %s WHERE instrument_id = %s", (at, ARM_INSTRUMENT))
    conn.commit()
    submission = BrokerPositionEditSubmission(_EDIT_OPERATION_ID, _POSITION_ID, _EDIT_OPERATION_ID, {"ok": True})

    def _edit(**kwargs: Any) -> BrokerPositionEditSubmission:
        kwargs["persist_response"](submission.raw_payload)
        return submission

    broker.edit_demo_strategy_position.side_effect = _edit
    assert _manage(conn, broker, at)[0] == "submitted"
    conn.execute("UPDATE strategy_position_operations SET submitted_at = %s", (at,))
    conn.commit()


def test_an_edit_that_never_lands_gives_way_to_a_protection_close_after_one_cycle(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _, broker = _opened_arm_leg(conn, monkeypatch)
    _unprotected(broker)
    _stuck_edit(conn, broker, _T0)

    # Inside the cycle the edit is still pending, and nothing else may act on the position.
    assert _manage(conn, broker, _T0 + timedelta(minutes=2)) == ("pending", "broker_edit_pending")
    broker.close_demo_strategy_position.assert_not_called()

    # The next cycle, a few seconds short of five minutes after the stamp, has failed it.
    assert _manage(conn, broker, _T0 + timedelta(minutes=4, seconds=55)) == ("submitted", "broker_close_accepted")
    assert _operations(conn) == [
        ("fixed_exit_repair", "entry_exit_gap", "reconcile_required", "superseded_by_trial_close"),
        ("close", "protection_failed", "submitted", None),
    ]


def test_a_due_deadline_supersedes_a_fresh_stuck_edit(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    _, broker = _opened_arm_leg(conn, monkeypatch)
    _unprotected(broker)
    due_at = datetime(2026, 10, 19, 15, 0, tzinfo=UTC)
    _stuck_edit(conn, broker, due_at - timedelta(minutes=2))

    assert _manage(conn, broker, due_at) == ("submitted", "broker_close_accepted")
    assert [row[1] for row in _operations(conn)] == ["entry_exit_gap", "exit_deadline"]


def test_a_pending_edit_on_a_protected_position_is_not_a_protection_failure(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _, broker = _opened_arm_leg(conn, monkeypatch)
    _unprotected(broker)
    _stuck_edit(conn, broker, _T0)
    # The operator tightens the stop by hand: the edit never lands as requested, but the
    # position carries both levels, so there is nothing to close it for.
    broker.get_portfolio.return_value = BrokerPortfolio(
        positions=(replace(_position(open_at=NOW), stop_loss_rate=Decimal("95")),),
        available_cash=Decimal("500"),
        raw_payload={},
    )
    assert _manage(conn, broker, _T0 + timedelta(minutes=10)) == ("pending", "broker_edit_pending")
    broker.close_demo_strategy_position.assert_not_called()


def test_an_uncertain_protection_close_is_not_resubmitted(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _, broker = _opened_arm_leg(conn, monkeypatch)
    _unprotected(broker)
    _eligibility(broker, edit=False)
    broker.close_demo_strategy_position.side_effect = BrokerPositionMutationUncertain("timeout")

    assert _manage(conn, broker, _T0) == ("reconcile_required", "broker_close_uncertain")
    assert _manage(conn, broker, _T0 + timedelta(minutes=5)) == ("reconcile_required", "trial_close_outstanding")
    broker.close_demo_strategy_position.assert_called_once()
    # Uncertain is not refused: the trial is not halted over a close that may have executed.
    assert _trial_state(conn)[-1][0] == "active"
