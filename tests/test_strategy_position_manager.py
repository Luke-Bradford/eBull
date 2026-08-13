"""#2452 exact-position paper manager integration tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import psycopg
import pytest

from app.providers.broker import (
    BrokerCloseOrderDetail,
    BrokerOrderDetail,
    BrokerPortfolio,
    BrokerPosition,
    BrokerPositionCloseSubmission,
    BrokerPositionEditSubmission,
    BrokerPositionExecution,
    BrokerPositionMutationError,
)
from app.services.strategy_control_plane import StrategyOwnershipError
from app.services.strategy_order_reconciliation import reconcile_strategy_order
from app.services.strategy_paper_executor import execute_fired_paper_signal
from app.services.strategy_position_manager import (
    RatchetBar,
    StrategyPositionManagerError,
    calculate_ratchet_stop,
    configure_position_manager,
    manage_owned_position,
    register_ratchet_variant,
)
from tests.test_strategy_paper_executor import _NOW, _REQUEST_ID, _broker, _seed

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("registered_strategy_test_candidates")]

_POSITION_ID = 24520001
_MANUAL_POSITION_ID = 24520999
_EDIT_OPERATION_ID = UUID("2165467c-73b8-4d2c-ac3c-b00968f0cfe3")


def _position(position_id: int, *, stop: Decimal | None, take: Decimal | None) -> BrokerPosition:
    return BrokerPosition(
        instrument_id=2449001,
        units=Decimal("1"),
        open_price=Decimal("100"),
        current_price=Decimal("105"),
        raw_payload={},
        position_id=position_id,
        open_date_time=_NOW - timedelta(days=2),
        stop_loss_rate=stop,
        take_profit_rate=take,
        is_no_stop_loss=stop is None,
        is_no_take_profit=take is None,
    )


def _order_detail() -> BrokerOrderDetail:
    execution = BrokerPositionExecution(
        position_id=_POSITION_ID,
        state="open",
        remaining_units=Decimal("1"),
        opening_units=Decimal("1"),
        average_price=Decimal("100"),
        execution_time=_NOW,
        fees=Decimal("0.1"),
        raw_payload={},
    )
    return BrokerOrderDetail(
        broker_order_ref="13902598",
        reference_id=str(_REQUEST_ID),
        status="filled",
        broker_status="Filled",
        instrument_id=2449001,
        position_executions=(execution,),
        last_update=_NOW,
        raw_payload={"positionExecutions": [{"positionId": _POSITION_ID}]},
    )


def _opened_trade(
    conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> tuple[int, int, MagicMock, BrokerPosition]:
    signal_id = _seed(conn)
    broker = _broker()
    monkeypatch.setattr("app.services.strategy_order_reconciliation.uuid4", lambda: _REQUEST_ID)
    execution = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)
    assert execution.strategy_trade_id is not None and execution.order_id is not None
    broker.lookup_order.return_value = _order_detail()
    reconciled = reconcile_strategy_order(conn, broker=broker, order_id=execution.order_id)
    assert reconciled.state == "resolved"
    deployment_row = conn.execute(
        "SELECT deployment_id FROM strategy_funding_decisions WHERE signal_id=%s", (signal_id,)
    ).fetchone()
    assert deployment_row is not None
    deployment_id = int(deployment_row[0])
    configure_position_manager(
        conn,
        deployment_id=deployment_id,
        max_position_age_seconds=None,
        ratchet_variant_id=None,
        updated_by="test",
        reason="exact position lifecycle",
    )
    conn.commit()
    manual = _position(_MANUAL_POSITION_ID, stop=None, take=None)
    broker.get_portfolio.return_value = BrokerPortfolio(
        positions=(_position(_POSITION_ID, stop=None, take=None), manual),
        available_cash=Decimal("500"),
        raw_payload={},
    )
    edit_submission = BrokerPositionEditSubmission(
        _EDIT_OPERATION_ID,
        _POSITION_ID,
        _REQUEST_ID,
        {
            "operationId": str(_EDIT_OPERATION_ID),
            "positionId": _POSITION_ID,
            "referenceId": str(_REQUEST_ID),
        },
    )
    close_submission = BrokerPositionCloseSubmission(
        "24521234",
        _POSITION_ID,
        {"orderForClose": {"orderID": 24521234, "positionID": _POSITION_ID}},
    )

    def _edit_position(**kwargs: Any) -> BrokerPositionEditSubmission:
        kwargs["persist_response"](edit_submission.raw_payload)
        return edit_submission

    def _close_position(**kwargs: Any) -> BrokerPositionCloseSubmission:
        kwargs["persist_response"](close_submission.raw_payload)
        return close_submission

    broker.edit_demo_strategy_position.side_effect = _edit_position
    broker.close_demo_strategy_position.side_effect = _close_position
    return execution.strategy_trade_id, deployment_id, broker, manual


def test_unowned_position_fails_before_any_broker_io(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    trade_id, _, broker, _ = _opened_trade(ebull_test_conn, monkeypatch)

    with pytest.raises(StrategyOwnershipError, match="exact active"):
        manage_owned_position(
            ebull_test_conn,
            broker=broker,
            strategy_trade_id=trade_id,
            broker_position_id=_MANUAL_POSITION_ID,
            now=_NOW,
        )

    broker.get_portfolio.assert_not_called()
    broker.edit_demo_strategy_position.assert_not_called()
    broker.close_demo_strategy_position.assert_not_called()


def test_bigint_broker_position_id_uses_a_valid_advisory_lock(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, _, broker, _ = _opened_trade(conn, monkeypatch)
    bigint_position_id = 2**40 + _POSITION_ID
    conn.execute(
        "UPDATE strategy_position_ownership SET broker_position_id=%s WHERE broker_position_id=%s",
        (bigint_position_id, _POSITION_ID),
    )
    conn.commit()
    broker.get_portfolio.return_value = BrokerPortfolio(
        positions=(_position(bigint_position_id, stop=Decimal("95"), take=Decimal("110")),),
        available_cash=Decimal("500"),
        raw_payload={},
    )

    result = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=bigint_position_id,
        now=_NOW,
    )

    assert result.state == "no_change"
    assert result.reason_code == "position_protected"


def test_fixed_exit_gap_is_repaired_once_and_manual_position_is_untouched(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, _, broker, manual = _opened_trade(conn, monkeypatch)

    submitted = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=_POSITION_ID,
        now=_NOW,
    )

    assert submitted.state == "submitted"
    call = broker.edit_demo_strategy_position.call_args.kwargs
    assert call["position_id"] == _POSITION_ID
    assert call["stop_loss_rate"] == Decimal("95.000000")
    assert call["take_profit_rate"] == Decimal("110.000000")
    assert manual.stop_loss_rate is None and manual.take_profit_rate is None

    broker.get_portfolio.return_value = BrokerPortfolio(
        positions=(
            _position(_POSITION_ID, stop=Decimal("95.000000"), take=Decimal("110.000000")),
            manual,
        ),
        available_cash=Decimal("500"),
        raw_payload={},
    )
    applied = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=_POSITION_ID,
        now=_NOW,
    )

    assert applied.state == "applied"
    broker.edit_demo_strategy_position.assert_called_once()
    assert conn.execute("SELECT count(*) FROM strategy_position_operations").fetchone() == (1,)
    assert conn.execute("SELECT status FROM strategy_position_operations").fetchone() == ("applied",)
    assert conn.execute("SELECT broker_response_json->>'operationId' FROM strategy_position_operations").fetchone() == (
        str(_EDIT_OPERATION_ID),
    )


def test_rejected_patch_is_a_single_material_event_not_a_retry_heap(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, _, broker, _ = _opened_trade(conn, monkeypatch)
    broker.edit_demo_strategy_position.side_effect = BrokerPositionMutationError("rejected")

    first = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=_POSITION_ID,
        now=_NOW,
    )
    second = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=_POSITION_ID,
        now=_NOW,
    )

    assert first.state == second.state == "rejected"
    broker.edit_demo_strategy_position.assert_called_once()
    assert conn.execute("SELECT count(*) FROM strategy_position_operations").fetchone() == (1,)


def test_ratchet_formula_is_causal_and_never_worsens_the_stop() -> None:
    bar = RatchetBar(
        completed_at=datetime(2026, 8, 7, 20, tzinfo=UTC),
        level_known_at=datetime(2026, 8, 6, 20, tzinfo=UTC),
        close=Decimal("111"),
        highest_close_since_entry=Decimal("114"),
        atr=Decimal("4"),
        broken_resistance=Decimal("108"),
    )
    assert calculate_ratchet_stop(
        current_stop=Decimal("95"),
        bar=bar,
        break_atr_multiple=Decimal("0.5"),
        chandelier_atr_multiple=Decimal("3"),
        structure_atr_multiple=Decimal("1.5"),
    ) == Decimal("102.000000")
    assert (
        calculate_ratchet_stop(
            current_stop=Decimal("103"),
            bar=bar,
            break_atr_multiple=Decimal("0.5"),
            chandelier_atr_multiple=Decimal("3"),
            structure_atr_multiple=Decimal("1.5"),
        )
        is None
    )
    with pytest.raises(ValueError, match="not causal"):
        calculate_ratchet_stop(
            current_stop=Decimal("95"),
            bar=RatchetBar(
                completed_at=bar.completed_at,
                level_known_at=bar.completed_at + timedelta(seconds=1),
                close=bar.close,
                highest_close_since_entry=bar.highest_close_since_entry,
                atr=bar.atr,
                broken_resistance=bar.broken_resistance,
            ),
            break_atr_multiple=Decimal("0.5"),
            chandelier_atr_multiple=Decimal("3"),
            structure_atr_multiple=Decimal("1.5"),
        )


def test_ratchet_requires_a_registered_promoted_backtest_arm(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, deployment_id, broker, manual = _opened_trade(conn, monkeypatch)
    promotion_row = conn.execute(
        """
        SELECT p.promotion_id, r.position_rule_set_version
        FROM strategy_promotions p
        JOIN strategy_promotion_results pr ON pr.promotion_id=p.promotion_id
        JOIN strategy_results_store r ON r.result_id=pr.result_id
        WHERE p.strategy_id='S-ALLOC'
        """
    ).fetchone()
    assert promotion_row is not None
    promotion_id, rule_version = promotion_row
    variant_id = register_ratchet_variant(
        conn,
        strategy_id="S-ALLOC",
        strategy_version="v1",
        promotion_id=int(promotion_id),
        rule_version=str(rule_version),
        break_atr_multiple=Decimal("0.5"),
        chandelier_atr_multiple=Decimal("3"),
        structure_atr_multiple=Decimal("1.5"),
        registered_by="test",
        reason="separate promoted ratchet arm",
    )
    revision = configure_position_manager(
        conn,
        deployment_id=deployment_id,
        max_position_age_seconds=None,
        ratchet_variant_id=variant_id,
        updated_by="test",
        reason="enable only the registered arm",
    )
    assert revision == 2

    conn.execute(
        "UPDATE quotes SET bid=120, ask=121, last=120.5, quoted_at=%s WHERE instrument_id=2449001",
        (_NOW,),
    )
    conn.commit()
    broker.get_portfolio.return_value = BrokerPortfolio(
        positions=(
            _position(_POSITION_ID, stop=Decimal("95"), take=Decimal("110")),
            manual,
        ),
        available_cash=Decimal("500"),
        raw_payload={},
    )
    fired = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=_POSITION_ID,
        ratchet_bar=RatchetBar(
            completed_at=_NOW,
            level_known_at=_NOW - timedelta(days=1),
            close=Decimal("111"),
            highest_close_since_entry=Decimal("114"),
            atr=Decimal("4"),
            broken_resistance=Decimal("108"),
        ),
        now=_NOW,
    )
    assert fired.state == "submitted"
    assert broker.edit_demo_strategy_position.call_args.kwargs["stop_loss_rate"] == Decimal("102.000000")
    assert conn.execute(
        "SELECT operation_type, prior_stop_rate, desired_stop_rate FROM strategy_position_operations"
    ).fetchone() == ("stop_ratchet", Decimal("95.000000"), Decimal("102.000000"))

    with pytest.raises(StrategyPositionManagerError, match="backtest arm"):
        register_ratchet_variant(
            conn,
            strategy_id="S-ALLOC",
            strategy_version="v1",
            promotion_id=int(promotion_id),
            rule_version="unmeasured-rule",
            break_atr_multiple=Decimal("0.5"),
            chandelier_atr_multiple=Decimal("3"),
            structure_atr_multiple=Decimal("1.5"),
            registered_by="test",
            reason="must fail",
        )


def test_exact_close_remains_available_under_kill_switch_and_reconciles(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, _, broker, manual = _opened_trade(conn, monkeypatch)
    conn.execute("UPDATE kill_switch SET is_active=true WHERE id=true")
    conn.execute(
        """
        INSERT INTO strategy_execution_blocks (source,active,reason,blocked_at,cleared_at)
        VALUES ('drawdown',true,'simulated live kill drill',now(),NULL)
        """
    )
    conn.commit()

    submitted = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=_POSITION_ID,
        close_reason="emergency_risk",
        now=_NOW,
    )
    assert submitted.state == "submitted"
    assert broker.close_demo_strategy_position.call_args.kwargs["position_id"] == _POSITION_ID

    close_detail = BrokerCloseOrderDetail(
        broker_order_ref="24521234",
        status="filled",
        broker_status="1",
        position_ids=(_POSITION_ID,),
        reference_id=None,
        raw_payload={"orderID": 24521234, "statusID": 1, "positions": [{"positionID": _POSITION_ID}]},
    )
    broker.get_demo_close_order.side_effect = lambda **kwargs: (
        kwargs["persist_response"](close_detail.raw_payload) or close_detail
    )
    broker.get_portfolio.return_value = BrokerPortfolio(
        positions=(manual,), available_cash=Decimal("600"), raw_payload={}
    )
    applied = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=_POSITION_ID,
        now=_NOW,
    )
    assert applied.state == "applied"
    assert manual.position_id == _MANUAL_POSITION_ID
    assert conn.execute(
        "SELECT status, release_reason FROM strategy_position_ownership WHERE broker_position_id=%s",
        (_POSITION_ID,),
    ).fetchone() == ("released", "emergency_risk")
    assert conn.execute("SELECT status FROM strategy_trades WHERE strategy_trade_id=%s", (trade_id,)).fetchone() == (
        "closed",
    )
    assert conn.execute(
        "SELECT raw_payload_json->'orderForClose'->>'positionID' FROM orders "
        "WHERE execution_origin='strategy' AND action='EXIT'"
    ).fetchone() == (str(_POSITION_ID),)
    assert conn.execute("SELECT broker_response_json->>'orderID' FROM strategy_position_operations").fetchone() == (
        "24521234",
    )


def test_operator_close_records_its_reason_and_never_targets_manual_same_instrument_position(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, _, broker, manual = _opened_trade(conn, monkeypatch)

    result = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=_POSITION_ID,
        close_reason="operator_close",
        now=_NOW,
    )

    assert result.state == "submitted"
    assert broker.close_demo_strategy_position.call_args.kwargs["position_id"] == _POSITION_ID
    assert manual.position_id == _MANUAL_POSITION_ID
    assert conn.execute("SELECT operation_type, trigger_code FROM strategy_position_operations").fetchone() == (
        "close",
        "operator_close",
    )


def test_configured_timeout_submits_an_exact_position_close(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, deployment_id, broker, _ = _opened_trade(conn, monkeypatch)
    configure_position_manager(
        conn,
        deployment_id=deployment_id,
        max_position_age_seconds=60,
        ratchet_variant_id=None,
        updated_by="test",
        reason="short deterministic timeout",
    )
    conn.commit()

    result = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=_POSITION_ID,
        now=_NOW,
    )

    assert result.state == "submitted"
    assert broker.close_demo_strategy_position.call_args.kwargs["position_id"] == _POSITION_ID
    assert conn.execute("SELECT operation_type, trigger_code FROM strategy_position_operations").fetchone() == (
        "close",
        "timeout",
    )


def test_terminal_close_failure_can_be_retried_as_a_new_audited_intent(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, _, broker, _ = _opened_trade(conn, monkeypatch)
    broker.close_demo_strategy_position.side_effect = [
        BrokerPositionMutationError("temporary rejection"),
        BrokerPositionCloseSubmission(
            "24521235",
            _POSITION_ID,
            {"orderForClose": {"orderID": 24521235, "positionID": _POSITION_ID}},
        ),
    ]

    rejected = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=_POSITION_ID,
        close_reason="emergency_risk",
        now=_NOW,
    )
    retried = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=_POSITION_ID,
        close_reason="emergency_risk",
        now=_NOW + timedelta(seconds=1),
    )

    assert rejected.state == "rejected"
    assert retried.state == "submitted"
    assert broker.close_demo_strategy_position.call_count == 2
    assert conn.execute(
        "SELECT status FROM strategy_position_operations ORDER BY position_operation_id"
    ).fetchall() == [("rejected",), ("submitted",)]


def test_restart_with_unidentified_intent_fails_closed_without_retrying_broker_write(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, _, broker, _ = _opened_trade(conn, monkeypatch)
    ownership_row = conn.execute(
        "SELECT ownership_id FROM strategy_position_ownership WHERE broker_position_id=%s", (_POSITION_ID,)
    ).fetchone()
    assert ownership_row is not None
    ownership_id = int(ownership_row[0])
    conn.execute(
        """
        INSERT INTO strategy_position_operations (
            ownership_id, operation_type, trigger_code, request_id, status,
            prior_stop_rate, desired_stop_rate, desired_take_profit_rate
        ) VALUES (%s,'fixed_exit_repair','entry_exit_gap',%s,'intent_persisted',NULL,95,110)
        """,
        (ownership_id, UUID("f95eab17-c3ac-4948-a281-d94fd1e2764b")),
    )
    conn.commit()

    result = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=_POSITION_ID,
        now=_NOW,
    )

    assert result.state == "reconcile_required"
    broker.edit_demo_strategy_position.assert_not_called()
    broker.close_demo_strategy_position.assert_not_called()
    assert conn.execute("SELECT status FROM strategy_trades WHERE strategy_trade_id=%s", (trade_id,)).fetchone() == (
        "reconcile_required",
    )
