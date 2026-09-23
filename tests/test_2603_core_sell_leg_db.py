"""#2603 sell leg — a ``sell_core`` becomes a WHOLE close through the position manager.

Spec: ``docs/proposals/ta/2026-09-23-core-sell-leg-close-rebuy.md`` (revision 11).

The world is #2949's file-backed broker, driven in-process: a real core buy is placed,
reconciled and owned, the mandate is then tightened so the holding sits above
``upper``, and the POSTs that follow go through ``execute_core_rebalance`` unchanged.

⚠ No broker mutation.  The double is file-backed, the database is a disposable
per-worker one, and no credential is decrypted.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import psycopg
import pytest

from app.providers.broker import BrokerProvider
from app.services.strategy_core_executor import CoreExecutionResult, execute_core_rebalance
from app.services.strategy_core_mandate import CORE_MANDATE_MODE, CORE_MANDATE_POLICY_VERSION
from app.services.strategy_core_submission_gate import core_submission_lock
from app.services.strategy_order_reconciliation import reconcile_backlog
from app.services.strategy_position_manager import StrategyPositionManagerError, manage_owned_position
from tests.fixtures.core_restart import (
    API_CREDENTIAL_ID,
    CLOCK,
    CORE_INSTRUMENT_ID,
    OPERATOR_ID,
    USER_CREDENTIAL_ID,
    FileBackedFakeBroker,
    close_state_report,
    core_ownership_coordinates,
    seed_core_execution_world,
    select_core_instrument,
)


@pytest.fixture
def broker(
    ebull_test_conn: psycopg.Connection[Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> FileBackedFakeBroker:
    from app.services import strategy_core_selection

    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_OUTCOME", None, raising=False)
    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_INSTRUMENT_ID", None, raising=False)
    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_EVIDENCE_REF", None, raising=False)
    select_core_instrument()
    seed_core_execution_world(ebull_test_conn)
    return FileBackedFakeBroker(tmp_path / "broker.json")


def _post(conn: psycopg.Connection[Any], broker: FileBackedFakeBroker) -> CoreExecutionResult:
    return execute_core_rebalance(
        conn,
        broker=cast(BrokerProvider, broker),
        operator_id=OPERATOR_ID,
        api_key_credential_id=API_CREDENTIAL_ID,
        user_key_credential_id=USER_CREDENTIAL_ID,
        recorded_by="sell-leg-test",
        clock=lambda: CLOCK,
    )


def _held_advisory_locks(conn: psycopg.Connection[Any]) -> int:
    row = conn.execute(
        "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND granted"
    ).fetchone()
    conn.commit()
    assert row is not None
    return int(row[0])


def _tighten_mandate(conn: psycopg.Connection[Any], *, target: int, band: int) -> None:
    """Append revision 2 so the owned holding sits ABOVE ``upper``."""
    conn.execute(
        """
        INSERT INTO strategy_core_mandate_events (
            revision, enabled, base_currency, core_instrument_id, core_target_pct,
            liquidity_reserve_pct, rebalance_band_pct, min_rebalance_amount,
            policy_version, changed_by, reason, mode
        ) VALUES (2, TRUE, 'USD', %s, %s, 5, %s, 25, %s, 'sell-leg-test', '#2603 sell leg', %s)
        """,
        (CORE_INSTRUMENT_ID, target, band, CORE_MANDATE_POLICY_VERSION, CORE_MANDATE_MODE),
    )
    conn.commit()


def _own_core_position(conn: psycopg.Connection[Any], broker: FileBackedFakeBroker) -> tuple[int, int]:
    bought = _post(conn, broker)
    assert bought.state == "submitted", bought
    assert [r.state for r in reconcile_backlog(conn, broker=cast(BrokerProvider, broker), limit=20)] == ["resolved"]
    return core_ownership_coordinates(conn)


def _scalar(conn: psycopg.Connection[Any], sql: str, params: tuple[Any, ...] = ()) -> Any:
    row = conn.execute(sql, params).fetchone()  # type: ignore[call-overload]
    conn.commit()
    assert row is not None
    return row[0]


def test_a_sell_closes_the_whole_position_and_the_next_post_finishes_it(
    ebull_test_conn: psycopg.Connection[Any], broker: FileBackedFakeBroker
) -> None:
    conn = ebull_test_conn
    _own_core_position(conn, broker)
    _tighten_mandate(conn, target=30, band=5)

    sold = _post(conn, broker)

    assert (sold.state, sold.reason_code) == ("submitted", "core_rebalance_close_submitted")
    assert sold.intent_id is not None
    assert _scalar(
        conn, "SELECT action FROM strategy_core_rebalance_intents WHERE core_rebalance_intent_id=%s", (sold.intent_id,)
    ) == ("sell_core")
    # The quote is recorded against the intent, bound to the position it priced.
    quoted = conn.execute(
        "SELECT broker_position_id, cost_upper_bound FROM strategy_core_rebalance_close_quotes "
        "WHERE core_rebalance_intent_id=%s",
        (sold.intent_id,),
    ).fetchone()
    conn.commit()
    assert quoted is not None
    assert int(quoted[0]) == core_ownership_coordinates(conn)[1]
    report = close_state_report(conn)
    assert report["close_statuses"] == ["submitted"]
    assert broker.read()["close_calls"] == 1
    # One BUY only: the sell never reached the entry verb.
    assert broker.read()["mutation_calls"] == 1
    assert _held_advisory_locks(conn) == 0

    # Step 0: the second POST drives the in-flight close with no new evaluation.
    intents_before = _scalar(conn, "SELECT count(*) FROM strategy_core_rebalance_intents")
    finished = _post(conn, broker)

    assert (finished.state, finished.reason_code) == ("closed", "core_rebalance_close_applied")
    assert finished.intent_id == sold.intent_id
    assert _scalar(conn, "SELECT count(*) FROM strategy_core_rebalance_intents") == intents_before
    report = close_state_report(conn)
    assert report["close_statuses"] == ["applied"]
    assert report["active_ownership"] == 0
    assert report["release_reasons"] == ["core_rebalance"]
    assert broker.read()["close_calls"] == 1
    assert _held_advisory_locks(conn) == 0


def test_a_rebalance_close_link_and_its_evidence_are_permanent(
    ebull_test_conn: psycopg.Connection[Any], broker: FileBackedFakeBroker
) -> None:
    conn = ebull_test_conn
    _own_core_position(conn, broker)
    _tighten_mandate(conn, target=30, band=5)
    sold = _post(conn, broker)
    assert sold.state == "submitted"
    operation_id, ownership_id = conn.execute(
        "SELECT position_operation_id, ownership_id FROM strategy_position_operations "
        "WHERE core_rebalance_intent_id=%s",
        (sold.intent_id,),
    ).fetchone() or (None, None)
    conn.commit()
    assert operation_id is not None

    frozen = [
        (
            "UPDATE strategy_position_operations SET ownership_id=ownership_id+0, trigger_code='operator_close' "
            "WHERE position_operation_id=%s",
            (operation_id,),
        ),
        ("DELETE FROM strategy_position_operations WHERE position_operation_id=%s", (operation_id,)),
        (
            "UPDATE strategy_core_rebalance_intents SET recorded_by='x' WHERE core_rebalance_intent_id=%s",
            (sold.intent_id,),
        ),
        ("DELETE FROM strategy_core_rebalance_close_quotes WHERE core_rebalance_intent_id=%s", (sold.intent_id,)),
        ("UPDATE strategy_trades SET instrument_id=instrument_id+1 WHERE core_rebalance_intent_id IS NOT NULL", ()),
        (
            "UPDATE strategy_position_ownership SET broker_position_id=broker_position_id+1 WHERE ownership_id=%s",
            (ownership_id,),
        ),
    ]
    for statement, params in frozen:
        with pytest.raises(psycopg.errors.RaiseException):
            conn.execute(statement, params)  # type: ignore[call-overload]
        conn.rollback()


def test_a_link_to_an_unquoted_or_non_sell_intent_is_refused(
    ebull_test_conn: psycopg.Connection[Any], broker: FileBackedFakeBroker
) -> None:
    conn = ebull_test_conn
    _own_core_position(conn, broker)
    buy_intent = _scalar(
        conn, "SELECT core_rebalance_intent_id FROM strategy_trades WHERE core_rebalance_intent_id IS NOT NULL"
    )
    ownership_id = _scalar(conn, "SELECT ownership_id FROM strategy_position_ownership WHERE status='active'")
    order_id = _scalar(
        conn,
        "INSERT INTO orders (instrument_id, action, order_type, status, execution_origin) "
        "VALUES (%s, 'EXIT', 'MARKET', 'submitted', 'strategy') RETURNING order_id",
        (CORE_INSTRUMENT_ID,),
    )
    insert = (
        "INSERT INTO strategy_position_operations (ownership_id, order_id, operation_type, trigger_code, "
        "request_id, status, core_rebalance_intent_id) VALUES (%s, %s, 'close', %s, gen_random_uuid(), "
        "'intent_persisted', %s)"
    )
    with pytest.raises(psycopg.errors.RaiseException):
        conn.execute(insert, (ownership_id, order_id, "core_rebalance", buy_intent))
    conn.rollback()
    # The link and the trigger code go together.
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(insert, (ownership_id, order_id, "core_rebalance", None))
    conn.rollback()


def test_the_manager_refuses_a_rebalance_close_outside_the_core_hold(
    ebull_test_conn: psycopg.Connection[Any], broker: FileBackedFakeBroker
) -> None:
    conn = ebull_test_conn
    trade_id, position_id = _own_core_position(conn, broker)
    ownership_id = _scalar(conn, "SELECT ownership_id FROM strategy_position_ownership WHERE status='active'")

    def close(**kwargs: Any) -> Any:
        return manage_owned_position(
            conn,
            broker=cast(BrokerProvider, broker),
            strategy_trade_id=trade_id,
            broker_position_id=position_id,
            now=CLOCK,
            **kwargs,
        )

    with pytest.raises(StrategyPositionManagerError, match="core_submission_lock"):
        close(close_reason="core_rebalance", core_rebalance_intent_id=1, expected_ownership_id=ownership_id)
    with pytest.raises(StrategyPositionManagerError, match="go together"):
        close(close_reason="operator_close", core_rebalance_intent_id=1)
    with pytest.raises(StrategyPositionManagerError, match="go together"):
        close(close_reason="core_rebalance")
    # The wrong ownership never closes, even inside the hold.
    with core_submission_lock(conn):
        result = close(close_reason=None, expected_ownership_id=ownership_id + 1)
    assert (result.state, result.reason_code) == ("rejected", "core_rebalance_ownership_mismatch")
    assert broker.read()["close_calls"] == 0
    assert _held_advisory_locks(conn) == 0


def test_an_unresolved_rebalance_close_quarantines_every_core_trade(
    ebull_test_conn: psycopg.Connection[Any], broker: FileBackedFakeBroker
) -> None:
    """An uncertain close stands as ``reconcile_required``: no buy and no sell follow it."""
    conn = ebull_test_conn
    _own_core_position(conn, broker)
    _tighten_mandate(conn, target=30, band=5)
    broker.close_failure = "uncertain_not_taken"

    sold = _post(conn, broker)

    assert (sold.state, sold.reason_code) == ("reconcile_required", "broker_close_uncertain")
    assert close_state_report(conn)["close_statuses"] == ["reconcile_required"]
    again = _post(conn, broker)
    # Admission's in-flight blocker fires first (the exit order has no reconciliation
    # row and the trade is not terminal); the DB preflight's `core_operation_outstanding`
    # is the second, independent refusal, pinned in `test_2603_core_sell_leg.py`.
    assert (again.state, again.reason_code) == ("refused", "core_trade_in_flight")
    # `uncertain_not_taken`: the broker never recorded the close, which our side cannot tell.
    assert broker.read()["close_calls"] == 0
    assert broker.read()["mutation_calls"] == 1
    assert _held_advisory_locks(conn) == 0
