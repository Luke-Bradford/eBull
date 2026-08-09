"""#2454 strategy governance and exact-position ownership integration tests."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services.strategy_control_plane import (
    StrategyControlError,
    StrategyOwnershipError,
    assert_exact_position_owned,
    claim_exact_position,
    configure_deployment,
    create_strategy_trade,
    current_stage,
    decide_funding,
    link_strategy_order,
    promote_strategy,
    record_order_position_execution,
    release_exact_position,
)

pytestmark = pytest.mark.integration


def _instrument(conn: psycopg.Connection[Any], instrument_id: int = 2454001) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)
        VALUES (%s, %s, %s, true)
        """,
        (instrument_id, f"T{instrument_id}", f"Test {instrument_id}"),
    )


def _signal(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int = 2454001,
    strategy_id: str = "S-OWN",
    strategy_version: str = "v1",
) -> int:
    row = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id, strategy_version, instrument_id, signal_bar_date,
            signal_kind, verdict, fill_bar_date, fill_price, universe,
            input_rule_set_versions
        ) VALUES (%s, %s, %s, '2026-08-06', 'entry', 'fired',
                  '2026-08-07', 100, 'survivor_only',
                  '{"indicator_series":"rules-v1"}'::jsonb)
        RETURNING signal_id
        """,
        (strategy_id, strategy_version, instrument_id),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _paper_stage(conn: psycopg.Connection[Any]) -> None:
    """Seed an already-audited chain; transition mechanics are tested separately."""
    conn.execute(
        """
        INSERT INTO strategy_promotions (
            strategy_id, strategy_version, from_stage, to_stage, gate_version,
            evidence_ref, promoted_by, reason
        ) VALUES
          ('S-OWN', 'v1', NULL, 'research_candidate', 'test-v1', NULL, 'test', 'registered'),
          ('S-OWN', 'v1', 'research_candidate', 'historical_validated', 'test-v1', 'e:hist', 'test', 'validated'),
          ('S-OWN', 'v1', 'historical_validated', 'forward_observation', 'test-v1', 'e:fwd', 'test', 'observe'),
          ('S-OWN', 'v1', 'forward_observation', 'paper_enabled', 'test-v1', 'e:paper', 'test', 'paper')
        """
    )


def _deployment_and_trade(conn: psycopg.Connection[Any], signal_id: int) -> int:
    deployment = configure_deployment(
        conn,
        strategy_id="S-OWN",
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal("1000"),
        enabled=True,
        changed_by="operator",
        reason="test allocation",
    )
    decision_id = decide_funding(
        conn,
        signal_id=signal_id,
        verdict="allocated",
        deployment_id=deployment.deployment_id,
        amount=Decimal("100"),
        reason_code="within_risk_budget",
    )
    return create_strategy_trade(conn, decision_id)


def _order(conn: psycopg.Connection[Any], *, instrument_id: int, origin: str = "strategy") -> int:
    row = conn.execute(
        """
        INSERT INTO orders (
            instrument_id, action, order_type, requested_amount, status,
            execution_origin
        ) VALUES (%s, 'BUY', 'MARKET', 100, 'filled', %s)
        RETURNING order_id
        """,
        (instrument_id, origin),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _position(conn: psycopg.Connection[Any], position_id: int, instrument_id: int) -> None:
    conn.execute(
        """
        INSERT INTO broker_positions (
            position_id, instrument_id, is_buy, units, amount,
            initial_amount_in_dollars, open_rate, open_conversion_rate,
            open_date_time, raw_payload
        ) VALUES (%s, %s, true, 1, 100, 100, 100, 1, now(), '{}'::jsonb)
        """,
        (position_id, instrument_id),
    )


def test_promotion_is_ordered_explicit_and_evidenced(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    first = promote_strategy(
        conn,
        strategy_id="S-GOV",
        strategy_version="v1",
        to_stage="research_candidate",
        promoted_by="operator",
        reason="register preregistered candidate",
    )
    assert first.from_stage is None
    assert current_stage(conn, "S-GOV", "v1") == "research_candidate"

    with pytest.raises(StrategyControlError, match="invalid promotion transition"):
        promote_strategy(
            conn,
            strategy_id="S-GOV",
            strategy_version="v1",
            to_stage="paper_enabled",
            promoted_by="operator",
            reason="skip evidence",
            evidence_ref="invalid:skip",
        )
    with pytest.raises(StrategyControlError, match="requires an immutable evidence_ref"):
        promote_strategy(
            conn,
            strategy_id="S-GOV",
            strategy_version="v1",
            to_stage="historical_validated",
            promoted_by="operator",
            reason="missing evidence",
        )

    # Runtime switches are deliberately irrelevant to governance state.
    conn.execute("UPDATE runtime_config SET enable_auto_trading = true, enable_live_trading = true WHERE id = true")
    assert current_stage(conn, "S-GOV", "v1") == "research_candidate"


def test_deployment_has_one_current_row_and_complete_history(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    _paper_stage(conn)
    first = configure_deployment(
        conn,
        strategy_id="S-OWN",
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal("1000"),
        enabled=True,
        changed_by="operator",
        reason="initial paper pot",
    )
    second = configure_deployment(
        conn,
        strategy_id="S-OWN",
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal("750"),
        enabled=False,
        changed_by="operator",
        reason="pause allocation",
    )
    assert second.deployment_id == first.deployment_id
    assert second.revision == 2
    assert conn.execute(
        "SELECT count(*) FROM strategy_deployments WHERE strategy_id='S-OWN' AND mode='paper'"
    ).fetchone() == (1,)
    assert conn.execute(
        "SELECT revision, capital_limit, enabled FROM strategy_deployment_events ORDER BY revision"
    ).fetchall() == [(1, Decimal("1000.000000"), True), (2, Decimal("750.000000"), False)]


def test_same_instrument_manual_position_is_never_inferred_as_owned(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    instrument_id = 2454001
    _instrument(conn, instrument_id)
    signal_id = _signal(conn, instrument_id=instrument_id)
    _paper_stage(conn)
    trade_id = _deployment_and_trade(conn, signal_id)

    manual_position_id = 900001
    strategy_position_id = 900002
    second_strategy_position_id = 900003
    _position(conn, manual_position_id, instrument_id)
    _position(conn, strategy_position_id, instrument_id)
    _position(conn, second_strategy_position_id, instrument_id)
    entry_order = _order(conn, instrument_id=instrument_id)
    link_strategy_order(conn, strategy_trade_id=trade_id, order_id=entry_order, purpose="entry")

    # Instrument equality is not provenance: the pre-existing manual position
    # cannot be claimed because detailed lookup did not return it for this order.
    with pytest.raises(StrategyOwnershipError, match="exact strategy entry order"):
        claim_exact_position(
            conn,
            strategy_trade_id=trade_id,
            entry_order_id=entry_order,
            broker_position_id=manual_position_id,
        )

    record_order_position_execution(conn, order_id=entry_order, broker_position_id=strategy_position_id)
    record_order_position_execution(conn, order_id=entry_order, broker_position_id=second_strategy_position_id)
    claim_exact_position(
        conn,
        strategy_trade_id=trade_id,
        entry_order_id=entry_order,
        broker_position_id=strategy_position_id,
    )
    # One entry order may produce several positionExecutions. Each exact id is
    # owned independently while the same-instrument manual id remains outside.
    claim_exact_position(
        conn,
        strategy_trade_id=trade_id,
        entry_order_id=entry_order,
        broker_position_id=second_strategy_position_id,
    )

    assert_exact_position_owned(conn, strategy_trade_id=trade_id, broker_position_id=strategy_position_id)
    assert_exact_position_owned(
        conn,
        strategy_trade_id=trade_id,
        broker_position_id=second_strategy_position_id,
    )
    with pytest.raises(StrategyOwnershipError, match="not actively owned"):
        assert_exact_position_owned(conn, strategy_trade_id=trade_id, broker_position_id=manual_position_id)

    release_exact_position(
        conn,
        strategy_trade_id=trade_id,
        broker_position_id=strategy_position_id,
        reason="paper position closed",
    )
    with pytest.raises(StrategyOwnershipError, match="not actively owned"):
        assert_exact_position_owned(conn, strategy_trade_id=trade_id, broker_position_id=strategy_position_id)


def test_manual_order_cannot_become_strategy_authority(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    instrument_id = 2454002
    _instrument(conn, instrument_id)
    signal_id = _signal(conn, instrument_id=instrument_id)
    _paper_stage(conn)
    trade_id = _deployment_and_trade(conn, signal_id)
    manual_order = _order(conn, instrument_id=instrument_id, origin="manual")

    with pytest.raises(StrategyControlError, match="manual orders cannot be linked"):
        link_strategy_order(conn, strategy_trade_id=trade_id, order_id=manual_order, purpose="entry")


def test_funding_is_once_only_and_cannot_exceed_operator_cap(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    _instrument(conn)
    signal_id = _signal(conn)
    _paper_stage(conn)
    deployment = configure_deployment(
        conn,
        strategy_id="S-OWN",
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal("100"),
        enabled=True,
        changed_by="operator",
        reason="bounded test pot",
    )
    with pytest.raises(StrategyControlError, match="exceeds"):
        decide_funding(
            conn,
            signal_id=signal_id,
            verdict="allocated",
            deployment_id=deployment.deployment_id,
            amount=Decimal("101"),
            reason_code="invalid",
        )
    first = decide_funding(
        conn,
        signal_id=signal_id,
        verdict="rejected",
        reason_code="risk_budget_exhausted",
    )
    assert first > 0
    with pytest.raises(psycopg.errors.UniqueViolation):
        decide_funding(
            conn,
            signal_id=signal_id,
            verdict="rejected",
            reason_code="duplicate",
        )


def test_funding_rechecks_stage_and_aggregate_active_reservations(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    for instrument_id in (2454011, 2454012, 2454013):
        _instrument(conn, instrument_id)
    _paper_stage(conn)
    deployment = configure_deployment(
        conn,
        strategy_id="S-OWN",
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal("100"),
        enabled=True,
        changed_by="operator",
        reason="aggregate reservation test",
    )
    first_signal = _signal(conn, instrument_id=2454011)
    first_decision = decide_funding(
        conn,
        signal_id=first_signal,
        verdict="allocated",
        deployment_id=deployment.deployment_id,
        amount=Decimal("60"),
        reason_code="within_risk_budget",
    )
    first_trade = create_strategy_trade(conn, first_decision)

    second_signal = _signal(conn, instrument_id=2454012)
    with pytest.raises(StrategyControlError, match="exceeds"):
        decide_funding(
            conn,
            signal_id=second_signal,
            verdict="allocated",
            deployment_id=deployment.deployment_id,
            amount=Decimal("41"),
            reason_code="would_exceed_active_reservations",
        )

    # Closed/failed trades release capacity; lifetime allocations do not make
    # an operator's fixed pot unusable forever.
    conn.execute(
        "UPDATE strategy_trades SET status = 'closed' WHERE strategy_trade_id = %s",
        (first_trade,),
    )
    second_decision = decide_funding(
        conn,
        signal_id=second_signal,
        verdict="allocated",
        deployment_id=deployment.deployment_id,
        amount=Decimal("100"),
        reason_code="capacity_released",
    )
    assert second_decision > first_decision

    promote_strategy(
        conn,
        strategy_id="S-OWN",
        strategy_version="v1",
        to_stage="paused",
        promoted_by="operator",
        reason="pause new entries",
    )
    third_signal = _signal(conn, instrument_id=2454013)
    with pytest.raises(StrategyControlError, match="cannot be allocated"):
        decide_funding(
            conn,
            signal_id=third_signal,
            verdict="allocated",
            deployment_id=deployment.deployment_id,
            amount=Decimal("1"),
            reason_code="must_fail_while_paused",
        )
