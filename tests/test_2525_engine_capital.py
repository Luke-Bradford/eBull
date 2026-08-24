"""Exact broker join for the engine-wide assigned-capital boundary (#2525)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.providers.broker import BrokerAccountRiskSnapshot, BrokerDirectPositionInvestment
from app.services.strategy_control_plane import configure_paper_pool
from app.services.strategy_core_allocator import CoreSleeveState
from app.services.strategy_core_mandate import CORE_MANDATE_POLICY_VERSION
from app.services.strategy_core_rebalance_intent import record_core_rebalance_intent
from app.services.strategy_engine_capital import (
    EngineCapitalAuthority,
    EngineCapitalObservationError,
    load_engine_capital_authority,
    resolve_engine_capital_usage,
)

NOW = datetime(2026, 8, 24, tzinfo=UTC)


def authority(*, ids: tuple[int, ...] = (11,), alpha: str = "200", pending: str = "100") -> EngineCapitalAuthority:
    return EngineCapitalAuthority(
        pool_event_id=1,
        enabled=True,
        capital_limit=Decimal("1000"),
        capital_mode="fixed",
        epoch_started_at=NOW,
        realised_delta=Decimal("0"),
        alpha_committed=Decimal(alpha),
        alpha_working=Decimal("150"),
        core_pending_committed=Decimal(pending),
        core_active_recorded_committed=Decimal("300") if ids else Decimal("0"),
        core_active_position_ids=ids,
    )


def position(
    position_id: int,
    *,
    instrument_id: int = 42,
    amount: str = "300",
    market_value: str = "330",
    is_buy: bool = True,
    partial: bool = False,
) -> BrokerDirectPositionInvestment:
    return BrokerDirectPositionInvestment(
        position_id=position_id,
        instrument_id=instrument_id,
        is_buy=is_buy,
        amount=Decimal(amount),
        unrealized_pnl=Decimal(market_value) - Decimal(amount),
        market_value=Decimal(market_value),
        is_partially_altered=partial,
    )


def snapshot(
    *rows: BrokerDirectPositionInvestment,
    account_currency_id: int | None = 1,
) -> BrokerAccountRiskSnapshot:
    return BrokerAccountRiskSnapshot(
        available_cash=Decimal("5000"),
        total_invested=Decimal("9000"),
        unrealized_pnl=Decimal("0"),
        equity=Decimal("14000"),
        instrument_investments=(),
        observed_at=NOW,
        raw_payload={},
        direct_positions=rows,
        account_currency_id=account_currency_id,
    )


def test_only_exact_owned_positions_enter_the_shared_boundary() -> None:
    usage = resolve_engine_capital_usage(
        authority(),
        snapshot(position(11), position(99, amount="8000", market_value="9000")),
        core_instrument_id=42,
    )

    assert usage.core_active_committed == Decimal("300")
    assert usage.core_market_value == Decimal("330")
    assert usage.committed == Decimal("600")
    assert usage.working == Decimal("450")
    assert usage.headroom.remaining == Decimal("400")


@pytest.mark.parametrize(
    ("rows", "instrument_id", "message"),
    [
        ((), 42, "absent"),
        ((position(11, instrument_id=43),), 42, "another instrument"),
        ((position(11, is_buy=False),), 42, "short"),
        ((position(11, partial=True),), 42, "partially altered"),
        ((position(11),), None, "no configured instrument"),
    ],
)
def test_an_inexact_active_core_population_refuses(
    rows: tuple[BrokerDirectPositionInvestment, ...], instrument_id: int | None, message: str
) -> None:
    with pytest.raises(EngineCapitalObservationError, match=message):
        resolve_engine_capital_usage(authority(), snapshot(*rows), core_instrument_id=instrument_id)


def test_no_core_positions_need_no_core_mandate() -> None:
    usage = resolve_engine_capital_usage(authority(ids=()), snapshot(position(99)), core_instrument_id=None)
    assert usage.core_active_committed == 0
    assert usage.committed == Decimal("300")


@pytest.mark.parametrize("account_currency_id", [None, 2])
def test_shared_boundary_refuses_a_snapshot_not_observed_as_usd(account_currency_id: int | None) -> None:
    with pytest.raises(EngineCapitalObservationError, match="not observed as USD"):
        resolve_engine_capital_usage(
            authority(ids=()),
            snapshot(account_currency_id=account_currency_id),
            core_instrument_id=None,
        )


def test_database_authority_begins_at_the_first_assigned_pool_event(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    assert load_engine_capital_authority(conn) is None

    pool = configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("1250"),
        capital_mode="fixed",
        risk_profile="balanced",
        approval_mode="manual",
        changed_by="operator",
        reason="assign the engine pot",
    )
    loaded = load_engine_capital_authority(conn)

    assert loaded is not None
    assert loaded.pool_event_id == pool.event_id
    assert loaded.capital_limit == Decimal("1250")
    assert loaded.alpha_committed == 0
    assert loaded.core_pending_committed == 0
    assert loaded.core_active_recorded_committed == 0
    assert loaded.core_active_position_ids == ()


def test_a_pending_allocation_before_the_assigned_pot_refuses_instead_of_disappearing(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (42,'ALPHA.PRE','Pre-pot allocation',TRUE)"
    )
    signal = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id,strategy_version,instrument_id,signal_bar_date,
            signal_kind,verdict,fill_bar_date,fill_price,universe,input_rule_set_versions
        ) VALUES ('pre_pot','v1',42,DATE '2026-08-20','entry','fired',
                  DATE '2026-08-21',10,'survivor_only','{"test":"v1"}'::jsonb)
        RETURNING signal_id
        """
    ).fetchone()
    deployment = conn.execute(
        """
        INSERT INTO strategy_deployments (
            strategy_id,strategy_version,mode,capital_limit,currency,enabled,updated_by,reason
        ) VALUES ('pre_pot','v1','paper',1000,'USD',TRUE,'test','test')
        RETURNING deployment_id
        """
    ).fetchone()
    assert signal is not None and deployment is not None
    conn.execute(
        """
        INSERT INTO strategy_funding_decisions (
            signal_id,deployment_id,verdict,amount,reason_code,decided_at
        ) VALUES (%s,%s,'allocated',100,'pre_pot',now() - interval '1 hour')
        """,
        (signal[0], deployment[0]),
    )
    configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("1250"),
        capital_mode="fixed",
        risk_profile="balanced",
        approval_mode="manual",
        changed_by="operator",
        reason="assign the engine pot",
    )

    with pytest.raises(EngineCapitalObservationError, match="predates the assigned pot"):
        load_engine_capital_authority(conn)


def test_a_trade_backed_by_a_non_actionable_core_intent_refuses_instead_of_disappearing(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("1250"),
        capital_mode="fixed",
        risk_profile="balanced",
        approval_mode="manual",
        changed_by="operator",
        reason="assign the engine pot",
    )
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (42,'CORE.BAD','Malformed core authority',TRUE)"
    )
    row = conn.execute(
        """
        INSERT INTO strategy_core_mandate_events (
            revision,enabled,base_currency,core_instrument_id,core_target_pct,
            liquidity_reserve_pct,rebalance_band_pct,min_rebalance_amount,
            policy_version,changed_by,reason,mode
        ) VALUES (1,FALSE,'USD',42,60,5,5,10,%s,'test','test','paper')
        RETURNING core_mandate_event_id
        """,
        (CORE_MANDATE_POLICY_VERSION,),
    ).fetchone()
    assert row is not None
    intent = record_core_rebalance_intent(
        conn,
        state=CoreSleeveState(42, Decimal("600"), Decimal("400"), "USD", NOW),
        recorded_by="test",
    )
    assert intent.decision.action == "refused"
    conn.execute(
        "INSERT INTO strategy_trades (core_rebalance_intent_id,instrument_id,status) VALUES (%s,42,'planned')",
        (intent.core_rebalance_intent_id,),
    )

    with pytest.raises(EngineCapitalObservationError, match="entry authority is incomplete"):
        load_engine_capital_authority(conn)
