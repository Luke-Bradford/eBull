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
        alpha_committed_by_instrument=((7, Decimal(alpha)),),
        core_pending_by_instrument=((42, Decimal(pending)),),
    )


def position(
    position_id: int,
    *,
    instrument_id: int = 42,
    amount: str = "300",
    market_value: str = "330",
    is_buy: bool = True,
    partial: bool = False,
    units: str = "10",
) -> BrokerDirectPositionInvestment:
    return BrokerDirectPositionInvestment(
        position_id=position_id,
        instrument_id=instrument_id,
        is_buy=is_buy,
        units=Decimal(units),
        amount=Decimal(amount),
        unrealized_pnl=Decimal(market_value) - Decimal(amount),
        market_value=Decimal(market_value),
        is_partially_altered=partial,
        close_rate=Decimal(market_value) / Decimal(units),
        close_conversion_rate=Decimal("1"),
        asset_currency_id=1,
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


def test_committed_in_splits_committed_by_instrument_without_loss() -> None:
    # #3541 slice 2: the per-instrument exposure cap's numerator. Core active counts only
    # under the core instrument; the split sums back to `committed`.
    usage = resolve_engine_capital_usage(authority(), snapshot(position(11)), core_instrument_id=42)

    assert usage.committed_in(42) == Decimal("400")  # core pending 100 + core active 300
    assert usage.committed_in(7) == Decimal("200")  # alpha
    assert usage.committed_in(99) == Decimal("0")
    assert usage.committed_in(42) + usage.committed_in(7) == usage.committed


@pytest.mark.parametrize(
    ("rows", "instrument_id", "message", "code"),
    [
        ((), 42, "absent", "engine_capital_ownership_unwitnessed"),
        ((position(11, instrument_id=43),), 42, "another instrument", "engine_capital_ownership_mismatched"),
        ((position(11, is_buy=False),), 42, "short", "engine_capital_ownership_mismatched"),
        ((position(11, partial=True),), 42, "partially altered", "engine_capital_ownership_mismatched"),
        (
            (position(11, market_value="-1"),),
            42,
            "market value is invalid",
            "engine_capital_ownership_mismatched",
        ),
        (
            (position(11, amount="-5"),),
            42,
            "outside safe bounds",
            "engine_capital_ownership_mismatched",
        ),
        ((position(11), position(11)), 42, "repeats a direct position id", "engine_capital_snapshot_unusable"),
        ((position(11),), None, "no configured instrument", "engine_capital_snapshot_unusable"),
    ],
)
def test_an_inexact_active_core_population_refuses(
    rows: tuple[BrokerDirectPositionInvestment, ...], instrument_id: int | None, message: str, code: str
) -> None:
    """Each refusal keeps its identifying message AND carries a machine-readable code.

    The message is asserted as well as the code because the code is deliberately a
    bucket: only the message names the position id, and a caller that reports the code
    alone (``strategy_paper_executor``) logs the message for exactly that reason.

    ⚠ A malformed BROKER amount buckets as ``ownership_mismatched``, not as a population
    problem.  ``_money`` validates our stored amounts too, so the bucket is a parameter
    at the call site rather than a property of the helper.
    """
    with pytest.raises(EngineCapitalObservationError, match=message) as raised:
        resolve_engine_capital_usage(authority(), snapshot(*rows), core_instrument_id=instrument_id)
    assert raised.value.reason_code == code


def test_the_refusal_code_does_not_displace_the_message() -> None:
    """``str(exc)`` is unchanged by the added argument.

    Four callers surface or log it (``read_core_sleeve``'s blocker detail among them),
    so folding the code into ``RuntimeError.args`` would change operator-visible text
    everywhere at once.
    """
    error = EngineCapitalObservationError("a message", "engine_capital_population_incomplete")
    assert str(error) == "a message"
    assert error.args == ("a message",)
    assert error.reason_code == "engine_capital_population_incomplete"


def test_no_core_positions_need_no_core_mandate() -> None:
    usage = resolve_engine_capital_usage(authority(ids=()), snapshot(position(99)), core_instrument_id=None)
    assert usage.core_active_committed == 0
    assert usage.committed == Decimal("300")


@pytest.mark.parametrize("account_currency_id", [None, 2])
def test_shared_boundary_refuses_a_snapshot_not_observed_as_usd(account_currency_id: int | None) -> None:
    with pytest.raises(EngineCapitalObservationError, match="not observed as USD") as raised:
        resolve_engine_capital_usage(
            authority(ids=()),
            snapshot(account_currency_id=account_currency_id),
            core_instrument_id=None,
        )
    assert raised.value.reason_code == "engine_capital_snapshot_unusable"


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
        max_concurrent_positions_override=None,
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
        max_concurrent_positions_override=None,
        changed_by="operator",
        reason="assign the engine pot",
    )

    with pytest.raises(EngineCapitalObservationError, match="predates the assigned pot") as raised:
        load_engine_capital_authority(conn)
    assert raised.value.reason_code == "engine_capital_population_incomplete"


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
        max_concurrent_positions_override=None,
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

    with pytest.raises(EngineCapitalObservationError, match="entry authority is incomplete") as raised:
        load_engine_capital_authority(conn)
    assert raised.value.reason_code == "engine_capital_population_incomplete"


def _alpha_allocation(
    conn: psycopg.Connection[Any], *, deployment_id: int, signal_instrument: int, amount: str, day: int
) -> int:
    signal = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id,strategy_version,instrument_id,signal_bar_date,
            signal_kind,verdict,fill_bar_date,fill_price,universe,input_rule_set_versions
        ) VALUES ('split','v1',%s,DATE '2026-08-20' + %s,'entry','fired',
                  DATE '2026-08-21' + %s,10,'survivor_only','{"test":"v1"}'::jsonb)
        RETURNING signal_id
        """,
        (signal_instrument, day, day),
    ).fetchone()
    assert signal is not None
    decision = conn.execute(
        """
        INSERT INTO strategy_funding_decisions (signal_id,deployment_id,verdict,amount,reason_code)
        VALUES (%s,%s,'allocated',%s,'split') RETURNING funding_decision_id
        """,
        (signal[0], deployment_id, amount),
    ).fetchone()
    assert decision is not None
    return int(decision[0])


def _split_fixture(conn: psycopg.Connection[Any]) -> int:
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (42,'SPLIT.A','Split A',TRUE),(43,'SPLIT.B','Split B',TRUE)"
    )
    configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("1250"),
        capital_mode="fixed",
        risk_profile="balanced",
        approval_mode="manual",
        max_concurrent_positions_override=None,
        changed_by="operator",
        reason="assign the engine pot",
    )
    deployment = conn.execute(
        """
        INSERT INTO strategy_deployments (
            strategy_id,strategy_version,mode,capital_limit,currency,enabled,updated_by,reason
        ) VALUES ('split','v1','paper',1000,'USD',TRUE,'test','test')
        RETURNING deployment_id
        """
    ).fetchone()
    assert deployment is not None
    return int(deployment[0])


def test_the_authority_splits_alpha_committed_by_the_funded_signals_instrument(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    deployment_id = _split_fixture(conn)
    # An allocation with no trade yet is committed too -- it is the reservation itself.
    _alpha_allocation(conn, deployment_id=deployment_id, signal_instrument=42, amount="100", day=0)
    traded = _alpha_allocation(conn, deployment_id=deployment_id, signal_instrument=43, amount="250", day=1)
    conn.execute("INSERT INTO strategy_trades (funding_decision_id,instrument_id) VALUES (%s,43)", (traded,))

    loaded = load_engine_capital_authority(conn)

    assert loaded is not None
    assert loaded.alpha_committed == Decimal("350")
    assert loaded.alpha_committed_by_instrument == ((42, Decimal("100")), (43, Decimal("250")))
    assert loaded.core_pending_by_instrument == ()


def test_an_alpha_trade_on_another_instrument_than_its_signal_refuses(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    deployment_id = _split_fixture(conn)
    decision = _alpha_allocation(conn, deployment_id=deployment_id, signal_instrument=42, amount="100", day=0)
    conn.execute("INSERT INTO strategy_trades (funding_decision_id,instrument_id) VALUES (%s,43)", (decision,))

    with pytest.raises(EngineCapitalObservationError, match="differs from its signal") as raised:
        load_engine_capital_authority(conn)
    assert raised.value.reason_code == "engine_capital_population_incomplete"
