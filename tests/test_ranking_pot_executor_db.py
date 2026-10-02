"""#2842 slice 5b — the pot's loader, slot ledger and executor against real Postgres (spec §7.2, ``sql/450``).

One executed-book entry is decided by the 5a path, submitted through the paper path's durable machinery with a stub
broker, closed, and its slot re-entered: deferred while the close is unbooked, then sized from the booked wealth.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from app.services import ranking_pot_executor as px
from app.services import ranking_pot_intent as pi
from app.services.ranking_pot_policy import RANKING_POT_POLICY_HASH
from app.services.strategy_control_plane import configure_deployment, configure_execution_policy
from app.services.strategy_paper_executor import PaperExecutionResult, StrategyPaperExecutionError
from tests.test_ai_trial_executor_db import _broker, _enable_trading
from tests.test_ranking_pot_exec_db import _decide_month, _next_month_session, _universes
from tests.test_ranking_pot_job_db import _first_window
from tests.test_ranking_pot_schema_db import POT, _frozen, _move

Conn = psycopg.Connection[Any]


def _market(conn: Conn, now: datetime, closes: dict[date, Decimal]) -> None:
    """Exchange, quotes, halt feed, pool, the pot's deployment and policy, its activation, the bars behind the basis."""
    conn.execute(
        "INSERT INTO exchanges (exchange_id, country, asset_class) VALUES ('2', 'US', 'us_equity') "
        "ON CONFLICT (exchange_id) DO UPDATE SET asset_class='us_equity'"
    )
    conn.execute("UPDATE instruments SET exchange = '2', currency = 'USD' WHERE instrument_id IN (2842, 2843, 2844)")
    conn.execute(
        "INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last, spread_pct, spread_flag) "
        "SELECT i, %s, 99.9, 100, 99.95, 0.1, false FROM unnest(ARRAY[2842, 2843, 2844]::bigint[]) AS i "
        "ON CONFLICT (instrument_id) DO UPDATE SET quoted_at = EXCLUDED.quoted_at",
        (now,),
    )
    conn.execute(
        "INSERT INTO strategy_halt_feed_state (source, fetched_at, source_pub_at, item_count, payload_sha256) "
        "VALUES ('nasdaq_trader_rss', %s, %s, 0, %s) "
        "ON CONFLICT (source) DO UPDATE SET fetched_at = EXCLUDED.fetched_at",
        (now, now, "0" * 64),
    )
    for d, close in closes.items():
        conn.execute(
            "INSERT INTO price_daily (instrument_id, price_date, open, high, low, close, volume) "
            "SELECT i, %s, %s, %s, %s, %s, 1000 FROM unnest(ARRAY[2842, 2843, 2844]::bigint[]) AS i "
            "ON CONFLICT DO NOTHING",
            (d, close, close, close, close),
        )
    conn.commit()


def _deploy_pot(conn: Conn, decl_id: int) -> None:
    from app.services.strategy_control_plane import configure_paper_pool

    configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("2000"),
        risk_profile="balanced",
        approval_mode="manual",
        max_concurrent_positions_override=None,
        changed_by="test",
        reason="#2842 pot executor fixture",
    )
    deployment = configure_deployment(
        conn,
        strategy_id=POT,
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal("1000"),
        enabled=True,
        changed_by="operator",
        reason="#2842 pot executor fixture",
    )
    configure_execution_policy(
        conn,
        deployment_id=deployment.deployment_id,
        ticket_sizing_mode="fixed",
        ticket_fraction=None,
        fixed_ticket_amount=Decimal("40"),
        max_ticket_amount=Decimal("500"),
        stop_loss_pct=Decimal("10"),
        take_profit_pct=Decimal("100"),
        max_quote_age_seconds=60,
        max_scan_age_seconds=60,
        max_halt_feed_age_seconds=60,
        max_cost_age_seconds=60,
        max_reconciliation_age_seconds=60,
        max_instrument_exposure_pct=Decimal("30"),
        max_portfolio_exposure_pct=Decimal("80"),
        max_drawdown_pct=Decimal("10"),
        min_net_expectancy_pct=Decimal("0"),
        cost_stress_multiplier=Decimal("2"),
        changed_by="test",
        reason="#2842 pot executor fixture",
    )
    conn.execute("INSERT INTO ranking_pot_activations (declaration_id, pot_capital) VALUES (%s, 1000)", (decl_id,))
    conn.commit()


def _signal_of(conn: Conn, attempt: int, iid: int) -> int:
    row = conn.execute(
        "SELECT signal_id FROM ranking_pot_exec_lifecycles WHERE attempt_id = %s AND instrument_id = %s",
        (attempt, iid),
    ).fetchone()
    conn.commit()
    assert row is not None
    return int(row[0])


def _pot_broker(iid: int, now: datetime) -> MagicMock:
    """The v1 stub broker, observed at ``now``."""
    broker = _broker(iid, spread_value="0.1")
    broker.get_account_risk_snapshot.return_value = replace(
        broker.get_account_risk_snapshot.return_value, observed_at=now
    )
    broker.get_what_if_costs.return_value = replace(broker.get_what_if_costs.return_value, last_updated=now)
    return broker


def _at(session: date) -> datetime:
    return datetime.combine(session, time(15, 30), UTC)


def _close_trade(conn: Conn, trade_id: int, *, position: int) -> None:
    """The reconciliation's effect, simulated: filled, then closed; the close event lands only when given."""
    conn.execute("UPDATE strategy_trades SET status = 'closed' WHERE strategy_trade_id = %s", (trade_id,))
    conn.execute(
        "INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id, status, claimed_at, "
        " released_at, release_reason) VALUES (%s, %s, 'released', now(), now(), 'closed')",
        (trade_id, position),
    )
    conn.commit()


def _book_close(conn: Conn, *, position: int, net_profit: str, fees: str = "0") -> None:
    conn.execute(
        "INSERT INTO trade_events (position_id, etoro_instrument_id, instrument_id, event_kind, side, units, price, "
        " executed_at, fees_usd, realized_pnl_usd, source, raw_payload) "
        "VALUES (%s, 2842, 2842, 'close', 'sell', 1, 107.5, now(), %s, %s, 'etoro_history', '{}')",
        (position, fees, net_profit),
    )
    conn.commit()


def test_an_entry_submits_from_its_slot_and_a_reentry_waits_for_the_booking(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    decl_id = _frozen(conn)
    _move(conn, decl_id, "shadow_only", "executing", "supervisor")
    _deploy_pot(conn, decl_id)
    _enable_trading(conn)
    conn.autocommit = True
    d, _ = _first_window(conn)

    # Month 1: 2842 enters slot 1 (ATR14 = 2 on the ticket, snapshot close 100).
    a1, r1 = _decide_month(conn, monkeypatch, d, _universes({2842: "0.9"}, {2842}), policy_hash=RANKING_POT_POLICY_HASH)
    assert r1.entries == 1
    target = conn.execute("SELECT target_session FROM ranking_pot_rebalance_attempts WHERE attempt_id = %s", (a1,))
    t1 = target.fetchone()[0]  # type: ignore[index]
    now = _at(t1)
    _market(conn, now, {d: Decimal(100)})
    conn.autocommit = False
    signal = _signal_of(conn, a1, 2842)
    assert px.due_pot_entries(conn, today=t1) == [signal]
    conn.commit()

    # Before 15:00 UTC the executor refuses to run at all (nothing persisted).
    with pytest.raises(StrategyPaperExecutionError, match="from 15:00"):
        px.execute_pot_signal(
            conn, broker=_pot_broker(2842, now), signal_id=signal, now=datetime.combine(t1, time(14), UTC)
        )

    broker = _pot_broker(2842, now)
    result = px.execute_pot_signal(conn, broker=broker, signal_id=signal, now=now)
    assert (result.verdict, result.reason_code) == ("submitted", "broker_accepted")
    body = broker.place_demo_strategy_order.call_args.args[0]
    # Slot wealth = 1000 / 25 = 40; the frozen 3 × ATR stop and 2R target off the 100 ask.
    assert (body.amount, body.stop_loss_rate, body.take_profit_rate) == (
        Decimal("40.00"),
        Decimal("94.000000"),
        Decimal("112.000000"),
    )
    sub = conn.execute(
        "SELECT slot, slot_wealth, requested_amount, amount, ask, atr14, stop_loss_rate, take_profit_rate "
        "FROM ranking_pot_exec_submissions"
    ).fetchone()
    assert sub == (
        1,
        Decimal("40.00"),
        Decimal("40.00"),
        Decimal("40.00"),
        Decimal(100),
        Decimal(2),
        Decimal(94),
        Decimal(112),
    )
    assert px.due_pot_entries(conn, today=t1) == []
    conn.commit()

    # The trade closes; its close event has not landed yet.
    assert isinstance(result, PaperExecutionResult) and result.strategy_trade_id is not None
    trade_id = result.strategy_trade_id
    _close_trade(conn, trade_id, position=2842001)

    # Month 2: 2842 left R last month? No — it closed; it may not re-enter the month of its exit, so 2843 takes
    # slot 1 (free again).
    conn.autocommit = True
    d2 = _next_month_session(d)
    a2, r2 = _decide_month(
        conn, monkeypatch, d2, _universes({2842: "0.9", 2843: "0.8"}, {2842, 2843}), policy_hash=RANKING_POT_POLICY_HASH
    )
    conn.autocommit = False
    lc = conn.execute(
        "SELECT instrument_id, slot FROM ranking_pot_exec_lifecycles WHERE attempt_id = %s", (a2,)
    ).fetchall()
    assert lc == [(2843, 1)]
    t2 = conn.execute("SELECT target_session FROM ranking_pot_rebalance_attempts WHERE attempt_id = %s", (a2,))
    t2 = t2.fetchone()[0]  # type: ignore[index]
    now2 = _at(t2)
    _market(conn, now2, {d2: Decimal(100)})
    signal2 = _signal_of(conn, a2, 2843)

    # Unbooked: deferred, nothing written, retried by the next fire.
    deferred = px.execute_pot_signal(conn, broker=_pot_broker(2843, now2), signal_id=signal2, now=now2)
    assert (deferred.verdict, deferred.reason_code) == ("deferred", "pot_slot_ledger_incomplete")
    assert conn.execute("SELECT 1 FROM strategy_funding_decisions WHERE signal_id = %s", (signal2,)).fetchone() is None
    conn.commit()

    # Booked (+7.50 net): the slot's wealth is 47.50 and the ticket is that.
    _book_close(conn, position=2842001, net_profit="7.5")
    broker2 = _pot_broker(2843, now2)
    entered = px.execute_pot_signal(conn, broker=broker2, signal_id=signal2, now=now2)
    assert entered.verdict == "submitted"
    assert broker2.place_demo_strategy_order.call_args.args[0].amount == Decimal("47.50")
    wealth = conn.execute(
        "SELECT slot_wealth FROM ranking_pot_exec_submissions s "
        "JOIN ranking_pot_exec_lifecycles l USING (lifecycle_id) "
        "WHERE l.signal_id = %s",
        (signal2,),
    ).fetchone()
    assert wealth == (Decimal("47.50"),)


def test_refusals_persist_and_the_submission_row_binds_its_own_authority(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    decl_id = _frozen(conn)
    _move(conn, decl_id, "shadow_only", "executing", "supervisor")
    _deploy_pot(conn, decl_id)
    _enable_trading(conn)
    conn.autocommit = True
    d, _ = _first_window(conn)
    a1, _ = _decide_month(
        conn, monkeypatch, d, _universes({2842: "0.9", 2843: "0.8"}, {2842, 2843}), policy_hash=RANKING_POT_POLICY_HASH
    )
    t1 = conn.execute("SELECT target_session FROM ranking_pot_rebalance_attempts WHERE attempt_id = %s", (a1,))
    t1 = t1.fetchone()[0]  # type: ignore[index]
    now = _at(t1)
    # 2843's stored close was rewritten after the snapshot: `basis_changed`, persisted (one attempt).
    _market(conn, now, {d: Decimal(100)})
    conn.execute("UPDATE price_daily SET close = 50 WHERE instrument_id = 2843 AND price_date = %s", (d,))
    conn.autocommit = False
    s2843 = _signal_of(conn, a1, 2843)
    refused = px.execute_pot_signal(conn, broker=_pot_broker(2843, now), signal_id=s2843, now=now)
    assert (refused.verdict, refused.reason_code) == ("rejected", "basis_changed")
    assert pi.load_pot_intent(conn, signal_id=s2843, now=now)[1] == "basis_changed"
    conn.rollback()

    # The policy hash must be the running one.
    s2842 = _signal_of(conn, a1, 2842)
    monkeypatch.setattr(pi, "RANKING_POT_POLICY_HASH", "f" * 64)
    assert pi.load_pot_intent(conn, signal_id=s2842, now=now)[1] == "pot_policy_drift"
    conn.rollback()
    monkeypatch.undo()

    # A halt: entries refuse `pot_not_executing` at load.
    conn.autocommit = True
    _move(conn, decl_id, "executing", "halted_operator", "operator")
    conn.autocommit = False
    assert pi.load_pot_intent(conn, signal_id=s2842, now=now)[1] == "pot_not_executing"
    conn.rollback()

    # The submission row cannot be written for another transaction's trade.
    trade = conn.execute(
        "INSERT INTO strategy_funding_decisions (signal_id, deployment_id, verdict, amount, reason_code) "
        "SELECT %s, deployment_id, 'allocated', 40, 't' FROM strategy_deployments WHERE strategy_id = %s "
        "RETURNING funding_decision_id",
        (s2842, POT),
    ).fetchone()
    assert trade is not None
    trade_id = conn.execute(
        "INSERT INTO strategy_trades (funding_decision_id, instrument_id, status) VALUES (%s, 2842, 'planned') "
        "RETURNING strategy_trade_id",
        (trade[0],),
    ).fetchone()
    assert trade_id is not None
    conn.commit()
    lifecycle = conn.execute("SELECT lifecycle_id FROM ranking_pot_exec_lifecycles WHERE signal_id = %s", (s2842,))
    lc_id = lifecycle.fetchone()[0]  # type: ignore[index]
    with pytest.raises(psycopg.errors.RaiseException, match="only by the transaction that created it"):
        conn.execute(
            "INSERT INTO ranking_pot_exec_submissions (lifecycle_id, strategy_trade_id, slot, slot_wealth, "
            " requested_amount, amount, ask, quote_at, atr14, stop_loss_rate, take_profit_rate) "
            "VALUES (%s, %s, 1, 40, 40, 40, 100, now(), 2, 94, 112)",
            (lc_id, trade_id[0]),
        )
    conn.rollback()


def _marked(broker: MagicMock, *, position: int, pnl: str, fees: str | None = "0") -> MagicMock:
    """The stub snapshot, now carrying the pot position's broker mark."""
    from app.providers.broker import BrokerDirectPositionInvestment, BrokerInstrumentInvestment

    mark = BrokerDirectPositionInvestment(
        position_id=position,
        instrument_id=2842,
        is_buy=True,
        units=Decimal("0.4"),
        amount=Decimal(40),
        unrealized_pnl=Decimal(pnl),
        market_value=Decimal(40) + Decimal(pnl),
        is_partially_altered=False,
        close_rate=Decimal(100),
        close_conversion_rate=Decimal(1),
        asset_currency_id=1,
        total_fees=None if fees is None else Decimal(fees),
    )
    snapshot = broker.get_account_risk_snapshot.return_value
    broker.get_account_risk_snapshot.return_value = replace(
        snapshot,
        direct_positions=(mark,),
        instrument_investments=(BrokerInstrumentInvestment(2842, Decimal(40), Decimal(40) + Decimal(pnl), 1, 0),),
    )
    return broker


def _state(conn: Conn, decl_id: int) -> tuple[str, str]:
    row = conn.execute(
        "SELECT to_state, actor FROM ranking_pot_state_events WHERE declaration_id = %s ORDER BY event_id DESC LIMIT 1",
        (decl_id,),
    ).fetchone()
    conn.commit()
    assert row is not None
    return str(row[0]), str(row[1])


def test_the_loss_check_defers_on_a_defect_halts_an_entry_and_runs_without_one(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    decl_id = _frozen(conn)
    _move(conn, decl_id, "shadow_only", "executing", "supervisor")
    _deploy_pot(conn, decl_id)  # POT_CAPITAL 1000 → the limit is −200
    _enable_trading(conn)
    conn.autocommit = True
    d, _ = _first_window(conn)
    a1, _ = _decide_month(
        conn, monkeypatch, d, _universes({2842: "0.9", 2843: "0.8"}, {2842, 2843}), policy_hash=RANKING_POT_POLICY_HASH
    )
    t1 = conn.execute("SELECT target_session FROM ranking_pot_rebalance_attempts WHERE attempt_id = %s", (a1,))
    t1 = t1.fetchone()[0]  # type: ignore[index]
    now = _at(t1)
    _market(conn, now, {d: Decimal(100)})
    conn.autocommit = False
    s2842, s2843 = _signal_of(conn, a1, 2842), _signal_of(conn, a1, 2843)

    # 2842 is submitted, then filled: the reconciliation's ownership row, simulated.
    first = px.execute_pot_signal(conn, broker=_pot_broker(2842, now), signal_id=s2842, now=now)
    assert isinstance(first, PaperExecutionResult) and first.strategy_trade_id is not None
    conn.execute("UPDATE strategy_trades SET status = 'open' WHERE strategy_trade_id = %s", (first.strategy_trade_id,))
    conn.execute(
        "INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id, status, claimed_at) "
        "VALUES (%s, 2842001, 'active', now())",
        (first.strategy_trade_id,),
    )
    conn.commit()

    # A mark with no totalFees is a data defect: deferred, nothing written.
    deferred = px.execute_pot_signal(
        conn, broker=_marked(_pot_broker(2843, now), position=2842001, pnl="-1", fees=None), signal_id=s2843, now=now
    )
    assert (deferred.verdict, deferred.reason_code) == ("deferred", "pot_loss_check_unavailable")
    assert conn.execute("SELECT 1 FROM strategy_funding_decisions WHERE signal_id = %s", (s2843,)).fetchone() is None
    conn.commit()

    # The periodic evaluator, no entry involved: −100 is inside the limit.
    assert px.evaluate_losses(conn, broker=_marked(_pot_broker(2843, now), position=2842001, pnl="-100")) == {
        decl_id: "ok"
    }
    assert _state(conn, decl_id) == ("executing", "supervisor")

    # −200 at the entry's own authority: refused, persisted, and the engine halts the pot.
    refused = px.execute_pot_signal(
        conn, broker=_marked(_pot_broker(2843, now), position=2842001, pnl="-200"), signal_id=s2843, now=now
    )
    assert (refused.verdict, refused.reason_code) == ("rejected", "pot_loss_limit")
    assert _state(conn, decl_id) == ("halted_loss", "engine")
    # Nothing is executing any more: no broker call.
    idle = _pot_broker(2843, now)
    assert px.evaluate_losses(conn, broker=idle) == {}
    idle.get_account_risk_snapshot.assert_not_called()

    # A supervisor resumption while still in breach is halted again by the evaluator alone.
    conn.autocommit = True
    _move(conn, decl_id, "halted_loss", "executing", "supervisor")
    conn.autocommit = False
    assert px.evaluate_losses(conn, broker=_marked(_pot_broker(2843, now), position=2842001, pnl="-250")) == {
        decl_id: "halted"
    }
    assert _state(conn, decl_id) == ("halted_loss", "engine")
