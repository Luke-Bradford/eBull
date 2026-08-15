"""#2453 exact-owned P&L and allocation-unbiased attribution tests."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import psycopg
import pytest
from fastapi import HTTPException, Request

from app.api.config import ConfigPatchRequest, patch_config
from app.api.strategies import (
    AllocationUpdateRequest,
    StrategyPaperPoolUpdateRequest,
    _current_versions,
    close_strategy_owned_position,
    get_fired_signals,
    get_strategy_overview,
    get_strategy_owned_positions,
    get_strategy_pnl_history,
    request_evidence_refresh,
    update_strategy_allocation,
    update_strategy_paper_pool,
)
from app.security.sessions import SessionRow
from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.research_price_structure_store import QUARANTINE_RULE_SET_VERSION
from app.services.runtime_config import get_runtime_config, update_runtime_config
from app.services.strategy_control_plane import configure_paper_pool, load_paper_pool
from app.services.strategy_monitoring import (
    load_attribution,
    load_control_state,
    load_entry_block_state,
    load_owned_pnl,
)
from app.services.strategy_position_manager import PositionManagerResult
from tests.fixtures.ebull_test_db import test_database_url


def _instrument(conn: psycopg.Connection[Any], instrument_id: int) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable)
        VALUES (%s, %s, %s, 'USD', true)
        """,
        (instrument_id, f"M{instrument_id}", f"Monitor {instrument_id}"),
    )


def _signal(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    strategy_id: str,
    strategy_version: str,
    signal_date: str,
    fill_price: Decimal,
) -> int:
    row = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id, strategy_version, instrument_id, signal_bar_date,
            signal_kind, verdict, fill_bar_date, fill_price, universe,
            input_rule_set_versions
        ) VALUES (%s, %s, %s, %s, 'entry', 'fired', %s::date + 1,
                  %s, 'survivorship_free',
                  '{"indicator_series":"rules-v1"}'::jsonb)
        RETURNING signal_id
        """,
        (strategy_id, strategy_version, instrument_id, signal_date, signal_date, fill_price),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _deployment(conn: psycopg.Connection[Any], strategy_id: str, strategy_version: str) -> int:
    row = conn.execute(
        """
        INSERT INTO strategy_deployments (
            strategy_id, strategy_version, mode, capital_limit, currency,
            enabled, revision, updated_by, reason
        ) VALUES (%s, %s, 'paper', 1000, 'USD', false, 1, 'tester', 'monitoring test')
        RETURNING deployment_id
        """,
        (strategy_id, strategy_version),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _funded_trade(conn: psycopg.Connection[Any], *, signal_id: int, deployment_id: int, instrument_id: int) -> int:
    decision = conn.execute(
        """
        INSERT INTO strategy_funding_decisions (
            signal_id, deployment_id, verdict, amount, reason_code
        ) VALUES (%s, %s, 'allocated', 100, 'test_funded')
        RETURNING funding_decision_id
        """,
        (signal_id, deployment_id),
    ).fetchone()
    assert decision is not None
    trade = conn.execute(
        """
        INSERT INTO strategy_trades (funding_decision_id, instrument_id, status)
        VALUES (%s, %s, 'open') RETURNING strategy_trade_id
        """,
        (decision[0], instrument_id),
    ).fetchone()
    assert trade is not None
    return int(trade[0])


def test_owned_pnl_excludes_manual_same_instrument_lifecycle(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "monitoring-owned"
    strategy_version = "monitoring-owned-v1"
    instrument_id = 2453001
    _instrument(ebull_test_conn, instrument_id)
    signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        signal_date="2026-08-01",
        fill_price=Decimal("10"),
    )
    deployment_id = _deployment(ebull_test_conn, strategy_id, strategy_version)
    trade_id = _funded_trade(
        ebull_test_conn,
        signal_id=signal_id,
        deployment_id=deployment_id,
        instrument_id=instrument_id,
    )
    ebull_test_conn.execute(
        """
        INSERT INTO broker_positions (
            position_id, instrument_id, is_buy, units, initial_units, amount,
            initial_amount_in_dollars, open_rate, open_conversion_rate,
            open_date_time, raw_payload
        ) VALUES
          (7001, %s, true, 5, 10, 100, 100, 10, 1, now(), '{}'::jsonb),
          (7002, %s, true, 8, 8, 80, 80, 10, 1, now(), '{}'::jsonb)
        """,
        (instrument_id, instrument_id),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_position_ownership (
            strategy_trade_id, broker_position_id, status
        ) VALUES (%s, 7001, 'active')
        """,
        (trade_id,),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last)
        VALUES (%s, now(), 11.9, 12.1, 12)
        """,
        (instrument_id,),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO trade_events (
            position_id, etoro_instrument_id, instrument_id, event_kind, side,
            units, price, executed_at, fees_usd, realized_pnl_usd, source, raw_payload
        ) VALUES
          (7001, %s, %s, 'close', 'sell', 5, 12, now(), 1.25, 12.50, 'etoro_history', '{}'::jsonb),
          (7002, %s, %s, 'close', 'sell', 8, 30, now(), 20, 999, 'etoro_history', '{}'::jsonb)
        """,
        (instrument_id, instrument_id, instrument_id, instrument_id),
    )

    pnl = load_owned_pnl(ebull_test_conn, versions=[strategy_version])[(strategy_id, strategy_version)]

    assert pnl.realised_pnl == Decimal("12.50")
    assert pnl.unrealised_pnl == Decimal("10")
    assert pnl.total_pnl == Decimal("22.50")
    assert pnl.observed_fees == Decimal("1.25")
    assert pnl.invested_capital == Decimal("100")
    assert pnl.owned_position_count == 1
    assert pnl.complete


def test_realised_history_keeps_retired_versions_and_excludes_manual_positions(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "retired-strategy"
    strategy_version = "retired-strategy-v1"
    instrument_id = 2453091
    _instrument(ebull_test_conn, instrument_id)
    signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        signal_date="2026-08-01",
        fill_price=Decimal("10"),
    )
    deployment_id = _deployment(ebull_test_conn, strategy_id, strategy_version)
    trade_id = _funded_trade(
        ebull_test_conn,
        signal_id=signal_id,
        deployment_id=deployment_id,
        instrument_id=instrument_id,
    )
    ebull_test_conn.execute(
        "INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id) VALUES (%s, 7453091)",
        (trade_id,),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO trade_events (
            position_id, etoro_instrument_id, instrument_id, event_kind, side,
            units, price, executed_at, fees_usd, realized_pnl_usd, source, raw_payload
        ) VALUES
          (7453091, %s, %s, 'close', 'sell', 1, 17, now(), 1, 7, 'etoro_history', '{}'::jsonb),
          (7453092, %s, %s, 'close', 'sell', 1, 900, now(), 1, 890, 'etoro_history', '{}'::jsonb)
        """,
        (instrument_id, instrument_id, instrument_id, instrument_id),
    )

    response = get_strategy_pnl_history(days=365, conn=ebull_test_conn)

    assert response.basis == "exact_owned_realised_pnl_only"
    assert response.total_return_available is False
    assert response.benchmark_comparison_available is False
    assert len(response.points) == 1
    assert response.points[0].total_pnl == Decimal("7")
    assert response.points[0].strategy_pnl == {strategy_id: Decimal("7")}


def test_wealth_history_combines_principal_realised_and_eod_open_marks_without_manual_positions(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    from app.api.strategies import get_strategy_wealth_history

    strategy_id = "wealth-strategy"
    strategy_version = "wealth-strategy-v1"
    instrument_id = 2453093
    _instrument(ebull_test_conn, instrument_id)
    signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        signal_date="2026-08-01",
        fill_price=Decimal("10"),
    )
    deployment_id = _deployment(ebull_test_conn, strategy_id, strategy_version)
    trade_id = _funded_trade(
        ebull_test_conn,
        signal_id=signal_id,
        deployment_id=deployment_id,
        instrument_id=instrument_id,
    )
    ebull_test_conn.execute(
        "INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id) VALUES (%s, 7453093)",
        (trade_id,),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_paper_pool_events
            (enabled, capital_limit, currency, capital_mode, changed_by, reason)
        VALUES (true, 1000, 'USD', 'fixed', 'test', 'fund sleeve')
        """
    )
    ebull_test_conn.execute(
        """
        INSERT INTO trade_events (
            position_id, etoro_instrument_id, instrument_id, event_kind, side,
            units, price, executed_at, fees_usd, realized_pnl_usd, source, raw_payload
        ) VALUES (7453093, %s, %s, 'close', 'sell', 1, 17, now(), 1, 7, 'etoro_history', '{}'::jsonb)
        """,
        (instrument_id, instrument_id),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
            snapshot_date, display_currency, total_value, positions_value, cash_value,
            positions_total, positions_priced, computed_at
        ) VALUES (CURRENT_DATE, 'GBP', 9999, 9999, 0, 2, 2, now())
        """
    )
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_position_snapshots (
            snapshot_date, position_id, instrument_id, units, close_price,
            native_currency, value_display, price_status, unrealised_pnl_usd
        ) VALUES
          (CURRENT_DATE, 7453093, %s, 1, 20, 'USD', 20, 'priced', 10),
          (CURRENT_DATE, 7453094, %s, 1, 999, 'USD', 999, 'priced', 999)
        """,
        (instrument_id, instrument_id),
    )

    response = get_strategy_wealth_history(days=365, conn=ebull_test_conn)

    assert response.basis == "exact_owned_mark_to_market_nav"
    assert response.total_return_available is False
    assert len(response.points) == 1
    point = response.points[0]
    assert point.principal == Decimal("1000")
    assert point.external_flow == Decimal("1000")
    assert point.realised_pnl == Decimal("7")
    assert point.unrealised_pnl == Decimal("10")
    assert point.total_pnl == Decimal("17")
    assert point.pot_value == Decimal("1017")
    assert point.complete


def test_strategy_positions_show_only_exact_owned_trade_with_portfolio_valuation(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "monitoring-owned"
    strategy_version = "monitoring-owned-v1"
    instrument_id = 2453011
    _instrument(ebull_test_conn, instrument_id)
    signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        signal_date="2026-08-01",
        fill_price=Decimal("10"),
    )
    deployment_id = _deployment(ebull_test_conn, strategy_id, strategy_version)
    trade_id = _funded_trade(
        ebull_test_conn,
        signal_id=signal_id,
        deployment_id=deployment_id,
        instrument_id=instrument_id,
    )
    ebull_test_conn.execute(
        """
        INSERT INTO broker_positions (
            position_id, instrument_id, is_buy, units, initial_units, amount,
            initial_amount_in_dollars, open_rate, open_conversion_rate,
            open_date_time, stop_loss_rate, take_profit_rate, raw_payload
        ) VALUES
          (7011, %s, true, 5, 5, 100, 100, 10, 1, now(), 9, 14, '{}'::jsonb),
          (7012, %s, true, 8, 8, 80, 80, 10, 1, now(), NULL, NULL, '{}'::jsonb)
        """,
        (instrument_id, instrument_id),
    )
    ebull_test_conn.execute(
        "INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id) VALUES (%s, 7011)",
        (trade_id,),
    )
    ebull_test_conn.execute(
        "INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last) VALUES (%s, now(), 11.9, 12.1, 12)",
        (instrument_id,),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO positions (
            instrument_id, open_date, avg_cost, current_units, cost_basis, source
        ) VALUES (%s, DATE '2026-08-01', 10, 13, 180, 'broker_sync')
        """,
        (instrument_id,),
    )

    response = get_strategy_owned_positions(ebull_test_conn)

    assert len(response.positions) == 1
    position = response.positions[0]
    assert position.strategy_trade_id == trade_id
    assert position.broker_position_id == 7011
    assert position.assigned_value == Decimal("100.0")
    assert position.current_value == Decimal("110.0")
    assert position.unrealised_pnl == Decimal("10.0")
    assert position.unrealised_return_pct == Decimal("10.0")
    assert position.stop_loss_rate == Decimal("9.0")
    assert position.take_profit_rate == Decimal("14.0")
    assert response.live_quote_instrument_ids == [instrument_id]


def test_strategy_position_close_passes_both_exact_ids_to_owned_manager(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    instrument_id = 2453012
    _instrument(ebull_test_conn, instrument_id)
    signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id="operator-close",
        strategy_version="operator-close-v1",
        signal_date="2026-08-01",
        fill_price=Decimal("10"),
    )
    deployment_id = _deployment(ebull_test_conn, "operator-close", "operator-close-v1")
    trade_id = _funded_trade(
        ebull_test_conn,
        signal_id=signal_id,
        deployment_id=deployment_id,
        instrument_id=instrument_id,
    )
    ebull_test_conn.execute(
        "INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id) VALUES (%s, 7021)",
        (trade_id,),
    )
    ebull_test_conn.commit()
    request = Request({"type": "http", "app": SimpleNamespace(state=SimpleNamespace())})
    monkeypatch.setattr("app.api.strategies.settings.etoro_env", "demo")
    monkeypatch.setattr(
        "app.api.strategies._load_strategy_broker_credentials",
        lambda *_args, **_kwargs: ("api", "user"),
    )
    provider = MagicMock()
    provider.__enter__.return_value = MagicMock()
    monkeypatch.setattr("app.api.strategies.EtoroBrokerProvider", lambda **_kwargs: provider)
    managed: dict[str, object] = {}

    def _manage(_conn: object, **kwargs: object) -> PositionManagerResult:
        managed.update(kwargs)
        return PositionManagerResult(trade_id, 7021, "submitted", "broker_close_accepted", 81)

    monkeypatch.setattr("app.api.strategies.manage_owned_position", _manage)

    response = close_strategy_owned_position(
        trade_id,
        7021,
        request,
        _session(),
        ebull_test_conn,
    )

    assert response.state == "submitted"
    assert managed["strategy_trade_id"] == trade_id
    assert managed["broker_position_id"] == 7021
    assert managed["close_reason"] == "operator_close"


def test_strategy_position_close_rejects_unowned_same_instrument_id_before_broker_io(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    instrument_id = 2453013
    _instrument(ebull_test_conn, instrument_id)
    signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id="operator-close",
        strategy_version="operator-close-v1",
        signal_date="2026-08-01",
        fill_price=Decimal("10"),
    )
    deployment_id = _deployment(ebull_test_conn, "operator-close", "operator-close-v1")
    trade_id = _funded_trade(
        ebull_test_conn,
        signal_id=signal_id,
        deployment_id=deployment_id,
        instrument_id=instrument_id,
    )
    ebull_test_conn.execute(
        "INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id) VALUES (%s, 7031)",
        (trade_id,),
    )
    ebull_test_conn.commit()
    request = Request({"type": "http", "app": SimpleNamespace(state=SimpleNamespace())})
    credentials = MagicMock()
    monkeypatch.setattr("app.api.strategies.settings.etoro_env", "demo")
    monkeypatch.setattr("app.api.strategies._load_strategy_broker_credentials", credentials)

    with pytest.raises(HTTPException) as exc:
        close_strategy_owned_position(
            trade_id,
            7032,
            request,
            _session(),
            ebull_test_conn,
        )

    assert exc.value.status_code == 404
    credentials.assert_not_called()


def test_missing_owned_mark_is_unknown_not_zero(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "monitoring-missing-mark"
    strategy_version = "monitoring-missing-mark-v1"
    instrument_id = 2453002
    _instrument(ebull_test_conn, instrument_id)
    signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        signal_date="2026-08-01",
        fill_price=Decimal("10"),
    )
    deployment_id = _deployment(ebull_test_conn, strategy_id, strategy_version)
    trade_id = _funded_trade(
        ebull_test_conn,
        signal_id=signal_id,
        deployment_id=deployment_id,
        instrument_id=instrument_id,
    )
    ebull_test_conn.execute(
        """
        INSERT INTO broker_positions (
            position_id, instrument_id, is_buy, units, amount,
            initial_amount_in_dollars, open_rate, open_conversion_rate,
            open_date_time, raw_payload
        ) VALUES (7101, %s, true, 5, 100, 100, 10, 1, now(), '{}'::jsonb)
        """,
        (instrument_id,),
    )
    ebull_test_conn.execute(
        "INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id) VALUES (%s, 7101)",
        (trade_id,),
    )

    pnl = load_owned_pnl(ebull_test_conn, versions=[strategy_version])[(strategy_id, strategy_version)]

    assert pnl.unrealised_pnl is None
    assert pnl.total_pnl is None
    assert "active_position_mark_unavailable" in pnl.incomplete_reasons


def test_unreconciled_funding_makes_all_owned_money_unknown(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "monitoring-unreconciled"
    strategy_version = "monitoring-unreconciled-v1"
    instrument_id = 2453005
    _instrument(ebull_test_conn, instrument_id)
    signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        signal_date="2026-08-01",
        fill_price=Decimal("10"),
    )
    deployment_id = _deployment(ebull_test_conn, strategy_id, strategy_version)
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_funding_decisions (
            signal_id, deployment_id, verdict, amount, reason_code
        ) VALUES (%s, %s, 'allocated', 100, 'awaiting_reconciliation')
        """,
        (signal_id, deployment_id),
    )

    pnl = load_owned_pnl(ebull_test_conn, versions=[strategy_version])[(strategy_id, strategy_version)]

    assert pnl.realised_pnl is None
    assert pnl.unrealised_pnl is None
    assert pnl.total_pnl is None
    assert pnl.invested_capital is None
    assert pnl.observed_fees is None
    assert pnl.incomplete_reasons == ("funding_not_reconciled_to_trade",)


def test_shadow_statistics_do_not_depend_on_later_allocation_configuration(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "monitoring-shadow"
    strategy_version = "monitoring-shadow-v1"
    instrument_id = 2453003
    _instrument(ebull_test_conn, instrument_id)
    funded_signal = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        signal_date="2026-08-01",
        fill_price=Decimal("100"),
    )
    rejected_signal = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        signal_date="2026-08-03",
        fill_price=Decimal("100"),
    )
    deployment_id = _deployment(ebull_test_conn, strategy_id, strategy_version)
    trade_id = _funded_trade(
        ebull_test_conn,
        signal_id=funded_signal,
        deployment_id=deployment_id,
        instrument_id=instrument_id,
    )
    order = ebull_test_conn.execute(
        """
        INSERT INTO orders (
            instrument_id, action, order_type, requested_amount, status, execution_origin
        ) VALUES (%s, 'BUY', 'MARKET', 100, 'rejected', 'strategy') RETURNING order_id
        """,
        (instrument_id,),
    ).fetchone()
    assert order is not None
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_trade_orders (strategy_trade_id, order_id, purpose)
        VALUES (%s, %s, 'entry')
        """,
        (trade_id, order[0]),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_order_position_executions (
            order_id, broker_position_id, opening_units, average_price
        ) VALUES (%s, 7201, 1, 101)
        """,
        (order[0],),
    )
    for signal_id, result in ((funded_signal, Decimal("10")), (rejected_signal, Decimal("-4"))):
        ebull_test_conn.execute(
            """
            INSERT INTO strategy_outcomes (
                signal_id, rule_set_version, input_rule_set_version, outcome,
                resolution_method, exit_bar_date, exit_price, bars_held, gross_return_pct
            ) VALUES (%s, %s, %s, 'expired', 'daily_bar', '2026-08-08', 100, 3, %s)
            """,
            (signal_id, OUTCOME_RULE_SET_VERSION, QUARANTINE_RULE_SET_VERSION, result),
        )

    before = load_attribution(
        ebull_test_conn,
        versions=[strategy_version],
        outcome_version=OUTCOME_RULE_SET_VERSION,
        input_version=QUARANTINE_RULE_SET_VERSION,
    )[(strategy_id, strategy_version)]
    ebull_test_conn.execute(
        "UPDATE strategy_deployments SET capital_limit=750, revision=2 WHERE deployment_id=%s",
        (deployment_id,),
    )
    after = load_attribution(
        ebull_test_conn,
        versions=[strategy_version],
        outcome_version=OUTCOME_RULE_SET_VERSION,
        input_version=QUARANTINE_RULE_SET_VERSION,
    )[(strategy_id, strategy_version)]

    assert before == after
    assert before.shadow_average_return_pct == Decimal("3")
    assert before.funded_shadow_average_return_pct == Decimal("10")
    assert before.rejected_shadow_average_return_pct == Decimal("-4")
    assert before.opportunity_gap_pct == Decimal("-14")
    assert before.funded_capture_rate == Decimal("0.5")
    assert before.fill_rate == Decimal("1")
    assert before.broker_rejected_entries == 0
    assert before.broker_rejection_rate == Decimal("0")
    assert before.average_slippage_pct == Decimal("1")


def test_unprocessed_current_entry_is_visible_as_not_funded_with_reason(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "s1-time-series-momentum"
    strategy_version = _current_versions()[strategy_id]
    instrument_id = 2453004
    _instrument(ebull_test_conn, instrument_id)
    signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        signal_date="2026-08-01",
        fill_price=Decimal("10"),
    )
    other_signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id="s3-mean-reversion-in-trend",
        strategy_version=_current_versions()["s3-mean-reversion-in-trend"],
        signal_date="2026-08-02",
        fill_price=Decimal("11"),
    )

    response = get_fired_signals(
        cursor=None,
        limit=50,
        strategy_id=strategy_id,
        conn=ebull_test_conn,
    )
    signal = next(item for item in response.items if item.signal_id == signal_id)

    assert other_signal_id not in {item.signal_id for item in response.items}
    assert signal.funding_status == "rejected"
    assert signal.funding_reason == "not_evaluated_by_allocator"
    assert signal.funded_amount is None
    assert signal.trade_lifecycle is None


def test_fired_signal_exposes_exact_completed_trade_lifecycle(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "s1-time-series-momentum"
    strategy_version = _current_versions()[strategy_id]
    instrument_id = 2453005
    _instrument(ebull_test_conn, instrument_id)
    signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        signal_date="2026-08-03",
        fill_price=Decimal("10"),
    )
    deployment_id = _deployment(ebull_test_conn, strategy_id, strategy_version)
    trade_id = _funded_trade(
        ebull_test_conn,
        signal_id=signal_id,
        deployment_id=deployment_id,
        instrument_id=instrument_id,
    )
    ownership_row = ebull_test_conn.execute(
        """
        INSERT INTO strategy_position_ownership (
            strategy_trade_id, broker_position_id, status, released_at, release_reason
        ) VALUES (%s, 7453005, 'released', now(), 'broker_close_observed')
        RETURNING ownership_id
        """,
        (trade_id,),
    ).fetchone()
    assert ownership_row is not None
    ownership_id = ownership_row[0]
    order_row = ebull_test_conn.execute(
        """
        INSERT INTO orders (
            broker_order_ref, instrument_id, action, order_type, requested_amount,
            status, execution_origin, strategy_request_id
        ) VALUES ('99005', %s, 'SELL', 'MARKET', 100, 'filled', 'strategy', %s)
        RETURNING order_id
        """,
        (instrument_id, uuid4()),
    ).fetchone()
    assert order_row is not None
    order_id = order_row[0]
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_trade_orders (strategy_trade_id, order_id, purpose)
        VALUES (%s, %s, 'exit')
        """,
        (trade_id, order_id),
    )
    operation_row = ebull_test_conn.execute(
        """
        INSERT INTO strategy_position_operations (
            ownership_id, order_id, operation_type, trigger_code, request_id,
            status, broker_order_ref, created_at, submitted_at, resolved_at
        ) VALUES (%s, %s, 'close', 'operator_close', %s,
                  'applied', 99005, now() - interval '2 minutes',
                  now() - interval '1 minute', now())
        RETURNING position_operation_id
        """,
        (ownership_id, order_id, uuid4()),
    ).fetchone()
    assert operation_row is not None
    operation_id = operation_row[0]
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_order_reconciliation_state (
            order_id, state, last_attempt_at, reconciled_at, attempt_count,
            broker_status, position_count, updated_at
        ) VALUES (%s, 'resolved', now(), now(), 1, 'closed', 1, now())
        """,
        (order_id,),
    )
    ebull_test_conn.execute(
        "UPDATE strategy_trades SET status='closed', updated_at=now() WHERE strategy_trade_id=%s",
        (trade_id,),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO trade_events (
            position_id, etoro_instrument_id, instrument_id, event_kind, side,
            units, price, executed_at, fees_usd, realized_pnl_usd, source, raw_payload
        ) VALUES (7453005, %s, %s, 'close', 'sell', 10, 11, now(), 1.25, 8.75,
                  'etoro_history', '{}'::jsonb)
        """,
        (instrument_id, instrument_id),
    )

    response = get_fired_signals(cursor=None, limit=50, strategy_id=strategy_id, conn=ebull_test_conn)
    signal = next(item for item in response.items if item.signal_id == signal_id)
    lifecycle = signal.trade_lifecycle

    assert lifecycle is not None
    assert lifecycle.trade_status == "closed"
    assert lifecycle.ownership_count == 1
    assert lifecycle.broker_position_id == 7453005
    assert lifecycle.ownership_status == "released"
    assert lifecycle.latest_operation_type == "close"
    assert lifecycle.latest_operation_trigger == "operator_close"
    assert lifecycle.latest_operation_status == "applied"
    assert lifecycle.latest_operation_id == operation_id
    assert lifecycle.latest_operation_order_id == order_id
    assert lifecycle.latest_reconciliation_state == "resolved"
    assert lifecycle.latest_reconciliation_broker_status == "closed"
    assert lifecycle.latest_reconciliation_attempt_count == 1
    assert lifecycle.latest_reconciliation_error is None
    assert lifecycle.close_event_count == 1
    assert lifecycle.realised_pnl_usd == Decimal("8.75")
    assert lifecycle.observed_fees_usd == Decimal("1.25")
    assert lifecycle.close_history_status == "complete"
    assert lifecycle.incomplete_reasons == []
    assert operation_id > 0


def test_fired_signal_keeps_open_owned_trade_distinct_from_closed_history(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "s1-time-series-momentum"
    strategy_version = _current_versions()[strategy_id]
    instrument_id = 2453006
    _instrument(ebull_test_conn, instrument_id)
    signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        signal_date="2026-08-04",
        fill_price=Decimal("12"),
    )
    trade_id = _funded_trade(
        ebull_test_conn,
        signal_id=signal_id,
        deployment_id=_deployment(ebull_test_conn, strategy_id, strategy_version),
        instrument_id=instrument_id,
    )
    ebull_test_conn.execute(
        "INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id) VALUES (%s, 7453006)",
        (trade_id,),
    )

    response = get_fired_signals(cursor=None, limit=50, strategy_id=strategy_id, conn=ebull_test_conn)
    matching = [item for item in response.items if item.signal_id == signal_id]
    assert len(matching) == 1
    lifecycle = matching[0].trade_lifecycle

    assert lifecycle is not None
    assert lifecycle.trade_status == "open"
    assert lifecycle.ownership_status == "active"
    assert lifecycle.close_event_count == 0
    assert lifecycle.realised_pnl_usd is None
    assert lifecycle.observed_fees_usd is None
    assert lifecycle.close_history_status == "not_closed"
    assert lifecycle.incomplete_reasons == []


def test_fired_signal_treats_failed_entry_without_position_as_terminal_not_missing(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "s1-time-series-momentum"
    strategy_version = _current_versions()[strategy_id]
    instrument_id = 2453011
    _instrument(ebull_test_conn, instrument_id)
    signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        signal_date="2026-08-04",
        fill_price=Decimal("12.50"),
    )
    trade_id = _funded_trade(
        ebull_test_conn,
        signal_id=signal_id,
        deployment_id=_deployment(ebull_test_conn, strategy_id, strategy_version),
        instrument_id=instrument_id,
    )
    ebull_test_conn.execute(
        "UPDATE strategy_trades SET status='failed' WHERE strategy_trade_id=%s",
        (trade_id,),
    )

    response = get_fired_signals(cursor=None, limit=50, strategy_id=strategy_id, conn=ebull_test_conn)
    matching = [item for item in response.items if item.signal_id == signal_id]
    assert len(matching) == 1
    lifecycle = matching[0].trade_lifecycle

    assert lifecycle is not None
    assert lifecycle.trade_status == "failed"
    assert lifecycle.ownership_count == 0
    assert lifecycle.broker_position_id is None
    assert lifecycle.close_history_status == "not_applicable"
    assert lifecycle.incomplete_reasons == []


def test_fired_signal_exposes_a_submitted_close_without_claiming_it_completed(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "s1-time-series-momentum"
    strategy_version = _current_versions()[strategy_id]
    instrument_id = 2453009
    _instrument(ebull_test_conn, instrument_id)
    signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        signal_date="2026-08-04",
        fill_price=Decimal("13"),
    )
    trade_id = _funded_trade(
        ebull_test_conn,
        signal_id=signal_id,
        deployment_id=_deployment(ebull_test_conn, strategy_id, strategy_version),
        instrument_id=instrument_id,
    )
    ownership_row = ebull_test_conn.execute(
        """
        INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id)
        VALUES (%s, 7453010) RETURNING ownership_id
        """,
        (trade_id,),
    ).fetchone()
    assert ownership_row is not None
    ownership_id = ownership_row[0]
    order_row = ebull_test_conn.execute(
        """
        INSERT INTO orders (
            broker_order_ref, instrument_id, action, order_type, requested_amount,
            status, execution_origin, strategy_request_id
        ) VALUES ('99010', %s, 'SELL', 'MARKET', 100, 'pending', 'strategy', %s)
        RETURNING order_id
        """,
        (instrument_id, uuid4()),
    ).fetchone()
    assert order_row is not None
    order_id = order_row[0]
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_position_operations (
            ownership_id, order_id, operation_type, trigger_code, request_id,
            status, broker_order_ref, created_at, submitted_at
        ) VALUES (%s, %s, 'close', 'strategy_exit', %s,
                  'submitted', 99010, now() - interval '1 minute', now())
        """,
        (ownership_id, order_id, uuid4()),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_order_reconciliation_state (
            order_id, state, last_attempt_at, attempt_count, broker_status,
            position_count, updated_at
        ) VALUES (%s, 'pending', now(), 1, 'pending', 1, now())
        """,
        (order_id,),
    )
    ebull_test_conn.execute(
        "UPDATE strategy_trades SET status='closing' WHERE strategy_trade_id=%s",
        (trade_id,),
    )

    response = get_fired_signals(cursor=None, limit=50, strategy_id=strategy_id, conn=ebull_test_conn)
    lifecycle = next(item for item in response.items if item.signal_id == signal_id).trade_lifecycle

    assert lifecycle is not None
    assert lifecycle.trade_status == "closing"
    assert lifecycle.latest_operation_type == "close"
    assert lifecycle.latest_operation_trigger == "strategy_exit"
    assert lifecycle.latest_operation_status == "submitted"
    assert lifecycle.latest_operation_resolved_at is None
    assert lifecycle.latest_reconciliation_state == "pending"
    assert lifecycle.latest_reconciliation_broker_status == "pending"
    assert lifecycle.close_history_status == "not_closed"
    assert lifecycle.realised_pnl_usd is None

    ebull_test_conn.execute(
        """
        UPDATE strategy_order_reconciliation_state
        SET state='ambiguous', broker_status='multiple_matches',
            last_error_code='multiple_position_executions', updated_at=now()
        WHERE order_id=%s
        """,
        (order_id,),
    )
    retried = get_fired_signals(cursor=None, limit=50, strategy_id=strategy_id, conn=ebull_test_conn)
    ambiguous = next(item for item in retried.items if item.signal_id == signal_id).trade_lifecycle
    assert ambiguous is not None
    assert ambiguous.latest_reconciliation_state == "ambiguous"
    assert ambiguous.latest_reconciliation_error == "multiple_position_executions"
    assert ambiguous.close_history_status == "not_closed"
    assert ambiguous.incomplete_reasons == ["position_operation_reconciliation_ambiguous"]

    ebull_test_conn.execute(
        """
        UPDATE strategy_order_reconciliation_state
        SET state='rejected', broker_status='rejected',
            reconciled_at=now(), last_error_code=NULL, updated_at=now()
        WHERE order_id=%s
        """,
        (order_id,),
    )
    rejected_response = get_fired_signals(
        cursor=None,
        limit=50,
        strategy_id=strategy_id,
        conn=ebull_test_conn,
    )
    rejected = next(item for item in rejected_response.items if item.signal_id == signal_id).trade_lifecycle
    assert rejected is not None
    assert rejected.latest_reconciliation_state == "rejected"
    assert rejected.latest_reconciliation_error is None
    assert rejected.incomplete_reasons == ["position_operation_reconciliation_rejected"]


def test_fired_signal_fails_closed_on_released_position_without_close_history(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "s1-time-series-momentum"
    strategy_version = _current_versions()[strategy_id]
    instrument_id = 2453007
    _instrument(ebull_test_conn, instrument_id)
    signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        signal_date="2026-08-05",
        fill_price=Decimal("14"),
    )
    trade_id = _funded_trade(
        ebull_test_conn,
        signal_id=signal_id,
        deployment_id=_deployment(ebull_test_conn, strategy_id, strategy_version),
        instrument_id=instrument_id,
    )
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_position_ownership (
            strategy_trade_id, broker_position_id, status, released_at, release_reason
        ) VALUES (%s, 7453007, 'released', now(), 'position_disappeared')
        """,
        (trade_id,),
    )
    ebull_test_conn.execute(
        "UPDATE strategy_trades SET status='closed' WHERE strategy_trade_id=%s",
        (trade_id,),
    )

    response = get_fired_signals(cursor=None, limit=50, strategy_id=strategy_id, conn=ebull_test_conn)
    lifecycle = next(item for item in response.items if item.signal_id == signal_id).trade_lifecycle

    assert lifecycle is not None
    assert lifecycle.close_history_status == "incomplete"
    assert lifecycle.realised_pnl_usd is None
    assert lifecycle.observed_fees_usd is None
    assert lifecycle.incomplete_reasons == ["released_position_missing_close_history"]


def test_fired_signal_never_returns_a_partial_close_money_pair(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "s1-time-series-momentum"
    strategy_version = _current_versions()[strategy_id]
    instrument_id = 2453012
    _instrument(ebull_test_conn, instrument_id)
    signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        signal_date="2026-08-05",
        fill_price=Decimal("15"),
    )
    trade_id = _funded_trade(
        ebull_test_conn,
        signal_id=signal_id,
        deployment_id=_deployment(ebull_test_conn, strategy_id, strategy_version),
        instrument_id=instrument_id,
    )
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_position_ownership (
            strategy_trade_id, broker_position_id, status, released_at, release_reason
        ) VALUES (%s, 7453012, 'released', now(), 'broker_close_observed')
        """,
        (trade_id,),
    )
    ebull_test_conn.execute(
        "UPDATE strategy_trades SET status='closed' WHERE strategy_trade_id=%s",
        (trade_id,),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO trade_events (
            position_id, etoro_instrument_id, instrument_id, event_kind, side,
            units, price, executed_at, fees_usd, realized_pnl_usd, source, raw_payload
        ) VALUES (7453012, %s, %s, 'close', 'sell', 1, 16, now(), 2, NULL,
                  'etoro_history', '{}'::jsonb)
        """,
        (instrument_id, instrument_id),
    )

    response = get_fired_signals(cursor=None, limit=50, strategy_id=strategy_id, conn=ebull_test_conn)
    lifecycle = next(item for item in response.items if item.signal_id == signal_id).trade_lifecycle

    assert lifecycle is not None
    assert lifecycle.close_event_count == 1
    assert lifecycle.close_history_status == "incomplete"
    assert lifecycle.realised_pnl_usd is None
    assert lifecycle.observed_fees_usd is None
    assert lifecycle.incomplete_reasons == ["realised_pnl_missing_from_history"]


def test_fired_signal_does_not_choose_between_ambiguous_position_owners(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "s1-time-series-momentum"
    strategy_version = _current_versions()[strategy_id]
    instrument_id = 2453008
    _instrument(ebull_test_conn, instrument_id)
    signal_id = _signal(
        ebull_test_conn,
        instrument_id=instrument_id,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        signal_date="2026-08-06",
        fill_price=Decimal("16"),
    )
    trade_id = _funded_trade(
        ebull_test_conn,
        signal_id=signal_id,
        deployment_id=_deployment(ebull_test_conn, strategy_id, strategy_version),
        instrument_id=instrument_id,
    )
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_position_ownership (
            strategy_trade_id, broker_position_id, status, released_at, release_reason
        ) VALUES
            (%s, 7453008, 'released', now(), 'first_claim_released'),
            (%s, 7453009, 'released', now(), 'second_claim_released')
        """,
        (trade_id, trade_id),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO trade_events (
            position_id, etoro_instrument_id, instrument_id, event_kind, side,
            units, price, executed_at, fees_usd, realized_pnl_usd, source, raw_payload
        ) VALUES
            (7453008, %s, %s, 'close', 'sell', 1, 17, now() - interval '1 minute',
             1, 4, 'etoro_history', '{}'::jsonb),
            (7453009, %s, %s, 'close', 'sell', 1, 18, now(),
             2, 5, 'etoro_history', '{}'::jsonb)
        """,
        (instrument_id, instrument_id, instrument_id, instrument_id),
    )

    response = get_fired_signals(cursor=None, limit=50, strategy_id=strategy_id, conn=ebull_test_conn)
    matching = [item for item in response.items if item.signal_id == signal_id]
    assert len(matching) == 1
    lifecycle = matching[0].trade_lifecycle

    assert lifecycle is not None
    assert lifecycle.ownership_count == 2
    assert lifecycle.broker_position_id is None
    assert lifecycle.ownership_status is None
    assert lifecycle.latest_operation_status is None
    assert lifecycle.close_history_status == "unavailable"
    assert lifecycle.close_event_count is None
    assert lifecycle.realised_pnl_usd is None
    assert lifecycle.observed_fees_usd is None
    assert lifecycle.incomplete_reasons == ["position_ownership_ambiguous"]


def _session(username: str = "allocation-operator") -> SessionRow:
    now = datetime.now(UTC)
    return SessionRow("test-session", uuid4(), username, now + timedelta(hours=1), now)


def test_shared_paper_pool_is_one_audited_human_event_and_overview_state(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ebull_test_conn
    actual_overview = get_strategy_overview

    def ready_overview(connection: psycopg.Connection[object]):
        overview = actual_overview(connection)
        overview.automation_readiness = overview.automation_readiness.model_copy(
            update={"ready": True, "state": "ready", "blockers": []}
        )
        return overview

    monkeypatch.setattr("app.api.strategies.get_strategy_overview", ready_overview)
    # runtime_config is a deliberately persistent singleton, unlike the
    # per-test pool event stream. Establish the transition this test asserts
    # when an earlier executor test enabled automation.
    runtime_before = get_runtime_config(conn)
    if runtime_before.enable_auto_trading or runtime_before.enable_live_trading:
        update_runtime_config(
            conn,
            updated_by="test-precondition",
            reason="establish paper-only automation precondition",
            enable_auto_trading=False if runtime_before.enable_auto_trading else None,
            enable_live_trading=False if runtime_before.enable_live_trading else None,
        )
    conn.commit()
    response = update_strategy_paper_pool(
        StrategyPaperPoolUpdateRequest(
            enabled=True,
            capital_limit=Decimal("750"),
            capital_mode="compound",
            risk_profile="balanced",
            reason="bounded paper workspace",
        ),
        _session(),
        conn,
    )

    assert response.enabled
    assert response.capital_limit == Decimal("750")
    assert response.capital_mode == "compound"
    assert response.mandate.configured
    assert response.mandate.policy_version == "portfolio-mandate-v1"
    assert response.mandate.risk_profile == "balanced"
    assert response.mandate.target_volatility_pct == Decimal("12")
    assert response.mandate.max_portfolio_drawdown_pct == Decimal("15")
    assert response.mandate.max_loss_per_position_pct == Decimal("0.75")
    assert response.mandate.max_daily_loss_pct == Decimal("1.5")
    assert response.mandate.active_risk_budget_pct == Decimal("20")
    assert response.mandate.cash_reserve_pct == Decimal("15")
    assert response.mandate.max_concurrent_positions == 8
    assert not response.mandate.shorts_allowed
    assert not response.mandate.leverage_allowed
    assert response.effective_capital == Decimal("750")
    assert response.remaining_capital == Decimal("750")
    assert conn.execute(
        "SELECT enabled,capital_limit,capital_mode,changed_by,reason FROM strategy_paper_pool_events"
    ).fetchone() == (True, Decimal("750.000000"), "compound", "allocation-operator", "bounded paper workspace")
    assert conn.execute(
        "SELECT enable_auto_trading,enable_live_trading FROM runtime_config WHERE id=TRUE"
    ).fetchone() == (False, False)
    assert conn.execute(
        """
        SELECT count(*)
        FROM runtime_config_audit
        WHERE field='enable_auto_trading'
          AND changed_by='allocation-operator'
          AND reason='bounded paper workspace'
        """
    ).fetchone() == (0,)
    conn.commit()

    with pytest.raises(HTTPException) as exc_info:
        update_strategy_paper_pool(
            StrategyPaperPoolUpdateRequest(
                enabled=True,
                capital_limit=Decimal("750"),
                capital_mode="compound",
                risk_profile="balanced",
                reason="no-op must not add audit noise",
            ),
            _session(),
            conn,
        )
    assert exc_info.value.status_code == 409
    assert conn.execute("SELECT count(*) FROM strategy_paper_pool_events").fetchone() == (1,)


def test_disabling_strategy_paper_does_not_disable_legacy_automation(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    runtime_before = get_runtime_config(conn)
    if not runtime_before.enable_auto_trading or runtime_before.enable_live_trading:
        update_runtime_config(
            conn,
            updated_by="test-precondition",
            reason="establish independently enabled legacy lane",
            enable_auto_trading=True if not runtime_before.enable_auto_trading else None,
            enable_live_trading=False if runtime_before.enable_live_trading else None,
        )
    configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("750"),
        risk_profile="balanced",
        changed_by="test-precondition",
        reason="establish enabled strategy lane",
    )
    conn.commit()
    try:
        response = update_strategy_paper_pool(
            StrategyPaperPoolUpdateRequest(
                enabled=False,
                capital_limit=Decimal("750"),
                capital_mode="fixed",
                risk_profile="balanced",
                reason="disable only the bounded strategy lane",
            ),
            _session(),
            conn,
        )

        assert not response.enabled
        assert conn.execute(
            "SELECT enable_auto_trading,enable_live_trading FROM runtime_config WHERE id=TRUE"
        ).fetchone() == (True, False)
    finally:
        current = get_runtime_config(conn)
        if (
            current.enable_auto_trading != runtime_before.enable_auto_trading
            or current.enable_live_trading != runtime_before.enable_live_trading
        ):
            update_runtime_config(
                conn,
                updated_by="test-cleanup",
                reason="restore runtime flags after lane-isolation proof",
                enable_auto_trading=(
                    runtime_before.enable_auto_trading
                    if current.enable_auto_trading != runtime_before.enable_auto_trading
                    else None
                ),
                enable_live_trading=(
                    runtime_before.enable_live_trading
                    if current.enable_live_trading != runtime_before.enable_live_trading
                    else None
                ),
            )
        conn.commit()


def test_shared_paper_pool_refuses_activation_outside_demo(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ebull_test_conn
    runtime_before = get_runtime_config(conn)
    monkeypatch.setattr("app.api.strategies.settings.etoro_env", "real")

    with pytest.raises(HTTPException) as exc_info:
        update_strategy_paper_pool(
            StrategyPaperPoolUpdateRequest(
                enabled=True,
                capital_limit=Decimal("750"),
                capital_mode="fixed",
                risk_profile="balanced",
                reason="must remain demo-only",
            ),
            _session(),
            conn,
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "paper automation can only be enabled in the demo environment"
    assert conn.execute("SELECT count(*) FROM strategy_paper_pool_events").fetchone() == (0,)
    assert get_runtime_config(conn) == runtime_before


def test_shared_paper_pool_refuses_activation_while_live_trading_is_enabled(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ebull_test_conn
    actual_overview = get_strategy_overview

    def ready_overview(connection: psycopg.Connection[object]):
        overview = actual_overview(connection)
        overview.automation_readiness = overview.automation_readiness.model_copy(
            update={"ready": True, "state": "ready", "blockers": []}
        )
        return overview

    monkeypatch.setattr("app.api.strategies.get_strategy_overview", ready_overview)
    runtime_before = get_runtime_config(conn)
    if runtime_before.enable_auto_trading or not runtime_before.enable_live_trading:
        update_runtime_config(
            conn,
            updated_by="test-precondition",
            reason="prove paper enable refuses live state",
            enable_auto_trading=False if runtime_before.enable_auto_trading else None,
            enable_live_trading=True if not runtime_before.enable_live_trading else None,
        )
    conn.commit()
    try:
        with pytest.raises(HTTPException) as exc_info:
            update_strategy_paper_pool(
                StrategyPaperPoolUpdateRequest(
                    enabled=True,
                    capital_limit=Decimal("750"),
                    capital_mode="fixed",
                    risk_profile="balanced",
                    reason="must remain paper-only",
                ),
                _session(),
                conn,
            )

        assert exc_info.value.status_code == 409
        assert exc_info.value.detail == ("paper automation cannot be enabled while system-wide live trading is enabled")
        assert conn.execute("SELECT count(*) FROM strategy_paper_pool_events").fetchone() == (0,)
        assert conn.execute(
            "SELECT enable_auto_trading,enable_live_trading FROM runtime_config WHERE id=TRUE"
        ).fetchone() == (False, True)
    finally:
        current = get_runtime_config(conn)
        if (
            current.enable_auto_trading != runtime_before.enable_auto_trading
            or current.enable_live_trading != runtime_before.enable_live_trading
        ):
            update_runtime_config(
                conn,
                updated_by="test-cleanup",
                reason="restore runtime flags after live-state refusal proof",
                enable_auto_trading=(
                    runtime_before.enable_auto_trading
                    if current.enable_auto_trading != runtime_before.enable_auto_trading
                    else None
                ),
                enable_live_trading=(
                    runtime_before.enable_live_trading
                    if current.enable_live_trading != runtime_before.enable_live_trading
                    else None
                ),
            )
        conn.commit()


def test_paper_enable_waits_for_concurrent_live_update_then_refuses(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ebull_test_conn
    runtime_before = get_runtime_config(conn)
    if runtime_before.enable_auto_trading or runtime_before.enable_live_trading:
        update_runtime_config(
            conn,
            updated_by="test-precondition",
            reason="establish live-off concurrency precondition",
            enable_auto_trading=False if runtime_before.enable_auto_trading else None,
            enable_live_trading=False if runtime_before.enable_live_trading else None,
        )
    conn.commit()
    monkeypatch.setattr(
        "app.api.strategies.get_strategy_overview",
        lambda _conn: SimpleNamespace(automation_readiness=SimpleNamespace(ready=True, blockers=[])),
    )
    blocker = psycopg.connect(test_database_url())
    worker = psycopg.connect(test_database_url())
    blocker.execute("UPDATE runtime_config SET enable_live_trading=TRUE WHERE id=TRUE")
    request = StrategyPaperPoolUpdateRequest(
        enabled=True,
        capital_limit=Decimal("750"),
        capital_mode="fixed",
        risk_profile="balanced",
        reason="must serialize behind live update",
    )
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(update_strategy_paper_pool, request, _session(), worker)
            with pytest.raises(FutureTimeoutError):
                future.result(timeout=0.1)
            blocker.commit()
            with pytest.raises(HTTPException) as exc_info:
                future.result(timeout=5)
        assert exc_info.value.status_code == 409
        assert exc_info.value.detail == ("paper automation cannot be enabled while system-wide live trading is enabled")
        assert conn.execute("SELECT count(*) FROM strategy_paper_pool_events").fetchone() == (0,)
    finally:
        blocker.rollback()
        worker.rollback()
        blocker.close()
        worker.close()
        conn.rollback()
        current = get_runtime_config(conn)
        if (
            current.enable_auto_trading != runtime_before.enable_auto_trading
            or current.enable_live_trading != runtime_before.enable_live_trading
        ):
            update_runtime_config(
                conn,
                updated_by="test-cleanup",
                reason="restore runtime flags after concurrency proof",
                enable_auto_trading=(
                    runtime_before.enable_auto_trading
                    if current.enable_auto_trading != runtime_before.enable_auto_trading
                    else None
                ),
                enable_live_trading=(
                    runtime_before.enable_live_trading
                    if current.enable_live_trading != runtime_before.enable_live_trading
                    else None
                ),
            )
        conn.commit()


def test_live_enable_waits_for_concurrent_paper_enable_then_refuses(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    runtime_before = get_runtime_config(conn)
    if runtime_before.enable_live_trading:
        update_runtime_config(
            conn,
            updated_by="test-precondition",
            reason="establish live-off reciprocal-race precondition",
            enable_live_trading=False,
        )
    conn.commit()
    blocker = psycopg.connect(test_database_url())
    worker = psycopg.connect(test_database_url())
    configure_paper_pool(
        blocker,
        enabled=True,
        capital_limit=Decimal("750"),
        risk_profile="balanced",
        changed_by="paper-operator",
        reason="concurrent bounded strategy enable",
    )
    request = ConfigPatchRequest(
        updated_by="live-operator",
        reason="must serialize behind strategy paper enable",
        enable_live_trading=True,
        confirm_live_enable=True,
    )
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(patch_config, request, worker)
            with pytest.raises(FutureTimeoutError):
                future.result(timeout=0.1)
            blocker.commit()
            with pytest.raises(HTTPException) as exc_info:
                future.result(timeout=5)
        assert exc_info.value.status_code == 409
        assert exc_info.value.detail == (
            "live trading cannot be enabled while strategy paper automation is enabled"
        )
        assert not get_runtime_config(conn).enable_live_trading
    finally:
        blocker.rollback()
        worker.rollback()
        blocker.close()
        worker.close()
        conn.rollback()
        current = get_runtime_config(conn)
        if current.enable_live_trading != runtime_before.enable_live_trading:
            update_runtime_config(
                conn,
                updated_by="test-cleanup",
                reason="restore runtime live flag after reciprocal-race proof",
                enable_live_trading=runtime_before.enable_live_trading,
            )
        conn.commit()


def test_shared_paper_pool_refuses_activation_without_a_ready_candidate(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    if get_runtime_config(conn).enable_auto_trading:
        update_runtime_config(
            conn,
            updated_by="test-precondition",
            reason="establish disabled automation precondition",
            enable_auto_trading=False,
        )
    conn.commit()

    with pytest.raises(HTTPException) as exc_info:
        update_strategy_paper_pool(
            StrategyPaperPoolUpdateRequest(
                enabled=True,
                capital_limit=Decimal("750"),
                capital_mode="fixed",
                risk_profile="balanced",
                reason="must fail without capital authority",
            ),
            _session(),
            conn,
        )

    assert exc_info.value.status_code == 409
    assert "no_capital_candidates" in str(exc_info.value.detail)
    assert conn.execute("SELECT count(*) FROM strategy_paper_pool_events").fetchone() == (0,)


def test_enabled_paper_pool_refuses_more_authority_while_readiness_is_degraded(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ebull_test_conn
    configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("500"),
        capital_mode="fixed",
        risk_profile="cautious",
        changed_by="test-precondition",
        reason="establish bounded enabled pool",
    )
    conn.commit()
    monkeypatch.setattr(
        "app.api.strategies.get_strategy_overview",
        lambda _conn: SimpleNamespace(
            automation_readiness=SimpleNamespace(ready=False, blockers=["prospective_assessment_stale"])
        ),
    )

    with pytest.raises(HTTPException) as exc_info:
        update_strategy_paper_pool(
            StrategyPaperPoolUpdateRequest(
                enabled=True,
                capital_limit=Decimal("750"),
                capital_mode="compound",
                risk_profile="balanced",
                reason="must not enlarge dormant authority",
            ),
            _session(),
            conn,
        )

    assert exc_info.value.status_code == 409
    assert "prospective_assessment_stale" in str(exc_info.value.detail)
    assert conn.execute("SELECT count(*) FROM strategy_paper_pool_events").fetchone() == (1,)


def test_enabled_paper_pool_allows_risk_reduction_while_readiness_is_degraded(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ebull_test_conn
    configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("750"),
        capital_mode="compound",
        risk_profile="balanced",
        changed_by="test-precondition",
        reason="establish enabled pool",
    )
    conn.commit()
    monkeypatch.setattr(
        "app.api.strategies.get_strategy_overview",
        lambda _conn: SimpleNamespace(
            automation_readiness=SimpleNamespace(ready=False, blockers=["prospective_assessment_stale"]),
            paper_pool=load_paper_pool(_conn),
        ),
    )

    response = update_strategy_paper_pool(
        StrategyPaperPoolUpdateRequest(
            enabled=True,
            capital_limit=Decimal("500"),
            capital_mode="fixed",
            risk_profile="cautious",
            reason="reduce authority while evidence is stale",
        ),
        _session(),
        conn,
    )

    assert response.enabled
    assert response.capital_limit == Decimal("500")
    assert response.capital_mode == "fixed"
    assert response.mandate.risk_profile == "cautious"


def test_evidence_refresh_queues_one_fixed_pinned_request(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    conn.commit()

    first = request_evidence_refresh(_session("research-operator"), conn)
    second = request_evidence_refresh(_session("research-operator"), conn)

    assert first.status == "queued"
    assert not first.already_active
    assert second.request_id == first.request_id
    assert second.already_active
    row = conn.execute(
        "SELECT requested_by,payload FROM pending_job_requests WHERE request_id=%s",
        (first.request_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == "research-operator"
    assert row[1]["params"] == {
        "refresh_recent": True,
        "holdout_purpose": "complete declared recent-regime evidence denominator",
        "holdout_accessed_by": "research-operator",
    }


def test_missing_evidence_refuses_new_allocation_without_writing_audit(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "s1-time-series-momentum"
    version = _current_versions()[strategy_id]
    ebull_test_conn.commit()

    with pytest.raises(HTTPException) as exc_info:
        update_strategy_allocation(
            strategy_id,
            AllocationUpdateRequest(
                strategy_version=version,
                capital_limit=Decimal("100"),
                enabled=False,
                reason="test unavailable evidence",
            ),
            _session(),
            ebull_test_conn,
        )

    assert exc_info.value.status_code == 409
    assert isinstance(exc_info.value.detail, dict)
    assert exc_info.value.detail.get("reason") == "allocation_unavailable"
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_deployments WHERE strategy_id=%s", (strategy_id,)
    ).fetchone() == (0,)
    assert ebull_test_conn.execute("SELECT count(*) FROM strategy_deployment_events").fetchone() == (0,)


def test_evidence_invalid_enabled_allocation_can_reduce_without_disabling(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "s1-time-series-momentum"
    version = _current_versions()[strategy_id]
    deployment_id = _deployment(ebull_test_conn, strategy_id, version)
    # ⚠ This seeded `currency='EUR'` until #2648, to prove the response echoes the
    # STORED currency rather than a hardcoded 'USD'. `sql/338` (#2363, USD-only
    # while FX is unmodelled) landed after the fixture and makes a non-USD row
    # unrepresentable at rest, so the discriminating value no longer exists to seed.
    # What replaced that coverage: `tests/test_strategy_control_plane.py::
    # test_an_unsupported_currency_is_unrepresentable_at_rest` (the constraint, by
    # name, on both the current-state and event tables) and the ` usd ` →
    # `USD` normalisation case above it.
    ebull_test_conn.execute(
        "UPDATE strategy_deployments SET enabled=true WHERE deployment_id=%s",
        (deployment_id,),
    )
    ebull_test_conn.commit()

    response = update_strategy_allocation(
        strategy_id,
        AllocationUpdateRequest(
            strategy_version=version,
            capital_limit=Decimal("500"),
            enabled=True,
            reason="reduce risk while evidence is unavailable",
        ),
        _session(),
        ebull_test_conn,
    )

    assert response.enabled
    assert response.capital_limit == Decimal("500")
    assert response.currency == "USD"
    assert ebull_test_conn.execute(
        "SELECT capital_limit, currency, enabled FROM strategy_deployment_events WHERE deployment_id=%s",
        (deployment_id,),
    ).fetchone() == (Decimal("500.000000"), "USD", True)


def test_disabled_pre_promotion_deployment_remains_visible_for_risk_reduction(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "s1-time-series-momentum"
    version = _current_versions()[strategy_id]
    deployment_id = _deployment(ebull_test_conn, strategy_id, version)

    state = load_control_state(ebull_test_conn, versions=[version])[(strategy_id, version)]

    assert state.stage is None
    assert state.deployment_id == deployment_id
    assert not state.enabled


def test_missing_runtime_singleton_is_visible_as_an_entry_block(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    ebull_test_conn.execute("DELETE FROM runtime_config")

    state = load_entry_block_state(ebull_test_conn)

    assert state.new_entries_blocked
    assert state.execution_block_reasons == ("runtime configuration unavailable",)
    assert not state.auto_trading_enabled


def test_legacy_automatic_trading_switch_does_not_block_the_strategy_lane(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    ebull_test_conn.execute("UPDATE runtime_config SET enable_auto_trading=false")

    state = load_entry_block_state(ebull_test_conn)

    assert not state.new_entries_blocked
    assert "automatic trading disabled" not in state.execution_block_reasons
    assert not state.auto_trading_enabled


def test_operator_allocation_uses_session_identity_and_immutable_event(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.services import strategy_control_plane

    strategy_id = "s1-time-series-momentum"
    version = _current_versions()[strategy_id]
    admitted_manifest: dict[str, Any] = dict(strategy_control_plane.STRATEGY_MANIFEST)
    admitted_manifest[strategy_id] = SimpleNamespace(purpose="capital_candidate")
    monkeypatch.setattr(strategy_control_plane, "STRATEGY_MANIFEST", admitted_manifest)
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_promotions (
            strategy_id, strategy_version, from_stage, to_stage, gate_version,
            evidence_ref, promoted_by, reason
        ) VALUES
          (%s, %s, NULL, 'research_candidate', 'test-v1', NULL, 'test', 'registered'),
          (%s, %s, 'research_candidate', 'historical_validated', 'test-v1', 'e:hist', 'test', 'validated'),
          (%s, %s, 'historical_validated', 'forward_observation', 'test-v1', 'e:fwd', 'test', 'observe'),
          (%s, %s, 'forward_observation', 'paper_enabled', 'test-v1', 'e:paper', 'test', 'paper')
        """,
        (strategy_id, version, strategy_id, version, strategy_id, version, strategy_id, version),
    )
    overview = get_strategy_overview(ebull_test_conn)
    strategy = next(row for row in overview.strategies if row.strategy_id == strategy_id)
    strategy.allocation_ready = True
    strategy.allocation_refusals = []
    monkeypatch.setattr("app.api.strategies.get_strategy_overview", lambda _conn: overview)
    ebull_test_conn.commit()

    response = update_strategy_allocation(
        strategy_id,
        AllocationUpdateRequest(
            strategy_version=version,
            capital_limit=Decimal("250"),
            enabled=True,
            reason="bounded paper sleeve",
        ),
        _session("real-session-user"),
        ebull_test_conn,
    )

    assert response.enabled
    assert response.capital_limit == Decimal("250")
    assert ebull_test_conn.execute(
        """
        SELECT capital_limit, enabled, changed_by, reason
        FROM strategy_deployment_events WHERE deployment_id=%s
        """,
        (response.deployment_id,),
    ).fetchone() == (Decimal("250.000000"), True, "real-session-user", "bounded paper sleeve")
