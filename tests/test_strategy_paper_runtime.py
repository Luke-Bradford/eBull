"""#2450 bounded recurring paper lifecycle tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest
from psycopg.pq import TransactionStatus

from app.services.cost_model import COST_MODEL_ID
from app.services.strategy_live_gate import assess_live_gate
from app.services.strategy_opportunity_forecast import OpportunityForecast, record_opportunity_forecast
from app.services.strategy_opportunity_ranker import persist_ranking_batch
from app.services.strategy_paper_executor import execute_fired_paper_signal
from app.services.strategy_paper_runtime import (
    _load_ranked_opportunities,
    refresh_strategy_health,
    run_strategy_paper_cycle,
)
from tests.test_strategy_paper_executor import _NOW, _REQUEST_ID, _authorise_forecast_scope, _broker, _seed
from tests.test_strategy_position_manager import _opened_trade

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("registered_strategy_test_candidates")]


def _add_weaker_newer_forecast(conn: psycopg.Connection[tuple]) -> int:
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id,symbol,company_name,exchange,currency,is_tradable
        ) VALUES (2449002,'TWEAK','Weaker newer test','2','USD',true)
        """
    )
    signal = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id,strategy_version,instrument_id,signal_bar_date,
            signal_kind,verdict,fill_bar_date,fill_price,universe,
            input_rule_set_versions,created_at
        ) VALUES (
            'S-ALLOC','v1',2449002,'2026-08-05','entry','fired',
            '2026-08-06',100,'survivor_only','{"indicator_series":"rules-v1"}'::jsonb,
            (SELECT max(promoted_at)+interval '1 second' FROM strategy_promotions
             WHERE strategy_id='S-ALLOC' AND strategy_version='v1'
               AND to_stage='paper_enabled')
        ) RETURNING signal_id
        """
    ).fetchone()
    assert signal is not None
    record_opportunity_forecast(
        conn,
        OpportunityForecast(
            signal_id=int(signal[0]),
            decided_at=_NOW,
            valid_through=_NOW + timedelta(days=7),
            horizon_market_days=5,
            target_barrier_pct=Decimal("10"),
            stop_barrier_pct=Decimal("5"),
            setup_version="weaker-setup-v1",
            exit_policy_version="test-exit-v1",
            calibration_id="test-calibration-v1",
            target_probability=Decimal("0.4"),
            stop_probability=Decimal("0.3"),
            timeout_probability=Decimal("0.3"),
            target_net_return_pct=Decimal("4"),
            stop_net_return_pct=Decimal("-2"),
            timeout_net_return_pct=Decimal("0"),
            expected_duration_hours=Decimal("24"),
            uncertainty_penalty_pct=Decimal("0.2"),
            tail_penalty_pct=Decimal("0.1"),
            correlation_penalty_pct=Decimal("0.1"),
            cost_stress_penalty_pct=Decimal("0.1"),
            conservative_net_expectancy_pct=Decimal("0.5"),
            cost_model_id=COST_MODEL_ID,
        ),
    )
    _authorise_forecast_scope(conn, setup_version="weaker-setup-v1")
    conn.commit()
    return int(signal[0])


def test_cycle_refreshes_health_then_executes_one_current_paper_candidate(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    broker = _broker()
    monkeypatch.setattr("app.services.strategy_order_reconciliation.uuid4", lambda: _REQUEST_ID)

    result = run_strategy_paper_cycle(
        conn,
        broker=broker,
        now=_NOW,
        strategy_versions=["v1"],
    )

    assert result.reconciled_orders == 0
    assert result.managed_positions == 0
    assert result.evaluated_signals == 1
    assert result.active_health_blocks == 0
    assert conn.execute(
        "SELECT verdict FROM strategy_funding_decisions WHERE signal_id=%s", (signal_id,)
    ).fetchone() == ("allocated",)
    assert conn.execute("SELECT source,active FROM strategy_execution_blocks ORDER BY source").fetchall() == [
        ("broker_availability", False),
        ("drawdown", False),
        ("order_reconciliation", False),
        ("quote_freshness", False),
        ("scan_freshness", False),
    ]


def test_runtime_ranks_the_complete_set_before_applying_its_execution_limit(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    stronger_older = _seed(conn)
    weaker_newer = _add_weaker_newer_forecast(conn)
    assert weaker_newer > stronger_older
    executed: list[int] = []
    monkeypatch.setattr(
        "app.services.strategy_paper_runtime.execute_fired_paper_signal",
        lambda _conn, *, broker, signal_id, ranking_member_id, now: executed.append(signal_id),
    )

    result = run_strategy_paper_cycle(
        conn,
        broker=_broker(),
        signal_limit=1,
        position_limit=1,
        now=_NOW,
        strategy_versions=["v1"],
    )

    assert result.evaluated_signals == 2
    assert executed == [stronger_older, weaker_newer]
    assert conn.execute("SELECT count(*) FROM strategy_opportunity_ranking_batches").fetchone() == (2,)
    assert conn.execute(
        """
        SELECT s.signal_id,m.rank,m.selected,m.reason_code
        FROM strategy_opportunity_ranking_members m
        JOIN strategy_opportunity_forecasts f ON f.forecast_id=m.forecast_id
        JOIN strategy_signals s ON s.signal_id=f.signal_id
        WHERE m.ranking_batch_id=(
          SELECT max(ranking_batch_id) FROM strategy_opportunity_ranking_batches
        ) ORDER BY m.rank
        """
    ).fetchall() == [
        (stronger_older, 1, True, "selected_for_execution"),
        (weaker_newer, 2, False, "below_execution_batch_limit"),
    ]
    conn.commit()

    run_strategy_paper_cycle(
        conn,
        broker=_broker(),
        signal_limit=1,
        position_limit=1,
        now=_NOW,
        strategy_versions=["v1"],
    )
    assert conn.execute("SELECT count(*) FROM strategy_opportunity_ranking_batches").fetchone() == (2,)
    assert conn.execute("SELECT count(*) FROM strategy_opportunity_ranking_members").fetchone() == (3,)


def test_declined_ranking_member_cannot_reach_broker_access(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    weaker_signal = _add_weaker_newer_forecast(conn)
    ranked = _load_ranked_opportunities(conn, strategy_versions=["v1"], observed_at=_NOW)
    persist_ranking_batch(conn, opportunities=ranked, selection_limit=1, decided_at=_NOW)
    declined = conn.execute(
        """
        SELECT m.ranking_member_id
        FROM strategy_opportunity_ranking_members m
        JOIN strategy_opportunity_forecasts f ON f.forecast_id=m.forecast_id
        WHERE f.signal_id=%s AND NOT m.selected
        """,
        (weaker_signal,),
    ).fetchone()
    assert declined is not None
    conn.commit()
    broker = _broker()

    result = execute_fired_paper_signal(
        conn,
        broker=broker,
        signal_id=weaker_signal,
        ranking_member_id=int(declined[0]),
        now=_NOW,
    )

    assert result.reason_code == "opportunity_ranking_member_not_selected"
    broker.get_account_risk_snapshot.assert_not_called()
    broker.place_demo_strategy_order.assert_not_called()


def test_stale_forecast_cannot_consume_the_bounded_ranked_set(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    current_signal = _seed(conn)
    stale_signal = _add_weaker_newer_forecast(conn)
    conn.execute(
        """
        UPDATE strategy_opportunity_forecasts
        SET decided_at=%s,valid_through=%s
        WHERE signal_id=%s
        """,
        (_NOW - timedelta(days=2), _NOW - timedelta(days=1), stale_signal),
    )
    conn.commit()

    ranked = _load_ranked_opportunities(conn, strategy_versions=["v1"], observed_at=_NOW)

    assert [opportunity.signal_id for opportunity in ranked] == [current_signal]
    assert conn.info.transaction_status == TransactionStatus.IDLE


def test_broker_outage_and_drawdown_unknown_block_entries_without_row_growth(
    ebull_test_conn: psycopg.Connection[tuple],
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    broker = _broker()
    broker.get_account_risk_snapshot.side_effect = RuntimeError("simulated outage")

    first = refresh_strategy_health(conn, broker=broker, now=_NOW)
    first_count = conn.execute("SELECT count(*) FROM strategy_execution_blocks").fetchone()
    second = refresh_strategy_health(conn, broker=broker, now=_NOW)
    second_count = conn.execute("SELECT count(*) FROM strategy_execution_blocks").fetchone()

    assert first == second == 2
    assert first_count == second_count == (5,)
    assert conn.execute(
        "SELECT active FROM strategy_execution_blocks WHERE source='broker_availability'"
    ).fetchone() == (True,)
    assert conn.execute("SELECT active FROM strategy_execution_blocks WHERE source='drawdown'").fetchone() == (True,)
    assert "strategy broker account-risk probe unavailable" in caplog.text

    conn.execute("UPDATE strategy_deployments SET enabled=false WHERE mode='paper'")
    conn.commit()
    assert refresh_strategy_health(conn, broker=broker, now=_NOW) == 0
    assert conn.execute("SELECT count(*) FROM strategy_execution_blocks WHERE active").fetchone() == (0,)


def test_health_refresh_ends_read_transaction_before_broker_call(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    broker = _broker()
    snapshot = broker.get_account_risk_snapshot.return_value

    def observe_broker_call() -> object:
        assert conn.info.transaction_status == TransactionStatus.IDLE
        return snapshot

    broker.get_account_risk_snapshot.side_effect = observe_broker_call

    refresh_strategy_health(conn, broker=broker, now=_NOW)

    assert conn.info.transaction_status == TransactionStatus.IDLE


def test_cycle_does_not_fund_a_signal_created_before_paper_promotion(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    conn.execute(
        """
        UPDATE strategy_signals
        SET created_at=(
          SELECT promoted_at-interval '1 second'
          FROM strategy_promotions
          WHERE strategy_id='S-ALLOC' AND strategy_version='v1' AND to_stage='paper_enabled'
        )
        WHERE signal_id=%s
        """,
        (signal_id,),
    )
    conn.commit()
    monkeypatch.setattr("app.services.strategy_order_reconciliation.uuid4", lambda: _REQUEST_ID)

    result = run_strategy_paper_cycle(conn, broker=_broker(), now=_NOW, strategy_versions=["v1"])

    assert result.evaluated_signals == 0
    assert conn.execute(
        "SELECT count(*) FROM strategy_funding_decisions WHERE signal_id=%s", (signal_id,)
    ).fetchone() == (0,)


def test_cycle_keeps_managing_owned_positions_after_deployment_is_paused(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, _deployment_id, broker, _manual = _opened_trade(conn, monkeypatch)
    position_row = conn.execute(
        "SELECT broker_position_id FROM strategy_position_ownership WHERE strategy_trade_id=%s",
        (trade_id,),
    ).fetchone()
    assert position_row is not None
    position_id = int(position_row[0])
    conn.execute(
        """
        UPDATE strategy_deployments d
        SET enabled=false
        FROM strategy_funding_decisions fd, strategy_trades t
        WHERE fd.deployment_id=d.deployment_id
          AND t.funding_decision_id=fd.funding_decision_id
          AND t.strategy_trade_id=%s
        """,
        (trade_id,),
    )
    conn.commit()
    managed: list[tuple[int, int]] = []

    def observe_management(
        _conn: psycopg.Connection[tuple],
        *,
        broker: object,
        strategy_trade_id: int,
        broker_position_id: int,
        now: object,
    ) -> None:
        managed.append((strategy_trade_id, broker_position_id))

    monkeypatch.setattr("app.services.strategy_paper_runtime.manage_owned_position", observe_management)

    result = run_strategy_paper_cycle(conn, broker=broker, strategy_versions=["no-candidates"])

    assert result.managed_positions == 1
    assert managed == [(trade_id, position_id)]


def test_health_refresh_persists_account_and_deployment_max_drawdown(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    broker = _broker()
    observed = datetime.now(UTC)
    initial = broker.get_account_risk_snapshot.return_value

    broker.get_account_risk_snapshot.return_value = replace(initial, equity=Decimal("1000"), observed_at=observed)
    refresh_strategy_health(conn, broker=broker, now=observed)
    broker.get_account_risk_snapshot.return_value = replace(
        initial,
        equity=Decimal("1200"),
        observed_at=observed + timedelta(minutes=1),
    )
    refresh_strategy_health(conn, broker=broker, now=observed + timedelta(minutes=1))
    broker.get_account_risk_snapshot.return_value = replace(
        initial,
        equity=Decimal("1100"),
        observed_at=observed + timedelta(minutes=2),
    )
    refresh_strategy_health(conn, broker=broker, now=observed + timedelta(minutes=2))

    account = conn.execute(
        "SELECT equity_high_water,last_equity,last_drawdown_pct FROM strategy_paper_account_risk_state"
    ).fetchone()
    deployment = conn.execute(
        "SELECT equity_high_water,last_equity,max_drawdown_pct FROM strategy_paper_deployment_risk_state"
    ).fetchone()
    assert account is not None and deployment is not None
    assert account[0:2] == (Decimal("1200.000000"), Decimal("1100.000000"))
    assert deployment[0:2] == (Decimal("1200.000000"), Decimal("1100.000000"))
    assert Decimal(str(account[2])) > Decimal("8.33")
    assert Decimal(str(deployment[2])) > Decimal("8.33")

    report = assess_live_gate(
        conn,
        strategy_id="S-ALLOC",
        strategy_version="v1",
        requested_capital=Decimal("1"),
        now=observed + timedelta(minutes=2),
    )
    assert report.facts.max_observed_drawdown_pct == deployment[2]
