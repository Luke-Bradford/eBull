"""#2450 bounded recurring paper lifecycle tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest

from app.services.strategy_live_gate import assess_live_gate
from app.services.strategy_paper_runtime import refresh_strategy_health, run_strategy_paper_cycle
from tests.test_strategy_paper_executor import _NOW, _REQUEST_ID, _broker, _seed
from tests.test_strategy_position_manager import _opened_trade

pytestmark = pytest.mark.integration


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


def test_broker_outage_and_drawdown_unknown_block_entries_without_row_growth(
    ebull_test_conn: psycopg.Connection[tuple],
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

    conn.execute("UPDATE strategy_deployments SET enabled=false WHERE mode='paper'")
    conn.commit()
    assert refresh_strategy_health(conn, broker=broker, now=_NOW) == 0
    assert conn.execute("SELECT count(*) FROM strategy_execution_blocks WHERE active").fetchone() == (0,)


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
