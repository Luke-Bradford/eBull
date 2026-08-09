"""#2449 demo allocator/executor integration tests against real Postgres."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from decimal import Decimal
from threading import Barrier, Event
from time import sleep
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import psycopg
import pytest

from app.providers.broker import (
    BrokerAccountRiskSnapshot,
    BrokerCostComponent,
    BrokerEligibilityResponse,
    BrokerInstrumentEligibility,
    BrokerInstrumentInvestment,
    BrokerLeverageConfig,
    BrokerOrderSubmission,
    BrokerOrderSubmissionUncertain,
    BrokerProvider,
    BrokerWhatIfCostResponse,
)
from app.services.result_ledger import store_holdout_result
from app.services.strategy_control_plane import (
    configure_deployment,
    configure_execution_policy,
    create_strategy_trade,
    decide_funding,
    link_strategy_order,
)
from app.services.strategy_paper_executor import PaperExecutionResult, execute_fired_paper_signal
from tests.fixtures.ebull_test_db import test_database_url
from tests.test_result_ledger import (
    BOOTSTRAP_BLOCK,
    build_control,
    build_deflated,
    build_metrics,
    build_result,
)

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 8, 7, 15, 0, tzinfo=UTC)  # Friday 11:00 New York
_REQUEST_ID = UUID("1c94300c-90aa-4303-9d00-dec376d74efb")


def _seed(conn: psycopg.Connection[Any], *, auto: bool = True) -> int:
    conn.execute(
        "INSERT INTO exchanges (exchange_id, country, asset_class) VALUES ('2', 'US', 'us_equity') "
        "ON CONFLICT (exchange_id) DO UPDATE SET asset_class='us_equity'"
    )
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency,
            is_tradable
        ) VALUES (2449001, 'TALLOC', 'Allocator test', '2', 'USD', true)
        """
    )
    metrics = build_metrics(
        **{
            **BOOTSTRAP_BLOCK,
            "expectancy_ci_low_pct": 5.0,
            "expectancy_ci_high_pct": 6.0,
        }
    )
    deflated = build_deflated()
    result_id = store_holdout_result(
        conn,
        build_result(
            strategy_id="S-ALLOC",
            strategy_version="v1",
            namespace="hold_out",
            window_start=date(2022, 1, 1),
            universe_basis="survivorship_free",
            carry_unmodelled=False,
            metrics=metrics,
            deflated=deflated,
            trial_count=deflated.declared_trials,
            deflated_sharpe=Decimal(repr(deflated.deflated_sharpe)),
            synthetic_control=build_control(
                metrics,
                mean_return_pct=0.0,
                mean_return_ci_low_pct=-1.0,
                mean_return_ci_high_pct=1.0,
                cohort_sharpe_threshold=-4.0,
                cohort_return_threshold_pct=-101.0,
            ),
        ),
        accessed_by="tests/test_strategy_paper_executor.py",
        purpose="paper allocation evidence fixture",
    )
    promotion_rows = conn.execute(
        """
        INSERT INTO strategy_promotions (
            strategy_id, strategy_version, from_stage, to_stage, gate_version,
            evidence_ref, promoted_by, reason
        ) VALUES
          ('S-ALLOC', 'v1', NULL, 'research_candidate', 'test-v1', NULL, 'test', 'registered'),
          ('S-ALLOC', 'v1', 'research_candidate', 'historical_validated', 'test-v1', 'e:h', 'test', 'historical'),
          ('S-ALLOC', 'v1', 'historical_validated', 'forward_observation', 'test-v1', 'e:f', 'test', 'forward'),
          ('S-ALLOC', 'v1', 'forward_observation', 'paper_enabled', 'test-v1', 'e:p', 'test', 'paper')
        RETURNING promotion_id, to_stage
        """
    ).fetchall()
    historical_id = next(int(row[0]) for row in promotion_rows if row[1] == "historical_validated")
    conn.execute(
        "INSERT INTO strategy_promotion_results (promotion_id, result_id) VALUES (%s, %s)",
        (historical_id, result_id),
    )
    deployment = configure_deployment(
        conn,
        strategy_id="S-ALLOC",
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal("1000"),
        enabled=True,
        changed_by="test",
        reason="paper allocation test",
    )
    configure_execution_policy(
        conn,
        deployment_id=deployment.deployment_id,
        ticket_fraction=Decimal("0.20"),
        max_ticket_amount=Decimal("500"),
        stop_loss_pct=Decimal("5"),
        take_profit_pct=Decimal("10"),
        max_quote_age_seconds=60,
        max_scan_age_seconds=60,
        max_halt_feed_age_seconds=60,
        max_cost_age_seconds=60,
        max_reconciliation_age_seconds=60,
        max_instrument_exposure_pct=Decimal("30"),
        max_portfolio_exposure_pct=Decimal("80"),
        max_drawdown_pct=Decimal("10"),
        min_net_expectancy_pct=Decimal("1"),
        cost_stress_multiplier=Decimal("2"),
        changed_by="test",
        reason="explicit test limits",
    )
    signal = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id, strategy_version, instrument_id, signal_bar_date,
            signal_kind, verdict, fill_bar_date, fill_price, universe,
            input_rule_set_versions
        ) VALUES ('S-ALLOC', 'v1', 2449001, '2026-08-05', 'entry', 'fired',
                  '2026-08-06', 100, 'survivor_only',
                  '{"indicator_series":"rules-v1"}'::jsonb)
        RETURNING signal_id
        """
    ).fetchone()
    assert signal is not None
    conn.execute(
        "INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last, spread_pct, spread_flag) "
        "VALUES (2449001, %s, 99, 100, 99.5, 1, false)",
        (_NOW,),
    )
    conn.execute(
        "INSERT INTO strategy_scan_watermark (strategy_id, strategy_version, frontier_date, updated_at) "
        "VALUES ('S-ALLOC', 'v1', '2026-08-07', %s) "
        "ON CONFLICT (strategy_id, strategy_version) DO UPDATE SET "
        "frontier_date=EXCLUDED.frontier_date, updated_at=EXCLUDED.updated_at",
        (_NOW,),
    )
    conn.execute(
        """
        INSERT INTO strategy_halt_feed_state (
            source, fetched_at, source_pub_at, item_count, payload_sha256
        ) VALUES ('nasdaq_trader_rss', %s, %s, 0, %s)
        ON CONFLICT (source) DO UPDATE SET
            fetched_at=EXCLUDED.fetched_at, source_pub_at=EXCLUDED.source_pub_at,
            item_count=EXCLUDED.item_count, payload_sha256=EXCLUDED.payload_sha256
        """,
        (_NOW, _NOW, "0" * 64),
    )
    conn.execute(
        """
        INSERT INTO runtime_config (
            id, enable_auto_trading, enable_live_trading, updated_by, reason
        ) VALUES (true, %s, true, 'test', 'paper allocator fixture')
        ON CONFLICT (id) DO UPDATE SET
            enable_auto_trading=EXCLUDED.enable_auto_trading,
            enable_live_trading=EXCLUDED.enable_live_trading,
            updated_by=EXCLUDED.updated_by, reason=EXCLUDED.reason,
            updated_at=now()
        """,
        (auto,),
    )
    conn.execute(
        """
        INSERT INTO kill_switch (id, is_active)
        VALUES (true, false)
        ON CONFLICT (id) DO UPDATE SET
            is_active=false, activated_at=NULL, activated_by=NULL, reason=NULL
        """
    )
    conn.commit()
    return int(signal[0])


def _broker(*, undocumented_cost: bool = False) -> MagicMock:
    broker = MagicMock(spec=BrokerProvider)
    broker.get_account_risk_snapshot.return_value = BrokerAccountRiskSnapshot(
        available_cash=Decimal("600"),
        total_invested=Decimal("400"),
        unrealized_pnl=Decimal("0"),
        equity=Decimal("1000"),
        instrument_investments=(BrokerInstrumentInvestment(2449001, Decimal("250")),),
        observed_at=_NOW,
        raw_payload={},
    )
    broker.check_instrument_eligibility.return_value = BrokerEligibilityResponse(
        currency="USD",
        eligibilities=(
            BrokerInstrumentEligibility(
                instrument_id=2449001,
                symbol="TALLOC",
                min_position_exposure=Decimal("10"),
                max_units_per_order=None,
                allow_open_position=True,
                allow_close_position=True,
                allow_partial_close_position=True,
                allow_trailing_stop_loss=False,
                leverage_configs=(
                    BrokerLeverageConfig(
                        settlement_type="real",
                        direction="LONG",
                        leverage_values=(1,),
                        min_position_amount=Decimal("10"),
                        allow_edit_stop_loss=True,
                        allow_edit_take_profit=True,
                        allow_stop_loss_take_profit=True,
                        raw_payload={},
                    ),
                ),
                raw_payload={},
            ),
        ),
        not_found_instrument_ids=(),
        not_found_symbols=(),
        raw_payload={},
    )
    broker.get_what_if_costs.return_value = BrokerWhatIfCostResponse(
        instrument_id=2449001,
        symbol="TALLOC",
        costs=(
            BrokerCostComponent(
                cost_type="marketSpread",
                amount=None if undocumented_cost else Decimal("0.5"),
                value=Decimal("0.5") if undocumented_cost else None,
                currency="USD",
                raw_payload={},
            ),
        ),
        last_updated=_NOW,
        raw_payload={},
    )
    broker.place_demo_strategy_order.return_value = BrokerOrderSubmission(
        broker_order_ref="13902598",
        reference_id=_REQUEST_ID,
        token=UUID("066faaee-e1e9-49d2-a568-c6e1cc336ad8"),
    )
    return broker


def test_allocation_counts_manual_risk_and_commits_identity_before_demo_io(
    ebull_test_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    broker = _broker()
    monkeypatch.setattr("app.services.strategy_order_reconciliation.uuid4", lambda: _REQUEST_ID)

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.verdict == "submitted"
    assert result.amount == Decimal("50.00")  # 30% equity cap - $250 manual/existing exposure
    submitted = broker.place_demo_strategy_order.call_args
    assert submitted.kwargs["request_id"] == _REQUEST_ID
    assert submitted.args[0].amount == Decimal("50.00")
    assert submitted.args[0].stop_loss_rate == Decimal("95.000000")
    assert submitted.args[0].take_profit_rate == Decimal("110.000000")
    assert conn.execute(
        "SELECT strategy_request_id, execution_origin, broker_order_ref FROM orders WHERE order_id=%s",
        (result.order_id,),
    ).fetchone() == (_REQUEST_ID, "strategy", "13902598")
    assert conn.execute(
        "SELECT verdict, allocated_amount, net_expectancy_pct FROM strategy_entry_preflights WHERE signal_id=%s",
        (signal_id,),
    ).fetchone() == ("allocated", Decimal("50.000000"), Decimal("3.00000000"))
    conn.commit()

    # Retry is read-only and cannot submit a duplicate.
    assert execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW).order_id == result.order_id
    broker.place_demo_strategy_order.assert_called_once()


def test_disabled_global_switch_keeps_an_unfunded_shadow_arm(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    signal_id = _seed(ebull_test_conn, auto=False)
    broker = _broker()

    result = execute_fired_paper_signal(ebull_test_conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.verdict == "rejected"
    assert result.reason_code == "auto_trading_disabled"
    broker.get_account_risk_snapshot.assert_not_called()
    broker.place_demo_strategy_order.assert_not_called()
    assert ebull_test_conn.execute(
        "SELECT verdict, reason_code FROM strategy_funding_decisions WHERE signal_id=%s",
        (signal_id,),
    ).fetchone() == ("rejected", "auto_trading_disabled")


def test_undocumented_cost_units_refuse_before_any_order_exists(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    signal_id = _seed(ebull_test_conn)
    broker = _broker(undocumented_cost=True)

    result = execute_fired_paper_signal(ebull_test_conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.reason_code == "cost_unit_undocumented"
    broker.place_demo_strategy_order.assert_not_called()
    assert ebull_test_conn.execute("SELECT count(*) FROM orders WHERE execution_origin='strategy'").fetchone() == (0,)


def test_non_positive_ask_is_a_preflight_rejection_not_submission_uncertainty(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    signal_id = _seed(ebull_test_conn)
    ebull_test_conn.execute("UPDATE quotes SET ask=0 WHERE instrument_id=2449001")
    ebull_test_conn.commit()
    broker = _broker()

    result = execute_fired_paper_signal(ebull_test_conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.verdict == "rejected"
    assert result.reason_code == "quote_ask_invalid"
    broker.get_account_risk_snapshot.assert_not_called()
    broker.place_demo_strategy_order.assert_not_called()
    assert ebull_test_conn.execute("SELECT count(*) FROM strategy_order_reconciliation_state").fetchone() == (0,)


def test_unresolved_local_strategy_order_consumes_risk_before_broker_snapshot_catches_up(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    signal_id = _seed(ebull_test_conn)
    deployment = ebull_test_conn.execute(
        "SELECT deployment_id FROM strategy_deployments WHERE strategy_id='S-ALLOC' AND mode='paper'"
    ).fetchone()
    assert deployment is not None
    deployment_id = int(deployment[0])
    prior_signal = ebull_test_conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id, strategy_version, instrument_id, signal_bar_date,
            signal_kind, verdict, fill_bar_date, fill_price, universe,
            input_rule_set_versions
        ) VALUES ('S-ALLOC', 'v1', 2449001, '2026-08-04', 'entry', 'fired',
                  '2026-08-05', 100, 'survivor_only',
                  '{"indicator_series":"rules-v1"}'::jsonb)
        RETURNING signal_id
        """
    ).fetchone()
    assert prior_signal is not None
    decision_id = decide_funding(
        ebull_test_conn,
        signal_id=int(prior_signal[0]),
        verdict="allocated",
        deployment_id=deployment_id,
        amount=Decimal("60"),
        reason_code="test_unresolved_order",
    )
    trade_id = create_strategy_trade(ebull_test_conn, decision_id)
    order = ebull_test_conn.execute(
        """
        INSERT INTO orders (
            instrument_id, action, order_type, requested_amount, status,
            raw_payload_json, execution_origin
        ) VALUES (2449001, 'BUY', 'MARKET', 60, 'submitted', NULL, 'strategy')
        RETURNING order_id
        """
    ).fetchone()
    assert order is not None
    link_strategy_order(
        ebull_test_conn,
        strategy_trade_id=trade_id,
        order_id=int(order[0]),
        purpose="entry",
    )
    ebull_test_conn.execute(
        "UPDATE strategy_trades SET status='reconcile_required' WHERE strategy_trade_id=%s",
        (trade_id,),
    )
    ebull_test_conn.commit()
    broker = _broker()

    result = execute_fired_paper_signal(ebull_test_conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.verdict == "rejected"
    assert result.reason_code == "risk_capacity_exhausted"
    broker.check_instrument_eligibility.assert_not_called()
    broker.place_demo_strategy_order.assert_not_called()


def test_transport_uncertainty_retries_only_the_committed_uuid(
    ebull_test_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal_id = _seed(ebull_test_conn)
    broker = _broker()
    accepted = broker.place_demo_strategy_order.return_value
    broker.place_demo_strategy_order.side_effect = [
        BrokerOrderSubmissionUncertain("timeout"),
        accepted,
    ]
    monkeypatch.setattr("app.services.strategy_order_reconciliation.uuid4", lambda: _REQUEST_ID)

    first = execute_fired_paper_signal(ebull_test_conn, broker=broker, signal_id=signal_id, now=_NOW)
    assert first.verdict == "submission_uncertain"
    second = execute_fired_paper_signal(ebull_test_conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert second.verdict == "submitted"
    assert [call.kwargs["request_id"] for call in broker.place_demo_strategy_order.call_args_list] == [
        _REQUEST_ID,
        _REQUEST_ID,
    ]
    assert ebull_test_conn.execute(
        "SELECT strategy_request_id FROM orders WHERE execution_origin='strategy'"
    ).fetchall() == [(_REQUEST_ID,)]


def test_concurrent_same_signal_callers_submit_exactly_once(
    ebull_test_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal_id = _seed(ebull_test_conn)
    broker = _broker()
    accepted = broker.place_demo_strategy_order.return_value
    both_started = Barrier(2)
    first_submission_entered = Event()
    release_submission = Event()

    def place_once(*args: Any, **kwargs: Any) -> BrokerOrderSubmission:
        del args, kwargs
        first_submission_entered.set()
        assert release_submission.wait(timeout=5)
        return accepted

    broker.place_demo_strategy_order.side_effect = place_once
    monkeypatch.setattr("app.services.strategy_order_reconciliation.uuid4", lambda: _REQUEST_ID)

    def execute() -> PaperExecutionResult:
        with psycopg.connect(test_database_url()) as worker_conn:
            both_started.wait(timeout=5)
            return execute_fired_paper_signal(worker_conn, broker=broker, signal_id=signal_id, now=_NOW)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(execute)
        second = pool.submit(execute)
        if not first_submission_entered.wait(timeout=5):
            release_submission.set()
            first.result(timeout=5)
            second.result(timeout=5)
            raise AssertionError("neither concurrent caller reached the broker submission boundary")
        sleep(0.1)
        assert broker.place_demo_strategy_order.call_count == 1
        release_submission.set()
        results = (first.result(timeout=5), second.result(timeout=5))

    assert results[0].order_id == results[1].order_id
    assert {result.verdict for result in results} == {"submitted"}
    assert broker.place_demo_strategy_order.call_count == 1
