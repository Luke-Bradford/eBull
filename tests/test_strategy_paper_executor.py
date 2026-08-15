"""#2449 demo allocator/executor integration tests against real Postgres."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from threading import Barrier, Event
from time import sleep
from typing import Any, cast
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
    BrokerOrderNotFound,
    BrokerOrderSubmission,
    BrokerOrderSubmissionUncertain,
    BrokerProvider,
    BrokerWhatIfCostResponse,
)
from app.services.cost_model import COST_MODEL_ID
from app.services.price_masked_bars import QUARANTINE_RULE_SET_VERSION
from app.services.result_ledger import store_holdout_result, store_in_sample_result
from app.services.strategies.validated_universe import VALIDATED_UNIVERSE_RULE_VERSION
from app.services.strategy_control_plane import (
    configure_deployment,
    configure_execution_policy,
    configure_paper_pool,
    create_strategy_trade,
    decide_funding,
    link_strategy_order,
)
from app.services.strategy_forecast_outcome_resolution import RESOLVER_VERSION as FORECAST_OUTCOME_RESOLVER_VERSION
from app.services.strategy_halt_identity import HALT_IDENTITY_RULE_VERSION
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_opportunity_forecast import (
    FORECAST_POLICY_VERSION,
    ForecastCalibration,
    OpportunityForecast,
    OpportunityForecastError,
    record_opportunity_forecast,
    register_forecast_calibration,
)
from app.services.strategy_opportunity_ranker import RankableOpportunity, persist_ranking_batch
from app.services.strategy_paper_executor import (
    COST_BASIS_BROKER_PREFLIGHT_VALUE,
    PaperExecutionResult,
    _effective_capital_bases,
    execute_fired_paper_signal,
)
from app.services.strategy_result_universe import (
    ResultUniverseRecord,
    store_result_universe,
)
from app.services.trial_register import TRIAL_REGISTER, TRIAL_REGISTER_VERSION
from tests.fixtures.ebull_test_db import test_database_url
from tests.test_result_ledger import (
    BOOTSTRAP_BLOCK,
    build_control,
    build_deflated,
    build_metrics,
    build_result,
)

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("registered_strategy_test_candidates")]

_NOW = datetime(2026, 8, 7, 15, 0, tzinfo=UTC)  # Friday 11:00 New York
_REQUEST_ID = UUID("1c94300c-90aa-4303-9d00-dec376d74efb")


def _authorise_forecast_scope(
    conn: psycopg.Connection[Any], *, setup_version: str, checked_at: datetime = _NOW
) -> None:
    """Explicit synthetic prospective authority for executor plumbing tests."""
    conn.execute(
        """
        INSERT INTO strategy_forecast_assessment_policies (
            policy_id,effective_from,recent_window_days,minimum_resolved_forecasts,
            adaptive_calibration_bins,max_normalized_brier_score,
            min_brier_skill_score,max_classwise_calibration_error,max_ambiguous_rate,max_unresolved_rate,
            max_pending_rate,max_assessment_age_days,evidence_ref
        ) VALUES ('test-assessment-policy-v1',%s - interval '1 day',90,30,5,0.2,0.01,0.1,0.05,0.05,0.2,2,
                  'synthetic executor fixture only')
        ON CONFLICT (policy_id) DO NOTHING
        """,
        (checked_at,),
    )
    assessment = conn.execute(
        """
        INSERT INTO strategy_forecast_assessments (
            policy_id,strategy_id,strategy_version,forecast_policy_version,model_version,
            calibration_id,setup_version,exit_policy_version,
            resolver_version,input_rule_set_version,window_start,window_end,evidence_hash,
            total_forecasts,resolved_forecasts,target_first_count,stop_first_count,timeout_count,
            ambiguous_count,unresolved_count,pending_count,normalized_brier_score,
            baseline_normalized_brier_score,brier_skill_score,max_classwise_calibration_error,
            ambiguous_rate,unresolved_rate,pending_rate,passed,reason_codes
        ) VALUES (
            'test-assessment-policy-v1','S-ALLOC','v1',%s,'test-model-v1','test-calibration-v1',%s,'test-exit-v1',%s,%s,
            %s::date-89,%s::date,'synthetic-' || %s,30,30,10,10,10,0,0,0,0,0.33333333,1,0,0,0,0,true,'[]'::jsonb
        ) RETURNING assessment_id
        """,
        (
            FORECAST_POLICY_VERSION,
            setup_version,
            FORECAST_OUTCOME_RESOLVER_VERSION,
            QUARANTINE_RULE_SET_VERSION,
            checked_at,
            checked_at,
            setup_version,
        ),
    ).fetchone()
    assert assessment is not None
    conn.execute(
        """
        INSERT INTO strategy_forecast_assessment_current (
            policy_id,strategy_id,strategy_version,forecast_policy_version,model_version,
            calibration_id,setup_version,exit_policy_version,
            resolver_version,input_rule_set_version,assessment_id,checked_at
        ) VALUES (
            'test-assessment-policy-v1','S-ALLOC','v1',%s,'test-model-v1',
            'test-calibration-v1',%s,'test-exit-v1',%s,%s,%s,%s
        )
        """,
        (
            FORECAST_POLICY_VERSION,
            setup_version,
            FORECAST_OUTCOME_RESOLVER_VERSION,
            QUARANTINE_RULE_SET_VERSION,
            int(assessment[0]),
            checked_at,
        ),
    )


def _seed(
    conn: psycopg.Connection[Any],
    *,
    ticket_sizing_mode: str = "percent",
) -> int:
    if conn.execute("SELECT 1 FROM strategy_paper_pool_events LIMIT 1").fetchone() is None:
        configure_paper_pool(
            conn,
            enabled=True,
            capital_limit=Decimal("2000"),
            risk_profile="balanced",
            changed_by="test",
            reason="shared paper pool fixture",
        )
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
    base_metrics = build_metrics(
        **{
            **BOOTSTRAP_BLOCK,
            "expectancy_ci_low_pct": 5.0,
            "expectancy_ci_high_pct": 6.0,
        }
    )
    deflated = build_deflated(
        declared_trials=TRIAL_REGISTER.declared_count,
        trial_register_version=TRIAL_REGISTER_VERSION,
    )
    shared_result = {
        "strategy_id": "S-ALLOC",
        "strategy_version": "v1",
        "universe_basis": "survivorship_free",
        "carry_unmodelled": False,
        "fx_unmodelled": False,
        "evaluated_instrument_count": 3,
        "deflated": deflated,
        "trial_count": deflated.declared_trials,
        "deflated_sharpe": Decimal(repr(deflated.deflated_sharpe)),
    }
    universe_record = ResultUniverseRecord(
        universe_rule_version=VALIDATED_UNIVERSE_RULE_VERSION,
        evaluated_instrument_ids=frozenset({1, 2, 3}),
        validated_universe_ids=frozenset({1, 2, 3}),
    )

    # The random-entry cohort is an in-sample falsification. The withheld row
    # remains control-free and replays the exact in-sample companion.
    for quarantine_arm in ("masked", "admitted"):
        support_id = store_in_sample_result(
            conn,
            build_result(
                **shared_result,
                metrics=base_metrics,
                namespace="in_sample",
                quarantine_arm=quarantine_arm,
                purpose="harness_validation",
                synthetic_control=build_control(
                    base_metrics,
                    mean_return_pct=0.0,
                    mean_return_ci_low_pct=-1.0,
                    mean_return_ci_high_pct=1.0,
                    cohort_sharpe_threshold=-4.0,
                    cohort_return_threshold_pct=-101.0,
                ),
            ),
        )
        store_result_universe(conn, result_id=support_id, record=universe_record)
    result_ids: list[int] = []
    for quarantine_arm in ("masked", "admitted"):
        result_id = store_holdout_result(
            conn,
            build_result(
                **shared_result,
                metrics=base_metrics,
                namespace="hold_out",
                quarantine_arm=quarantine_arm,
                window_start=date(2022, 1, 1),
                window_end=date(2024, 9, 27),
                synthetic_control=None,
            ),
            accessed_by="tests/test_strategy_paper_executor.py",
            purpose="paper allocation evidence fixture",
        )
        result_ids.append(result_id)
        store_result_universe(conn, result_id=result_id, record=universe_record)
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
    paper_id = next(int(row[0]) for row in promotion_rows if row[1] == "paper_enabled")
    for result_id in result_ids:
        conn.execute(
            "INSERT INTO strategy_promotion_results (promotion_id, result_id) VALUES (%s, %s)",
            (historical_id, result_id),
        )
        conn.execute(
            "INSERT INTO strategy_promotion_results (promotion_id, result_id) VALUES (%s, %s)",
            (paper_id, result_id),
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
        ticket_sizing_mode=cast(Any, ticket_sizing_mode),
        ticket_fraction=Decimal("0.20") if ticket_sizing_mode == "percent" else None,
        fixed_ticket_amount=Decimal("125") if ticket_sizing_mode == "fixed" else None,
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
    register_forecast_calibration(
        conn,
        ForecastCalibration(
            calibration_id="test-calibration-v1",
            model_version="test-model-v1",
            holdout_start=date(2026, 1, 1),
            holdout_end=date(2026, 7, 31),
            sample_size=500,
            brier_score=Decimal("0.18"),
            calibration_error=Decimal("0.04"),
            passed=True,
            evidence_ref="synthetic executor fixture only",
        ),
    )
    forecast_id = record_opportunity_forecast(
        conn,
        OpportunityForecast(
            signal_id=int(signal[0]),
            decided_at=_NOW,
            valid_through=_NOW + timedelta(days=7),
            horizon_market_days=5,
            target_barrier_pct=Decimal("10"),
            stop_barrier_pct=Decimal("5"),
            setup_version="test-setup-v1",
            exit_policy_version="test-exit-v1",
            calibration_id="test-calibration-v1",
            target_probability=Decimal("0.6"),
            stop_probability=Decimal("0.2"),
            timeout_probability=Decimal("0.2"),
            target_net_return_pct=Decimal("4"),
            stop_net_return_pct=Decimal("-2"),
            timeout_net_return_pct=Decimal("0"),
            expected_duration_hours=Decimal("24"),
            uncertainty_penalty_pct=Decimal("0.2"),
            tail_penalty_pct=Decimal("0.1"),
            correlation_penalty_pct=Decimal("0.1"),
            cost_stress_penalty_pct=Decimal("0.1"),
            conservative_net_expectancy_pct=Decimal("1.5"),
            cost_model_id=COST_MODEL_ID,
        ),
    )
    _authorise_forecast_scope(conn, setup_version="test-setup-v1")
    persist_ranking_batch(
        conn,
        opportunities=[
            RankableOpportunity(
                signal_id=int(signal[0]),
                forecast_id=forecast_id,
                strategy_id="S-ALLOC",
                strategy_version="v1",
                instrument_id=2449001,
                signal_bar_date=date(2026, 8, 5),
                side="long",
                horizon_market_days=5,
                setup_version="test-setup-v1",
                exit_policy_version="test-exit-v1",
                decided_at=_NOW,
                conservative_net_expectancy_pct=Decimal("1.5"),
            )
        ],
        selection_limit=5,
        decided_at=_NOW,
    )
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
        ) VALUES (true, false, false, 'test', 'paper allocator fixture')
        ON CONFLICT (id) DO UPDATE SET
            enable_auto_trading=EXCLUDED.enable_auto_trading,
            enable_live_trading=EXCLUDED.enable_live_trading,
            updated_by=EXCLUDED.updated_by, reason=EXCLUDED.reason,
            updated_at=now()
        """
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


def _rerank_signal(conn: psycopg.Connection[Any], signal_id: int) -> None:
    row = conn.execute(
        """
        SELECT f.forecast_id,s.strategy_id,s.strategy_version,s.instrument_id,
               s.signal_bar_date,f.side,f.horizon_market_days,f.setup_version,
               f.exit_policy_version,f.decided_at,f.conservative_net_expectancy_pct
        FROM strategy_signals s
        JOIN strategy_opportunity_forecasts f ON f.signal_id=s.signal_id
        WHERE s.signal_id=%s
        """,
        (signal_id,),
    ).fetchone()
    assert row is not None
    persist_ranking_batch(
        conn,
        opportunities=[
            RankableOpportunity(
                signal_id=signal_id,
                forecast_id=int(row[0]),
                strategy_id=str(row[1]),
                strategy_version=str(row[2]),
                instrument_id=int(row[3]),
                signal_bar_date=row[4],
                side=str(row[5]),
                horizon_market_days=int(row[6]),
                setup_version=str(row[7]),
                exit_policy_version=str(row[8]),
                decided_at=row[9],
                conservative_net_expectancy_pct=Decimal(str(row[10])),
            )
        ],
        selection_limit=5,
        decided_at=_NOW,
    )
    conn.commit()


def _existing_allocated_trade(
    conn: psycopg.Connection[Any],
    *,
    index: int,
    amount: Decimal,
    status: str = "open",
) -> int:
    deployment = conn.execute(
        "SELECT deployment_id FROM strategy_deployments WHERE strategy_id='S-ALLOC' AND strategy_version='v1'"
    ).fetchone()
    assert deployment is not None
    signal_date = date(2026, 6, 1) + timedelta(days=index)
    signal = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id,strategy_version,instrument_id,signal_bar_date,
            signal_kind,verdict,fill_bar_date,fill_price,universe,input_rule_set_versions
        ) VALUES ('S-ALLOC','v1',2449001,%s,'entry','fired',%s,100,
                  'survivor_only','{"indicator_series":"rules-v1"}'::jsonb)
        RETURNING signal_id
        """,
        (signal_date, signal_date + timedelta(days=1)),
    ).fetchone()
    assert signal is not None
    decision = conn.execute(
        """
        INSERT INTO strategy_funding_decisions (
            signal_id,deployment_id,verdict,amount,reason_code
        ) VALUES (%s,%s,'allocated',%s,'existing_allocation')
        RETURNING funding_decision_id
        """,
        (signal[0], deployment[0], amount),
    ).fetchone()
    assert decision is not None
    trade = conn.execute(
        """
        INSERT INTO strategy_trades (funding_decision_id,instrument_id,status)
        VALUES (%s,2449001,%s)
        RETURNING strategy_trade_id
        """,
        (decision[0], status),
    ).fetchone()
    assert trade is not None
    return int(trade[0])


def test_harness_signal_is_rejected_before_runtime_or_broker_checks(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    signal_id = _seed(ebull_test_conn)
    harness = next(iter(STRATEGY_MANIFEST.values()))
    ebull_test_conn.execute(
        "UPDATE strategy_signals SET strategy_id=%s WHERE signal_id=%s",
        (harness.strategy_id, signal_id),
    )
    ebull_test_conn.commit()
    broker = _broker()

    result = execute_fired_paper_signal(ebull_test_conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.verdict == "rejected"
    assert result.reason_code == "harness_validation_only"
    assert ebull_test_conn.execute(
        "SELECT halt_identity_rule_version FROM strategy_entry_preflights WHERE signal_id=%s",
        (signal_id,),
    ).fetchone() == (None,)
    broker.place_demo_strategy_order.assert_not_called()


def test_unregistered_signal_is_rejected_before_runtime_or_broker_checks(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    signal_id = _seed(ebull_test_conn)
    ebull_test_conn.execute(
        "UPDATE strategy_signals SET strategy_id='S-UNREGISTERED' WHERE signal_id=%s",
        (signal_id,),
    )
    ebull_test_conn.commit()
    broker = _broker()

    result = execute_fired_paper_signal(ebull_test_conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.verdict == "rejected"
    assert result.reason_code == "strategy_not_capital_candidate"
    broker.place_demo_strategy_order.assert_not_called()


def test_plain_nasdaq_halt_refuses_an_etoro_session_variant(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    signal_id = _seed(ebull_test_conn)
    ebull_test_conn.execute("UPDATE instruments SET symbol='TALLOC.RTH' WHERE instrument_id=2449001")
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_market_halts (
            source, symbol, halt_at, market, reason_code, resumed_at, observed_at
        ) VALUES ('nasdaq_trader_rss', 'TALLOC', %s, 'NASDAQ', 'T1', NULL, %s)
        """,
        (_NOW, _NOW),
    )
    ebull_test_conn.commit()
    broker = _broker()

    result = execute_fired_paper_signal(ebull_test_conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.verdict == "rejected"
    assert result.reason_code == "instrument_halted"
    assert ebull_test_conn.execute(
        "SELECT halt_identity_rule_version FROM strategy_entry_preflights WHERE signal_id=%s",
        (signal_id,),
    ).fetchone() == (HALT_IDENTITY_RULE_VERSION,)
    broker.place_demo_strategy_order.assert_not_called()


def _broker(
    *,
    undocumented_cost: bool = False,
    eligibility_currency: str = "USD",
    cost_currency: str = "USD",
    arm_settlement_types: tuple[str, ...] = ("real",),
) -> MagicMock:
    broker = MagicMock(spec=BrokerProvider)
    broker.get_account_risk_snapshot.return_value = BrokerAccountRiskSnapshot(
        available_cash=Decimal("600"),
        total_invested=Decimal("400"),
        unrealized_pnl=Decimal("0"),
        equity=Decimal("1000"),
        # `unrealized_pnl=0` above, so committed capital and market value coincide
        # here; the executor reads only `amount` (#2704).
        instrument_investments=(BrokerInstrumentInvestment(2449001, Decimal("250"), Decimal("250"), 1, 0),),
        observed_at=_NOW,
        raw_payload={},
    )
    broker.check_instrument_eligibility.return_value = BrokerEligibilityResponse(
        currency=eligibility_currency,
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
                leverage_configs=tuple(
                    BrokerLeverageConfig(
                        settlement_type=settlement_type,
                        direction="LONG",
                        leverage_values=(1,),
                        min_position_amount=Decimal("10"),
                        allow_edit_stop_loss=True,
                        allow_edit_take_profit=True,
                        allow_stop_loss_take_profit=True,
                        raw_payload={},
                    )
                    for settlement_type in arm_settlement_types
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
            # ⚠ THE DEFAULT IS THE LIVE SHAPE, not the documented one (#2598 step 3):
            # eToro sends `value` and omits `amount` as a key, so the happy path here
            # exercises what the broker actually returns. `undocumented_cost=True` is
            # now the shape that REMAINS undocumented -- both fields present, which has
            # never been observed and has no documented rule for which one wins.
            BrokerCostComponent(
                cost_type="marketSpread",
                amount=Decimal("0.5") if undocumented_cost else None,
                value=Decimal("0.5"),
                currency=cost_currency,
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

    assert result.verdict == "submitted", result
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
    forecast_id = conn.execute(
        """
        SELECT f.forecast_id,m.ranking_member_id
        FROM strategy_opportunity_forecasts f
        JOIN strategy_opportunity_ranking_members m ON m.forecast_id=f.forecast_id
        WHERE f.signal_id=%s AND m.selected
        """,
        (signal_id,),
    ).fetchone()
    assert forecast_id is not None
    # ⚠ `cost_basis` is asserted HERE rather than in a test of its own because this
    # is the only place the writer meets the real constraint: `sql/342` makes it NOT
    # NULL on an allocated row, so a writer that stopped binding it would fail this
    # insert outright — and a basis that named a path other than the one `_costs`
    # took would pass the CHECK while lying about provenance (#2598 step 4).
    assert conn.execute(
        "SELECT verdict, allocated_amount, net_expectancy_pct, forecast_id, ranking_member_id, cost_basis, "
        "halt_identity_rule_version "
        "FROM strategy_entry_preflights WHERE signal_id=%s",
        (signal_id,),
    ).fetchone() == (
        "allocated",
        Decimal("50.000000"),
        Decimal("3.00000000"),
        forecast_id[0],
        forecast_id[1],
        COST_BASIS_BROKER_PREFLIGHT_VALUE,
        HALT_IDENTITY_RULE_VERSION,
    )
    conn.commit()

    # Retry is read-only and cannot submit a duplicate.
    assert execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW).order_id == result.order_id
    broker.place_demo_strategy_order.assert_called_once()


def test_current_result_policy_supersession_refuses_before_broker_access(
    ebull_test_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    broker = _broker()
    monkeypatch.setattr("app.services.strategy_result.TRIAL_REGISTER_VERSION", "superseded-after-paper-approval")

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.verdict == "rejected"
    assert result.reason_code == "pinned_promotion_evidence_invalid"
    broker.get_account_risk_snapshot.assert_not_called()
    broker.check_instrument_eligibility.assert_not_called()
    broker.get_what_if_costs.assert_not_called()
    broker.place_demo_strategy_order.assert_not_called()


def test_missing_opportunity_forecast_refuses_before_broker_access(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    conn.execute("DELETE FROM strategy_opportunity_ranking_members")
    conn.execute("DELETE FROM strategy_opportunity_ranking_batches")
    conn.execute("DELETE FROM strategy_opportunity_forecasts WHERE signal_id=%s", (signal_id,))
    conn.commit()
    broker = _broker()

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.reason_code == "opportunity_forecast_missing"
    broker.get_account_risk_snapshot.assert_not_called()
    broker.place_demo_strategy_order.assert_not_called()
    assert conn.execute(
        "SELECT forecast_id, halt_identity_rule_version FROM strategy_entry_preflights WHERE signal_id=%s",
        (signal_id,),
    ).fetchone() == (None, HALT_IDENTITY_RULE_VERSION)


def test_unbatched_opportunity_refuses_before_broker_access(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    conn.execute("DELETE FROM strategy_opportunity_ranking_members")
    conn.execute("DELETE FROM strategy_opportunity_ranking_batches")
    conn.commit()
    broker = _broker()

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.reason_code == "opportunity_ranking_member_missing"
    broker.get_account_risk_snapshot.assert_not_called()
    broker.place_demo_strategy_order.assert_not_called()


@pytest.mark.parametrize(
    ("invalidity", "reason_code"),
    [
        ("calibration", "opportunity_calibration_not_passed"),
        ("cost", "opportunity_forecast_cost_model_stale"),
        ("expiry", "opportunity_forecast_not_current"),
    ],
)
def test_invalid_opportunity_evidence_refuses_before_broker_access(
    ebull_test_conn: psycopg.Connection[Any],
    invalidity: str,
    reason_code: str,
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    if invalidity == "calibration":
        conn.execute("UPDATE strategy_forecast_calibrations SET passed=false")
    elif invalidity == "cost":
        conn.execute("UPDATE strategy_opportunity_forecasts SET cost_model_id='obsolete-cost-model'")
    else:
        conn.execute("UPDATE strategy_opportunity_forecasts SET valid_through=decided_at")
    conn.commit()
    broker = _broker()
    decision_time = _NOW + timedelta(seconds=1) if reason_code == "opportunity_forecast_not_current" else _NOW

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=decision_time)

    assert result.reason_code == reason_code
    broker.get_account_risk_snapshot.assert_not_called()
    broker.place_demo_strategy_order.assert_not_called()


@pytest.mark.parametrize(
    ("invalidity", "reason_code"),
    [
        ("policy", "opportunity_assessment_policy_missing"),
        ("missing", "opportunity_assessment_missing"),
        ("strategy_scope", "opportunity_assessment_missing"),
        ("calibration_scope", "opportunity_assessment_missing"),
        ("failed", "opportunity_assessment_not_passed"),
        ("stale", "opportunity_assessment_stale"),
    ],
)
def test_prospective_assessment_refuses_before_broker_access(
    ebull_test_conn: psycopg.Connection[Any],
    invalidity: str,
    reason_code: str,
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    if invalidity == "policy":
        conn.execute("UPDATE strategy_forecast_assessment_policies SET effective_from=%s + interval '1 day'", (_NOW,))
    elif invalidity == "missing":
        conn.execute("DELETE FROM strategy_forecast_assessment_current")
    elif invalidity == "strategy_scope":
        conn.execute("UPDATE strategy_forecast_assessment_current SET strategy_id='S-OTHER'")
    elif invalidity == "calibration_scope":
        conn.execute(
            """
            INSERT INTO strategy_forecast_calibrations (
                calibration_id,model_version,holdout_start,holdout_end,sample_size,
                brier_score,calibration_error,passed,evidence_ref
            ) VALUES ('other-calibration','test-model-v1','2026-01-01','2026-07-31',500,0.18,0.04,true,'test')
            """
        )
        conn.execute("UPDATE strategy_forecast_assessment_current SET calibration_id='other-calibration'")
    elif invalidity == "failed":
        conn.execute("UPDATE strategy_forecast_assessments SET passed=false")
    else:
        conn.execute("UPDATE strategy_forecast_assessment_current SET checked_at=%s - interval '3 days'", (_NOW,))
    conn.commit()
    broker = _broker()

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.reason_code == reason_code
    broker.get_account_risk_snapshot.assert_not_called()
    broker.place_demo_strategy_order.assert_not_called()


def test_immutable_forecast_duplicate_does_not_abort_the_callers_transaction(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    existing = conn.execute(
        "SELECT forecast_id FROM strategy_opportunity_forecasts WHERE signal_id=%s",
        (signal_id,),
    ).fetchone()
    assert existing is not None

    with pytest.raises(OpportunityForecastError, match="immutable opportunity forecast"):
        record_opportunity_forecast(
            conn,
            OpportunityForecast(
                signal_id=signal_id,
                decided_at=_NOW,
                valid_through=_NOW + timedelta(days=7),
                horizon_market_days=5,
                target_barrier_pct=Decimal("10"),
                stop_barrier_pct=Decimal("5"),
                setup_version="test-setup-v1",
                exit_policy_version="test-exit-v1",
                calibration_id="test-calibration-v1",
                target_probability=Decimal("0.6"),
                stop_probability=Decimal("0.2"),
                timeout_probability=Decimal("0.2"),
                target_net_return_pct=Decimal("4"),
                stop_net_return_pct=Decimal("-2"),
                timeout_net_return_pct=Decimal("0"),
                expected_duration_hours=Decimal("24"),
                uncertainty_penalty_pct=Decimal("0.2"),
                tail_penalty_pct=Decimal("0.1"),
                correlation_penalty_pct=Decimal("0.1"),
                cost_stress_penalty_pct=Decimal("0.1"),
                conservative_net_expectancy_pct=Decimal("1.5"),
                cost_model_id=COST_MODEL_ID,
            ),
        )

    assert conn.execute("SELECT count(*) FROM strategy_opportunity_forecasts").fetchone() == (1,)


def test_fixed_ticket_mode_requests_a_currency_amount_before_risk_caps(
    ebull_test_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal_id = _seed(ebull_test_conn, ticket_sizing_mode="fixed")
    broker = _broker()
    broker.get_account_risk_snapshot.return_value = BrokerAccountRiskSnapshot(
        available_cash=Decimal("600"),
        total_invested=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        equity=Decimal("1000"),
        instrument_investments=(),
        observed_at=_NOW,
        raw_payload={},
    )
    monkeypatch.setattr("app.services.strategy_order_reconciliation.uuid4", lambda: _REQUEST_ID)

    result = execute_fired_paper_signal(ebull_test_conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.amount == Decimal("125.00")
    assert ebull_test_conn.execute(
        """
        SELECT ticket_sizing_mode,ticket_fraction,fixed_ticket_amount,max_ticket_amount
        FROM strategy_execution_policy_events
        ORDER BY policy_event_id DESC LIMIT 1
        """
    ).fetchone() == ("fixed", None, Decimal("125.000000"), Decimal("500.000000"))


def test_capital_modes_use_realised_owned_pnl_and_refuse_unknown_pnl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = MagicMock()
    intent = MagicMock(
        capital_mode="compound",
        deployment_limit=Decimal("1000"),
        pool_limit=Decimal("2000"),
        strategy_id="S-ALLOC",
        strategy_version="v1",
    )
    monkeypatch.setattr(
        "app.services.strategy_paper_executor.load_paper_realised_pnl",
        lambda _conn: {
            ("S-ALLOC", "v1"): Decimal("100"),
            ("S-OTHER", "v2"): Decimal("-25"),
        },
    )
    assert _effective_capital_bases(conn, intent) == (Decimal("1100"), Decimal("2075"))

    intent.capital_mode = "fixed"
    assert _effective_capital_bases(conn, intent) == (Decimal("1000"), Decimal("2000"))
    monkeypatch.setattr(
        "app.services.strategy_paper_executor.load_paper_realised_pnl",
        lambda _conn: {
            ("S-ALLOC", "v1"): Decimal("-100"),
            ("S-OTHER", "v2"): Decimal("25"),
        },
    )
    assert _effective_capital_bases(conn, intent) == (Decimal("900"), Decimal("1925"))

    monkeypatch.setattr(
        "app.services.strategy_paper_executor.load_paper_realised_pnl",
        lambda _conn: None,
    )
    assert _effective_capital_bases(conn, intent) == "realised_pnl_incomplete"


def test_legacy_automatic_switch_does_not_control_the_bounded_strategy_lane(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    signal_id = _seed(ebull_test_conn)
    broker = _broker()

    result = execute_fired_paper_signal(ebull_test_conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.verdict == "submitted"
    assert result.reason_code == "broker_accepted"
    broker.get_account_risk_snapshot.assert_called_once()
    broker.place_demo_strategy_order.assert_called_once()


def test_shared_paper_pool_is_a_master_switch_and_hard_cap(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("400"),
        risk_profile="balanced",
        changed_by="operator",
        reason="bounded shared pot",
    )
    _rerank_signal(conn, signal_id)
    conn.commit()
    broker = _broker()
    broker.get_account_risk_snapshot.return_value = BrokerAccountRiskSnapshot(
        available_cash=Decimal("2000"),
        total_invested=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        equity=Decimal("2000"),
        instrument_investments=(),
        observed_at=_NOW,
        raw_payload={},
    )

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.verdict == "submitted"
    assert result.amount == Decimal("60.00")


def test_mandate_loss_at_stop_reduces_position_size(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    conn.execute("UPDATE strategy_paper_pool_events SET max_loss_per_position_pct=0.25")
    conn.commit()
    broker = _broker()
    broker.get_account_risk_snapshot.return_value = BrokerAccountRiskSnapshot(
        available_cash=Decimal("2000"),
        total_invested=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        equity=Decimal("2000"),
        instrument_investments=(),
        observed_at=_NOW,
        raw_payload={},
    )

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.verdict == "submitted"
    assert result.amount == Decimal("100.00")


def test_forecast_barriers_drive_loss_sizing_and_submitted_tp_sl(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    conn.execute("UPDATE strategy_opportunity_forecasts SET target_barrier_pct=4,stop_barrier_pct=2")
    conn.execute("UPDATE strategy_paper_pool_events SET max_loss_per_position_pct=0.1")
    conn.commit()
    broker = _broker()
    broker.get_account_risk_snapshot.return_value = BrokerAccountRiskSnapshot(
        available_cash=Decimal("2000"),
        total_invested=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        equity=Decimal("2000"),
        instrument_investments=(),
        observed_at=_NOW,
        raw_payload={},
    )

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.amount == Decimal("100.00")
    order = broker.place_demo_strategy_order.call_args.args[0]
    assert order.stop_loss_rate == Decimal("98.000000")
    assert order.take_profit_rate == Decimal("104.000000")
    assert conn.execute(
        "SELECT stop_loss_rate,take_profit_rate FROM strategy_entry_preflights WHERE signal_id=%s",
        (signal_id,),
    ).fetchone() == (Decimal("98.000000"), Decimal("104.000000"))


@pytest.mark.parametrize(
    ("target_barrier", "stop_barrier", "reason_code"),
    [
        (None, None, "opportunity_forecast_target_barrier_missing"),
        (Decimal("10"), Decimal("6"), "opportunity_forecast_stop_exceeds_policy"),
    ],
)
def test_invalid_forecast_barrier_geometry_refuses_before_broker_access(
    ebull_test_conn: psycopg.Connection[Any],
    target_barrier: Decimal | None,
    stop_barrier: Decimal | None,
    reason_code: str,
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    conn.execute(
        """
        UPDATE strategy_opportunity_forecasts
        SET target_barrier_pct=%s,stop_barrier_pct=%s
        """,
        (target_barrier, stop_barrier),
    )
    conn.commit()
    broker = _broker()

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.reason_code == reason_code
    broker.get_account_risk_snapshot.assert_not_called()
    broker.place_demo_strategy_order.assert_not_called()


def test_mandate_position_loss_below_broker_minimum_refuses_without_submission(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    conn.execute("UPDATE strategy_paper_pool_events SET max_loss_per_position_pct=0.0001")
    conn.commit()
    broker = _broker()

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.reason_code == "below_broker_minimum"
    broker.place_demo_strategy_order.assert_not_called()


def test_mandate_active_risk_budget_reduces_position_size(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    conn.execute("UPDATE strategy_paper_pool_events SET active_risk_budget_pct=5")
    conn.commit()
    broker = _broker()
    broker.get_account_risk_snapshot.return_value = BrokerAccountRiskSnapshot(
        available_cash=Decimal("2000"),
        total_invested=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        equity=Decimal("2000"),
        instrument_investments=(),
        observed_at=_NOW,
        raw_payload={},
    )

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.verdict == "submitted"
    assert result.amount == Decimal("100.00")


def test_mandate_concurrency_refuses_before_order_submission(
    ebull_test_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    for index in range(8):
        _existing_allocated_trade(conn, index=index, amount=Decimal("1"))
    conn.commit()
    monkeypatch.setattr(
        "app.services.strategy_paper_executor.load_paper_realised_pnl",
        lambda _conn: {("S-ALLOC", "v1"): Decimal("0")},
    )
    broker = _broker()

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.reason_code == "portfolio_concurrency_limit"
    broker.place_demo_strategy_order.assert_not_called()


def test_mandate_cash_reserve_refuses_when_allocated_capital_reaches_boundary(
    ebull_test_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    conn.execute("UPDATE strategy_paper_pool_events SET active_risk_budget_pct=50,cash_reserve_pct=50")
    _existing_allocated_trade(conn, index=0, amount=Decimal("1000"))
    conn.commit()
    monkeypatch.setattr(
        "app.services.strategy_paper_executor.load_paper_realised_pnl",
        lambda _conn: {("S-ALLOC", "v1"): Decimal("0")},
    )
    broker = _broker()

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.reason_code == "portfolio_cash_reserve_limit"
    broker.place_demo_strategy_order.assert_not_called()


@pytest.mark.parametrize(
    ("mandate_drawdown_pct", "equity", "reason_code"),
    [
        (Decimal("15"), Decimal("900"), "account_drawdown_limit"),
        (Decimal("5"), Decimal("950"), "portfolio_drawdown_limit"),
    ],
)
def test_stricter_drawdown_limit_refuses_at_the_exact_boundary(
    ebull_test_conn: psycopg.Connection[Any],
    mandate_drawdown_pct: Decimal,
    equity: Decimal,
    reason_code: str,
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    conn.execute(
        "UPDATE strategy_paper_pool_events SET max_portfolio_drawdown_pct=%s",
        (mandate_drawdown_pct,),
    )
    conn.execute(
        """
        INSERT INTO strategy_paper_account_risk_state (
            id,equity_high_water,last_equity,last_drawdown_pct,observed_at
        ) VALUES (true,1000,1000,0,%s)
        """,
        (_NOW,),
    )
    conn.commit()
    broker = _broker()
    broker.get_account_risk_snapshot.return_value = BrokerAccountRiskSnapshot(
        available_cash=equity,
        total_invested=Decimal("0"),
        unrealized_pnl=equity - Decimal("1000"),
        equity=equity,
        instrument_investments=(),
        observed_at=_NOW,
        raw_payload={},
    )

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.reason_code == reason_code
    broker.place_demo_strategy_order.assert_not_called()


def test_mandate_daily_loss_refuses_at_the_exact_market_day_boundary(
    ebull_test_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    trade_id = _existing_allocated_trade(conn, index=0, amount=Decimal("100"), status="closed")
    conn.execute(
        """
        INSERT INTO strategy_position_ownership (
            strategy_trade_id,broker_position_id,status,released_at,release_reason
        ) VALUES (%s,7443001,'released',%s,'closed')
        """,
        (trade_id, _NOW),
    )
    conn.execute(
        """
        INSERT INTO trade_events (
            position_id,etoro_instrument_id,instrument_id,event_kind,side,units,
            price,executed_at,fees_usd,realized_pnl_usd,investment_usd,source,raw_payload
        ) VALUES (7443001,2449001,2449001,'close','sell',1,100,%s,0,-30,100,'etoro_history','{}')
        """,
        (_NOW,),
    )
    conn.commit()
    monkeypatch.setattr(
        "app.services.strategy_paper_executor.load_paper_realised_pnl",
        lambda _conn: {("S-ALLOC", "v1"): Decimal("0")},
    )
    broker = _broker()

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.reason_code == "portfolio_daily_loss_limit"
    broker.place_demo_strategy_order.assert_not_called()


def test_legacy_enabled_pool_without_mandate_cannot_authorise_an_order(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    conn.execute(
        """
        INSERT INTO strategy_paper_pool_events (
            enabled,capital_limit,currency,capital_mode,changed_by,reason
        ) VALUES (true,2000,'USD','fixed','legacy','pre-mandate authority')
        """
    )
    conn.commit()
    broker = _broker()

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.verdict == "rejected"
    assert result.reason_code == "portfolio_mandate_unconfigured"
    broker.get_account_risk_snapshot.assert_not_called()
    broker.place_demo_strategy_order.assert_not_called()


def test_disabled_pool_reason_precedes_unconfigured_mandate_reason(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    conn.execute(
        """
        INSERT INTO strategy_paper_pool_events (
            enabled,capital_limit,currency,capital_mode,changed_by,reason
        ) VALUES (false,2000,'USD','fixed','legacy','disabled pre-mandate authority')
        """
    )
    conn.execute("UPDATE runtime_config SET enable_auto_trading=true WHERE id=true")
    conn.commit()
    broker = _broker()

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert result.verdict == "rejected"
    assert result.reason_code == "paper_pool_disabled"
    broker.get_account_risk_snapshot.assert_not_called()
    broker.place_demo_strategy_order.assert_not_called()


def test_shared_paper_pool_excludes_future_live_reservations(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("400"),
        risk_profile="balanced",
        changed_by="operator",
        reason="paper-only shared pot",
    )
    _rerank_signal(conn, signal_id)
    live_signal = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id,strategy_version,instrument_id,signal_bar_date,
            signal_kind,verdict,fill_bar_date,fill_price,universe,input_rule_set_versions
        ) VALUES ('S-FUTURE-LIVE','v1',2449001,'2026-08-04','entry','fired',
                  '2026-08-05',100,'survivor_only','{"indicator_series":"rules-v1"}'::jsonb)
        RETURNING signal_id
        """
    ).fetchone()
    live_deployment = conn.execute(
        """
        INSERT INTO strategy_deployments (
            strategy_id,strategy_version,mode,capital_limit,currency,enabled,updated_by,reason
        ) VALUES ('S-FUTURE-LIVE','v1','live',100,'USD',true,'test','future live fixture')
        RETURNING deployment_id
        """
    ).fetchone()
    assert live_signal is not None and live_deployment is not None
    conn.execute(
        """
        INSERT INTO strategy_funding_decisions (
            signal_id,deployment_id,verdict,amount,reason_code
        ) VALUES (%s,%s,'allocated',25,'future_live_reservation')
        """,
        (live_signal[0], live_deployment[0]),
    )
    conn.commit()

    broker = _broker()
    broker.get_account_risk_snapshot.return_value = BrokerAccountRiskSnapshot(
        available_cash=Decimal("2000"),
        total_invested=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        equity=Decimal("2000"),
        instrument_investments=(),
        observed_at=_NOW,
        raw_payload={},
    )
    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert (result.verdict, result.reason_code, result.amount) == (
        "submitted",
        "broker_accepted",
        Decimal("60.00"),
    )


def test_undocumented_cost_units_refuse_before_any_order_exists(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠ WHAT COUNTS AS UNDOCUMENTED MOVED (#2598 step 3). A `value`-only row is now
    priced -- that is the live shape, and its unit was decoded on a 60-instrument
    census. The refusal remains for a row carrying BOTH fields, which has never been
    observed and has no documented rule for which one wins."""
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


def test_uncertain_harness_submission_is_looked_up_without_resubmission(
    ebull_test_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal_id = _seed(ebull_test_conn)
    broker = _broker()
    broker.place_demo_strategy_order.side_effect = BrokerOrderSubmissionUncertain("timeout")
    broker.lookup_order.side_effect = BrokerOrderNotFound("not found yet")
    monkeypatch.setattr("app.services.strategy_order_reconciliation.uuid4", lambda: _REQUEST_ID)
    first = execute_fired_paper_signal(ebull_test_conn, broker=broker, signal_id=signal_id, now=_NOW)
    assert first.verdict == "submission_uncertain"
    harness = next(iter(STRATEGY_MANIFEST.values()))
    ebull_test_conn.execute(
        "UPDATE strategy_signals SET strategy_id=%s WHERE signal_id=%s",
        (harness.strategy_id, signal_id),
    )
    ebull_test_conn.commit()

    second = execute_fired_paper_signal(ebull_test_conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert second.verdict == "submission_uncertain"
    assert broker.place_demo_strategy_order.call_count == 1
    assert broker.lookup_order.call_args.kwargs == {"reference_id": str(_REQUEST_ID)}


def test_unexpected_initial_submission_bug_propagates_with_reconciliation_authority(
    ebull_test_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal_id = _seed(ebull_test_conn)
    broker = _broker()
    broker.place_demo_strategy_order.side_effect = RuntimeError("provider programming bug")
    monkeypatch.setattr("app.services.strategy_order_reconciliation.uuid4", lambda: _REQUEST_ID)

    with pytest.raises(RuntimeError, match="provider programming bug"):
        execute_fired_paper_signal(ebull_test_conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert ebull_test_conn.execute(
        """
        SELECT t.status, o.strategy_request_id, r.state
        FROM strategy_trades t
        JOIN strategy_trade_orders sto ON sto.strategy_trade_id=t.strategy_trade_id
        JOIN orders o ON o.order_id=sto.order_id
        JOIN strategy_order_reconciliation_state r ON r.order_id=o.order_id
        """
    ).fetchone() == ("reconcile_required", _REQUEST_ID, "unresolved")


def test_unexpected_uncertain_retry_bug_propagates_instead_of_looping_silently(
    ebull_test_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal_id = _seed(ebull_test_conn)
    broker = _broker()
    broker.place_demo_strategy_order.side_effect = [
        BrokerOrderSubmissionUncertain("timeout"),
        RuntimeError("retry programming bug"),
    ]
    monkeypatch.setattr("app.services.strategy_order_reconciliation.uuid4", lambda: _REQUEST_ID)
    assert (
        execute_fired_paper_signal(ebull_test_conn, broker=broker, signal_id=signal_id, now=_NOW).verdict
        == "submission_uncertain"
    )

    with pytest.raises(RuntimeError, match="retry programming bug"):
        execute_fired_paper_signal(ebull_test_conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert broker.place_demo_strategy_order.call_count == 2


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


@pytest.mark.parametrize(
    ("broker_kwargs", "expected"),
    [
        ({"eligibility_currency": "GBP"}, "eligibility_unresolved"),
        ({"cost_currency": "GBP"}, "cost_currency_or_value_invalid"),
        # `.upper()` is preserved at both sites, so a lowercase response still passes.
        ({"eligibility_currency": "usd", "cost_currency": "usd"}, None),
        # ...and `strip()` is deliberately NOT added, so a padded one still fails.
        ({"eligibility_currency": " USD "}, "eligibility_unresolved"),
    ],
    ids=["eligibility_gbp", "cost_gbp", "lowercase_ok", "padded_rejected"],
)
def test_a_broker_response_must_match_the_deployment_currency(
    ebull_test_conn: psycopg.Connection[Any],
    broker_kwargs: dict[str, str],
    expected: str | None,
) -> None:
    """#2603 item 4: the broker must answer in the DEPLOYMENT's currency.

    The comparison is equality against ``intent.currency``, not membership of
    ``SUPPORTED_DEPLOYMENT_CURRENCIES``.  Membership is the shape that breaks on
    widening: with ``{"USD","GBP"}`` a GBP-quoted response would satisfy a membership
    test on a USD deployment, and ``_costs`` would then add a USD component and a GBP
    one into one total with no FX -- the arithmetic #2363 refused to perform.  The
    eligibility and cost currencies arrive on separate responses, so tying each to
    ``intent.currency`` is also what ties them to each other.

    The last two cases pin the swap from a ``"USD"`` literal to ``intent.currency`` as
    behaviour-preserving: ``.upper()`` stays, ``strip()`` is not added.  What the
    broker may put in this field is governed by its contract, not by ISO 4217, so the
    operator-input normaliser is deliberately not reused here.
    """
    conn = ebull_test_conn
    signal_id = _seed(conn)

    broker = _broker(
        eligibility_currency=broker_kwargs.get("eligibility_currency", "USD"),
        cost_currency=broker_kwargs.get("cost_currency", "USD"),
    )

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    if expected is None:
        assert result.verdict == "submitted"
    else:
        assert result.verdict == "rejected"
        assert result.reason_code == expected


def test_widening_the_supported_set_does_not_widen_what_the_broker_may_quote(
    ebull_test_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The equality-vs-membership distinction, tested where it actually differs.

    The cases above pass under either shape while the supported set has one member --
    GBP is refused by equality against a USD deployment AND by membership of
    ``{"USD"}``.  So they pin behaviour-preservation, not the property.

    Widening the set separates them.  The deployment stays USD (sql/338 makes anything
    else unrepresentable anyway); only the set moves.  Under equality the GBP cost
    component is still refused.  Under membership it would be ACCEPTED, and ``_costs``
    would then add a GBP amount to a USD total with no FX -- #2603 item 4's "never a
    partial lift", made a failing test rather than a sentence.
    """
    conn = ebull_test_conn
    signal_id = _seed(conn)
    monkeypatch.setattr(
        "app.services.strategy_paper_executor.SUPPORTED_DEPLOYMENT_CURRENCIES",
        frozenset({"USD", "GBP"}),
    )

    result = execute_fired_paper_signal(conn, broker=_broker(cost_currency="GBP"), signal_id=signal_id, now=_NOW)

    assert result.verdict == "rejected"
    assert result.reason_code == "cost_currency_or_value_invalid"


@pytest.mark.parametrize(
    ("arm_settlement_types", "expected"),
    [
        # The measured SPY (3000) shape: every arm is a CFD, so zero qualify. The
        # broker read the request perfectly; this fund is not offered as the
        # underlying product on this account.
        (("cfd", "cfd"), "no_underlying_arm"),
        # Genuinely unreadable: two arms both claiming to be the underlying.
        (("real", "real"), "eligibility_arm_ambiguous"),
        (("real",), None),
    ],
    ids=["cfd_only_is_not_ambiguous", "two_real_arms_are", "one_real_arm_funds"],
)
def test_no_underlying_arm_is_reported_separately_from_a_genuinely_ambiguous_one(
    ebull_test_conn: psycopg.Connection[Any],
    arm_settlement_types: tuple[str, ...],
    expected: str | None,
) -> None:
    """#2678 — zero qualifying arms and many are different answers.

    Zero is a fact about the INSTRUMENT (not offered as the underlying); many is a
    fact about the RESPONSE (we cannot read it). Filing both as
    ``eligibility_arm_ambiguous`` sent triage looking for a parser bug that does
    not exist.

    ⚠ Safe to re-label rather than add-and-migrate because the stored vocabulary
    is open and unread: `strategy_entry_preflights.reason_code` CHECKs only
    non-empty/≤100 (sql/287), `strategy_funding_decisions.reason_code` is plain
    TEXT (sql/281), the API passes `funding_reason` through as a free string, and
    both tables held **0 rows** on dev when this shipped — measured, not assumed.
    """
    conn = ebull_test_conn
    signal_id = _seed(conn)

    broker = _broker(arm_settlement_types=arm_settlement_types)
    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    if expected is None:
        assert result.verdict == "submitted"
    else:
        assert result.verdict == "rejected"
        assert result.reason_code == expected
