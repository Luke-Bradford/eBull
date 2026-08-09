"""Read-only strategy monitoring contracts (#2447)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg

from app.api.strategies import ResultArm, _current_versions, _promotion_refusals, get_strategy_overview
from app.services.cost_model import COST_MODEL_ID
from app.services.equity_curve import BENCHMARK_RULE_ID, SIZING_RULE_ID
from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.position_builder import RULE_SET_VERSION as POSITION_RULE_SET_VERSION
from app.services.research_price_structure_store import QUARANTINE_RULE_SET_VERSION
from app.services.result_ledger import store_holdout_result, store_in_sample_result
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_recent_evidence import RECENT_EVIDENCE_WINDOWS
from app.services.strategy_result import CORPUS_VERSION
from tests.test_result_ledger import build_metrics, build_result


def test_current_versions_cover_the_manifest_including_excluded_s4() -> None:
    versions = _current_versions()
    assert set(versions) == set(STRATEGY_MANIFEST)
    assert "s4-volatility-compression-breakout" in versions
    assert all(version.startswith("strategy-registry-v1+") for version in versions.values())


def test_result_refusals_fail_closed_without_expanding_the_database() -> None:
    row: dict[str, object] = {
        "universe_basis": "survivor_only",
        "carry_unmodelled": True,
        "evaluated_instrument_count": 5266,
        "deflated_sharpe": None,
        "trial_count": None,
        "effective_sample_size": None,
        "synthetic_control_model_id": None,
        "synthetic_control_passed": None,
    }
    assert _promotion_refusals(
        row,
        ambiguity_complete=False,
        quarantine_complete=False,
        accesses_complete=False,
    ) == [
        "universe_basis_not_survivorship_free",
        "carry_unmodelled",
        "holdout_accesses_unrecorded",
        "deflated_sharpe_not_computed",
        "trial_count_undeclared",
        "effective_sample_size_not_computed",
        "ambiguity_arms_not_compared",
        "quarantine_arms_not_compared",
        "synthetic_control_not_run",
    ]


def test_a_complete_measured_result_still_exposes_standing_refusals() -> None:
    row: dict[str, object] = {
        "universe_basis": "survivor_only",
        "carry_unmodelled": True,
        "evaluated_instrument_count": 5266,
        "deflated_sharpe": 0.8,
        "trial_count": 12,
        "effective_sample_size": 200,
        "synthetic_control_model_id": "random-entry-v1",
        "synthetic_control_passed": True,
        "synthetic_control_mean_return_ci_low_pct": -1,
        "synthetic_control_mean_return_ci_high_pct": 1,
        "sharpe": 0.8,
        "synthetic_control_sharpe_threshold": 0.5,
    }
    assert _promotion_refusals(
        row,
        ambiguity_complete=True,
        quarantine_complete=True,
        accesses_complete=True,
    ) == [
        "universe_basis_not_survivorship_free",
        "carry_unmodelled",
    ]


def test_partial_arm_refusals_do_not_claim_the_completed_comparison_is_missing() -> None:
    row: dict[str, object] = {
        "universe_basis": "survivorship_free",
        "carry_unmodelled": False,
        "evaluated_instrument_count": 1,
        "deflated_sharpe": 0.8,
        "trial_count": 12,
        "effective_sample_size": 200,
        "synthetic_control_model_id": "random-entry-v1",
        "synthetic_control_mean_return_ci_low_pct": -1,
        "synthetic_control_mean_return_ci_high_pct": 1,
        "sharpe": 0.8,
        "synthetic_control_sharpe_threshold": 0.5,
    }
    assert _promotion_refusals(
        row,
        ambiguity_complete=True,
        quarantine_complete=False,
        accesses_complete=True,
    ) == ["quarantine_arms_not_compared"]


def test_result_arm_accepts_valid_undefined_downside_metrics() -> None:
    arm = ResultArm(
        result_version="v1",
        ambiguity_arm="best_case",
        quarantine_arm="admitted",
        universe_basis="survivor_only",
        corpus_version="corpus-v1",
        cost_model_id="cost-v1",
        sizing_rule="equal-weight",
        benchmark_rule="buy-and-hold",
        position_rule_set_version="position-v1",
        outcome_rule_set_version="outcome-v1",
        input_rule_set_version="input-v1",
        evaluated_instrument_count=1,
        trade_count=1,
        expectancy_per_trade_pct=Decimal("1"),
        expectancy_ci_low_pct=None,
        expectancy_ci_high_pct=None,
        total_return_pct=Decimal("1"),
        cagr_pct=Decimal("1"),
        sharpe=Decimal("1"),
        sortino=None,
        max_drawdown_pct=Decimal("0"),
        profit_factor=None,
        exposure_time_pct=Decimal("1"),
        turnover_annualised=Decimal("1"),
        return_vs_buy_and_hold_pct=Decimal("1"),
        deflated_sharpe=None,
        promotion_refusals=[],
    )

    assert arm.sortino is None
    assert arm.profit_factor is None


def test_empty_ledgers_still_return_all_manifest_strategies(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    overview = get_strategy_overview(ebull_test_conn)
    assert [item.strategy_id for item in overview.strategies] == sorted(STRATEGY_MANIFEST)
    assert all(item.scan.status == "never_run" for item in overview.strategies)
    assert all(not item.all_recent_evidence_complete for item in overview.strategies)
    assert all(len(item.evidence_windows) == 8 for item in overview.strategies)
    s4 = next(item for item in overview.strategies if item.strategy_id == "s4-volatility-compression-breakout")
    assert not s4.runnable
    assert s4.exclusion_reason is not None


def test_completed_zero_signal_scan_uses_its_watermark(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "s2-cross-sectional-momentum"
    version = _current_versions()[strategy_id]
    frontier = date(2026, 7, 8)
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_scan_watermark (strategy_id, strategy_version, frontier_date)
        VALUES (%s, %s, %s)
        """,
        (strategy_id, version, frontier),
    )

    overview = get_strategy_overview(ebull_test_conn)
    strategy = next(item for item in overview.strategies if item.strategy_id == strategy_id)

    assert strategy.scan.frontier_date == frontier
    assert strategy.scan.status == "stale"
    assert strategy.scan.fired_entries == 0
    assert strategy.scan.fired_exits == 0
    assert strategy.scan.not_fired == 0
    assert strategy.scan.not_evaluable == 0


def test_scan_freshness_reads_the_ingest_census_without_scanning_daily_bars(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "s2-cross-sectional-momentum"
    version = _current_versions()[strategy_id]
    frontier = date(2026, 7, 8)
    ebull_test_conn.execute(
        """
        INSERT INTO research_price_series (
            vendor,vendor_symbol,upstream_source,licence,adjustment_basis,
            first_bar,last_bar,bar_count
        ) VALUES ('test','CENSUS-ONLY','other','test','split_adjusted',%s,%s,1)
        """,
        (frontier, frontier),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_scan_watermark (strategy_id,strategy_version,frontier_date)
        VALUES (%s,%s,%s)
        """,
        (strategy_id, version, frontier),
    )

    overview = get_strategy_overview(ebull_test_conn)
    strategy = next(item for item in overview.strategies if item.strategy_id == strategy_id)

    assert strategy.scan.status == "current"
    assert ebull_test_conn.execute("SELECT count(*) FROM research_price_daily").fetchone() == (0,)


def test_scan_health_reads_durable_daily_counts_after_detail_retention(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "s1-time-series-momentum"
    version = _current_versions()[strategy_id]
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_scan_watermark (strategy_id, strategy_version, frontier_date)
        VALUES (%s, %s, %s)
        """,
        (strategy_id, version, date(2026, 7, 8)),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_signal_daily_counts (
            strategy_id, strategy_version, signal_bar_date,
            signal_kind, verdict, reason_code, row_count
        ) VALUES (%s, %s, %s, 'entry', 'fired', '', 17)
        """,
        (strategy_id, version, date(2026, 7, 7)),
    )

    overview = get_strategy_overview(ebull_test_conn)
    strategy = next(item for item in overview.strategies if item.strategy_id == strategy_id)

    assert strategy.scan.fired_entries == 17


def test_overview_maps_only_exact_current_holdout_provenance(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    strategy_id = "s1-time-series-momentum"
    strategy_version = _current_versions()[strategy_id]
    window = RECENT_EVIDENCE_WINDOWS["primary-2022-plus"].window
    identity = {
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        "window_start": window.start,
        "window_end": window.end,
        "corpus_version": CORPUS_VERSION,
        "cost_model_id": COST_MODEL_ID,
        "sizing_rule": SIZING_RULE_ID,
        "benchmark_rule": BENCHMARK_RULE_ID,
        "position_rule_set_version": POSITION_RULE_SET_VERSION,
        "outcome_rule_set_version": OUTCOME_RULE_SET_VERSION,
        "input_rule_set_version": QUARANTINE_RULE_SET_VERSION,
    }
    metrics = build_metrics(
        profit_factor=None,
        sortino=None,
        losing_trade_count=0,
        losing_period_count=0,
    )
    exact = build_result(**identity, ambiguity_arm="best_case", quarantine_arm="admitted", metrics=metrics)
    store_holdout_result(
        ebull_test_conn,
        exact,
        accessed_by="tests/test_api_strategies.py",
        purpose="verify API result mapping",
    )
    store_holdout_result(
        ebull_test_conn,
        build_result(
            **{**identity, "cost_model_id": "stale-cost-v0"},
            ambiguity_arm="worst_case",
            quarantine_arm="admitted",
        ),
        accessed_by="tests/test_api_strategies.py",
        purpose="prove stale provenance is excluded",
    )
    store_in_sample_result(
        ebull_test_conn,
        build_result(
            **identity,
            namespace="in_sample",
            ambiguity_arm="worst_case",
            quarantine_arm="masked",
        ),
    )

    overview = get_strategy_overview(ebull_test_conn)
    strategy = next(item for item in overview.strategies if item.strategy_id == strategy_id)
    primary = next(item for item in strategy.evidence_windows if item.window_id == "primary-2022-plus")

    assert primary.status == "partial"
    assert len(primary.arms) == 1
    assert primary.arms[0].result_version == exact.identity.version
    assert primary.arms[0].sortino is None
    assert primary.arms[0].profit_factor is None
    assert strategy.legacy_result_count == 2
