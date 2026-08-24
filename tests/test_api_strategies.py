"""Read-only strategy monitoring contracts (#2447)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg
import pytest

from app.api.strategies import (
    _PRESENTATION,
    _TITLES,
    REGIME_COHORT_DISPLAY_ORDER,
    ResultArm,
    _ambiguity_record_from_result_row,
    _current_scan_versions,
    _current_versions,
    _promotion_refusals,
    get_strategy_overview,
)
from app.services.backtest_run import BACKTEST_UNIVERSE, corpus_version_for
from app.services.cost_model import COST_MODEL_ID
from app.services.equity_curve import BENCHMARK_RULE_ID, SIZING_RULE_ID
from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.position_builder import RULE_SET_VERSION as POSITION_RULE_SET_VERSION
from app.services.research_price_structure_store import QUARANTINE_RULE_SET_VERSION
from app.services.result_ledger import store_holdout_result, store_in_sample_result
from app.services.strategy_engine_capital import EngineCapitalObservationError
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_recent_evidence import RECENT_EVIDENCE_WINDOWS
from app.services.strategy_regime_evidence import (
    REGIME_COHORT_LABELS,
    RegimeCohort,
    store_result_regime_cohorts,
)
from app.services.strategy_result import LEGACY_RETURN_BASIS, TOTAL_RETURN_BASIS
from app.services.strategy_result_ambiguity import AMBIGUITY_RULE_VERSION, AmbiguityRecord, record_sha256
from app.services.trial_register import TRIAL_REGISTER, TRIAL_REGISTER_VERSION
from tests.fixtures.ebull_test_db import seed_universe_anchor
from tests.test_result_ledger import build_metrics, build_result

_IMMATERIAL_AMBIGUITY: dict[str, object] = {
    "ambiguity_record_rule_version": AMBIGUITY_RULE_VERSION,
    "ambiguity_comparison_basis": "shared_measurement",
    "ambiguity_best_case_sharpe": None,
    "ambiguity_worst_case_sharpe": None,
    "ambiguity_cohort_gap_threshold": None,
}


def test_operator_ambiguity_payloads_are_hash_verified_when_loaded_from_sql() -> None:
    record = AmbiguityRecord(
        ambiguity_rule_version=AMBIGUITY_RULE_VERSION,
        comparison_basis="arm_sharpes",
        best_case_sharpe=0.7,
        worst_case_sharpe=0.4,
        cohort_gap_threshold=0.5,
    )
    row: dict[str, object] = {
        "support_ambiguity_record_rule_version": record.ambiguity_rule_version,
        "support_ambiguity_comparison_basis": record.comparison_basis,
        "support_ambiguity_best_case_sharpe": record.best_case_sharpe,
        "support_ambiguity_worst_case_sharpe": record.worst_case_sharpe,
        "support_ambiguity_cohort_gap_threshold": record.cohort_gap_threshold,
        "support_ambiguity_payload_sha256": record_sha256(record),
    }

    assert _ambiguity_record_from_result_row(row, prefix="support_") == record
    row["support_ambiguity_payload_sha256"] = "0" * 64
    with pytest.raises(RuntimeError, match="ambiguity payload hash mismatch"):
        _ambiguity_record_from_result_row(row, prefix="support_")


def test_current_versions_cover_the_manifest_including_s4() -> None:
    versions = _current_versions()
    assert set(versions) == set(STRATEGY_MANIFEST)
    assert "s4-volatility-compression-breakout" in versions
    assert all(version.startswith("strategy-registry-v1+") for version in versions.values())


def test_every_manifest_strategy_has_operator_readable_presentation() -> None:
    """A new strategy must not fall through to its internal id and generic copy."""

    assert set(_TITLES) == set(STRATEGY_MANIFEST)
    assert set(_PRESENTATION) == set(STRATEGY_MANIFEST)
    assert all(title != strategy_id for strategy_id, title in _TITLES.items())
    assert all(description != "Evidence-backed automated strategy." for description, _exit in _PRESENTATION.values())


def test_result_refusals_fail_closed_without_expanding_the_database() -> None:
    row: dict[str, object] = {
        **_IMMATERIAL_AMBIGUITY,
        "purpose": "capital_candidate",
        "universe_basis": "survivor_only",
        "carry_unmodelled": True,
        "fx_unmodelled": True,
        "evaluated_instrument_count": 5266,
        "deflated_sharpe": None,
        "trial_count": None,
        "effective_sample_size": None,
        "trial_register_version": None,
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
        "fx_unmodelled",
        "holdout_accesses_unrecorded",
        "deflated_sharpe_not_computed",
        "trial_count_undeclared",
        "effective_sample_size_not_computed",
        "ambiguity_arms_not_compared",
        "quarantine_arms_not_compared",
        "synthetic_control_not_run",
    ]


def test_harness_result_carries_a_permanent_refusal() -> None:
    row: dict[str, object] = {
        **_IMMATERIAL_AMBIGUITY,
        "purpose": "harness_validation",
        "universe_basis": "survivorship_free",
        "carry_unmodelled": False,
        "fx_unmodelled": False,
        "evaluated_instrument_count": 1,
        "deflated_sharpe": 1,
        "trial_count": TRIAL_REGISTER.declared_count,
        "effective_sample_size": 10,
        "trial_register_version": TRIAL_REGISTER_VERSION,
        "synthetic_control_model_id": "control-v1",
        "synthetic_control_mean_return_ci_low_pct": -1,
        "synthetic_control_mean_return_ci_high_pct": 1,
        "sharpe": 1,
        "synthetic_control_sharpe_threshold": 0,
    }
    assert _promotion_refusals(
        row,
        ambiguity_complete=True,
        quarantine_complete=True,
        accesses_complete=True,
    ) == ["harness_validation_only"]


def test_the_operator_view_recomputes_a_material_ambiguity_gap() -> None:
    row: dict[str, object] = {
        **_IMMATERIAL_AMBIGUITY,
        "ambiguity_comparison_basis": "arm_sharpes",
        "ambiguity_best_case_sharpe": 0.75,
        "ambiguity_worst_case_sharpe": 0.25,
        "ambiguity_cohort_gap_threshold": 0.125,
        "purpose": "capital_candidate",
        "universe_basis": "survivorship_free",
        "carry_unmodelled": False,
        "fx_unmodelled": False,
        "evaluated_instrument_count": 1,
        "deflated_sharpe": 1,
        "trial_count": TRIAL_REGISTER.declared_count,
        "effective_sample_size": 10,
        "trial_register_version": TRIAL_REGISTER_VERSION,
        "synthetic_control_model_id": "control-v1",
        "synthetic_control_mean_return_ci_low_pct": -1,
        "synthetic_control_mean_return_ci_high_pct": 1,
        "sharpe": 1,
        "synthetic_control_sharpe_threshold": 0,
    }
    assert _promotion_refusals(
        row,
        ambiguity_complete=True,
        quarantine_complete=True,
        accesses_complete=True,
    ) == ["ambiguity_material"]


def test_a_structurally_complete_pair_with_no_frozen_verdict_still_refuses() -> None:
    row: dict[str, object] = {
        **_IMMATERIAL_AMBIGUITY,
        "ambiguity_record_rule_version": None,
        "purpose": "capital_candidate",
        "universe_basis": "survivorship_free",
        "carry_unmodelled": False,
        "fx_unmodelled": False,
        "evaluated_instrument_count": 1,
        "deflated_sharpe": 1,
        "trial_count": TRIAL_REGISTER.declared_count,
        "effective_sample_size": 10,
        "trial_register_version": TRIAL_REGISTER_VERSION,
        "synthetic_control_model_id": "control-v1",
        "synthetic_control_mean_return_ci_low_pct": -1,
        "synthetic_control_mean_return_ci_high_pct": 1,
        "sharpe": 1,
        "synthetic_control_sharpe_threshold": 0,
    }
    assert _promotion_refusals(
        row,
        ambiguity_complete=True,
        quarantine_complete=True,
        accesses_complete=True,
    ) == ["ambiguity_verdict_unrecorded"]


def test_a_complete_measured_result_still_exposes_standing_refusals() -> None:
    row: dict[str, object] = {
        **_IMMATERIAL_AMBIGUITY,
        "purpose": "capital_candidate",
        "universe_basis": "survivor_only",
        "carry_unmodelled": True,
        "fx_unmodelled": True,
        "evaluated_instrument_count": 5266,
        "deflated_sharpe": 0.8,
        "trial_count": TRIAL_REGISTER.declared_count,
        "effective_sample_size": 200,
        "trial_register_version": "trial-register-2026-08-10",
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
        "fx_unmodelled",
        "trial_register_superseded",
    ]


def test_holdout_display_uses_derived_control_and_not_its_empty_own_columns() -> None:
    row: dict[str, object] = {
        **_IMMATERIAL_AMBIGUITY,
        "namespace": "hold_out",
        "purpose": "capital_candidate",
        "universe_basis": "survivorship_free",
        "carry_unmodelled": False,
        "fx_unmodelled": False,
        "evaluated_instrument_count": 3,
        "deflated_sharpe": 0.8,
        "trial_count": TRIAL_REGISTER.declared_count,
        "effective_sample_size": 200,
        "trial_register_version": TRIAL_REGISTER_VERSION,
        "synthetic_control_model_id": None,
        "synthetic_control_passed": None,
        "control_support_candidate_count": 1,
        "control_synthetic_control_model_id": "random-entry-v1",
        "control_synthetic_control_mean_return_ci_low_pct": -1,
        "control_synthetic_control_mean_return_ci_high_pct": 1,
        "control_sharpe": 0.8,
        "control_synthetic_control_sharpe_threshold": 0.5,
    }

    assert (
        _promotion_refusals(
            row,
            ambiguity_complete=True,
            quarantine_complete=True,
            accesses_complete=True,
        )
        == []
    )


def test_holdout_display_composes_the_exact_in_sample_ambiguity_verdict() -> None:
    row: dict[str, object] = {
        **_IMMATERIAL_AMBIGUITY,
        "ambiguity_comparison_basis": "arm_sharpes",
        "ambiguity_best_case_sharpe": 0.7,
        "ambiguity_worst_case_sharpe": 0.4,
        "ambiguity_cohort_gap_threshold": None,
        "support_ambiguity_record_rule_version": AMBIGUITY_RULE_VERSION,
        "support_ambiguity_comparison_basis": "arm_sharpes",
        "support_ambiguity_best_case_sharpe": 0.7,
        "support_ambiguity_worst_case_sharpe": 0.4,
        "support_ambiguity_cohort_gap_threshold": 0.5,
        "namespace": "hold_out",
        "purpose": "capital_candidate",
        "universe_basis": "survivorship_free",
        "carry_unmodelled": False,
        "fx_unmodelled": False,
        "evaluated_instrument_count": 3,
        "deflated_sharpe": 0.8,
        "trial_count": TRIAL_REGISTER.declared_count,
        "effective_sample_size": 200,
        "trial_register_version": TRIAL_REGISTER_VERSION,
        "synthetic_control_model_id": None,
        "control_support_candidate_count": 1,
        "control_synthetic_control_model_id": "random-entry-v1",
        "control_synthetic_control_mean_return_ci_low_pct": -1,
        "control_synthetic_control_mean_return_ci_high_pct": 1,
        "control_sharpe": 0.8,
        "control_synthetic_control_sharpe_threshold": 0.5,
    }

    assert (
        _promotion_refusals(
            row,
            ambiguity_complete=True,
            quarantine_complete=True,
            accesses_complete=True,
        )
        == []
    )

    row["control_support_candidate_count"] = 0
    assert _promotion_refusals(
        row,
        ambiguity_complete=True,
        quarantine_complete=True,
        accesses_complete=True,
    ) == ["ambiguity_arms_not_compared", "synthetic_control_not_run"]


def test_partial_arm_refusals_do_not_claim_the_completed_comparison_is_missing() -> None:
    row: dict[str, object] = {
        **_IMMATERIAL_AMBIGUITY,
        "purpose": "capital_candidate",
        "universe_basis": "survivorship_free",
        "carry_unmodelled": False,
        "fx_unmodelled": False,
        "evaluated_instrument_count": 1,
        "deflated_sharpe": 0.8,
        "trial_count": TRIAL_REGISTER.declared_count,
        "effective_sample_size": 200,
        "trial_register_version": TRIAL_REGISTER_VERSION,
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


def test_a_current_register_label_cannot_hide_a_stale_trial_count() -> None:
    row: dict[str, object] = {
        **_IMMATERIAL_AMBIGUITY,
        "purpose": "capital_candidate",
        "universe_basis": "survivorship_free",
        "carry_unmodelled": False,
        "fx_unmodelled": False,
        "evaluated_instrument_count": 1,
        "deflated_sharpe": 0.8,
        "trial_count": 12,
        "effective_sample_size": 200,
        "trial_register_version": TRIAL_REGISTER_VERSION,
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
    ) == ["trial_register_superseded"]


def test_a_missing_count_is_not_also_described_as_superseded_when_the_version_is_current() -> None:
    row: dict[str, object] = {
        **_IMMATERIAL_AMBIGUITY,
        "purpose": "capital_candidate",
        "universe_basis": "survivorship_free",
        "carry_unmodelled": False,
        "fx_unmodelled": False,
        "evaluated_instrument_count": 1,
        "deflated_sharpe": 0.8,
        "trial_count": None,
        "effective_sample_size": 200,
        "trial_register_version": TRIAL_REGISTER_VERSION,
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
    ) == ["trial_count_undeclared"]


def test_result_arm_accepts_valid_undefined_downside_metrics() -> None:
    arm = ResultArm(
        result_version="v1",
        purpose="capital_candidate",
        ambiguity_arm="best_case",
        quarantine_arm="admitted",
        universe_basis="survivor_only",
        corpus_version="corpus-v1",
        cost_model_id="cost-v1",
        sizing_rule="equal-weight",
        benchmark_rule="buy-and-hold",
        return_basis=TOTAL_RETURN_BASIS,
        ambiguity_rule_version=AMBIGUITY_RULE_VERSION,
        position_rule_set_version="position-v1",
        outcome_rule_set_version="outcome-v1",
        input_rule_set_version="input-v1",
        evaluated_instrument_count=1,
        trade_count=1,
        losing_trade_count=0,
        open_trade_count=0,
        unpriced_trade_count=0,
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
        metric_set_id="criterion7-v1",
        median_hold_days=None,
        hold_days_p25=None,
        hold_days_p75=None,
        promotion_refusals=[],
        regime_cohorts=[],
    )

    assert arm.sortino is None
    assert arm.profit_factor is None


def test_empty_ledgers_still_return_all_manifest_strategies(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    seed_universe_anchor(ebull_test_conn)
    overview = get_strategy_overview(ebull_test_conn)
    assert [item.strategy_id for item in overview.strategies] == sorted(STRATEGY_MANIFEST)
    assert all(item.scan.status == "never_run" for item in overview.strategies)
    assert all(not item.all_recent_evidence_complete for item in overview.strategies)
    assert all(len(item.evidence_windows) == len(RECENT_EVIDENCE_WINDOWS) for item in overview.strategies)
    assert not overview.automation_readiness.ready
    assert overview.automation_readiness.state == "no_capital_candidates"
    assert overview.automation_readiness.capital_candidate_count == 0
    assert overview.automation_readiness.resolved_forecasts == 0
    assert overview.account_equity_evidence.status == "unavailable"
    assert overview.account_equity_evidence.days_collected == 0
    s4 = next(item for item in overview.strategies if item.strategy_id == "s4-volatility-compression-breakout")
    assert s4.runnable
    assert s4.exclusion_reason is None


def test_incomplete_shared_capital_keeps_overview_readable_but_withholds_headroom(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_universe_anchor(ebull_test_conn)

    def incomplete(_conn: object) -> None:
        raise EngineCapitalObservationError("test incomplete ownership")

    monkeypatch.setattr("app.api.strategies.load_engine_capital_authority", incomplete)
    overview = get_strategy_overview(ebull_test_conn)

    assert not overview.paper_pool.capital_observation_complete
    assert overview.paper_pool.invested_capital is None
    assert overview.paper_pool.remaining_capital is None


def test_completed_zero_signal_scan_uses_its_watermark(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    seed_universe_anchor(ebull_test_conn)
    strategy_id = "s2-cross-sectional-momentum"
    version = _current_scan_versions()[strategy_id]
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


# `test_scan_freshness_reads_the_ingest_census_without_scanning_daily_bars` stood
# here until #2817 and is deliberately gone rather than repaired. It seeded ONE
# `research_price_series` row and asserted `scan.status == "current"` — i.e. it
# asserted `MAX(last_bar)` over the research archive, which is precisely the
# basis #2809 removed as "wrong table, wrong statistic, wrong population"
# (`_corpus_frontier`'s docstring). The behaviour it pinned no longer exists, so
# it failed from that merge onward; the replacement contract, including a
# structural guard that the overview never reads that archive again, is
# `tests/test_2809_scan_freshness_basis.py`.


def test_scan_health_reads_durable_daily_counts_after_detail_retention(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    seed_universe_anchor(ebull_test_conn)
    strategy_id = "s1-time-series-momentum"
    version = _current_scan_versions()[strategy_id]
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
    # ⚠ The FIRE RATE off the same seeded row (#2806). #2803 moved this seeding to
    # the scan basis and asserted only the scan card, so the fire-rate block —
    # which reads the very same table through `load_fire_rate` — kept reporting
    # `never_scanned` for every strategy with nothing failing.
    assert strategy.fire_rate.scanned_days == 1
    assert strategy.fire_rate.fired_days == 1
    assert strategy.fire_rate.fired_entry_signals == 17
    assert strategy.fire_rate.fired_share_of_evaluable == Decimal("1.0000")
    assert strategy.fire_rate.share_unavailable_reason is None
    # One bar date carries no span, so the throughput rate refuses rather than
    # inventing a week — that refusal is the correct state here, not a gap.
    assert strategy.fire_rate.entries_per_calendar_week is None
    assert strategy.fire_rate.weekly_rate_unavailable_reason == "single_scan_day"


def test_overview_maps_only_exact_current_holdout_provenance(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    seed_universe_anchor(ebull_test_conn)
    strategy_id = "s1-time-series-momentum"
    strategy_version = _current_versions()[strategy_id]
    window = RECENT_EVIDENCE_WINDOWS["primary-2022-plus"].window
    identity = {
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        "window_start": window.start,
        "window_end": window.end,
        "corpus_version": corpus_version_for(BACKTEST_UNIVERSE),
        "cost_model_id": COST_MODEL_ID,
        "sizing_rule": SIZING_RULE_ID,
        "benchmark_rule": BENCHMARK_RULE_ID,
        "return_basis": TOTAL_RETURN_BASIS,
        "ambiguity_rule_version": AMBIGUITY_RULE_VERSION,
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
    store_holdout_result(
        ebull_test_conn,
        build_result(
            **{**identity, "return_basis": LEGACY_RETURN_BASIS},
            ambiguity_arm="worst_case",
            quarantine_arm="masked",
        ),
        accessed_by="tests/test_api_strategies.py",
        purpose="prove legacy price-return evidence is excluded",
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
    assert strategy.legacy_result_count == 3


def test_overview_regime_cohorts_follow_the_result_ids_not_a_second_pin_predicate(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#2817. Two arms, one current and one on a stale cost model, both with cohorts.

    The current arm must carry ITS OWN cohorts and nothing else. Storing cohorts
    against the excluded result is the point of the fixture: a reader that
    restated `_RESULTS_SQL`'s identity pins instead of keying on the ids that
    query returned would either leak the stale split in or drop both.

    Also pins the display order. The two labels are stored `bear_quiet` then
    `bull_quiet` — which is what `build_regime_cohorts` writes, since it sorts
    alphabetically — and must READ back bull-before-bear.
    """
    seed_universe_anchor(ebull_test_conn)
    strategy_id = "s1-time-series-momentum"
    strategy_version = _current_versions()[strategy_id]
    window = RECENT_EVIDENCE_WINDOWS["primary-2022-plus"].window
    identity = {
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        "window_start": window.start,
        "window_end": window.end,
        "corpus_version": corpus_version_for(BACKTEST_UNIVERSE),
        "cost_model_id": COST_MODEL_ID,
        "sizing_rule": SIZING_RULE_ID,
        "benchmark_rule": BENCHMARK_RULE_ID,
        "return_basis": TOTAL_RETURN_BASIS,
        "ambiguity_rule_version": AMBIGUITY_RULE_VERSION,
        "position_rule_set_version": POSITION_RULE_SET_VERSION,
        "outcome_rule_set_version": OUTCOME_RULE_SET_VERSION,
        "input_rule_set_version": QUARANTINE_RULE_SET_VERSION,
    }
    metrics = build_metrics(trade_count=5, losing_trade_count=2, open_trade_count=0)
    current_id = store_holdout_result(
        ebull_test_conn,
        build_result(**identity, ambiguity_arm="best_case", quarantine_arm="admitted", metrics=metrics),
        accessed_by="tests/test_api_strategies.py",
        purpose="verify regime cohort mapping",
    )
    stale_id = store_holdout_result(
        ebull_test_conn,
        build_result(
            **{**identity, "cost_model_id": "stale-cost-v0"},
            ambiguity_arm="worst_case",
            quarantine_arm="admitted",
            metrics=metrics,
        ),
        accessed_by="tests/test_api_strategies.py",
        purpose="prove an excluded result's cohorts cannot leak in",
    )
    # `worst_trade_pct` must not exceed the cohort expectancy (`RegimeCohort`
    # refuses it), so the two arms carry their own worst trade as well.
    for result_id, expectancy, worst in ((current_id, 1.5, -3.5), (stale_id, -9.5, -20.0)):
        store_result_regime_cohorts(
            ebull_test_conn,
            result_id=result_id,
            cohorts=[
                RegimeCohort(
                    regime="bear_quiet",
                    trade_count=2,
                    instrument_count=2,
                    decision_date_count=2,
                    losing_trade_count=1,
                    expectancy_pct=expectancy,
                    profit_factor=1.25,
                    worst_trade_pct=worst,
                    effective_sample_size=None,
                    expectancy_ci_low_pct=None,
                    expectancy_ci_high_pct=None,
                    bootstrap_block_length=None,
                    bootstrap_cluster_count=None,
                    bootstrap_resamples=None,
                    bootstrap_seed=None,
                    bootstrap_design_effect=None,
                    bootstrap_model_id=None,
                ),
                RegimeCohort(
                    regime="bull_quiet",
                    trade_count=3,
                    instrument_count=3,
                    decision_date_count=3,
                    losing_trade_count=0,
                    expectancy_pct=expectancy,
                    # Null exactly because the cohort has no losing trade — the
                    # strongest cohort, not an absent measurement.
                    profit_factor=None,
                    worst_trade_pct=worst,
                    effective_sample_size=None,
                    expectancy_ci_low_pct=None,
                    expectancy_ci_high_pct=None,
                    bootstrap_block_length=None,
                    bootstrap_cluster_count=None,
                    bootstrap_resamples=None,
                    bootstrap_seed=None,
                    bootstrap_design_effect=None,
                    bootstrap_model_id=None,
                ),
            ],
            expected_trade_count=5,
        )

    overview = get_strategy_overview(ebull_test_conn)
    strategy = next(item for item in overview.strategies if item.strategy_id == strategy_id)
    primary = next(item for item in strategy.evidence_windows if item.window_id == "primary-2022-plus")

    assert len(primary.arms) == 1
    cohorts = primary.arms[0].regime_cohorts
    assert [item.regime for item in cohorts] == ["bull_quiet", "bear_quiet"]
    assert [item.trade_count for item in cohorts] == [3, 2]
    assert sum(item.trade_count for item in cohorts) == primary.arms[0].trade_count
    # The stale arm's -9.5 never appears: it is excluded by provenance, so its
    # cohorts are not fetched at all.
    assert all(item.expectancy_pct == Decimal("1.5") for item in cohorts)
    assert cohorts[0].profit_factor is None
    assert cohorts[0].losing_trade_count == 0
    assert cohorts[1].profit_factor == Decimal("1.25")


def test_overview_leaves_regime_cohorts_empty_for_a_result_written_before_the_writer_existed(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#2817. Every one of the 324 rows stored on dev predates #2726 and has no split.

    The arm must report an EMPTY list beside a non-zero `trade_count`, which is
    what lets the reader say "not measured for this result version" instead of
    "no trades in any regime".
    """
    seed_universe_anchor(ebull_test_conn)
    strategy_id = "s1-time-series-momentum"
    window = RECENT_EVIDENCE_WINDOWS["primary-2022-plus"].window
    store_holdout_result(
        ebull_test_conn,
        build_result(
            strategy_id=strategy_id,
            strategy_version=_current_versions()[strategy_id],
            window_start=window.start,
            window_end=window.end,
            corpus_version=corpus_version_for(BACKTEST_UNIVERSE),
            cost_model_id=COST_MODEL_ID,
            sizing_rule=SIZING_RULE_ID,
            benchmark_rule=BENCHMARK_RULE_ID,
            return_basis=TOTAL_RETURN_BASIS,
            ambiguity_rule_version=AMBIGUITY_RULE_VERSION,
            position_rule_set_version=POSITION_RULE_SET_VERSION,
            outcome_rule_set_version=OUTCOME_RULE_SET_VERSION,
            input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
            ambiguity_arm="best_case",
            quarantine_arm="admitted",
        ),
        accessed_by="tests/test_api_strategies.py",
        purpose="verify an unsplit result reads as unsplit",
    )

    overview = get_strategy_overview(ebull_test_conn)
    strategy = next(item for item in overview.strategies if item.strategy_id == strategy_id)
    primary = next(item for item in strategy.evidence_windows if item.window_id == "primary-2022-plus")

    assert primary.arms[0].trade_count > 0
    assert primary.arms[0].regime_cohorts == []


def test_regime_cohort_display_order_covers_every_label_the_producer_can_write() -> None:
    """A label added to `RegimeCohortLabel` must land in a defined display slot.

    `REGIME_COHORT_DISPLAY_ORDER` is `get_args(RegimeCohortLabel)`, so this holds
    by construction today; the test is what stops a later hand-written copy from
    silently dropping a label and raising `ValueError` inside the sort.
    """
    assert set(REGIME_COHORT_DISPLAY_ORDER) == set(REGIME_COHORT_LABELS)
    assert REGIME_COHORT_DISPLAY_ORDER[-1] == "unclassified"


def test_overview_declares_exactly_the_manifest_strategies_with_exit_adapters_as_forward_resolvable(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    seed_universe_anchor(ebull_test_conn)
    overview = get_strategy_overview(ebull_test_conn)
    support = {item.strategy_id: item.forward_outcome_supported for item in overview.strategies}

    assert support == {
        "s1-time-series-momentum": False,
        "s2-cross-sectional-momentum": False,
        "s3-mean-reversion-in-trend": False,
        "s4-volatility-compression-breakout": True,
        "s5-support-bounce": True,
        "s6-resistance-breakout": True,
        "s7-trend-pullback": True,
        "s8-range-mean-reversion": True,
        "s9-squeeze-expansion": True,
        "s10-relative-strength-leader": False,
        # #2840's S-11 carries S-4's bracket, so it resolves forward like S-4.
        "s11-volatile-regime-gated-breakout": True,
    }
