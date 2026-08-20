"""Phase 5c — the frozen split, the result identity, and the promotion refusal.

Pure tier: no database. The gate is a pure function by design (phase 7's
``execution_guard`` calls it in the order path), so everything it does is
table-testable.

⚠ THE ``SPEC_*`` LITERALS BELOW ARE RESTATED, NOT IMPORTED, and that is the
#2240 S-3 lesson: *"a reference that IMPORTS the constant it validates is a
tautology"*. They are transcribed from
``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §5.2 and §6, and
``TestSpecConstants`` is the single bridge asserting the module agrees with
them. Change the module's literal and exactly one test fails, loudly, naming
the spec section — rather than every test quietly re-passing against the new
value.
"""

from __future__ import annotations

import re
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from app.services.deflated_sharpe import DeflatedSharpeResult
from app.services.random_entry_cohort import (
    MATCH_QUALITY_POLICY_ID,
    SyntheticControl,
    SyntheticControlMatchQuality,
)
from app.services.strategy_promotion_evidence import (
    EVIDENCE_VERSION,
    REQUIRED_CHALLENGERS,
    REQUIRED_CONTRASTS,
    REQUIRED_COST_INPUTS,
    ChallengerEvidence,
    ExpectedValueBucket,
    OutcomeContrast,
    PromotionEvidence,
    RecentYearEvidence,
)
from app.services.strategy_result import (
    AMBIGUITY_AWARE_RESULT_SET_ID,
    BENCHMARK_RULE,
    CORPUS_VERSION,
    EVALUATION_WINDOW_END,
    EVALUATION_WINDOW_START,
    HOLDOUT_BOUNDARY,
    HOLDOUT_WEIGHTING,
    LEGACY_RETURN_BASIS,
    METRIC_AXIS_RULE_VERSION,
    PROMOTABLE_UNIVERSE_BASES,
    PROMOTION_REFUSALS,
    RESULT_SET_ID,
    SIZING_RULE,
    TOTAL_RETURN_BASIS,
    TOTAL_RETURN_RESULT_SET_ID,
    UNIVERSE_BASES,
    PromotionCandidate,
    ResultIdentity,
    StrategyResult,
    check_promotable,
    deflation_promotion_refusals,
    holdout_count_promotion_refusals,
    is_promotable,
    metric_axis_sha256,
    namespace_for_bar,
    namespace_for_position,
    namespace_for_signal,
    purpose_promotion_refusals,
    synthetic_control_promotion_refusals,
)
from app.services.strategy_result_ambiguity import AMBIGUITY_RULE_VERSION, LEGACY_AMBIGUITY_RULE_VERSION
from app.services.strategy_statistics import StrategyMetrics, periods_per_year
from app.services.trial_register import TRIAL_REGISTER, TRIAL_REGISTER_VERSION

# --- transcribed from the spec, never imported -----------------------------

#: §5.2's adopted boundary, the FIRST HOLD-OUT BAR.
SPEC_HOLDOUT_BOUNDARY = date(2021, 6, 29)
#: §5.2: "Adopted: bar-weighted."
SPEC_HOLDOUT_WEIGHTING = "bar"
#: §5.2's frozen evaluation end; §7 M2's corpus first bar.
SPEC_WINDOW_END = date(2026, 7, 8)
SPEC_WINDOW_START = date(1962, 1, 2)
#: §7 M2 — the one vendor the corpus carries.
SPEC_CORPUS_VENDOR = "paperswithbacktest/Stocks-Daily-Price"
#: §5.4's declared v1 rule.
SPEC_SIZING_RULE = "equal_weight_concurrent_v1"
#: sql/255's vocabulary, which §6 reuses for the result row.
SPEC_UNIVERSE_BASES = {"survivor_only", "survivorship_free"}
_CURRENT_TEST_AXIS = (EVALUATION_WINDOW_START, date(2021, 6, 28))
_CURRENT_TEST_PPY = periods_per_year(_CURRENT_TEST_AXIS)
_CURRENT_TEST_CAGR = (1.21 ** (1.0 / ((len(_CURRENT_TEST_AXIS) - 1) / _CURRENT_TEST_PPY)) - 1.0) * 100.0


def _identity(**overrides: object) -> ResultIdentity:
    base: dict[str, object] = {
        "strategy_id": "S-1",
        "strategy_version": "strategy-registry-v1+aaaaaaaaaaaa",
        "result_scope": "sleeve",
        "namespace": "hold_out",
        "ambiguity_arm": "worst_case",
        "quarantine_arm": "masked",
        "sizing_rule": SIZING_RULE,
        "benchmark_rule": BENCHMARK_RULE,
        "cost_model_id": "static-p75-insession-v1",
        "corpus_version": CORPUS_VERSION,
        "window_start": EVALUATION_WINDOW_START,
        "window_end": EVALUATION_WINDOW_END,
        "position_rule_set_version": "position-builder-v1+bbbbbbbbbbbb",
        "outcome_rule_set_version": "outcome-resolver-v1+cccccccccccc",
        "input_rule_set_version": "price-quarantine-v1+dddddddddddd",
        "return_basis": LEGACY_RETURN_BASIS,
    }
    base.update(overrides)
    return ResultIdentity(**base)  # type: ignore[arg-type]


def _metrics(**overrides: object) -> StrategyMetrics:
    """A complete criterion-7 set, including stage 5e-2's block-bootstrap block.

    ⚠ The whole bootstrap set is present, not just ``effective_sample_size``:
    ``StrategyMetrics`` refuses a partial one (criterion 3 asks for the sample
    size AND its interval), so setting the ESS alone would raise here rather
    than produce the clean candidate every promotion-gate test starts from."""
    base: dict[str, object] = {
        "expectancy_per_trade_pct": 0.5,
        "profit_factor": 1.2,
        "cagr_pct": _CURRENT_TEST_CAGR,
        "annualised_volatility_pct": 12.0,
        "sharpe": 0.33,
        "sortino": 0.44,
        "max_drawdown_pct": -18.0,
        "exposure_time_pct": 61.0,
        "turnover_annualised": 2.5,
        "trade_count": 100,
        "effective_sample_size": 41.0,
        "return_vs_buy_and_hold_pct": -1.5,
        "losing_trade_count": 40,
        "losing_period_count": 300,
        "open_trade_count": 2,
        "unpriced_trade_count": 1,
        "periods_per_year": _CURRENT_TEST_PPY,
        "total_return_pct": 21.0,
        "buy_and_hold_return_pct": 22.5,
        "expectancy_ci_low_pct": -0.2,
        "expectancy_ci_high_pct": 1.1,
        "bootstrap_block_length": 9,
        "bootstrap_cluster_count": 80,
        "bootstrap_resamples": 2_000,
        "bootstrap_seed": 20260807,
        "bootstrap_design_effect": 2.44,
        "bootstrap_model_id": "c3-block-bootstrap-v1",
        # #2623 gap 1 — required alongside a non-zero trade_count under the
        # current METRIC_SET_ID.
        "hold_days_p25": 3.0,
        "median_hold_days": 8.0,
        "hold_days_p75": 21.0,
    }
    base.update(overrides)
    return StrategyMetrics(**base)  # type: ignore[arg-type]


#: Every block-bootstrap field, so a test can clear the SET rather than the one
#: field it cares about. ⚠ Clearing ``effective_sample_size`` alone raises — the
#: all-or-nothing invariant — so a test wanting "no criterion-3 measurement"
#: must go through here.
_BOOTSTRAP_FIELDS = (
    "effective_sample_size",
    "expectancy_ci_low_pct",
    "expectancy_ci_high_pct",
    "bootstrap_block_length",
    "bootstrap_cluster_count",
    "bootstrap_resamples",
    "bootstrap_seed",
    "bootstrap_design_effect",
    "bootstrap_model_id",
)


def _metrics_without_bootstrap(**overrides: object) -> StrategyMetrics:
    """A criterion-7 set with no criterion-3 measurement on it at all.

    This is what ``compute_metrics`` returns when the caller declares no
    ``bootstrap_seed`` — the fail-closed state the promotion gate refuses on.
    """
    return _metrics(**{field: None for field in _BOOTSTRAP_FIELDS}, **overrides)


def _result(**overrides: object) -> StrategyResult:
    axis = _CURRENT_TEST_AXIS
    base: dict[str, object] = {
        "identity": _identity(
            namespace="in_sample",
            metric_axis_rule_version=METRIC_AXIS_RULE_VERSION,
            metric_axis_dates=axis,
            metric_axis_start=axis[0],
            metric_axis_end=axis[-1],
            metric_axis_digest=metric_axis_sha256(axis),
            opportunity_set_digest="a" * 64,
        ),
        "purpose": "capital_candidate",
        "metrics": _metrics(),
        "universe_basis": "survivor_only",
        "carry_unmodelled": True,
        "fx_unmodelled": True,
        "evaluated_instrument_count": 3,
        "trial_count": None,
        "deflated_sharpe": None,
    }
    base.update(overrides)
    return StrategyResult(**base)  # type: ignore[arg-type]


def _passing_control(**overrides: object) -> SyntheticControl:
    """§9's control on a cohort this strategy clears BOTH thresholds against.

    ⚠ The two strategy-side figures must equal ``_metrics()``'s ``sharpe`` and
    ``total_return_pct`` — ``StrategyResult`` binds them, because a control
    evaluated against one Sharpe and stored beside another describes a
    comparison nobody made.
    """
    base: dict[str, object] = {
        "model_id": "permuted-entry-uniform-gap-v1",
        "cohort_size": 1000,
        "root_seed": 20260808,
        # Straddles zero → §9's first threshold holds.
        "mean_return_pct": 0.1,
        "mean_return_ci_low_pct": -0.4,
        "mean_return_ci_high_pct": 0.6,
        "sharpe_percentile": 95.0,
        # Below the metric set's 0.33 → §9's second threshold holds.
        "cohort_sharpe_threshold": 0.20,
        "strategy_sharpe": 0.33,
        "cohort_return_threshold_pct": 5.0,
        "strategy_return_pct": 21.0,
        "match_quality": SyntheticControlMatchQuality(
            policy_id=MATCH_QUALITY_POLICY_ID,
            placement_space_id="test-fixed-panel-v1",
            matchable_trade_count=100,
            cohort_mean_trade_count=100.0,
            unmatchable_by_reason={},
            no_slack_series=0,
            series_placed=3,
            strategy_exposure_time_pct=61.0,
            cohort_mean_exposure_time_pct=61.0,
            strategy_turnover_annualised=2.5,
            cohort_mean_turnover_annualised=2.5,
        ),
    }
    base.update(overrides)
    return SyntheticControl(**base)  # type: ignore[arg-type]


class TestSyntheticControlMatchQuality:
    def test_exact_policy_does_not_hide_sub_nanounit_residuals(self) -> None:
        match = _passing_control().match_quality
        assert match is not None
        changed = replace(
            match,
            cohort_mean_trade_count=match.cohort_mean_trade_count + 5e-10,
            cohort_mean_exposure_time_pct=match.cohort_mean_exposure_time_pct + 5e-10,
            cohort_mean_turnover_annualised=match.cohort_mean_turnover_annualised + 5e-10,
        )
        assert not changed.population_matches
        assert not changed.exposure_matches
        assert not changed.turnover_matches
        assert not changed.passed

    @pytest.mark.parametrize(
        ("field", "value"),
        [("cohort_mean_trade_count", float("nan")), ("cohort_mean_turnover_annualised", float("inf"))],
    )
    def test_non_finite_match_measurements_are_refused(self, field: str, value: float) -> None:
        match = _passing_control().match_quality
        assert match is not None
        with pytest.raises(ValueError, match="must be finite"):
            replace(match, **{field: value})

    def test_the_reason_census_cannot_change_after_validation(self) -> None:
        match = _passing_control().match_quality
        assert match is not None
        with pytest.raises(TypeError):
            match.unmatchable_by_reason["late mutation"] = 1  # type: ignore[index]


def _deflated_result(**overrides: object) -> DeflatedSharpeResult:
    """A complete DSR provenance block for promotion-gate tests."""
    base: dict[str, object] = {
        "deflated_sharpe": 0.72,
        "expected_max_sharpe": 0.015,
        "trade_sharpe": 0.017,
        "skewness": -0.4,
        "kurtosis": 8.0,
        "effective_sample_size": 41.0,
        "declared_trials": TRIAL_REGISTER.declared_count,
        "independent_trials": 9.0,
        "average_trial_correlation": 0.2,
        "trial_sharpe_variance": 1e-4,
        "measured_trials": 2,
        "trial_register_version": TRIAL_REGISTER_VERSION,
    }
    base.update(overrides)
    return DeflatedSharpeResult(**base)  # type: ignore[arg-type]


#: Everything a clean result needs EXCEPT its synthetic control, so a test can
#: vary that one field without restating (and drifting from) the other four.
_CLEAN_RESULT_FIELDS: dict[str, object] = {
    "universe_basis": "survivorship_free",
    "carry_unmodelled": False,
    "fx_unmodelled": False,
    "trial_count": TRIAL_REGISTER.declared_count,
    "deflated_sharpe": Decimal("0.72"),
    "deflated": _deflated_result(),
}


def _passing_promotion_evidence(**overrides: object) -> PromotionEvidence:
    base: dict[str, object] = {
        "evidence_version": EVIDENCE_VERSION,
        "causal_observation_rule_version": "causal-v1",
        "fill_rule_version": "fills-v1",
        "overlap_rule_version": "overlap-v1",
        "after_cost_expectancy_ci_low_pct": Decimal("0.1"),
        "max_drawdown_pct": Decimal("-8"),
        "expected_shortfall_5_pct": Decimal("-3"),
        "worst_gap_pct": Decimal("-5"),
        "excluding_best_1_expectancy_pct": Decimal("0.2"),
        "recent_year_stable": True,
        "recent_years_evaluated": 3,
        "recent_year_evidence": (
            RecentYearEvidence(2024, 34, Decimal("0.2"), Decimal("-0.1"), True),
            RecentYearEvidence(2025, 33, Decimal("0.3"), Decimal("0.0"), True),
            RecentYearEvidence(2026, 33, Decimal("0.4"), Decimal("0.1"), True),
        ),
        "max_date_contribution_pct": Decimal("8"),
        "max_name_contribution_pct": Decimal("7"),
        "max_sector_contribution_pct": Decimal("20"),
        "max_concurrency": 12,
        "capacity_usd": Decimal("100000"),
        "risk_limits_version": "test-risk-v1",
        "risk_limits_passed": True,
        "probability_calibration_passed": True,
        "path_diagnostics_complete": True,
        "outcome_count": 100,
        "profitable_outcome_count": 60,
        "losing_outcome_count": 40,
        "flat_outcome_count": 0,
        "target_first_count": 40,
        "stop_first_count": 30,
        "timeout_count": 30,
        "ambiguous_path_count": 2,
        "observed_cost_inputs": REQUIRED_COST_INPUTS,
        "cost_observed_on": date(2026, 7, 8),
        "cost_valid_through": date(2026, 8, 13),
        "cost_source_version": "etoro-quote-v1",
        "spread_bps": Decimal("8"),
        "slippage_bps": Decimal("5"),
        "financing_bps_per_day": Decimal("1"),
        "fx_bps": Decimal("2"),
        "broker_eligible": True,
        "challengers": tuple(
            ChallengerEvidence(
                role=role,
                observation_count=100,
                expectancy_pct=Decimal("0.1"),
                candidate_minus_challenger_pct=Decimal("0.2"),
                same_observations_and_fills=True,
                causal_observation_rule_version="causal-v1",
                fill_rule_version="fills-v1",
                overlap_rule_version="overlap-v1",
            )
            for role in sorted(REQUIRED_CHALLENGERS)
        ),
        "ev_buckets": (
            ExpectedValueBucket(1, 34, Decimal("-0.2"), Decimal("-0.1")),
            ExpectedValueBucket(2, 33, Decimal("0.1"), Decimal("0.2")),
            ExpectedValueBucket(3, 33, Decimal("0.4"), Decimal("0.5")),
        ),
        "outcome_contrasts": tuple(
            OutcomeContrast(role, 60, 40, Decimal("1"), Decimal("0"), Decimal("1"))
            for role in sorted(REQUIRED_CONTRASTS)
        ),
    }
    base.update(overrides)
    return PromotionEvidence(**base)  # type: ignore[arg-type]


def _clean_candidate(**overrides: object) -> PromotionCandidate:
    """A candidate that passes EVERY check — the only shape ``check_promotable`` clears.

    ⚠ It is not reachable from today's pipeline and says so: the basis is
    ``survivorship_free``, which §6 records as *"not a value any current corpus
    can produce"*, and carry is charged, which #2277 has not established. It
    exists so each test can break exactly one thing and attribute the refusal.
    """
    base: dict[str, object] = {
        "result": _result(**_CLEAN_RESULT_FIELDS, synthetic_control=_passing_control()),
        "evaluated_instrument_ids": frozenset({1, 2, 3}),
        "validated_universe_ids": frozenset({1, 2, 3, 4}),
        "holdout_evaluations": 1,
        "recorded_accesses": 1,
        "ambiguity_material": False,
        "quarantine_arms_compared": True,
        "promotion_evidence": _passing_promotion_evidence(),
    }
    base.update(overrides)
    return PromotionCandidate(**base)  # type: ignore[arg-type]


class TestSpecConstants:
    """The one bridge between the transcribed spec literals and the module.

    ⚠ These are the assertions a recalibration or a re-freeze must break. Every
    other test in this file uses the module's constants, so without this class
    the whole file would re-pass against a silently changed literal.
    """

    def test_the_boundary_is_the_spec_boundary(self) -> None:
        assert HOLDOUT_BOUNDARY == SPEC_HOLDOUT_BOUNDARY

    def test_the_SQL_migrations_carry_the_same_dates_as_python(self) -> None:
        """⚠ EVERY DATE LITERAL IN A MIGRATION IS ANOTHER COPY OF A PYTHON CONSTANT.

        `sql/359`'s `strategy_results_metric_axis_namespace` and `sql/360`'s
        control-support predicate both hardcode boundary dates. A CHECK or a
        WHERE that disagrees with the code either rejects valid rows or admits
        invalid ones, and nothing sat in between — the Python constants are
        pinned against the spec above, so a recalibration fails THERE while the
        SQL keeps the old date silently.

        ⚠ Reads each literal OUT of the migration rather than restating it, so
        this test cannot itself become one more copy of the same date.

        ⚠ Scoped per file to the PREDICATE that owns the literal, never to the
        whole file — `sql/359`'s namespace CHECK and `sql/360`'s `candidates`
        CTE. `sql/359` also registers the evidence windows, which carry their own
        different and correct dates; and an unrelated literal added to either
        file later must not break this test for the wrong reason.
        """
        sql_dir = Path(__file__).resolve().parents[1] / "sql"

        namespace_block = re.search(
            r"ADD CONSTRAINT strategy_results_metric_axis_namespace CHECK \((.*?)\n    \)",
            (sql_dir / "359_strategy_result_metric_axis.sql").read_text(),
            re.S,
        )
        assert namespace_block, "constraint strategy_results_metric_axis_namespace not found in sql/359"

        candidates_cte = re.search(
            r"WITH candidates AS \((.*?)\n\)",
            (sql_dir / "360_strategy_control_support_metric_axis.sql").read_text(),
            re.S,
        )
        assert candidates_cte, "CTE `candidates` not found in sql/360"

        cases: list[tuple[str, str, set[date]]] = [
            ("sql/359 metric_axis_namespace", namespace_block.group(1), {HOLDOUT_BOUNDARY}),
            (
                "sql/360 candidates CTE",
                candidates_cte.group(1),
                {EVALUATION_WINDOW_START, HOLDOUT_BOUNDARY},
            ),
        ]
        for label, text, expected in cases:
            literals = {date.fromisoformat(m) for m in re.findall(r"DATE '(\d{4}-\d{2}-\d{2})'", text)}
            assert literals == expected, f"{label} carries {sorted(literals)}, python says {sorted(expected)}"

    def test_the_split_is_bar_weighted(self) -> None:
        assert HOLDOUT_WEIGHTING == SPEC_HOLDOUT_WEIGHTING

    def test_the_window_is_the_spec_window(self) -> None:
        assert (EVALUATION_WINDOW_START, EVALUATION_WINDOW_END) == (SPEC_WINDOW_START, SPEC_WINDOW_END)

    def test_the_corpus_version_names_the_vendor_and_the_frozen_last_bar(self) -> None:
        assert CORPUS_VERSION == f"{SPEC_CORPUS_VENDOR}@{SPEC_WINDOW_END.isoformat()}"

    def test_the_sizing_rule_is_the_declared_one(self) -> None:
        assert SIZING_RULE == SPEC_SIZING_RULE

    def test_the_universe_vocabulary_matches_sql_255(self) -> None:
        assert set(UNIVERSE_BASES) == SPEC_UNIVERSE_BASES

    def test_only_survivorship_free_is_promotable(self) -> None:
        """⚠ An ALLOWLIST with one member (§6). Widening the vocabulary must not
        widen this — a new basis lands on the refused side by construction."""
        assert PROMOTABLE_UNIVERSE_BASES == {"survivorship_free"}
        assert "survivor_only" not in PROMOTABLE_UNIVERSE_BASES


class TestFrozenSplit:
    """§5.2 — and specifically that the boundary date is WITHHELD, not trained on."""

    def test_the_boundary_bar_itself_is_hold_out(self) -> None:
        """⚠ THE 4,021-BAR TEST. The selection rule ("first date whose cumulative
        count exceeds 75%") and the split rule ("that date is the first hold-out
        bar") are two rules, and a single date hides the difference. Measured on
        the corpus: 4,021 bars fall on 2021-06-29. A `>` here instead of `>=`
        moves every one of them into training — 0.02% of the corpus, invisible
        in any summary statistic, and exactly criterion 5's leak."""
        assert namespace_for_bar(HOLDOUT_BOUNDARY) == "hold_out"

    def test_the_bar_before_the_boundary_is_in_sample(self) -> None:
        assert namespace_for_bar(date(2021, 6, 28)) == "in_sample"

    def test_a_bar_after_the_boundary_is_hold_out(self) -> None:
        assert namespace_for_bar(date(2021, 6, 30)) == "hold_out"

    def test_the_window_start_is_in_sample(self) -> None:
        assert namespace_for_bar(EVALUATION_WINDOW_START) == "in_sample"

    def test_the_window_end_is_hold_out(self) -> None:
        assert namespace_for_bar(EVALUATION_WINDOW_END) == "hold_out"


class TestSignalNamespace:
    """§5.2's inclusivity rule, including the purge."""

    def test_a_signal_and_fill_both_in_sample_is_in_sample(self) -> None:
        assert namespace_for_signal(date(2019, 1, 2), date(2019, 1, 3)) == "in_sample"

    def test_a_signal_and_fill_both_hold_out_is_hold_out(self) -> None:
        assert namespace_for_signal(date(2022, 1, 3), date(2022, 1, 4)) == "hold_out"

    def test_a_signal_decided_in_sample_but_filled_on_the_boundary_is_purged(self) -> None:
        """§5.2: "A signal whose signal_bar_date is in-sample but whose
        fill_bar_date is on or after the boundary is PURGED — it is neither,
        because acting on it needs a price from the withheld side."

        ⚠ The FILL is on the boundary itself, which is the tightest case: one
        day earlier and it is in-sample."""
        assert namespace_for_signal(date(2021, 6, 28), HOLDOUT_BOUNDARY) == "purged"

    def test_the_signal_one_day_earlier_on_both_sides_is_not_purged(self) -> None:
        """The discriminator for the case above — without it, a `namespace_for_signal`
        that purged everything near the boundary would pass."""
        assert namespace_for_signal(date(2021, 6, 25), date(2021, 6, 28)) == "in_sample"

    def test_a_fill_preceding_its_signal_is_purged_rather_than_assigned(self) -> None:
        """⚠ Unreachable through the ledger — sql/255's
        `strategy_signals_fill_after_signal` CHECK forbids it — but the function
        must stay total, and the fail-closed answer for a corrupt row is to drop
        it rather than credit either arm with it."""
        assert namespace_for_signal(date(2022, 1, 4), date(2019, 1, 3)) == "purged"


class TestPositionNamespace:
    """§5.2 — "a position that spans the boundary belongs to the hold-out"."""

    def test_a_position_closed_in_sample_is_in_sample(self) -> None:
        assert namespace_for_position(date(2019, 1, 3), date(2019, 2, 1)) == "in_sample"

    def test_a_position_spanning_the_boundary_is_hold_out(self) -> None:
        """Entry in-sample, close hold-out. Splitting its return across
        namespaces would put hold-out prices into an in-sample number."""
        assert namespace_for_position(date(2021, 6, 1), date(2021, 7, 15)) == "hold_out"

    def test_a_position_closed_exactly_on_the_boundary_is_hold_out(self) -> None:
        assert namespace_for_position(date(2021, 6, 1), HOLDOUT_BOUNDARY) == "hold_out"

    def test_an_open_position_entered_in_sample_is_hold_out(self) -> None:
        """⚠ The case a naive reading gets wrong. An open position is marked at
        the last usable close of the EVALUATION WINDOW, which ends 2026-07-08 —
        on the withheld side. "No close" is not "no span"."""
        assert namespace_for_position(date(2019, 1, 3), None) == "hold_out"

    def test_an_open_position_entered_in_hold_out_is_hold_out(self) -> None:
        assert namespace_for_position(date(2022, 1, 3), None) == "hold_out"

    def test_a_close_before_its_entry_raises(self) -> None:
        """⚠ Unreachable through `position_builder` (every close it emits is at
        or after the entry fill bar, and sql/256 bounds `bars_held >= 0`), but
        the function is public and takes two bare dates. `namespace_for_signal`
        already refuses ITS corrupt pair by returning `purged`; the asymmetry was
        the finding. It raises rather than returning because `ResultNamespace`
        has no third member — answering on the close alone would be a verdict
        with no signal attached. Review NITPICK, PR #2360."""
        with pytest.raises(ValueError, match="before its entry fill"):
            namespace_for_position(date(2022, 1, 3), date(2019, 1, 3))

    def test_a_same_bar_open_and_close_is_allowed(self) -> None:
        """⚠ The discriminator for the guard above: `<`, not `<=`. sql/256 says
        `bars_held = 0` IS LEGAL — a TP or SL touched on the fill bar — so a
        position opening and closing on one bar is a real trade, not a reversed
        pair. A `<=` here would reject it."""
        assert namespace_for_position(date(2022, 1, 3), date(2022, 1, 3)) == "hold_out"


class TestResultIdentity:
    """Criterion 11, asserted MEMBER BY MEMBER rather than once in aggregate.

    ⚠ C11 asks for "six assertions, not one" on the strategy hash; the same
    argument applies here. A single "the hash changes when I change something"
    test passes while twelve of thirteen fields are omitted from the payload.
    """

    def test_the_version_carries_the_result_set_id(self) -> None:
        assert _identity().version.startswith(f"{RESULT_SET_ID}+")

    def test_the_version_is_stable_across_calls(self) -> None:
        assert _identity().version == _identity().version

    def test_the_legacy_basis_keeps_the_pre_2429_hash(self) -> None:
        assert _identity(return_basis=LEGACY_RETURN_BASIS).version == "strategy-result-v1+cbea07eb9d2d"

    @pytest.mark.parametrize(
        ("field", "changed"),
        [
            ("strategy_id", "S-3"),
            ("strategy_version", "strategy-registry-v1+zzzzzzzzzzzz"),
            ("result_scope", "portfolio"),
            ("namespace", "in_sample"),
            ("ambiguity_arm", "best_case"),
            ("sizing_rule", "volatility_targeted_v1"),
            ("benchmark_rule", "cap_weighted_spy_v1"),
            ("cost_model_id", "static-p75-insession-v2"),
            ("corpus_version", "some-vendor@2027-01-01"),
            ("window_start", date(1970, 1, 2)),
            ("window_end", date(2026, 7, 7)),
            ("position_rule_set_version", "position-builder-v1+zzzzzzzzzzzz"),
            ("outcome_rule_set_version", "outcome-resolver-v1+zzzzzzzzzzzz"),
            ("input_rule_set_version", "price-quarantine-v1+zzzzzzzzzzzz"),
            ("return_basis", TOTAL_RETURN_BASIS),
            ("ambiguity_rule_version", AMBIGUITY_RULE_VERSION),
        ],
    )
    def test_every_member_moves_the_version(self, field: str, changed: object) -> None:
        assert _identity().version != _identity(**{field: changed}).version

    def test_the_sizing_rule_moves_it(self) -> None:
        """⚠ Named separately as well as parametrised, because it is the one C11
        calls out: "a sizing change that did not move the version would let a
        different strategy inherit a track record"."""
        assert _identity().version != _identity(sizing_rule="fixed_fraction_v1").version

    def test_total_return_results_use_a_new_identity_generation(self) -> None:
        assert _identity(return_basis=TOTAL_RETURN_BASIS).version.startswith(f"{TOTAL_RETURN_RESULT_SET_ID}+")

    def test_the_matched_control_rule_uses_a_new_identity_generation(self) -> None:
        identity = _identity(return_basis=TOTAL_RETURN_BASIS, ambiguity_rule_version=AMBIGUITY_RULE_VERSION)
        assert identity.version.startswith(f"{AMBIGUITY_AWARE_RESULT_SET_ID}+")
        assert (
            identity.version
            != _identity(
                return_basis=TOTAL_RETURN_BASIS,
                ambiguity_rule_version=LEGACY_AMBIGUITY_RULE_VERSION,
            ).version
        )

    def test_a_successor_ambiguity_rule_moves_the_v3_hash(self) -> None:
        current = _identity(return_basis=TOTAL_RETURN_BASIS, ambiguity_rule_version=AMBIGUITY_RULE_VERSION)
        successor = _identity(
            return_basis=TOTAL_RETURN_BASIS,
            ambiguity_rule_version="ambiguity-verdict-2099-v3",
        )
        assert current.version != successor.version

    def test_the_ambiguity_arm_moves_it(self) -> None:
        """§3.4's two arms are two results, not two views of one."""
        assert _identity(ambiguity_arm="worst_case").version != _identity(ambiguity_arm="best_case").version

    def test_current_identity_hashes_the_interior_axis_and_opportunity_population(self) -> None:
        first_axis = (date(2022, 1, 1), date(2023, 1, 3), date(2024, 9, 27))
        second_axis = (date(2022, 1, 1), date(2023, 1, 4), date(2024, 9, 27))

        def current(
            axis: tuple[date, ...],
            *,
            opportunity: str = "a" * 64,
            window_id: str = "primary-2022-plus",
        ) -> ResultIdentity:
            return _identity(
                return_basis=TOTAL_RETURN_BASIS,
                ambiguity_rule_version=AMBIGUITY_RULE_VERSION,
                window_start=date(2022, 1, 1),
                window_end=date(2024, 9, 27),
                metric_axis_rule_version=METRIC_AXIS_RULE_VERSION,
                metric_axis_dates=axis,
                metric_axis_start=axis[0],
                metric_axis_end=axis[-1],
                metric_axis_digest=metric_axis_sha256(axis),
                opportunity_set_digest=opportunity,
                evidence_window_id=window_id,
            )

        baseline = current(first_axis)
        assert baseline.version.startswith("strategy-result-v4+")
        assert baseline.version != current(second_axis)
        assert baseline.version != current(first_axis, opportunity="b" * 64)
        assert baseline.version != current(first_axis, window_id="successor-window")

        with pytest.raises(ValueError, match="opportunity-set digest"):
            current(first_axis, opportunity="not-a-digest")
        with pytest.raises(ValueError, match="non-blank evidence-window ID"):
            current(first_axis, window_id="   ")

    def test_current_axis_must_stay_inside_its_declared_window(self) -> None:
        axis = (date(2019, 12, 31), date(2020, 1, 2))
        with pytest.raises(ValueError, match="contained in the declared evaluation window"):
            _identity(
                namespace="in_sample",
                window_start=date(2020, 1, 1),
                window_end=date(2020, 1, 3),
                metric_axis_rule_version=METRIC_AXIS_RULE_VERSION,
                metric_axis_dates=axis,
                metric_axis_start=axis[0],
                metric_axis_end=axis[-1],
                metric_axis_digest=metric_axis_sha256(axis),
                opportunity_set_digest="a" * 64,
            )

    def test_in_sample_axis_must_stop_before_the_holdout_boundary(self) -> None:
        axis = (date(2021, 6, 28), HOLDOUT_BOUNDARY)
        with pytest.raises(ValueError, match="cannot reach the frozen hold-out boundary"):
            _identity(
                namespace="in_sample",
                metric_axis_rule_version=METRIC_AXIS_RULE_VERSION,
                metric_axis_dates=axis,
                metric_axis_start=axis[0],
                metric_axis_end=axis[-1],
                metric_axis_digest=metric_axis_sha256(axis),
                opportunity_set_digest="a" * 64,
            )


class TestStrategyResultValidation:
    """The writer-side shape, which RAISES — unlike the gate."""

    def test_a_well_formed_row_constructs(self) -> None:
        assert _result().identity.strategy_id == "S-1"

    def test_purged_is_not_a_result_namespace(self) -> None:
        """§5.2: a purged signal contributes to NO result. Accepting it as a
        namespace would give it one."""
        with pytest.raises(ValueError, match="purged"):
            _result(identity=_identity(namespace="purged"))

    def test_an_unknown_scope_is_refused(self) -> None:
        with pytest.raises(ValueError, match="result scope"):
            _result(identity=_identity(result_scope="signal"))

    def test_an_unknown_ambiguity_arm_is_refused(self) -> None:
        with pytest.raises(ValueError, match="ambiguity arm"):
            _result(identity=_identity(ambiguity_arm="conservative"))

    def test_an_unknown_return_basis_is_refused(self) -> None:
        with pytest.raises(ValueError, match="unknown return basis"):
            _result(identity=_identity(return_basis="adjusted-sometimes"))

    @pytest.mark.parametrize(
        "field",
        [
            "strategy_id",
            "strategy_version",
            "sizing_rule",
            "benchmark_rule",
            "return_basis",
            "ambiguity_rule_version",
            "cost_model_id",
            "corpus_version",
            "position_rule_set_version",
            "outcome_rule_set_version",
            "input_rule_set_version",
        ],
    )
    def test_a_blank_identity_field_is_refused(self, field: str) -> None:
        """⚠ The #2286 shape: a present-but-empty value is not caught by NOT
        NULL, and every field here is identity — an empty one silently merges
        two results into one bucket."""
        with pytest.raises(ValueError, match="blank"):
            _result(identity=_identity(**{field: ""}))

    def test_a_backwards_window_is_refused(self) -> None:
        with pytest.raises(ValueError, match="ends before it starts"):
            _result(identity=_identity(window_start=date(2026, 1, 1), window_end=date(2025, 1, 1)))

    def test_a_zero_trial_count_is_refused(self) -> None:
        """Criterion 6 counts abandoned branches and discarded parameter values,
        so zero trials is not a state that can be reached — it is a writer that
        meant NULL."""
        with pytest.raises(ValueError, match="trial_count"):
            _result(trial_count=0)

    def test_an_absent_trial_count_constructs(self) -> None:
        """NULL is a real state — 5d writes results before 5e computes the DSR.
        The GATE refuses on it; the shape does not."""
        assert _result(trial_count=None).trial_count is None


class TestTheDeflatedSharpeBindsItsScalars:
    """Stage 5e-3 — ``sql/266``'s all-or-nothing, checked where the field is named.

    ⚠ The binding is ONE WAY. A ``deflated`` object forces the two scalars to
    agree with it; a declared trial count with no DSR stays legal, because the
    gate has a live refusal describing exactly that row.
    """

    def _deflated(self, **overrides: object) -> DeflatedSharpeResult:
        base: dict[str, object] = {
            "deflated_sharpe": 0.72,
            "expected_max_sharpe": 0.015,
            "trade_sharpe": 0.017,
            "skewness": -0.4,
            "kurtosis": 8.0,
            "effective_sample_size": 104291.8,
            "declared_trials": 11,
            "independent_trials": 9.0,
            "average_trial_correlation": 0.2,
            "trial_sharpe_variance": 1e-4,
            "measured_trials": 2,
            "trial_register_version": "trial-register-2026-08-07",
        }
        base.update(overrides)
        return DeflatedSharpeResult(**base)  # type: ignore[arg-type]

    def _with(self, **overrides: object):  # noqa: ANN202 - local helper
        deflated = self._deflated()
        kwargs: dict[str, object] = {
            "deflated": deflated,
            "trial_count": deflated.declared_trials,
            "deflated_sharpe": Decimal(repr(deflated.deflated_sharpe)),
            "metrics": _metrics(effective_sample_size=deflated.effective_sample_size),
        }
        kwargs.update(overrides)
        return _result(**kwargs)

    def test_a_consistent_set_constructs(self) -> None:
        assert self._with().deflated is not None

    def test_a_disagreeing_trial_count_is_refused(self) -> None:
        with pytest.raises(ValueError, match="disagrees with the 11 trials"):
            self._with(trial_count=41)

    def test_a_missing_trial_count_beside_a_dsr_is_refused(self) -> None:
        with pytest.raises(ValueError, match="disagrees with the 11 trials"):
            self._with(trial_count=None)

    def test_a_disagreeing_deflated_sharpe_is_refused(self) -> None:
        with pytest.raises(ValueError, match="two copies of one number"):
            self._with(deflated_sharpe=Decimal("0.31"))

    def test_a_sample_size_the_metric_set_does_not_carry_is_refused(self) -> None:
        """⚠⚠ ONE SAMPLE SIZE, ONE COLUMN.

        ``sql/266`` gives the DSR no ``effective_sample_size`` of its own — it
        consumes criterion 3's, and the ledger rebuilds the field FROM that
        column. Without this, a row could be deflated against one sample size
        and stored declaring another, and the round trip would silently swap
        the first for the second.
        """
        with pytest.raises(ValueError, match="criterion 6 consumes criterion 3's number"):
            self._with(metrics=_metrics_without_bootstrap())

    def test_a_declared_count_without_a_dsr_stays_legal(self) -> None:
        """The converse is NOT bound — and the gate is what reports it."""
        result = _result(trial_count=11, deflated_sharpe=None, deflated=None)
        assert result.deflated is None
        assert result.trial_count == 11


class TestPromotionGateClears:
    def test_a_fully_clean_candidate_is_promotable(self) -> None:
        assert check_promotable(_clean_candidate()) == ()
        assert is_promotable(_clean_candidate()) is True


class TestPromotionGateRefusals:
    def test_a_legacy_result_has_unproven_metric_axis(self) -> None:
        candidate = _clean_candidate(result=_result(identity=_identity(), **_CLEAN_RESULT_FIELDS))
        assert "metric_axis_unproven" in check_promotable(candidate)

    def test_metrics_that_do_not_reconcile_with_the_axis_are_unproven(self) -> None:
        result = _result(
            **_CLEAN_RESULT_FIELDS,
            metrics=_metrics(periods_per_year=_CURRENT_TEST_PPY + 1.0),
            synthetic_control=_passing_control(),
        )
        assert "metric_axis_unproven" in check_promotable(_clean_candidate(result=result))

    @pytest.mark.parametrize("window_id", ["invented", "year-2023"])
    def test_holdout_axis_requires_the_exact_registered_evidence_window(self, window_id: str) -> None:
        axis = (date(2022, 1, 3), date(2024, 9, 27))
        ppy = periods_per_year(axis)
        metrics = _metrics(
            periods_per_year=ppy,
            cagr_pct=(1.21 ** (1.0 / ((len(axis) - 1) / ppy)) - 1.0) * 100.0,
        )
        identity = _identity(
            namespace="hold_out",
            window_start=date(2022, 1, 1),
            window_end=date(2024, 9, 27),
            return_basis=TOTAL_RETURN_BASIS,
            ambiguity_rule_version=AMBIGUITY_RULE_VERSION,
            metric_axis_rule_version=METRIC_AXIS_RULE_VERSION,
            metric_axis_dates=axis,
            metric_axis_start=axis[0],
            metric_axis_end=axis[-1],
            metric_axis_digest=metric_axis_sha256(axis),
            opportunity_set_digest="a" * 64,
            evidence_window_id=window_id,
        )
        result = _result(
            identity=identity,
            metrics=metrics,
            **_CLEAN_RESULT_FIELDS,
            synthetic_control=_passing_control(),
        )

        assert "metric_axis_unproven" in check_promotable(_clean_candidate(result=result))

    def test_a_harness_control_is_permanently_refused(self) -> None:
        assert (
            check_promotable(
                _clean_candidate(
                    result=_result(
                        **_CLEAN_RESULT_FIELDS,
                        purpose="harness_validation",
                        synthetic_control=_passing_control(),
                    )
                )
            )[0]
            == "harness_validation_only"
        )

    """One test per refusal, each breaking exactly one thing off the clean candidate."""

    def test_an_absent_basis_is_refused_distinctly_from_survivor_only(self) -> None:
        """⚠ Two codes for one verdict, because they are two operator actions:
        absent means the WRITER is broken, survivor_only means the CORPUS is."""
        candidate = _clean_candidate(
            result=_result(universe_basis="", carry_unmodelled=False, trial_count=1, deflated_sharpe=Decimal("1"))
        )
        assert "universe_basis_absent" in check_promotable(candidate)

    def test_survivor_only_is_refused(self) -> None:
        candidate = _clean_candidate(
            result=_result(
                universe_basis="survivor_only", carry_unmodelled=False, trial_count=1, deflated_sharpe=Decimal("1")
            )
        )
        assert "universe_basis_not_survivorship_free" in check_promotable(candidate)

    def test_an_unrecognised_basis_is_refused_not_raised(self) -> None:
        """⚠⚠ FAIL CLOSED IS AN ALLOWLIST. A typo, a future label and
        survivor_only are refused identically — and the gate RETURNS the refusal
        rather than raising, because phase 7's guard needs a reason string to
        write to decision_audit, not a traceback."""
        candidate = _clean_candidate(
            result=_result(
                universe_basis="survivorship_freee", carry_unmodelled=False, trial_count=1, deflated_sharpe=Decimal("1")
            )
        )
        assert "universe_basis_not_survivorship_free" in check_promotable(candidate)

    def test_carry_unmodelled_is_refused(self) -> None:
        candidate = _clean_candidate(
            result=_result(
                universe_basis="survivorship_free", carry_unmodelled=True, trial_count=1, deflated_sharpe=Decimal("1")
            )
        )
        assert "carry_unmodelled" in check_promotable(candidate)

    def test_an_instrument_outside_the_validated_universe_is_refused(self) -> None:
        candidate = _clean_candidate(evaluated_instrument_ids=frozenset({1, 2, 99}))
        assert "instrument_outside_validated_universe" in check_promotable(candidate)

    def test_an_empty_evaluated_set_is_refused_rather_than_passing_vacuously(self) -> None:
        """⚠ `set() - anything` is empty, so a result over NO instruments would
        sail through the subset test while being no evidence at all."""
        refusals = check_promotable(_clean_candidate(evaluated_instrument_ids=frozenset()))
        assert "no_instruments_evaluated" in refusals
        assert "instrument_outside_validated_universe" not in refusals

    def test_an_unlinked_only_opportunity_set_is_not_vacuously_empty(self) -> None:
        refusals = check_promotable(
            _clean_candidate(
                evaluated_instrument_ids=frozenset(),
                evaluated_series_ids=frozenset({101}),
            )
        )
        assert "no_instruments_evaluated" not in refusals

    def test_a_never_evaluated_holdout_is_refused(self) -> None:
        refusals = check_promotable(_clean_candidate(holdout_evaluations=0, recorded_accesses=0))
        assert "holdout_never_evaluated" in refusals

    def test_an_evaluation_without_a_recorded_access_is_refused(self) -> None:
        """⚠ STRICTER THAN CRITERION 5'S LITERAL WORDING, deliberately. "Evaluated
        more than once without a recorded access" read literally lets a SINGLE
        unrecorded look pass — which is the same governance failure, just the
        first one. Every evaluation must have a record."""
        assert "holdout_accesses_unrecorded" in check_promotable(
            _clean_candidate(holdout_evaluations=1, recorded_accesses=0)
        )

    def test_more_evaluations_than_accesses_is_refused(self) -> None:
        assert "holdout_accesses_unrecorded" in check_promotable(
            _clean_candidate(holdout_evaluations=3, recorded_accesses=2)
        )

    def test_a_missing_deflated_sharpe_is_refused(self) -> None:
        candidate = _clean_candidate(
            result=_result(
                universe_basis="survivorship_free", carry_unmodelled=False, trial_count=9, deflated_sharpe=None
            )
        )
        assert "deflated_sharpe_not_computed" in check_promotable(candidate)

    def test_an_undeclared_trial_count_is_refused_even_with_a_deflated_sharpe(self) -> None:
        """Criterion 6: the count is what the deflation divides by, so a DSR
        computed without one is not a DSR. ⚠ It "does not default to the number
        of shipped strategies"."""
        candidate = _clean_candidate(
            result=_result(
                universe_basis="survivorship_free",
                carry_unmodelled=False,
                trial_count=None,
                deflated_sharpe=Decimal("2.5"),
            )
        )
        refusals = check_promotable(candidate)
        assert "trial_count_undeclared" in refusals
        assert "deflated_sharpe_not_computed" not in refusals

    def test_a_dsr_against_a_superseded_trial_register_is_refused(self) -> None:
        deflated = _deflated_result(trial_register_version="trial-register-superseded")
        result = _result(
            **{
                **_CLEAN_RESULT_FIELDS,
                "trial_count": deflated.declared_trials,
                "deflated_sharpe": Decimal(repr(deflated.deflated_sharpe)),
                "deflated": deflated,
            },
            metrics=_metrics(effective_sample_size=deflated.effective_sample_size),
            synthetic_control=_passing_control(),
        )
        assert "trial_register_superseded" in check_promotable(_clean_candidate(result=result))

    def test_a_dsr_without_register_provenance_is_refused(self) -> None:
        result = _result(
            universe_basis="survivorship_free",
            carry_unmodelled=False,
            trial_count=11,
            deflated_sharpe=Decimal("0.72"),
            deflated=None,
            synthetic_control=_passing_control(),
        )
        assert "trial_register_superseded" in check_promotable(_clean_candidate(result=result))

    def test_a_dsr_with_the_current_version_but_a_stale_count_is_refused(self) -> None:
        deflated = _deflated_result(declared_trials=12, independent_trials=9.0)
        result = _result(
            **{
                **_CLEAN_RESULT_FIELDS,
                "trial_count": deflated.declared_trials,
                "deflated_sharpe": Decimal(repr(deflated.deflated_sharpe)),
                "deflated": deflated,
            },
            metrics=_metrics(effective_sample_size=deflated.effective_sample_size),
            synthetic_control=_passing_control(),
        )
        assert "trial_register_superseded" in check_promotable(_clean_candidate(result=result))

    def test_the_current_trial_register_does_not_add_a_refusal(self) -> None:
        deflated = _deflated_result(trial_register_version=TRIAL_REGISTER_VERSION)
        result = _result(
            **{
                **_CLEAN_RESULT_FIELDS,
                "trial_count": deflated.declared_trials,
                "deflated_sharpe": Decimal(repr(deflated.deflated_sharpe)),
                "deflated": deflated,
            },
            metrics=_metrics(effective_sample_size=deflated.effective_sample_size),
            synthetic_control=_passing_control(),
        )
        assert "trial_register_superseded" not in check_promotable(_clean_candidate(result=result))

    def test_an_uncompared_ambiguity_pair_is_refused(self) -> None:
        """§3.4. ⚠ "Not measured" and "measured and bad" are different states and
        get different codes — collapsing them is how a phase ships that cannot
        demonstrate it works."""
        refusals = check_promotable(_clean_candidate(ambiguity_material=None))
        assert "ambiguity_arms_not_compared" in refusals
        assert "ambiguity_material" not in refusals

    def test_a_material_ambiguity_gap_is_refused(self) -> None:
        refusals = check_promotable(_clean_candidate(ambiguity_material=True))
        assert "ambiguity_material" in refusals
        assert "ambiguity_arms_not_compared" not in refusals

    def test_an_unrun_quarantine_sensitivity_arm_is_refused(self) -> None:
        """Criterion 9 — *"so exclusion is visible rather than assumed
        harmless"*. The default is ``False`` and therefore refused: a candidate
        assembled by a caller that has never heard of the arm must not clear."""
        assert "quarantine_arms_not_compared" in check_promotable(_clean_candidate(quarantine_arms_compared=False))

    def test_the_quarantine_gate_has_no_materiality_twin(self) -> None:
        """⚠⚠ THE ASYMMETRY WITH ``ambiguity_material`` IS THE ASSERTION.

        §3.4 declares a materiality rule for the ambiguity arms; criterion 9
        declares none, and no published rule fixes a "delta this large blocks
        promotion" cut. A `quarantine_material` code appearing here would be an
        invented threshold wearing a criterion's name, so its ABSENCE from the
        closed vocabulary is pinned rather than left to review."""
        assert "quarantine_arms_not_compared" in PROMOTION_REFUSALS
        assert "quarantine_material" not in PROMOTION_REFUSALS

    def test_a_result_with_no_synthetic_control_is_refused(self) -> None:
        """§9's control is the null distribution the Sharpe is read against, and
        a result with none is a number with no scale. ⚠ NULL is the fail-closed
        default, the same posture as the DSR and the effective sample size."""
        candidate = _clean_candidate(result=_result(**_CLEAN_RESULT_FIELDS, synthetic_control=None))
        assert "synthetic_control_not_run" in check_promotable(candidate)

    def test_a_legacy_control_without_match_evidence_is_refused(self) -> None:
        control = replace(_passing_control(), match_quality=None)
        candidate = _clean_candidate(result=_result(**_CLEAN_RESULT_FIELDS, synthetic_control=control))
        assert set(check_promotable(candidate)) == {"synthetic_control_match_evidence_missing"}

    def test_an_unknown_match_policy_is_refused(self) -> None:
        control = _passing_control()
        assert control.match_quality is not None
        control = replace(
            control,
            match_quality=replace(control.match_quality, policy_id="synthetic-control-favourable-tolerance-v99"),
        )
        candidate = _clean_candidate(result=_result(**_CLEAN_RESULT_FIELDS, synthetic_control=control))
        assert set(check_promotable(candidate)) == {"synthetic_control_match_policy_unrecognised"}

    def test_every_match_dimension_is_checked_independently(self) -> None:
        """No favourable cohort can hide a population, exposure, or turnover
        mismatch behind the two outcome thresholds."""
        control = _passing_control()
        assert control.match_quality is not None
        control = replace(
            control,
            match_quality=replace(
                control.match_quality,
                cohort_mean_trade_count=99.0,
                unmatchable_by_reason={"open_at_window_end": 1},
                no_slack_series=1,
                cohort_mean_exposure_time_pct=60.0,
                cohort_mean_turnover_annualised=2.4,
            ),
        )
        candidate = _clean_candidate(result=_result(**_CLEAN_RESULT_FIELDS, synthetic_control=control))
        assert set(check_promotable(candidate)) == {
            "synthetic_control_population_mismatch",
            "synthetic_control_exposure_mismatch",
            "synthetic_control_turnover_mismatch",
        }

    def test_a_cohort_whose_mean_return_excludes_zero_blocks_the_result(self) -> None:
        """§9's FIRST threshold, and it is a verdict on the HARNESS rather than
        on the strategy — *"a harness that finds edge in noise is broken
        regardless of what else it explains"*. ⚠ The strategy here still clears
        the Sharpe threshold, so exactly one code fires and it is the cohort's."""
        control = _passing_control(mean_return_ci_low_pct=0.4, mean_return_ci_high_pct=0.9)
        candidate = _clean_candidate(result=_result(**_CLEAN_RESULT_FIELDS, synthetic_control=control))
        refusals = set(check_promotable(candidate))
        assert refusals == {"synthetic_control_cohort_shows_edge"}

    def test_a_sharpe_at_the_cohort_threshold_does_not_exceed_it(self) -> None:
        """§9 says "must EXCEED", so equality is refused. ⚠ A `>=` reading would
        admit a strategy indistinguishable from the 950th random member."""
        control = _passing_control(cohort_sharpe_threshold=0.33)
        candidate = _clean_candidate(result=_result(**_CLEAN_RESULT_FIELDS, synthetic_control=control))
        assert set(check_promotable(candidate)) == {"synthetic_control_sharpe_below_cohort"}

    def test_missing_edge_attribution_evidence_refuses_an_otherwise_clean_candidate(self) -> None:
        assert check_promotable(_clean_candidate(promotion_evidence=None)) == ("promotion_evidence_missing",)

    def test_edge_attribution_failures_are_returned_by_the_shared_gate(self) -> None:
        evidence = _passing_promotion_evidence(after_cost_expectancy_ci_low_pct=Decimal("0"))
        assert check_promotable(_clean_candidate(promotion_evidence=evidence)) == (
            "expectancy_lower_bound_not_positive",
        )

    def test_both_synthetic_failures_are_reported_together(self) -> None:
        """⚠ NOT the first one. An operator seeing only the strategy-level code
        would tune the strategy against a cohort that invalidates every result
        measured under it."""
        control = _passing_control(
            mean_return_ci_low_pct=0.4,
            mean_return_ci_high_pct=0.9,
            cohort_sharpe_threshold=0.99,
        )
        candidate = _clean_candidate(result=_result(**_CLEAN_RESULT_FIELDS, synthetic_control=control))
        assert set(check_promotable(candidate)) == {
            "synthetic_control_cohort_shows_edge",
            "synthetic_control_sharpe_below_cohort",
        }

    def test_the_reported_return_percentile_does_not_gate(self) -> None:
        """⚠ The permutation-test statistic is REPORTED and never blocks. §9's
        acceptance names two thresholds; adding a third in code would be this
        module inventing an acceptance criterion."""
        control = _passing_control(cohort_return_threshold_pct=10_000.0)
        assert control.return_exceeds_cohort is False
        candidate = _clean_candidate(result=_result(**_CLEAN_RESULT_FIELDS, synthetic_control=control))
        assert check_promotable(candidate) == ()

    def test_a_missing_effective_sample_size_is_refused_independently_of_the_dsr(self) -> None:
        """Criterion 3, and it is SEPARATE from ``deflated_sharpe_not_computed``
        on purpose. ⚠ Criterion 6's deflation CONSUMES the effective sample size,
        so a DSR present with the sample size missing is a DSR deflated on a
        nominal n — the number criterion 3 forbids reporting anywhere. Collapsing
        the two codes would make that exact state unreportable."""
        candidate = _clean_candidate(
            result=_result(
                metrics=_metrics_without_bootstrap(),
                universe_basis="survivorship_free",
                carry_unmodelled=False,
                trial_count=9,
                deflated_sharpe=Decimal("1.1"),
            )
        )
        refusals = check_promotable(candidate)
        assert "effective_sample_size_not_computed" in refusals
        assert "deflated_sharpe_not_computed" not in refusals

    def test_a_result_computed_with_no_declared_seed_is_refused_on_the_sample_size(self) -> None:
        """⚠ THE FAIL-CLOSED STATE, asserted rather than described.

        ⚠⚠ This test's PREMISE changed at stage 5e-2 and the change is the point.
        Before it, ``compute_metrics`` returned ``effective_sample_size=None``
        unconditionally and this asserted that no result the stage could produce
        was promotable. The bootstrap now exists, so the null is no longer
        unconditional — it is what a caller gets when it declares no
        ``bootstrap_seed``. The refusal must still fire in exactly that case,
        which is §6's *"the gate's initial state is 'nothing is promotable'"*
        surviving the arrival of the thing that can clear it."""
        assert "effective_sample_size_not_computed" in check_promotable(
            _clean_candidate(result=_result(metrics=_metrics_without_bootstrap()))
        )


class TestBoundaryDerivation:
    """The VERIFY SCRIPT's re-derivation, which is the frozen literal's only check.

    ⚠⚠ ``_derive_boundary`` is the independent reference that
    ``scripts/verify_2240_result_model.py --frozen`` compares ``HOLDOUT_BOUNDARY``
    against. An unverified reference is the prevention log's *"independent
    verifier that is only ACCIDENTALLY right"* — if it conflates the selection
    rule with the split rule, it agrees with a module that does the same and the
    arm reports PASS on a 4,021-bar leak.

    ⚠ It stays in ``scripts/`` rather than moving into ``strategy_result``: a
    literal validated by code living beside it is the #2240 S-3 tautology.
    """

    def test_the_boundary_date_s_own_bars_are_hold_out(self) -> None:
        """⚠ THE DISCRIMINATOR between the two rules. Counts [70, 10, 10, 10]
        cross 75% on the second date at a running total of 80. The boundary is
        that date, and in-sample is 70 — NOT 80. Returning the running total
        puts the boundary date's own bars in training, which is precisely the
        conflation the real corpus expresses as 4,021 bars."""
        from scripts.verify_2240_result_model import _derive_boundary

        dates = [(date(2000, 1, d), n) for d, n in ((1, 70), (2, 10), (3, 10), (4, 10))]
        assert _derive_boundary(dates) == (date(2000, 1, 2), 70, 30)

    def test_the_selection_rule_needs_a_STRICT_exceedance(self) -> None:
        """Counts [75, 25] land the cumulative exactly ON 75%. `>` puts the
        boundary at the second date and gives a clean 75/25; `>=` would put it
        at the FIRST, leaving in-sample empty and withholding everything."""
        from scripts.verify_2240_result_model import _derive_boundary

        dates = [(date(2000, 1, 1), 75), (date(2000, 1, 2), 25)]
        assert _derive_boundary(dates) == (date(2000, 1, 2), 75, 25)

    def test_an_empty_slice_raises_rather_than_inventing_a_boundary(self) -> None:
        from scripts.verify_2240_result_model import _derive_boundary

        with pytest.raises(RuntimeError, match="slice is empty"):
            _derive_boundary([])


class TestPromotionGateReportsEverything:
    def test_every_refusal_is_returned_not_just_the_first(self) -> None:
        """⚠⚠ Short-circuiting makes fixing the gate a discover-one-at-a-time
        loop and hides HOW FAR a result is from promotable. This asserts the
        exact SET, so a check silently dropped from the function fails here."""
        candidate = PromotionCandidate(
            result=_result(universe_basis="", carry_unmodelled=True, trial_count=None, deflated_sharpe=None),
            evaluated_instrument_ids=frozenset(),
            validated_universe_ids=frozenset(),
            holdout_evaluations=0,
            recorded_accesses=0,
            ambiguity_material=None,
        )
        assert set(check_promotable(candidate)) == {
            "universe_basis_absent",
            "carry_unmodelled",
            "fx_unmodelled",
            "no_instruments_evaluated",
            "holdout_never_evaluated",
            "deflated_sharpe_not_computed",
            "trial_count_undeclared",
            "ambiguity_arms_not_compared",
            "quarantine_arms_not_compared",
            "synthetic_control_not_run",
            "promotion_evidence_missing",
        }

    def test_todays_real_pipeline_state_is_refused(self) -> None:
        """§6's stated initial state — "nothing is promotable. That is correct,
        not a bug to work around." This is the shape 5d will actually produce:
        a survivor-only corpus, carry unmodelled, and no hold-out machinery
        yet."""
        candidate = PromotionCandidate(
            result=_result(universe_basis="survivor_only", carry_unmodelled=True),
            evaluated_instrument_ids=frozenset({1, 2, 3}),
            validated_universe_ids=frozenset({1, 2, 3}),
        )
        assert set(check_promotable(candidate)) == {
            "universe_basis_not_survivorship_free",
            "carry_unmodelled",
            "fx_unmodelled",
            "holdout_never_evaluated",
            "deflated_sharpe_not_computed",
            "trial_count_undeclared",
            "ambiguity_arms_not_compared",
            "quarantine_arms_not_compared",
            "synthetic_control_not_run",
            "promotion_evidence_missing",
        }
        assert is_promotable(candidate) is False

    def test_every_returned_code_is_in_the_closed_vocabulary(self) -> None:
        """Criterion 9 counts refusals, and a code outside the vocabulary cannot
        be counted. Same guard sql/255 puts on `not_evaluable_reason`."""
        candidate = PromotionCandidate(
            result=_result(universe_basis="", carry_unmodelled=True),
            evaluated_instrument_ids=frozenset({7}),
            validated_universe_ids=frozenset(),
        )
        assert set(check_promotable(candidate)) <= PROMOTION_REFUSALS

    def test_the_gate_is_pure_and_leaves_the_candidate_untouched(self) -> None:
        candidate = _clean_candidate()
        before = replace(candidate)
        check_promotable(candidate)
        assert candidate == before


# ---------------------------------------------------------------------------
# #2639 — the shared clause helpers
# ---------------------------------------------------------------------------


def _contains_in_order(whole: tuple[str, ...], part: tuple[str, ...]) -> bool:
    """``part`` appears in ``whole`` as a CONTIGUOUS run, in the same order."""
    if not part:
        return True
    return any(whole[i : i + len(part)] == part for i in range(len(whole) - len(part) + 1))


#: Which codes each extracted helper OWNS. ⚠ Written out rather than derived
#: from the helper's own output: a derivation would agree with whatever the
#: helper does, including emitting nothing, and the equivalence test below needs
#: an independent statement of what the gate is expected to delegate.
_HELPER_CODES: dict[str, frozenset[str]] = {
    "purpose": frozenset({"harness_validation_only"}),
    "holdout": frozenset({"holdout_never_evaluated", "holdout_accesses_unrecorded"}),
    "deflation": frozenset(
        {
            "deflated_sharpe_not_computed",
            "trial_count_undeclared",
            "trial_register_superseded",
            "effective_sample_size_not_computed",
        }
    ),
    "synthetic": frozenset(
        {
            "synthetic_control_not_run",
            "synthetic_control_match_evidence_missing",
            "synthetic_control_match_policy_unrecognised",
            "synthetic_control_population_mismatch",
            "synthetic_control_exposure_mismatch",
            "synthetic_control_turnover_mismatch",
            "synthetic_control_cohort_shows_edge",
            "synthetic_control_sharpe_below_cohort",
        }
    ),
}


def _helper_outputs(candidate: PromotionCandidate) -> dict[str, tuple[str, ...]]:
    result = candidate.result
    return {
        "purpose": purpose_promotion_refusals(result.purpose),
        "holdout": holdout_count_promotion_refusals(
            holdout_evaluations=candidate.holdout_evaluations,
            recorded_accesses=candidate.recorded_accesses,
        ),
        "deflation": deflation_promotion_refusals(
            deflated_sharpe=result.deflated_sharpe,
            trial_count=result.trial_count,
            deflated=result.deflated,
            effective_sample_size=result.metrics.effective_sample_size,
        ),
        "synthetic": synthetic_control_promotion_refusals(result.synthetic_control),
    }


def _equivalence_candidates() -> list[PromotionCandidate]:
    """One candidate per state the extracted clauses can reach, plus the extremes."""
    superseded = _deflated_result(trial_register_version="trial-register-v0-superseded")
    # ⚠ Only the COHORT side may be varied here. ``StrategyResult`` binds the
    # control's strategy_sharpe / strategy_return_pct to ``metrics``, so moving
    # them raises at construction instead of producing the refusal under test.
    edge_control = _passing_control(mean_return_ci_low_pct=0.5, mean_return_ci_high_pct=1.5)
    weak_control = _passing_control(cohort_sharpe_threshold=9.0)
    return [
        _clean_candidate(),
        _clean_candidate(result=_result(**_CLEAN_RESULT_FIELDS, synthetic_control=None)),
        _clean_candidate(result=_result(**_CLEAN_RESULT_FIELDS, synthetic_control=edge_control)),
        _clean_candidate(result=_result(**_CLEAN_RESULT_FIELDS, synthetic_control=weak_control)),
        _clean_candidate(holdout_evaluations=0, recorded_accesses=0),
        _clean_candidate(holdout_evaluations=3, recorded_accesses=2),
        _clean_candidate(
            result=_result(
                **{**_CLEAN_RESULT_FIELDS, "deflated": superseded},
                synthetic_control=_passing_control(),
            )
        ),
        _clean_candidate(
            result=_result(
                **{
                    **_CLEAN_RESULT_FIELDS,
                    "purpose": "harness_validation",
                    "deflated_sharpe": None,
                    "trial_count": None,
                    "deflated": None,
                    # ⚠ The whole bootstrap set, not the ESS alone —
                    # `StrategyMetrics` refuses a partial one.
                    "metrics": _metrics_without_bootstrap(),
                },
                synthetic_control=None,
            )
        ),
        # The everything-broken shape: no clause is masked by another.
        PromotionCandidate(
            result=_result(purpose="harness_validation", universe_basis="", carry_unmodelled=True),
            evaluated_instrument_ids=frozenset(),
            validated_universe_ids=frozenset(),
        ),
    ]


class TestTheExtractedClauseHelpersAreTheGate:
    """⚠⚠ #2639 EXTRACTED FOUR CLAUSE BLOCKS SO THE PROMOTION TRANSITION CAN
    REPLAY THEM OFF A STORED ROW — and an extraction is only safe while it is
    provably the same rule.

    ``promote_strategy`` holds a ``result_id``, not a ``StrategyResult``, so it
    cannot call ``check_promotable``; it calls these helpers instead. A second
    hand-written copy is exactly what ``structural_promotion_refusals`` was
    extracted to prevent, and the failure would be silent in the worst
    direction — the transition passing a result the gate refuses.

    Two properties, and both are needed. Membership alone would tolerate the
    blocks being reordered (the spec's order is what makes a missing check
    visible as a missing block); order alone would tolerate the gate dropping a
    code the helper still emits.
    """

    @pytest.mark.parametrize("candidate", _equivalence_candidates())
    def test_each_helper_emits_exactly_the_codes_the_gate_emits(self, candidate: PromotionCandidate) -> None:
        gate = set(check_promotable(candidate))
        for name, output in _helper_outputs(candidate).items():
            assert set(output) == gate & _HELPER_CODES[name], (
                f"{name}: the helper and check_promotable disagree about this candidate — the transition "
                "would reach a different verdict from the gate"
            )

    @pytest.mark.parametrize("candidate", _equivalence_candidates())
    def test_each_helper_s_output_is_a_contiguous_run_of_the_gate_s(self, candidate: PromotionCandidate) -> None:
        gate = check_promotable(candidate)
        for name, output in _helper_outputs(candidate).items():
            assert _contains_in_order(gate, output), (
                f"{name}: {output} is not a contiguous in-order run of {gate} — the extraction has "
                "reordered the spec's blocks"
            )

    def test_no_code_is_owned_by_two_helpers(self) -> None:
        seen: set[str] = set()
        for codes in _HELPER_CODES.values():
            assert not (seen & codes)
            seen |= codes

    def test_every_owned_code_is_in_the_closed_vocabulary(self) -> None:
        for codes in _HELPER_CODES.values():
            assert codes <= PROMOTION_REFUSALS

    def test_the_deflation_clauses_are_independent_and_all_fire_together(self) -> None:
        """⚠ FOUR ``if``s, never an ``elif``. A DSR with no trial count is as
        refused as no DSR at all, and an ``elif`` chain would report one reason
        where four apply — which is the "how far is this from promotable"
        number an operator actually reads."""
        assert set(
            deflation_promotion_refusals(
                deflated_sharpe=0.9,
                trial_count=None,
                deflated=None,
                effective_sample_size=None,
            )
        ) == {"trial_count_undeclared", "trial_register_superseded", "effective_sample_size_not_computed"}

    def test_trial_register_supersession_is_guarded_on_the_probability_not_the_object(self) -> None:
        """⚠ A row with a stored ``deflated_sharpe`` and no reconstructed object
        is exactly the state the clause is for. Guarding on ``deflated`` instead
        would let it through."""
        assert "trial_register_superseded" in deflation_promotion_refusals(
            deflated_sharpe=0.9, trial_count=11, deflated=None, effective_sample_size=1000.0
        )
        assert "trial_register_superseded" not in deflation_promotion_refusals(
            deflated_sharpe=None, trial_count=11, deflated=None, effective_sample_size=1000.0
        )

    def test_a_decimal_deflated_sharpe_is_accepted_as_a_presence(self) -> None:
        """⚠ Off a stored row the probability arrives as psycopg's NUMERIC →
        ``Decimal``, not a float. The clause is a ``None`` test and must not
        narrow the type into a conversion that could raise where the gate
        refuses."""
        assert "deflated_sharpe_not_computed" not in deflation_promotion_refusals(
            deflated_sharpe=Decimal("0.9"), trial_count=11, deflated=None, effective_sample_size=1000.0
        )

    def test_both_synthetic_control_thresholds_can_fail_at_once(self) -> None:
        """⚠ Derived from the control's own properties, never from the row's
        stored ``synthetic_control_passed`` — that column is the CONJUNCTION, so
        reading it would collapse the two codes and lose which threshold failed."""
        control = _passing_control(
            mean_return_ci_low_pct=0.5,
            mean_return_ci_high_pct=1.5,
            strategy_sharpe=0.01,
            cohort_sharpe_threshold=9.0,
        )
        assert synthetic_control_promotion_refusals(control) == (
            "synthetic_control_cohort_shows_edge",
            "synthetic_control_sharpe_below_cohort",
        )

    def test_a_single_unrecorded_holdout_evaluation_refuses(self) -> None:
        """⚠ Criterion 5 read literally would allow one unrecorded look. The rule
        applied is that EVERY evaluation carries a record."""
        assert holdout_count_promotion_refusals(holdout_evaluations=1, recorded_accesses=0) == (
            "holdout_accesses_unrecorded",
        )
        assert holdout_count_promotion_refusals(holdout_evaluations=0, recorded_accesses=0) == (
            "holdout_never_evaluated",
        )
        assert holdout_count_promotion_refusals(holdout_evaluations=2, recorded_accesses=2) == ()
