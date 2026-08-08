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

from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest

from app.services.deflated_sharpe import DeflatedSharpeResult
from app.services.random_entry_cohort import SyntheticControl
from app.services.strategy_result import (
    BENCHMARK_RULE,
    CORPUS_VERSION,
    EVALUATION_WINDOW_END,
    EVALUATION_WINDOW_START,
    HOLDOUT_BOUNDARY,
    HOLDOUT_WEIGHTING,
    PROMOTABLE_UNIVERSE_BASES,
    PROMOTION_REFUSALS,
    RESULT_SET_ID,
    SIZING_RULE,
    UNIVERSE_BASES,
    PromotionCandidate,
    ResultIdentity,
    StrategyResult,
    check_promotable,
    is_promotable,
    namespace_for_bar,
    namespace_for_position,
    namespace_for_signal,
)
from app.services.strategy_statistics import StrategyMetrics

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
        "cagr_pct": 4.0,
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
        "periods_per_year": 251.7,
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
    base: dict[str, object] = {
        "identity": _identity(),
        "metrics": _metrics(),
        "universe_basis": "survivor_only",
        "carry_unmodelled": True,
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
    }
    base.update(overrides)
    return SyntheticControl(**base)  # type: ignore[arg-type]


#: Everything a clean result needs EXCEPT its synthetic control, so a test can
#: vary that one field without restating (and drifting from) the other four.
_CLEAN_RESULT_FIELDS: dict[str, object] = {
    "universe_basis": "survivorship_free",
    "carry_unmodelled": False,
    "trial_count": 17,
    "deflated_sharpe": Decimal("0.42"),
}


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
        ],
    )
    def test_every_member_moves_the_version(self, field: str, changed: object) -> None:
        assert _identity().version != _identity(**{field: changed}).version

    def test_the_sizing_rule_moves_it(self) -> None:
        """⚠ Named separately as well as parametrised, because it is the one C11
        calls out: "a sizing change that did not move the version would let a
        different strategy inherit a track record"."""
        assert _identity().version != _identity(sizing_rule="fixed_fraction_v1").version

    def test_the_ambiguity_arm_moves_it(self) -> None:
        """§3.4's two arms are two results, not two views of one."""
        assert _identity(ambiguity_arm="worst_case").version != _identity(ambiguity_arm="best_case").version


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

    @pytest.mark.parametrize(
        "field",
        [
            "strategy_id",
            "strategy_version",
            "sizing_rule",
            "benchmark_rule",
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
            "no_instruments_evaluated",
            "holdout_never_evaluated",
            "deflated_sharpe_not_computed",
            "trial_count_undeclared",
            "ambiguity_arms_not_compared",
            "quarantine_arms_not_compared",
            "synthetic_control_not_run",
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
            "holdout_never_evaluated",
            "deflated_sharpe_not_computed",
            "trial_count_undeclared",
            "ambiguity_arms_not_compared",
            "quarantine_arms_not_compared",
            "synthetic_control_not_run",
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
