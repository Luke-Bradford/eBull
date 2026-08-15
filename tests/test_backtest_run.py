"""Pure-logic tests for §3.2's backtest run (#2394, #2240).

⚠ NO DATABASE. Every decision this job takes that a table test can pin is a
pure function — the runnable set, the hold-out pairing, the refusal-list
projection, the namespace axis, the deflation refusals, the arm-pair assembly —
and the corpus-shaped half is covered by the full-population run recorded on the
PR. The repo's standing preference: *extract the decision into a pure function
and table-test it*.
"""

from __future__ import annotations

import math
from array import array
from collections import Counter
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.services import backtest_run
from app.services.backtest_run import (
    AMBIGUITY_ARM_ORDER,
    BACKTEST_BOOTSTRAP_SEED,
    QUARANTINE_ARM_ORDER,
    STANDING_REFUSALS,
    ArmMeasurement,
    BacktestRunReport,
    ExcludedStrategy,
    NamespaceMeasurement,
    WrittenRow,
    _absorb,
    _ambiguity_material_for,
    _ambiguity_record_for,
    _assert_ambiguity_contract,
    _assert_every_runnable_produced_rows,
    _benchmark_book,
    _check_holdout_pairing,
    _Corpus,  # noqa: PLC2701 - the axis holder the namespace rule reads
    _expected_refusals,
    _fills,
    _measure_namespace,
    _NamespaceBook,
    _namespaces_for_window,
    _shifted,
    _signals_for,
    assert_no_existing_results,
    build_in_sample_split,
    build_result,
    deflate_group,
    evaluate_arm,
    evaluate_level_arms,
    runnable_strategies,
)
from app.services.cost_model import CARRY_UNMODELLED, COST_MODEL_ID, FX_UNMODELLED, UNKNOWN_NOMINAL_PRICE_BAND
from app.services.deflated_sharpe import DSR_MODEL_ID, TradeMoments
from app.services.equity_curve import BENCHMARK_RULE_ID, SIZING_RULE_ID, LegBook
from app.services.indicator_series import BarSeries
from app.services.market_regime import Regime, RegimeSeries, unconstrained_regime
from app.services.position_builder import RULE_SET_VERSION as POSITION_RULE_SET_VERSION
from app.services.position_builder import Position, Window
from app.services.position_costing import cost_position
from app.services.price_structure import StructureBar
from app.services.random_entry_cohort import (
    COHORT_MODEL_ID,
    COHORT_ROOT_SEED,
    SPEC_COHORT_SIZE,
    SPEC_SHARPE_PERCENTILE,
    SyntheticControl,
)
from app.services.research_price_structure_store import (
    QUARANTINE_ARMS,
    QUARANTINE_RULE_SET_VERSION,
    MaskedSeries,
)
from app.services.signal_ledger import LedgerRow
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_regime_evidence import RegimeTradeObservation
from app.services.strategy_result import (
    AMBIGUITY_ARMS,
    CORPUS_VERSION,
    EVALUATION_WINDOW_END,
    EVALUATION_WINDOW_START,
    HOLDOUT_BOUNDARY,
    TOTAL_RETURN_BASIS,
    ResultIdentity,
    StrategyResult,
)
from app.services.strategy_segmented_evaluation import segmented_member
from app.services.strategy_statistics import StrategyMetrics
from app.services.trial_register import TRIAL_REGISTER
from app.services.universe_selection import INTRADER_CAPTURE_DATE, SURVIVORSHIP_FREE_VENDOR
from app.services.walk_forward import FOLD_COUNT, WALK_FORWARD_MODEL_ID
from app.workers.scheduler import _optional_str  # noqa: PLC2701 - the blank-is-absent rule under test


def _metrics(*, sharpe: float = 0.5, ess: float | None = 100.0) -> StrategyMetrics:
    """A metric set that constructs, with criterion 3's block present or absent.

    ⚠ ALL NINE bootstrap fields move together — ``StrategyMetrics`` enforces the
    set as a whole — so a helper that flipped only ``effective_sample_size``
    would raise at construction rather than produce the absent case.
    """
    present = ess is not None
    return StrategyMetrics(
        expectancy_per_trade_pct=0.1,
        profit_factor=1.2,
        cagr_pct=3.0,
        annualised_volatility_pct=12.0,
        sharpe=sharpe,
        sortino=0.9,
        max_drawdown_pct=-10.0,
        exposure_time_pct=40.0,
        turnover_annualised=1.5,
        trade_count=200,
        effective_sample_size=ess,
        return_vs_buy_and_hold_pct=1.0,
        losing_trade_count=80,
        losing_period_count=90,
        open_trade_count=2,
        unpriced_trade_count=1,
        periods_per_year=250.0,
        total_return_pct=20.0,
        buy_and_hold_return_pct=19.0,
        expectancy_ci_low_pct=-0.1 if present else None,
        expectancy_ci_high_pct=0.3 if present else None,
        bootstrap_block_length=5 if present else None,
        bootstrap_cluster_count=50 if present else None,
        bootstrap_resamples=2000 if present else None,
        bootstrap_seed=BACKTEST_BOOTSTRAP_SEED if present else None,
        bootstrap_design_effect=2.0 if present else None,
        bootstrap_model_id="c3-block-bootstrap-v1" if present else None,
        # #2623 gap 1 — required alongside a non-zero trade_count under the
        # current METRIC_SET_ID.
        hold_days_p25=3.0,
        median_hold_days=8.0,
        hold_days_p75=21.0,
    )


def _control(
    *,
    ci_low: float,
    ci_high: float,
    cohort_sharpe: float,
    strategy_sharpe: float,
) -> SyntheticControl:
    """§9's control with only the four numbers its two thresholds read varied."""
    return SyntheticControl(
        model_id=COHORT_MODEL_ID,
        cohort_size=SPEC_COHORT_SIZE,
        root_seed=COHORT_ROOT_SEED,
        mean_return_pct=(ci_low + ci_high) / 2,
        mean_return_ci_low_pct=ci_low,
        mean_return_ci_high_pct=ci_high,
        sharpe_percentile=SPEC_SHARPE_PERCENTILE,
        cohort_sharpe_threshold=cohort_sharpe,
        strategy_sharpe=strategy_sharpe,
        cohort_return_threshold_pct=1.0,
        strategy_return_pct=2.0,
    )


def _measurement(
    *,
    namespace: str = "in_sample",
    sharpe: float = 0.5,
    trade_sharpe: float = 0.05,
    ess: float | None = 100.0,
    daily: dict[date, float] | None = None,
) -> NamespaceMeasurement:
    return NamespaceMeasurement(
        namespace=namespace,  # type: ignore[arg-type]
        metrics=_metrics(sharpe=sharpe, ess=ess),
        moments=TradeMoments(sharpe=trade_sharpe, skewness=0.2, kurtosis=4.0, trade_count=200),
        daily_returns=daily
        if daily is not None
        else {date(2010, 1, 4): 0.1, date(2010, 1, 5): -0.2, date(2010, 1, 6): 0.3},
        evaluated_instrument_ids=frozenset({1, 2}),
        position_count=200,
        axis_first=date(2010, 1, 4),
        axis_last=date(2010, 1, 6),
    )


class TestRunnableStrategies:
    """§3 — every declared close source must have its outcome producer."""

    def test_all_four_are_runnable_once_s4_declares_causal_exit_levels(self) -> None:
        runnable, excluded = runnable_strategies()
        assert list(runnable) == [
            "s1-time-series-momentum",
            "s10-relative-strength-leader",
            "s2-cross-sectional-momentum",
            "s3-mean-reversion-in-trend",
            "s4-volatility-compression-breakout",
            "s5-support-bounce",
            "s6-resistance-breakout",
            "s7-trend-pullback",
            "s8-range-mean-reversion",
            "s9-squeeze-expansion",
        ]
        assert excluded == ()

    def test_every_manifest_entry_is_accounted_for(self) -> None:
        from app.services.strategy_manifest import STRATEGY_MANIFEST

        runnable, excluded = runnable_strategies()
        assert set(runnable) | {entry.strategy_id for entry in excluded} == set(STRATEGY_MANIFEST)

    def test_a_level_based_entry_that_stops_refusing_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The exclusion is DERIVED from the refusal, so a missing refusal stops the run.

        ⚠ Not "fall back to runnable" and not "keep excluding it anyway": both
        would be a guess about which way a changed ``build_positions`` went, and
        the whole of §3 rests on this raise.

        ⚠ The demonstration is stubbed rather than provoked, because there is no
        regime a caller can construct today for which the builder does NOT
        refuse — which is the property under test. Stubbing the probe is the
        only way to reach the branch, and a branch nothing can reach is a branch
        nothing can prove works.
        """
        from app.services.strategy_manifest import STRATEGY_MANIFEST

        s4 = STRATEGY_MANIFEST["s4-volatility-compression-breakout"]
        manifest = {
            **STRATEGY_MANIFEST,
            s4.strategy_id: replace(s4, exit_levels=None, exit_levels_batch=None),
        }
        monkeypatch.setattr(backtest_run, "_demonstrate_level_refusal", lambda entry, regime: None)
        with pytest.raises(RuntimeError, match="did NOT refuse"):
            runnable_strategies(manifest)


class TestArmVocabularyCoverage:
    """The iteration order must cover the closed vocabularies, or rows go missing."""

    def test_orders_cover_both_vocabularies(self) -> None:
        assert set(AMBIGUITY_ARM_ORDER) == AMBIGUITY_ARMS
        assert set(QUARANTINE_ARM_ORDER) == QUARANTINE_ARMS

    def test_masked_precedes_admitted_is_not_assumed_by_the_pair_writer(self) -> None:
        """``store_*_arm_pair`` takes ``(masked, admitted)`` positionally.

        The iteration order here is alphabetical and therefore ``admitted``
        first; the writer is called with the arms named, not in loop order, and
        this pins that the two are not accidentally coupled.
        """
        assert QUARANTINE_ARM_ORDER[0] == "admitted"


class _UniformRegimeProvider:
    """A regime stand-in for pure-logic tests that have no database.

    ⚠ The strategies these tests exercise (S-1…S-4) predate the regime and ignore
    the argument, so a uniform value keeps the test about what it is actually
    testing. A test covering a regime-GATED strategy (S-5…S-10) must NOT use
    this — it would assert a market condition nobody measured and the strategy
    would fire in conditions it declares it avoids.
    """

    def for_dates(self, dates: tuple[date, ...]) -> RegimeSeries:
        return unconstrained_regime(len(dates))


class TestLevelArmSharedPass:
    """The S-4 fast path is result-equivalent to two isolated arm passes."""

    @staticmethod
    def _fixture() -> tuple[_Corpus, MaskedSeries]:
        start = date(2022, 1, 3)
        dates = tuple(start + timedelta(days=index) for index in range(220))
        bars = tuple(
            StructureBar(
                bar_date=when,
                open=Decimal(str(100 + index)),
                high=Decimal(str(100 + index + (20 if index == 150 else 1))),
                low=Decimal(str(100 + index - (20 if index == 150 else 1))),
                close=Decimal(str(100 + index)),
                volume=1000,
            )
            for index, when in enumerate(dates)
        )
        corpus = _Corpus(
            universe=(1,),
            axis=dates,
            axis_pos={when: index for index, when in enumerate(dates)},
            pairs=((1, 10),),
            evaluation_start=dates[0],
            evaluation_end=dates[-1],
        )
        return corpus, MaskedSeries(
            series_id=10,
            bars=bars,
            wealth_closes=tuple(bar.close for bar in bars),
            range_masked=0,
            return_masked=0,
            range_flagged=0,
            return_flagged=0,
            bars_flagged=0,
            arm="admitted",
        )

    def test_shared_pass_matches_isolated_measurements_and_loads_once(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        corpus, loaded = self._fixture()

        entry = STRATEGY_MANIFEST["s4-volatility-compression-breakout"]
        identity = entry.identity(universe="survivor_only", cost_model_id=COST_MODEL_ID)
        calls: list[int] = []

        def load(_conn: object, series_id: int, *, arm: str) -> MaskedSeries:
            calls.append(series_id)
            return replace(loaded, arm=arm)  # type: ignore[arg-type]

        monkeypatch.setattr(backtest_run, "load_masked_series", load)
        isolated = tuple(
            evaluate_arm(
                object(),  # type: ignore[arg-type]
                entry,
                corpus=corpus,
                quarantine_arm="admitted",
                ambiguity_arm=ambiguity,
                identity=identity,
                namespaces=("hold_out",),
                regime_provider=_UniformRegimeProvider(),  # type: ignore[arg-type]
            )
            for ambiguity in AMBIGUITY_ARM_ORDER
        )
        assert calls == [10, 10]

        calls.clear()
        shared = evaluate_level_arms(
            object(),  # type: ignore[arg-type]
            entry,
            corpus=corpus,
            quarantine_arm="admitted",
            identity=identity,
            namespaces=("hold_out",),
            regime_provider=_UniformRegimeProvider(),  # type: ignore[arg-type]
        )

        assert calls == [10]
        assert tuple(replace(item, elapsed_s=0.0) for item in shared) == tuple(
            replace(item, elapsed_s=0.0) for item in isolated
        )
        assert shared[0].namespaces["hold_out"].metrics != shared[1].namespaces["hold_out"].metrics


class TestHoldoutPairing:
    """§10 — exactly one of "neither" and "both, non-empty" is legal."""

    def test_neither_is_an_in_sample_run(self) -> None:
        assert _check_holdout_pairing(purpose=None, accessed_by=None) is False

    def test_both_non_empty_is_a_holdout_run(self) -> None:
        assert _check_holdout_pairing(purpose="audit the withheld side", accessed_by="operator") is True

    @pytest.mark.parametrize(
        ("purpose", "accessed_by"),
        [
            ("audit", None),
            (None, "operator"),
            ("audit", ""),
            ("", "operator"),
            ("audit", "   "),
        ],
    )
    def test_one_of_two_refuses(self, purpose: str | None, accessed_by: str | None) -> None:
        """⚠ A BLANK IS NOT SUPPLIED. The #2286 shape — present-but-empty passing
        a presence check — is what would let a hold-out row carry an audit record
        whose purpose is the empty string."""
        with pytest.raises(ValueError, match="needs holdout_purpose AND holdout_accessed_by"):
            _check_holdout_pairing(purpose=purpose, accessed_by=accessed_by)


class TestOptionalStr:
    """The scheduler collapses "absent" and "blank" into the one state it can judge."""

    @pytest.mark.parametrize(("raw", "expected"), [(None, None), ("", None), ("  ", None), (" x ", "x")])
    def test_blank_is_none(self, raw: object, expected: str | None) -> None:
        assert _optional_str(raw) == expected


class TestExpectedRefusals:
    """§9's table, as the projection the run gates on."""

    def test_full_holdout_run_with_a_dsr_leaves_only_the_standing_refusals(self) -> None:
        assert _expected_refusals(holdout_requested=True, deflated=True) == STANDING_REFUSALS | {
            "synthetic_control_not_run",
        }

    def test_a_survivorship_free_run_drops_the_universe_refusal_and_nothing_else(self) -> None:
        """#2721 step 3 — the refusal became CONDITIONAL on the run's universe:
        a ``survivorship_free`` corpus has defined its termination treatment,
        which is exactly what the refusal existed to demand."""
        survivor = _expected_refusals(holdout_requested=True, deflated=True, universe_basis="survivor_only")
        free = _expected_refusals(holdout_requested=True, deflated=True, universe_basis="survivorship_free")
        assert survivor - free == {"universe_basis_not_survivorship_free"}
        assert free - survivor == set()

    def test_harness_validation_purpose_is_predicted_before_the_write(self) -> None:
        assert _expected_refusals(
            holdout_requested=True,
            deflated=True,
            purpose="harness_validation",
        ) == STANDING_REFUSALS | {
            "harness_validation_only",
            "synthetic_control_not_run",
        }

    def test_in_sample_run_adds_holdout_never_evaluated(self) -> None:
        assert _expected_refusals(holdout_requested=False, deflated=True) == STANDING_REFUSALS | {
            "holdout_never_evaluated",
            "synthetic_control_not_run",
        }

    @pytest.mark.parametrize(
        ("holdout_requested", "prior", "expects_never_evaluated"),
        [
            (False, 0, True),  # first run, in-sample only -- the refusal applies
            (False, 12, False),  # RE-RUN of a version already hold-out evaluated (#2433)
            (True, 0, False),  # this run evaluates the hold-out
            (True, 12, False),  # both -- still evaluated
        ],
    )
    def test_holdout_never_evaluated_follows_the_LEDGER_not_the_invocation(
        self, holdout_requested: bool, prior: int, expects_never_evaluated: bool
    ) -> None:
        """#2433 — ``check_promotable`` derives this refusal from stored hold-out
        rows, so predicting it from ``holdout_requested`` alone is right only on
        a FIRST run.

        ⚠ Row two is the one that mattered: it was unreachable until #2426 moved
        every ``result_version``, and the first re-run it allowed rejected after
        a full corpus pass with the corrected buy-and-hold numbers unwritten.
        """
        refusals = _expected_refusals(
            holdout_requested=holdout_requested, deflated=True, prior_holdout_evaluations=prior
        )
        assert ("holdout_never_evaluated" in refusals) is expects_never_evaluated

    def test_a_prior_evaluation_does_not_disturb_the_other_refusals(self) -> None:
        assert _expected_refusals(
            holdout_requested=False, deflated=False, prior_holdout_evaluations=12
        ) == STANDING_REFUSALS | {
            "deflated_sharpe_not_computed",
            "trial_count_undeclared",
            "synthetic_control_not_run",
        }

    def test_no_dsr_adds_both_criterion_6_refusals(self) -> None:
        """⚠ TWO codes, not one. A DSR with no trial count is as refused as no
        DSR at all, and collapsing them would make "which of the two is missing"
        unanswerable."""
        assert _expected_refusals(holdout_requested=True, deflated=False) == STANDING_REFUSALS | {
            "deflated_sharpe_not_computed",
            "trial_count_undeclared",
            "synthetic_control_not_run",
        }

    def test_the_standing_refusals_are_the_ones_this_cut_cannot_close(self) -> None:
        """⚠ ONE member. ``synthetic_control_not_run`` left this set in #2601;
        ``carry_unmodelled``/``fx_unmodelled`` closed structurally in #2720
        (predicted from the cost-model constants, the same source the row is
        stamped from); ``universe_basis_not_survivorship_free`` left in #2721
        step 3 — conditional on the run's universe, not standing."""
        assert STANDING_REFUSALS == {"promotion_evidence_missing"}
        assert not CARRY_UNMODELLED and not FX_UNMODELLED

    def test_carry_and_fx_are_predicted_from_the_stamping_constants(self) -> None:
        """#2720's structural closure: under cost model v3 no run carries
        either refusal, and the prediction reads the SAME constants
        ``build_result`` stamps — a standing entry mis-predicted every
        post-#2720 run (caught by the #2721 smoke)."""
        refusals = _expected_refusals(holdout_requested=True, deflated=True)
        assert "carry_unmodelled" not in refusals
        assert "fx_unmodelled" not in refusals

    def test_a_run_without_the_control_still_declares_it_unrun(self) -> None:
        assert "synthetic_control_not_run" in _expected_refusals(holdout_requested=True, deflated=True)

    @pytest.mark.parametrize(
        ("ci_low", "ci_high", "cohort_sharpe", "strategy_sharpe", "expected"),
        [
            # The cohort's mean return brackets zero and the strategy clears the
            # cohort's 95th percentile — §9's conjunction, both halves passed.
            (-0.4, 0.4, 0.10, 0.90, set()),
            # The null itself makes money: the cohort is measuring something
            # other than the strategy's edge, so no strategy number is readable.
            (0.2, 0.9, 0.10, 0.90, {"synthetic_control_cohort_shows_edge"}),
            # A strategy that does not beat random entry.
            (-0.4, 0.4, 0.95, 0.90, {"synthetic_control_sharpe_below_cohort"}),
            # ⚠ BOTH, together. Reporting one would leave the operator repairing
            # half a failure and re-running for the other half.
            (0.2, 0.9, 0.95, 0.90, {"synthetic_control_cohort_shows_edge", "synthetic_control_sharpe_below_cohort"}),
        ],
    )
    def test_the_control_decides_which_of_its_two_codes_apply(
        self, ci_low: float, ci_high: float, cohort_sharpe: float, strategy_sharpe: float, expected: set[str]
    ) -> None:
        """⚠ §9's two thresholds predicted INDEPENDENTLY of ``check_promotable``.
        The equality between the two is criterion 8's check; a shared helper
        would make it agree with itself."""
        refusals = _expected_refusals(
            holdout_requested=True,
            deflated=True,
            synthetic_control=_control(
                ci_low=ci_low,
                ci_high=ci_high,
                cohort_sharpe=cohort_sharpe,
                strategy_sharpe=strategy_sharpe,
            ),
        )
        extra = {str(code) for code in refusals - STANDING_REFUSALS}
        assert extra - {"universe_basis_not_survivorship_free"} == expected
        assert "synthetic_control_not_run" not in refusals


class TestDeflateGroup:
    """§2 — the trials are the strategies, and every refusal is stated."""

    def test_one_measured_trial_refuses_with_the_minimum(self) -> None:
        deflation, reason = deflate_group({"s1-time-series-momentum": _measurement()})
        assert deflation is None
        assert reason is not None and "MIN_MEASURED_TRIALS" in reason

    def test_a_trial_with_no_effective_sample_size_does_not_count(self) -> None:
        """Criterion 6 consumes criterion 3's number, so a trial without one is
        not a measured trial for this purpose."""
        deflation, reason = deflate_group(
            {
                "s1-time-series-momentum": _measurement(),
                "s3-mean-reversion-in-trend": _measurement(ess=None),
            }
        )
        assert deflation is None
        assert reason is not None and "MIN_MEASURED_TRIALS" in reason

    def test_an_undeclared_trial_id_refuses_rather_than_raising(self) -> None:
        """⚠ A measured trial the register does not declare under-counts the
        search and RAISES the DSR — the favourable direction — so it must fail
        rather than be quietly dropped."""
        deflation, reason = deflate_group(
            {
                "s1-time-series-momentum": _measurement(),
                "not-a-declared-trial": _measurement(trade_sharpe=0.09),
            }
        )
        assert deflation is None
        assert reason is not None and "not declared trials" in reason

    def test_identical_sharpes_leave_no_variance_to_deflate_against(self) -> None:
        deflation, reason = deflate_group(
            {
                "s1-time-series-momentum": _measurement(trade_sharpe=0.05),
                "s3-mean-reversion-in-trend": _measurement(trade_sharpe=0.05),
            }
        )
        assert deflation is None
        assert reason == "V[SR_n] is zero or undefined over the measured trials"

    def test_fewer_than_two_shared_dates_refuses(self) -> None:
        deflation, reason = deflate_group(
            {
                "s1-time-series-momentum": _measurement(trade_sharpe=0.05, daily={date(2010, 1, 4): 0.1}),
                "s3-mean-reversion-in-trend": _measurement(trade_sharpe=0.09, daily={date(2010, 1, 4): 0.2}),
            }
        )
        assert deflation is None
        assert reason is not None and "fewer than 2 active dates" in reason

    def test_a_constant_return_series_refuses_and_is_not_read_as_uncorrelated(self) -> None:
        """⚠⚠ A ZERO-VARIANCE TRIAL IS DETECTED ON THE INPUT, NOT ON THE MATRIX.

        ``np.corrcoef`` is widely believed to return NaN for a constant series.
        Measured on **numpy 2.4.4** (2026-08-08) it returns a finite **0.0** —
        pinned below — so an ``isfinite`` guard is dead code and the trial would
        be read as UNCORRELATED, pushing the implied independent trial count
        toward ``M`` on evidence that does not exist.

        ⚠ ``scripts/verify_2240_statistics.py::_criterion6`` carried exactly that
        dead guard until #2420; it now tests the input the same way this does, and
        keeps ``isfinite`` only as a backstop against a future numpy restoring
        NaN. This docstring named that script as the counter-example until the
        fix landed — kept as the reference the two implementations share, since a
        pin whose subject silently changes is how the next copy gets made.
        """
        flat = {date(2010, 1, 4): 0.1, date(2010, 1, 5): 0.1, date(2010, 1, 6): 0.1}
        # The behaviour the guard cannot rely on, pinned so a numpy change that
        # restores NaN is visible here rather than silently making the live
        # check redundant.
        import numpy as np

        matrix = np.corrcoef(np.array([[0.1, 0.1, 0.1], [0.1, -0.2, 0.3]]))
        assert bool(np.all(np.isfinite(matrix)))

        deflation, reason = deflate_group(
            {
                "s1-time-series-momentum": _measurement(trade_sharpe=0.05, daily=flat),
                "s3-mean-reversion-in-trend": _measurement(trade_sharpe=0.09),
            }
        )
        assert deflation is None
        assert reason is not None and "constant return series" in reason

    def test_two_declared_trials_deflate(self) -> None:
        deflation, reason = deflate_group(
            {
                "s1-time-series-momentum": _measurement(trade_sharpe=0.05),
                "s3-mean-reversion-in-trend": _measurement(
                    trade_sharpe=0.09,
                    # ⚠ POSITIVELY correlated with the default series above. A
                    # strongly NEGATIVE measured rho falls outside A.3's
                    # `(-1/(M-1), 1]` bound and is a refusal, not a deflation —
                    # covered by its own case rather than reached by accident
                    # here.
                    daily={date(2010, 1, 4): 0.2, date(2010, 1, 5): -0.3, date(2010, 1, 6): 0.5},
                ),
            }
        )
        assert reason is None
        assert deflation is not None
        assert deflation.measured_trials == 2
        assert deflation.variance > 0.0
        assert -1.0 <= deflation.correlation <= 1.0


class TestBuildResult:
    """§7 — thirteen of the fourteen identity members are module constants."""

    def test_every_identity_member_comes_from_the_module_that_froze_it(self) -> None:
        result = build_result(
            _measurement(),
            strategy_id="s1-time-series-momentum",
            strategy_version="strategy-v1+abc",
            purpose="capital_candidate",
            ambiguity_arm="worst_case",
            quarantine_arm="masked",
            deflated=None,
        )
        identity = result.identity
        assert identity.result_scope == "sleeve"
        assert identity.sizing_rule == SIZING_RULE_ID
        assert identity.cost_model_id == COST_MODEL_ID
        assert identity.corpus_version == f"{SURVIVORSHIP_FREE_VENDOR}@{INTRADER_CAPTURE_DATE.isoformat()}"
        assert identity.window_start == EVALUATION_WINDOW_START
        assert identity.window_end == INTRADER_CAPTURE_DATE
        assert identity.position_rule_set_version == POSITION_RULE_SET_VERSION
        # ⚠⚠ THE QUARANTINE RULE SET, not StrategyIdentity.input_rule_set_versions
        # (indicator-only, and already inside strategy_version — folding it in
        # here would hash it twice).
        assert identity.input_rule_set_version == QUARANTINE_RULE_SET_VERSION
        assert result.universe_basis == "survivorship_free"
        assert result.evaluated_instrument_count == 2

    def test_no_dsr_leaves_both_criterion_6_scalars_null(self) -> None:
        """``sql/266``'s CHECK is all-or-nothing across twelve columns, so a
        declared trial count beside a null DSR is a row the table refuses."""
        result = build_result(
            _measurement(),
            strategy_id="s1-time-series-momentum",
            strategy_version="strategy-v1+abc",
            purpose="capital_candidate",
            ambiguity_arm="worst_case",
            quarantine_arm="masked",
            deflated=None,
        )
        assert result.trial_count is None
        assert result.deflated_sharpe is None

    def test_a_dsr_binds_both_scalars_to_itself(self) -> None:
        from app.services.deflated_sharpe import DeflatedSharpeResult

        deflated = DeflatedSharpeResult(
            deflated_sharpe=0.42,
            expected_max_sharpe=0.1,
            trade_sharpe=0.05,
            skewness=0.2,
            kurtosis=4.0,
            effective_sample_size=100.0,
            declared_trials=TRIAL_REGISTER.declared_count,
            independent_trials=5.0,
            average_trial_correlation=0.3,
            trial_sharpe_variance=0.001,
            measured_trials=3,
            trial_register_version=TRIAL_REGISTER.version,
            model_id=DSR_MODEL_ID,
        )
        result = build_result(
            _measurement(),
            strategy_id="s1-time-series-momentum",
            strategy_version="strategy-v1+abc",
            purpose="capital_candidate",
            ambiguity_arm="worst_case",
            quarantine_arm="masked",
            deflated=deflated,
        )
        assert result.trial_count == TRIAL_REGISTER.declared_count
        assert result.deflated_sharpe == Decimal("0.42")
        # `StrategyResult.__post_init__` binds the DSR's sample size to the
        # metric set's; a mismatch would mean the stored DSR is a number no
        # stored input produces.
        assert result.deflated is deflated

    def test_the_arms_are_the_only_thing_separating_two_rows(self) -> None:
        """Criterion 9's pair writer requires the admitted identity to be the
        masked one with the arm flipped, and nothing else."""
        common = {
            "strategy_id": "s1-time-series-momentum",
            "strategy_version": "strategy-v1+abc",
            "purpose": "capital_candidate",
            "ambiguity_arm": "worst_case",
            "deflated": None,
        }
        masked = build_result(_measurement(), quarantine_arm="masked", **common)  # type: ignore[arg-type]
        admitted = build_result(_measurement(), quarantine_arm="admitted", **common)  # type: ignore[arg-type]
        assert masked.identity.version != admitted.identity.version
        from dataclasses import replace

        assert replace(masked.identity, quarantine_arm="admitted") == admitted.identity

    def test_a_recent_window_is_part_of_the_result_identity(self) -> None:
        recent = Window(date(2024, 7, 9), EVALUATION_WINDOW_END)
        legacy = build_result(
            _measurement(),
            strategy_id="s1-time-series-momentum",
            strategy_version="strategy-v1+abc",
            purpose="capital_candidate",
            ambiguity_arm="worst_case",
            quarantine_arm="masked",
            deflated=None,
        )
        result = build_result(
            _measurement(),
            strategy_id="s1-time-series-momentum",
            strategy_version="strategy-v1+abc",
            purpose="capital_candidate",
            ambiguity_arm="worst_case",
            quarantine_arm="masked",
            deflated=None,
            evaluation_window=recent,
        )
        assert (result.identity.window_start, result.identity.window_end) == (recent.start, recent.end)
        assert result.identity.version != legacy.identity.version


class TestRecentWindowNamespace:
    def test_registered_recent_window_is_holdout_only_and_audited(self) -> None:
        recent = Window(date(2022, 1, 1), EVALUATION_WINDOW_END)
        assert _namespaces_for_window(holdout_requested=True, evaluation_window=recent) == ("hold_out",)
        with pytest.raises(ValueError, match="requires an audited access"):
            _namespaces_for_window(holdout_requested=False, evaluation_window=recent)

    def test_custom_window_cannot_reclassify_pre_boundary_data(self) -> None:
        with pytest.raises(ValueError, match="on or after the frozen hold-out boundary"):
            _namespaces_for_window(
                holdout_requested=True,
                evaluation_window=Window(date(2020, 1, 1), date(2022, 1, 1)),
            )


class TestPlannedIdentities:
    """§10 — a colliding ``result_version`` is refused before the corpus pass."""

    def test_two_planned_rows_sharing_a_version_raise_without_touching_the_database(self) -> None:
        identity = ResultIdentity(
            strategy_id="s1",
            strategy_version="v1",
            result_scope="sleeve",
            namespace="in_sample",
            ambiguity_arm="worst_case",
            quarantine_arm="masked",
            sizing_rule=SIZING_RULE_ID,
            benchmark_rule=BENCHMARK_RULE_ID,
            cost_model_id=COST_MODEL_ID,
            corpus_version=CORPUS_VERSION,
            window_start=EVALUATION_WINDOW_START,
            window_end=EVALUATION_WINDOW_END,
            position_rule_set_version=POSITION_RULE_SET_VERSION,
            outcome_rule_set_version="outcome-v1",
            input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
            return_basis=TOTAL_RETURN_BASIS,
        )
        # ⚠ ``conn`` is never reached: the duplicate check runs before the
        # query, which is what makes this a pure test of the planning step.
        with pytest.raises(RuntimeError, match="two rows with the same result_version"):
            assert_no_existing_results(None, [identity, identity])  # type: ignore[arg-type]


class TestShiftedLegBook:
    """§5's re-basing: the same legs on a truncated axis."""

    def test_indices_shift_and_the_large_arrays_are_shared(self) -> None:
        book = LegBook()
        book.add(
            entry_index=10,
            exit_index=12,
            entry_price=1.0,
            exit_price=1.1,
            half_spread=0.01,
            realised=True,
            marks=[1.0, 1.05, 1.1],
        )
        shifted = _shifted(book, 10)
        assert shifted.entry_index == [0]
        assert shifted.exit_index == [2]
        # Shared rather than copied: the marks array is the large one and it is
        # read-only from here on.
        assert shifted.marks is book.marks
        assert shifted.entry_price is book.entry_price


class TestBenchmarkBook:
    """Criterion 7's twelfth metric, clipped to one namespace's axis."""

    @staticmethod
    def _closes() -> dict[int, tuple[int, array[float]]]:
        # Instrument 1 spans axis indices 0..5, instrument 2 spans 4..9.
        return {
            1: (0, array("d", [10.0, 11.0, 12.0, 13.0, 14.0, 15.0])),
            2: (4, array("d", [20.0, 21.0, 22.0, 23.0, 24.0, 25.0])),
        }

    def test_only_the_namespaces_own_instruments_get_a_leg(self) -> None:
        book = _benchmark_book(
            instruments=frozenset({1}),
            raw_closes_by_instrument=self._closes(),
            wealth_closes_by_instrument=self._closes(),
            lo=0,
            hi=9,
        )
        assert len(book) == 1

    def test_a_leg_is_clipped_to_the_axis_rather_than_dropped(self) -> None:
        book = _benchmark_book(
            instruments=frozenset({1, 2}),
            raw_closes_by_instrument=self._closes(),
            wealth_closes_by_instrument=self._closes(),
            lo=3,
            hi=6,
        )
        assert len(book) == 2
        # Instrument 1 straddles the lower bound: it opens at index 3 (offset 0
        # on the truncated axis) and closes at its own last bar, index 5.
        assert book.entry_index[0] == 0
        assert book.exit_index[0] == 2
        # Instrument 2 starts inside the window at index 4 (offset 1).
        assert book.entry_index[1] == 1
        assert book.exit_index[1] == 3

    def test_an_instrument_wholly_outside_the_axis_contributes_nothing(self) -> None:
        book = _benchmark_book(
            instruments=frozenset({1, 2}),
            raw_closes_by_instrument=self._closes(),
            wealth_closes_by_instrument=self._closes(),
            lo=7,
            hi=9,
        )
        assert len(book) == 1

    def test_a_single_usable_bar_is_not_a_round_trip(self) -> None:
        book = _benchmark_book(
            instruments=frozenset({1}),
            raw_closes_by_instrument={1: (0, array("d", [10.0, math.nan, math.nan]))},
            wealth_closes_by_instrument={1: (0, array("d", [10.0, math.nan, math.nan]))},
            lo=0,
            hi=2,
        )
        assert len(book) == 0

    def test_the_benchmark_is_charged_the_same_cost_model(self) -> None:
        """A cost-free benchmark would make every strategy look worse by exactly
        the amount the cost model charges — a comparison of cost models."""
        book = _benchmark_book(
            instruments=frozenset({1}),
            raw_closes_by_instrument=self._closes(),
            wealth_closes_by_instrument=self._closes(),
            lo=0,
            hi=5,
        )
        assert book.entry_price[0] > 10.0
        assert book.exit_price[0] < 15.0
        assert book.half_spread[0] > 0.0

    def test_split_adjusted_close_uses_the_maximum_spread_and_wealth_close_measures_return(self) -> None:
        book = _benchmark_book(
            instruments=frozenset({1}),
            raw_closes_by_instrument={1: (0, array("d", [10.0, 11.0]))},
            wealth_closes_by_instrument={1: (0, array("d", [100.0, 120.0]))},
            lo=0,
            hi=1,
        )
        assert book.entry_price[0] > 100.0
        assert book.exit_price[0] < 120.0
        assert book.half_spread[0] == float(UNKNOWN_NOMINAL_PRICE_BAND.half_spread)
        assert book.marks.tolist() == [100.0, 120.0]

    def test_non_finite_wealth_observation_excludes_the_benchmark_leg(self) -> None:
        book = _benchmark_book(
            instruments=frozenset({1}),
            raw_closes_by_instrument={1: (0, array("d", [10.0, 11.0, 12.0]))},
            wealth_closes_by_instrument={1: (0, array("d", [100.0, math.inf, 120.0]))},
            lo=0,
            hi=2,
        )
        assert book.entry_index == []


class TestTotalReturnStrategyLeg:
    def test_a_raw_loss_can_be_a_total_return_gain_without_changing_the_fill(self) -> None:
        entry_day, exit_day = date(2024, 1, 2), date(2024, 1, 3)
        position = Position(
            strategy_id="s1",
            strategy_version="v1",
            instrument_id=1,
            entry_signal_id=1,
            entry_signal_bar_date=date(2024, 1, 1),
            entry_fill_bar_date=entry_day,
            entry_fill_price=Decimal("100"),
            close_source="signal_pair",
            close_bar_date=exit_day,
            close_price=Decimal("90"),
            bars_held=1,
            open_reason=None,
            mark_price=None,
        )
        series = BarSeries(
            dates=(entry_day, exit_day),
            rows=(
                {
                    "open": Decimal("100"),
                    "high": Decimal("101"),
                    "low": Decimal("99"),
                    "close": Decimal("100"),
                    "volume": 1000,
                },
                {
                    "open": Decimal("90"),
                    "high": Decimal("91"),
                    "low": Decimal("89"),
                    "close": Decimal("90"),
                    "volume": 1000,
                },
            ),
        )
        books = {"hold_out": _NamespaceBook()}
        _absorb(
            [cost_position(position, price_basis="split_adjusted")],
            series=series,
            window=Window(entry_day, exit_day),
            axis_pos={entry_day: 0, exit_day: 1},
            raw_closes=[100.0, 90.0],
            wealth_closes=[80.0, 90.0],
            first_axis_index=0,
            instrument_id=1,
            books=books,  # type: ignore[arg-type]
            close_sources=Counter(),
            discarded=Counter(),
            market_regime_by_date={date(2024, 1, 1): Regime.BULL_QUIET},
        )
        book = books["hold_out"]
        assert book.returns[0] > 0.0
        assert book.book.entry_price[0] < float(position.entry_fill_price)
        assert book.book.marks.tolist() == [80.0, 90.0]
        assert len(book.regime_observations) == 1
        assert book.regime_observations[0].signal_date == position.entry_signal_bar_date
        assert book.regime_observations[0].regime is Regime.BULL_QUIET


class TestNamespaceAxis:
    """§5 — the axis is measured from the namespace's own positions."""

    @staticmethod
    def _corpus(axis: tuple[date, ...]) -> _Corpus:
        return _Corpus(
            universe=(1,),
            axis=axis,
            axis_pos={when: index for index, when in enumerate(axis)},
            pairs=((1, 1),),
        )

    def test_an_in_sample_position_closing_on_the_boundary_raises(self) -> None:
        """⚠ A violation means ``namespace_for_position`` mis-classified. It
        never means the axis needs widening, so it raises rather than adjusting.
        """
        axis = (date(2021, 6, 25), date(2021, 6, 28), HOLDOUT_BOUNDARY)
        book = _NamespaceBook()
        book.add_leg(
            entry_index=0,
            exit_index=2,
            entry_price=1.0,
            exit_price=1.1,
            half_spread=0.01,
            realised=True,
            marks=[1.0, 1.05, 1.1],
        )
        with pytest.raises(RuntimeError, match="on or after the frozen boundary"):
            _measure_namespace(
                "in_sample",
                book,
                corpus=self._corpus(axis),
                raw_closes_by_instrument={},
                wealth_closes_by_instrument={},
            )

    def test_an_in_sample_namespace_holding_an_open_position_raises(self) -> None:
        """⚠ ``namespace_for_position`` sends EVERY open position to the hold-out,
        so an open leg on the in-sample book means the partition broke. It matters
        beyond the axis now: criterion 5's embargo is measured off these label
        windows, and an open one contributes the span to its MARK bar — a hold the
        strategy never realised.
        """
        axis = tuple(date(2010, 1, day) for day in range(1, 11))
        book = _NamespaceBook(records_label_windows=True)
        book.instruments.add(1)
        book.add_leg(
            entry_index=3,
            exit_index=6,
            entry_price=1.0,
            exit_price=1.2,
            half_spread=0.0,
            realised=False,
            marks=[1.0, 1.1, 1.15, 1.2],
        )
        book.open_at_end = 1
        with pytest.raises(RuntimeError, match="open at the window end"):
            _measure_namespace(
                "in_sample",
                book,
                corpus=self._corpus(axis),
                raw_closes_by_instrument={},
                wealth_closes_by_instrument={},
            )

    def test_a_namespace_with_no_positions_measures_nothing(self) -> None:
        axis = (date(2021, 6, 25), date(2021, 6, 28))
        assert (
            _measure_namespace(
                "in_sample",
                _NamespaceBook(),
                corpus=self._corpus(axis),
                raw_closes_by_instrument={},
                wealth_closes_by_instrument={},
            )
            is None
        )

    def test_the_axis_is_the_span_of_the_namespaces_own_legs(self) -> None:
        axis = tuple(date(2010, 1, day) for day in range(1, 11))
        book = _NamespaceBook()
        book.instruments.add(1)
        book.add_leg(
            entry_index=3,
            exit_index=6,
            entry_price=1.0,
            exit_price=1.2,
            half_spread=0.0,
            realised=True,
            marks=[1.0, 1.1, 1.15, 1.2],
        )
        # ⚠ Several trades on distinct entry dates: criterion 3's block
        # bootstrap runs over the CLUSTER axis, and a single cluster gives it
        # nothing to resample.
        for offset, value in enumerate((20.0, -5.0, 7.5, -2.0)):
            book.returns.append(value)
            book.entry_dates.append(axis[3 + offset])
            book.regime_observations.append(
                RegimeTradeObservation(
                    instrument_key=1,
                    signal_date=axis[3 + offset],
                    net_return_pct=value,
                    regime=Regime.BULL_QUIET,
                )
            )
            # #2623 gap 1 — the three axes are positionally parallel and
            # `TradeReturns` refuses a short one, so a hand-built book has to
            # fill this too. Exit one bar after entry; the durations are not
            # what this test is about.
            book.exit_dates.append(axis[4 + offset])
        outcome = _measure_namespace(
            "in_sample",
            book,
            corpus=self._corpus(axis),
            raw_closes_by_instrument={1: (3, array("d", [1.0, 1.1, 1.15, 1.2]))},
            wealth_closes_by_instrument={1: (3, array("d", [1.0, 1.1, 1.15, 1.2]))},
        )
        assert outcome is not None
        # ⚠ NOT the corpus start. A strategy's warm-up means its first position
        # lands well after the first bar, and padding the axis back would dilute
        # exactly the CAGR the rule exists to protect.
        assert outcome.axis_first == axis[3]
        assert outcome.axis_last == axis[6]
        assert outcome.metrics.effective_sample_size is not None

    def test_a_namespace_whose_bootstrap_cannot_run_refuses_rather_than_storing_a_nominal_n(self) -> None:
        """Acceptance 7 — every stored row carries a non-null effective sample size.

        ⚠ Criterion 3 forbids reporting a nominal *n* in its place, so the
        honest answer to "the block bootstrap had one cluster" is that no row
        can be written for that namespace — not a row with the count the
        criterion exists to replace.
        """
        axis = tuple(date(2010, 1, day) for day in range(1, 11))
        book = _NamespaceBook()
        book.instruments.add(1)
        book.add_leg(
            entry_index=3,
            exit_index=6,
            entry_price=1.0,
            exit_price=1.2,
            half_spread=0.0,
            realised=True,
            marks=[1.0, 1.1, 1.15, 1.2],
        )
        book.returns.append(20.0)
        book.entry_dates.append(axis[3])
        book.exit_dates.append(axis[4])
        with pytest.raises(RuntimeError, match="no effective sample size"):
            _measure_namespace(
                "in_sample",
                book,
                corpus=self._corpus(axis),
                raw_closes_by_instrument={1: (3, array("d", [1.0, 1.1, 1.15, 1.2]))},
                wealth_closes_by_instrument={1: (3, array("d", [1.0, 1.1, 1.15, 1.2]))},
            )


class TestAmbiguityCensus:
    """§6 — a runnable strategy cannot close ``ambiguous``, and it is measured."""

    @staticmethod
    def _arm(close_sources: dict[str, int]) -> ArmMeasurement:
        return ArmMeasurement(
            strategy_id="s1-time-series-momentum",
            strategy_version="v1",
            quarantine_arm="masked",
            namespaces={},
            holdout_positions_discarded=0,
            close_sources=close_sources,
            series_evaluated=1,
            elapsed_s=0.0,
        )

    def test_a_clean_census_passes(self) -> None:
        _assert_ambiguity_contract(self._arm({"signal_pair": 10, "open_at_window_end": 1}))

    def test_one_ambiguous_close_falsifies_the_single_measurement_claim(self) -> None:
        with pytest.raises(RuntimeError, match="after arm resolution"):
            _assert_ambiguity_contract(self._arm({"signal_pair": 10, "ambiguous": 1}))


class TestAmbiguityMateriality:
    """A real level-arm delta cannot be waved through without its threshold."""

    @staticmethod
    def _arm(ambiguity: str | None, sharpe: float, *, control: SyntheticControl | None = None) -> ArmMeasurement:
        return ArmMeasurement(
            strategy_id="s4",
            strategy_version="v1",
            ambiguity_arm=ambiguity,  # type: ignore[arg-type]
            quarantine_arm="masked",
            namespaces={"in_sample": _measurement(sharpe=sharpe)},
            holdout_positions_discarded=0,
            close_sources={},
            series_evaluated=1,
            elapsed_s=0.0,
            cohort=None if control is None else SimpleNamespace(control=control),  # type: ignore[arg-type]
        )

    @staticmethod
    def _result() -> StrategyResult:
        return build_result(
            _measurement(),
            strategy_id="s4",
            strategy_version="v1",
            purpose="capital_candidate",
            ambiguity_arm="best_case",
            quarantine_arm="masked",
            deflated=None,
        )

    def test_a_shared_non_level_measurement_proves_zero_gap(self) -> None:
        assert _ambiguity_material_for((self._arm(None, 0.5),), self._result()) is False

    def test_equal_level_arms_prove_zero_gap(self) -> None:
        arms = (self._arm("best_case", 0.5), self._arm("worst_case", 0.5))
        assert _ambiguity_material_for(arms, self._result()) is False

    def test_unequal_level_arms_refuse_until_the_control_threshold_is_attached(self) -> None:
        arms = (self._arm("best_case", 0.6), self._arm("worst_case", 0.4))
        assert _ambiguity_material_for(arms, self._result()) is None
        assert "ambiguity_arms_not_compared" in _expected_refusals(
            holdout_requested=True,
            deflated=True,
            ambiguity_material=None,
        )

    def test_two_matched_controls_attach_the_weaker_margin_and_decide_materiality(self) -> None:
        arms = (
            self._arm(
                "best_case",
                0.6,
                control=_control(ci_low=-0.1, ci_high=0.1, cohort_sharpe=0.2, strategy_sharpe=0.6),
            ),
            self._arm(
                "worst_case",
                0.4,
                control=_control(ci_low=-0.1, ci_high=0.1, cohort_sharpe=0.3, strategy_sharpe=0.4),
            ),
        )
        record = _ambiguity_record_for(arms, self._result())
        assert record.cohort_gap_threshold == pytest.approx(0.1)
        assert _ambiguity_material_for(arms, self._result()) is True

    def test_a_non_finite_sharpe_reads_as_unpriced_rather_than_crashing(self) -> None:
        # ⚠ The old code reached `None` here BY ACCIDENT — `nan == nan` is
        # False, so it fell through to "not compared". `AmbiguityRecord` refuses
        # a non-finite value, so without the explicit branch in
        # `_ambiguity_record_for` a degenerate zero-volatility measurement would
        # turn a verdict into a crashed run.
        arms = (self._arm("best_case", float("nan")), self._arm("worst_case", 0.4))
        assert _ambiguity_material_for(arms, self._result()) is None

    def test_the_frozen_record_does_not_depend_on_the_ambiguity_arm(self) -> None:
        """⚠⚠ SIBLING CONSISTENCY — the two rows of one comparison must agree.

        The verdict is a function of (strategy_id, quarantine_arm, namespace)
        and NOT of `ambiguity_arm`: `_ambiguity_record_for` filters on the first
        three only. So the two result rows that differ solely in their ambiguity
        arm are two views of ONE comparison and must freeze identical records.

        That holds by construction today and nothing enforced it. An edit that
        made the record depend on the arm would give a single §3.4 comparison
        two stored verdicts, and `promote_strategy` would then admit whichever
        of the pair happened to pass.
        """
        arms = (self._arm("best_case", 0.6), self._arm("worst_case", 0.4))
        best = build_result(
            _measurement(),
            strategy_id="s4",
            strategy_version="v1",
            purpose="capital_candidate",
            ambiguity_arm="best_case",
            quarantine_arm="masked",
            deflated=None,
        )
        worst = build_result(
            _measurement(),
            strategy_id="s4",
            strategy_version="v1",
            purpose="capital_candidate",
            ambiguity_arm="worst_case",
            quarantine_arm="masked",
            deflated=None,
        )
        assert best.identity.ambiguity_arm != worst.identity.ambiguity_arm
        assert _ambiguity_record_for(arms, best) == _ambiguity_record_for(arms, worst)


class TestRowCompleteness:
    """§11 — a RUNNABLE strategy that produced no row fails the run."""

    @staticmethod
    def _report(rows: tuple[WrittenRow, ...], runnable: tuple[str, ...]) -> BacktestRunReport:
        return BacktestRunReport(
            runnable=runnable,
            excluded=(ExcludedStrategy(strategy_id="s4-volatility-compression-breakout", reason="level-based"),),
            holdout_requested=False,
            arms=(),
            rows=rows,
        )

    @staticmethod
    def _rows(strategy_ids: tuple[str, ...]) -> tuple[WrittenRow, ...]:
        rows: list[WrittenRow] = []
        for strategy_id in strategy_ids:
            for ambiguity in AMBIGUITY_ARM_ORDER:
                for quarantine in QUARANTINE_ARM_ORDER:
                    rows.append(
                        WrittenRow(
                            strategy_id=strategy_id,
                            result_version=f"{strategy_id}-{ambiguity}-{quarantine}",
                            namespace="in_sample",
                            ambiguity_arm=ambiguity,
                            quarantine_arm=quarantine,
                            result_id=len(rows),
                            evaluated_instrument_count=10,
                            refusals=(),
                            # ⚠ A COMPLETE in-sample row now carries its whole
                            # split — the run asserts it, so the helper that
                            # describes a complete run has to produce it.
                            folds_written=FOLD_COUNT,
                        )
                    )
        return tuple(rows)

    def test_a_complete_in_sample_run_passes(self) -> None:
        runnable = ("s1", "s3")
        _assert_every_runnable_produced_rows(
            self._report(self._rows(runnable), runnable),
            namespaces=("in_sample",),
        )

    def test_a_runnable_strategy_with_no_row_fails(self) -> None:
        """⚠ Against the RUNNABLE set, never the manifest: S-4's absence is a
        reported exclusion and must not fail the run, which is the same
        wrong-side anchoring the review bot found in §3.1's population gate."""
        with pytest.raises(RuntimeError, match=r"runnable strategies \['s3'\] produced no result row"):
            _assert_every_runnable_produced_rows(
                self._report(self._rows(("s1",)), ("s1", "s3")),
                namespaces=("in_sample",),
            )

    def test_a_short_arm_fails_even_when_every_strategy_appears(self) -> None:
        rows = self._rows(("s1", "s3"))[:-1]
        with pytest.raises(RuntimeError, match="rows written against"):
            _assert_every_runnable_produced_rows(
                self._report(rows, ("s1", "s3")),
                namespaces=("in_sample",),
            )

    def test_the_excluded_strategy_is_not_counted_against_the_run(self) -> None:
        runnable = ("s1", "s3")
        report = self._report(self._rows(runnable), runnable)
        assert [entry.strategy_id for entry in report.excluded] == ["s4-volatility-compression-breakout"]
        _assert_every_runnable_produced_rows(report, namespaces=("in_sample",))

    def test_an_in_sample_row_without_its_whole_split_fails(self) -> None:
        """⚠ ``check_promotable`` adds no walk-forward refusal (sql/269: a code
        invented there would be a gate semantic with no source rule), so the run
        is what has to notice a validity gate that did not run."""
        runnable = ("s1", "s3")
        rows = list(self._rows(runnable))
        rows[0] = replace(rows[0], folds_written=FOLD_COUNT - 1)
        with pytest.raises(RuntimeError, match=r"stored 3 fold\(s\) against 4"):
            _assert_every_runnable_produced_rows(self._report(tuple(rows), runnable), namespaces=("in_sample",))

    def test_a_hold_out_row_carrying_folds_fails(self) -> None:
        """⚠ The other direction, and the one sql/269's trigger exists for: a
        fold row on a hold-out result claims a cross-validation of the withheld
        side that nobody ran."""
        runnable = ("s1",)
        rows = tuple(replace(row, namespace="hold_out", folds_written=FOLD_COUNT) for row in self._rows(runnable))
        with pytest.raises(RuntimeError, match=r"stored 4 fold\(s\) against 0"):
            _assert_every_runnable_produced_rows(self._report(rows, runnable), namespaces=("hold_out",))


class TestInSampleSplit:
    """Criterion 5's split, cut over one in-sample population.

    ⚠ The axis is eight equal-bar dates, which ``bar_weighted_folds`` cuts into
    four blocks of two. Transcribed from the construction rather than imported,
    so a change to the cut rule shows up here as a failure instead of being
    followed silently.
    """

    AXIS = tuple(date(2010, 1, day) for day in range(1, 9))
    BARS = (1,) * 8
    EDGES = ((0, 1), (2, 3), (4, 5), (6, 7))

    def test_the_axis_is_cut_into_four_contiguous_blocks_carrying_their_dates(self) -> None:
        split = build_in_sample_split([0, 2, 4, 6], [0, 2, 4, 6], axis=self.AXIS, bar_counts=self.BARS)
        assert [(r.fold.first_index, r.fold.last_index) for r in split.folds] == list(self.EDGES)
        # ⚠ The dates must describe the indices beside them: sql/269 stores both
        # because an index is unreadable once the corpus axis moves.
        assert [(r.first_date, r.last_date) for r in split.folds] == [
            (self.AXIS[lo], self.AXIS[hi]) for lo, hi in self.EDGES
        ]
        assert [r.bar_count for r in split.folds] == [2, 2, 2, 2]
        assert split.model_id == WALK_FORWARD_MODEL_ID

    def test_the_geometry_does_not_move_with_the_population(self) -> None:
        """⚠⚠ What makes criterion 9's two arms comparable at all.

        ``bar_weighted_folds`` reads only the axis, so the masked and admitted
        arms are cut at the same four boundaries and only their censuses differ.
        A geometry that moved with the arm would make every delta between them a
        comparison of differently-cut folds.
        """
        sparse = build_in_sample_split([0], [0], axis=self.AXIS, bar_counts=self.BARS)
        dense = build_in_sample_split(list(range(8)), list(range(8)), axis=self.AXIS, bar_counts=self.BARS)
        geometry = [
            (r.fold.first_index, r.fold.last_index, r.first_date, r.last_date, r.bar_count) for r in sparse.folds
        ]
        assert [
            (r.fold.first_index, r.fold.last_index, r.first_date, r.last_date, r.bar_count) for r in dense.folds
        ] == geometry
        assert sparse.observation_count == 1
        assert dense.observation_count == 8

    def test_every_fold_classifies_every_observation(self) -> None:
        """F1 — conservation. The only check that catches overlapping branches."""
        starts, ends = [0, 1, 2, 5, 6], [0, 4, 3, 5, 7]
        split = build_in_sample_split(starts, ends, axis=self.AXIS, bar_counts=self.BARS)
        for record in split.folds:
            assert record.census.total == len(starts)

    def test_an_observation_spanning_a_fold_is_purged_and_not_train(self) -> None:
        """⚠ Every price the fold owns lies inside that observation's label window."""
        # One observation, entering before fold 1 (dates 2-3) and closing after it.
        split = build_in_sample_split([0], [5], axis=self.AXIS, bar_counts=self.BARS)
        assert split.folds[1].census.purged == 1
        assert split.folds[1].census.train == 0

    def test_a_population_with_no_closed_observation_refuses(self) -> None:
        with pytest.raises(ValueError, match="no closed in-sample observation"):
            build_in_sample_split([], [], axis=self.AXIS, bar_counts=self.BARS)

    def test_mismatched_label_window_arrays_refuse(self) -> None:
        with pytest.raises(ValueError, match="label-window starts against"):
            build_in_sample_split([0, 1], [0], axis=self.AXIS, bar_counts=self.BARS)

    def test_fold_zero_can_carry_no_purged_observation(self) -> None:
        """⚠ STRUCTURAL, not a property of this corpus. Purging needs an
        observation starting BEFORE the fold, and nothing starts before index 0 —
        which is why §8's measured table reports ``purged`` 0 for fold 0 on both
        S-1 and S-3. Pinned so a future reader does not read that 0 as a corpus
        accident and 'fix' it.
        """
        split = build_in_sample_split([0, 2, 4, 6], [7, 7, 7, 7], axis=self.AXIS, bar_counts=self.BARS)
        assert split.folds[0].census.purged == 0
        # Only the observation entering at index 0 starts inside fold 0 (dates
        # 0-1); every later one starts outside it and cannot reach back.
        assert split.folds[0].census.test == 1

    def test_the_embargo_is_measured_off_the_post_purge_training_side(self) -> None:
        """⚠⚠ Ordering is the rule: purge, then measure the embargo, then census.

        Fold 1 is dates 2-3. Of the three observations, the spanning one is
        PURGED and must not set the embargo — measuring over the pre-purge
        candidates would read the length of a trade the fold's own prices
        resolved, which is the circularity ``training_embargo_bars`` is written
        to avoid.

        Measured off the training side the embargo is 1 (observation B's span);
        measured off everything it would be 5, and at 5 observation B would
        itself fall inside the embargo window and the fold would have no
        training data at all. So ``train == 1`` is what discriminates the two.
        """
        # A(0->5) spans fold 1 -> purged.   B(6->7) span 1 -> train.
        # C(4->4) span 0 -> train at measurement time, embargoed once the
        # measured embargo of 1 covers index 4.
        split = build_in_sample_split([0, 6, 4], [5, 7, 4], axis=self.AXIS, bar_counts=self.BARS)
        fold = split.folds[1]
        assert fold.embargo_bars == 1
        assert (fold.census.purged, fold.census.train, fold.census.embargoed, fold.census.test) == (1, 1, 1, 0)


class TestCutSplits:
    """One split per ``(strategy, quarantine arm)``, keyed by the arm it measured."""

    AXIS = tuple(date(2010, 1, day) for day in range(1, 9))

    def _corpus(self) -> _Corpus:
        return _Corpus(
            universe=(1,),
            axis=self.AXIS,
            axis_pos={when: index for index, when in enumerate(self.AXIS)},
            pairs=((1, 1),),
            in_sample_axis=self.AXIS,
            in_sample_bar_counts=(1,) * 8,
        )

    @staticmethod
    def _result(arm: str):  # noqa: ANN205 - StrategyResult, built by the module under test
        return build_result(
            _measurement(),
            strategy_id="s1",
            strategy_version="v1",
            purpose="capital_candidate",
            ambiguity_arm="best_case",
            quarantine_arm=arm,  # type: ignore[arg-type]
            deflated=None,
        )

    @staticmethod
    def _arm(arm: str, starts: list[int], ends: list[int]) -> ArmMeasurement:
        outcome = NamespaceMeasurement(
            namespace="in_sample",
            metrics=_metrics(),
            moments=TradeMoments(sharpe=0.05, skewness=0.2, kurtosis=4.0, trade_count=len(starts)),
            daily_returns={},
            evaluated_instrument_ids=frozenset({1}),
            position_count=len(starts),
            axis_first=date(2010, 1, 1),
            axis_last=date(2010, 1, 8),
            label_starts=array("i", starts),
            label_ends=array("i", ends),
        )
        return ArmMeasurement(
            strategy_id="s1",
            strategy_version="v1",
            quarantine_arm=arm,  # type: ignore[arg-type]
            namespaces={"in_sample": outcome},
            holdout_positions_discarded=0,
            close_sources={},
            series_evaluated=1,
            elapsed_s=0.0,
        )

    def test_each_arm_keeps_its_own_census(self) -> None:
        """⚠ The arms differ in POPULATION, so a split handed to the wrong row
        would report the other arm's leakage — and criterion 9's whole point is
        that the two are compared."""
        splits = backtest_run._cut_splits(  # noqa: SLF001 - the assembly under test
            (self._arm("masked", [0, 2], [1, 3]), self._arm("admitted", [0, 2, 4, 6], [1, 3, 5, 7])),
            corpus=self._corpus(),
        )
        assert sorted(splits) == [("s1", None, "admitted"), ("s1", None, "masked")]
        assert splits[("s1", None, "masked")].observation_count == 2
        assert splits[("s1", None, "admitted")].observation_count == 4
        # The geometry is shared; only the censuses moved.
        assert [r.fold.first_index for r in splits[("s1", None, "masked")].folds] == [
            r.fold.first_index for r in splits[("s1", None, "admitted")].folds
        ]

    def test_a_pending_in_sample_row_with_no_split_is_refused_before_any_insert(self) -> None:
        """⚠ The lookup itself sits INSIDE the per-pair transaction, so an
        uncovered row would surface only after that pair had been inserted —
        and #2423 records that folds cannot be attached to a row afterwards.
        Checked up front so the run refuses with zero rows written instead.
        """
        masked = self._result("masked")
        admitted = self._result("admitted")
        pending = [("s1", "in_sample", "best_case", masked, admitted)]
        splits = backtest_run._cut_splits(  # noqa: SLF001
            (self._arm("masked", [0, 2], [1, 3]),), corpus=self._corpus()
        )
        # Only the masked arm was cut, so the admitted row of the pair is uncovered.
        with pytest.raises(
            RuntimeError,
            match=r"no walk-forward split was cut for \[\('s1', 'best_case', 'admitted'\)\]",
        ):
            backtest_run._assert_every_in_sample_row_has_a_split(pending, splits)  # type: ignore[arg-type]  # noqa: SLF001

    def test_a_covered_pair_passes_and_a_hold_out_row_needs_no_split(self) -> None:
        masked = self._result("masked")
        admitted = self._result("admitted")
        splits = backtest_run._cut_splits(  # noqa: SLF001
            (self._arm("masked", [0, 2], [1, 3]), self._arm("admitted", [0, 4], [1, 5])),
            corpus=self._corpus(),
        )
        backtest_run._assert_every_in_sample_row_has_a_split(  # noqa: SLF001
            [("s1", "in_sample", "best_case", masked, admitted)],  # type: ignore[arg-type]
            splits,
        )
        # ⚠ A hold-out row is skipped, not refused — sql/269 has no folds for it.
        backtest_run._assert_every_in_sample_row_has_a_split(  # noqa: SLF001
            [("s9", "hold_out", "best_case", masked, admitted)],  # type: ignore[arg-type]
            {},
        )

    def test_two_measurements_of_one_arm_are_refused_rather_than_overwritten(self) -> None:
        """⚠ A plain assignment would be a SILENT last-write-wins, and the
        surviving split would describe a different population than the metrics
        on the rows it lands on. Unreachable from `run_backtest` today; refused
        because the failure would be invisible, not because it is likely."""
        with pytest.raises(RuntimeError, match="produced a second in-sample measurement"):
            backtest_run._cut_splits(  # noqa: SLF001
                (self._arm("masked", [0, 2], [1, 3]), self._arm("masked", [0, 4], [1, 5])),
                corpus=self._corpus(),
            )

    def test_a_hold_out_only_measurement_contributes_no_split(self) -> None:
        arm = self._arm("masked", [0, 2], [1, 3])
        holdout_only = ArmMeasurement(
            strategy_id="s1",
            strategy_version="v1",
            quarantine_arm="masked",
            namespaces={},
            holdout_positions_discarded=0,
            close_sources={},
            series_evaluated=1,
            elapsed_s=0.0,
        )
        assert backtest_run._cut_splits((holdout_only,), corpus=self._corpus()) == {}  # noqa: SLF001
        assert len(backtest_run._cut_splits((arm,), corpus=self._corpus())) == 1  # noqa: SLF001


class TestLabelWindowCollection:
    """The in-sample book accumulates criterion 5's label windows; nothing else does."""

    @staticmethod
    def _leg(book: _NamespaceBook, *, entry: int, exit_: int, realised: bool) -> None:
        book.add_leg(
            entry_index=entry,
            exit_index=exit_,
            entry_price=1.0,
            exit_price=1.1,
            half_spread=0.0,
            realised=realised,
            marks=[1.0] * (exit_ - entry + 1),
        )

    def test_a_hold_out_book_records_nothing(self) -> None:
        """⚠ ``walk_forward``: the hold-out is not an input to any function in
        that module and never becomes one — and sql/269's trigger refuses a fold
        row on a hold-out result, so the arrays would be memory nothing may read.
        """
        book = _NamespaceBook()
        self._leg(book, entry=0, exit_=3, realised=True)
        assert len(book.label_starts) == 0
        assert len(book.label_ends) == 0

    def test_an_in_sample_book_records_the_window_of_each_realised_leg(self) -> None:
        book = _NamespaceBook(records_label_windows=True)
        self._leg(book, entry=0, exit_=3, realised=True)
        self._leg(book, entry=5, exit_=9, realised=True)
        assert list(book.label_starts) == [0, 5]
        assert list(book.label_ends) == [3, 9]

    def test_an_unrealised_leg_contributes_no_label_window(self) -> None:
        """⚠ Its end index is a MARK bar, not a close: the label is unresolved,
        and feeding it to the embargo would report a span never realised."""
        book = _NamespaceBook(records_label_windows=True)
        self._leg(book, entry=0, exit_=3, realised=True)
        self._leg(book, entry=1, exit_=7, realised=False)
        assert list(book.label_starts) == [0]
        assert list(book.label_ends) == [3]


class TestFills:
    """Only ``fired`` rows carry a fill, and the two legs are split by kind."""

    @staticmethod
    def _row(kind: str, verdict: str) -> LedgerRow:
        filled = verdict == "fired"
        return LedgerRow(
            strategy_id="s1",
            strategy_version="v1",
            instrument_id=7,
            signal_bar_date=date(2010, 1, 4),
            signal_kind=kind,  # type: ignore[arg-type]
            verdict=verdict,  # type: ignore[arg-type]
            universe="survivor_only",
            input_rule_set_versions={"indicator_series": "indicator-v1"},
            not_evaluable_reason="no_fill_bar" if verdict == "not_evaluable" else None,
            fill_bar_date=date(2010, 1, 5) if filled else None,
            fill_price=Decimal("10.5") if filled else None,
        )

    def test_only_fired_rows_reach_the_builder(self) -> None:
        rows = [
            self._row("entry", "fired"),
            self._row("entry", "not_fired"),
            self._row("exit", "fired"),
            self._row("exit", "not_evaluable"),
        ]
        entries, exits = _fills(rows, 7)
        assert len(entries) == 1
        assert len(exits) == 1
        assert entries[0].fill_price == Decimal("10.5")
        assert exits[0].instrument_id == 7


class TestSeriesBreakBoundary:
    """Every strategy computation restarts at an unresolved transition."""

    def test_s4_restarts_state_and_warmup_after_a_break(self) -> None:
        dates = tuple(date(2020, 1, 1) + timedelta(days=index) for index in range(260))
        rows = tuple(
            {
                "open": Decimal(100 + index),
                "high": Decimal(101 + index),
                "low": Decimal(99 + index),
                "close": Decimal(100 + index),
            }
            for index in range(260)
        )
        series = BarSeries(dates=dates, rows=rows)  # type: ignore[arg-type]

        provider = _UniformRegimeProvider()
        entry = STRATEGY_MANIFEST["s4-volatility-compression-breakout"]
        whole = _signals_for(entry, series, instrument_id=1, ranking=None, regime_provider=provider)  # type: ignore[arg-type]
        segmented = _signals_for(
            entry,
            series,
            instrument_id=1,
            ranking=None,
            unresolved_breaks=(dates[150],),
            regime_provider=provider,  # type: ignore[arg-type]
        )
        assert whole[200].verdict == "fired"
        assert (segmented[149].verdict, segmented[149].reason) == ("not_evaluable", "no_fill_bar")
        assert (segmented[200].verdict, segmented[200].reason) == ("not_evaluable", "insufficient_warmup")

    def test_cross_sectional_state_also_restarts_after_a_break(self) -> None:
        dates = tuple(date(2020, 1, 1) + timedelta(days=index) for index in range(600))
        rows = tuple(
            {
                "open": Decimal(100 + index),
                "high": Decimal(101 + index),
                "low": Decimal(99 + index),
                "close": Decimal(100 + index),
            }
            for index in range(600)
        )
        series = BarSeries(dates=dates, rows=rows)  # type: ignore[arg-type]
        entry = STRATEGY_MANIFEST["s2-cross-sectional-momentum"]
        decision_dates = frozenset(dates)
        whole = segmented_member(
            entry,
            series,
            panel_decision_dates=decision_dates,
            universe="survivor_only",
            masked_reason="quarantined_bar",
            unresolved_breaks=(),
            regime=unconstrained_regime(len(series)),
        )
        segmented = segmented_member(
            entry,
            series,
            panel_decision_dates=decision_dates,
            universe="survivor_only",
            masked_reason="quarantined_bar",
            unresolved_breaks=(dates[300],),
            regime=unconstrained_regime(len(series)),
        )
        assert whole.verdicts[400] is None
        before_break = segmented.verdicts[299]
        after_break = segmented.verdicts[400]
        assert before_break is not None and after_break is not None
        assert (before_break.verdict, before_break.reason) == (
            "not_evaluable",
            "no_fill_bar",
        )
        assert (after_break.verdict, after_break.reason) == (
            "not_evaluable",
            "insufficient_warmup",
        )
