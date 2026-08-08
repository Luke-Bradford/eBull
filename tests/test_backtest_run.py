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
from datetime import date
from decimal import Decimal

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
    _assert_ambiguity_unreachable,
    _assert_every_runnable_produced_rows,
    _benchmark_book,
    _check_holdout_pairing,
    _Corpus,  # noqa: PLC2701 - the axis holder the namespace rule reads
    _expected_refusals,
    _fills,
    _measure_namespace,
    _NamespaceBook,
    _shifted,
    assert_no_existing_results,
    build_result,
    deflate_group,
    runnable_strategies,
)
from app.services.cost_model import COST_MODEL_ID
from app.services.deflated_sharpe import DSR_MODEL_ID, TradeMoments
from app.services.equity_curve import BENCHMARK_RULE_ID, SIZING_RULE_ID, LegBook
from app.services.position_builder import RULE_SET_VERSION as POSITION_RULE_SET_VERSION
from app.services.research_price_structure_store import QUARANTINE_ARMS, QUARANTINE_RULE_SET_VERSION
from app.services.signal_ledger import LedgerRow
from app.services.strategy_result import (
    AMBIGUITY_ARMS,
    CORPUS_VERSION,
    EVALUATION_WINDOW_END,
    EVALUATION_WINDOW_START,
    HOLDOUT_BOUNDARY,
    ResultIdentity,
)
from app.services.strategy_statistics import StrategyMetrics
from app.services.trial_register import TRIAL_REGISTER
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
    """§3 — S-4 is refused with the builder's own message, never skipped."""

    def test_three_runnable_and_s4_excluded_with_the_builder_message(self) -> None:
        runnable, excluded = runnable_strategies()
        assert list(runnable) == [
            "s1-time-series-momentum",
            "s2-cross-sectional-momentum",
            "s3-mean-reversion-in-trend",
        ]
        assert [entry.strategy_id for entry in excluded] == ["s4-volatility-compression-breakout"]
        # ⚠ The message is the RAISE's, demonstrated by calling the builder —
        # not a paraphrase, which would go stale silently the day the rule moves.
        assert "level-based entry with no outcome" in excluded[0].reason

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
        monkeypatch.setattr(backtest_run, "_demonstrate_level_refusal", lambda entry, regime: None)
        with pytest.raises(RuntimeError, match="did NOT refuse"):
            runnable_strategies()


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

    def test_full_holdout_run_with_a_dsr_leaves_only_the_three_standing_refusals(self) -> None:
        assert _expected_refusals(holdout_requested=True, deflated=True) == STANDING_REFUSALS

    def test_in_sample_run_adds_holdout_never_evaluated(self) -> None:
        assert _expected_refusals(holdout_requested=False, deflated=True) == STANDING_REFUSALS | {
            "holdout_never_evaluated"
        }

    def test_no_dsr_adds_both_criterion_6_refusals(self) -> None:
        """⚠ TWO codes, not one. A DSR with no trial count is as refused as no
        DSR at all, and collapsing them would make "which of the two is missing"
        unanswerable."""
        assert _expected_refusals(holdout_requested=True, deflated=False) == STANDING_REFUSALS | {
            "deflated_sharpe_not_computed",
            "trial_count_undeclared",
        }

    def test_the_three_standing_refusals_are_the_ones_this_cut_cannot_close(self) -> None:
        assert STANDING_REFUSALS == {
            "universe_basis_not_survivorship_free",
            "carry_unmodelled",
            "synthetic_control_not_run",
        }


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

        ``verify_2240_statistics.py``'s P11 comment says ``np.corrcoef``
        returns NaN for a constant series. Measured on **numpy 2.4.4**
        (2026-08-08) it returns a finite **0.0** — pinned below — so an
        ``isfinite`` guard is dead code and the trial would be read as
        UNCORRELATED, pushing the implied independent trial count toward ``M``
        on evidence that does not exist.
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
            ambiguity_arm="worst_case",
            quarantine_arm="masked",
            deflated=None,
        )
        identity = result.identity
        assert identity.result_scope == "sleeve"
        assert identity.sizing_rule == SIZING_RULE_ID
        assert identity.cost_model_id == COST_MODEL_ID
        assert identity.corpus_version == CORPUS_VERSION
        assert identity.window_start == EVALUATION_WINDOW_START
        assert identity.window_end == EVALUATION_WINDOW_END
        assert identity.position_rule_set_version == POSITION_RULE_SET_VERSION
        # ⚠⚠ THE QUARANTINE RULE SET, not StrategyIdentity.input_rule_set_versions
        # (indicator-only, and already inside strategy_version — folding it in
        # here would hash it twice).
        assert identity.input_rule_set_version == QUARANTINE_RULE_SET_VERSION
        assert result.universe_basis == "survivor_only"
        assert result.evaluated_instrument_count == 2

    def test_no_dsr_leaves_both_criterion_6_scalars_null(self) -> None:
        """``sql/266``'s CHECK is all-or-nothing across twelve columns, so a
        declared trial count beside a null DSR is a row the table refuses."""
        result = build_result(
            _measurement(),
            strategy_id="s1-time-series-momentum",
            strategy_version="strategy-v1+abc",
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
            "ambiguity_arm": "worst_case",
            "deflated": None,
        }
        masked = build_result(_measurement(), quarantine_arm="masked", **common)  # type: ignore[arg-type]
        admitted = build_result(_measurement(), quarantine_arm="admitted", **common)  # type: ignore[arg-type]
        assert masked.identity.version != admitted.identity.version
        from dataclasses import replace

        assert replace(masked.identity, quarantine_arm="admitted") == admitted.identity


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
            closes_by_instrument=self._closes(),
            lo=0,
            hi=9,
        )
        assert len(book) == 1

    def test_a_leg_is_clipped_to_the_axis_rather_than_dropped(self) -> None:
        book = _benchmark_book(
            instruments=frozenset({1, 2}),
            closes_by_instrument=self._closes(),
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
            closes_by_instrument=self._closes(),
            lo=7,
            hi=9,
        )
        assert len(book) == 1

    def test_a_single_usable_bar_is_not_a_round_trip(self) -> None:
        book = _benchmark_book(
            instruments=frozenset({1}),
            closes_by_instrument={1: (0, array("d", [10.0, math.nan, math.nan]))},
            lo=0,
            hi=2,
        )
        assert len(book) == 0

    def test_the_benchmark_is_charged_the_same_cost_model(self) -> None:
        """A cost-free benchmark would make every strategy look worse by exactly
        the amount the cost model charges — a comparison of cost models."""
        book = _benchmark_book(
            instruments=frozenset({1}),
            closes_by_instrument=self._closes(),
            lo=0,
            hi=5,
        )
        assert book.entry_price[0] > 10.0
        assert book.exit_price[0] < 15.0
        assert book.half_spread[0] > 0.0


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
                closes_by_instrument={},
            )

    def test_a_namespace_with_no_positions_measures_nothing(self) -> None:
        axis = (date(2021, 6, 25), date(2021, 6, 28))
        assert (
            _measure_namespace("in_sample", _NamespaceBook(), corpus=self._corpus(axis), closes_by_instrument={})
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
        outcome = _measure_namespace(
            "in_sample",
            book,
            corpus=self._corpus(axis),
            closes_by_instrument={1: (3, array("d", [1.0, 1.1, 1.15, 1.2]))},
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
        with pytest.raises(RuntimeError, match="no effective sample size"):
            _measure_namespace(
                "in_sample",
                book,
                corpus=self._corpus(axis),
                closes_by_instrument={1: (3, array("d", [1.0, 1.1, 1.15, 1.2]))},
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
        _assert_ambiguity_unreachable(self._arm({"signal_pair": 10, "open_at_window_end": 1}))

    def test_one_ambiguous_close_falsifies_the_single_measurement_claim(self) -> None:
        with pytest.raises(RuntimeError, match="ambiguity arms are one measurement is falsified"):
            _assert_ambiguity_unreachable(self._arm({"signal_pair": 10, "ambiguous": 1}))


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
