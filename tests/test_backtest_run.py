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
from dataclasses import replace
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
    build_in_sample_split,
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
            _measure_namespace("in_sample", book, corpus=self._corpus(axis), closes_by_instrument={})

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
        assert sorted(splits) == [("s1", "admitted"), ("s1", "masked")]
        assert splits[("s1", "masked")].observation_count == 2
        assert splits[("s1", "admitted")].observation_count == 4
        # The geometry is shared; only the censuses moved.
        assert [r.fold.first_index for r in splits[("s1", "masked")].folds] == [
            r.fold.first_index for r in splits[("s1", "admitted")].folds
        ]

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
