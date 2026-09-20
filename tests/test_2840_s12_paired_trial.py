"""S-H arm 2's paired acceptance test — the statistic the amended contract declares.

Pure tier: no database, no corpus, no ledger. Refs #2840.
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pytest

from app.services.block_bootstrap import CONFIDENCE, MIN_CLUSTERS, cluster_by_date, optimal_block_length
from app.services.strategy_s12_paired_trial import (
    DECLARED_ARMS,
    MIN_SUPPORTING_CLUSTERS,
    RESAMPLES,
    PairedBooks,
    S12PairedTrialRefused,
    evaluate_arm,
    evaluate_s12_paired_trial,
    portfolio_returns,
)

_START = date(2000, 1, 3)
_AXIS = 250


def _dates(count: int) -> tuple[date, ...]:
    return tuple(_START + timedelta(days=index) for index in range(count))


def _equity(returns: np.ndarray) -> np.ndarray:
    return np.concatenate((np.asarray([1.0]), np.cumprod(1.0 + returns)))


def _clusters(days, returns):
    return cluster_by_date([float(value) for value in returns], list(days))


def _books(
    *,
    arm: str = "masked",
    treatment_daily: np.ndarray,
    control_daily: np.ndarray,
    treatment_trades: np.ndarray | None = None,
    control_trades: np.ndarray | None = None,
) -> PairedBooks:
    days = _dates(len(treatment_daily) + 1)
    trade_days = days[1:]
    treatment_trades = treatment_daily if treatment_trades is None else treatment_trades
    control_trades = control_daily if control_trades is None else control_trades
    return PairedBooks(
        arm=arm,
        dates=days,
        treatment_equity=_equity(treatment_daily),
        control_equity=_equity(control_daily),
        treatment_clusters=_clusters(trade_days[: len(treatment_trades)], treatment_trades),
        control_clusters=_clusters(trade_days[: len(control_trades)], control_trades),
    )


def _noise(seed: int, size: int = _AXIS, scale: float = 0.01) -> np.ndarray:
    return np.random.default_rng(seed).normal(0.0, scale, size)


class TestTheFrozenConstruction:
    def test_the_block_length_is_the_selectors_output_on_the_difference_series(self) -> None:
        # ⚠ The first cut PINNED the block at MAX_HOLD_BARS and called that a
        # derivation. It is not, and a fixed block under-states the long-run
        # variance. ⚠⚠ An earlier version of THIS test claimed to compare a
        # serially dependent series against a white-noise one and did not: it put
        # the serial component in BOTH books, where the difference cancels it. The
        # assertion now pins the evaluator's block to the selector's own output on
        # the series the evaluator declares it measures.
        treatment, control = _noise(1) + 0.004, _noise(2)
        verdict = evaluate_arm(_books(arm="masked", treatment_daily=treatment, control_daily=control))
        expected = optimal_block_length(
            portfolio_returns(_equity(treatment), label="t") - portfolio_returns(_equity(control), label="c")
        )
        assert verdict.selector_block_length == expected
        assert verdict.block_length == expected

    def test_a_difference_selected_block_can_disagree_with_the_treatments_own(self) -> None:
        # ⚠⚠ The declared transfer's known weakness, asserted rather than only
        # described: common serial dependence CANCELS in the difference, so the
        # block protecting the difference leg can be far shorter than the one the
        # treatment series alone would ask for — and the own-return leg is then
        # under-blocked. The contract declares this; the test stops it being
        # quietly "fixed" without the contract moving.
        serial = np.cumsum(_noise(3, scale=0.003))
        treatment, control = serial + 0.004, serial
        verdict = evaluate_arm(_books(arm="masked", treatment_daily=treatment, control_daily=control))
        treatment_own = optimal_block_length(portfolio_returns(_equity(treatment), label="t"))
        assert verdict.block_length < treatment_own

    def test_the_only_declared_sample_floor_is_the_published_one(self) -> None:
        # ⚠ There is deliberately no 170. The first cut inverted a selector's
        # tuning cap into an axis floor, which is a design choice wearing a
        # theorem's clothes; no sample size certifying tail coverage is
        # derivable here and none is invented.
        assert MIN_SUPPORTING_CLUSTERS == MIN_CLUSTERS == 2


class TestTheConjunction:
    def test_a_treatment_that_dominates_on_both_axes_passes(self) -> None:
        control = _noise(11)
        verdict = evaluate_arm(_books(treatment_daily=control + 0.004, control_daily=control))
        assert verdict.portfolio_own.positive
        assert verdict.portfolio_difference.positive
        assert verdict.expectancy_own.positive
        assert verdict.expectancy_difference.positive
        assert verdict.passes

    def test_beating_the_control_while_losing_money_still_fails(self) -> None:
        control = np.full(_AXIS, -0.006) + _noise(12, scale=0.002)
        verdict = evaluate_arm(_books(treatment_daily=control + 0.003, control_daily=control))
        assert verdict.portfolio_difference.positive
        assert not verdict.portfolio_own.positive
        assert not verdict.passes

    def test_making_money_without_beating_the_control_fails(self) -> None:
        control = np.full(_AXIS, 0.006) + _noise(13, scale=0.002)
        verdict = evaluate_arm(_books(treatment_daily=control - 0.003, control_daily=control))
        assert verdict.portfolio_own.positive
        assert not verdict.portfolio_difference.positive
        assert not verdict.passes

    def test_either_expectancy_leg_alone_can_fail_the_arm(self) -> None:
        # ⚠ Finding 46: the earlier single case failed BOTH expectancy legs, so
        # deleting either one from the decision left it green. These isolate them.
        control = _noise(40)
        healthy_portfolio = {"treatment_daily": control + 0.004, "control_daily": control}
        own_only = evaluate_arm(
            _books(
                **healthy_portfolio,
                treatment_trades=np.full(_AXIS, -0.2) + _noise(41, scale=0.05),
                control_trades=np.full(_AXIS, -0.9) + _noise(42, scale=0.05),
            )
        )
        assert not own_only.expectancy_own.positive
        assert own_only.expectancy_difference.positive
        assert not own_only.passes

        difference_only = evaluate_arm(
            _books(
                **healthy_portfolio,
                treatment_trades=np.full(_AXIS, 0.2) + _noise(43, scale=0.05),
                control_trades=np.full(_AXIS, 0.9) + _noise(44, scale=0.05),
            )
        )
        assert difference_only.expectancy_own.positive
        assert not difference_only.expectancy_difference.positive
        assert not difference_only.passes

    def test_both_expectancy_conjuncts_failing_also_fails_the_arm(self) -> None:
        # ⚠⚠ THE TEST THE THIRD CHECKPOINT PASS SAID WAS MISSING (finding 54).
        # Without it, dropping the expectancy legs from ``passes`` would leave
        # the whole suite green while the contract's decision metric stopped
        # deciding anything. The two axes are separate inputs, so a book can be
        # healthy on the portfolio path and negative per trade.
        control = _noise(14)
        verdict = evaluate_arm(
            _books(
                treatment_daily=control + 0.004,
                control_daily=control,
                treatment_trades=np.full(_AXIS, -0.4) + _noise(15, scale=0.05),
                control_trades=np.full(_AXIS, 0.4) + _noise(16, scale=0.05),
            )
        )
        assert verdict.portfolio_own.positive
        assert verdict.portfolio_difference.positive
        assert not verdict.expectancy_own.positive
        assert not verdict.expectancy_difference.positive
        assert not verdict.passes

    def test_one_failing_arm_fails_the_trial(self) -> None:
        control = _noise(17)
        good = _books(arm="masked", treatment_daily=control + 0.004, control_daily=control)
        bad = _books(arm="admitted", treatment_daily=control - 0.004, control_daily=control)
        result = evaluate_s12_paired_trial([good, bad])
        assert result.arms[0].passes
        assert not result.arms[1].passes
        assert not result.conjuncts_pass


class TestTheArmSetIsTheDeclaredOne:
    """⚠ Finding 1: the first cut checked only that the names were distinct."""

    def test_no_arm_is_refused(self) -> None:
        with pytest.raises(S12PairedTrialRefused, match="requires both"):
            evaluate_s12_paired_trial([])

    def test_a_single_declared_arm_is_refused(self) -> None:
        control = _noise(18)
        only_masked = _books(arm="masked", treatment_daily=control + 0.004, control_daily=control)
        with pytest.raises(S12PairedTrialRefused, match="requires both"):
            evaluate_s12_paired_trial([only_masked])

    def test_two_arms_under_invented_names_are_refused(self) -> None:
        control = _noise(19)
        with pytest.raises(S12PairedTrialRefused, match="requires both"):
            evaluate_s12_paired_trial(
                [
                    _books(arm="masked", treatment_daily=control + 0.004, control_daily=control),
                    _books(arm="best_case", treatment_daily=control + 0.004, control_daily=control),
                ]
            )

    def test_a_repeated_arm_is_refused(self) -> None:
        control = _noise(20)
        book = _books(arm="masked", treatment_daily=control + 0.004, control_daily=control)
        with pytest.raises(S12PairedTrialRefused, match="requires both"):
            evaluate_s12_paired_trial([book, book])

    def test_the_declared_arms_are_the_contracts_two(self) -> None:
        assert DECLARED_ARMS == ("masked", "admitted")


class TestThePairingIsReal:
    """⚠⚠ The construction's whole claim is that the covariance is captured.

    A test that only checked "the bound is positive when the effect is large"
    would pass just as well against two independently resampled means, which is
    the defect the construction exists to avoid.
    """

    def test_a_constant_advantage_has_a_degenerate_difference_interval(self) -> None:
        control = _noise(21, scale=0.02)
        verdict = evaluate_arm(_books(treatment_daily=control + 0.005, control_daily=control))
        assert verdict.portfolio_difference.lower == pytest.approx(0.005, abs=1e-12)
        assert verdict.portfolio_difference.high == pytest.approx(0.005, abs=1e-12)
        assert verdict.portfolio_own.high - verdict.portfolio_own.lower > 1e-4

    def test_an_unpaired_construction_cannot_reproduce_that_collapse(self) -> None:
        # ⚠ Finding 50: the previous version asserted a width ratio without ever
        # BUILDING the unpaired alternative. Here it is built — two independent
        # resamples of the same two series, differenced afterwards — and its
        # interval is wide where the paired one is a point.
        control = _noise(22, scale=0.02)
        treatment = control + 0.005
        verdict = evaluate_arm(_books(treatment_daily=treatment, control_daily=control))
        generator = np.random.default_rng(999)
        unpaired = np.asarray(
            [
                treatment[generator.integers(0, len(treatment), len(treatment))].mean()
                - control[generator.integers(0, len(control), len(control))].mean()
                for _ in range(400)
            ]
        )
        unpaired_width = float(np.percentile(unpaired, 97.5) - np.percentile(unpaired, 2.5))
        paired_width = verdict.portfolio_difference.high - verdict.portfolio_difference.lower
        assert paired_width == pytest.approx(0.0, abs=1e-12)
        assert unpaired_width > 1e-3

    def test_the_same_input_gives_the_same_bounds(self) -> None:
        control = _noise(23)
        book = _books(treatment_daily=control + 0.002, control_daily=control)
        first, second = evaluate_arm(book), evaluate_arm(book)
        assert first.portfolio_difference == second.portfolio_difference
        assert first.expectancy_difference == second.expectancy_difference
        assert first.block_length == second.block_length


class TestTheExpectancyAxis:
    def test_the_estimator_is_trade_weighted_and_not_a_mean_of_date_means(self) -> None:
        # ⚠⚠ Finding 51: every earlier fixture put ONE trade on each date, under
        # which a wrongly equal-weighted cluster mean is indistinguishable from
        # the ratio estimator. This fixture is deliberately unbalanced.
        control_daily = _noise(24)
        days = _dates(_AXIS + 1)[1:]
        heavy_day, light_day = days[0], days[1]
        trade_dates = [heavy_day] * 20 + [light_day] + list(days[2:])
        trade_returns = [0.1] * 20 + [8.0] + [0.1] * (len(days) - 2)
        verdict = evaluate_arm(
            PairedBooks(
                arm="masked",
                dates=_dates(_AXIS + 1),
                treatment_equity=_equity(control_daily + 0.003),
                control_equity=_equity(control_daily),
                treatment_clusters=_clusters(trade_dates, trade_returns),
                control_clusters=_clusters(days, np.zeros(len(days))),
            )
        )
        pooled = sum(trade_returns) / len(trade_returns)
        date_means = np.mean([0.1, 8.0, *([0.1] * (len(days) - 2))])
        assert verdict.expectancy_own.point == pytest.approx(pooled)
        assert verdict.expectancy_own.point != pytest.approx(date_means)

    def test_a_control_only_date_stays_in_the_controls_own_estimand(self) -> None:
        control_daily = _noise(25)
        days = _dates(_AXIS + 1)[1:]
        control_trades = np.random.default_rng(26).normal(0.5, 1.0, _AXIS)
        treatment_trades = control_trades[::2] + 1.0
        verdict = evaluate_arm(
            PairedBooks(
                arm="masked",
                dates=_dates(_AXIS + 1),
                treatment_equity=_equity(control_daily + 0.003),
                control_equity=_equity(control_daily),
                treatment_clusters=_clusters(days[::2], treatment_trades),
                control_clusters=_clusters(days, control_trades),
            )
        )
        assert verdict.expectancy_clusters == _AXIS
        assert verdict.expectancy_difference.point == pytest.approx(
            float(np.mean(treatment_trades)) - float(np.mean(control_trades))
        )


class TestTheRefusals:
    def test_a_starved_treatment_is_refused_before_any_draw(self) -> None:
        # ⚠ Finding 6: the union axis was dense, so a six-trade treatment passed
        # every earlier size check. The support that matters is each book's own.
        control_daily = _noise(27)
        days = _dates(_AXIS + 1)[1:]
        with pytest.raises(S12PairedTrialRefused, match="supported by 1 decision date"):
            evaluate_arm(
                PairedBooks(
                    arm="masked",
                    dates=_dates(_AXIS + 1),
                    treatment_equity=_equity(control_daily + 0.003),
                    control_equity=_equity(control_daily),
                    treatment_clusters=_clusters([days[5]], [0.5]),
                    control_clusters=_clusters(days, np.zeros(len(days))),
                )
            )

    def test_a_degenerate_marginal_is_refused_though_a_degenerate_difference_is_not(self) -> None:
        # ⚠ Finding 7. A constant treatment book has no marginal sampling
        # variability at all — that is an unsampled marginal, not a certainty.
        control_daily = _noise(28)
        days = _dates(_AXIS + 1)[1:]
        with pytest.raises(S12PairedTrialRefused, match="zero variance across replications"):
            evaluate_arm(
                PairedBooks(
                    arm="masked",
                    dates=_dates(_AXIS + 1),
                    treatment_equity=_equity(np.full(_AXIS, 0.001)),
                    control_equity=_equity(control_daily),
                    treatment_clusters=_clusters(days, np.full(len(days), 0.5)),
                    control_clusters=_clusters(days, np.zeros(len(days))),
                )
            )

    def test_a_cluster_date_off_the_shared_axis_is_refused(self) -> None:
        control_daily = _noise(29)
        days = _dates(_AXIS + 1)
        stray = days[-1] + timedelta(days=400)
        with pytest.raises(S12PairedTrialRefused, match="absent from the shared axis"):
            evaluate_arm(
                PairedBooks(
                    arm="masked",
                    dates=days,
                    treatment_equity=_equity(control_daily + 0.003),
                    control_equity=_equity(control_daily),
                    treatment_clusters=_clusters([*days[1:], stray], np.zeros(_AXIS + 1) + 0.5),
                    control_clusters=_clusters(days[1:], np.zeros(_AXIS)),
                )
            )

    @pytest.mark.parametrize(
        ("mangle", "message"),
        [
            (lambda days: tuple(reversed(days)), "not ascending"),
            (lambda days: (days[0],) * len(days), "repeats a date"),
        ],
    )
    def test_an_axis_that_is_not_an_axis_is_refused(self, mangle, message) -> None:
        control = _noise(30)
        book = _books(treatment_daily=control + 0.002, control_daily=control)
        with pytest.raises(S12PairedTrialRefused, match=message):
            evaluate_arm(
                PairedBooks(
                    arm=book.arm,
                    dates=mangle(book.dates),
                    treatment_equity=book.treatment_equity,
                    control_equity=book.control_equity,
                    treatment_clusters=book.treatment_clusters,
                    control_clusters=book.control_clusters,
                )
            )

    def test_a_ruined_equity_path_is_refused_not_divided_through(self) -> None:
        with pytest.raises(S12PairedTrialRefused, match="zero or below"):
            portfolio_returns(np.linspace(1.0, -0.5, _AXIS + 1), label="treatment")

    def test_a_non_finite_mark_is_refused(self) -> None:
        equity = np.ones(_AXIS + 1)
        equity[7] = np.nan
        with pytest.raises(S12PairedTrialRefused, match="non-finite"):
            portfolio_returns(equity, label="treatment")

    def test_a_scalar_path_is_refused_by_shape_and_not_by_TypeError(self) -> None:
        # ⚠ Finding 10: the refusal message itself must not raise.
        with pytest.raises(S12PairedTrialRefused, match="one-dimensional"):
            portfolio_returns(np.asarray(1.0), label="treatment")

    def test_two_books_on_different_length_axes_are_refused(self) -> None:
        control = _noise(31)
        book = _books(treatment_daily=control + 0.002, control_daily=control)
        with pytest.raises(S12PairedTrialRefused, match="one shared axis"):
            evaluate_arm(
                PairedBooks(
                    arm=book.arm,
                    dates=book.dates,
                    treatment_equity=book.treatment_equity[:-1],
                    control_equity=book.control_equity,
                    treatment_clusters=book.treatment_clusters,
                    control_clusters=book.control_clusters,
                )
            )


class TestTheReportedShape:
    def test_the_frozen_constants_are_the_declared_ones(self) -> None:
        assert CONFIDENCE == 0.95
        assert RESAMPLES == 10_000

    def test_the_block_length_never_exceeds_either_axis(self) -> None:
        control = _noise(32)
        verdict = evaluate_arm(_books(treatment_daily=control + 0.002, control_daily=control))
        assert 1 <= verdict.block_length <= verdict.portfolio_observations
        assert verdict.block_length <= verdict.expectancy_clusters


class TestTheFalsePassPathIsClosed:
    """⚠⚠ The concrete false pass the third checkpoint pass reproduced.

    A circular block as long as its own axis draws every observation exactly
    once in every replication, so the statistic is constant and the interval
    collapses onto the point estimate — and any positive point estimate then
    passes automatically. The first cut clipped with
    ``min(selector, portfolio_n, cluster_n)`` and relied on a ``lower == high``
    check to catch the result, which summation-order noise escaped at a width of
    2.2e-16. The guard is now structural.
    """

    @staticmethod
    def _sparse_cluster_arm(seed: int, trade_dates: int) -> PairedBooks:
        """Healthy dense portfolio axis, deliberately tiny cluster axis.

        ⚠ The drift is serially dependent so the SELECTOR returns a long block
        (28 on this fixture), which the cluster axis of ``trade_dates`` cannot
        support. The equity paths vary, so the portfolio guards are satisfied and
        the cluster-axis refusal is the one under test.
        """
        generator = np.random.default_rng(seed)
        control = generator.normal(0.0, 0.01, _AXIS)
        treatment = control + np.cumsum(generator.normal(0.0, 0.001, _AXIS))
        days = _dates(_AXIS + 1)[1:]
        chosen = [days[index * 5] for index in range(trade_dates)]
        return PairedBooks(
            arm="masked",
            dates=_dates(_AXIS + 1),
            treatment_equity=_equity(treatment),
            control_equity=_equity(control),
            treatment_clusters=_clusters(chosen, generator.normal(0.5, 0.2, trade_dates)),
            control_clusters=_clusters(chosen, generator.normal(0.0, 0.2, trade_dates)),
        )

    def test_a_block_as_long_as_the_cluster_axis_is_refused(self) -> None:
        with pytest.raises(S12PairedTrialRefused, match="draws the whole axis in every replication"):
            evaluate_arm(self._sparse_cluster_arm(60, 3))

    def test_the_guard_is_structural_and_not_a_width_tolerance(self) -> None:
        # ⚠ The refusal must not depend on the collapsed interval being exactly
        # zero-width — that is the property float noise defeated. It names the
        # CONFIGURATION, and it fires before any bound is computed.
        with pytest.raises(S12PairedTrialRefused) as excinfo:
            evaluate_arm(self._sparse_cluster_arm(61, 2))
        message = str(excinfo.value)
        assert "cluster axis holds 2 observation(s)" in message
        assert "zero variance" not in message


class TestTheNumericalEdges:
    def test_a_scalar_path_is_refused_by_the_public_evaluator_too(self) -> None:
        # ⚠ Finding 27: ``_validate_axis`` called ``len()`` before any shape
        # check, so a scalar escaped the refusal protocol as a raw TypeError. The
        # earlier test exercised only the helper, which checks its own shape.
        control = _noise(62)
        book = _books(treatment_daily=control + 0.002, control_daily=control)
        with pytest.raises(S12PairedTrialRefused, match="one-dimensional"):
            evaluate_arm(
                PairedBooks(
                    arm=book.arm,
                    dates=book.dates,
                    treatment_equity=np.asarray(1.0),
                    control_equity=book.control_equity,
                    treatment_clusters=book.treatment_clusters,
                    control_clusters=book.control_clusters,
                )
            )

    def test_ruin_by_underflow_is_refused_though_both_marks_are_positive(self) -> None:
        # ⚠ Finding 33: 1e300 -> 1e-300 is a finite positive pair whose ratio is
        # a total loss. The positivity check alone passes it.
        with pytest.raises(S12PairedTrialRefused, match="at or below -100%"):
            portfolio_returns(np.asarray([1e300, 1e-300, 1.0]), label="treatment")

    def test_a_fractional_cluster_count_is_refused_not_truncated(self) -> None:
        # ⚠ Finding 30: ``DateClusters`` accepts a count of 1.5 and the union
        # builder silently truncated it to 1, moving a supplied pooled mean of
        # 0.30 to a reported 0.45.
        control_daily = _noise(63)
        days = _dates(_AXIS + 1)[1:]
        clusters = _clusters(days, np.full(len(days), 0.3))
        # ⚠ Two entries moved so the TOTAL still matches ``trade_count`` — otherwise
        # the count-total refusal fires first and this case never reaches the
        # integrality check it exists for.
        fractional = clusters.trade_counts.astype(np.float64)
        fractional[0], fractional[1] = 1.5, 0.5
        object.__setattr__(clusters, "trade_counts", fractional)
        with pytest.raises(S12PairedTrialRefused, match="whole number of"):
            evaluate_arm(
                PairedBooks(
                    arm="masked",
                    dates=_dates(_AXIS + 1),
                    treatment_equity=_equity(control_daily + 0.003),
                    control_equity=_equity(control_daily),
                    treatment_clusters=clusters,
                    control_clusters=_clusters(days, np.zeros(len(days))),
                )
            )

    def test_a_degenerate_portfolio_marginal_is_caught_on_its_own(self) -> None:
        # ⚠ Finding 49: the existing degeneracy test made the EXPECTANCY leg
        # constant too, so removing the portfolio guard left it green. Here the
        # trade book varies and only the equity path is flat.
        control_daily = _noise(64)
        days = _dates(_AXIS + 1)[1:]
        with pytest.raises(S12PairedTrialRefused, match="portfolio own"):
            evaluate_arm(
                PairedBooks(
                    arm="masked",
                    dates=_dates(_AXIS + 1),
                    treatment_equity=_equity(np.full(_AXIS, 0.001)),
                    control_equity=_equity(control_daily),
                    treatment_clusters=_clusters(days, np.full(len(days), 0.5) + _noise(65, scale=0.1)),
                    control_clusters=_clusters(days, _noise(66, scale=0.1)),
                )
            )

    def test_the_result_carries_the_evidence_a_reader_needs(self) -> None:
        # ⚠ Finding 35: without the raw selector output and the per-strategy
        # support, several distinct failures are indistinguishable in the result.
        control = _noise(67)
        verdict = evaluate_arm(_books(treatment_daily=control + 0.002, control_daily=control))
        assert verdict.selector_block_length >= verdict.block_length
        assert verdict.treatment_supporting_dates == _AXIS
        assert verdict.control_supporting_dates == _AXIS
        assert verdict.portfolio_own.replication_variance > 0.0

    def test_a_mutated_cluster_count_total_is_refused(self) -> None:
        # ⚠⚠ Codex checkpoint 2, P2. ``DateClusters`` is frozen around WRITABLE
        # arrays, so its constructor checks describe construction and nothing
        # later. Mutating one count to 101 leaves ``trade_count`` at 250 while the
        # array sums to 350 — the pooled point estimate moved 0.50 -> 0.357 and the
        # arm still PASSED.
        control = _noise(68)
        days = _dates(_AXIS + 1)[1:]
        clusters = _clusters(days, np.full(len(days), 0.5))
        clusters.trade_counts[0] = 101
        with pytest.raises(S12PairedTrialRefused, match="different nominal counts"):
            evaluate_arm(
                PairedBooks(
                    arm="masked",
                    dates=_dates(_AXIS + 1),
                    treatment_equity=_equity(control + 0.004),
                    control_equity=_equity(control),
                    treatment_clusters=clusters,
                    control_clusters=_clusters(days, np.zeros(len(days))),
                )
            )
