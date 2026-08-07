"""Phase 5e-3 — criterion 6's Deflated Sharpe (#2240).

⚠⚠ THE REFERENCE ARM QUOTES THE PAPER, IT DOES NOT IMPORT OUR CONSTANTS.

Every number in ``TestPaperNumericalExample`` is a ``PAPER_*`` literal
transcribed from Bailey & López de Prado (2014), SSRN 2460551 — the worked
example on pp. 9-10 and its two follow-ups. None is read back from
``app.services.deflated_sharpe``. That is the prevention-log lesson from
2026-08-06: *"a reference that IMPORTS the constant it validates is a
tautology"* — it would pass against any implementation, including one that
returned its input.
"""

from __future__ import annotations

import math

import numpy as np
import pytest

from app.services.deflated_sharpe import (
    DSR_MODEL_ID,
    MIN_MEASURED_TRIALS,
    DeflatedSharpeResult,
    TradeMoments,
    average_trial_correlation,
    deflated_sharpe,
    expected_max_sharpe,
    implied_independent_trials,
    trade_moments,
)

# --- the paper's worked example, transcribed ------------------------------
#: "The analyst responds that N=100, V[{SR_n}]=1/2, T=1250, y3=-3 and y4=10",
#: against an annualised SR of 2.5 over "a daily sample of 5 years", i.e. 250
#: observations per year.
PAPER_TRIALS = 100
PAPER_SR_VARIANCE_ANNUALISED = 0.5
PAPER_SAMPLE_LENGTH = 1250
PAPER_SKEWNESS = -3.0
PAPER_KURTOSIS = 10.0
PAPER_SHARPE_ANNUALISED = 2.5
PAPER_OBSERVATIONS_PER_YEAR = 250

#: "SR_0 = ... ~ 0.1132, non-annualized (with 250 observations per year)".
PAPER_EXPECTED_MAX_SHARPE = 0.1132
#: "DSR ~ ... = 0.9004 < 0.95".
PAPER_DSR = 0.9004
#: "Should the strategist have made his discovery after running only N=46
#: independent trials, the investor may have allocated some funds, as DSR would
#: have been 0.9505".
PAPER_TRIALS_AT_THRESHOLD = 46
#: "If the strategy had exhibited Normal returns (y3=0, y4=3), DSR = 0.9505
#: after N=88 independent trials."
PAPER_TRIALS_IF_NORMAL = 88
PAPER_DSR_AT_THRESHOLD = 0.9505


def _paper_moments(*, skewness: float, kurtosis: float) -> TradeMoments:
    """The paper's selected strategy, de-annualised as its own example does."""
    return TradeMoments(
        sharpe=PAPER_SHARPE_ANNUALISED / math.sqrt(PAPER_OBSERVATIONS_PER_YEAR),
        skewness=skewness,
        kurtosis=kurtosis,
        trade_count=PAPER_SAMPLE_LENGTH,
    )


def _paper_dsr(*, trials: int, skewness: float, kurtosis: float) -> float:
    """Run our implementation over the paper's inputs.

    ⚠ ``average_correlation=0.0`` so equation (9) returns ``M`` unchanged: the
    paper states ``N`` as a count of INDEPENDENT trials directly, so the
    correlation step must be a no-op for this arm to test what it claims to.
    """
    result = deflated_sharpe(
        _paper_moments(skewness=skewness, kurtosis=kurtosis),
        effective_sample_size=PAPER_SAMPLE_LENGTH,
        trial_sharpe_variance=PAPER_SR_VARIANCE_ANNUALISED / PAPER_OBSERVATIONS_PER_YEAR,
        declared_trials=trials,
        average_correlation=0.0,
        measured_trials=trials,
        trial_register_version="test",
    )
    assert result is not None
    return result.deflated_sharpe


class TestPaperNumericalExample:
    """The published answers. ⚠ Literals above, nothing imported from us."""

    def test_expected_max_sharpe_matches_the_paper(self) -> None:
        threshold = expected_max_sharpe(
            trial_sharpe_variance=PAPER_SR_VARIANCE_ANNUALISED / PAPER_OBSERVATIONS_PER_YEAR,
            independent_trials=PAPER_TRIALS,
        )
        assert threshold == pytest.approx(PAPER_EXPECTED_MAX_SHARPE, abs=5e-5)

    def test_deflated_sharpe_matches_the_paper(self) -> None:
        value = _paper_dsr(trials=PAPER_TRIALS, skewness=PAPER_SKEWNESS, kurtosis=PAPER_KURTOSIS)
        assert value == pytest.approx(PAPER_DSR, abs=5e-5)

    def test_the_paper_s_forty_six_trial_counterfactual(self) -> None:
        """ "...after running only N=46 independent trials ... DSR would have been 0.9505"."""
        value = _paper_dsr(trials=PAPER_TRIALS_AT_THRESHOLD, skewness=PAPER_SKEWNESS, kurtosis=PAPER_KURTOSIS)
        assert value == pytest.approx(PAPER_DSR_AT_THRESHOLD, abs=5e-5)

    def test_the_paper_s_normal_returns_counterfactual(self) -> None:
        """Normal returns buy 88 trials rather than 46 at the same DSR.

        ⚠ This is the arm that pins the SKEW/KURTOSIS half of equation (2). The
        two above would both pass against an implementation that ignored the
        moments entirely and used ``sqrt(1 + SR^2/2)``; this one moves only
        because ``y3`` and ``y4`` enter the denominator.
        """
        value = _paper_dsr(trials=PAPER_TRIALS_IF_NORMAL, skewness=0.0, kurtosis=3.0)
        assert value == pytest.approx(PAPER_DSR_AT_THRESHOLD, abs=5e-5)

    def test_normal_returns_reduce_to_the_classical_standard_error(self) -> None:
        """At ``y3=0, y4=3`` equation (2)'s bracket collapses to ``1 + SR^2/2``.

        Lo (2002)'s IID Sharpe standard error, which is what makes the term
        recognisable — and a check that the ``+3`` convention is the one wired.
        ⚠ Computed here from the paper's own algebra, not from our source.
        """
        moments = _paper_moments(skewness=0.0, kurtosis=3.0)
        result = deflated_sharpe(
            moments,
            effective_sample_size=PAPER_SAMPLE_LENGTH,
            trial_sharpe_variance=PAPER_SR_VARIANCE_ANNUALISED / PAPER_OBSERVATIONS_PER_YEAR,
            declared_trials=PAPER_TRIALS,
            average_correlation=0.0,
            measured_trials=PAPER_TRIALS,
            trial_register_version="test",
        )
        assert result is not None
        classical = math.sqrt(1.0 + moments.sharpe**2 / 2.0)
        expected = (moments.sharpe - result.expected_max_sharpe) * math.sqrt(PAPER_SAMPLE_LENGTH - 1) / classical
        from statistics import NormalDist

        assert result.deflated_sharpe == pytest.approx(NormalDist().cdf(expected), abs=1e-12)


class TestEffectiveSampleSizeIsTheSampleLength:
    """⚠⚠ THE STAGE'S POINT: ``T`` is criterion 3's ESS, never the trade count."""

    def test_a_nominal_n_overstates_the_deflated_sharpe(self) -> None:
        """The S-1 shape: deff 30.04 means the nominal count inflates ``sqrt(T-1)``.

        A DSR computed on the nominal trade count must come out HIGHER than one
        computed on the effective size, because the test statistic scales with
        ``sqrt(T-1)`` and the strategy's Sharpe here beats the threshold.
        """
        moments = TradeMoments(sharpe=0.017, skewness=-0.4, kurtosis=8.0, trade_count=3_133_100)
        shared = {
            "trial_sharpe_variance": 1e-4,
            "declared_trials": 11,
            "average_correlation": 0.2,
            "measured_trials": 2,
            "trial_register_version": "test",
        }
        on_nominal = deflated_sharpe(moments, effective_sample_size=3_133_100, **shared)  # type: ignore[arg-type]
        on_effective = deflated_sharpe(moments, effective_sample_size=104_291.8, **shared)  # type: ignore[arg-type]
        assert on_nominal is not None
        assert on_effective is not None
        # ⚠ BOTH strictly inside (0, 1). A Sharpe far from the threshold
        # saturates the Normal CDF at exactly 1.0 on both arms, and the
        # comparison then passes for any implementation — including one that
        # ignored `effective_sample_size` entirely.
        assert 0.0 < on_effective.deflated_sharpe < on_nominal.deflated_sharpe < 1.0
        # The gap is the finding, not a rounding difference: the same trades
        # read as "beyond any plausible threshold" on the nominal count and
        # "nowhere near one" on the corrected size.
        assert on_nominal.deflated_sharpe > 0.99
        assert on_effective.deflated_sharpe < 0.95

    def test_a_nominal_n_also_BURIES_a_strategy_below_the_threshold(self) -> None:
        """⚠⚠ THE DIRECTION IS NOT ALWAYS FLATTERING, AND THE CORPUS PROVED IT.

        Equation (2) multiplies ``(SR - SR_0)`` by ``sqrt(T-1)``, so a larger T
        amplifies whatever SIGN that difference already carries. The test above
        uses a Sharpe ABOVE the threshold and the nominal count inflates it;
        this one uses a Sharpe BELOW and the nominal count drives it toward
        zero instead.

        This is not hypothetical. ``verify_2240_statistics.py`` originally
        asserted the one-sided version and FAILED on the full population,
        because both S-1 and S-3 sit below their threshold (S-1's per-trade
        Sharpe is negative — its expectancy is -0.44%/trade). A favourable
        single case had made the one-sided claim look universal.
        """
        below = TradeMoments(sharpe=0.005, skewness=-0.4, kurtosis=8.0, trade_count=3_133_100)
        shared = {
            "trial_sharpe_variance": 1e-4,
            "declared_trials": 11,
            "average_correlation": 0.2,
            "measured_trials": 2,
            "trial_register_version": "test",
        }
        on_nominal = deflated_sharpe(below, effective_sample_size=3_133_100, **shared)  # type: ignore[arg-type]
        on_effective = deflated_sharpe(below, effective_sample_size=104_291.8, **shared)  # type: ignore[arg-type]
        assert on_nominal is not None
        assert on_effective is not None
        # The Sharpe is below the threshold, so the nominal count makes the DSR
        # SMALLER — the opposite of the case above, on the same code.
        assert below.sharpe < on_effective.expected_max_sharpe
        assert on_nominal.deflated_sharpe < on_effective.deflated_sharpe

    def test_a_nominal_n_always_overstates_confidence_whichever_way_it_leans(self) -> None:
        """The sign-agnostic invariant the two cases above share.

        ⚠ This is the property ``verify_2240_statistics.py`` P9 asserts on the
        full population: a nominal *n* pushes the DSR further from 0.5 in
        whichever direction it already leans. "Flatters" is a special case of
        it, not the rule.
        """
        shared = {
            "trial_sharpe_variance": 1e-4,
            "declared_trials": 11,
            "average_correlation": 0.2,
            "measured_trials": 2,
            "trial_register_version": "test",
        }
        for sharpe in (0.017, 0.005):
            moments = TradeMoments(sharpe=sharpe, skewness=-0.4, kurtosis=8.0, trade_count=3_133_100)
            on_nominal = deflated_sharpe(moments, effective_sample_size=3_133_100, **shared)  # type: ignore[arg-type]
            on_effective = deflated_sharpe(moments, effective_sample_size=104_291.8, **shared)  # type: ignore[arg-type]
            assert on_nominal is not None
            assert on_effective is not None
            assert abs(on_nominal.deflated_sharpe - 0.5) > abs(on_effective.deflated_sharpe - 0.5)

    def test_the_stored_sample_length_is_the_effective_one(self) -> None:
        moments = TradeMoments(sharpe=0.05, skewness=0.0, kurtosis=3.0, trade_count=3_133_100)
        result = deflated_sharpe(
            moments,
            effective_sample_size=104_291.8,
            trial_sharpe_variance=1e-4,
            declared_trials=11,
            average_correlation=0.2,
            measured_trials=2,
            trial_register_version="test",
        )
        assert result is not None
        assert result.effective_sample_size == 104_291.8
        # ⚠ The nominal count survives on the moments and NOT on the result's
        # sample length — criterion 3 forbids reporting it as the sample size.
        assert moments.trade_count != result.effective_sample_size


class TestImpliedIndependentTrials:
    """Appendix A.3, equations (8) and (9)."""

    def test_zero_correlation_returns_every_trial(self) -> None:
        """ "as rho -> 0, then N -> M"."""
        assert implied_independent_trials(0.0, 11) == pytest.approx(11.0)

    def test_perfect_correlation_collapses_to_one_trial(self) -> None:
        """ "as rho -> 1, then N -> 1"."""
        assert implied_independent_trials(1.0, 11) == pytest.approx(1.0)

    def test_correlated_trials_never_exceed_the_declared_count(self) -> None:
        for rho in (0.0, 0.1, 0.5, 0.9, 1.0):
            assert 1.0 <= implied_independent_trials(rho, 11) <= 11.0

    def test_average_correlation_is_the_mean_off_diagonal(self) -> None:
        """Equation (8) on a matrix whose answer is arithmetic, not our code."""
        matrix = np.array([[1.0, 0.2, 0.4], [0.2, 1.0, 0.6], [0.4, 0.6, 1.0]])
        assert average_trial_correlation(matrix) == pytest.approx((0.2 + 0.4 + 0.6) / 3.0)

    def test_a_correlation_below_the_positive_definite_bound_is_refused(self) -> None:
        """A.3 bounds rho at ``-1/(M-1)``, which is TIGHTER than -1."""
        with pytest.raises(ValueError, match="outside"):
            implied_independent_trials(-0.6, 3)  # bound is -0.5

    def test_a_single_trial_has_no_average_correlation(self) -> None:
        with pytest.raises(ValueError, match="at least 2"):
            average_trial_correlation(np.array([[1.0]]))

    def test_an_asymmetric_matrix_is_refused(self) -> None:
        with pytest.raises(ValueError, match="not symmetric"):
            average_trial_correlation(np.array([[1.0, 0.3], [0.7, 1.0]]))

    def test_a_covariance_matrix_is_refused(self) -> None:
        """⚠⚠ SQUARE AND SYMMETRIC IS NOT ENOUGH — a covariance matrix is both.

        Eq. (8) would happily average its off-diagonal COVARIANCES into a
        number that looks like a correlation, feed it to eq. (9) and land it in
        `N_hat` with nothing downstream able to tell. The unit diagonal is what
        separates the two.
        """
        covariance = np.array([[4.0, 1.2], [1.2, 9.0]])
        assert np.allclose(covariance, covariance.T)  # symmetric, so the earlier guards pass
        with pytest.raises(ValueError, match="diagonal that is not all ones"):
            average_trial_correlation(covariance)

    def test_an_out_of_range_entry_is_refused(self) -> None:
        with pytest.raises(ValueError, match=r"outside \[-1, 1\]"):
            average_trial_correlation(np.array([[1.0, 1.4], [1.4, 1.0]]))

    def test_more_correlation_means_a_lower_threshold(self) -> None:
        """Correlated trials are not independent evidence of a wide search."""
        independent = expected_max_sharpe(
            trial_sharpe_variance=1e-4, independent_trials=implied_independent_trials(0.0, 11)
        )
        correlated = expected_max_sharpe(
            trial_sharpe_variance=1e-4, independent_trials=implied_independent_trials(0.8, 11)
        )
        assert correlated < independent


class TestRefusals:
    """Each returns ``None``; the promotion gate refuses on the null."""

    def _moments(self) -> TradeMoments:
        return TradeMoments(sharpe=0.05, skewness=0.0, kurtosis=3.0, trade_count=1000)

    def _call(self, **overrides: object) -> DeflatedSharpeResult | None:
        kwargs: dict[str, object] = {
            "effective_sample_size": 500.0,
            "trial_sharpe_variance": 1e-4,
            "declared_trials": 11,
            "average_correlation": 0.2,
            "measured_trials": 2,
            "trial_register_version": "test",
        }
        kwargs.update(overrides)
        return deflated_sharpe(self._moments(), **kwargs)  # type: ignore[arg-type]

    def test_one_measured_trial_has_no_sharpe_variance(self) -> None:
        assert self._call(measured_trials=1) is None

    def test_the_minimum_is_two(self) -> None:
        assert MIN_MEASURED_TRIALS == 2
        assert self._call(measured_trials=2) is not None

    def test_an_effective_sample_of_one_trade_is_not_a_sample(self) -> None:
        assert self._call(effective_sample_size=1.0) is None

    def test_a_missing_effective_sample_size_cannot_be_faked(self) -> None:
        assert self._call(effective_sample_size=0.0) is None

    def test_zero_trial_variance_is_refused(self) -> None:
        assert self._call(trial_sharpe_variance=0.0) is None

    def test_perfectly_correlated_trials_refuse_rather_than_raise(self) -> None:
        """⚠⚠ REACHABLE, and it was a CRASH until Codex caught it.

        At ``rho == 1`` equation (9) gives ``N_hat == 1`` and ``Z^-1[1 - 1/N]``
        is ``Z^-1[0] == -inf``. Two register entries running the same rule over
        two corpora have near-identical return series — the #2260 arms are
        exactly that shape — so this is a real state, and this function's
        contract is to fail closed on it rather than raise into the caller.
        """
        assert self._call(average_correlation=1.0) is None

    def test_negatively_correlated_trials_refuse_rather_than_raise(self) -> None:
        """⚠⚠ THE OTHER END OF THE SAME BOUND, AND IT WAS MISSED FIRST TIME.

        A NEGATIVE average correlation gives ``N_hat > M`` — at ``M = 11,
        rho = -0.09``, ``N_hat = 11.9`` — and ``DeflatedSharpeResult`` refuses
        ``N > M``. A.3 derives the interpolation only between ``rho -> 1``
        (``N -> 1``) and ``rho -> 0`` (``N -> M``), so this is outside the
        published rule, and clamping to ``M`` would invent a treatment the
        paper does not give.

        ⚠ Realistic, not a corner case: a momentum sleeve against a
        mean-reversion one. Codex's checkpoint-2 P2 fixed only the ``rho == 1``
        end; the review bot on PR #2372 found this one still raising.
        """
        # The correlation is INSIDE A.3's positive-definite bound of -1/(M-1),
        # so `implied_independent_trials` accepts it — the refusal has to be the
        # N-hat bound, not the rho bound.
        assert implied_independent_trials(-0.09, 11) > 11
        assert self._call(average_correlation=-0.09) is None

    def test_the_threshold_helper_still_raises_on_a_single_trial(self) -> None:
        """⚠ The RAISE is kept where a direct caller could pass nonsense.

        ``deflated_sharpe`` converts it to a refusal; ``expected_max_sharpe``
        itself must still refuse loudly, because an ``N <= 1`` reaching it
        directly is a programming error, not a measurement that came back
        degenerate.
        """
        with pytest.raises(ValueError, match="must exceed 1"):
            expected_max_sharpe(trial_sharpe_variance=1e-4, independent_trials=1.0)

    def test_a_zero_standard_error_is_refused(self) -> None:
        """⚠⚠ ZERO IS THE REACHABLE CASE; STRICTLY NEGATIVE IS NOT.

        Equation (2)'s bracket is a quadratic in ``SR`` whose discriminant is
        ``y3^2 - (y4 - 1)``. Pearson's inequality gives ``y4 >= y3^2 + 1``,
        i.e. ``y4 - 1 >= y3^2``, so that discriminant is **always <= 0** and the
        bracket can never go below zero for a valid moment pair — it only
        TOUCHES zero, at the Pearson boundary and the single Sharpe
        ``2*y3/(y4-1)``.

        ⚠ This became true only once the moment guard enforced ``y3^2 + 1``
        rather than a bare 1 (review NITPICK on PR #2372). The earlier version
        of this test used ``y3=-1, y4=1`` — a pair Pearson forbids — to drive
        the bracket negative, so it was asserting a refusal on an input that
        cannot exist. The refusal is still live, because zero is reachable and
        dividing by ``sqrt(0)`` is just as fatal.
        """
        skewness, kurtosis = 3.0, 10.0
        assert kurtosis == skewness**2 + 1.0  # exactly on Pearson's bound
        moments = TradeMoments(
            sharpe=2.0 * skewness / (kurtosis - 1.0),
            skewness=skewness,
            kurtosis=kurtosis,
            trade_count=1000,
        )
        variance_term = 1.0 - moments.skewness * moments.sharpe + (moments.kurtosis - 1.0) / 4.0 * moments.sharpe**2
        assert variance_term == pytest.approx(0.0, abs=1e-12)
        assert (
            deflated_sharpe(
                moments,
                effective_sample_size=500.0,
                trial_sharpe_variance=1e-4,
                declared_trials=11,
                average_correlation=0.2,
                measured_trials=2,
                trial_register_version="test",
            )
            is None
        )


class TestTradeMoments:
    def test_the_sharpe_is_scale_invariant(self) -> None:
        """Percent-vs-fraction must not move any of the three moments."""
        returns = [1.2, -0.4, 0.9, -1.8, 2.4, 0.1]
        as_percent = trade_moments(returns)
        as_fraction = trade_moments([value / 100.0 for value in returns])
        assert as_percent is not None
        assert as_fraction is not None
        assert as_percent.sharpe == pytest.approx(as_fraction.sharpe)
        assert as_percent.skewness == pytest.approx(as_fraction.skewness)
        assert as_percent.kurtosis == pytest.approx(as_fraction.kurtosis)

    def test_kurtosis_is_raw_not_excess(self) -> None:
        """⚠ A Normal sample must land near 3, not near 0.

        The whole ``(y4 - 1)/4`` term depends on this convention, and excess
        kurtosis would pass every other test in this file while shrinking
        equation (2)'s denominator by a silent constant.
        """
        rng = np.random.default_rng(20260807)
        moments = trade_moments(rng.normal(0.0, 1.0, 200_000).tolist())
        assert moments is not None
        assert moments.kurtosis == pytest.approx(3.0, abs=0.05)
        assert moments.skewness == pytest.approx(0.0, abs=0.05)

    def test_the_sharpe_denominator_is_the_population_deviation(self) -> None:
        """⚠⚠ ddof=0, PINNED ON A SAMPLE SMALL ENOUGH TO SEE IT.

        The paper's ``SR`` is the plain ratio of the sample mean to the sample
        standard deviation, and equation (2) supplies the small-sample
        correction itself through ``sqrt(T-1)`` and the moment terms — a second
        correction here would double-count it.

        ⚠ THIS TEST EXISTS BECAUSE A REVERT PROBE WENT UNCAUGHT. The probe
        injecting ``ddof=1`` was first aimed at the paper's reference arm, which
        builds ``TradeMoments`` directly and never runs this denominator; and on
        the full population the two differ by ``sqrt(n/(n-1))``, which at 3.1M
        trades is 1.00000016 and invisible to any tolerance. At n=4 the same
        defect moves the Sharpe from 2.2361 to 1.9365.

        ⚠ Both values are computed from the definition below, not read back.
        """
        returns = [1.0, 2.0, 3.0, 4.0]
        count = len(returns)
        mean = sum(returns) / count
        squares = sum((value - mean) ** 2 for value in returns)
        population = mean / math.sqrt(squares / count)
        sample = mean / math.sqrt(squares / (count - 1))
        assert population != pytest.approx(sample)

        moments = trade_moments(returns)
        assert moments is not None
        assert moments.sharpe == pytest.approx(population)

    def test_a_constant_return_series_is_degenerate(self) -> None:
        assert trade_moments([0.5, 0.5, 0.5]) is None

    def test_a_single_trade_has_no_moments(self) -> None:
        assert trade_moments([0.5]) is None

    def test_excess_kurtosis_is_refused_at_construction(self) -> None:
        """Passing ``y4 - 3`` for a Normal gives 0.0, which is not a raw moment."""
        with pytest.raises(ValueError, match="excess kurtosis"):
            TradeMoments(sharpe=0.1, skewness=0.0, kurtosis=0.0, trade_count=100)

    def test_a_kurtosis_between_zero_and_one_is_refused(self) -> None:
        """⚠ THE BOUND IS 1, NOT 0. For any real distribution ``y4 >= y3^2 + 1``.

        A ``> 0`` guard admits the whole of ``(0, 1)`` while claiming those
        values impossible — and a near-Normal series passed as EXCESS kurtosis
        lands in exactly that range.
        """
        with pytest.raises(ValueError, match="excess kurtosis"):
            TradeMoments(sharpe=0.1, skewness=0.0, kurtosis=0.4, trade_count=100)

    def test_a_kurtosis_below_the_skewness_floor_is_refused(self) -> None:
        """⚠⚠ THE FLOOR IS ``y3^2 + 1``, NOT A BARE 1.

        Pearson's inequality ties the two moments, so a large skew REQUIRES a
        large kurtosis: at ``y3 = 2`` the floor is 5, and ``y4 = 1`` beside it
        is impossible rather than merely unusual — yet it clears a bare ``>= 1``
        guard. ``trade_moments`` cannot emit such a pair (it computes both from
        one series), so this catches a DIRECT caller, which is the only way an
        inconsistent pair can enter.
        """
        with pytest.raises(ValueError, match=r"below y3\^2 \+ 1"):
            TradeMoments(sharpe=0.1, skewness=2.0, kurtosis=1.0, trade_count=100)

    def test_a_consistent_skew_kurtosis_pair_constructs(self) -> None:
        """The same skew with a kurtosis above its floor is fine."""
        assert TradeMoments(sharpe=0.1, skewness=2.0, kurtosis=5.5, trade_count=100).kurtosis == 5.5

    def test_a_two_point_distribution_attains_exactly_one(self) -> None:
        """⚠ 1 is ATTAINABLE, so the bound is inclusive rather than strict.

        Equal-sized wins and equal-sized losses in equal number is a Bernoulli
        shape, whose raw kurtosis is exactly 1 — a reachable trade population,
        not a mathematical curiosity. Computed here, not asserted from a
        literal.
        """
        moments = trade_moments([1.0, -1.0, 1.0, -1.0])
        assert moments is not None
        assert moments.kurtosis == pytest.approx(1.0)
        assert TradeMoments(sharpe=0.1, skewness=0.0, kurtosis=1.0, trade_count=4).kurtosis == 1.0


class TestResultInvariants:
    def _result(self, **overrides: object) -> DeflatedSharpeResult:
        kwargs: dict[str, object] = {
            "deflated_sharpe": 0.9,
            "expected_max_sharpe": 0.01,
            "trade_sharpe": 0.05,
            "skewness": 0.0,
            "kurtosis": 3.0,
            "effective_sample_size": 500.0,
            "declared_trials": 11,
            "independent_trials": 9.0,
            "average_trial_correlation": 0.2,
            "trial_sharpe_variance": 1e-4,
            "measured_trials": 2,
            "trial_register_version": "test",
        }
        kwargs.update(overrides)
        return DeflatedSharpeResult(**kwargs)  # type: ignore[arg-type]

    def test_the_model_id_is_stamped(self) -> None:
        assert self._result().model_id == DSR_MODEL_ID

    def test_a_dsr_outside_zero_one_is_not_a_probability(self) -> None:
        with pytest.raises(ValueError, match="probability"):
            self._result(deflated_sharpe=1.4)

    def test_independent_trials_cannot_exceed_declared(self) -> None:
        with pytest.raises(ValueError, match="exceeds"):
            self._result(independent_trials=12.0)

    def test_measured_trials_cannot_exceed_declared(self) -> None:
        with pytest.raises(ValueError, match="measured trials"):
            self._result(measured_trials=12)

    def test_a_blank_register_version_is_refused(self) -> None:
        with pytest.raises(ValueError, match="trial_register_version is blank"):
            self._result(trial_register_version="")
