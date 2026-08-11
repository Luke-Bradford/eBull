"""Phase 5e-3 — criterion 6's Deflated Sharpe Ratio, on a declared trial count.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §8 (stage 5e-3) and
§9's C6. Parent ``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md``
criterion 6 — *"Use the Deflated Sharpe Ratio (Bailey & López de Prado), which
takes the number of trials, their correlation, and the skew/kurtosis of returns.
Its trial count must include every variant evaluated — abandoned branches,
manual eyeballing, and parameter values tried and discarded."* Refs #2240.

SOURCE RULE — EVERY EQUATION BELOW IS THE PAPER'S, NONE IS REASONED OUT
----------------------------------------------------------------------
Bailey, D. H. and López de Prado, M. (2014), *The Deflated Sharpe Ratio:
Correcting for Selection Bias, Backtest Overfitting and Non-Normality*, Journal
of Portfolio Management 40(5):94-107. SSRN id 2460551. Read at implementation
time, not recalled: the equation numbers below are the paper's own.

    (1)/(6)  E[max{SR_n}] ~ E[{SR_n}]
                            + sqrt(V[{SR_n}]) ((1-g) Z^-1[1 - 1/N]
                                               + g Z^-1[1 - (1/N) e^-1])

    (2)      DSR = Z[ (SR - SR_0) sqrt(T - 1)
                      / sqrt(1 - y3 SR + ((y4 - 1)/4) SR^2) ]

    (8)      rho = 2 sum_{i} sum_{j>i} rho_ij / (M (M - 1))

    (9)      N_hat = rho_hat + (1 - rho_hat) M

``g`` is the Euler-Mascheroni constant (``numpy.euler_gamma``, not a hand-typed
literal), ``Z`` the standard Normal CDF and ``Z^-1`` its inverse, ``y3`` the
skewness and ``y4`` the kurtosis of the returns, ``T`` the sample length, ``M``
the number of trials run and ``N`` the number of INDEPENDENT trials. Under the
null ``H0: SR = 0`` the paper sets ``E[{SR_n}] = 0`` in (1), which is the form
(2) uses for ``SR_0``.

⚠⚠ THE PAPER'S INPUTS ARE PER-OBSERVATION, NOT ANNUALISED, AND THE NUMERICAL
EXAMPLE IS WHERE THAT IS VISIBLE.

Its worked example carries an ANNUALISED ``SR`` of 2.5 and an annualised
``V[{SR_n}]`` of ``1/2`` over 5 daily years, and then computes with ``2.5 /
sqrt(250)``, ``1 / (2 * 250)`` and ``T = 1250``. So ``SR``, ``V[{SR_n}]`` and
``T`` are all in units of ONE OBSERVATION, and mixing an annualised Sharpe into
(2) would inflate the numerator by ``sqrt(periods per year)`` while leaving the
denominator alone. Nothing here annualises anything, and
``tests/test_deflated_sharpe.py`` pins all three of the paper's published
answers (0.9004, 0.9505 and 0.9505) as the reference.

⚠⚠ WHICH AXIS THE FOUR INPUTS LIVE ON IS OURS, AND IT IS FIXED BY 5e-2.

The paper requires ``SR``, ``y3``, ``y4`` and ``T`` to describe the SAME return
series — (2) divides a Sharpe by the standard error of that same Sharpe, and the
standard error is built from that series' own third and fourth moments and its
own length. Two candidate series exist here and only one of them can supply all
four:

- **the equity curve's per-period returns**, which is what criterion 7's
  ``StrategyMetrics.sharpe`` is computed on (annualised, ``strategy_statistics``);
- **the realised per-trade returns**, which is what criterion 3's block
  bootstrap clusters and therefore what ``effective_sample_size`` counts —
  ``block_bootstrap``'s header is explicit that Kish's ESS is *"in units of
  TRADES, so it is commensurable with trade_count"*.

§5.2 says criterion 6's Deflated Sharpe **consumes** the effective sample size,
and criterion 3 forbids a nominal *n* anywhere. So ``T`` must be the ESS, the
ESS is in trades, and therefore ``SR``, ``y3`` and ``y4`` are computed on the
TRADE axis too. The alternative — keeping 5d's per-period Sharpe and dividing
its period count by the design effect — would carry a design effect measured by
clustering TRADES onto a series of PERIODS, and no test on either side could see
it. ⚠ The consequence is stated rather than hidden: ``trade_sharpe`` is NOT
``StrategyMetrics.sharpe``, the two are different statistics on different axes,
and ``sql/266`` stores this one under its own name.

⚠ ``y4`` IS THE RAW FOURTH MOMENT, NOT EXCESS KURTOSIS. The paper's example
gives ``y4 = 10`` and says a Normal distribution has ``y3 = 0, y4 = 3`` — so the
``(y4 - 1)/4`` term expects Fisher's ``+3`` convention. Passing excess kurtosis
would shrink the denominator and inflate every DSR by a silent constant.

⚠ WHAT IS NOT MODELLED, STATED RATHER THAN OMITTED
--------------------------------------------------
Appendix A.3 offers the average-correlation route to ``N_hat`` (implemented
here) and then names its own two weaknesses: correlation is *"a limited notion
of linear dependence"*, and where ``M`` exceeds the sample length the
correlation estimate is itself overfit. It points at an information-theoretic
alternative for the number of non-redundant sources. That is NOT implemented.
The register is now a conservative, hand-declared historical search floor, and
its growth to three digits makes that published limitation material rather than
hypothetical. The current DSR is therefore not sufficient promotion evidence
while the historical experiment ledger and its measurable return series remain
incomplete; the promotion gate refuses results from superseded populations.

⚠ Exhibit 3.1 measures the accuracy of (1): the analytic value OVERSTATES the
empirical expected maximum by under 0.05 for ``N < 50`` at ``V = 1``, falling to
0.006 by ``N = 1000``. Our ``N`` is small, so this is the loose end of the
published range — and it errs toward a HIGHER ``SR_0``, hence a LOWER DSR, which
is the conservative direction. Stated because the paper's own proof requires
``N >> 1``.
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

import numpy as np
import numpy.typing as npt

#: The identity of this construction, in the same role as ``BOOTSTRAP_MODEL_ID``
#: and ``METRIC_SET_ID``: the choices that are OURS rather than the paper's (the
#: trade axis for all four inputs, ``T`` taken as the effective sample size, the
#: ddof=1 estimator for ``V[{SR_n}]``) are frozen behind it, so a stored
#: ``deflated_sharpe`` cannot silently change meaning. Bumping it is a new
#: evaluation.
DSR_MODEL_ID: Final = "c6-deflated-sharpe-v1"

#: Euler-Mascheroni, from numpy rather than typed out. ⚠ The paper writes
#: "approx. 0.5772", and a four-digit transcription of a constant that appears
#: inside two normal quantiles is the kind of silent precision loss this repo
#: has no way to detect afterwards.
EULER_MASCHERONI: Final[float] = float(np.euler_gamma)

#: The standard Normal, for ``Z`` and ``Z^-1``. ⚠ stdlib, NOT a new dependency:
#: scipy is absent from this project (verified at implementation time) and
#: criterion 6 needs exactly two functions from it. ``NormalDist.inv_cdf``
#: reproduces all three of the paper's published DSR values to 4 decimal places,
#: which is the accuracy claim and it is tested rather than assumed.
_NORMAL: Final = statistics.NormalDist(0.0, 1.0)

#: ⚠ ``V[{SR_n}]`` is a sample variance across trials and needs at least two
#: measured trials to exist at all. One trial is not a narrow distribution of
#: trial Sharpes, it is no distribution: ``SR_0`` would collapse to zero and the
#: DSR would silently become an undeflated PSR — the exact correction criterion
#: 6 exists to apply, reported as if it had been applied.
MIN_MEASURED_TRIALS: Final = 2


@dataclass(frozen=True)
class TradeMoments:
    """The selected strategy's half of equation (2), on the trade axis.

    ⚠ ``sharpe`` here is PER TRADE and is deliberately not annualised — see the
    module header. It is also NOT ``StrategyMetrics.sharpe``, which is the
    annualised per-period Sharpe of the equity curve.
    """

    #: ``mean(net return) / stdev(net return)`` over the realised trades.
    sharpe: float
    #: Fisher-Pearson skewness ``y3``.
    skewness: float
    #: RAW kurtosis ``y4`` — Normal is 3, not 0. See the header.
    kurtosis: float
    #: The nominal trade count the moments were computed over. ⚠ Recorded but
    #: NOT used as ``T``: equation (2)'s ``T`` is the effective sample size, and
    #: this is the number criterion 3 forbids reporting in its place. It is kept
    #: so the ratio between the two is visible on the row.
    trade_count: int

    def __post_init__(self) -> None:
        if self.trade_count < 2:
            raise ValueError(f"trade_count must be at least 2 for a moment to exist, got {self.trade_count}")
        # ⚠ THE BOUND IS 1, NOT 0. For any real distribution
        # `y4 >= y3^2 + 1 >= 1`, with equality at a two-point symmetric
        # distribution (Bernoulli(1/2) has y4 exactly 1) — which is a reachable
        # trade population: every win the same size, every loss the same size.
        # So 1 is attainable and anything below it is impossible, whereas the
        # old `> 0` let the whole of (0, 1) through while the message claimed
        # otherwise. A Normal passed as EXCESS kurtosis arrives as 0.0 and is
        # still caught, which is the defect this guards.
        # ⚠ THE BOUND IS `y3^2 + 1`, NOT A BARE 1. Pearson's inequality relates
        # the two moments, so a large skew REQUIRES a large kurtosis — at
        # `y3 = 2` the floor is 5, and `y4 = 1` beside it is not merely unusual
        # but impossible. `trade_moments` cannot produce such a pair (it
        # computes both from one series), so this catches a DIRECT caller, which
        # is the only way an inconsistent pair can enter.
        floor = self.skewness**2 + 1.0
        if self.kurtosis < floor:
            raise ValueError(
                f"kurtosis {self.kurtosis} is below y3^2 + 1 = {floor} for skewness {self.skewness} — Pearson's "
                "inequality makes that pair impossible for any real distribution, and a value below 1 in particular "
                "means excess kurtosis was passed (see the header)"
            )


@dataclass(frozen=True)
class DeflatedSharpeResult:
    """Criterion 6's output, plus every declared input needed to re-run it."""

    #: Equation (2). A probability in ``[0, 1]``: *"the probability that the true
    #: SR is above the multiple-testing-adjusted threshold"*.
    deflated_sharpe: float
    #: Equation (1) under ``H0``, the rejection threshold this deflates against.
    #: Per-trade, like ``trade_sharpe``.
    expected_max_sharpe: float
    trade_sharpe: float
    skewness: float
    kurtosis: float
    #: ``T`` — the EFFECTIVE sample size (criterion 3), never the trade count.
    effective_sample_size: float
    #: ``M`` — every trial the register declares, including the ones with no
    #: measured Sharpe.
    declared_trials: int
    #: ``N_hat`` — equation (9)'s implied INDEPENDENT trials. ⚠ Always <= ``M``.
    independent_trials: float
    #: ``rho_hat`` — equation (8)'s average correlation between trials.
    average_trial_correlation: float
    #: ``V[{SR_n}]`` — sample variance of the measured trials' per-trade Sharpes.
    trial_sharpe_variance: float
    #: How many of the ``M`` trials carried a measured Sharpe. ⚠ Reported
    #: because ``V[{SR_n}]`` is estimated from THESE and applied to ALL of them;
    #: see ``TrialRegister``'s header for the direction of that bias.
    measured_trials: int
    #: Which declaration ``M`` was counted from. ⚠ REQUIRED, no default: the
    #: count alone does not say WHICH trials, and a DSR against eleven declared
    #: trials is a different statement from one against thirty.
    trial_register_version: str
    model_id: str = DSR_MODEL_ID

    def __post_init__(self) -> None:
        if not 0.0 <= self.deflated_sharpe <= 1.0:
            raise ValueError(f"deflated_sharpe is a probability and must be in [0, 1], got {self.deflated_sharpe}")
        if self.independent_trials > self.declared_trials:
            raise ValueError(
                f"implied independent trials {self.independent_trials} exceeds the {self.declared_trials} declared — "
                "equation (9) interpolates between 1 and M and cannot leave that range"
            )
        if self.measured_trials > self.declared_trials:
            raise ValueError(
                f"{self.measured_trials} measured trials against {self.declared_trials} declared — a measured trial "
                "that is not declared is a trial missing from criterion 6's count"
            )
        if not self.trial_register_version:
            raise ValueError("trial_register_version is blank — a DSR that names no trial population states nothing")


def trade_moments(net_return_pct: Sequence[float]) -> TradeMoments | None:
    """The trade axis' Sharpe, skewness and RAW kurtosis. ``None`` if degenerate.

    ⚠ Scale-invariant throughout, so percent-vs-fraction does not matter: a
    Sharpe is a ratio of two first-degree-homogeneous quantities and the
    standardised moments divide by ``sigma^k``. Stated because
    ``net_return_pct`` is in PERCENT and every other consumer of it has to care.

    ⚠ ``ddof=0`` for the moments and ``ddof=0`` for the Sharpe's denominator.
    The paper's ``SR`` is the plain ratio of the sample mean to the sample
    standard deviation, and (2) supplies the small-sample correction itself
    through ``sqrt(T-1)`` and the moment terms — applying a second one here
    would double-count it.

    Returns ``None`` when fewer than two trades exist or every trade returned
    the same number, because the Sharpe's denominator is then zero.
    """
    returns = np.asarray(net_return_pct, dtype=np.float64)
    if returns.size < 2:
        return None

    sigma = float(returns.std(ddof=0))
    if sigma <= 0.0 or not math.isfinite(sigma):
        return None

    centred = returns - returns.mean()
    skewness = float((centred**3).mean() / sigma**3)
    kurtosis = float((centred**4).mean() / sigma**4)
    if not (math.isfinite(skewness) and math.isfinite(kurtosis)):
        return None

    return TradeMoments(
        sharpe=float(returns.mean()) / sigma,
        skewness=skewness,
        kurtosis=kurtosis,
        trade_count=int(returns.size),
    )


def average_trial_correlation(correlation_matrix: npt.NDArray[np.float64]) -> float:
    """Equation (8) — the equal-weighted average of the off-diagonal correlations.

    ⚠ The paper derives this as the constant ``rho`` that leaves the quadratic
    form ``1' C 1`` unchanged, which is why it is the plain mean of the
    off-diagonal entries and not, say, a Fisher-z mean. A.3 notes Fisher's
    transform *"could further enrich"* the method by controlling the estimation
    error's variance; that is an enrichment the paper does not itself adopt, so
    neither do we.
    """
    matrix = np.asarray(correlation_matrix, dtype=np.float64)
    if matrix.ndim != 2 or matrix.shape[0] != matrix.shape[1]:
        raise ValueError(f"a correlation matrix must be square, got shape {matrix.shape}")
    size = matrix.shape[0]
    if size < 2:
        raise ValueError(f"average correlation needs at least 2 trials, got {size} — A.3 requires M > 1 to exist")
    if not np.allclose(matrix, matrix.T, equal_nan=False):
        raise ValueError("correlation matrix is not symmetric, so rho_ij and rho_ji disagree")
    # ⚠ SQUARE AND SYMMETRIC IS NOT ENOUGH — a COVARIANCE matrix is both, and
    # eq. (8) would average its off-diagonal covariances into a number that
    # looks like a correlation, feeds eq. (9) and lands in `N_hat` with nothing
    # downstream able to tell. The two properties that separate the two are the
    # unit diagonal and the [-1, 1] range, so both are checked here.
    if not np.allclose(np.diag(matrix), 1.0):
        raise ValueError(
            "correlation matrix has a diagonal that is not all ones — a covariance matrix is also square and "
            "symmetric, and averaging its off-diagonal entries would produce a meaningless rho"
        )
    if not (np.all(matrix >= -1.0) and np.all(matrix <= 1.0)):
        raise ValueError("correlation matrix has an entry outside [-1, 1], so it is not a correlation matrix")

    upper = matrix[np.triu_indices(size, k=1)]
    return float(2.0 * upper.sum() / (size * (size - 1)))


def implied_independent_trials(average_correlation: float, declared_trials: int) -> float:
    """Equation (9) — ``N_hat = rho + (1 - rho) M``.

    A.3's derivation, in its own terms: *"as rho -> 1, then N -> 1. Similarly,
    as rho -> 0, then N -> M. Given an estimated average correlation rho_hat, we
    could therefore interpolate between these two extreme outcomes."*

    ⚠ THE POINT OF THIS FUNCTION IS THAT ``M`` IS THE WRONG NUMBER. A.3 opens
    with *"using M instead of N will overstate E[max{SR_n}]"* — and overstating
    the rejection threshold understates the DSR, so the error is conservative
    but it is still an error. Correlated trials are not independent evidence
    that a strategy survived a wide search.

    ⚠ ``average_correlation`` is bounded below by ``-1/(M-1)`` for a
    positive-definite matrix (A.3), not by ``-1``. Anything outside that means
    the matrix it came from was not a correlation matrix.
    """
    if declared_trials < 2:
        raise ValueError(f"declared_trials must be at least 2, got {declared_trials} — A.3 requires M > 1")
    lower_bound = -1.0 / (declared_trials - 1)
    if not lower_bound < average_correlation <= 1.0:
        raise ValueError(
            f"average correlation {average_correlation} is outside ({lower_bound}, 1] — A.3 bounds it there for a "
            f"positive-definite {declared_trials}x{declared_trials} correlation matrix"
        )
    return average_correlation + (1.0 - average_correlation) * declared_trials


def expected_max_sharpe(
    *,
    trial_sharpe_variance: float,
    independent_trials: float,
) -> float:
    """Equations (1)/(6) under ``H0`` — the expected maximum Sharpe after ``N`` trials.

    ⚠ ``E[{SR_n}]`` IS FIXED AT ZERO AND IS NOT A PARAMETER. The paper carries
    the trials' mean through (1) for the general statement and then drops it in
    equation (2)'s ``SR_0``, which is evaluated under the null ``H0: SR = 0`` —
    the only form this module ever needs. An earlier version exposed it as a
    defaulted keyword no caller ever set, which is unreachable and untested
    flexibility rather than a capability; if a non-null form is ever wanted, it
    should arrive with the caller that needs it.

    ⚠ ``independent_trials`` is a FLOAT because equation (9) interpolates. The
    paper's ``N`` is a count, but ``N_hat`` is not — rounding it would move the
    threshold by an amount nobody chose.
    """
    if trial_sharpe_variance < 0.0:
        raise ValueError(f"trial_sharpe_variance must be non-negative, got {trial_sharpe_variance}")
    if independent_trials <= 1.0:
        # ⚠ NOT a clamp. At N = 1, `Z^-1[1 - 1/N]` is `Z^-1[0]` = -inf, and at
        # N < 1 the argument is negative and the quantile is undefined. Both are
        # states where "the expected maximum of N trials" has no meaning, and a
        # substituted value would be an invented threshold.
        raise ValueError(
            f"independent_trials must exceed 1, got {independent_trials} — Z^-1[1 - 1/N] is undefined at or below it"
        )

    tail = 1.0 - 1.0 / independent_trials
    tail_over_e = 1.0 - (1.0 / independent_trials) * math.exp(-1.0)
    max_z = (1.0 - EULER_MASCHERONI) * _NORMAL.inv_cdf(tail) + EULER_MASCHERONI * _NORMAL.inv_cdf(tail_over_e)
    # ⚠ `E[{SR_n}] = 0` under H0, so (1)'s leading term vanishes. Written as the
    # bare product rather than `0.0 + ...` so nothing reads as a placeholder.
    return math.sqrt(trial_sharpe_variance) * max_z


def deflated_sharpe(
    moments: TradeMoments,
    *,
    effective_sample_size: float,
    trial_sharpe_variance: float,
    declared_trials: int,
    average_correlation: float,
    measured_trials: int,
    trial_register_version: str,
) -> DeflatedSharpeResult | None:
    """Equation (2). Pure; reads no database.

    ⚠⚠ ``effective_sample_size`` IS ``T``, AND THAT IS THE WHOLE STAGE. §5.2:
    criterion 6's Deflated Sharpe consumes criterion 3's effective sample size.
    Passing the nominal trade count instead would inflate ``sqrt(T - 1)`` by
    ``sqrt(deff)`` — on the S-1 measurement that is ``sqrt(30.04)``, a factor of
    5.5 on the test statistic — and produce a confident DSR from evidence the
    block bootstrap already showed was not there.

    ⚠ RETURNS ``None`` IN FOUR STATES, AND EACH IS REAL:

    1. **Fewer than ``MIN_MEASURED_TRIALS`` measured trials.** ``V[{SR_n}]``
       does not exist, so there is no distribution of trial Sharpes to take a
       maximum over.
    2. **``T <= 1``.** ``sqrt(T - 1)`` is zero or imaginary. An effective sample
       size at or below one trade is not a sample.
    3. **A non-positive variance term** under the square root in (2)'s
       denominator — in practice ZERO, because strictly negative is
       unreachable. ⚠ The bracket is a quadratic in ``SR`` with discriminant
       ``y3^2 - (y4 - 1)``, and ``TradeMoments`` enforces Pearson's
       ``y4 >= y3^2 + 1``, so that discriminant is always ``<= 0``: the bracket
       touches zero at the Pearson boundary (at ``SR = 2*y3/(y4-1)``) and is
       positive everywhere else. Zero is enough — the standard error is then
       zero and (2) divides by it — so the guard stays ``<= 0`` rather than
       being narrowed to an equality that would read as if the negative case
       were still live.
    4. **An implied independent trial count outside ``(1, M]``**, at either
       end. ⚠ BOTH ENDS ARE REACHABLE and each was found separately:

       - ``rho_hat == 1`` gives ``N_hat == 1``, where ``Z^-1[1 - 1/N]`` is
         ``Z^-1[0] == -inf`` — there is no expected maximum of one trial to
         deflate against. Two register entries running the same rule over two
         corpora have near-identical return series, and the #2260 arms are
         exactly that shape. *Caught by Codex at checkpoint 2.*
       - A NEGATIVE ``rho_hat`` gives ``N_hat > M`` (at ``M = 11, rho = -0.09``,
         ``N_hat = 11.9``). Negative average correlation is realistic — a
         momentum sleeve against a mean-reversion one — and A.3 derives the
         interpolation only between ``rho -> 1`` (``N -> 1``) and ``rho -> 0``
         (``N -> M``), so ``N_hat > M`` is OUTSIDE the published rule. Clamping
         it to ``M`` would be inventing a treatment the paper does not give.
         *Caught by the review bot on PR #2372, where it was still a raise —
         the same class as the first, fixed at only one end.*

       Both would otherwise crash the caller instead of failing closed the way
       this function documents and ``sql/266`` expects.

    In each the caller leaves ``deflated_sharpe`` NULL and the promotion gate
    refuses on ``deflated_sharpe_not_computed``. Criterion 6 says an undeclared
    trial count *"fails; it does not default"* — and a DSR that could not be
    computed defaults to nothing either.
    """
    if measured_trials < MIN_MEASURED_TRIALS:
        return None
    if effective_sample_size <= 1.0 or not math.isfinite(effective_sample_size):
        return None
    if trial_sharpe_variance <= 0.0 or not math.isfinite(trial_sharpe_variance):
        return None

    independent = implied_independent_trials(average_correlation, declared_trials)
    # ⚠⚠ Refusal 4, BOTH ENDS — see the docstring. A.3 derives eq. (9) only
    # between `rho -> 1` (`N -> 1`) and `rho -> 0` (`N -> M`), so anything
    # outside `(1, M]` is outside the published rule: at the low end
    # `Z^-1[1 - 1/N]` is `-inf`, and at the high end (any NEGATIVE rho)
    # `DeflatedSharpeResult` refuses `N > M`. Both are real states rather than
    # caller bugs — duplicate trials at one end, a momentum sleeve against a
    # mean-reversion one at the other — and this function's contract is to fail
    # closed on them. The raises are kept where they are (a direct caller
    # passing a nonsense N IS a bug) and converted here.
    #
    # ⚠ Clamping to `M` instead would invent a treatment the paper does not give.
    if not 1.0 < independent <= declared_trials:
        return None
    threshold = expected_max_sharpe(
        trial_sharpe_variance=trial_sharpe_variance,
        independent_trials=independent,
    )

    sharpe = moments.sharpe
    # Equation (2)'s denominator: the estimated standard error of the Sharpe
    # under non-Normal returns. ⚠ `(y4 - 1) / 4` with a RAW y4 — for a Normal
    # (y3 = 0, y4 = 3) the whole bracket collapses to `1 + SR^2 / 2`, which is
    # the classical Lo (2002) result and is what makes this term recognisable.
    variance_term = 1.0 - moments.skewness * sharpe + (moments.kurtosis - 1.0) / 4.0 * sharpe**2
    if variance_term <= 0.0 or not math.isfinite(variance_term):
        return None

    statistic = (sharpe - threshold) * math.sqrt(effective_sample_size - 1.0) / math.sqrt(variance_term)
    if not math.isfinite(statistic):
        return None

    return DeflatedSharpeResult(
        deflated_sharpe=_NORMAL.cdf(statistic),
        expected_max_sharpe=threshold,
        trade_sharpe=sharpe,
        skewness=moments.skewness,
        kurtosis=moments.kurtosis,
        effective_sample_size=effective_sample_size,
        declared_trials=declared_trials,
        independent_trials=independent,
        average_trial_correlation=average_correlation,
        trial_sharpe_variance=trial_sharpe_variance,
        measured_trials=measured_trials,
        trial_register_version=trial_register_version,
    )


__all__ = [
    "DSR_MODEL_ID",
    "EULER_MASCHERONI",
    "MIN_MEASURED_TRIALS",
    "DeflatedSharpeResult",
    "TradeMoments",
    "average_trial_correlation",
    "deflated_sharpe",
    "expected_max_sharpe",
    "implied_independent_trials",
    "trade_moments",
]
