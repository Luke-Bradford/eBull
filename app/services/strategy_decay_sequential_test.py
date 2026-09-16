"""The declared repeated-look method for #2500's decay monitor.

#2500's evaluation contract, clause 4: *"Repeated peeking must not reuse an ordinary 95%
interval; use a declared alpha-spending or anytime-valid method."*  Slice 1
(``strategy_monitoring_baseline``) resolved WHICH approved evidence a deployed version is
monitored against and stopped, because the comparison could not be written before this
method existed.  ``MISSING_ENVELOPE_COMPONENTS`` names the gap as
``checkpoint_plan_and_error_budget``.  This module fills it and stops: pure rule, no I/O, no
caller, no gate.

SOURCE RULE -- Howard, Ramdas, McAuliffe & Sekhon (2021), *Time-uniform, nonparametric,
nonasymptotic confidence sequences*, Ann. Statist. 49(2) 1055-1080 (arXiv:1810.08240),
**equation (14)**, the two-sided normal-mixture uniform boundary at ``l0 = 1`` (Definition 1,
scalar case)::

    u(v) = sqrt( (v + rho) * log( (v + rho) / (alpha^2 * rho) ) )

Lemma 2 / Proposition 5 give ``P(exists t : |S_t| >= u(V_t)) <= alpha`` SIMULTANEOUSLY over
all ``t`` -- which is the repeated-look property, and the reason an ordinary interval is
forbidden: a 95% interval re-read after every trade has a false-alarm rate tending to 1.

⚠ Alpha-spending (Lan-DeMets / O'Brien-Fleming) was rejected on SOURCING, not on principle.
It needs a declared maximum sample or a declared infinite allocation shape; a strategy's
firing cadence is ``firing_interarrival_range``, which slice 1 recorded as stored nowhere,
so both would have been invented.

See ``docs/proposals/ta/2026-09-16-2500-sequential-decay-test.md``.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final, Literal

#: Frozen identity of the RULE, not of a run.  A verdict stored under a different id is a
#: different statistical claim and must be re-derived rather than compared across versions
#: -- the same contract ``BASELINE_RULE_VERSION`` carries in slice 1.
#:
#: ⚠ The id alone does NOT identify the claim.  ``alpha``, ``rho``, the declared bounds, the
#: baseline mean and the observation cohort are all part of it, which is why
#: ``SequentialLookVerdict`` carries every one of them back to the caller.
SEQUENTIAL_TEST_ID: Final = "anytime-valid-normal-mixture-v1"

#: ``l0`` from Definition 1.  Written down because it is the difference between eq (14) as
#: published and eq (14) as used: the general form carries ``l0^2`` inside the logarithm and
#: ``l0 = 1`` only in the scalar case, which is ours.
NORMAL_MIXTURE_L0: Final = 1.0

Outcome = Literal["not_rejected", "deteriorated", "refused"]

#: Closed refusal vocabulary, in EVALUATION ORDER so a caller reading the first entry gets
#: the most specific cause.  Every one is a configuration or data fault; none is a verdict.
ALPHA_OUT_OF_RANGE: Final = "alpha_out_of_range"
INVALID_DECLARED_BOUNDS: Final = "invalid_declared_bounds"
INVALID_TUNING_TARGET: Final = "invalid_tuning_target"
BASELINE_OUTSIDE_DECLARED_BOUNDS: Final = "baseline_outside_declared_bounds"
NO_OBSERVATIONS: Final = "no_observations"
NON_FINITE_OBSERVATION: Final = "non_finite_observation"
OBSERVATION_OUTSIDE_DECLARED_BOUNDS: Final = "observation_outside_declared_bounds"
NON_FINITE_BOUNDARY: Final = "non_finite_boundary"


class SequentialTestError(ValueError):
    """A boundary or tuning input that cannot produce a number at all.

    Distinct from a refusal: refusals describe a caller's DECLARATION being unusable and are
    returned by ``evaluate_sequential_look``.  This is raised by the low-level primitives,
    which have no verdict to return and must not invent one.
    """


def mixture_tuning_k(alpha: float) -> float:
    """Solve ``(1 + k) * exp(-k) = alpha^2`` for ``k > 0``.

    Proposition 3(a) tunes ``rho`` so that ``v -> u(v)/sqrt(v)`` is uniquely minimised at a
    target intrinsic time ``m``, via ``m/rho = -W_{-1}(-alpha^2 / (e * l0^2)) - 1``.  There is
    no Lambert W in this project's dependency set and adding scipy for one branch of one
    special function is not justified, so the rule is reduced instead: substituting
    ``w = -(1 + k)`` into ``w * e^w = -alpha^2 / e`` gives the identity above, with
    ``k = m / rho``.

    ⚠ SOLVED IN LOG SPACE -- ``log1p(k) - k = 2 * log(alpha)`` -- and not on the identity as
    written.  ``alpha^2`` underflows to exactly 0.0 below ``alpha ~ 1.5e-154`` and
    ``exp(-k)`` underflows for ``k > 745``, either of which turns a well-posed root into a
    comparison of two zeros.  The log form is exact over the whole domain and is strictly
    decreasing in ``k`` (derivative ``1/(1+k) - 1 = -k/(1+k) < 0``), so the root is unique.
    """
    if not math.isfinite(alpha) or not 0.0 < alpha < 1.0:
        raise SequentialTestError(f"alpha must be finite and in (0, 1); got {alpha!r}")
    target = 2.0 * math.log(alpha)

    def residual(k: float) -> float:
        return math.log1p(k) - k

    high = 1.0
    # Bracket by doubling.  Bounded: `residual` falls without limit, and `alpha > 0` puts a
    # finite floor under `target`, so this terminates for every admissible alpha.  The cap is
    # a backstop against a non-finite target sneaking past the guard above, not a tolerance.
    for _ in range(4096):
        if residual(high) <= target:
            break
        high *= 2.0
    else:  # pragma: no cover - unreachable while the alpha guard holds
        raise SequentialTestError(f"could not bracket the tuning root for alpha={alpha!r}")
    low = 0.0
    # 200 halvings exhausts float64 precision on any bracket this can produce; bisection is
    # used rather than Newton because it cannot diverge and needs no derivative guard.
    for _ in range(200):
        mid = (low + high) / 2.0
        if residual(mid) > target:
            low = mid
        else:
            high = mid
    return (low + high) / 2.0


def mixture_rho(*, target_intrinsic_time: float, alpha: float) -> float:
    """``rho = m / k`` -- the mixture tuned to be tightest at intrinsic time ``m``.

    ⚠⚠ ``rho`` is a function of ``(alpha, m)`` ALONE, and both are declarations frozen before
    the first look.  Re-deriving it after seeing outcomes voids the guarantee: a boundary
    chosen to fit the data it judges is no longer a boundary the data had to clear.
    """
    if not math.isfinite(target_intrinsic_time) or target_intrinsic_time <= 0.0:
        raise SequentialTestError(f"target intrinsic time must be finite and positive; got {target_intrinsic_time!r}")
    rho = target_intrinsic_time / mixture_tuning_k(alpha)
    if not math.isfinite(rho) or rho <= 0.0:
        raise SequentialTestError("tuned rho is not a usable positive number")
    return rho


def uniform_boundary(intrinsic_time: float, *, rho: float, alpha: float) -> float:
    """Equation (14) at ``l0 = 1``: the value ``|S_t|`` must stay below at intrinsic time ``v``.

    ⚠ The logarithm is evaluated as ``log(v + rho) - log(rho) - 2*log(alpha)`` rather than as
    ``log((v + rho) / (alpha^2 * rho))``.  The literal form divides by ``alpha^2 * rho``,
    which underflows to zero for small alpha and overflows for large ``v + rho``; the
    difference-of-logs is exact wherever the inputs themselves are finite.

    ⚠ The logarithm's argument is ``(v + rho) / (alpha^2 * rho)``, which for ``v >= 0`` and
    ``alpha < 1`` always exceeds 1, so the square root's argument is always positive.  It is
    still checked -- a non-finite result from finite inputs is a real failure mode, and the
    caller turns it into ``non_finite_boundary`` rather than a verdict.
    """
    if not math.isfinite(alpha) or not 0.0 < alpha < 1.0:
        raise SequentialTestError(f"alpha must be finite and in (0, 1); got {alpha!r}")
    if not math.isfinite(rho) or rho <= 0.0:
        raise SequentialTestError(f"rho must be finite and positive; got {rho!r}")
    if not math.isfinite(intrinsic_time) or intrinsic_time < 0.0:
        raise SequentialTestError(f"intrinsic time must be finite and non-negative; got {intrinsic_time!r}")
    shifted = intrinsic_time + rho
    log_term = math.log(shifted) - math.log(rho) - 2.0 * math.log(alpha) + 2.0 * math.log(NORMAL_MIXTURE_L0)
    if log_term <= 0.0:  # pragma: no cover - unreachable for v >= 0 and alpha < 1
        raise SequentialTestError("boundary logarithm is non-positive")
    value = math.sqrt(shifted * log_term)
    if not math.isfinite(value):
        raise SequentialTestError("boundary is not finite")
    return value


def hoeffding_variance_proxy(*, lower_bound: float, upper_bound: float) -> float:
    """``(b - a)^2 / 4`` -- Hoeffding's lemma, and the only reason eq (14) applies here.

    Hoeffding (1963); Boucheron-Lugosi-Massart Lemma 2.2: ``X in [a, b]`` almost surely
    implies ``X - E[X]`` is sub-Gaussian with variance proxy ``(b - a)^2 / 4``.  Applied
    conditionally it makes each martingale increment conditionally sub-Gaussian, which is what
    a uniform boundary needs.

    ⚠⚠ WE DO NOT ASSUME SUB-GAUSSIANITY AND COULD NOT.  The 2026-08-22 cut-and-reset measured
    skew 36 and kurtosis 1,976 on s8's per-trade returns.  Those moments do not themselves
    prove non-sub-Gaussianity -- a bounded variable can have arbitrarily large standardised
    kurtosis -- but they remove any basis for picking a proxy by inspection, which is exactly
    why the proxy has to come from a bound rather than from the data.

    ⚠⚠ AND THE BOUND MUST BE A TRUE ALMOST-SURE BOUND.  Policing it afterwards does not make
    it one: Codex checkpoint 1's counterexample is ``X = -1`` w.p. 0.99 and ``X = +99``
    otherwise -- mean zero, declared ``[-1, 1]`` -- where 17 consecutive losses cross the lower
    boundary before the ``+99`` that would trigger the refusal ever arrives, with probability
    ``0.99^17 ~ 84%``.  The caller must therefore widen the strategy's TP/SL contract by the
    pinned ``worst_gap_pct`` rather than pass the naive barriers.
    """
    if not math.isfinite(lower_bound) or not math.isfinite(upper_bound):
        raise SequentialTestError("declared bounds must both be finite")
    if not lower_bound < upper_bound:
        raise SequentialTestError(f"declared bounds must satisfy lower < upper; got [{lower_bound}, {upper_bound}]")
    width = upper_bound - lower_bound
    proxy = (width * width) / 4.0
    if not math.isfinite(proxy) or proxy <= 0.0:
        raise SequentialTestError("variance proxy is not a usable positive number")
    return proxy


@dataclass(frozen=True)
class SequentialLookVerdict:
    """One look's outcome plus everything needed to reproduce the claim.

    ⚠ A method id alone does not identify a statistical claim, so the declaration
    (``alpha``, ``rho``, target, bounds, baseline) travels with the verdict.  A stored row
    carrying only ``outcome`` could never be re-derived.
    """

    test_id: str
    outcome: Outcome
    refusals: tuple[str, ...]
    alpha: float
    baseline_mean: float
    lower_bound: float
    upper_bound: float
    tuning_target_trades: int
    variance_proxy: float | None
    rho: float | None
    observation_count: int
    intrinsic_time: float | None
    boundary: float | None
    worst_prefix_index: int | None
    worst_prefix_sum: float | None
    worst_prefix_margin: float | None


def _refused(
    reason: str,
    *,
    alpha: float,
    baseline_mean: float,
    lower_bound: float,
    upper_bound: float,
    tuning_target_trades: int,
    observation_count: int,
    variance_proxy: float | None = None,
    rho: float | None = None,
) -> SequentialLookVerdict:
    return SequentialLookVerdict(
        test_id=SEQUENTIAL_TEST_ID,
        outcome="refused",
        refusals=(reason,),
        alpha=alpha,
        baseline_mean=baseline_mean,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        tuning_target_trades=tuning_target_trades,
        variance_proxy=variance_proxy,
        rho=rho,
        observation_count=observation_count,
        intrinsic_time=None,
        boundary=None,
        worst_prefix_index=None,
        worst_prefix_sum=None,
        worst_prefix_margin=None,
    )


def evaluate_sequential_look(
    *,
    observations: Sequence[float],
    baseline_mean: float,
    lower_bound: float,
    upper_bound: float,
    alpha: float,
    tuning_target_trades: int,
) -> SequentialLookVerdict:
    """Has the net-return stream fallen below its approved expectancy, at any look so far?

    THE NULL IS CONDITIONAL::

        H0:  E[X_i | F_{i-1}] >= baseline_mean   almost surely, for every i

    ⚠⚠ Conditional on the filtration, NOT marginal, and that distinction is load-bearing.
    Codex checkpoint 1 killed the marginal version in one line: if ``X_i = Z`` for every trade
    with ``Z`` a fair sign, every marginal mean is zero yet the lower boundary is crossed with
    probability 1/2.  Overlapping positions exposed to one market shock, outcome-dependent
    resolution ordering and selective inclusion of resolved trades all break the conditional
    null; the caller owes a stream whose increments are adapted and one-per-resolution.

    Under it, ``X_i - m0 = (X_i - E[X_i|F_{i-1}]) + (E[X_i|F_{i-1}] - m0)``: the first term is
    a bounded martingale difference, the second is non-negative.  So ``S_n`` dominates the
    pure martingale ``M_n`` and ``P(exists n : S_n <= -u(V_n)) <= P(exists n : M_n <= -u(V_n))
    <= alpha``.

    ⚠ THE TWO-SIDED BOUND APPLIES TO ``M_n``, NOT TO ``S_n``.  Under a composite null with
    strictly positive drift the UPWARD crossing of ``S_n`` can be certain, so
    ``P(either crossing of S_n) <= alpha`` is false.  Only the lower-tail domination is
    claimed.  No factor of two is needed -- the two-sided boundary already covers each tail at
    level ``alpha``.

    ⚠ EVERY PREFIX IS CHECKED, not only ``n``.  A crossing latches: without this, a sum that
    crosses at ``k`` and recovers by ``n`` would report ``not_rejected``, which silently
    converts the time-uniform guarantee back into the single-look one #2500 forbids.

    ⚠ ``not_rejected`` IS NOT HEALTH.  It is named for what it is.  It does not establish
    unchanged expectancy, it tests cumulative expectancy rather than recent change (a long
    favourable history can mask later decay), and it says nothing about the other envelope
    components.  Nothing here has a production default: ``alpha``, the target and the bounds
    are declarations, and a default would be the invented constant this slice exists to avoid.
    """
    count = len(observations)
    fixed = {
        "alpha": alpha,
        "baseline_mean": baseline_mean,
        "lower_bound": lower_bound,
        "upper_bound": upper_bound,
        "tuning_target_trades": tuning_target_trades,
        "observation_count": count,
    }
    if not math.isfinite(alpha) or not 0.0 < alpha < 1.0:
        return _refused(ALPHA_OUT_OF_RANGE, **fixed)
    try:
        proxy = hoeffding_variance_proxy(lower_bound=lower_bound, upper_bound=upper_bound)
    except SequentialTestError:
        return _refused(INVALID_DECLARED_BOUNDS, **fixed)
    if tuning_target_trades <= 0:
        return _refused(INVALID_TUNING_TARGET, variance_proxy=proxy, **fixed)
    if not math.isfinite(baseline_mean) or not lower_bound <= baseline_mean <= upper_bound:
        # A pinned expectancy outside the declared range makes the whole comparison
        # incoherent -- every observation would be centred against an unreachable number.
        return _refused(BASELINE_OUTSIDE_DECLARED_BOUNDS, variance_proxy=proxy, **fixed)
    try:
        rho = mixture_rho(target_intrinsic_time=tuning_target_trades * proxy, alpha=alpha)
    except SequentialTestError:
        return _refused(INVALID_TUNING_TARGET, variance_proxy=proxy, **fixed)
    if count == 0:
        return _refused(NO_OBSERVATIONS, variance_proxy=proxy, rho=rho, **fixed)

    total = 0.0
    worst_margin: float | None = None
    worst_index = 0
    worst_sum = 0.0
    boundary_at_n = 0.0
    for index, raw in enumerate(observations, start=1):
        value = float(raw)
        if not math.isfinite(value):
            return _refused(NON_FINITE_OBSERVATION, variance_proxy=proxy, rho=rho, **fixed)
        if not lower_bound <= value <= upper_bound:
            # #2500 evaluation-contract class 1 (implementation/data drift), NOT deterioration.
            # Widening the bound to admit it would convert a broken barrier contract into a
            # wider tolerance, which is weakening a gate.
            return _refused(OBSERVATION_OUTSIDE_DECLARED_BOUNDS, variance_proxy=proxy, rho=rho, **fixed)
        total += value - baseline_mean
        try:
            boundary_at_n = uniform_boundary(index * proxy, rho=rho, alpha=alpha)
        except SequentialTestError:
            return _refused(NON_FINITE_BOUNDARY, variance_proxy=proxy, rho=rho, **fixed)
        margin = total + boundary_at_n
        if worst_margin is None or margin < worst_margin:
            worst_margin = margin
            worst_index = index
            worst_sum = total

    assert worst_margin is not None
    return SequentialLookVerdict(
        test_id=SEQUENTIAL_TEST_ID,
        outcome="deteriorated" if worst_margin <= 0.0 else "not_rejected",
        refusals=(),
        alpha=alpha,
        baseline_mean=baseline_mean,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        tuning_target_trades=tuning_target_trades,
        variance_proxy=proxy,
        rho=rho,
        observation_count=count,
        intrinsic_time=count * proxy,
        boundary=boundary_at_n,
        worst_prefix_index=worst_index,
        worst_prefix_sum=worst_sum,
        worst_prefix_margin=worst_margin,
    )


__all__ = [
    "ALPHA_OUT_OF_RANGE",
    "BASELINE_OUTSIDE_DECLARED_BOUNDS",
    "INVALID_DECLARED_BOUNDS",
    "INVALID_TUNING_TARGET",
    "NON_FINITE_BOUNDARY",
    "NON_FINITE_OBSERVATION",
    "NORMAL_MIXTURE_L0",
    "NO_OBSERVATIONS",
    "OBSERVATION_OUTSIDE_DECLARED_BOUNDS",
    "SEQUENTIAL_TEST_ID",
    "Outcome",
    "SequentialLookVerdict",
    "SequentialTestError",
    "evaluate_sequential_look",
    "hoeffding_variance_proxy",
    "mixture_rho",
    "mixture_tuning_k",
    "uniform_boundary",
]
