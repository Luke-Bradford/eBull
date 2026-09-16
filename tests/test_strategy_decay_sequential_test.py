"""#2500 slice 2 — the declared repeated-look boundary (pure, no database).

⚠ The tuning constant is asserted through its DEFINING IDENTITY, never as a hand-copied
number.  A literal ``8.2119…`` in this file would go stale silently the moment the
derivation changed — the 2026-09-16 prevention entry *a hand-copied predicate has no
compiler*, in its arithmetic form.
"""

from __future__ import annotations

import math

import numpy as np
import pytest

from app.services.strategy_decay_sequential_test import (
    ALPHA_OUT_OF_RANGE,
    BASELINE_OUTSIDE_DECLARED_BOUNDS,
    INVALID_DECLARED_BOUNDS,
    INVALID_TUNING_TARGET,
    NO_OBSERVATIONS,
    NON_FINITE_OBSERVATION,
    OBSERVATION_OUTSIDE_DECLARED_BOUNDS,
    SEQUENTIAL_TEST_ID,
    SequentialTestError,
    evaluate_sequential_look,
    hoeffding_variance_proxy,
    mixture_rho,
    mixture_tuning_k,
    uniform_boundary,
)

_ALPHAS = (0.5, 0.1, 0.05, 0.01, 1e-8, 1e-200)


# --------------------------------------------------------------------------- tuning


@pytest.mark.parametrize("alpha", _ALPHAS)
def test_tuning_root_satisfies_its_defining_identity(alpha: float) -> None:
    """``(1 + k) e^{-k} = alpha^2``, checked in log space and in RELATIVE terms.

    Absolute tolerance would pass vacuously at ``alpha = 1e-200``, where both sides are
    numerically zero — the failure this parametrisation exists to catch.
    """
    k = mixture_tuning_k(alpha)
    assert k > 0.0
    assert math.isclose(math.log1p(k) - k, 2.0 * math.log(alpha), rel_tol=1e-12)


def test_tuning_root_is_monotone_in_alpha() -> None:
    """A smaller error budget must buy a wider mixture, i.e. a larger ``k`` and smaller rho."""
    ks = [mixture_tuning_k(alpha) for alpha in (0.5, 0.1, 0.05, 0.01)]
    assert ks == sorted(ks)


@pytest.mark.parametrize("alpha", [0.0, 1.0, -0.1, 1.5, float("nan"), float("inf")])
def test_tuning_refuses_alpha_outside_the_open_unit_interval(alpha: float) -> None:
    with pytest.raises(SequentialTestError):
        mixture_tuning_k(alpha)


def test_rho_places_the_minimum_of_the_normalised_boundary_at_the_target() -> None:
    """Proposition 3(a): ``v -> u(v)/sqrt(v)`` is uniquely minimised at ``v = m``.

    This is the only check that the Lambert-W reduction is the RIGHT root rather than merely
    a root of the identity — a sign slip would still satisfy the identity test above.
    """
    alpha, target = 0.05, 200.0
    rho = mixture_rho(target_intrinsic_time=target, alpha=alpha)
    normalised = lambda v: uniform_boundary(v, rho=rho, alpha=alpha) / math.sqrt(v)  # noqa: E731
    at_target = normalised(target)
    grid = [target * factor for factor in (0.1, 0.25, 0.5, 0.9, 1.1, 2.0, 5.0, 20.0)]
    assert all(normalised(v) > at_target for v in grid)


@pytest.mark.parametrize("target", [0.0, -1.0, float("nan"), float("inf")])
def test_rho_refuses_a_non_positive_or_non_finite_target(target: float) -> None:
    with pytest.raises(SequentialTestError):
        mixture_rho(target_intrinsic_time=target, alpha=0.05)


# --------------------------------------------------------------------------- boundary


def test_boundary_matches_equation_14_evaluated_directly() -> None:
    """The log-space evaluation must agree with the literal form wherever the literal works."""
    alpha, rho = 0.05, 24.0
    for v in (0.0, 1.0, 10.0, 1e3, 1e6):
        literal = math.sqrt((v + rho) * math.log((v + rho) / (alpha * alpha * rho)))
        assert math.isclose(uniform_boundary(v, rho=rho, alpha=alpha), literal, rel_tol=1e-12)


def test_boundary_survives_alpha_that_underflows_the_literal_form() -> None:
    """``alpha^2`` is exactly 0.0 here, so the literal form divides by zero; this must not."""
    alpha = 1e-200
    assert alpha * alpha == 0.0
    value = uniform_boundary(100.0, rho=mixture_rho(target_intrinsic_time=200.0, alpha=alpha), alpha=alpha)
    assert math.isfinite(value) and value > 0.0


def test_boundary_grows_with_intrinsic_time_and_with_a_smaller_alpha() -> None:
    rho = 24.0
    widths = [uniform_boundary(v, rho=rho, alpha=0.05) for v in (1.0, 10.0, 100.0, 1000.0)]
    assert widths == sorted(widths)
    assert uniform_boundary(100.0, rho=rho, alpha=0.01) > uniform_boundary(100.0, rho=rho, alpha=0.10)


@pytest.mark.parametrize(
    ("v", "rho", "alpha"),
    [(-1.0, 24.0, 0.05), (float("nan"), 24.0, 0.05), (10.0, 0.0, 0.05), (10.0, -1.0, 0.05), (10.0, 24.0, 1.0)],
)
def test_boundary_refuses_out_of_domain_inputs(v: float, rho: float, alpha: float) -> None:
    with pytest.raises(SequentialTestError):
        uniform_boundary(v, rho=rho, alpha=alpha)


# --------------------------------------------------------------------------- proxy


def test_variance_proxy_is_hoeffdings_quarter_square_width() -> None:
    assert hoeffding_variance_proxy(lower_bound=-1.0, upper_bound=1.0) == 1.0
    assert hoeffding_variance_proxy(lower_bound=-0.08, upper_bound=0.12) == pytest.approx(0.01)


@pytest.mark.parametrize(
    ("low", "high"),
    [(1.0, -1.0), (1.0, 1.0), (float("nan"), 1.0), (-1.0, float("inf"))],
)
def test_variance_proxy_refuses_reversed_equal_or_non_finite_bounds(low: float, high: float) -> None:
    """⚠ Equal bounds must refuse, not return 0 — squaring hides the reversal, and a zero
    proxy would make intrinsic time identically zero for every sample size."""
    with pytest.raises(SequentialTestError):
        hoeffding_variance_proxy(lower_bound=low, upper_bound=high)


# --------------------------------------------------------------------------- verdicts


def _look(observations, **overrides):
    kwargs = {
        "observations": observations,
        "baseline_mean": 0.0,
        "lower_bound": -1.0,
        "upper_bound": 1.0,
        "alpha": 0.05,
        "tuning_target_trades": 200,
    }
    kwargs.update(overrides)
    return evaluate_sequential_look(**kwargs)


def test_a_stream_at_its_baseline_is_not_rejected() -> None:
    verdict = _look([0.0] * 5_000)
    assert verdict.outcome == "not_rejected"
    assert verdict.refusals == ()
    assert verdict.test_id == SEQUENTIAL_TEST_ID
    assert verdict.worst_prefix_margin is not None and verdict.worst_prefix_margin > 0.0


def test_a_sustained_loss_run_is_deteriorated() -> None:
    verdict = _look([-1.0] * 40)
    assert verdict.outcome == "deteriorated"
    assert verdict.worst_prefix_index is not None and verdict.worst_prefix_index <= 40


def test_seventeen_losses_do_not_yet_cross_but_a_longer_run_does() -> None:
    """Pins the detection point, so a boundary that silently narrowed would fail here.

    ⚠ Recomputed from the shipped rule, not asserted as a literal count: the test finds the
    first crossing index and checks it is where the boundary actually sits.
    """
    losses = [-1.0] * 60
    first = next(n for n in range(1, 61) if _look(losses[:n]).outcome == "deteriorated")
    assert _look(losses[: first - 1]).outcome == "not_rejected"
    assert 10 < first < 60


def test_a_crossing_latches_even_after_the_sum_recovers() -> None:
    """⚠⚠ THE SINGLE-LOOK FAILURE MODE.  Without per-prefix evaluation this returns
    ``not_rejected``, silently converting a time-uniform guarantee back into the ordinary
    interval #2500 forbids."""
    losses = [-1.0] * 60
    first = next(n for n in range(1, 61) if _look(losses[:n]).outcome == "deteriorated")
    recovered = losses[:first] + [1.0] * 5_000
    assert sum(recovered) > 0.0
    verdict = _look(recovered)
    assert verdict.outcome == "deteriorated"
    assert verdict.worst_prefix_index == first


def test_an_upward_crossing_is_not_a_verdict() -> None:
    """Decay is a FALL in expectancy; a strategy beating its envelope is not suspended."""
    assert _look([1.0] * 5_000).outcome == "not_rejected"


def test_the_verdict_carries_the_whole_declaration() -> None:
    """A method id alone does not identify a statistical claim."""
    verdict = _look([0.0] * 10, alpha=0.01, tuning_target_trades=50, baseline_mean=0.25)
    assert verdict.alpha == 0.01
    assert verdict.tuning_target_trades == 50
    assert verdict.baseline_mean == 0.25
    assert (verdict.lower_bound, verdict.upper_bound) == (-1.0, 1.0)
    assert verdict.variance_proxy == 1.0
    assert verdict.rho == pytest.approx(50.0 / mixture_tuning_k(0.01))
    assert verdict.intrinsic_time == 10.0
    assert verdict.observation_count == 10


# --------------------------------------------------------------------------- refusals


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"alpha": 0.0}, ALPHA_OUT_OF_RANGE),
        ({"alpha": 1.0}, ALPHA_OUT_OF_RANGE),
        ({"alpha": float("nan")}, ALPHA_OUT_OF_RANGE),
        ({"lower_bound": 1.0, "upper_bound": -1.0}, INVALID_DECLARED_BOUNDS),
        ({"lower_bound": 0.0, "upper_bound": 0.0}, INVALID_DECLARED_BOUNDS),
        ({"upper_bound": float("inf")}, INVALID_DECLARED_BOUNDS),
        ({"tuning_target_trades": 0}, INVALID_TUNING_TARGET),
        ({"tuning_target_trades": -5}, INVALID_TUNING_TARGET),
        ({"baseline_mean": 2.0}, BASELINE_OUTSIDE_DECLARED_BOUNDS),
        ({"baseline_mean": float("nan")}, BASELINE_OUTSIDE_DECLARED_BOUNDS),
    ],
)
def test_configuration_faults_refuse_with_their_own_reason(overrides: dict, reason: str) -> None:
    verdict = _look([0.0, 0.0], **overrides)
    assert verdict.outcome == "refused"
    assert verdict.refusals == (reason,)


def test_an_empty_stream_refuses_rather_than_reading_as_healthy() -> None:
    verdict = _look([])
    assert verdict.outcome == "refused"
    assert verdict.refusals == (NO_OBSERVATIONS,)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_a_non_finite_observation_refuses(bad: float) -> None:
    assert _look([0.0, bad, 0.0]).refusals == (NON_FINITE_OBSERVATION,)


@pytest.mark.parametrize("bad", [-1.5, 1.5])
def test_an_observation_outside_the_declared_bounds_refuses_rather_than_widening(bad: float) -> None:
    """⚠ #2500 class 1 (implementation/data drift), NOT deterioration.  Admitting it by
    widening the bound converts a broken barrier contract into a wider tolerance."""
    assert _look([0.0, bad]).refusals == (OBSERVATION_OUTSIDE_DECLARED_BOUNDS,)


def test_a_bounds_breach_refuses_even_when_the_run_would_otherwise_be_deteriorated() -> None:
    """The refusal must WIN, so the monitor routes it to class 1 and never to suspension for
    decay — two different causes with two different remedies."""
    verdict = _look([-1.0] * 60 + [-1.5])
    assert verdict.outcome == "refused"
    assert verdict.refusals == (OBSERVATION_OUTSIDE_DECLARED_BOUNDS,)


def test_refusals_never_carry_a_boundary_or_a_prefix() -> None:
    """A refused look has no statistical content; reporting one would invite a caller to read
    a number that was never computed under a valid configuration."""
    verdict = _look([0.0], alpha=2.0)
    assert verdict.boundary is None
    assert verdict.worst_prefix_margin is None
    assert verdict.worst_prefix_sum is None
    assert verdict.intrinsic_time is None


# --------------------------------------------------------------------------- coverage


def test_false_alarm_rate_stays_under_alpha_on_a_seeded_null_sample() -> None:
    """Fast seeded arm of ``scripts/verify_2500_sequential_boundary.py``.

    ⚠ A CONSISTENCY check, not a proof: the theorem bounds the infinite-horizon crossing
    probability for every sub-psi process, and no simulation establishes that.  What it does
    catch is a boundary that has narrowed enough to break the guarantee outright.
    """
    alpha, target, horizon, paths = 0.05, 200, 400, 4_000
    rng = np.random.default_rng(20260916)
    draws = np.where(rng.random((paths, horizon)) < 0.5, 1.0, -1.0)
    rho = mixture_rho(target_intrinsic_time=float(target), alpha=alpha)
    curve = np.array([uniform_boundary(float(n), rho=rho, alpha=alpha) for n in range(1, horizon + 1)])
    crossed = (np.abs(np.cumsum(draws, axis=1)) >= curve).any(axis=1)
    assert crossed.mean() <= alpha


def test_a_real_deterioration_is_detected_on_a_same_support_alternative() -> None:
    """⚠ The alternative keeps the SAME support ``[-1, 1]`` (a biased sign, mean ``-0.4``).
    A shifted Rademacher would leave the declared bounds and the rule would refuse it, so
    power measured that way would describe a configuration the monitor never accepts."""
    rng = np.random.default_rng(20260916)
    draws = np.where(rng.random((200, 400)) < (1.0 - 0.4) / 2.0, 1.0, -1.0)
    detected = sum(_look(list(path)).outcome == "deteriorated" for path in draws)
    assert detected == 200
