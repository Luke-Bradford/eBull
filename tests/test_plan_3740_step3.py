"""#3740: premise 5's planner, on synthetic 119-month paths only."""

from __future__ import annotations

import math
import random
from collections.abc import Mapping, Sequence
from fractions import Fraction
from statistics import NormalDist

import numpy as np
import pytest

import scripts.plan_3740_step3 as planner
from app.services.factor_book_path import Month, next_month
from app.services.factor_book_series import ARMS
from scripts.plan_3740_step3 import (
    HISTORICAL_MONTHS,
    PREFERENCE,
    ArmFit,
    PlanError,
    arm_fit,
    arm_power,
    bars,
    candidate_sets,
    conjunction_bound,
    draws,
    nearest_rank,
    plan,
    plan_set,
    replicate,
    resample,
    seed,
    simulated_coefficients,
)
from scripts.report_3609_baselines import FACTOR_REGRESSORS
from scripts.report_3609_step2_verdict import G1Result

COLUMNS = (*FACTOR_REGRESSORS, "RF")
THREE = ("value", "GP/A", "investment")


def _months(count: int = HISTORICAL_MONTHS) -> list[Month]:
    out: list[Month] = [(2014, 10)]
    while len(out) < count:
        out.append(next_month(out[-1]))
    return out


def _path(
    loadings: Mapping[str, float], noise: float, *, seed_value: int = 0, count: int = HISTORICAL_MONTHS
) -> tuple[dict[Month, float], dict[str, dict[Month, float]], list[Month]]:
    rng = np.random.default_rng(seed_value)
    months = _months(count)
    factors = {n: dict(zip(months, rng.normal(0.0, 0.03, count).tolist(), strict=True)) for n in COLUMNS}
    residual = rng.normal(0.0, noise, count)
    returns = {
        m: factors["RF"][m] + 0.9 * factors["Mkt-RF"][m] + sum(b * factors[n][m] for n, b in loadings.items()) + e
        for m, e in zip(months, residual, strict=True)
    }
    return returns, factors, months


def _fit(loadings: Mapping[str, float], noise: float = 0.01, seed_value: int = 0) -> ArmFit:
    return arm_fit(*_path(loadings, noise, seed_value=seed_value))


STRONG = {"HML": 0.6, "RMW": 0.6, "CMA": 0.6}


def test_candidate_sets_keep_the_preference_order_and_only_supported_families() -> None:
    assert candidate_sets(THREE) == PREFERENCE
    assert candidate_sets(("investment", "value")) == (("value", "investment"), ("value",), ("investment",))
    assert candidate_sets(()) == ()
    with pytest.raises(PlanError, match="unknown"):
        candidate_sets(("momentum",))


def test_the_seed_joins_the_set_in_the_tables_order_and_names_arm_and_kind() -> None:
    assert seed(("value", "GP/A"), "worst_case", "null") == "3740-step3-planning:value+GP/A:worst_case:null"
    with pytest.raises(PlanError):
        seed(("GP/A", "value"), "worst_case", "null")
    with pytest.raises(PlanError):
        seed(("value",), "middle_case", "null")


def test_draws_are_eight_uniform_starts_per_replication_from_the_runs_own_generator() -> None:
    got = draws(("value",), "best_case", "power", 5)
    rng = random.Random("3740-step3-planning:value:best_case:power")
    assert got == [tuple(rng.randrange(HISTORICAL_MONTHS) for _ in range(8)) for _ in range(5)]
    assert got != draws(("value",), "best_case", "null", 5)
    many = draws(THREE, "worst_case", "null", 2_000)
    starts = [s for r in many for s in r]
    assert min(starts) == 0 and max(starts) == HISTORICAL_MONTHS - 1


def test_blocks_are_three_contiguous_months_wrapping_from_the_last_to_the_first() -> None:
    rows = resample((117, 118, 0, 5, 50, 116, 1, 2))
    assert len(rows) == 24
    assert rows[:9] == (117, 118, 0, 118, 0, 1, 0, 1, 2)
    assert rows[15:18] == (116, 117, 118)


def test_the_nearest_rank_is_computed_exactly() -> None:
    values = [float(v) for v in range(1, 10_001)]
    random.Random(1).shuffle(values)
    assert nearest_rank(values, Fraction(95, 100)) == 9_500.0
    # float(7/25) * 100 rounds above 28, whose ceiling would be 29; the exact rank is 28.
    assert nearest_rank([float(v) for v in range(1, 101)], Fraction(7, 25)) == 28.0
    assert nearest_rank(values, 1 - Fraction(5, 100) / 3) == 9_834.0  # ceil(9,833.33...)
    assert nearest_rank(values, Fraction(1)) == 10_000.0


def test_the_bar_is_the_larger_of_the_normal_bar_and_each_arms_null_quantile() -> None:
    z = NormalDist().inv_cdf(1 - 0.05 / 2)
    low = [(0.1, 0.1)] * 100
    high: list[tuple[float, ...] | None] = [(0.1, float(i)) for i in range(100)]
    got = bars({"best_case": low, "worst_case": high}, ("value", "GP/A"))
    assert got["HML"] == z  # both arms' quantiles are below z
    assert got["RMW"] == 97.0  # rank ceil(0.975 * 100) = 98 of 0..99, in the worst-case arm
    # A refused null replication counts as t = +inf: 3% refused stays outside the 5% tail, 6% reaches the quantile.
    refused: list[tuple[float, ...] | None] = [None] * 3 + [(0.0,)] * 97
    assert bars({"best_case": refused, "worst_case": low}, ("value",))["HML"] == NormalDist().inv_cdf(0.95)
    refused = [None] * 6 + [(0.0,)] * 94
    assert bars({"best_case": refused, "worst_case": low}, ("value",))["HML"] == math.inf


def test_power_needs_every_loading_strictly_above_its_bar_and_refusals_stay_in_the_denominator() -> None:
    bar = {"HML": 2.0, "RMW": 3.0}
    ts: list[tuple[float, ...] | None] = [(2.5, 3.5), (2.0, 3.5), (2.5, 3.0), None, (9.0, 9.0)]
    assert arm_power(ts, ("value", "GP/A"), bar) == Fraction(2, 5)
    assert conjunction_bound({"best_case": Fraction(9, 10), "worst_case": Fraction(85, 100)}) == Fraction(3, 4)


def test_a_bound_of_exactly_the_target_passes() -> None:
    # In floating point, 1 - ((1 - 0.8006) + (1 - 0.9994)) is 0.7999999999999999.
    powers = {"best_case": Fraction(8_006, 10_000), "worst_case": Fraction(9_994, 10_000)}
    assert conjunction_bound(powers) == planner.TARGET_POWER


def test_simulated_coefficients_zero_the_intercept_and_override_only_the_intended_loadings() -> None:
    fit = _fit({"HML": 0.3, "RMW": 0.2})
    got = simulated_coefficients(fit, {"HML": 0.0})
    assert got[0] == 0.0
    expected = [0.0 if n == "HML" else fit.coefficients[n] for n in FACTOR_REGRESSORS]
    assert got[1:].tolist() == expected


def test_a_replication_rebuilds_the_book_from_the_resampled_joint_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    fit = _fit({"HML": 0.3})
    coefficients = simulated_coefficients(fit, {"HML": 0.0})
    indices = resample((118, 0, 40, 41, 7, 7, 90, 3))
    seen: dict[str, object] = {}
    real = planner.g1

    def spy(
        returns: Mapping[Month, float], factors: Mapping[str, Mapping[Month, float]], months: Sequence[Month]
    ) -> G1Result:
        seen.update(returns=returns, factors=factors, months=months)
        return real(returns, factors, months)

    monkeypatch.setattr(planner, "g1", spy)
    assert replicate(fit, coefficients, indices) is not None
    months = seen["months"]
    assert isinstance(months, tuple) and len(months) == 24 and len(set(months)) == 24
    returns, factors = seen["returns"], seen["factors"]
    assert isinstance(returns, dict) and isinstance(factors, dict)
    for position, (month, row) in enumerate(zip(months, indices, strict=True)):
        x = fit.rows[row]
        assert factors["RF"][month] == x[-1]
        assert factors["HML"][month] == x[COLUMNS.index("HML")]
        expected = x[-1] + float(x[:-1] @ coefficients[1:]) + fit.residuals[row]
        assert returns[month] == pytest.approx(expected, abs=1e-15), position


def test_a_replication_g1_refuses_is_none_and_lifts_the_null_bar_to_infinity() -> None:
    # SMB constant at zero in every row: each resample's design is rank-deficient, so G1 refuses every replication.
    # The null's bar is +inf and nothing clears for power.
    base = _fit({"HML": 0.3})
    rows = base.rows.copy()
    rows[:, COLUMNS.index("SMB")] = 0.0
    fit = ArmFit(rows, base.coefficients, base.standard_errors, base.residuals)
    result = plan_set(dict.fromkeys(ARMS, fit), ("value",), replications=50)
    assert result.refused == dict.fromkeys(ARMS, (50, 50))
    assert result.bars["HML"] == math.inf
    assert result.power == dict.fromkeys(ARMS, Fraction(0))
    assert not result.passes


def test_the_historical_fit_refuses_a_path_off_its_119_months_and_g1_refusals() -> None:
    with pytest.raises(PlanError, match="118 months"):
        arm_fit(*_path({"HML": 0.3}, 0.01, count=118))
    returns, factors, months = _path({"HML": 0.3}, 0.01, count=120)
    with pytest.raises(PlanError, match="in order"):  # 119 aligned months, shifted one month later
        arm_fit(returns, factors, months[1:])
    returns, factors, months = _path({"HML": 0.3}, 0.01)
    with pytest.raises(PlanError, match="in order"):
        arm_fit(returns, factors, months[::-1])
    returns, factors, months = _path({"HML": 0.3}, 0.01)
    factors["SMB"][months[5]] = math.nan
    with pytest.raises(PlanError, match="non_finite"):
        arm_fit(returns, factors, months)


def test_strong_loadings_power_the_first_set_and_the_plan_stops_there() -> None:
    asked: list[tuple[str, ...]] = []

    def fits_for(chosen: tuple[str, ...]) -> dict[str, ArmFit]:
        asked.append(chosen)
        return {arm: _fit(STRONG, noise=0.004, seed_value=i) for i, arm in enumerate(ARMS)}

    result = plan(THREE, fits_for, replications=200)
    assert result.chosen == THREE
    assert asked == [THREE]  # later sets are never fitted
    assert [p.label for p in result.evaluated] == ["value+GP/A+investment"]
    only = result.evaluated[0]
    assert only.passes and only.conjunction_bound >= planner.TARGET_POWER
    assert only.conjunction_bound == pytest.approx(conjunction_bound(only.power))
    z = NormalDist().inv_cdf(1 - 0.05 / 3)
    assert all(b >= z for b in only.bars.values())


def test_no_powered_set_evaluates_every_candidate_and_chooses_none() -> None:
    asked: list[tuple[str, ...]] = []

    def fits_for(chosen: tuple[str, ...]) -> dict[str, ArmFit]:
        asked.append(chosen)
        return dict.fromkeys(ARMS, _fit({}, noise=0.03))

    result = plan(("value", "investment"), fits_for, replications=100)
    assert result.chosen is None
    assert asked == [("value", "investment"), ("value",), ("investment",)]
    assert [p.passes for p in result.evaluated] == [False, False, False]


def test_a_set_needs_a_fit_for_each_arm() -> None:
    with pytest.raises(PlanError, match="one fit per arm"):
        plan_set({"worst_case": _fit(STRONG)}, ("value",), replications=10)
    with pytest.raises(PlanError, match="at least one replication"):
        plan_set(dict.fromkeys(ARMS, _fit(STRONG)), ("value",), replications=0)
