"""#3609 step 2 slice 3c-iv(d): the verdict, on synthetic series only (no corpus, no stage B)."""

from __future__ import annotations

import math
import random
from collections.abc import Callable, Mapping
from datetime import date

import numpy as np
import pytest

import scripts.report_3609_step2_verdict as verdict_module
from app.services.factor_book import BookRefusal
from app.services.factor_book_path import BoundaryState, Month, PathResult, next_month
from app.services.factor_book_references import B1Path
from app.services.factor_book_series import ARMS, Scenario, SeriesRun, Summary
from scripts.report_3609_baselines import FACTOR_REGRESSORS, Regression
from scripts.report_3609_step2_verdict import (
    G1_CRITICAL,
    TURNOVER_VETO,
    g1,
    log_growth,
    stage_b_months,
    stage_b_returns,
    verdict,
)

COSTS: tuple[str, ...] = ("gross", "net", "stress_2x")


def _months() -> tuple[Month, ...]:
    out = [(2014, 10)]
    while out[-1] < (2024, 8):
        out.append(next_month(out[-1]))
    return tuple(out)


MONTHS = _months()
STAGE_B = [m for m in MONTHS if m >= (2021, 6)]


def _factors(seed: int = 7) -> dict[str, dict[Month, float]]:
    rng = random.Random(seed)
    out = {name: {m: rng.gauss(0.0, 0.03) for m in MONTHS} for name in FACTOR_REGRESSORS}
    out["RF"] = dict.fromkeys(MONTHS, 0.001)
    return out


FACTORS = _factors()


def _loaded(loadings: Mapping[str, float], bump: Callable[[Month], float] = lambda m: 0.0) -> dict[Month, float]:
    """RF + loadings · factors + seeded noise + ``bump``."""
    rng = random.Random(11)
    return {
        m: FACTORS["RF"][m] + sum(b * FACTORS[n][m] for n, b in loadings.items()) + rng.gauss(0.0, 0.005) + bump(m)
        for m in MONTHS
    }


GOOD = {"HML": 1.0, "RMW": 1.0, "CMA": 1.0}
BOUNDARY: Month = (2021, 5)
STATE = BoundaryState(date(2021, 5, 31), (), 1.0, 1.0)


def _run(
    book: Mapping[str, dict[Month, float]] | None = None,
    *,
    b1: float = 0.0,
    control: tuple[float, ...] = (-0.01, 0.0, 0.001),
    turnover: Mapping[Month, float] | None = None,
    insufficient: tuple[date, ...] = (),
    boundary_cost: float = 0.0,
    control_boundary_cost: float = 0.0,
) -> SeriesRun:
    """``book`` per cost scenario (default: a strong-loading book with a 2% monthly bump, every cost alike);
    B1 and each control draw are constant monthly returns. Every path's boundary is 2021-05 at a pre-trade NAV of
    1; the book's trades there cost ``boundary_cost`` of it, each draw's ``control_boundary_cost``."""
    book = book or dict.fromkeys(COSTS, _loaded(GOOD, lambda m: 0.02))
    flat = {m: 0.0 for m in MONTHS}
    scenarios: list[Scenario] = [(arm, cost) for arm in ARMS for cost in COSTS]

    def path(returns: Mapping[Month, float], cost: float = 0.0) -> PathResult:
        return PathResult(
            returns=dict(returns), turnover=dict(turnover or {}), nav={BOUNDARY: 1.0 - cost}, boundary=STATE
        )

    def draw(r: float) -> Summary:
        return Summary(dict.fromkeys(MONTHS, r), {}, {BOUNDARY: 1.0 - control_boundary_cost}, {}, None, STATE)

    return SeriesRun(
        months=MONTHS,
        book={s: path(book[s[1]], boundary_cost) for s in scenarios},
        equal_weight={s: path(flat) for s in scenarios},
        cap_weighted={s: path(flat) for s in scenarios},
        control={s: tuple(draw(r) for r in control) for s in scenarios},
        b1={c: B1Path(dict.fromkeys(MONTHS, b1), "band", 0.0, 0.0) for c in COSTS},
        pools=(),
        insufficient=insufficient,
    )


def test_the_g1_critical_value_is_the_specs() -> None:
    assert round(G1_CRITICAL, 3) == 2.128


def test_g1_passes_on_three_strong_positive_loadings_and_fails_on_a_negative_one() -> None:
    result = g1(_loaded(GOOD), FACTORS, MONTHS)
    assert result.refusal is None and result.passed and result.lag == 4
    assert result.coefficients["HML"] == pytest.approx(1.0, abs=0.05)
    assert all(result.t_stats[n] > G1_CRITICAL for n in ("HML", "RMW", "CMA"))
    assert not g1(_loaded({**GOOD, "CMA": -1.0}), FACTORS, MONTHS).passed
    assert not g1(_loaded({"HML": 1.0, "RMW": 1.0}), FACTORS, MONTHS).passed  # CMA unloaded: t below the bar


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda r, f: f["CMA"].pop((2019, 1)), "missing_factor_month"),
        (lambda r, f: f["RF"].__setitem__((2019, 1), math.nan), "non_finite"),
        (lambda r, f: r.__setitem__((2024, 9), 0.0), "misaligned"),
        (lambda r, f: r.pop((2019, 1)), "misaligned"),
        (lambda r, f: f["SMB"].update(f["HML"]), "rank_deficient"),
    ],
)
def test_g1_refusals(mutate: Callable[[dict, dict], object], reason: str) -> None:
    returns = _loaded(GOOD)
    factors = {n: dict(v) for n, v in _factors().items()}
    mutate(returns, factors)
    result = g1(returns, factors, MONTHS)
    assert result.refusal is not None and result.refusal.startswith(reason) and not result.passed


def _stub_fit(monkeypatch: pytest.MonkeyPatch, errors: list[float], loadings: tuple[str, ...] = ("HML",)) -> None:
    """The fit returns a coefficient of 1 on each of ``loadings`` and 0 elsewhere (y = HML exactly by default: the
    float solve leaves a ~1e-17 residual, so the exact-zero check is tested on a stubbed fit), with the given
    standard errors."""
    coefficients = np.array([0.0, *(1.0 if n in loadings else 0.0 for n in FACTOR_REGRESSORS)])

    def fit(y: np.ndarray, x: np.ndarray) -> Regression:
        return Regression(coefficients, coefficients / np.array(errors), len(y), 4, np.array(errors))

    monkeypatch.setattr(verdict_module, "ols_newey_west", fit)


def test_g1_refuses_zero_residual_variance(monkeypatch: pytest.MonkeyPatch) -> None:
    factors = {**FACTORS, "RF": dict.fromkeys(MONTHS, 0.0)}
    _stub_fit(monkeypatch, [1.0] * 7)
    assert g1(dict(factors["HML"]), factors, MONTHS).refusal == "zero_residual_variance"


@pytest.mark.parametrize(("t", "passed"), [(2.12, False), (2.13, True)])
def test_g1_needs_each_t_above_the_bonferroni_bar(monkeypatch: pytest.MonkeyPatch, t: float, passed: bool) -> None:
    errors = [1.0] * 7
    errors[1 + FACTOR_REGRESSORS.index("CMA")] = 1.0 / t  # coefficient 1, so CMA's t is ``t``; the others are 1.0 / 1
    errors[1 + FACTOR_REGRESSORS.index("HML")] = errors[1 + FACTOR_REGRESSORS.index("RMW")] = 0.1
    _stub_fit(monkeypatch, errors, ("HML", "RMW", "CMA"))
    result = g1(_loaded(GOOD), FACTORS, MONTHS)
    assert result.refusal is None and result.t_stats["CMA"] == pytest.approx(t) and result.passed is passed


@pytest.mark.parametrize("bad", [0.0, -1.0, math.nan, math.inf])
def test_g1_refuses_a_newey_west_error_not_finite_and_positive(monkeypatch: pytest.MonkeyPatch, bad: float) -> None:
    _stub_fit(monkeypatch, [1.0, 1.0, 1.0, bad, 1.0, 1.0, 1.0])
    result = g1(_loaded(GOOD), FACTORS, MONTHS)  # a real residual, so only the error check can refuse
    assert result.refusal is not None and result.refusal.startswith("newey_west_se_invalid")


def test_log_growth_is_the_specs_formula_and_refuses_a_non_finite_g() -> None:
    returns = {(2021, 6): 0.1, (2021, 7): -0.05}
    assert log_growth(returns, list(returns), "x") == 6.0 * (math.log(1.1) + math.log(0.95))
    with pytest.raises(BookRefusal) as caught:
        log_growth({(2021, 6): math.inf}, [(2021, 6)], "x")
    assert caught.value.code == "COMPARATOR_INVALID"


def test_stage_b_is_2021_06_to_2024_08() -> None:
    assert stage_b_months(MONTHS) == STAGE_B and len(STAGE_B) == 39
    with pytest.raises(ValueError, match="do not cover stage B"):
        stage_b_months(MONTHS[:-1])


def test_a_strong_book_passes_with_no_annotation() -> None:
    result = verdict(_run(), FACTORS)
    assert result.status == "PASS" and result.annotations == ()
    assert result.g2 is not None and all(r.passed for r in result.g2.values())


def test_insufficient_comes_before_g1() -> None:
    bad = {c: {**_loaded(GOOD), (2019, 1): math.nan} for c in COSTS}
    result = verdict(_run(bad, insufficient=(date(2016, 3, 31),)), FACTORS)
    assert result.status == "INSUFFICIENT" and result.insufficient == (date(2016, 3, 31),)


def test_g1_refused_stops_before_g2() -> None:
    factors = {n: dict(v) for n, v in FACTORS.items()}
    del factors["Mom"][(2015, 1)]
    result = verdict(_run(control=(math.inf,)), factors)  # G2 would refuse COMPARATOR_INVALID if computed
    assert result.status == "G1_REFUSED" and result.g2 is None
    assert result.reason is not None and result.reason.count("missing_factor_month") == len(ARMS)


def test_fail_names_every_failing_gate_and_arm() -> None:
    weak = dict.fromkeys(COSTS, _loaded({"HML": 1.0, "RMW": 1.0, "CMA": -1.0}))
    result = verdict(_run(weak, b1=0.05, control=(0.5,)), FACTORS)
    assert result.status == "FAIL"
    assert result.reason == (
        "G1 best_case; G1 worst_case; G2 best_case vs B1, control median; G2 worst_case vs B1, control median"
    )


def test_g2_compares_against_the_control_median_not_the_best_draw() -> None:
    # The book's G sits between the median draw (0.0) and the best (0.5): it passes.
    assert verdict(_run(control=(-0.5, 0.0, 0.5)), FACTORS).status == "PASS"
    assert verdict(_run(control=(0.5, 0.5, -0.5)), FACTORS).reason == (
        "G2 best_case vs control median; G2 worst_case vs control median"
    )


def test_the_turnover_veto_reads_stage_b_formations_only() -> None:
    # The path keys turnover by formation; stage B's formations are 2021-05..2024-07.
    assert verdict(_run(turnover={(2021, 4): 0.9}), FACTORS).status == "PASS"
    assert verdict(_run(turnover={(2021, 6): TURNOVER_VETO}), FACTORS).status == "PASS"  # "above", not at
    result = verdict(_run(turnover={(2021, 5): 0.6, (2023, 1): 0.7, (2024, 7): 0.8}), FACTORS)
    assert result.status == "FAIL" and result.reason == "TURNOVER_BENEFIT_UNSHOWN"
    assert result.turnover == {arm: ((2021, 5), (2023, 1), (2024, 7)) for arm in ARMS}


def test_stage_b_starts_from_the_boundarys_pre_trade_nav() -> None:
    returns = {(2021, 6): 0.1, (2021, 7): 0.2}
    state = BoundaryState(date(2021, 5, 31), (), 0.0, 2.0)
    out = stage_b_returns(returns, {(2021, 5): 1.5}, state, [(2021, 6), (2021, 7)])
    assert out == {(2021, 6): 1.1 * (1.5 / 2.0) - 1.0, (2021, 7): 0.2}
    with pytest.raises(ValueError, match="boundary state"):
        stage_b_returns(returns, {}, None, [(2021, 6)])
    with pytest.raises(ValueError, match="does not precede"):
        stage_b_returns(returns, {(2021, 5): 1.5}, state, [(2021, 7)])


def test_the_boundary_formations_cost_counts_against_stage_b() -> None:
    # The same path passes when the 2021-05 rebalance is free and fails vs B1 when it costs half the NAV.
    assert verdict(_run(b1=0.01), FACTORS).status == "PASS"
    result = verdict(_run(b1=0.01, boundary_cost=0.5), FACTORS)
    assert result.status == "FAIL" and result.reason is not None and "vs B1" in result.reason
    # Each control draw's boundary cost counts against it too: draws that out-earn the book pass it once charged.
    assert verdict(_run(control=(0.1,)), FACTORS).status == "FAIL"
    assert verdict(_run(control=(0.1,), control_boundary_cost=0.99999), FACTORS).status == "PASS"


def test_pass_annotations_name_the_stress_failure_and_each_year_the_pass_depends_on() -> None:
    base = _loaded(GOOD, lambda m: 0.002 + (0.05 if m[0] == 2022 else 0.0))
    stress = {m: r - 0.01 for m, r in base.items()}
    result = verdict(_run({"gross": base, "net": base, "stress_2x": stress}, b1=0.01, control=(0.0,)), FACTORS)
    assert result.status == "PASS"
    assert result.annotations == (
        "fails at stress cost (best_case vs B1; worst_case vs B1)",
        "depends on 2022 (best_case vs B1, control median; worst_case vs B1, control median)",
    )


def test_a_non_finite_annotation_statistic_refuses() -> None:
    stress = {**_loaded(GOOD, lambda m: 0.02), (2022, 1): math.inf}
    run = _run({"gross": _loaded(GOOD), "net": _loaded(GOOD, lambda m: 0.02), "stress_2x": stress})
    with pytest.raises(BookRefusal) as caught:
        verdict(run, FACTORS)
    assert caught.value.code == "COMPARATOR_INVALID"
