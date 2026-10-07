"""#3609 step 2 slice 3c-v(b): turnover-above-50% months and attribution, on synthetic series only (no corpus, no
stage B)."""

from __future__ import annotations

import math
import random
from collections.abc import Mapping
from datetime import date

import numpy as np
import pytest

from app.services.factor_book import UNCLASSIFIED
from app.services.factor_book_path import (
    BoundaryState,
    Decision,
    Month,
    PathResult,
    Trade,
    TradeCategory,
    next_month,
)
from app.services.factor_book_references import B1Path
from app.services.factor_book_series import ARMS, Scenario, SeriesRun, Summary
from scripts.report_3609_baselines import FACTOR_REGRESSORS
from scripts.report_3609_step2_attribution import (
    FLAG_NO_REFERENCE,
    FLAG_OVERWEIGHT,
    MIN_INFORMATION,
    IndustryWeight,
    attribution,
    average_weights,
    diagnostics,
    high_turnover,
    industry_weights,
    information,
    turnover_months,
)
from scripts.report_3609_step2_operations import Window, stage_windows
from scripts.report_3609_step2_signals import summarise
from scripts.report_3609_step2_verdict import g1, log_growth, stage_b_returns

COSTS = {"gross": 0.0, "net": 0.01, "stress_2x": 0.02}
BOUNDARY: Month = (2021, 5)
#: The boundary formation's cost: 1% of the pre-trade NAV, for the book and the equal-weight universe alike.
STATE = BoundaryState(date(2021, 5, 31), (), 0.0, 1.0)
POST: dict[Month, float] = {BOUNDARY: 0.99}


def _months(first: Month, last: Month) -> tuple[Month, ...]:
    out = [first]
    while out[-1] < last:
        out.append(next_month(out[-1]))
    return tuple(out)


MONTHS = _months((2014, 10), (2024, 8))
FORMATIONS = _months((2014, 9), (2024, 7))
B1 = {m: 0.004 + 0.001 * (i % 5) for i, m in enumerate(MONTHS)}
#: Distinct per month so a month shifted by one shows.
BOOK = {m: 2.0 * B1[m] + 0.001 * (i % 3) for i, m in enumerate(MONTHS)}
EQUAL = {m: B1[m] + 0.0005 for m in MONTHS}
HIGH: Month = (2016, 3)


def _factors(seed: int = 3) -> dict[str, dict[Month, float]]:
    rng = random.Random(seed)
    return {n: {m: rng.gauss(0.0, 0.03) for m in MONTHS} for n in (*FACTOR_REGRESSORS, "RF")}


FACTORS = _factors()


def _book(cost: str, turnover: Mapping[Month, float]) -> PathResult:
    drag = COSTS[cost]
    trades = [
        Trade(HIGH, 1, TradeCategory.ENTRY, 0.4, 0.004, 1.0),
        Trade(HIGH, 2, TradeCategory.DISCRETIONARY_EXIT, 0.3, 0.003, 1.0),
        Trade(next_month(HIGH), 3, TradeCategory.ENTRY, 9.0, 9.0, 1.0),
    ]
    return PathResult(
        returns={m: r - drag for m, r in BOOK.items()},
        nav=dict(POST),
        turnover=dict(turnover),
        trades=trades,
        boundary=STATE,
    )


def _run(turnover: Mapping[str, Mapping[Month, float]] | None = None) -> SeriesRun:
    """The book's turnover is 0.1 except where ``turnover`` says, per arm; three draws earn 0, 0.5% and 1% a month,
    less the cost scenario's drag."""
    turnover = turnover or {}
    scenarios: list[Scenario] = [(arm, cost) for arm in ARMS for cost in COSTS]
    equal = PathResult(returns=dict(EQUAL), nav=dict(POST), boundary=STATE)

    def draw(r: float) -> Summary:
        return Summary(dict.fromkeys(MONTHS, r), {}, dict(POST), {}, None, STATE)

    def book_turnover(arm: str) -> dict[Month, float]:
        return {m: 0.1 for m in FORMATIONS[1:]} | dict(turnover.get(arm, {}))

    return SeriesRun(
        months=MONTHS,
        book={s: _book(s[1], book_turnover(s[0])) for s in scenarios},
        equal_weight=dict.fromkeys(scenarios, equal),
        cap_weighted=dict.fromkeys(scenarios, equal),
        control={s: tuple(draw(r - COSTS[s[1]]) for r in (0.0, 0.005, 0.01)) for s in scenarios},
        b1={c: B1Path(dict(B1), "band", 0.0, 0.0) for c in COSTS},
        pools=(),
        insufficient=(),
    )


# --------------------------------------------------------------------------- turnover above 50%


def test_a_high_turnover_formation_prints_its_trades_and_the_next_months_return_against_the_control() -> None:
    arm = ARMS[0]
    (row,) = high_turnover(_run({arm: {HIGH: 0.62}}), arm)
    held = next_month(HIGH)
    assert (row.formation, row.turnover, row.held) == (HIGH, 0.62, held)
    # Only the formation's own trades: the 9.0 entry belongs to the next formation.
    assert row.notional == pytest.approx(0.7)
    assert row.cost == pytest.approx(0.007)
    # The control's median draw earns 0.5% gross and −0.5% net; the book earns BOOK less the same drag.
    assert row.gross_vs_control == pytest.approx(BOOK[held] - 0.005)
    assert row.net_vs_control == pytest.approx(BOOK[held] - 0.01 - (0.005 - 0.01))


def test_the_threshold_is_strict_and_n_counts_calendar_months_in_either_arm() -> None:
    a, b = ARMS
    run = _run({a: {HIGH: 0.5, (2017, 1): 0.51, (2018, 1): 0.7}, b: {(2017, 1): 0.6, (2019, 2): 0.55}})
    found = turnover_months(run)
    assert [r.formation for r in found.by_arm[a]] == [(2017, 1), (2018, 1)]
    assert [r.formation for r in found.by_arm[b]] == [(2017, 1), (2019, 2)]
    assert found.n == 3


def test_no_high_turnover_month_prints_none() -> None:
    assert turnover_months(_run()).n == 0


# --------------------------------------------------------------------------- attribution


def test_universe_effect_and_selection_split_the_books_g_over_b1() -> None:
    run = _run()
    for window in stage_windows(run):
        result = attribution(run, ARMS[0], window, FACTORS)
        assert result.universe_effect + result.selection == pytest.approx(result.book_g - result.b1_g)
        assert result.universe_effect == pytest.approx(result.equal_weight_g - result.b1_g)
        assert result.universe_effect > 0  # EQUAL is B1 plus 5bp a month


def test_stage_b_attribution_starts_from_the_boundarys_pre_trade_nav() -> None:
    run = _run()
    _, stage_b, _ = stage_windows(run)
    result = attribution(run, ARMS[0], stage_b, FACTORS)
    first = stage_b.months[0]
    folded = {m: BOOK[m] - 0.01 for m in stage_b.months} | {first: (1.0 + BOOK[first] - 0.01) * 0.99 - 1.0}
    assert result.book_g == pytest.approx(log_growth(folded, stage_b.months, "expected"))
    assert result.b1_g == pytest.approx(log_growth(B1, stage_b.months, "b1"))


def test_spy_beta_is_the_slope_on_b1_and_undefined_when_b1_is_flat() -> None:
    run = _run()
    stage_a = stage_windows(run)[0]
    beta = attribution(run, ARMS[0], stage_a, FACTORS).spy_beta
    assert beta is not None and 1.9 < beta < 2.1
    flat = _run()
    for path in flat.b1.values():
        path.returns.update(dict.fromkeys(MONTHS, 0.004))
    assert attribution(flat, ARMS[0], stage_a, FACTORS).spy_beta is None


def test_the_loadings_are_g1s_regression_on_the_windows_months() -> None:
    run = _run()
    stage_a = stage_windows(run)[0]
    expected = g1({m: BOOK[m] - 0.01 for m in stage_a.months}, FACTORS, stage_a.months)
    assert attribution(run, ARMS[0], stage_a, FACTORS).regression == expected
    missing = {n: dict(v) for n, v in FACTORS.items()}
    del missing["HML"][stage_a.months[3]]
    assert attribution(run, ARMS[0], stage_a, missing).regression.refusal is not None


# --------------------------------------------------------------------------- FF-12 weights


def _decision(formation: Month, targets: tuple[int, ...], weights: Mapping[int, float] | None = None) -> Decision:
    return Decision(date(*formation, 28), targets, {}, {}, {}, weights=weights)


INDUSTRY = {1: "Hlth", 2: "Hlth", 3: "BusEq", 4: UNCLASSIFIED, 5: "Utils", 6: "Enrgy"}


def test_industry_weights_compare_the_book_with_the_cap_weighted_universe_and_flag() -> None:
    f = FORMATIONS[0]
    book = [_decision(f, (1, 2, 3, 4))]
    reference = [_decision(f, (1, 3, 4, 5, 6), {1: 0.2, 3: 0.3, 4: 0.125, 5: 0.25, 6: 0.125})]
    weights = industry_weights(book, reference, [INDUSTRY])[f]
    assert weights == {
        "BusEq": IndustryWeight(0.25, 0.3),
        "Enrgy": IndustryWeight(0.0, 0.125),
        "Hlth": IndustryWeight(0.5, 0.2),
        UNCLASSIFIED: IndustryWeight(0.25, 0.125),
        "Utils": IndustryWeight(0.0, 0.25),
    }
    assert weights["Hlth"].flag == FLAG_OVERWEIGHT
    assert weights[UNCLASSIFIED].flag is None  # exactly 2x is not above it
    assert weights["BusEq"].flag is None
    assert IndustryWeight(0.1, 0.0).flag == FLAG_NO_REFERENCE
    assert IndustryWeight(0.0, 0.0).flag is None


def test_a_formation_holding_nothing_has_no_industry_weight() -> None:
    f = FORMATIONS[0]
    assert industry_weights([_decision(f, ())], [_decision(f, (5,), {5: 1.0})], [INDUSTRY])[f] == {
        "Utils": IndustryWeight(0.0, 1.0)
    }


def test_industry_weights_refuse_a_held_name_without_an_industry_and_misaligned_inputs() -> None:
    f, g = FORMATIONS[:2]
    with pytest.raises(ValueError, match="no industry"):
        industry_weights([_decision(f, (1, 9))], [_decision(f, (1,), {1: 1.0})], [INDUSTRY])
    with pytest.raises(ValueError, match="formations differ"):
        industry_weights([_decision(f, (1,))], [_decision(g, (1,), {1: 1.0})], [INDUSTRY])
    with pytest.raises(ValueError, match="industry maps"):
        industry_weights([_decision(f, (1,))], [_decision(f, (1,), {1: 1.0})], [])


def test_average_weights_are_over_the_formations_held_into_the_window() -> None:
    monthly = {
        f: {"Hlth": IndustryWeight(1.0, 0.5)} if f[0] < 2021 else {"Utils": IndustryWeight(0.4, 0.0)}
        for f in FORMATIONS
    }
    stage_a, stage_b, pooled = stage_windows(_run())
    assert average_weights(monthly, stage_b) == {"Utils": IndustryWeight(0.4, 0.0)}
    # Stage A holds 2014-10..2021-05: the 2014-09..2020-12 formations hold Hlth, 2021-01..2021-04 hold Utils.
    a = average_weights(monthly, stage_a)
    assert a["Hlth"].book == pytest.approx(76 / 80)
    assert a["Utils"].book == pytest.approx(0.4 * 4 / 80)
    assert a["Utils"].flag == FLAG_NO_REFERENCE
    assert math.fsum(w.book for w in average_weights(monthly, pooled).values()) < 1.0


def test_diagnostics_assembles_every_arm_and_stage_window() -> None:
    run = _run({ARMS[1]: {HIGH: 0.8}})
    decisions = [_decision(f, (1, 2)) for f in FORMATIONS]
    reference = [_decision(f, (1, 5), {1: 0.5, 5: 0.5}) for f in FORMATIONS]
    out = diagnostics(run, FACTORS, decisions, reference, [INDUSTRY] * len(FORMATIONS))
    assert out.turnover.n == 1
    assert set(out.attribution) == set(ARMS)
    assert all(set(w) == {"stage A", "stage B", "pooled"} for w in out.attribution.values())
    assert out.industry_average["pooled"]["Hlth"] == IndustryWeight(1.0, 0.5)
    assert len(out.industry_monthly) == len(FORMATIONS)


# --------------------------------------------------------------------------- information


def _active(run: SeriesRun, window: Window) -> list[float]:
    path = run.book[(ARMS[0], "net")]
    if window.from_boundary:
        book = stage_b_returns(path.returns, path.nav, path.boundary, window.months)
    else:
        book = {m: path.returns[m] for m in window.months}
    return [book[m] - B1[m] for m in window.months]


def test_information_is_the_lag_1_autocorrelation_and_the_error_ratio_of_the_active_return() -> None:
    run = _run()
    for window in stage_windows(run):
        active = _active(run, window)
        out = information(run, ARMS[0], window)
        assert out.autocorrelation == pytest.approx(float(np.corrcoef(active[:-1], active[1:])[0, 1]))
        # n_eff is n / ratio² on the same series, capped at n.
        summary = summarise(dict(zip(window.months, active, strict=True)), window, ic_ir=False)
        assert summary.n_eff is not None and out.se_ratio is not None
        n = len(active)
        assert min(n, n / out.se_ratio**2) == pytest.approx(summary.n_eff)


def test_information_needs_24_pairs_and_24_months() -> None:
    run = _run()
    months = MONTHS[:MIN_INFORMATION]
    short = information(run, ARMS[0], Window("w", months, months))
    assert short.autocorrelation is None and short.se_ratio is not None  # 23 pairs, 24 months
    months = MONTHS[: MIN_INFORMATION - 1]
    assert information(run, ARMS[0], Window("w", months, months)).se_ratio is None


def test_a_constant_active_return_has_no_information() -> None:
    run = _run()
    run.book[(ARMS[0], "net")].returns.update({m: B1[m] + 0.1 for m in MONTHS})
    stage_a = stage_windows(run)[0]
    # 0.1 above B1, in floats: the active return is constant only by equality of the float differences.
    active = np.array(_active(run, stage_a))
    if np.ptp(active) != 0.0:
        pytest.skip("this fixture's float differences are not all equal")
    out = information(run, ARMS[0], stage_a)
    assert out.autocorrelation is None and out.se_ratio is None
