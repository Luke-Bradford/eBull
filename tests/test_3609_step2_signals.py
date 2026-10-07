"""#3609 step 2 slice 3c-v(c): the signal diagnostics, on synthetic inputs only (no corpus, no stage B)."""

from __future__ import annotations

import math
from datetime import date
from fractions import Fraction

import numpy as np
import pytest

from app.services.factor_book import COMPOSITE, Bands, Exact, Scores, compare
from app.services.factor_book_path import Formation, HoldingReturn, Month, next_month
from app.services.factor_book_series import ARMS
from app.services.factor_panel_prices import HoldingStatus
from scripts.report_3609_baselines import ols_newey_west
from scripts.report_3609_step2_operations import Window
from scripts.report_3609_step2_signals import (
    MIN_EFFECTIVE,
    SIGNALS,
    average_ranks,
    ic,
    monthly,
    population,
    quintile_spread,
    signals,
    summarise,
)

EMPTY = Bands((), frozenset(), frozenset(), frozenset(), frozenset())


def _months(first: Month, count: int) -> tuple[Month, ...]:
    out = [first]
    while len(out) < count:
        out.append(next_month(out[-1]))
    return tuple(out)


def _exact(values: dict[int, float]) -> dict[int, Exact]:
    return {n: Exact.raw(v) for n, v in values.items()}


def _formation(formation: Month, universe: range, returns: dict[int, dict[str, float]]) -> Formation:
    held = {n: HoldingReturn(HoldingStatus.OBSERVED, r) for n, r in returns.items()}  # type: ignore[arg-type]
    return Formation(date(*formation, 28), date(*formation, 28), frozenset(universe), EMPTY, {}, {}, held)


# --------------------------------------------------------------------------- monthly values


def test_average_ranks_share_a_tie_and_scores_tie_exactly() -> None:
    assert average_ranks({1: 3.0, 2: 1.0, 3: 3.0, 4: 2.0}, lambda a, b: (a > b) - (a < b)) == {
        1: 3.5,
        2: 1.0,
        3: 3.5,
        4: 2.0,
    }
    # 2 / sqrt(4) and 1 / sqrt(1) are the same score built two ways.
    equal = {1: Exact.of({Fraction(4): Fraction(2)}), 2: Exact.raw(1.0), 3: Exact.raw(0.5)}
    assert average_ranks(equal, compare) == {1: 2.5, 2: 2.5, 3: 1.0}


def test_ic_is_spearman_and_undefined_below_30_names_or_without_variance() -> None:
    scores = _exact({n: float(n) for n in range(30)})
    assert ic(scores, {n: 0.01 * n**3 for n in range(30)}) == pytest.approx(1.0)
    assert ic(scores, {n: -float(n) for n in range(30)}) == pytest.approx(-1.0)
    assert ic(dict(list(scores.items())[:29]), {n: float(n) for n in range(29)}) is None
    assert ic(scores, dict.fromkeys(range(30), 0.01)) is None
    assert ic(_exact(dict.fromkeys(range(30), 1.0)), {n: float(n) for n in range(30)}) is None
    # Tied scores take their average rank: Pearson on the hand ranks.
    tied = _exact({n: float(n // 2) for n in range(30)})
    returns = {n: float((7 * n) % 30) for n in range(30)}
    x = [n // 2 * 2 + 1.5 for n in range(30)]
    y = [r + 1.0 for r in returns.values()]
    assert ic(tied, returns) == pytest.approx(float(np.corrcoef(x, y)[0, 1]))


def test_quintile_spread_is_q1_minus_q5_at_the_specs_boundaries() -> None:
    n = 26  # boundaries at 6, 11, 16, 21: Q1 holds 6 names, Q5 holds 5
    scores = _exact({k: float(k) for k in range(n)})
    returns = {k: float(k) for k in range(n)}
    q1, q5 = (25 + 24 + 23 + 22 + 21 + 20) / 6, (4 + 3 + 2 + 1 + 0) / 5
    assert quintile_spread(scores, returns) == pytest.approx(q1 - q5)
    # 24 names: Q5 gets 4 (boundaries 5, 10, 15, 20).
    assert quintile_spread(_exact({k: float(k) for k in range(24)}), {k: 0.0 for k in range(24)}) is None


def test_quintile_ties_break_by_name_key() -> None:
    # 25 names, five per quintile; names 4 and 5 tie at the Q1 boundary, so the lower key (4) is in Q1.
    values = {k: float(-k) for k in range(25)} | {5: -4.0}
    returns = {k: 0.0 for k in range(25)} | {4: 1.0, 5: 100.0}
    assert quintile_spread(_exact(values), returns) == pytest.approx(1.0 / 5)


def test_population_and_monthly_key_by_the_holding_month() -> None:
    arm = ARMS[0]
    f = _formation((2016, 3), range(40), {n: {arm: 0.01 * n} for n in range(36)} | {36: {ARMS[1]: 0.0}})
    scores = _exact({n: float(n) for n in range(38)} | {99: 1.0})  # 99 is outside the universe
    s, r = population(f, scores, arm)
    assert sorted(s) == sorted(r) == list(range(36))
    out = monthly([f], [Scores({COMPOSITE: scores})], COMPOSITE, arm)
    assert out.names == {(2016, 4): 36}
    assert out.ic[(2016, 4)] == pytest.approx(1.0)
    assert monthly([f], [Scores()], "value", arm).ic == {(2016, 4): None}


# --------------------------------------------------------------------------- summaries


def _window(n: int) -> Window:
    months = _months((2015, 1), n)
    return Window("w", months, months)


def test_a_summary_needs_12_defined_months() -> None:
    w = _window(20)
    series: dict[Month, float | None] = {m: (0.1 if i < 11 else None) for i, m in enumerate(w.months)}
    out = summarise(series, w, ic_ir=True)
    assert (out.defined, out.mean, out.std, out.t, out.ic_ir, out.n_eff) == (11, None, None, None, None, None)
    assert out.insufficient


def test_an_undefined_month_leaves_mean_and_deviation_but_no_t_or_n_eff() -> None:
    w = _window(30)
    series: dict[Month, float | None] = {m: 0.01 * (i % 7) for i, m in enumerate(w.months)}
    series[w.months[4]] = None
    out = summarise(series, w, ic_ir=True)
    values = [v for v in series.values() if v is not None]
    assert out.defined == 29
    assert out.mean == pytest.approx(float(np.mean(values)))
    assert out.std == pytest.approx(float(np.std(values, ddof=1)))
    assert out.mean is not None and out.std is not None
    assert out.ic_ir == pytest.approx(out.mean / out.std)
    assert out.t is None and out.n_eff is None and out.insufficient


def test_the_t_is_step_0s_newey_west_and_n_eff_is_capped_at_n() -> None:
    w = _window(40)
    alternating = {m: (0.02 if i % 2 else -0.01) for i, m in enumerate(w.months)}
    out = summarise(alternating, w, ic_ir=False)
    y = np.array(list(alternating.values()))
    assert out.t == pytest.approx(float(ols_newey_west(y, np.empty((40, 0))).t_stats[0]))
    assert out.ic_ir is None  # the spread prints no ratio
    assert out.n_eff == 40.0  # negative autocovariance would push it above n
    assert not out.insufficient


def test_n_eff_shrinks_under_positive_autocorrelation_and_marks_insufficient() -> None:
    w = _window(40)
    smooth = {m: math.sin(i / 6.0) + 0.001 * i for i, m in enumerate(w.months)}
    out = summarise(smooth, w, ic_ir=True)
    assert out.n_eff is not None and out.n_eff < MIN_EFFECTIVE
    assert out.insufficient


def test_a_constant_series_has_no_ratio_t_or_n_eff() -> None:
    w = _window(24)
    out = summarise(dict.fromkeys(w.months, 0.03), w, ic_ir=True)
    assert out.mean == pytest.approx(0.03) and out.std == 0.0
    assert out.ic_ir is None and out.t is None and out.n_eff is None


def test_signals_covers_every_arm_signal_and_window() -> None:
    formations = [
        _formation(m, range(40), {n: dict.fromkeys(ARMS, 0.001 * ((n * (i + 3)) % 19)) for n in range(40)})
        for i, m in enumerate(_months((2014, 9), 15))
    ]
    scores = [Scores({s: _exact({n: float(n % 11) for n in range(40)}) for s in SIGNALS}) for _ in formations]
    held = tuple(next_month(f) for f in _months((2014, 9), 15))
    out = signals(formations, scores, [Window("all", held, held)])
    assert set(out) == set(ARMS)
    assert all(set(per) == set(SIGNALS) for per in out.values())
    summary = out[ARMS[0]][COMPOSITE]
    assert summary.ic["all"].defined == 15 and summary.spread["all"].defined == 15
