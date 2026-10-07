"""#3609 step 2's signal diagnostics (slice 3c-v(c)): printed, never gated.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"Diagnostics" (PR #3666): "Signals: one-month
horizon, for the three retained families and the composite", and "Minimum effective sample". Pure functions over
step 1's formations and the book's scores (``factor_book.composite_scores``, one per formation). Selection-conditioned:
the families were chosen on stage A. The Newey–West rule is step 0's (``report_3609_baselines``).

* **Population,** per formation M, signal and arm: universe names with the signal's score and a step-1 holding-month
  return. Each monthly value is keyed by its holding month M + 1, so a window's months are
  ``report_3609_step2_operations``'s return months.
* **IC_M:** Spearman, average ranks for ties (scores tie by :func:`~app.services.factor_book.compare`, exactly);
  undefined below 30 names or at zero variance in either rank vector.
* **Quintile spread_M:** score descending, ties by ``name_key``; boundaries at ranks ⌈kn/5⌉, k = 1..4; Q1 minus Q5,
  equal-weighted, gross; undefined when any quintile has fewer than 5 names.
* **Summaries per window,** on the defined months: mean, standard deviation (ddof 1) and a Newey–West t on the mean;
  IC-IR (mean / deviation, monthly) for IC only. All undefined below 12 defined months; IC-IR undefined at zero
  deviation; the t undefined when any month in the window is undefined or its standard error is not finite and
  positive, which a constant series (every value equal) implies.
* **n_eff** = n × (iid variance of the mean) / (Newey–West variance of the mean), capped at n. The iid variance is
  the same estimator at lag 0 (Σe² / n²), so a series with no autocovariance has n_eff = n. It is undefined when the
  t is, and a summary whose n_eff is below ``MIN_EFFECTIVE`` or undefined is marked insufficient. It captures
  dependence only up to the Newey–West bandwidth.
"""

from __future__ import annotations

import functools
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Final

import numpy as np

from app.services.factor_book import COMPOSITE, FAMILIES, Exact, Scores, compare
from app.services.factor_book_path import Formation, Month, month_of, next_month
from app.services.factor_book_series import ARMS
from app.services.strategy_result import AmbiguityArm
from scripts.report_3609_baselines import ols_newey_west
from scripts.report_3609_step2_operations import Window

#: The three retained families and the composite.
SIGNALS: Final = (*FAMILIES, COMPOSITE)
#: §"Diagnostics", Signals: the fewest names an IC is computed on, and a quintile's fewest.
MIN_IC_NAMES: Final = 30
MIN_QUINTILE_NAMES: Final = 5
QUINTILES: Final = 5
#: The fewest defined months a summary is computed on, and the minimum effective sample.
MIN_MONTHS: Final = 12
MIN_EFFECTIVE: Final = 24


# --------------------------------------------------------------------------- monthly values


def population(
    formation: Formation, scores: Mapping[int, Exact], arm: AmbiguityArm
) -> tuple[dict[int, Exact], dict[int, float]]:
    """The universe names with a score and a holding-month return in ``arm``, in ``name_key`` order."""
    names = sorted(
        n for n in formation.universe if n in scores and n in formation.returns and arm in formation.returns[n].by_arm
    )
    return {n: scores[n] for n in names}, {n: formation.returns[n].by_arm[arm] for n in names}


def average_ranks[T](values: Mapping[int, T], cmp: Callable[[T, T], int]) -> dict[int, float]:
    """1-based ranks, ascending; a run of equal values (``cmp`` 0) takes its average rank."""
    order = sorted(values, key=functools.cmp_to_key(lambda a, b: cmp(values[a], values[b])))
    ranks: dict[int, float] = {}
    start = 0
    while start < len(order):
        end = start + 1
        while end < len(order) and cmp(values[order[start]], values[order[end]]) == 0:
            end += 1
        for name in order[start:end]:
            ranks[name] = (start + 1 + end) / 2.0
        start = end
    return ranks


def _float_cmp(a: float, b: float) -> int:
    return (a > b) - (a < b)


def ic(scores: Mapping[int, Exact], returns: Mapping[int, float]) -> float | None:
    """Spearman's correlation between score and return over the same names."""
    if scores.keys() != returns.keys():
        raise ValueError("an IC needs one score and one return per name")
    if len(scores) < MIN_IC_NAMES:
        return None
    names = sorted(scores)
    x_ranks, y_ranks = average_ranks(scores, compare), average_ranks(returns, _float_cmp)
    x = np.array([x_ranks[n] for n in names])
    y = np.array([y_ranks[n] for n in names])
    if np.ptp(x) == 0 or np.ptp(y) == 0:
        return None
    return float(np.corrcoef(x, y)[0, 1])


def quintile_spread(scores: Mapping[int, Exact], returns: Mapping[int, float]) -> float | None:
    """Q1 (highest scores) minus Q5, equal-weighted."""
    if scores.keys() != returns.keys():
        raise ValueError("a spread needs one score and one return per name")
    order = sorted(scores, key=functools.cmp_to_key(lambda a, b: compare(scores[b], scores[a]) or (a > b) - (a < b)))
    n = len(order)
    cuts = [0, *(math.ceil(k * n / QUINTILES) for k in range(1, QUINTILES)), n]
    groups = [order[cuts[i] : cuts[i + 1]] for i in range(QUINTILES)]
    if any(len(g) < MIN_QUINTILE_NAMES for g in groups):
        return None
    top, bottom = groups[0], groups[-1]
    return math.fsum(returns[n] for n in top) / len(top) - math.fsum(returns[n] for n in bottom) / len(bottom)


@dataclass(frozen=True)
class Monthly:
    """Per holding month (formation M + 1): the value, ``None`` where undefined, and the population size."""

    ic: Mapping[Month, float | None]
    spread: Mapping[Month, float | None]
    names: Mapping[Month, int]


def monthly(formations: Sequence[Formation], scores: Sequence[Scores], signal: str, arm: AmbiguityArm) -> Monthly:
    if len(formations) != len(scores):
        raise ValueError(f"{len(scores)} score sets for {len(formations)} formations")
    ics: dict[Month, float | None] = {}
    spreads: dict[Month, float | None] = {}
    names: dict[Month, int] = {}
    for formation, score in zip(formations, scores, strict=True):
        held = next_month(month_of(formation.formation))
        s, r = population(formation, score.by_operation.get(signal, {}), arm)
        ics[held], spreads[held], names[held] = ic(s, r), quintile_spread(s, r), len(s)
    return Monthly(ics, spreads, names)


# --------------------------------------------------------------------------- summaries


@dataclass(frozen=True)
class SeriesSummary:
    defined: int
    mean: float | None = None
    std: float | None = None
    t: float | None = None
    ic_ir: float | None = None
    n_eff: float | None = None

    @property
    def insufficient(self) -> bool:
        return self.n_eff is None or self.n_eff < MIN_EFFECTIVE


def summarise(series: Mapping[Month, float | None], window: Window, *, ic_ir: bool) -> SeriesSummary:
    """The window's summary of one monthly series (§"Summaries per window")."""
    values = [series.get(m) for m in window.months]
    defined = np.array([v for v in values if v is not None], dtype=float)
    n = len(defined)
    if n < MIN_MONTHS:
        return SeriesSummary(n)
    # A constant series is tested by equality, never by a float deviation: the float mean of equal values can miss
    # them by an ulp and leave a ~1e-17 deviation (docs/review-prevention-log.md, the #2394 ``ptp`` entry).
    constant = bool(np.ptp(defined) == 0.0)
    mean = float(defined[0]) if constant else float(defined.mean())
    std = 0.0 if constant else float(defined.std(ddof=1))
    ratio = mean / std if ic_ir and std > 0 else None
    t = n_eff = None
    if n == len(values) and std > 0:
        fit = ols_newey_west(defined, np.empty((n, 0)))
        se = float(fit.standard_errors[0])
        if math.isfinite(se) and se > 0:
            t = mean / se
            iid = float(np.sum((defined - mean) ** 2)) / n**2
            n_eff = min(float(n), n * iid / se**2)
    return SeriesSummary(n, mean, std, t, ratio, n_eff)


@dataclass(frozen=True)
class SignalSummary:
    monthly: Monthly
    #: Per stage window label.
    ic: Mapping[str, SeriesSummary]
    spread: Mapping[str, SeriesSummary]


def signals(
    formations: Sequence[Formation], scores: Sequence[Scores], windows: Sequence[Window]
) -> dict[str, dict[str, SignalSummary]]:
    """Per arm, then per signal: the monthly series and each window's summaries."""
    out: dict[str, dict[str, SignalSummary]] = {}
    for arm in ARMS:
        out[arm] = {}
        for signal in SIGNALS:
            series = monthly(formations, scores, signal, arm)
            out[arm][signal] = SignalSummary(
                series,
                {w.label: summarise(series.ic, w, ic_ir=True) for w in windows},
                {w.label: summarise(series.spread, w, ic_ir=False) for w in windows},
            )
    return out


__all__ = [
    "MIN_EFFECTIVE",
    "MIN_IC_NAMES",
    "MIN_MONTHS",
    "MIN_QUINTILE_NAMES",
    "QUINTILES",
    "SIGNALS",
    "Monthly",
    "SeriesSummary",
    "SignalSummary",
    "average_ranks",
    "ic",
    "monthly",
    "population",
    "quintile_spread",
    "signals",
    "summarise",
]
