"""#3621 slice 2b: the printed diagnostics, never gated.

Spec: ``docs/research/2026-10-08-3621-avoidance-filters.md`` §"Diagnostics" and the exhaustion rule of §"Decision
rule" (PR #3724). Pure functions over slice 2a's paths and target sets (:mod:`scripts.report_3621_books`).

* **Windows** are labelled by return month and sliced from the one path: whole (2014-10..2024-08), stage A
  (2014-10..2021-05), stage B (2021-06..2024-08) and each calendar year, 2014 and 2024 labelled partial. Trade costs
  and turnover stay where ``value_path`` keys them (month M); a formation's flag counts and excluded weight belong to
  the holding month it starts (M + 1).
* **Per book:** G = (12/n) Σ ln(1 + r); arithmetic annualised 12 × mean; volatility sd(ddof 1) × √12; the maximum
  drawdown from the window's opening wealth; step 0's turnover per year (Σ one-way turnover / years); cost per year,
  Σ(charged cost / pre-trade NAV) / years over the trades charged in the window (:func:`charged_month`), so costs
  on different NAV scales add as fractions of NAV.
* **The differential** D_t = r(U_F) − r(U): mean, sd, IR 12 × mean / (sd × √12) (``None`` when sd is 0, tested by
  equality), a Newey–West t on the mean with the Bartlett kernel at the spec's frozen lag 3 and null zero (step 0's
  HAC form, :func:`~scripts.report_3609_baselines.ols_newey_west`, which instead picks its lag from n), and the
  maximum drawdown of the wealth ratio W(U_F) / W(U) from the window's opening.
* **Exhaustion:** a statistic of a book that reached non-positive wealth, over a window whose last month is on or
  after the exhaustion month, is :class:`Undefined`; so is a differential using it. Other windows print normally.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import math
import statistics
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final

import numpy as np

from app.services.avoidance_filters import Filter, MaxMissing, NameFlags
from app.services.factor_book_path import Month, PathResult, Trade, TradeCategory, month_of, next_month
from scripts.report_3609_baselines import max_drawdown
from scripts.report_3609_step2 import PanelMonth
from scripts.report_3609_step2_segments import band_of
from scripts.report_3609_step2_verdict import STAGE_B, log_growth
from scripts.report_3621_books import Population, Targets

#: §"Diagnostics": the differential's Newey–West lag, frozen.
NW_LAG: Final = 3
STAGE_A: Final[tuple[Month, Month]] = ((2014, 10), (2021, 5))
SIZE_SEGMENTS: Final = (Population.MICRO, Population.SMALL, Population.LARGE, Population.MEGA)


@dataclass(frozen=True)
class Window:
    label: str
    months: tuple[Month, ...]
    partial: bool = False

    def __post_init__(self) -> None:
        if not self.months:
            raise ValueError(f"window {self.label!r} holds no return month")


def windows(months: Sequence[Month]) -> tuple[Window, ...]:
    """Whole, stage A, stage B and each calendar year, over the path's return months."""
    ordered = tuple(sorted(months))

    def between(first: Month, last: Month) -> tuple[Month, ...]:
        return tuple(m for m in ordered if first <= m <= last)

    out = [Window("whole", ordered), Window("stage A", between(*STAGE_A)), Window("stage B", between(*STAGE_B))]
    for year in sorted({m[0] for m in ordered}):
        inside = tuple(m for m in ordered if m[0] == year)
        out.append(Window(str(year), inside, partial=len(inside) < 12))
    return tuple(out)


@dataclass(frozen=True)
class Undefined:
    exhausted_at: Month

    def __str__(self) -> str:
        return f"undefined (wealth exhausted at {self.exhausted_at[0]}-{self.exhausted_at[1]:02d})"


def _exhausted(paths: Iterable[PathResult], window: Window) -> Undefined | None:
    """The earliest exhaustion among ``paths`` on or before the window's last month."""
    hits = [p.nonpositive for p in paths if p.nonpositive is not None and p.nonpositive <= window.months[-1]]
    return Undefined(min(hits)) if hits else None


def charged_month(trade: Trade) -> Month:
    """The return month a trade's cost falls in: its own, except the initial purchase, which ``value_path`` keys to
    the first formation and charges in the first return month (§"The books")."""
    return next_month(trade.month) if trade.category is TradeCategory.INITIAL_PURCHASE else trade.month


@dataclass(frozen=True)
class BookStats:
    g: float
    arithmetic: float
    volatility: float | None
    max_drawdown: float
    turnover_per_year: float
    cost_per_year: float


def book_stats(path: PathResult, window: Window) -> BookStats | Undefined:
    if (undefined := _exhausted([path], window)) is not None:
        return undefined
    r = [path.returns[m] for m in window.months]
    years = len(r) / 12.0
    inside = set(window.months)
    return BookStats(
        g=log_growth(path.returns, window.months, window.label),
        arithmetic=12.0 * statistics.fmean(r),
        volatility=statistics.stdev(r) * math.sqrt(12.0) if len(r) > 1 else None,
        max_drawdown=max_drawdown(r),
        turnover_per_year=math.fsum(path.turnover.get(m, 0.0) for m in window.months) / years,
        cost_per_year=math.fsum(t.cost / t.pre_nav for t in path.trades if charged_month(t) in inside) / years,
    )


def newey_west_t(values: Sequence[float], lag: int = NW_LAG) -> float | None:
    """The mean over its Bartlett-kernel HAC standard error at ``lag``, null zero; ``None`` when that error is not
    finite and positive."""
    if not values:
        raise ValueError("a Newey-West t needs at least one value")
    x = np.asarray(values, dtype=float)
    n = len(x)
    e = x - x.mean()
    s = float(e @ e)
    for k in range(1, min(lag, n - 1) + 1):
        s += 2.0 * (1.0 - k / (lag + 1.0)) * float(e[k:] @ e[:-k])
    se = math.sqrt(s) / n if s >= 0 else math.nan
    return float(x.mean()) / se if math.isfinite(se) and se > 0 else None


@dataclass(frozen=True)
class DifferentialStats:
    months: int
    defined: int
    mean: float
    sd: float | None
    ir: float | None
    nw_t: float | None
    ratio_max_drawdown: float


def differential(unfiltered: PathResult, filtered: PathResult, window: Window) -> DifferentialStats | Undefined:
    if (undefined := _exhausted([unfiltered, filtered], window)) is not None:
        return undefined
    u = np.array([unfiltered.returns[m] for m in window.months])
    f = np.array([filtered.returns[m] for m in window.months])
    d = f - u
    sd = float(np.std(d, ddof=1)) if len(d) > 1 else None
    constant = bool(np.ptp(d) == 0.0)  # equality, never a float deviation (the #2394 ``ptp`` prevention entry)
    ratio = np.concatenate(([1.0], np.cumprod(1.0 + f) / np.cumprod(1.0 + u)))
    return DifferentialStats(
        months=len(window.months),
        defined=len(d),
        mean=float(d.mean()),
        sd=sd,
        ir=None if sd is None or constant else 12.0 * float(d.mean()) / (sd * math.sqrt(12.0)),
        nw_t=None if constant else newey_west_t(list(d)),
        ratio_max_drawdown=float((ratio / np.maximum.accumulate(ratio) - 1.0).min()),
    )


# --------------------------------------------------------------------------- per-formation counts


def _covered(names: frozenset[int], flags: Mapping[int, NameFlags]) -> frozenset[int]:
    """``names``, refusing any without flags (as slice 2a's ``targets`` does)."""
    missing = sorted(names - flags.keys())
    if missing:
        raise ValueError(f"{len(missing)} population names have no flags: {missing[:5]}")
    return names


@dataclass(frozen=True)
class FormationCounts:
    admitted: int
    max_above_cutoff: int
    max_screened: int
    max_short: int
    max_zero_heavy: int
    sub5: int
    young: int
    #: |B_F| / |U|, the flagged names' target-weight share of U; ``None`` for an empty population.
    excluded_weight: float | None


def formation_counts(
    population: frozenset[int], flags: Mapping[int, NameFlags], filter_set: frozenset[Filter]
) -> FormationCounts:
    readings = [flags[n] for n in _covered(population, flags)]
    missing = [f.reading.missing for f in readings]
    flagged = sum(1 for f in readings if f.removed_by(filter_set))
    return FormationCounts(
        admitted=len(readings),
        max_above_cutoff=sum(1 for f in readings if Filter.MAX in f.flagged and f.reading.missing is None),
        max_screened=missing.count(MaxMissing.SCREENED),
        max_short=missing.count(MaxMissing.SHORT),
        max_zero_heavy=missing.count(MaxMissing.ZERO_HEAVY),
        sub5=sum(1 for f in readings if Filter.SUB5 in f.flagged),
        young=sum(1 for f in readings if Filter.YOUNG in f.flagged),
        excluded_weight=flagged / len(readings) if readings else None,
    )


def holding_month(formation: date) -> Month:
    """The window month a formation's counts belong to: the holding month it starts."""
    return next_month(month_of(formation))


def spread(values: Iterable[float | None]) -> tuple[float, float, float] | None:
    """(min, median, max) of the defined values; ``None`` when there are none."""
    defined = [v for v in values if v is not None]
    return (min(defined), statistics.median(defined), max(defined)) if defined else None


def screened_targets(population: frozenset[int], flags: Mapping[int, NameFlags]) -> Targets:
    """The screened-only component: U minus the names whose MAX window was screened."""
    screened = frozenset(n for n in _covered(population, flags) if flags[n].reading.missing is MaxMissing.SCREENED)
    return Targets(population, population - screened, screened)


def cell_counts(
    month: PanelMonth,
    segments: Mapping[Population, frozenset[int]],
    flags: Mapping[int, NameFlags],
    filter_set: frozenset[Filter],
) -> dict[tuple[Population, str], tuple[int, int]]:
    """(flagged, retained) per (size segment, step 0 cost band of the raw close at s(M)) cell."""
    out: dict[tuple[Population, str], list[int]] = {}
    for segment in SIZE_SEGMENTS:
        for name in _covered(segments[segment], flags):
            cell = out.setdefault((segment, band_of(month.close.get(name))), [0, 0])
            cell[0 if flags[name].removed_by(filter_set) else 1] += 1
    return {key: (flagged, retained) for key, (flagged, retained) in sorted(out.items())}


# --------------------------------------------------------------------------- the names file


@dataclass(frozen=True)
class ExcludedName:
    formation: date
    name_key: int
    symbol: str
    population: Population
    filters: tuple[Filter, ...]


def names_file(rows: Iterable[ExcludedName]) -> tuple[bytes, str]:
    """One canonical JSON line per excluded (M, ``name_key``, population), in that order, gzipped reproducibly, and
    its sha256."""
    ordered = sorted(rows, key=lambda r: (r.formation, r.name_key, str(r.population)))
    lines = [
        json.dumps(
            {
                "M": r.formation.isoformat(),
                "name_key": r.name_key,
                "symbol": r.symbol,
                "population": str(r.population),
                "filters": sorted(str(f) for f in r.filters),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        for r in ordered
    ]
    payload = gzip.compress("".join(f"{line}\n" for line in lines).encode(), mtime=0)
    return payload, hashlib.sha256(payload).hexdigest()
