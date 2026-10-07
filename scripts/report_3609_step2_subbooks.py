"""#3609 step 2's per-band sub-books, monthly series (slice 3c-v(g)): printed, never gated.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"Diagnostics", "Segments", "Per-band sub-books"
(PR #3666). Attribution series, not tradable portfolios: no cash, and they do not sum to the book. The per-window
metrics and the FF5+momentum regression are a separate slice; this one builds what they read.

A name's band at formation M is step 0's cost band of its raw close at s(M) (:func:`entry_band`). The book has
already refused ``PRICE_INVALID`` for any universe name or holding without a finite positive close at s(M)
(``factor_book_path.check_closes``), so every held or traded name has a band here; a missing close is a caller error.

For band b at formation M, under one scenario (arm, cost multiplier) of the book's path:

* **V_b** is the book's post-trade value in band-b names: ``Decision.share`` of the path's post-cost NAV at M, the
  path's own arithmetic. **g_b** is their value-weighted holding-month return in the arm (``Decision.returns``, the
  returns the path valued). **C_b** is every cost the path charged at M on trades in band-b names, sales of names
  leaving the book included, each already at its position's entry band. **L_b** is the 2024-08 final-liquidation
  cost on band-b holdings, bands by s(2024-07), so it is non-zero only in the last formation's holding month.
* **Net return** = ((1 + g_b) − L_b/V_b) / (1 + C_b/V_b) − 1: step 0's one-pass rule on the band's capital before
  its costs. Keyed by the holding month M + 1, like the signal block and B1. The book's path instead charges
  formation M's costs in return month M, so a sub-book's month carries the costs of the formation that opened it.
* **States:** V_b = 0 is ``undefined`` (checked first). Otherwise V_b, g_b, C_b and L_b must be finite with V_b > 0,
  C_b ≥ 0, L_b ≥ 0 and g_b ≥ −1, every quotient, the numerator and the denominator finite, the factor finite and
  strictly positive, and the net return not exactly −1; anything else is ``invalid`` with its reason. Fewer than
  ``THIN_NAMES`` band-b names is ``thin``, a flag beside the state; the count is printed.

**Trades by band,** per formation month (``Trade.month``; the final liquidation's is 2024-08, banded at s(2024-07)):
order notional and cost; turnover as band-b (buys + sells) / 2 over the **book's** pre-trade NAV, so band turnovers
sum to the book's; the initial purchase and the final liquidation excluded from turnover as in step 0 and printed
separately. Terminal and coverage-exit realisations (no order, no cost) are printed by band at their holding month.

**Per window** (:func:`window_metrics`; stage A, stage B and pooled over the sub-book's holding months):

* A window with any undefined, invalid or thin month, in either the base or the gross series, is uncomputable: every
  metric is ``undefined`` with the first month's reason, and nothing is computed.
* Otherwise, step 0's metrics against B1 at base cost, computed as :func:`report_3609_step2_operations.book_stats`
  computes the book's: net and gross annualised return as exp(G) − 1 with G beside it, volatility, maximum drawdown
  on log wealth, active return, tracking error, IR, beta and maximum relative drawdown, any non-finite figure
  ``None``. Then G1's FF5+momentum regression (:func:`report_3609_step2_verdict.g1`) on the window's exact months,
  with its Newey–West lag from their count; a G1 refusal is ``undefined`` with its reason, never a stop.
* The window's n_eff is the signal block's (:func:`report_3609_step2_signals.summarise`) on the base net return
  series; below ``MIN_EFFECTIVE``, or undefined, every metric is still printed and marked ``insufficient``.
* Sub-book month m carries formation m − 1, so its windows hold the formations before their months: stage B is
  formations 2021-05..2024-07 plus the 2024-08 liquidation and needs no boundary folding, and stage A is formations
  through 2021-04. Trades by band per window sum over those formations, so stage A plus stage B is pooled.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

import numpy as np

from app.services.factor_book import BookRefusal
from app.services.factor_book_path import (
    Decision,
    Month,
    PathResult,
    TradeCategory,
    entry_band,
    month_of,
    next_month,
)
from app.services.factor_book_series import ARMS, Scenario, SeriesRun
from app.services.strategy_result import AmbiguityArm
from scripts.report_3609_baselines import window_stats
from scripts.report_3609_step2_operations import Window, annualised, drawdown, stage_windows
from scripts.report_3609_step2_signals import SeriesSummary, summarise
from scripts.report_3609_step2_verdict import BASE, GROSS, G1Result, g1, log_growth

#: §"Diagnostics", per-band sub-books: a month with fewer band-b names is thin. Fixed by construction.
THIN_NAMES: Final = 5
#: Step 0: these categories carry no turnover entry and are printed separately.
NO_TURNOVER: Final = frozenset({TradeCategory.INITIAL_PURCHASE, TradeCategory.FINAL_LIQUIDATION})


class MonthStatus(StrEnum):
    OK = "ok"
    UNDEFINED = "undefined"
    INVALID = "invalid"


@dataclass(frozen=True)
class SubBookMonth:
    """Band b's sub-book for one formation, keyed by its holding month."""

    month: Month
    names: int
    value: float
    gross: float | None
    cost: float
    liquidation: float
    net: float | None
    status: MonthStatus
    thin: bool
    reason: str | None = None


def band_at(decision: Decision, name: int) -> str:
    """``name``'s reporting band at the decision's formation."""
    close = decision.close.get(name)
    if close is None:
        raise ValueError(f"{decision.formation}: name {name} has no raw close at s(M) to band")
    return entry_band(close)[0]


def _finite_sum(values: Sequence[float]) -> float:
    """``math.fsum``, with its overflow and inf − inf raises mapped to the non-finite value they stand for."""
    try:
        return math.fsum(values)
    except OverflowError:
        return math.inf
    except ValueError:
        return math.nan


def net_return(value: float, gross: float, cost: float, liquidation: float) -> tuple[float | None, str | None]:
    """The spec's ratio form, or ``(None, reason)`` for an ``invalid`` month. ``value`` = 0 is the caller's
    ``undefined`` and is checked before this."""
    checks = (
        (math.isfinite(value) and value > 0, f"V_b {value!r} not finite and positive"),
        (math.isfinite(gross) and gross >= -1.0, f"g_b {gross!r} not finite and >= -1"),
        (math.isfinite(cost) and cost >= 0, f"C_b {cost!r} not finite and non-negative"),
        (math.isfinite(liquidation) and liquidation >= 0, f"L_b {liquidation!r} not finite and non-negative"),
    )
    for ok, reason in checks:
        if not ok:
            return None, reason
    numerator = (1.0 + gross) - liquidation / value
    denominator = 1.0 + cost / value
    if not all(math.isfinite(x) for x in (liquidation / value, cost / value, numerator, denominator)):
        return None, "a quotient, the numerator or the denominator is not finite"
    factor = numerator / denominator
    if not (math.isfinite(factor) and factor > 0):
        return None, f"factor {factor!r} not finite and strictly positive"
    net = factor - 1.0
    if net == -1.0:
        return None, "net return computes to exactly -1"
    return net, None


def sub_book_month(
    decision: Decision,
    band: str,
    arm: AmbiguityArm,
    nav: float,
    cost: float,
    liquidation: float,
) -> SubBookMonth:
    """Band ``band`` at ``decision``'s formation. ``nav`` is the path's post-cost NAV at that formation; ``cost`` and
    ``liquidation`` are C_b and L_b."""
    held = next_month(month_of(decision.formation))
    names = [n for n in decision.targets if band_at(decision, n) == band]
    shares = [decision.share(nav, n) for n in names]
    value = _finite_sum(shares)
    thin = len(names) < THIN_NAMES
    if value == 0.0:
        return SubBookMonth(held, len(names), value, None, cost, liquidation, None, MonthStatus.UNDEFINED, thin)
    growth = _finite_sum([s * decision.returns[n].by_arm[arm] for s, n in zip(shares, names, strict=True)])
    gross = growth / value if math.isfinite(value) and value > 0 else math.nan
    net, reason = net_return(value, gross, cost, liquidation)
    status = MonthStatus.OK if reason is None else MonthStatus.INVALID
    return SubBookMonth(held, len(names), value, gross, cost, liquidation, net, status, thin, reason)


@dataclass(frozen=True)
class BandTrades:
    """One band's trades, keyed by formation month (``Trade.month``)."""

    notional: Mapping[Month, float]
    cost: Mapping[Month, float]
    #: Formations after the first: band-b turnover-bearing notional / 2 / the book's pre-trade NAV.
    turnover: Mapping[Month, float]
    #: The initial purchase's and the final liquidation's notional, excluded from turnover.
    excluded: Mapping[Month, float]
    #: Terminal and coverage-exit realisation value, keyed by holding month.
    realised: Mapping[Month, float]


@dataclass(frozen=True)
class SubBook:
    months: Mapping[Month, SubBookMonth]
    trades: BandTrades


def _banded_trades(decisions: Sequence[Decision], path: PathResult) -> dict[str, dict[str, dict[Month, list[float]]]]:
    """``band -> field -> month -> values`` for every trade and realisation on ``path``."""
    by_formation = {month_of(d.formation): d for d in decisions}
    last = decisions[-1]
    out: dict[str, dict[str, dict[Month, list[float]]]] = {}

    def add(band: str, key: str, month: Month, value: float) -> None:
        out.setdefault(band, {}).setdefault(key, {}).setdefault(month, []).append(value)

    for trade in path.trades:
        final = trade.category is TradeCategory.FINAL_LIQUIDATION
        if not final and trade.month not in by_formation:
            raise ValueError(f"trade on {trade.month} has no decision")
        band = band_at(last if final else by_formation[trade.month], trade.name)
        add(band, "notional", trade.month, trade.notional)
        add(band, "cost", trade.month, trade.cost)
        if trade.category in NO_TURNOVER:
            add(band, "excluded", trade.month, trade.notional)
        elif trade.month in path.turnover:
            add(band, "turnover", trade.month, trade.notional / 2.0 / trade.pre_nav)
    for realisation in path.realisations:
        formation = by_formation[_previous(realisation.month)]
        add(band_at(formation, realisation.name), "realised", realisation.month, realisation.value)
    return out


def _previous(month: Month) -> Month:
    year, m = month
    return (year - 1, 12) if m == 1 else (year, m - 1)


def sub_books(decisions: Sequence[Decision], path: PathResult, arm: AmbiguityArm) -> dict[str, SubBook]:
    """Every band's sub-book on one scenario's path. ``decisions`` are the book's, the ones ``path`` valued.

    Bands are every band any target or traded name had at any formation; a formation where a band holds nothing is
    ``undefined`` for it."""
    if not decisions:
        raise ValueError("sub-books need at least one decision")
    if path.nonpositive is not None:
        raise ValueError(f"the path stopped at {path.nonpositive}; the series run refuses that before diagnostics")
    traded = _banded_trades(decisions, path)
    bands = sorted(traded.keys() | {band_at(d, n) for d in decisions for n in d.targets})
    last = month_of(decisions[-1].formation)
    out: dict[str, SubBook] = {}
    for band in bands:
        fields = traded.get(band, {})

        def summed(key: str, fields: Mapping[str, Mapping[Month, list[float]]] = fields) -> dict[Month, float]:
            return {m: _finite_sum(v) for m, v in sorted(fields.get(key, {}).items())}

        cost, notional = summed("cost"), summed("notional")
        months = {}
        for decision in decisions:
            month = month_of(decision.formation)
            liquidation = cost.get(next_month(last), 0.0) if month == last else 0.0
            row = sub_book_month(decision, band, arm, path.nav[month], cost.get(month, 0.0), liquidation)
            months[row.month] = row
        turnover = summed("turnover")
        trades = BandTrades(
            notional,
            cost,
            {m: turnover.get(m, 0.0) for m in sorted(path.turnover)},
            summed("excluded"),
            summed("realised"),
        )
        out[band] = SubBook(months, trades)
    return out


def all_sub_books(decisions: Sequence[Decision], run: SeriesRun) -> dict[Scenario, dict[str, SubBook]]:
    """Every scenario of the book's path: gross, base and stress cost, per arm."""
    return {scenario: sub_books(decisions, path, scenario[0]) for scenario, path in sorted(run.book.items())}


# --------------------------------------------------------------------------- per window


@dataclass(frozen=True)
class WindowTrades:
    """One band's trades over a window's formations (and the 2024-08 liquidation if the window holds it)."""

    notional: float
    cost: float
    #: Step 0's annual turnover: the band's one-way turnover summed, over the window's years of months.
    turnover_per_yr: float
    excluded: float
    realised: float


def window_trades(trades: BandTrades, window: Window, liquidation: Month) -> WindowTrades:
    """``liquidation`` is the final liquidation's month (the path's last holding month); it counts only in a window
    that holds it. Realisations count by their holding month."""
    held = set(window.months)
    keys = {_previous(m) for m in window.months} | ({liquidation} & held)

    def total(values: Mapping[Month, float], chosen: set[Month]) -> float:
        return _finite_sum([v for m, v in values.items() if m in chosen])

    return WindowTrades(
        total(trades.notional, keys),
        total(trades.cost, keys),
        total(trades.turnover, keys) / (len(window.months) / 12.0),
        total(trades.excluded, keys),
        total(trades.realised, held),
    )


@dataclass(frozen=True)
class WindowMetrics:
    label: str
    #: Why the window is uncomputable (an undefined, invalid or thin month); then no metric is set.
    undefined: str | None
    stats: Mapping[str, float | None]
    regression: G1Result | None
    summary: SeriesSummary | None
    trades: WindowTrades

    @property
    def insufficient(self) -> bool:
        """A computable window's metrics are all marked ``insufficient`` below the minimum effective sample; an
        uncomputable window is ``undefined`` instead."""
        return self.summary is not None and self.summary.insufficient


def _blocker(series: Sequence[SubBook], window: Window) -> str | None:
    for month in window.months:
        for book in series:
            row = book.months.get(month)
            if row is None:
                return f"{month}: no sub-book month"
            if row.status is not MonthStatus.OK:
                return f"{month}: {row.status}" + (f" ({row.reason})" if row.reason else "")
            if row.thin:
                return f"{month}: thin ({row.names} names)"
    return None


def window_metrics(
    base: SubBook,
    gross: SubBook,
    b1: Mapping[Month, float],
    factors: Mapping[str, Mapping[Month, float]],
    window: Window,
    liquidation: Month,
) -> WindowMetrics:
    """One band's figures over ``window``. ``b1`` is B1's base-cost returns; ``factors`` are G1's; ``liquidation``
    is the path's last holding month."""
    trades = window_trades(base.trades, window, liquidation)
    blocker = _blocker((base, gross), window)
    if blocker is not None:
        return WindowMetrics(window.label, blocker, {}, None, None, trades)
    months = list(window.months)
    # Every month is ``ok`` here, so every net is set.
    values: dict[str, dict[Month, float]] = {
        cost: {m: v for m in months if (v := book.months[m].net) is not None}
        for cost, book in ((BASE, base), (GROSS, gross))
    }
    bench = {m: b1[m] for m in months}
    with np.errstate(all="ignore"):
        # No stress series, turnover or regression here: stress and turnover are dropped below, G1 runs after.
        raw = window_stats(
            months,
            net=values[BASE],
            gross=values[GROSS],
            stress=values[BASE],
            benchmark=bench,
            turnover={},
            factors={},
            regression=None,
        )
    stats: dict[str, float | None] = {
        k: v if v is not None and math.isfinite(v) else None
        for k, v in raw.items()
        if k not in {"ann_stress_2x", "turnover_per_yr"}
    }
    for cost, key in ((BASE, "ann_net"), (GROSS, "ann_gross")):
        try:
            g = log_growth(values[cost], months, f"sub-book {window.label} {cost}")
        except BookRefusal:
            g = None
        stats[f"{key}_g"] = g
        stats[key] = None if g is None else annualised(g)
    net_ann, gross_ann = stats["ann_net"], stats["ann_gross"]
    drag = None if net_ann is None or gross_ann is None else gross_ann - net_ann
    stats["cost_drag"] = drag if drag is not None and math.isfinite(drag) else None
    logs = [math.log1p(values[BASE][m]) for m in months]
    stats["max_dd_monthly"] = drawdown(logs)
    stats["max_rel_dd_vs_b1"] = drawdown([x - math.log1p(bench[m]) for x, m in zip(logs, months, strict=True)])
    regression = g1(values[BASE], factors, months)
    summary = summarise(dict(values[BASE]), window, ic_ir=False)
    return WindowMetrics(window.label, None, stats, regression, summary, trades)


def windows_of(run: SeriesRun) -> tuple[Window, ...]:
    """Stage A, stage B and pooled over the sub-books' holding months (the run's return months)."""
    return tuple(Window(w.label, w.months, w.months) for w in stage_windows(run))


def all_window_metrics(
    decisions: Sequence[Decision], run: SeriesRun, factors: Mapping[str, Mapping[Month, float]]
) -> dict[AmbiguityArm, dict[str, dict[str, WindowMetrics]]]:
    """Per arm, band and window: :func:`window_metrics` against B1 at base cost."""
    books = all_sub_books(decisions, run)
    out: dict[AmbiguityArm, dict[str, dict[str, WindowMetrics]]] = {}
    for arm in ARMS:
        base, gross = books[(arm, BASE)], books[(arm, GROSS)]
        out[arm] = {
            band: {
                w.label: window_metrics(base[band], gross[band], run.b1[BASE].returns, factors, w, run.months[-1])
                for w in windows_of(run)
            }
            for band in base
        }
    return out


__all__ = [
    "NO_TURNOVER",
    "THIN_NAMES",
    "BandTrades",
    "MonthStatus",
    "SubBook",
    "SubBookMonth",
    "WindowMetrics",
    "WindowTrades",
    "all_sub_books",
    "all_window_metrics",
    "band_at",
    "net_return",
    "sub_book_month",
    "sub_books",
    "window_metrics",
    "window_trades",
    "windows_of",
]
