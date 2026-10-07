"""#3609 step 2's operations diagnostics (slice 3c-v(a)): printed, never gated.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"Diagnostics" (PR #3666): the windows, "Book, per
window and per calendar year", "Operations" (the minimum ticket) and "Control"; §"References and the control",
"Percentiles". Pure functions over :class:`~app.services.factor_book_series.SeriesRun`, which has already refused
every data, comparator and non-positive-wealth condition. The statistics are step 0's
(``scripts/report_3609_baselines.py``): ``window_stats`` and ``nearest_rank``.

**Windows.** A window is a set of return months plus the formations whose trades those months are charged. The path
charges formation M's trades in return month M (``scripts/report_3609_step2_verdict.py``, "Month labels"), so:

* **Stage A** is return months 2014-10..2021-05 as the path valued them. Its last month carries the boundary
  formation's (2021-05) trading cost and turnover, which stage B also counts.
* **Stage B** starts from the boundary's pre-trade NAV (:func:`stage_b_returns`): 2021-06..2024-08 with the 2021-05
  cost folded into its first month, and formations 2021-05..2024-07.
* **Pooled** and **each calendar year** are the path's own return months, each with the formations in them. So
  pooled is not stage A plus stage B: the boundary cost is counted once.

Turnover follows step 0: the initial purchase and the final liquidation have no turnover entry.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from app.services.factor_book_path import BoundaryState, Decision, Month, PathResult, Trade, month_of, next_month
from app.services.factor_book_series import ARMS, SeriesRun, Summary
from app.services.factor_panel_prices import HoldingStatus
from app.services.strategy_result import AmbiguityArm
from scripts.report_3609_baselines import nearest_rank, window_stats
from scripts.report_3609_step2_verdict import (
    BASE,
    GROSS,
    STAGE_B_FORMATIONS,
    STRESS,
    log_growth,
    stage_b_months,
    stage_b_returns,
)

#: §"Diagnostics", Operations: the minimum ticket, in dollars.
MIN_TICKET: Final = 10.0
#: ``value_path`` starts every path from 1.0 in cash.
NAV_0: Final = 1.0
#: §"References and the control": the control's printed quantiles.
CONTROL_PERCENTILES: Final = (5, 50, 95)


# --------------------------------------------------------------------------- windows


@dataclass(frozen=True)
class Window:
    label: str
    #: Return months, in order.
    months: tuple[Month, ...]
    #: The formations whose trades those months are charged (the path's turnover keys).
    formations: tuple[Month, ...]
    #: Stage B: the slice starts from the boundary's pre-trade NAV.
    from_boundary: bool = False


def _own(label: str, months: Sequence[Month]) -> Window:
    return Window(label, tuple(months), tuple(months))


def stage_windows(run: SeriesRun) -> tuple[Window, ...]:
    """Stage A, stage B and pooled, as the module docstring defines them."""
    stage_b = stage_b_months(run.months)
    first, last = STAGE_B_FORMATIONS
    formations = [m for m in run.months if first <= m <= last]
    return (
        _own("stage A", [m for m in run.months if m < stage_b[0]]),
        Window("stage B", tuple(stage_b), tuple(formations), from_boundary=True),
        _own("pooled", run.months),
    )


def calendar_years(run: SeriesRun) -> tuple[Window, ...]:
    years = sorted({m[0] for m in run.months})
    return tuple(_own(str(year), [m for m in run.months if m[0] == year]) for year in years)


def window_returns(
    returns: Mapping[Month, float], nav: Mapping[Month, float], boundary: BoundaryState | None, window: Window
) -> dict[Month, float]:
    if window.from_boundary:
        return stage_b_returns(returns, nav, boundary, window.months)
    return {m: returns[m] for m in window.months}


def turnover_per_year(turnover: Mapping[Month, float], window: Window) -> float:
    """Step 0's annual turnover: the window's one-way turnover summed, over its years of return months."""
    return math.fsum(turnover.get(m, 0.0) for m in window.formations) / (len(window.months) / 12.0)


# --------------------------------------------------------------------------- the minimum ticket


@dataclass(frozen=True)
class MinimumTicket:
    """The largest NAV, and initial capital, at which some trade on the path falls below ``MIN_TICKET``."""

    nav: float
    nav_trade: Trade
    capital: float
    capital_trade: Trade


def minimum_ticket(path: PathResult) -> MinimumTicket:
    """Every trade counts, the final liquidation and the rebalance adds and trims included. A trade's weight is its
    notional over its pre-trade NAV; it falls below the ticket at a NAV of ``MIN_TICKET / weight``, and at an
    initial capital of that × NAV_0 / NAV_t. A zero-notional trade falls below it at any NAV (``inf``). The first
    trade in path order wins a tie."""
    if not path.trades:
        raise ValueError("the minimum ticket needs at least one trade")

    def at(trade: Trade, nav: float) -> float:
        return math.inf if trade.notional <= 0 else MIN_TICKET * nav / trade.notional

    nav_trade = max(path.trades, key=lambda t: at(t, t.pre_nav))
    capital_trade = max(path.trades, key=lambda t: at(t, NAV_0))
    return MinimumTicket(at(nav_trade, nav_trade.pre_nav), nav_trade, at(capital_trade, NAV_0), capital_trade)


# --------------------------------------------------------------------------- the book, per window


def status_weights(decisions: Sequence[Decision], window: Window) -> dict[HoldingStatus, float]:
    """The mean, over the window's months, of the post-trade weight held into each month grouped by the status that
    month ends in. A month held in cash adds zero to every status."""
    held = {next_month(month_of(d.formation)): d for d in decisions}
    totals = dict.fromkeys(HoldingStatus, 0.0)
    for month in window.months:
        decision = held[month]
        for name in decision.targets:
            totals[decision.returns[name].status] += decision.share(1.0, name)
    return {status: total / len(window.months) for status, total in totals.items()}


def book_size(path: PathResult, window: Window) -> float:
    """The mean post-trade holding count held into the window's months."""
    held = {next_month(formation): count for formation, count in path.holdings.items()}
    return math.fsum(held[m] for m in window.months) / len(window.months)


def book_stats(run: SeriesRun, decisions: Sequence[Decision], arm: AmbiguityArm, window: Window) -> dict[str, object]:
    """§"Diagnostics", "Book, per window and per calendar year": step 0's figures against B1 (base cost), without its
    regression (Attribution prints that). No Sharpe."""

    def returns(cost: str) -> dict[Month, float]:
        path = run.book[(arm, cost)]
        return window_returns(path.returns, path.nav, path.boundary, window)

    base = run.book[(arm, BASE)]
    b1 = {m: run.b1[BASE].returns[m] for m in window.months}
    stats: dict[str, object] = dict(
        window_stats(list(window.months), returns(BASE), returns(GROSS), returns(STRESS), b1, {}, {}, None)
    )
    stats["turnover_per_yr"] = turnover_per_year(base.turnover, window)
    stats["book_size"] = book_size(base, window)
    stats["status_weights"] = status_weights(decisions, window)
    return stats


# --------------------------------------------------------------------------- the control


@dataclass(frozen=True)
class Distribution:
    """The control's nearest-rank quantiles (``CONTROL_PERCENTILES``) and the book's mid-rank percentile."""

    quantiles: tuple[float, ...]
    book: float
    book_percentile: float


def mid_rank(values: Sequence[float], x: float) -> float:
    """§"Percentiles": (draws below ``x`` + half the draws equal to it) / draws."""
    below = sum(1 for v in values if v < x)
    equal = sum(1 for v in values if v == x)
    return (below + 0.5 * equal) / len(values)


def distribution(draws: Sequence[float], book: float) -> Distribution:
    if not draws:
        raise ValueError("no percentile is computed without the control's draws")
    return Distribution(tuple(nearest_rank(draws, p) for p in CONTROL_PERCENTILES), book, mid_rank(draws, book))


#: The control's printed metrics: net and gross annualised log growth G, step 0's annual turnover (base cost) and
#: cost drag (annualised gross minus annualised net, exp(G) − 1 each).
CONTROL_METRICS: Final = ("net_g", "gross_g", "turnover_per_yr", "cost_drag")


def _metrics(net: Summary | PathResult, gross: Summary | PathResult, window: Window, label: str) -> dict[str, float]:
    g = {
        cost: log_growth(window_returns(s.returns, s.nav, s.boundary, window), window.months, f"{label} {cost}")
        for cost, s in ((BASE, net), (GROSS, gross))
    }
    return {
        "net_g": g[BASE],
        "gross_g": g[GROSS],
        "turnover_per_yr": turnover_per_year(net.turnover, window),
        "cost_drag": math.exp(g[GROSS]) - math.exp(g[BASE]),
    }


def control_distributions(run: SeriesRun, arm: AmbiguityArm, window: Window) -> dict[str, Distribution]:
    """§"Diagnostics", Control: per metric, the control's quantiles over every draw and the book's percentile. The
    returns compare G values, never rounded returns (§"Decision rule")."""
    draws = [
        _metrics(net, gross, window, f"control draw {d} {arm}")
        for d, (net, gross) in enumerate(zip(run.control[(arm, BASE)], run.control[(arm, GROSS)], strict=True))
    ]
    book = _metrics(run.book[(arm, BASE)], run.book[(arm, GROSS)], window, f"book {arm}")
    return {name: distribution([d[name] for d in draws], book[name]) for name in CONTROL_METRICS}


# --------------------------------------------------------------------------- assembly


@dataclass(frozen=True)
class Operations:
    #: Per arm, at base cost.
    minimum_ticket: Mapping[str, MinimumTicket]
    #: Per arm, then per window label (stage windows, then calendar years).
    book: Mapping[str, Mapping[str, Mapping[str, object]]]
    #: Per arm, then per stage window label, then per metric.
    control: Mapping[str, Mapping[str, Mapping[str, Distribution]]]


def operations(run: SeriesRun, decisions: Sequence[Decision]) -> Operations:
    """Every figure in this module, for both arms. ``decisions`` are the book's (``book_decisions``)."""
    stages = stage_windows(run)
    windows = (*stages, *calendar_years(run))
    return Operations(
        minimum_ticket={arm: minimum_ticket(run.book[(arm, BASE)]) for arm in ARMS},
        book={arm: {w.label: book_stats(run, decisions, arm, w) for w in windows} for arm in ARMS},
        control={arm: {w.label: control_distributions(run, arm, w) for w in stages} for arm in ARMS},
    )


__all__ = [
    "CONTROL_METRICS",
    "CONTROL_PERCENTILES",
    "MIN_TICKET",
    "NAV_0",
    "Distribution",
    "MinimumTicket",
    "Operations",
    "Window",
    "book_size",
    "book_stats",
    "calendar_years",
    "control_distributions",
    "distribution",
    "mid_rank",
    "minimum_ticket",
    "operations",
    "stage_windows",
    "status_weights",
    "turnover_per_year",
    "window_returns",
]
