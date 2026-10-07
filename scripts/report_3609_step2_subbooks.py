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
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from app.services.factor_book_path import (
    Decision,
    Month,
    PathResult,
    TradeCategory,
    entry_band,
    month_of,
    next_month,
)
from app.services.factor_book_series import Scenario, SeriesRun
from app.services.strategy_result import AmbiguityArm

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


__all__ = [
    "NO_TURNOVER",
    "THIN_NAMES",
    "BandTrades",
    "MonthStatus",
    "SubBook",
    "SubBookMonth",
    "all_sub_books",
    "band_at",
    "net_return",
    "sub_book_month",
    "sub_books",
]
