"""The #3740 (#3609 step 3) book's valuation: cash, missed formations and the first actual purchase.

Spec: ``docs/research/2026-10-10-3740-step3-vw-book.md`` §"Source rules" ("Cash in the path", "Empty targets") and
§"Forward accrual" (start, end, the missed-formation transition) (PR #3755). Step 2's
:func:`app.services.factor_book_path.value_path` refuses weights that do not sum to 1 and holds no cash while targets
exist; this module is its step-3 generalisation, kept separate because step 2's declared run hashes
``factor_book*.py`` (see :mod:`app.services.vw_factor_book`). Step 2's one-pass self-financing rule, timing, bands,
realisations and final liquidation are reused unchanged, with these differences:

* **Cash weight.** A captured formation carries target weights w_i and a cash weight c, Σ w + c = 1 (the cap
  solution's residual when the caps are infeasible; 1 with zero targets). Trade notionals are w_i × pre-cost NAV less
  the held value; target values are w_i × post-cost NAV and cash is the residual (c × post-cost NAV to rounding).
  Carried cash, terminal and coverage-exit proceeds and sale proceeds are all in the pre-trade NAV. Cash trades cost
  nothing and are not turnover.
* **Missed formation.** No trades: positions keep their values and entry bands, cash is carried, and every carried
  holding still takes its holding-month return; terminal and coverage-exit proceeds join cash. Its month's return is
  the NAV change alone.
* **Initial purchase.** The first formation at which the series buys anything, not the first formation. Its trades
  are ``INITIAL_PURCHASE`` and its formation is excluded from turnover. A purchase at the first formation is charged
  in the next month's return over the pre-cost 1.0 (step 2's timing); a delayed first purchase is charged in its own
  formation month's return, like every later formation's trades.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date
from typing import Final

from app.services.factor_book import BookRefusal
from app.services.factor_book_path import (
    HoldingReturn,
    Month,
    PathResult,
    Position,
    Realisation,
    Trade,
    TradeCategory,
    check_cost_multiplier,
    entry_band,
    month_of,
    next_month,
)
from app.services.factor_panel_prices import HoldingStatus
from app.services.strategy_result import AmbiguityArm

#: Spec "The cap solution", tests: Σ w + cash = 1 to 1e-12. The cash leg is the post-trade residual, so a total
#: inside this bound cannot create or destroy NAV.
WEIGHT_SUM_TOLERANCE: Final = 1e-12


@dataclass(frozen=True)
class Captured:
    """A captured formation's trades: post-trade targets with their weights and the cash weight."""

    formation: date
    targets: tuple[int, ...]
    weights: Mapping[int, float]
    cash: float
    sales: Mapping[int, TradeCategory]
    close: Mapping[int, float]
    #: The holding-month return of every target.
    returns: Mapping[int, HoldingReturn]


@dataclass(frozen=True)
class Missed:
    """A missed formation (§"Lifecycle and verdict table", ``formation_missed``): no trades."""

    formation: date
    reason: str
    #: The holding-month return of every position carried through it.
    returns: Mapping[int, HoldingReturn]


Step = Captured | Missed


@dataclass(frozen=True)
class FormationLedger:
    """One formation's NAV accounting: pre-trade positions + cash = post-trade positions + cash + cost."""

    month: Month
    pre_positions: float
    pre_cash: float
    cost: float
    post_positions: float
    post_cash: float


@dataclass
class VwPathResult(PathResult):
    #: Per formation, its NAV accounting (printed beside the cap and cash counts).
    ledger: list[FormationLedger] = field(default_factory=list)
    #: The missed formations and their reasons.
    missed: dict[Month, str] = field(default_factory=dict)
    #: The formation whose trades are the initial purchase; ``None`` while the series has bought nothing.
    initial_purchase: Month | None = None


def _check_captured(step: Captured, positions: Mapping[int, Position]) -> None:
    if step.weights.keys() != set(step.targets) or not all(math.isfinite(w) and w > 0 for w in step.weights.values()):
        raise ValueError(f"{step.formation}: weights must be finite, positive and cover exactly the targets")
    if not (math.isfinite(step.cash) and step.cash >= 0):
        raise ValueError(f"{step.formation}: the cash weight must be finite and non-negative, got {step.cash!r}")
    if abs(math.fsum([*step.weights.values(), step.cash]) - 1.0) > WEIGHT_SUM_TOLERANCE:
        raise ValueError(f"{step.formation}: weights and cash must sum to 1")
    unpriced = [
        n
        for n in step.targets
        if n not in positions and not ((c := step.close.get(n)) is not None and math.isfinite(c) and c > 0)
    ]
    if unpriced:
        raise BookRefusal("PRICE_INVALID", f"{step.formation}: entries without a valid raw close: {unpriced[:5]}")
    stray = sorted((positions.keys() - set(step.targets)) ^ step.sales.keys())
    if stray:
        raise ValueError(f"{step.formation}: sales must be exactly the holdings not kept: {stray[:5]}")


def value_path(steps: Sequence[Step], *, arm: AmbiguityArm, cost_multiplier: float) -> VwPathResult:
    """Value ``steps`` (consecutive monthly formations, captured or missed) on one continuous path from 1.0 in cash.

    A month whose wealth factor is not finite and positive stops the path, as in step 2."""
    if not steps:
        raise ValueError("a path needs at least one formation")
    check_cost_multiplier(cost_multiplier)
    result = VwPathResult()
    positions: dict[int, Position] = {}
    cash = 1.0
    previous = 1.0
    for index, step in enumerate(steps):
        month = month_of(step.formation)
        if index and month != next_month(month_of(steps[index - 1].formation)):
            raise ValueError(f"formations must be consecutive months: {steps[index - 1].formation} → {month}")
        pre_positions = math.fsum(p.value for p in positions.values())
        pre = pre_positions + cash
        trades: list[tuple[int, TradeCategory, float, float]] = []  # (name, category, notional, half-spread)
        bands: dict[int, tuple[str, float]] = {}
        post_cash = cash  # a missed formation carries its cash
        if isinstance(step, Missed):
            result.missed[month] = step.reason
        else:
            _check_captured(step, positions)
            buys = any(name not in positions for name in step.targets)
            first = buys and result.initial_purchase is None
            for name, position in sorted(positions.items()):
                if name in step.sales:
                    trades.append((name, step.sales[name], position.value, position.half_spread))
                elif delta := step.weights[name] * pre - position.value:
                    category = TradeCategory.REBALANCE_ADD if delta > 0 else TradeCategory.REBALANCE_TRIM
                    trades.append((name, category, abs(delta), position.half_spread))
            for name in step.targets:
                if name not in positions:
                    bands[name] = entry_band(step.close[name])
                    category = TradeCategory.INITIAL_PURCHASE if first else TradeCategory.ENTRY
                    trades.append((name, category, step.weights[name] * pre, bands[name][1]))
            if first:
                result.initial_purchase = month
        cost = 0.0
        for name, category, notional, half in trades:
            charged = notional * half * cost_multiplier
            cost += charged
            result.trades.append(Trade(month, name, category, notional, charged, pre))
        post = pre - cost
        if index:
            factor = post / previous
            result.returns[month] = factor - 1.0
            if not (math.isfinite(factor) and factor > 0):
                result.nonpositive = month
                return result
            if isinstance(step, Captured) and result.initial_purchase != month:
                result.turnover[month] = sum(t[2] for t in trades) / 2.0 / pre
            previous = post  # a purchase at the first formation is charged in the next month, over the pre-cost 1.0
        result.nav[month] = post
        if isinstance(step, Captured):
            positions = {
                name: replace(positions[name], value=step.weights[name] * post)
                if name in positions
                else Position(name, step.weights[name] * post, step.formation, *bands[name])
                for name in step.targets
            }
            # The residual, not ``step.cash * post``: positions + cash = post to an ulp, whatever the weights' rounding.
            post_cash = post - math.fsum(p.value for p in positions.values())
        result.ledger.append(
            FormationLedger(month, pre_positions, cash, cost, math.fsum(p.value for p in positions.values()), post_cash)
        )
        cash = post_cash
        result.holdings[month] = len(positions)
        # The holding month M + 1.
        missing = [name for name in positions if name not in step.returns or arm not in step.returns[name].by_arm]
        if missing:
            raise ValueError(f"{step.formation}: {len(missing)} holdings have no {arm} return: {missing[:5]}")
        held = next_month(month)
        for name in list(positions):
            outcome = step.returns[name]
            value = positions[name].value * (1.0 + outcome.by_arm[arm])
            if outcome.status is HoldingStatus.OBSERVED:
                positions[name] = replace(positions[name], value=value)
            else:
                result.realisations.append(Realisation(held, name, outcome.status, value))
                cash += value
                del positions[name]
    # The path's end: the last holding month, then every remaining position sold at its band (step 2).
    month = next_month(month_of(steps[-1].formation))
    end = math.fsum(p.value for p in positions.values()) + cash
    cost = 0.0
    for name, position in sorted(positions.items()):
        charged = position.value * position.half_spread * cost_multiplier
        cost += charged
        result.trades.append(Trade(month, name, TradeCategory.FINAL_LIQUIDATION, position.value, charged, end))
    factor = (end - cost) / previous
    result.returns[month] = factor - 1.0
    if not (math.isfinite(factor) and factor > 0):
        result.nonpositive = month
        return result
    result.nav[month] = end - cost
    return result


__all__ = [
    "WEIGHT_SUM_TOLERANCE",
    "Captured",
    "FormationLedger",
    "Missed",
    "Step",
    "VwPathResult",
    "value_path",
]
