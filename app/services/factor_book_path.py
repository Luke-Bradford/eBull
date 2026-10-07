"""The #3609 step 2 book's path: the holdings rule and the self-financing valuation, on one continuous path.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"The book" steps 3, 5, 6 and 7, the position state
machine, the path ends, §"Insufficient book", the stage boundary state (§"Dates, samples and the hold-out") and the
trade categories (§"References and the control") (PR #3666). Pure functions; the report supplies each formation's
inputs from the panel and runs the scoring core (:mod:`app.services.factor_book`) first.

Two halves, so the matched control (slice 3c) can reuse the second:

* :func:`book_decisions` applies the book's holdings rule once per formation. Selections do not depend on the
  termination arm or the cost scenario (no status depends on the arm), so they are computed once.
* :func:`value_path` values a sequence of decisions under one arm and one cost multiplier, by step 0's one-pass
  self-financing rule (``scripts/report_3609_baselines.py::simulate``): cost = Σ|Δnotional| × half-spread at the
  pre-cost NAV, then equal weights on NAV − cost. A position's half-spread is fixed at its entry by its raw close
  (``cost_model.cost_band_for``, as traded) and never re-keyed; a name that re-enters is a new position.

Timing: formation M trades at s(M), the close ending month M, and holds through month M + 1. A month's return is
NAV at its end (after that formation's trades) over NAV at the previous month end, so a decision month's return
includes its trading cost, and the first return month includes the initial purchase (its denominator is 1.0). The
last holding month carries the final liquidation, which is charged but excluded from turnover.
"""

from __future__ import annotations

import functools
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Final

from app.services import cost_model
from app.services.factor_book import Bands, BookRefusal
from app.services.factor_panel import add_months
from app.services.factor_panel_prices import HoldingStatus
from app.services.strategy_result import AmbiguityArm

#: (year, month): a trade dated M happens at the close ending month M; a return dated M is month M's.
Month = tuple[int, int]

#: §"The book" step 3: the raw-close floor for entering and for staying.
PRICE_FLOOR: Final = 5.0
#: §"Source rules", archive seasoning: the series' first admitted bar at least this many months before s(M).
SEASONING_MONTHS: Final = 36
#: §"Insufficient book", fixed by construction.
MIN_HOLDINGS: Final = 10


class ExitReason(StrEnum):
    """§"The book" step 5, in its test order: the first that holds is the sale's primary reason."""

    LEFT_UNIVERSE = "left_universe"
    LOST_COMPOSITE = "lost_composite"
    BELOW_PRICE_FLOOR = "below_price_floor"
    LEFT_TERCILE = "left_tercile"


FORCED_REASONS: Final = frozenset({ExitReason.LEFT_UNIVERSE, ExitReason.LOST_COMPOSITE, ExitReason.BELOW_PRICE_FLOOR})


class TradeCategory(StrEnum):
    """§"References and the control", trade categories. Terminal realisations are not trades (no order, no cost)."""

    INITIAL_PURCHASE = "initial_purchase"
    FORCED_EXIT = "forced_exit"
    DISCRETIONARY_EXIT = "discretionary_exit"
    #: The control's down-sizing sales (its step 3); the book never makes one.
    COUNT_ADJUSTMENT = "count_adjustment"
    ENTRY = "entry"
    REBALANCE_ADD = "rebalance_add"
    REBALANCE_TRIM = "rebalance_trim"
    FINAL_LIQUIDATION = "final_liquidation"


SALE_CATEGORIES: Final = frozenset(
    {TradeCategory.FORCED_EXIT, TradeCategory.DISCRETIONARY_EXIT, TradeCategory.COUNT_ADJUSTMENT}
)


def month_of(day: date) -> Month:
    return (day.year, day.month)


def next_month(month: Month) -> Month:
    year, m = month
    return (year + 1, 1) if m == 12 else (year, m + 1)


def archive_seasoned(first_bar: date, session: date) -> bool:
    """The series' first admitted bar is at least ``SEASONING_MONTHS`` calendar months before s(M) (day clamped)."""
    return first_bar <= add_months(session, -SEASONING_MONTHS)


@dataclass(frozen=True)
class HoldingReturn:
    """Step 1's holding-month contract for one name: its status and its return per termination arm.

    ``observed`` is the month's total return; ``terminal`` and ``coverage_exit`` are already realised to cash at their
    ``end_bar`` (with the terminal value for ``terminal``), so the position is cash at the next formation."""

    status: HoldingStatus
    by_arm: Mapping[AmbiguityArm, float]


@dataclass(frozen=True)
class Formation:
    """One formation's inputs, from the panel and the scoring core."""

    formation: date
    #: s(M), the decision session.
    session: date
    universe: frozenset[int]
    bands: Bands
    #: Raw close at s(M) for every universe name and every holding.
    close: Mapping[int, float]
    #: The first admitted bar of each universe name's series (archive seasoning).
    first_bar: Mapping[int, date]
    #: The holding-month return of every universe name.
    returns: Mapping[int, HoldingReturn]

    @functools.cached_property
    def scored(self) -> frozenset[int]:
        """The names with a composite."""
        return frozenset(self.bands.order)

    @functools.cached_property
    def eligible(self) -> frozenset[int]:
        return eligible_to_enter(self)


@dataclass(frozen=True)
class Decision:
    """One formation's trades: the post-trade holdings and the category of every sale."""

    formation: date
    targets: tuple[int, ...]
    sales: Mapping[int, TradeCategory]
    close: Mapping[int, float]
    returns: Mapping[int, HoldingReturn]
    #: Every reason that held for each sale, primary first (the rest are printed flags); empty for the control's
    #: count adjustments.
    reasons: Mapping[int, tuple[ExitReason, ...]] = field(default_factory=dict)
    #: Target weight per target name; ``None`` is equal weight (the book and the control).
    weights: Mapping[int, float] | None = None

    def weight(self, name: int) -> float:
        return 1.0 / len(self.targets) if self.weights is None else self.weights[name]

    @property
    def discretionary(self) -> int:
        """k_M: the number of discretionary exits."""
        return sum(1 for category in self.sales.values() if category is TradeCategory.DISCRETIONARY_EXIT)


@dataclass(frozen=True)
class BookDecisions:
    decisions: tuple[Decision, ...]
    #: §"Insufficient book": formations whose post-trade holdings number fewer than ``MIN_HOLDINGS``.
    insufficient: tuple[date, ...]


def check_closes(formation: Formation, held: frozenset[int]) -> None:
    """§"The book" step 3: every universe name and every holding needs a finite, positive raw close at s(M)."""
    bad = sorted(
        name
        for name in formation.universe | held
        if not ((close := formation.close.get(name)) is not None and math.isfinite(close) and close > 0)
    )
    if bad:
        raise BookRefusal(
            "PRICE_INVALID", f"{formation.formation}: {len(bad)} names lack a finite positive raw close: {bad[:5]}"
        )


def exit_reasons(formation: Formation, name: int) -> tuple[ExitReason, ...]:
    """Every §"The book" step 5 reason that holds for a holding at ``formation``, in test order."""
    return tuple(
        reason
        for reason, holds in (
            (ExitReason.LEFT_UNIVERSE, name not in formation.universe),
            (ExitReason.LOST_COMPOSITE, name not in formation.scored),
            (ExitReason.BELOW_PRICE_FLOOR, formation.close[name] < PRICE_FLOOR),
            (ExitReason.LEFT_TERCILE, name not in formation.bands.tercile),
        )
        if holds
    )


def eligible_to_enter(formation: Formation) -> frozenset[int]:
    """§"The book" step 3: universe names with a composite, raw close >= $5 and archive seasoning."""
    missing = sorted(name for name in formation.bands.order if name not in formation.first_bar)
    if missing:
        raise ValueError(
            f"{formation.formation}: {len(missing)} scored names have no first admitted bar: {missing[:5]}"
        )
    return frozenset(
        name
        for name in formation.bands.order
        if name in formation.universe
        and formation.close[name] >= PRICE_FLOOR
        and archive_seasoned(formation.first_bar[name], formation.session)
    )


def still_held(formation: Formation, targets: Sequence[int]) -> frozenset[int]:
    """The targets that are holdings at the next formation: those whose holding month ended ``observed``."""
    missing = [name for name in targets if name not in formation.returns]
    if missing:
        raise ValueError(f"{formation.formation}: {len(missing)} holdings have no holding-month return: {missing[:5]}")
    return frozenset(name for name in targets if formation.returns[name].status is HoldingStatus.OBSERVED)


def book_decisions(formations: Sequence[Formation]) -> BookDecisions:
    """§"The book" step 5 at each formation, from an all-cash start.

    A holding is a name bought at an earlier formation whose holding month ended ``observed``; a ``terminal`` or
    ``coverage_exit`` name is already cash."""
    held: frozenset[int] = frozenset()
    decisions: list[Decision] = []
    insufficient: list[date] = []
    for formation in formations:
        check_closes(formation, held)
        reasons = {name: found for name in sorted(held) if (found := exit_reasons(formation, name))}
        entrants = (formation.bands.decile & formation.eligible) - held
        targets = tuple(sorted((held - reasons.keys()) | entrants))
        if len(targets) < MIN_HOLDINGS:
            insufficient.append(formation.formation)
        sales = {
            name: TradeCategory.FORCED_EXIT if found[0] in FORCED_REASONS else TradeCategory.DISCRETIONARY_EXIT
            for name, found in reasons.items()
        }
        decisions.append(Decision(formation.formation, targets, sales, formation.close, formation.returns, reasons))
        held = still_held(formation, targets)
    return BookDecisions(tuple(decisions), tuple(insufficient))


# --------------------------------------------------------------------------- valuation


@dataclass(frozen=True)
class Position:
    name: int
    value: float
    entry: date
    band: str
    half_spread: float


@dataclass(frozen=True)
class Trade:
    month: Month
    name: int
    category: TradeCategory
    #: Order notional, from the one-pass targets at the pre-cost NAV.
    notional: float
    #: Charged cost (notional × half-spread × the cost multiplier).
    cost: float
    #: NAV before the formation's trades (the minimum-ticket diagnostic's denominator).
    pre_nav: float


@dataclass(frozen=True)
class Realisation:
    """An imputed terminal or coverage-exit realisation: the position's value as it became cash. No order, no cost."""

    month: Month
    name: int
    status: HoldingStatus
    value: float


@dataclass(frozen=True)
class BoundaryState:
    """§"Dates, samples and the hold-out": the state at the boundary formation, before its trades."""

    formation: date
    positions: tuple[Position, ...]
    cash: float
    nav: float


@dataclass
class PathResult:
    #: Per holding month: the net return (the final liquidation charged in the last month).
    returns: dict[Month, float] = field(default_factory=dict)
    #: Per month end: NAV after that month's trades (the last after the liquidation).
    nav: dict[Month, float] = field(default_factory=dict)
    #: One-way turnover per formation after the first: traded notional / 2 / pre-trade NAV (step 0).
    turnover: dict[Month, float] = field(default_factory=dict)
    #: Post-trade holding count per formation.
    holdings: dict[Month, int] = field(default_factory=dict)
    trades: list[Trade] = field(default_factory=list)
    realisations: list[Realisation] = field(default_factory=list)
    boundary: BoundaryState | None = None
    #: The first month whose wealth factor is not finite and positive (§"Decision rule", ``WEALTH_NONPOSITIVE``);
    #: nothing after it is valued.
    nonpositive: Month | None = None


def _half_spread(close: float) -> tuple[str, float]:
    band = cost_model.cost_band_for(Decimal(repr(close)), price_basis="as_traded")
    return band.label, float(band.half_spread)


#: Float weights such as ME / ΣME sum to 1 only to rounding; this bound catches an unnormalised map, nothing finer.
_WEIGHT_SUM_TOLERANCE: Final = 1e-9


def _valid_weights(decision: Decision) -> bool:
    weights = decision.weights or {}
    return (
        weights.keys() == set(decision.targets)
        and all(math.isfinite(w) and w > 0 for w in weights.values())
        and abs(math.fsum(weights.values()) - 1.0) <= _WEIGHT_SUM_TOLERANCE
    )


def value_path(
    decisions: Sequence[Decision],
    *,
    arm: AmbiguityArm,
    cost_multiplier: float,
    boundary: date | None = None,
) -> PathResult:
    """Value ``decisions`` (consecutive monthly formations) on one continuous path from 1.0 in cash.

    ``boundary`` names the formation whose pre-trade state is captured. A month whose wealth factor is not finite
    and positive stops the path: it is recorded in ``nonpositive`` with its return, and nothing later is valued."""
    if not decisions:
        raise ValueError("a path needs at least one decision")
    if boundary is not None and boundary not in {decision.formation for decision in decisions}:
        raise ValueError(f"boundary {boundary} is not one of the decisions' formations")
    result = PathResult()
    positions: dict[int, Position] = {}
    cash = 1.0
    previous = 1.0
    for index, decision in enumerate(decisions):
        month = month_of(decision.formation)
        if index and month != next_month(month_of(decisions[index - 1].formation)):
            raise ValueError(f"formations must be consecutive months: {decisions[index - 1].formation} → {month}")
        if decision.weights is not None and not _valid_weights(decision):
            raise ValueError(f"{decision.formation}: weights must be finite, positive, cover the targets and sum to 1")
        unvalued = [n for n in decision.targets if n not in decision.returns or arm not in decision.returns[n].by_arm]
        if unvalued:
            raise ValueError(f"{decision.formation}: {len(unvalued)} targets have no {arm} return: {unvalued[:5]}")
        unpriced = [
            n
            for n in decision.targets
            if n not in positions and not ((c := decision.close.get(n)) is not None and math.isfinite(c) and c > 0)
        ]
        if unpriced:
            raise BookRefusal(
                "PRICE_INVALID", f"{decision.formation}: entries without a valid raw close: {unpriced[:5]}"
            )
        pre = sum(p.value for p in positions.values()) + cash
        if decision.formation == boundary:
            result.boundary = BoundaryState(
                decision.formation, tuple(positions[name] for name in sorted(positions)), cash, pre
            )
        stray = sorted((positions.keys() - set(decision.targets)) ^ decision.sales.keys())
        if stray:
            raise ValueError(f"{decision.formation}: sales must be exactly the holdings not kept: {stray[:5]}")
        trades: list[tuple[int, TradeCategory, float, float]] = []  # (name, category, notional, half-spread)
        bands: dict[int, tuple[str, float]] = {}
        for name, position in sorted(positions.items()):
            if name in decision.sales:
                trades.append((name, decision.sales[name], position.value, position.half_spread))
            elif delta := pre * decision.weight(name) - position.value:
                category = TradeCategory.REBALANCE_ADD if delta > 0 else TradeCategory.REBALANCE_TRIM
                trades.append((name, category, abs(delta), position.half_spread))
        for name in decision.targets:
            if name not in positions:
                bands[name] = _half_spread(decision.close[name])
                category = TradeCategory.INITIAL_PURCHASE if index == 0 else TradeCategory.ENTRY
                trades.append((name, category, pre * decision.weight(name), bands[name][1]))
        cost = 0.0
        for name, category, notional, half in trades:
            charged = notional * half * cost_multiplier
            cost += charged
            result.trades.append(Trade(month, name, category, notional, charged, pre))
        if index:
            factor = (pre - cost) / previous
            result.returns[month] = factor - 1.0
            if not (math.isfinite(factor) and factor > 0):
                result.nonpositive = month
                return result
            result.turnover[month] = sum(t[2] for t in trades) / 2.0 / pre
        post = pre - cost
        result.nav[month] = post
        if index:  # the initial purchase is charged in the first return month, over the pre-cost 1.0
            previous = post
        result.holdings[month] = len(decision.targets)
        positions = {
            name: replace(positions[name], value=post * decision.weight(name))
            if name in positions
            else Position(name, post * decision.weight(name), decision.formation, *bands[name])
            for name in decision.targets
        }
        cash = 0.0 if decision.targets else post
        # The holding month M + 1.
        held = next_month(month)
        for name in list(positions):
            outcome = decision.returns[name]
            value = positions[name].value * (1.0 + outcome.by_arm[arm])
            if outcome.status is HoldingStatus.OBSERVED:
                positions[name] = replace(positions[name], value=value)
            else:
                result.realisations.append(Realisation(held, name, outcome.status, value))
                cash += value
                del positions[name]
    # The path's end: the last holding month, then every remaining position sold at its band.
    month = next_month(month_of(decisions[-1].formation))
    end = sum(p.value for p in positions.values()) + cash
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
    "FORCED_REASONS",
    "MIN_HOLDINGS",
    "PRICE_FLOOR",
    "SALE_CATEGORIES",
    "SEASONING_MONTHS",
    "BookDecisions",
    "BoundaryState",
    "Decision",
    "ExitReason",
    "Formation",
    "HoldingReturn",
    "Month",
    "PathResult",
    "Position",
    "Realisation",
    "Trade",
    "TradeCategory",
    "archive_seasoned",
    "book_decisions",
    "check_closes",
    "eligible_to_enter",
    "exit_reasons",
    "month_of",
    "next_month",
    "still_held",
    "value_path",
]
