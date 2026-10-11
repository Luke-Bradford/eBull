"""The #3740 (#3609 step 3) book's holdings rule: eligibility, the MAX filter, forced exits and capped weights.

Spec: ``docs/research/2026-10-10-3740-step3-vw-book.md`` §"The book" steps 3, 5 and 6, §"Source rules" (MAX flag
and treatment, weights) and §"Population and eligibility" (PR #3755). Step 2's holdings rule
(``factor_book_path.book_decisions``) with these differences:

* **Eligible to enter:** in the universe, with a composite, raw close at s(M) >= $5, the age rule, not MAX-flagged,
  and tradable at s(M). The age rule is the caller's: archive seasoning on the historical path, listing age forward
  (slice F). ``tradable`` is ``None`` on the historical path, which has no eToro rule.
* **Forced exits,** in test order after step 2's three: MAX-flagged, then no longer tradable; leaving the top tercile
  stays the one discretionary reason.
* **Weights:** value weight on ME at s(M) across the targets under both caps, by
  :func:`app.services.vw_factor_book.cap_solution` with the formation's universe as the cap reference; the
  infeasible residual is the cash weight.
* **MAX flag** (:func:`max_flagged`): #3621's rule (``avoidance_filters``), top decile of ``rmax1_21d`` over the
  formation's whole population with a value at M, ties at the cutoff flagging, screened data flagging; a value that
  cannot be computed (too few returns, too many zeros) leaves the name unflagged.

The returned steps feed :func:`app.services.vw_factor_book_path.value_path`. A formation the caller could not capture
is passed through as its :class:`~app.services.vw_factor_book_path.Missed` step: no trades, holdings carried.
"""

from __future__ import annotations

import functools
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import Final

from app.services.avoidance_filters import MaxMissing, MaxReading, max_cutoff
from app.services.factor_book import Bands, BookRefusal
from app.services.factor_book_path import MIN_HOLDINGS, PRICE_FLOOR, HoldingReturn, TradeCategory
from app.services.factor_panel_prices import HoldingStatus
from app.services.vw_factor_book import CapSolution, cap_solution, sector_caps
from app.services.vw_factor_book_path import Captured, Missed, Step


class ExitReason(StrEnum):
    """§"The book" step 5, in its test order: the first that holds is the sale's primary reason."""

    LEFT_UNIVERSE = "left_universe"
    LOST_COMPOSITE = "lost_composite"
    BELOW_PRICE_FLOOR = "below_price_floor"
    MAX_FLAGGED = "max_flagged"
    NOT_TRADABLE = "not_tradable"
    LEFT_TERCILE = "left_tercile"


FORCED_REASONS: Final = frozenset(set(ExitReason) - {ExitReason.LEFT_TERCILE})


def max_flagged(readings: Mapping[int, MaxReading]) -> frozenset[int]:
    """#3621's MAX flag over the formation's population (``readings`` for every population name at s(M)).

    ``MAX_EMPTY`` (``avoidance_filters.AvoidanceError``) refuses when no name has a value."""
    cutoff = max_cutoff([r.value for r in readings.values() if r.value is not None])
    return frozenset(
        name
        for name, r in readings.items()
        if r.missing is MaxMissing.SCREENED or (r.value is not None and r.value >= cutoff)
    )


@dataclass(frozen=True)
class Formation:
    """One captured formation's inputs."""

    formation: date
    session: date
    #: The top 1,000 by ME at s(M): the ranking population and the cap reference.
    universe: frozenset[int]
    #: ME at s(M) and FF-12 group (the unclassified bucket is a group) of every universe name.
    me: Mapping[int, float]
    group: Mapping[int, str]
    bands: Bands
    #: Raw close at s(M) for every universe name and every holding.
    close: Mapping[int, float]
    #: The names meeting the age rule at s(M) (archive seasoning historically, listing age forward).
    aged: frozenset[int]
    max_flagged: frozenset[int]
    #: The names tradable at s(M); ``None`` where no eToro rule applies (the historical path).
    tradable: frozenset[int] | None
    #: The holding-month return of every universe name and every holding.
    returns: Mapping[int, HoldingReturn]

    @functools.cached_property
    def scored(self) -> frozenset[int]:
        return frozenset(self.bands.order)

    @functools.cached_property
    def eligible(self) -> frozenset[int]:
        """§"The book" step 3: eligible to enter (never a ranking population)."""
        return frozenset(
            name
            for name in self.bands.order
            if name in self.universe
            and self.close[name] >= PRICE_FLOOR
            and name in self.aged
            and name not in self.max_flagged
            and (self.tradable is None or name in self.tradable)
        )


@dataclass(frozen=True)
class BookDecisions:
    steps: tuple[Step, ...]
    #: §"Insufficient book": captured formations whose post-trade holdings number fewer than ``MIN_HOLDINGS``.
    insufficient: tuple[date, ...]
    #: Each captured formation's cap solution (cap, cash and binding counts are printed from it).
    caps: Mapping[date, CapSolution]
    #: Each captured formation's sales: every reason that held, primary first.
    reasons: Mapping[date, Mapping[int, tuple[ExitReason, ...]]]


def exit_reasons(formation: Formation, name: int) -> tuple[ExitReason, ...]:
    """Every §"The book" step 5 reason that holds for a holding at ``formation``, in test order."""
    return tuple(
        reason
        for reason, holds in (
            (ExitReason.LEFT_UNIVERSE, name not in formation.universe),
            (ExitReason.LOST_COMPOSITE, name not in formation.scored),
            (ExitReason.BELOW_PRICE_FLOOR, formation.close[name] < PRICE_FLOOR),
            (ExitReason.MAX_FLAGGED, name in formation.max_flagged),
            (ExitReason.NOT_TRADABLE, formation.tradable is not None and name not in formation.tradable),
            (ExitReason.LEFT_TERCILE, name not in formation.bands.tercile),
        )
        if holds
    )


def check_formation(formation: Formation, held: frozenset[int]) -> None:
    """``PRICE_INVALID`` for a universe name or holding without a valid raw close; universe names need ME and group."""
    bad = sorted(
        name
        for name in formation.universe | held
        if not ((close := formation.close.get(name)) is not None and math.isfinite(close) and close > 0)
    )
    if bad:
        raise BookRefusal(
            "PRICE_INVALID", f"{formation.formation}: {len(bad)} names lack a finite positive raw close: {bad[:5]}"
        )
    uncovered = sorted(name for name in formation.universe if name not in formation.me or name not in formation.group)
    if uncovered:
        raise ValueError(f"{formation.formation}: {len(uncovered)} universe names lack an ME or group: {uncovered[:5]}")


def carried(step: Step, names: Sequence[int]) -> frozenset[int]:
    """The names still held at the next formation: those whose holding month ended ``observed``."""
    missing = [name for name in names if name not in step.returns]
    if missing:
        raise ValueError(f"{step.formation}: {len(missing)} holdings have no holding-month return: {missing[:5]}")
    return frozenset(name for name in names if step.returns[name].status is HoldingStatus.OBSERVED)


def capped_weights(formation: Formation, targets: Sequence[int]) -> CapSolution:
    """§"The book" step 6: value weights on ME at s(M) under both caps; the universe is the cap reference."""
    reference = {name: formation.me[name] for name in formation.universe}
    return cap_solution(
        {name: formation.me[name] for name in targets}, formation.group, sector_caps(reference, formation.group)
    )


def book_decisions(formations: Sequence[Formation | Missed]) -> BookDecisions:
    """§"The book" steps 3, 5 and 6 at each formation, from an all-cash start; a ``Missed`` step carries holdings."""
    held: frozenset[int] = frozenset()
    steps: list[Step] = []
    insufficient: list[date] = []
    caps: dict[date, CapSolution] = {}
    reasons_by: dict[date, dict[int, tuple[ExitReason, ...]]] = {}
    for formation in formations:
        if isinstance(formation, Missed):
            steps.append(formation)
            held = carried(formation, sorted(held))
            continue
        check_formation(formation, held)
        reasons = {name: found for name in sorted(held) if (found := exit_reasons(formation, name))}
        entrants = (formation.bands.decile & formation.eligible) - held
        targets = tuple(sorted((held - reasons.keys()) | entrants))
        if len(targets) < MIN_HOLDINGS:
            insufficient.append(formation.formation)
        solution = capped_weights(formation, targets)
        sales = {
            name: TradeCategory.FORCED_EXIT if found[0] in FORCED_REASONS else TradeCategory.DISCRETIONARY_EXIT
            for name, found in reasons.items()
        }
        step = Captured(
            formation.formation, targets, solution.weights, solution.cash, sales, formation.close, formation.returns
        )
        steps.append(step)
        caps[formation.formation] = solution
        reasons_by[formation.formation] = reasons
        held = carried(step, targets)
    return BookDecisions(tuple(steps), tuple(insufficient), caps, reasons_by)


__all__ = [
    "FORCED_REASONS",
    "BookDecisions",
    "ExitReason",
    "Formation",
    "book_decisions",
    "capped_weights",
    "carried",
    "check_formation",
    "exit_reasons",
    "max_flagged",
]
