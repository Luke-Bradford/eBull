"""#3609 step 2's construction counts: printed, never gated.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"Source rules", "Fixed by construction" (PR #3666):
uninformative groups ("occurrences, names and book weight affected are counted per operation and printed"), missing
members ("per formation, the count and book weight of each membership pattern") and identifier-decided selections
("every such selection is counted and printed"). Pure functions over one formation's scores, bands and book decision.

* **Population:** the formation's book universe, the population every operation ranks within.
* **Book weight** (§"Diagnostics", Weights): a held name's is its post-trade weight on post-cost NAV, which is its
  ``Decision.share`` (as in ``report_3609_step2_universe``) on every path, since the book holds no cash beside its
  targets; a formation holding nothing is all cash and weighs zero. A name the book sold at the formation is
  "affected" at its pre-trade weight on pre-trade NAV, which depends on the path, so it is printed per arm and cost
  scenario (:func:`sold_weights`).
* **Uninformative groups,** per operation (each characteristic, each family, the composite; zeros included): the
  groups that gave no score, their names, and the book's holdings and weight among those names.
* **Membership pattern** of a name: the operations that gave it a score, in ``CHARACTERISTICS``, ``FAMILIES``,
  composite order, joined with ``+`` (``none`` when nothing scored). A family is present when it scored, which needs at
  least one member scored and an informative family group.
* **Identifier-decided selections,** at the decile and the tercile boundary: the names whose side ``name_key``
  decided (``Bands.decile_by_identifier`` / ``tercile_by_identifier``: every name sharing the composite that straddles
  the boundary), how many of them ``name_key`` put inside the band, and the book's holdings and weight among them.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final

from app.services.factor_book import CHARACTERISTICS, COMPOSITE, FAMILIES, Bands, Scores
from app.services.factor_book_path import SALE_CATEGORIES, Decision, PathResult, month_of
from app.services.factor_book_series import Scenario

#: Every operation, in scoring order.
OPERATIONS: Final = (*CHARACTERISTICS, *FAMILIES, COMPOSITE)
NO_SCORE: Final = "none"


#: Per path scenario, each sold name's pre-trade weight; ``None`` where the path stopped before the formation.
SoldWeights = Mapping[Scenario, Mapping[int, float] | None]


@dataclass(frozen=True)
class Affected:
    """Names a construction rule touched; the book's post-trade holdings and weight among them; and the book's sales
    among them, with their pre-trade weight per path scenario (``None`` where that path stopped earlier)."""

    names: int
    book_names: int
    book_weight: float
    sold_names: int
    sold_weight: Mapping[Scenario, float | None]


@dataclass(frozen=True)
class Uninformative:
    groups: int
    affected: Affected


@dataclass(frozen=True)
class IdentifierDecided:
    #: Of the tied names, those ``name_key`` placed inside the band.
    inside: int
    affected: Affected


@dataclass(frozen=True)
class ConstructionMonth:
    formation: date
    #: Per operation in ``OPERATIONS``.
    uninformative: Mapping[str, Uninformative]
    #: Pattern -> its names; patterns with no name are absent.
    patterns: Mapping[str, Affected]
    decile: IdentifierDecided
    tercile: IdentifierDecided


def sold_weights(decision: Decision, paths: Mapping[Scenario, PathResult]) -> SoldWeights:
    """Each of ``decision``'s sales' pre-trade weight at s(M) on pre-trade NAV: its sale notional (the whole
    position) over the trade's ``pre_nav``, per path. A valued formation's sale trades are exactly its sales."""
    month = month_of(decision.formation)
    out: dict[Scenario, Mapping[int, float] | None] = {}
    for scenario, path in paths.items():
        if path.nonpositive is not None and path.nonpositive < month:
            out[scenario] = None
            continue
        sales = {
            t.name: t.notional / t.pre_nav for t in path.trades if t.month == month and t.category in SALE_CATEGORIES
        }
        if sales.keys() != decision.sales.keys():
            raise ValueError(
                f"{decision.formation}: path {scenario} sold {sorted(sales)}, the book {sorted(decision.sales)}"
            )
        out[scenario] = sales
    return out


def _affected(names: Collection[int], decision: Decision, sold: SoldWeights) -> Affected:
    held = [name for name in decision.targets if name in names]
    gone = [name for name in decision.sales if name in names]
    return Affected(
        len(names),
        len(held),
        math.fsum(decision.share(1.0, name) for name in held),
        len(gone),
        {s: None if w is None else math.fsum(w[name] for name in gone) for s, w in sold.items()},
    )


def pattern(scores: Scores, name: int) -> str:
    """The operations that scored ``name``, ``+``-joined in ``OPERATIONS`` order."""
    present = [op for op in OPERATIONS if name in scores.by_operation.get(op, {})]
    return "+".join(present) or NO_SCORE


def construction_month(
    universe: Sequence[int], scores: Scores, bands: Bands, decision: Decision, sold: SoldWeights
) -> ConstructionMonth:
    """One formation's counts. ``universe`` is the book universe, ``scores`` and ``bands`` its scoring, ``decision``
    the book's decision at this formation and ``sold`` its :func:`sold_weights`."""
    population = set(universe)
    if not set(decision.targets) <= population:
        raise ValueError(f"{decision.formation}: the book holds a name outside the universe")
    if not {name for names in scores.uninformative.values() for name in names} <= population:
        raise ValueError(f"{decision.formation}: an uninformative group holds a name outside the universe")
    by_operation: dict[str, list[tuple[int, ...]]] = defaultdict(list)
    for (operation, _group), names in scores.uninformative.items():
        if operation not in OPERATIONS:
            raise ValueError(f"{decision.formation}: unknown operation {operation!r}")
        by_operation[operation].append(names)
    uninformative = {
        op: Uninformative(
            len(by_operation[op]), _affected({n for names in by_operation[op] for n in names}, decision, sold)
        )
        for op in OPERATIONS
    }
    members: dict[str, set[int]] = defaultdict(set)
    for name in universe:
        members[pattern(scores, name)].add(name)

    def decided(band: frozenset[int], tied: frozenset[int]) -> IdentifierDecided:
        return IdentifierDecided(len(tied & band), _affected(tied, decision, sold))

    return ConstructionMonth(
        formation=decision.formation,
        uninformative=uninformative,
        patterns={key: _affected(names, decision, sold) for key, names in sorted(members.items())},
        decile=decided(bands.decile, bands.decile_by_identifier),
        tercile=decided(bands.tercile, bands.tercile_by_identifier),
    )


def construction(
    universes: Sequence[Sequence[int]],
    scores: Sequence[Scores],
    bands: Sequence[Bands],
    decisions: Sequence[Decision],
    paths: Mapping[Scenario, PathResult],
) -> tuple[ConstructionMonth, ...]:
    """Every formation's counts, in formation order; ``paths`` are the book's valued paths."""
    return tuple(
        construction_month(u, s, b, d, sold_weights(d, paths))
        for u, s, b, d in zip(universes, scores, bands, decisions, strict=True)
    )


__all__ = [
    "NO_SCORE",
    "OPERATIONS",
    "Affected",
    "ConstructionMonth",
    "IdentifierDecided",
    "SoldWeights",
    "Uninformative",
    "construction",
    "construction_month",
    "pattern",
    "sold_weights",
]
