"""The #3609 step 2 matched random control: random selection from the book's own opportunity set.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"References and the control", "Matched random
control" (PR #3666). One draw is a sequence of :class:`~app.services.factor_book_path.Decision` that
:func:`~app.services.factor_book_path.value_path` values exactly as it values the book (weights, costs, bands,
statuses and arms all match).

**Estimand.** After each formation the control holds the book's count n_M, and its random replacements are capped at
the book's discretionary exit count k_M. It does not match total turnover, traded notional or cost: its forced exits
and count adjustments are its own.

At each formation M, in order:
1. forced exits: its holdings that left the universe, lost a composite or closed below $5;
2. discretionary exits: min(k_M, h) holdings chosen at random, h its count after step 1;
3. down-sizing: if it still holds more than n_M, the excess, chosen at random;
4. purchases: names chosen at random from the book's eligible-to-enter set, excluding its holdings and every name it
   sold at M, until it holds n_M. A shortage refuses the run (``CONTROL_SHORT``).

**Reproducibility.** One ``random.Random(f"3609-step2:{d}:{M.isoformat()}")`` per draw and formation; every sampled
population is sorted by ``name_key`` ascending, and the three calls run in the order above as
``rng.sample(population, count)``.
"""

from __future__ import annotations

import random
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final

from app.services.factor_book import BookRefusal
from app.services.factor_book_path import (
    FORCED_REASONS,
    BookDecisions,
    Decision,
    Formation,
    TradeCategory,
    check_closes,
    exit_reasons,
    still_held,
)

DRAWS: Final = 1000


def control_seed(draw: int, formation: date) -> str:
    return f"3609-step2:{draw}:{formation.isoformat()}"


def _sample(rng: random.Random, population: Collection[int], count: int) -> list[int]:
    """``rng.sample`` over the population sorted by ``name_key`` ascending (set order is not reproducible)."""
    return rng.sample(sorted(population), count)


@dataclass(frozen=True)
class Pool:
    """One formation's purchase step for one draw: the pool it sampled from and the purchases it needed."""

    formation: date
    pool: int
    needed: int


@dataclass(frozen=True)
class ControlDraw:
    draw: int
    decisions: tuple[Decision, ...]
    pools: tuple[Pool, ...]


def control_decisions(formations: Sequence[Formation], book: BookDecisions, draw: int) -> ControlDraw:
    """One draw of the control over the book's formations, from an all-cash start."""
    if [d.formation for d in book.decisions] != [f.formation for f in formations]:
        raise ValueError("the book's decisions must cover exactly the control's formations, in order")
    held: frozenset[int] = frozenset()
    decisions: list[Decision] = []
    pools: list[Pool] = []
    for formation, book_decision in zip(formations, book.decisions, strict=True):
        rng = random.Random(control_seed(draw, formation.formation))
        check_closes(formation, held)
        n, k = len(book_decision.targets), book_decision.discretionary
        sales: dict[int, TradeCategory] = {}
        for name in sorted(held):
            reasons = exit_reasons(formation, name)
            if reasons and reasons[0] in FORCED_REASONS:
                sales[name] = TradeCategory.FORCED_EXIT
        remaining = held - sales.keys()
        for name in _sample(rng, remaining, min(k, len(remaining))):
            sales[name] = TradeCategory.DISCRETIONARY_EXIT
        remaining = held - sales.keys()
        for name in _sample(rng, remaining, max(len(remaining) - n, 0)):
            sales[name] = TradeCategory.COUNT_ADJUSTMENT
        kept = held - sales.keys()
        pool = formation.eligible - held  # every name sold at M was held, so this excludes them too
        needed = n - len(kept)
        pools.append(Pool(formation.formation, len(pool), needed))
        if needed > len(pool):
            raise BookRefusal(
                "CONTROL_SHORT",
                f"draw {draw} at {formation.formation}: {needed} purchases needed from {len(pool)} eligible names",
            )
        targets = tuple(sorted(kept | set(_sample(rng, pool, needed))))
        decisions.append(Decision(formation.formation, targets, sales, formation.close, formation.returns))
        held = still_held(formation, targets)
    return ControlDraw(draw, tuple(decisions), tuple(pools))


__all__ = ["DRAWS", "ControlDraw", "Pool", "control_decisions", "control_seed"]
