"""The #3740 (#3609 step 3) matched random control.

Spec: ``docs/research/2026-10-10-3740-step3-vw-book.md`` §"References and the control" (PR #3755): step 2's
target-count control (``factor_book_control``: n_M, k_M, the three samples in order, ``CONTROL_SHORT``) with these
differences:

* its forced exits are step 3's (:data:`~app.services.vw_factor_book_decisions.FORCED_REASONS`), so they include
  MAX-flagged and no-longer-tradable holdings;
* it buys from this book's eligible-to-enter set;
* its holdings are value-weighted under both caps by the same cap solution and reference, with the same cash rule;
* a missed formation is passed through, as for the book (the caller's ``Missed`` returns cover every series' holdings);
* seeds are ``random.Random(f"3740-step3:{d}:{M.isoformat()}")``.

One draw is a sequence of steps that :func:`~app.services.vw_factor_book_path.value_path` values exactly as it values
the book.
"""

from __future__ import annotations

import random
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date

from app.services.factor_book import BookRefusal
from app.services.factor_book_control import DRAWS, Pool, _sample
from app.services.factor_book_path import TradeCategory
from app.services.vw_factor_book_decisions import (
    FORCED_REASONS,
    BookDecisions,
    Formation,
    capped_weights,
    carried,
    check_formation,
    exit_reasons,
)
from app.services.vw_factor_book_path import Captured, Missed, Step


def control_seed(draw: int, formation: date) -> str:
    return f"3740-step3:{draw}:{formation.isoformat()}"


@dataclass(frozen=True)
class ControlDraw:
    draw: int
    steps: tuple[Step, ...]
    pools: tuple[Pool, ...]


def control_decisions(formations: Sequence[Formation | Missed], book: BookDecisions, draw: int) -> ControlDraw:
    """One draw of the control over the book's formations, from an all-cash start."""
    if [s.formation for s in book.steps] != [f.formation for f in formations] or any(
        isinstance(s, Missed) != isinstance(f, Missed) for s, f in zip(book.steps, formations, strict=True)
    ):
        raise ValueError("the book's steps must cover exactly the control's formations, captured or missed, in order")
    held: frozenset[int] = frozenset()
    steps: list[Step] = []
    pools: list[Pool] = []
    for formation, book_step in zip(formations, book.steps, strict=True):
        if isinstance(formation, Missed):
            steps.append(formation)
            held = carried(formation, sorted(held))
            continue
        if not isinstance(book_step, Captured):  # unreachable after the check above; narrows, and survives -O
            raise ValueError(f"{formation.formation}: the book's step is not captured")
        rng = random.Random(control_seed(draw, formation.formation))
        check_formation(formation, held)
        n = len(book_step.targets)
        k = sum(1 for c in book_step.sales.values() if c is TradeCategory.DISCRETIONARY_EXIT)
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
        solution = capped_weights(formation, targets)
        step = Captured(
            formation.formation, targets, solution.weights, solution.cash, sales, formation.close, formation.returns
        )
        steps.append(step)
        held = carried(step, targets)
    return ControlDraw(draw, tuple(steps), tuple(pools))


__all__ = ["DRAWS", "ControlDraw", "control_decisions", "control_seed"]
