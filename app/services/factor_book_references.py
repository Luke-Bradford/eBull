"""The #3609 step 2 universe references: equal-weight and reconstituted cap-weighted.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"References and the control" (PR #3666).

* **Equal-weight universe:** all 1,000 names, rebalanced monthly to equal weight, costed the same way as the book.
  Used for attribution.
* **Reconstituted cap-weighted universe:** the top 1,000 at each formation, cap-weighted by ME at s(M) and rebalanced
  to those weights monthly, costed on its own turnover at step 0's bands. A reconstituted index, not a buy-and-hold
  portfolio.

Both are sequences of :class:`~app.services.factor_book_path.Decision` that
:func:`~app.services.factor_book_path.value_path` values like the book (statuses, arms, bands, the one-pass rule).
A name that leaves the universe is sold as a forced exit (``left_universe``). The $5 and seasoning rules are entry
rules of the book only; the references hold the whole universe.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence

from app.services.factor_book import BookRefusal
from app.services.factor_book_path import (
    Decision,
    ExitReason,
    Formation,
    TradeCategory,
    check_closes,
    still_held,
)


def reference_decisions(
    formations: Sequence[Formation], me: Sequence[Mapping[int, float]] | None = None
) -> tuple[Decision, ...]:
    """The equal-weight universe, or with ``me`` (one map per formation) the cap-weighted one."""
    if me is not None and len(me) != len(formations):
        raise ValueError(f"{len(me)} ME maps for {len(formations)} formations")
    held: frozenset[int] = frozenset()
    out: list[Decision] = []
    for index, formation in enumerate(formations):
        check_closes(formation, held)
        targets = tuple(sorted(formation.universe))
        weights: dict[int, float] | None = None
        if me is not None:
            caps = me[index]
            bad = sorted(n for n in targets if not ((v := caps.get(n)) is not None and math.isfinite(v) and v > 0))
            if bad:
                raise BookRefusal("ME_INVALID", f"{formation.formation}: {len(bad)} universe names: {bad[:5]}")
            total = math.fsum(caps[n] for n in targets)
            weights = {n: caps[n] / total for n in targets}
        left = sorted(held - formation.universe)
        out.append(
            Decision(
                formation.formation,
                targets,
                dict.fromkeys(left, TradeCategory.FORCED_EXIT),
                formation.close,
                formation.returns,
                {n: (ExitReason.LEFT_UNIVERSE,) for n in left},
                weights,
            )
        )
        held = still_held(formation, targets)
    return tuple(out)


__all__ = ["reference_decisions"]
