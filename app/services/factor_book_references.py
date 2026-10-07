"""The #3609 step 2 universe references: equal-weight and reconstituted cap-weighted.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"References and the control" (PR #3666).

* **Equal-weight universe:** all 1,000 names, rebalanced monthly to equal weight, costed the same way as the book.
  Used for attribution.
* **B1:** step 0's SPY total-return path (``paths.json``, ``"B1 SPY"``), its continuing returns over the window,
  with entry and exit re-costed at the band of SPY's raw close at the path's start (step 0's saved costs carry its
  2009-12 band). One band applies to both trades, and both are charged.
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
from dataclasses import dataclass

from app.services.factor_book import BookRefusal
from app.services.factor_book_path import (
    Decision,
    ExitReason,
    Formation,
    Month,
    TradeCategory,
    check_closes,
    check_cost_multiplier,
    entry_band,
    next_month,
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
        if me is not None and targets:  # an empty universe holds cash, as the equal-weight path does
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


@dataclass(frozen=True)
class B1Path:
    returns: dict[Month, float]
    band: str
    #: The two charges in NAV units: the purchase on the starting 1.0, the sale on the wealth just before it.
    entry_cost: float
    exit_cost: float


def b1_path(
    saved: Mapping[str, Sequence[object]],
    *,
    first: Month,
    last: Month,
    close: float,
    cost_multiplier: float,
) -> B1Path:
    """B1 over ``first..last`` from step 0's saved path (``months``, ``continuing``, ``rebalance_cost``).

    ``continuing`` is step 0's net path, so a month inside the window that carries a rebalance cost would bring step
    0's 2009-12 band with it; that refuses. Buy-and-hold carries none. An incomplete or non-finite window is
    comparator incompleteness (§"Decision rule": ``COMPARATOR_INVALID``)."""
    if first > last:
        raise ValueError(f"B1 window {first}..{last} is inverted")
    check_cost_multiplier(cost_multiplier)
    by_month: dict[Month, tuple[float, float]] = {}
    try:
        for m, r, c in zip(saved["months"], saved["continuing"], saved["rebalance_cost"], strict=True):
            year, month_number = str(m).split("-")
            by_month[(int(year), int(month_number))] = (float(str(r)), float(str(c)))
    except (KeyError, TypeError, ValueError) as exc:  # a missing or non-iterable column, a ragged row, a bad cell
        raise BookRefusal("COMPARATOR_INVALID", f"B1's saved path does not parse: {exc}") from exc
    window: list[Month] = []
    month = first
    while month <= last:
        window.append(month)
        month = next_month(month)
    missing = [m for m in window if m not in by_month or not math.isfinite(by_month[m][0])]
    if missing:
        raise BookRefusal("COMPARATOR_INVALID", f"B1 lacks a finite return for {len(missing)} months: {missing[:5]}")
    charged = [m for m in window if by_month[m][1] != 0.0]  # NaN included: it is not 0.0
    if charged:
        raise BookRefusal("COMPARATOR_INVALID", f"B1 carries step 0 rebalance costs in the window: {charged[:5]}")
    band, half = entry_band(close)
    charge = half * cost_multiplier
    returns = {m: by_month[m][0] for m in window}
    before_sale = (1.0 - charge) * math.prod(1.0 + r for r in returns.values())
    returns[first] = (1.0 - charge) * (1.0 + returns[first]) - 1.0
    returns[last] = (1.0 + returns[last]) * (1.0 - charge) - 1.0
    return B1Path(returns, band, charge, before_sale * charge)


__all__ = ["B1Path", "b1_path", "reference_decisions"]
