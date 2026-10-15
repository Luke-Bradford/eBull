"""Momentum book (#3822): risk-adjusted 12-1 momentum, top 30 US large caps, rebalanced monthly. Pure core.

Evidence and rule choices are on #3822 and in ``docs/research/2026-10-15-3822-momentum-evidence.md``: on the
survivorship-free Intrader archive (2014-10..2024-09) this book returned 16.97%/yr against SPY's 13.19% and beat all
20 random 30-name baskets from the same universe; it is regime-dependent (it lagged SPY 2014-2019).

The rule, fixed before the build:

* **Universe** at formation: tradable US equities with a close of at least ``MIN_CLOSE`` in the formation month and
  the full formation history below, then the ``UNIVERSE_SIZE`` largest by average daily dollar volume over the last
  ``ADV_SESSIONS`` sessions (the backtest's order).
* **Signal**: the 11 monthly price returns of months t-11..t-1 (t = the last completed month, which is skipped),
  compounded, divided by their sample standard deviation. A name needs all 12 month-end closes t-12..t-1 and a close
  in month t.
* **Book**: the ``BOOK_SIZE`` best scores, equal weight. Names that leave the book are sold at the rebalance; no
  holding band (a 1.5N/2N band lowered the backtest return, #3822).

eToro daily closes are split-adjusted and exclude dividends, so the live signal is price momentum; the backtest used
total return. Nothing here reads the database or the clock.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final

STRATEGY_ID: Final = "momentum-30"
STRATEGY_VERSION: Final = "v1"
BOOK_SIZE: Final = 30
UNIVERSE_SIZE: Final = 500
MIN_CLOSE: Final = 5.0
ADV_SESSIONS: Final = 63
FORMATION_MONTHS: Final = 11  # returns of months t-11..t-1


def month_key(day: date) -> int:
    return day.year * 12 + day.month - 1


@dataclass(frozen=True)
class Candidate:
    instrument_id: int
    #: Average daily dollar volume over the last ``ADV_SESSIONS`` sessions.
    adv: float
    #: Month key -> last close of that month (completed months only, plus the formation month t).
    month_closes: Mapping[int, float]


def formation_returns(month_closes: Mapping[int, float], t: int) -> list[float] | None:
    """The 11 monthly returns of months t-11..t-1, or ``None`` when any of closes t-12..t-1 is missing or not
    positive."""
    closes = [month_closes.get(m) for m in range(t - FORMATION_MONTHS - 1, t)]
    if any(c is None or not math.isfinite(c) or c <= 0 for c in closes):
        return None
    known = [c for c in closes if c is not None]
    return [b / a - 1.0 for a, b in zip(known, known[1:])]


def score(returns: Sequence[float]) -> float | None:
    """Compounded return divided by the sample standard deviation; ``None`` when the deviation is zero."""
    if len(returns) != FORMATION_MONTHS:
        return None
    mean = sum(returns) / len(returns)
    sd = math.sqrt(sum((r - mean) ** 2 for r in returns) / (len(returns) - 1))
    if not sd > 0:
        return None
    return (math.prod(1.0 + r for r in returns) - 1.0) / sd


def universe(candidates: Iterable[Candidate], t: int) -> dict[int, float]:
    """The ``UNIVERSE_SIZE`` names the book is ranked in at the end of month ``t``: instrument id -> score.

    It is also the random control's opportunity set (``momentum_book_scorecard``)."""
    # History first, then the dollar-volume cut: the order the evidence was measured in (#3822, Codex ckpt-2).
    eligible: list[tuple[float, int, float]] = []
    for c in candidates:
        if (c.month_closes.get(t) or 0.0) < MIN_CLOSE or not math.isfinite(c.adv):
            continue
        returns = formation_returns(c.month_closes, t)
        s = None if returns is None else score(returns)
        if s is not None and math.isfinite(s):
            eligible.append((c.adv, c.instrument_id, s))
    return {iid: s for _, iid, s in sorted(eligible, key=lambda x: (-x[0], x[1]))[:UNIVERSE_SIZE]}


def target_book(candidates: Iterable[Candidate], t: int) -> tuple[int, ...]:
    """The book formed at the end of month ``t``: ``BOOK_SIZE`` instrument ids, best score first.

    Ties break on the lower instrument id, so the result is a function of the inputs alone."""
    scored = sorted(((s, iid) for iid, s in universe(candidates, t).items()), key=lambda x: (-x[0], x[1]))
    return tuple(iid for _, iid in scored[:BOOK_SIZE])


@dataclass(frozen=True)
class RebalancePlan:
    exits: tuple[int, ...]
    entries: tuple[int, ...]
    keeps: tuple[int, ...]


def rebalance_plan(target: Sequence[int], held: Iterable[int]) -> RebalancePlan:
    """Sell what left the book, buy what joined it, keep the rest. Entries follow the target's order."""
    held_set = set(held)
    target_set = set(target)
    return RebalancePlan(
        exits=tuple(sorted(held_set - target_set)),
        entries=tuple(i for i in target if i not in held_set),
        keeps=tuple(i for i in target if i in held_set),
    )


__all__ = [
    "ADV_SESSIONS",
    "BOOK_SIZE",
    "FORMATION_MONTHS",
    "MIN_CLOSE",
    "STRATEGY_ID",
    "STRATEGY_VERSION",
    "UNIVERSE_SIZE",
    "Candidate",
    "RebalancePlan",
    "formation_returns",
    "month_key",
    "rebalance_plan",
    "score",
    "target_book",
    "universe",
]
