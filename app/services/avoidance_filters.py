"""#3621 slice 1: the avoidance-filter flags (MAX, sub-$5, young) for one formation.

Spec: ``docs/research/2026-10-08-3621-avoidance-filters.md`` §"Source rules" (PR #3724). Pure; the run supplies each
admitted name's daily bars, raw close at s(M) and first admitted bar.

``rmax1_21d`` follows JKP (``bkelly-lab/ReplicationCrisis`` at ``67174c7f``: ``GlobalFactors/main.sas`` calls
``roll_apply_daily`` with ``__n=1, __min=15`` for the ``_21d`` set; ``GlobalFactors/market_chars.sas`` takes
``max(ret)`` and drops stock-months with ``zero_obs >= 10``): the largest daily total return over the SPY sessions of
s(M)'s calendar month up to s(M), at least 15 returns, fewer than 10 of them zero. Stated substitutions:

- a return exists only between usable bars on adjacent SPY sessions (total-return availability, not JKP's
  excess-return test), and zeros are counted among those returns;
- the plain maximum, where JKP keeps ``ret_rank <= 5`` first: the two differ only when the highest return is tied
  across ten or more days, which JKP drops and this keeps (pinned by a fixture).

Both of ``factor_panel_prices.series_prices``' screens apply. A return below ``SCREEN_RETURN_LOW`` or above
``SCREEN_RETURN_HIGH`` (adjacent sessions only), or an ``adj_close / close`` move with
``|ln(ratio1 / ratio0)| > ln(1 + SCREEN_RATIO_MOVE)`` between consecutive usable session bars with no stamp dated in
between (across missing sessions too), screens the window when the pair's later bar is inside it.

Missing values, in precedence order: screened (the name is flagged: "MAX plus screened-data exclusion"), fewer than
15 returns (not flagged), 10 or more zero returns (not flagged).

The cutoff q is the ``ceil(0.9 N)``-th smallest of the N valid values over all admitted names at M (BCW sort the
whole sample); a value >= q is flagged, so ties at q all flag.

``scripts/measure_3621_filter_premise.py`` is an independent implementation of the same rule; the spec requires
this module to reproduce its stage-A counts, after the declaration and access row (§"Samples and design history").
"""

from __future__ import annotations

import math
from bisect import bisect_left
from collections.abc import Hashable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from itertools import pairwise
from typing import Final

from app.services.factor_book_path import PRICE_FLOOR, archive_seasoned
from app.services.factor_panel_prices import SCREEN_RATIO_MOVE, SCREEN_RETURN_HIGH, SCREEN_RETURN_LOW, DailyBar

#: §"Source rules", MAX threshold: the top decile, ``ceil(0.9 N)``-th smallest (1-based).
MAX_DECILE: Final = 0.9
#: JKP ``main.sas`` ``__min=15`` and ``market_chars.sas`` ``zero_obs < 10``.
MAX_MIN_RETURNS: Final = 15
MAX_ZERO_RETURNS: Final = 10
JKP_CODE_COMMIT: Final = "67174c7f"


class AvoidanceError(RuntimeError):
    pass


class MaxMissing(StrEnum):
    SCREENED = "screened"
    SHORT = "short"
    ZERO_HEAVY = "zero_heavy"


class Filter(StrEnum):
    MAX = "max"
    SUB5 = "sub5"
    YOUNG = "young"


#: §"The books": the five filter sets, in the spec's order. No other combination is eligible.
FILTER_SETS: Final[tuple[frozenset[Filter], ...]] = (
    frozenset({Filter.MAX}),
    frozenset({Filter.SUB5}),
    frozenset({Filter.YOUNG}),
    frozenset({Filter.SUB5, Filter.YOUNG}),
    frozenset({Filter.MAX, Filter.SUB5, Filter.YOUNG}),
)


@dataclass(frozen=True, slots=True)
class MaxReading:
    #: ``None`` exactly when ``missing`` is set.
    value: float | None
    returns: int
    zeros: int
    missing: MaxMissing | None


class MaxSeries:
    """One series' usable session bars and stamp positions, read at any decision session index."""

    def __init__(self, bars: Sequence[DailyBar], sessions: Sequence[date]) -> None:
        # The panel's contracts (``SessionGrid``, ``DailyBar.usable``), checked here because the bisects and the
        # ratio screen's log silently misread input that breaks them.
        if any(a >= b for a, b in pairwise(sessions)):
            raise AvoidanceError("sessions are not strictly ascending")
        self._sessions = sessions
        position = {day: i for i, day in enumerate(sessions)}
        # A stamp on any bar, session or not, usable or not, counts at the first session on or after it.
        self._stamps = sorted(bisect_left(sessions, bar.bar_date) for bar in bars if bar.stamped)
        admitted = [(position[bar.bar_date], bar) for bar in bars if bar.usable and bar.bar_date in position]
        for _, bar in admitted:
            if not (0.0 < bar.close < math.inf and 0.0 < bar.adj_close < math.inf):
                raise AvoidanceError(f"usable bar {bar.bar_date} has a non-positive or non-finite price")
        self._index = [i for i, _ in admitted]
        if any(a >= b for a, b in pairwise(self._index)):
            raise AvoidanceError("bars are not strictly ascending by date")
        self._close = [bar.close for _, bar in admitted]
        self._adj = [bar.adj_close for _, bar in admitted]

    def _stamped_between(self, i0: int, i1: int) -> bool:
        """A stamp at a session index in (i0, i1]."""
        j = bisect_left(self._stamps, i0 + 1)
        return j < len(self._stamps) and self._stamps[j] <= i1

    def at(self, k: int) -> MaxReading:
        """``rmax1_21d`` at s(M) = ``sessions[k]``."""
        sessions = self._sessions
        month = (sessions[k].year, sessions[k].month)
        start = k
        while start > 0 and (sessions[start - 1].year, sessions[start - 1].month) == month:
            start -= 1
        first = bisect_left(self._index, start)
        last = bisect_left(self._index, k + 1)
        returns: list[float] = []
        zeros = 0
        screened = False
        for j in range(max(first, 1), last):
            i0, i1 = self._index[j - 1], self._index[j]
            moved = abs(math.log((self._adj[j] / self._close[j]) / (self._adj[j - 1] / self._close[j - 1])))
            if moved > math.log(1.0 + SCREEN_RATIO_MOVE) and not self._stamped_between(i0, i1):
                screened = True
            if i1 != i0 + 1:
                continue
            r = self._adj[j] / self._adj[j - 1] - 1.0
            if r < SCREEN_RETURN_LOW or r > SCREEN_RETURN_HIGH:
                screened = True
            returns.append(r)
            if r == 0.0:
                zeros += 1
        if screened:
            missing: MaxMissing | None = MaxMissing.SCREENED
        elif len(returns) < MAX_MIN_RETURNS:
            missing = MaxMissing.SHORT
        elif zeros >= MAX_ZERO_RETURNS:
            missing = MaxMissing.ZERO_HEAVY
        else:
            missing = None
        return MaxReading(max(returns) if missing is None else None, len(returns), zeros, missing)


def max_cutoff(values: Sequence[float]) -> float:
    """The ``ceil(0.9 N)``-th smallest of the N valid values; ``MAX_EMPTY`` refuses the run."""
    if not values:
        raise AvoidanceError("MAX_EMPTY: no admitted name has an rmax1_21d value at this formation")
    return sorted(values)[math.ceil(MAX_DECILE * len(values)) - 1]


@dataclass(frozen=True, slots=True)
class NameInputs:
    reading: MaxReading
    #: Raw close at s(M), never split-adjusted.
    close: float
    first_bar: date


@dataclass(frozen=True, slots=True)
class NameFlags:
    reading: MaxReading
    flagged: frozenset[Filter]

    def removed_by(self, filter_set: frozenset[Filter]) -> bool:
        return not self.flagged.isdisjoint(filter_set)


def flag_formation[K: Hashable](names: Mapping[K, NameInputs], session: date) -> tuple[float, dict[K, NameFlags]]:
    """Every admitted name's flags at s(M) = ``session``, and the MAX cutoff over all of them."""
    cutoff = max_cutoff([n.reading.value for n in names.values() if n.reading.value is not None])
    out: dict[K, NameFlags] = {}
    for key, name in names.items():
        value = name.reading.value
        hits = {
            Filter.MAX: name.reading.missing is MaxMissing.SCREENED or (value is not None and value >= cutoff),
            Filter.SUB5: name.close < PRICE_FLOOR,
            Filter.YOUNG: not archive_seasoned(name.first_bar, session),
        }
        out[key] = NameFlags(name.reading, frozenset(f for f, hit in hits.items() if hit))
    return cutoff, out


__all__ = [
    "FILTER_SETS",
    "JKP_CODE_COMMIT",
    "MAX_DECILE",
    "MAX_MIN_RETURNS",
    "MAX_ZERO_RETURNS",
    "AvoidanceError",
    "Filter",
    "MaxMissing",
    "MaxReading",
    "MaxSeries",
    "NameFlags",
    "NameInputs",
    "flag_formation",
    "max_cutoff",
]
