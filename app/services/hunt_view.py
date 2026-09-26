"""What a hunt signal sees: bars ≤ t, rebased to the as-traded close on t.

#3385 slice 3c, spec ``docs/proposals/ta/2026-09-26-3385-hunt-harness.md`` (v5), "Signals".
A signal is ``signal(t, view, constants) -> {series_id: score}`` and may import this module
(it is on ``tests/test_hunt_signal_imports.py``'s allowlist), so it stays pure: stdlib only.

⚠ THE CUT IS MADE HERE, NOT BY THE CALLER. :func:`rebased_view` takes each series' whole
loaded history and keeps only sessions ≤ t, so no wiring mistake upstream can hand a signal
a later bar. ⚠ The rebase uses the ratio-basis bars (split-continuous, re-denominated to
the loaded window's terminal unit) scaled by k = as-traded close(t) / ratio close(t): the
levels a signal sees on t are the as-traded ones, and a split AFTER t cannot move them.
Volume is divided by k so price × volume is unchanged.

⚠ Built once per formation session for thousands of series, so nothing is copied: each
column is a lazy PREFIX of the loaded history (O(log n) to build via ``bisect``), scaled on
read, and raises ``IndexError`` past t like any sequence (Codex ckpt-2: materialising every
prefix every session is quadratic in history length).

#3386 slice 2 (spec ``docs/proposals/ta/2026-09-26-3386-hunt-feature-store.md`` v3, "Part B"):
the view also carries the session DATES up to t and each series' EX-DATES ≤ t with their
cash amounts, rebased by the same k as the prices. Both are cut here too. A date is a plain
``(year, month, day, weekday)`` int tuple (Monday = 0), never a ``datetime.date``: a date
object hands a signal ``date.today()``, a clock, which the import allowlist exists to close. ⚠ An ex-date ≤ t
is treated as known on t: the archive has no declaration or revision timestamp (declared
point-in-time assumption); an ex-date after t is never visible. A NaN amount passes through,
and the harness filters only the final score, so a signal must treat a non-finite input.
"""

from __future__ import annotations

import math
from bisect import bisect_right
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import overload


@dataclass(frozen=True)
class Bars:
    """One series' bars on the ratio basis, keyed by strictly increasing session ordinals.

    Any read-only sequence: the harness passes ``array.array`` columns, because a
    discovery panel is ~16M bars and a tuple of floats costs 32 bytes a value.
    """

    ordinals: Sequence[int]
    open: Sequence[float]
    high: Sequence[float]
    low: Sequence[float]
    close: Sequence[float]
    volume: Sequence[float]

    def __post_init__(self) -> None:
        count = len(self.ordinals)
        if any(len(column) != count for column in (self.open, self.high, self.low, self.close, self.volume)):
            raise ValueError("every bar column must have one value per ordinal")
        ordinals = self.ordinals
        if any(ordinals[i + 1] <= ordinals[i] for i in range(count - 1)):
            raise ValueError("ordinals must be strictly increasing")


class Prefix[T: (int, float)](Sequence[T]):
    """The first ``end`` values of ``values``, each multiplied by ``factor`` on read.

    Read-only and lazy: indexing past ``end`` raises ``IndexError``, negative indices count
    back from ``end``, and a slice materialises only the values it covers.
    """

    __slots__ = ("_end", "_factor", "_values")

    def __init__(self, values: Sequence[T], end: int, factor: T) -> None:
        if not 0 <= end <= len(values):
            raise ValueError(f"prefix end {end} outside [0, {len(values)}]")
        self._values = values
        self._end = end
        self._factor = factor

    def __len__(self) -> int:
        return self._end

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[T, ...]: ...

    def __getitem__(self, index: int | slice) -> T | tuple[T, ...]:
        if isinstance(index, slice):
            return tuple(self._values[i] * self._factor for i in range(*index.indices(self._end)))
        position = index + self._end if index < 0 else index
        if not 0 <= position < self._end:
            raise IndexError(f"index {index} is outside the {self._end} bars up to t")
        return self._values[position] * self._factor


@dataclass(frozen=True)
class Dividends:
    """One series' ex-dates (session ordinals, strictly increasing) and cash amounts on the ratio basis."""

    ordinals: Sequence[int]
    amounts: Sequence[float]

    def __post_init__(self) -> None:
        if len(self.ordinals) != len(self.amounts):
            raise ValueError("every ex-date needs one amount")
        ordinals = self.ordinals
        if any(ordinals[i + 1] <= ordinals[i] for i in range(len(ordinals) - 1)):
            raise ValueError("ex-date ordinals must be strictly increasing")


NO_DIVIDENDS = Dividends((), ())

#: A session's calendar date as (year, month, day, weekday), Monday = 0.
SessionDate = tuple[int, int, int, int]


class Head[T](Sequence[T]):
    """The first ``end`` items of ``values``, unscaled: read-only, lazy, ``IndexError`` past ``end``."""

    __slots__ = ("_end", "_values")

    def __init__(self, values: Sequence[T], end: int) -> None:
        if not 0 <= end <= len(values):
            raise ValueError(f"head end {end} outside [0, {len(values)}]")
        self._values = values
        self._end = end

    def __len__(self) -> int:
        return self._end

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[T, ...]: ...

    def __getitem__(self, index: int | slice) -> T | tuple[T, ...]:
        if isinstance(index, slice):
            return tuple(self._values[i] for i in range(*index.indices(self._end)))
        position = index + self._end if index < 0 else index
        if not 0 <= position < self._end:
            raise IndexError(f"index {index} is outside the {self._end} items up to t")
        return self._values[position]


@dataclass(frozen=True)
class SeriesView:
    """One eligible series as a signal sees it: bars ≤ t, rebased, lazily read.

    ``dividend_ordinals`` / ``dividend_amounts``: its ex-dates ≤ t (a session index, which may
    be a session without a bar) and their cash, rebased by the prices' k.
    """

    ordinals: Prefix[int]
    open: Prefix[float]
    high: Prefix[float]
    low: Prefix[float]
    close: Prefix[float]
    volume: Prefix[float]
    dividend_ordinals: Prefix[int]
    dividend_amounts: Prefix[float]


@dataclass(frozen=True)
class SignalView:
    """Eligible series on t, each with bars ≤ t only and its last bar on t.

    ``dates[i]`` is session ordinal i's :data:`SessionDate`, for i ≤ t only.
    """

    t: int
    series: Mapping[int, SeriesView]
    dates: Head[SessionDate]

    def __post_init__(self) -> None:
        if len(self.dates) != self.t + 1:
            raise ValueError(f"a view on t = {self.t} carries the dates of sessions 0 … t, not {len(self.dates)}")
        for series_id, bars in self.series.items():
            if not bars.ordinals or bars.ordinals[-1] != self.t:
                raise ValueError(f"series {series_id}: a view's series must end on t = {self.t}")


def rebased_view(
    t: int,
    ratio_bars: Mapping[int, Bars],
    as_traded_close_on_t: Mapping[int, float],
    *,
    sessions: Sequence[SessionDate],
    dividends: Mapping[int, Dividends],
) -> SignalView:
    """The view on t for every series in ``ratio_bars`` (the caller passes the eligible set).

    Each series is cut to ordinals ≤ t, must have its bar on t, and is rebased so its close
    on t equals ``as_traded_close_on_t[series]``. ``sessions`` (every loaded session) is cut
    to 0 … t; ``dividends`` (by series; absent = none) to ex-dates ≤ t, amounts scaled by k.
    """
    if not 0 <= t < len(sessions):
        raise ValueError(f"t = {t} is not a loaded session")
    series: dict[int, SeriesView] = {}
    for series_id, bars in ratio_bars.items():
        end = bisect_right(bars.ordinals, t)
        if end == 0 or bars.ordinals[end - 1] != t:
            raise ValueError(f"series {series_id} has no bar on t = {t}; it is not eligible")
        ratio_close, traded_close = bars.close[end - 1], as_traded_close_on_t.get(series_id)
        if traded_close is None or not (math.isfinite(traded_close) and traded_close > 0.0):
            raise ValueError(f"series {series_id}: no valid as-traded close on t = {t}")
        if not (math.isfinite(ratio_close) and ratio_close > 0.0):
            raise ValueError(f"series {series_id}: no valid ratio-basis close on t = {t}")
        factor = traded_close / ratio_close
        paid = dividends.get(series_id, NO_DIVIDENDS)
        paid_end = bisect_right(paid.ordinals, t)
        series[series_id] = SeriesView(
            ordinals=Prefix(bars.ordinals, end, 1),
            open=Prefix(bars.open, end, factor),
            high=Prefix(bars.high, end, factor),
            low=Prefix(bars.low, end, factor),
            close=Prefix(bars.close, end, factor),
            volume=Prefix(bars.volume, end, 1.0 / factor),
            dividend_ordinals=Prefix(paid.ordinals, paid_end, 1),
            dividend_amounts=Prefix(paid.amounts, paid_end, factor),
        )
    return SignalView(t=t, series=MappingProxyType(series), dates=Head(sessions, t + 1))


__all__ = [
    "NO_DIVIDENDS",
    "Bars",
    "Dividends",
    "Head",
    "Prefix",
    "SeriesView",
    "SessionDate",
    "SignalView",
    "rebased_view",
]
