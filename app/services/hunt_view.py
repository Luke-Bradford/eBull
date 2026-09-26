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
    """One series' bars on the ratio basis, keyed by strictly increasing session ordinals."""

    ordinals: tuple[int, ...]
    open: tuple[float, ...]
    high: tuple[float, ...]
    low: tuple[float, ...]
    close: tuple[float, ...]
    volume: tuple[float, ...]

    def __post_init__(self) -> None:
        count = len(self.ordinals)
        if any(len(column) != count for column in (self.open, self.high, self.low, self.close, self.volume)):
            raise ValueError("every bar column must have one value per ordinal")
        if any(later <= earlier for earlier, later in zip(self.ordinals, self.ordinals[1:], strict=False)):
            raise ValueError("ordinals must be strictly increasing")


class Prefix[T: (int, float)](Sequence[T]):
    """The first ``end`` values of ``values``, each multiplied by ``factor`` on read.

    Read-only and lazy: indexing past ``end`` raises ``IndexError``, negative indices count
    back from ``end``, and a slice materialises only the values it covers.
    """

    __slots__ = ("_end", "_factor", "_values")

    def __init__(self, values: tuple[T, ...], end: int, factor: T) -> None:
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
class SeriesView:
    """One eligible series as a signal sees it: bars ≤ t, rebased, lazily read."""

    ordinals: Prefix[int]
    open: Prefix[float]
    high: Prefix[float]
    low: Prefix[float]
    close: Prefix[float]
    volume: Prefix[float]


@dataclass(frozen=True)
class SignalView:
    """Eligible series on t, each with bars ≤ t only and its last bar on t."""

    t: int
    series: Mapping[int, SeriesView]

    def __post_init__(self) -> None:
        for series_id, bars in self.series.items():
            if not bars.ordinals or bars.ordinals[-1] != self.t:
                raise ValueError(f"series {series_id}: a view's series must end on t = {self.t}")


def rebased_view(t: int, ratio_bars: Mapping[int, Bars], as_traded_close_on_t: Mapping[int, float]) -> SignalView:
    """The view on t for every series in ``ratio_bars`` (the caller passes the eligible set).

    Each series is cut to ordinals ≤ t, must have its bar on t, and is rebased so its close
    on t equals ``as_traded_close_on_t[series]``.
    """
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
        series[series_id] = SeriesView(
            ordinals=Prefix(bars.ordinals, end, 1),
            open=Prefix(bars.open, end, factor),
            high=Prefix(bars.high, end, factor),
            low=Prefix(bars.low, end, factor),
            close=Prefix(bars.close, end, factor),
            volume=Prefix(bars.volume, end, 1.0 / factor),
        )
    return SignalView(t=t, series=MappingProxyType(series))


__all__ = ["Bars", "Prefix", "SeriesView", "SignalView", "rebased_view"]
