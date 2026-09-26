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
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType


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


@dataclass(frozen=True)
class SignalView:
    """Eligible series on t, each with bars ≤ t only and its last bar on t."""

    t: int
    series: Mapping[int, Bars]

    def __post_init__(self) -> None:
        for series_id, bars in self.series.items():
            if not bars.ordinals or bars.ordinals[-1] != self.t:
                raise ValueError(f"series {series_id}: a view's series must end on t = {self.t}")


def _cut(bars: Bars, t: int) -> Bars:
    keep = sum(1 for ordinal in bars.ordinals if ordinal <= t)
    return Bars(
        ordinals=bars.ordinals[:keep],
        open=bars.open[:keep],
        high=bars.high[:keep],
        low=bars.low[:keep],
        close=bars.close[:keep],
        volume=bars.volume[:keep],
    )


def _scaled(values: Sequence[float], factor: float) -> tuple[float, ...]:
    return tuple(value * factor for value in values)


def rebased_view(t: int, ratio_bars: Mapping[int, Bars], as_traded_close_on_t: Mapping[int, float]) -> SignalView:
    """The view on t for every series in ``ratio_bars`` (the caller passes the eligible set).

    Each series is cut to ordinals ≤ t, must have its bar on t, and is rebased so its close
    on t equals ``as_traded_close_on_t[series]``.
    """
    series: dict[int, Bars] = {}
    for series_id, bars in ratio_bars.items():
        cut = _cut(bars, t)
        if not cut.ordinals or cut.ordinals[-1] != t:
            raise ValueError(f"series {series_id} has no bar on t = {t}; it is not eligible")
        ratio_close, traded_close = cut.close[-1], as_traded_close_on_t.get(series_id)
        if traded_close is None or not (math.isfinite(traded_close) and traded_close > 0.0):
            raise ValueError(f"series {series_id}: no valid as-traded close on t = {t}")
        if not (math.isfinite(ratio_close) and ratio_close > 0.0):
            raise ValueError(f"series {series_id}: no valid ratio-basis close on t = {t}")
        factor = traded_close / ratio_close
        series[series_id] = Bars(
            ordinals=cut.ordinals,
            open=_scaled(cut.open, factor),
            high=_scaled(cut.high, factor),
            low=_scaled(cut.low, factor),
            close=_scaled(cut.close, factor),
            volume=_scaled(cut.volume, 1.0 / factor),
        )
    return SignalView(t=t, series=MappingProxyType(series))


__all__ = ["Bars", "SignalView", "rebased_view"]
