"""Result-equivalent batch adapters for strategy-owned exit-level factories.

This module is deliberately outside ``app.services.strategies``. Strategy
modules hash their own source into ``StrategyIdentity``; a runner-only
optimisation must not mint a new strategy version. The scalar factory remains
the semantic owner and oracle, while this adapter shares only its immutable
indicator calculation across requests.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from math import isfinite

from app.services.indicator_series import BarSeries, Universe, atr_series
from app.services.outcome_resolver import ExitLevels, UnresolvedReason
from app.services.strategies.s4_volatility_compression_breakout import (
    ATR_PERIOD,
    ATR_STOP_MULTIPLE,
    ATR_TARGET_MULTIPLE,
    MAX_HOLD_BARS,
)


def s4_exit_levels_batch(
    series: BarSeries,
    *,
    requests: Sequence[tuple[int, Decimal]],
    universe: Universe,
) -> tuple[ExitLevels | UnresolvedReason, ...]:
    """Build S-4 brackets for several fills from one Wilder ATR pass.

    Each request keeps its scalar semantics: ATR is read at the causal signal
    index and the bracket is centred on that request's next-open entry price.
    Output is positional so duplicate requests remain distinct.
    """
    atr = atr_series(series, period=ATR_PERIOD, universe=universe)
    levels: list[ExitLevels | UnresolvedReason] = []
    for signal_index, entry_price in requests:
        if not 0 <= signal_index < len(series):
            raise ValueError(f"signal_index {signal_index} is outside the {len(series)}-bar series")
        if not entry_price.is_finite():
            raise ValueError(f"entry_price must be finite, got {entry_price}")
        if entry_price <= 0:
            raise ValueError(f"entry_price must be positive, got {entry_price}")
        value = atr.values[signal_index]
        if value is None or value <= 0:
            raise ValueError(f"ATR{ATR_PERIOD} is unavailable or non-positive at signal index {signal_index}")
        if not isfinite(value):
            levels.append("unorderable_exit_levels")
            continue
        distance = Decimal(str(value))
        stop = entry_price - Decimal(str(ATR_STOP_MULTIPLE)) * distance
        target = entry_price + Decimal(str(ATR_TARGET_MULTIPLE)) * distance
        if not target.is_finite() or not stop.is_finite() or stop <= 0 or stop >= entry_price or target <= entry_price:
            levels.append("unorderable_exit_levels")
            continue
        levels.append(ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=MAX_HOLD_BARS))
    return tuple(levels)


__all__ = ["s4_exit_levels_batch"]
