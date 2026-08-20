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
from app.services.outcome_resolver import ExitLevels, UnresolvedReason, exit_levels_are_orderable
from app.services.price_levels import LevelScan
from app.services.strategies.s4_volatility_compression_breakout import (
    ATR_PERIOD,
    ATR_STOP_MULTIPLE,
    ATR_TARGET_MULTIPLE,
    MAX_HOLD_BARS,
)

# ⚠ The private helpers are imported rather than re-implemented, and the strategy
# module is NOT edited to accept precomputed inputs. Editing it would rehash its
# source into ``StrategyIdentity`` and move every strategy version — the exact
# cost this module's docstring exists to avoid. Importing changes no source hash.
from app.services.strategies.s5_support_bounce import (
    ATR_PERIOD as S5_ATR_PERIOD,
)
from app.services.strategies.s5_support_bounce import (
    ATR_STOP_MULTIPLE as S5_ATR_STOP_MULTIPLE,
)
from app.services.strategies.s5_support_bounce import (
    ATR_TARGET_MULTIPLE as S5_ATR_TARGET_MULTIPLE,
)
from app.services.strategies.s5_support_bounce import (
    MAX_HOLD_BARS as S5_MAX_HOLD_BARS,
)
from app.services.strategies.s5_support_bounce import (
    _support_below,
    _volumes,
)
from app.services.strategies.s6_resistance_breakout import (
    ATR_PERIOD as S6_ATR_PERIOD,
)
from app.services.strategies.s6_resistance_breakout import (
    ATR_STOP_MULTIPLE as S6_ATR_STOP_MULTIPLE,
)
from app.services.strategies.s6_resistance_breakout import (
    ATR_TARGET_MULTIPLE as S6_ATR_TARGET_MULTIPLE,
)
from app.services.strategies.s6_resistance_breakout import (
    MAX_HOLD_BARS as S6_MAX_HOLD_BARS,
)
from app.services.strategies.s6_resistance_breakout import (
    _resistance_below,
)
from app.services.strategies.s6_resistance_breakout import (
    _volumes as _s6_volumes,
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
        if not exit_levels_are_orderable(entry_price=entry_price, take_profit=target, stop_loss=stop):
            levels.append("unorderable_exit_levels")
            continue
        levels.append(ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=MAX_HOLD_BARS))
    return tuple(levels)


def s5_exit_levels_batch(
    series: BarSeries,
    *,
    requests: Sequence[tuple[int, Decimal]],
    universe: Universe,
) -> tuple[ExitLevels | UnresolvedReason, ...]:
    """Build S-5 brackets for several fills from ONE ATR pass and ONE level scan.

    The scalar ``s5_exit_bracket`` derives both from the whole series and then
    reads a single bar out of each, so evaluating a series' fills one at a time
    rebuilt the ATR, the swing-pivot scan and the volume array once per signal.
    Measured on a 300-series s5 profile: ``atr_series`` 24,378 calls / 51.2s
    cumulative, ``_volumes`` 24,634 calls / 24.5s, ``swing_pivots`` 12.3s —
    together about 38% of the evaluation, all of it redundant.

    ⚠⚠ SHARING THE INDICATORS IS LEGAL; SHARING A VERDICT WOULD NOT BE. The ATR
    at bar ``i`` reads bars ``<= i`` and ``LevelScan`` shares only pivot
    DETECTION — whether bar ``i`` is a pivot depends on bars ``i-5..i+5`` and
    never on where the observer stands. ``scan.at(index)`` still applies the
    causal confirmation filter per request, and ``atr.values[index]`` still reads
    that request's own bar. Nothing here is keyed by anything but the exact bar
    index, which is the property ``_support_below``'s docstring requires.

    ⚠ Each request keeps its scalar semantics, including the failure modes:
    ``ValueError`` is the bracket's own refusal (no ATR, no live level) and
    ``IndexError`` is an out-of-range index, and both map to
    ``unorderable_exit_levels`` exactly as ``_s5_exit_levels`` does. Output is
    positional so duplicate requests stay distinct.
    """
    atr = atr_series(series, period=S5_ATR_PERIOD, universe=universe)
    scan = LevelScan.build(highs=series.array_highs, lows=series.array_lows, volumes=_volumes(series))
    levels: list[ExitLevels | UnresolvedReason] = []
    for signal_index, entry_price in requests:
        try:
            atr_at_signal = atr.values[signal_index]
            if atr_at_signal is None:
                raise ValueError(f"S-5 bracket needs ATR at the signal bar; index {signal_index} is unevaluable")
            level = _support_below(series, index=signal_index, atr=atr_at_signal, scan=scan)
            if level is None:
                raise ValueError(f"S-5 bracket needs the support level at index {signal_index}; none is live")
            stop = Decimal(str(level - S5_ATR_STOP_MULTIPLE * atr_at_signal))
            target = entry_price + Decimal(str(S5_ATR_TARGET_MULTIPLE * atr_at_signal))
        except ValueError, IndexError:
            levels.append("unorderable_exit_levels")
            continue
        if not exit_levels_are_orderable(entry_price=entry_price, take_profit=target, stop_loss=stop):
            levels.append("unorderable_exit_levels")
            continue
        levels.append(ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=S5_MAX_HOLD_BARS))
    return tuple(levels)


def s6_exit_levels_batch(
    series: BarSeries,
    *,
    requests: Sequence[tuple[int, Decimal]],
    universe: Universe,
) -> tuple[ExitLevels | UnresolvedReason, ...]:
    """S-6's brackets from one ATR pass and one level scan — S-5's shape mirrored.

    ⚠ S-6 has its OWN ``_volumes`` and its own multiples, so nothing is shared
    with S-5 beyond the shape. The stop anchors to the RESISTANCE level and the
    target to the entry, and ``ATR_TARGET_MULTIPLE`` is 3.0 here against S-5's
    2.0 — reading either from the wrong module would be a silent strategy change
    that no type checker could see, which is why every constant is aliased with
    its strategy prefix.

    Same legality argument as ``s5_exit_levels_batch``: the ATR at bar ``i``
    reads bars ``<= i``, ``LevelScan`` shares only pivot detection, and
    ``scan.at(index)`` still applies the causal confirmation filter per request.
    """
    atr = atr_series(series, period=S6_ATR_PERIOD, universe=universe)
    scan = LevelScan.build(highs=series.array_highs, lows=series.array_lows, volumes=_s6_volumes(series))
    levels: list[ExitLevels | UnresolvedReason] = []
    for signal_index, entry_price in requests:
        try:
            atr_at_signal = atr.values[signal_index]
            if atr_at_signal is None:
                raise ValueError(f"S-6 bracket needs ATR at the signal bar; index {signal_index} is unevaluable")
            level = _resistance_below(series, index=signal_index, atr=atr_at_signal, scan=scan)
            if level is None:
                raise ValueError(f"S-6 bracket needs the resistance level at index {signal_index}; none is live")
            stop = Decimal(str(level - S6_ATR_STOP_MULTIPLE * atr_at_signal))
            target = entry_price + Decimal(str(S6_ATR_TARGET_MULTIPLE * atr_at_signal))
        except ValueError, IndexError:
            levels.append("unorderable_exit_levels")
            continue
        if not exit_levels_are_orderable(entry_price=entry_price, take_profit=target, stop_loss=stop):
            levels.append("unorderable_exit_levels")
            continue
        levels.append(ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=S6_MAX_HOLD_BARS))
    return tuple(levels)


__all__ = ["s4_exit_levels_batch", "s5_exit_levels_batch", "s6_exit_levels_batch"]
