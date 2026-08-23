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

from app.services.indicator_series import BarSeries, Universe, atr_series, bollinger_series
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
from app.services.strategies.s7_trend_pullback import (
    ATR_PERIOD as S7_ATR_PERIOD,
)
from app.services.strategies.s7_trend_pullback import (
    ATR_STOP_MULTIPLE as S7_ATR_STOP_MULTIPLE,
)
from app.services.strategies.s7_trend_pullback import (
    MAX_HOLD_BARS as S7_MAX_HOLD_BARS,
)
from app.services.strategies.s8_range_mean_reversion import (
    ATR_PERIOD as S8_ATR_PERIOD,
)
from app.services.strategies.s8_range_mean_reversion import (
    ATR_STOP_MULTIPLE as S8_ATR_STOP_MULTIPLE,
)
from app.services.strategies.s8_range_mean_reversion import (
    ENTRY_BAND_NUM_STD as S8_ENTRY_BAND_NUM_STD,
)
from app.services.strategies.s8_range_mean_reversion import (
    ENTRY_BAND_PERIOD as S8_ENTRY_BAND_PERIOD,
)
from app.services.strategies.s8_range_mean_reversion import (
    MAX_HOLD_BARS as S8_MAX_HOLD_BARS,
)
from app.services.strategies.s9_squeeze_expansion import (
    ATR_PERIOD as S9_ATR_PERIOD,
)
from app.services.strategies.s9_squeeze_expansion import (
    ATR_STOP_MULTIPLE as S9_ATR_STOP_MULTIPLE,
)
from app.services.strategies.s9_squeeze_expansion import (
    ATR_TARGET_MULTIPLE as S9_ATR_TARGET_MULTIPLE,
)
from app.services.strategies.s9_squeeze_expansion import (
    MAX_HOLD_BARS as S9_MAX_HOLD_BARS,
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

    ⚠ Each request keeps its scalar semantics, including the failure modes, and
    every one of them lands as ``unorderable_exit_levels`` rather than
    propagating (#2781). This function used to RAISE for a bad index, a
    non-finite or non-positive entry price, and a missing or non-positive ATR,
    while its five siblings refused all three. An uncaught exception in an
    exit-level factory aborts the WHOLE outcome batch for one bad bar instead of
    recording one unresolved outcome — the failure mode
    ``tests/test_2437_exit_level_refusal_surface.py`` exists to prevent, and
    from which S-4 was excluded.

    ⚠ Latent, not live, and that is why it survived: ``_exit_levels_for_entries``
    only asks for indices where an entry fired, and the registry refuses to fire
    on an unevaluable bar, so a fired signal has a valid ATR in practice. One
    masked bar from being live — the quarantine ``masked`` arm exists precisely
    to remove bars.

    ⚠⚠ NO ``strategy_version`` MOVES, and that is load-bearing rather than
    convenient. ``_source_hash()`` hashes the DEFINING strategy module, and this
    is not one; nor is it in ``INPUT_RULE_SETS``. A behaviour change that does
    not move the version means rows from old and new code share a ledger key, so
    it is only admissible because the delta is confined to inputs the registry
    cannot currently produce. If that ever stops being true, this needs a
    version bump, not a wider ``except``. ``_s4_exit_levels``'s docstring
    already states the intent this completes: *"The batch adapter owns the typed
    refusal because changing the hashed S-4 module merely to add an exception
    class would mint a new strategy version."*
    """
    atr = atr_series(series, period=ATR_PERIOD, universe=universe)
    levels: list[ExitLevels | UnresolvedReason] = []
    for signal_index, entry_price in requests:
        try:
            if not 0 <= signal_index < len(series):
                raise ValueError(f"signal_index {signal_index} is outside the {len(series)}-bar series")
            if not entry_price.is_finite():
                raise ValueError(f"entry_price must be finite, got {entry_price}")
            if entry_price <= 0:
                raise ValueError(f"entry_price must be positive, got {entry_price}")
            value = atr.values[signal_index]
            if value is None or value <= 0:
                raise ValueError(f"ATR{ATR_PERIOD} is unavailable or non-positive at signal index {signal_index}")
        except ValueError, IndexError:
            levels.append("unorderable_exit_levels")
            continue
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


def s7_exit_levels_batch(
    series: BarSeries,
    *,
    requests: Sequence[tuple[int, Decimal]],
    universe: Universe,
) -> tuple[ExitLevels | UnresolvedReason, ...]:
    """S-7's STOP-ONLY brackets from one ATR pass.

    ⚠⚠ ``take_profit`` IS ``None`` BY DESIGN and must stay so — the stop-only
    bracket #2723 introduced. Substituting a target here would make
    ``outcome_resolver``'s precedence rules 2/3/5 reachable and change which bar
    closes the position. There is no level scan: S-7's stop is ENTRY-anchored,
    because no level's failure defines the trade being wrong.
    """
    atr = atr_series(series, period=S7_ATR_PERIOD, universe=universe)
    levels: list[ExitLevels | UnresolvedReason] = []
    for signal_index, entry_price in requests:
        try:
            atr_at_signal = atr.values[signal_index]
            if atr_at_signal is None:
                raise ValueError(f"S-7 bracket needs ATR at the signal bar; index {signal_index} is unevaluable")
            stop = entry_price - Decimal(str(S7_ATR_STOP_MULTIPLE * atr_at_signal))
        except ValueError, IndexError:
            levels.append("unorderable_exit_levels")
            continue
        if not exit_levels_are_orderable(entry_price=entry_price, take_profit=None, stop_loss=stop):
            levels.append("unorderable_exit_levels")
            continue
        levels.append(ExitLevels(take_profit=None, stop_loss=stop, max_hold_bars=S7_MAX_HOLD_BARS))
    return tuple(levels)


def s9_exit_levels_batch(
    series: BarSeries,
    *,
    requests: Sequence[tuple[int, Decimal]],
    universe: Universe,
) -> tuple[ExitLevels | UnresolvedReason, ...]:
    """S-9's brackets from one ATR pass. Both legs anchor to the ENTRY.

    ⚠ S-9 has no level — its setup is a volatility state, not a price — so there
    is nothing to scan and nothing to anchor a stop to but the entry, same as
    S-4. Only the ATR pass is shared.
    """
    atr = atr_series(series, period=S9_ATR_PERIOD, universe=universe)
    levels: list[ExitLevels | UnresolvedReason] = []
    for signal_index, entry_price in requests:
        try:
            atr_at_signal = atr.values[signal_index]
            if atr_at_signal is None:
                raise ValueError(f"S-9 bracket needs ATR at the signal bar; index {signal_index} is unevaluable")
            stop = entry_price - Decimal(str(S9_ATR_STOP_MULTIPLE * atr_at_signal))
            target = entry_price + Decimal(str(S9_ATR_TARGET_MULTIPLE * atr_at_signal))
        except ValueError, IndexError:
            levels.append("unorderable_exit_levels")
            continue
        if not exit_levels_are_orderable(entry_price=entry_price, take_profit=target, stop_loss=stop):
            levels.append("unorderable_exit_levels")
            continue
        levels.append(ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=S9_MAX_HOLD_BARS))
    return tuple(levels)


def s8_exit_levels_batch(
    series: BarSeries,
    *,
    requests: Sequence[tuple[int, Decimal]],
    universe: Universe,
) -> tuple[ExitLevels | UnresolvedReason, ...]:
    """S-8's brackets from one ATR pass and one Bollinger pass.

    ⚠ S-8 rebuilt TWO whole-series indicators per signal, not one — the bands as
    well as the ATR — so it had the worst per-signal cost of the bracket
    strategies despite being cheap per bar.

    ⚠⚠ THE TARGET IS THE MIDDLE BAND AT THE SIGNAL BAR AND NEVER MOVES. Reading
    it at any other index would make the target track the band, which is a
    trailing exit — a different rule, whose exit price depends on bars after the
    entry. Sharing the SERIES is safe precisely because each request still reads
    its own ``signal_index`` out of it.

    ⚠ An inverted bracket is reachable here and is not a bug: the target anchors
    to the signal bar's band while the stop anchors to the FILL, so a gap up
    leaves ``target <= stop``. That refuses as ``unorderable_exit_levels``, and
    the batch must reproduce the refusal rather than repair it.
    """
    atr = atr_series(series, period=S8_ATR_PERIOD, universe=universe)
    bands = bollinger_series(series, universe=universe, period=S8_ENTRY_BAND_PERIOD, num_std=S8_ENTRY_BAND_NUM_STD)
    middles = bands.components["middle"]
    levels: list[ExitLevels | UnresolvedReason] = []
    for signal_index, entry_price in requests:
        try:
            atr_at_signal = atr.values[signal_index]
            if atr_at_signal is None:
                raise ValueError(f"S-8 bracket needs ATR at the signal bar; index {signal_index} is unevaluable")
            middle = middles[signal_index]
            if middle is None:
                raise ValueError(
                    f"S-8 bracket needs the middle band at the signal bar; index {signal_index} is unevaluable"
                )
            stop = entry_price - Decimal(str(S8_ATR_STOP_MULTIPLE * atr_at_signal))
            target = Decimal(str(middle))
        except ValueError, IndexError:
            levels.append("unorderable_exit_levels")
            continue
        if not exit_levels_are_orderable(entry_price=entry_price, take_profit=target, stop_loss=stop):
            levels.append("unorderable_exit_levels")
            continue
        levels.append(ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=S8_MAX_HOLD_BARS))
    return tuple(levels)


__all__ = [
    "s4_exit_levels_batch",
    "s5_exit_levels_batch",
    "s6_exit_levels_batch",
    "s7_exit_levels_batch",
    "s8_exit_levels_batch",
    "s9_exit_levels_batch",
]
