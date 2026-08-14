"""S-6 — resistance breakout with volume confirmation. The catalogue's fifth strategy.

Spec: ``docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md`` §3 (S-6), §1 (the
regime filter), §2 (level construction), §0 (walk-forward on regime). Registry
contract: ``app/services/strategy_registry.py``. Refs #2437.

THE RULE, VERBATIM FROM §3
--------------------------
    **Setup:** live resistance level; regime ∈ {``bull_quiet``}. ⚠ Excluded from
    ``bull_volatile`` deliberately — breakouts into a Bulge are the classic
    false-break regime. **Signal:** ``close(t)`` > level **and** ``volume(t)`` ≥
    1.2 × 20-bar average volume. ⚠ The 1.2 multiple is retail convention, **not**
    a published result — frozen by construction, recorded as such. **Exit:** stop
    ``level − 1.0 × ATR(14)`` (back inside the range = failed break); target
    ``entry + 3.0 × ATR(14)``; max hold 40 bars. **Params:** 4. **Data:** OHLC +
    volume.

⚠⚠ THE STOP IS ANCHORED TO THE LEVEL AND THE TARGET TO THE ENTRY, AND MAKING
THEM CONSISTENT WOULD BE A SPEC VIOLATION. S-4's bracket is symmetric — both
legs measured from the fill — because S-4 has no level to measure from. S-6
does, and §3 uses it on purpose: *"back inside the range = failed break"* is a
statement about the LEVEL, not about the entry. The two are not the same price:
the fill is ``open(t+1)`` and the level is wherever the cluster sat, so a gap-up
open makes the stop distance materially wider than ``1 × ATR`` and a marginal
break makes it narrower. That is the rule doing its job — the trade is wrong when
price returns inside the range, however far the fill happened to be from it.

⚠⚠ THE LEVEL IS PART OF THE SIGNAL, SO IT IS RE-DERIVED AND NEVER CARRIED.
A ``StrategySignal`` carries a bar index and nothing else (registry docstring),
so ``s6_exit_bracket`` cannot be handed the level the signal broke. It recomputes
it from the same function the signal used, at the same index, from bars ``<= t``.
That is the S-4 pattern (*"recomputes ``atr_series`` and reads ``signal_index``
— never ``signal_index + 1``"*) applied to a richer object, and it is safe for
the same reason: ``LevelScan.at`` is deterministic and causal, so re-deriving it
cannot produce a level the signal did not see. ``_breakout_level`` is the single
definition both call.

⚠⚠ WHICH RESISTANCE, WHEN SEVERAL ARE LIVE — FIXED BY CONSTRUCTION.
§3 says *"live resistance level"* and *"close(t) > level"* without saying which
of several, and the choice is load-bearing because the stop is anchored to it.
There is no published rule to cite, so this is declared rather than inferred:

    level(t) := the LOWEST live resistance level strictly above ``close(t-1)``

Two properties make it the one to pick. It is determined **before** bar ``t``
opens — it is a function of ``close(t-1)`` and of levels known at ``t`` — so
"the level being broken" is not chosen with knowledge of where the close landed.
And it is unique: there is exactly one lowest such level, whereas "the level
closest to close(t)" is ambiguous when the close overshoots several and would let
a large up-day silently re-select a higher, further stop. Levels above it that
``close(t)`` also cleared are simply not this signal's level.

⚠ A CONSEQUENCE, STATED RATHER THAN LEFT TO BE FOUND: the strength ranking in
``PriceLevel`` is NOT used here. ``price_levels`` says strength *"ranks levels
against each other and is NEVER compared to a threshold"*, and selecting the
strongest live resistance would make it decide the trade — a fitted parameter
wearing a score's clothing. Price ordering has no such freedom.

⚠ THE VOLUME AVERAGE EXCLUDES ``t``, for S-4's reason in its own words:
including it makes the condition *"partly self-referential"*. A bar with enough
volume to break out raises the average it is compared against, so the test would
be weaker exactly where the rule means it to bite. The trailing-20 form is also
the conventional reading of "20-bar average volume". Both windows are pinned by
``TestVolumeWindowBoundary``.

⚠ THE REGIME IS A DIFFERENT INSTRUMENT'S DATA, AND IT IS A DECLARED INPUT.
S-1…S-4 read only their own bars. S-6 reads SPY's regime, so a bar of AAPL is
unjudgeable when SPY has no classifiable session that day — the instrument's own
data being perfect changes nothing. Declaring the gate among ``inputs`` is what
makes the registry refuse that bar *before* the condition runs, with the reason
``missing_market_context`` (its eleventh code) rather than a silent
``not_fired``. Warm-up on the benchmark stays ``insufficient_warmup``;
``MarketContext.gate_series`` is where the two are separated.

⚠ A REFUSED REGIME IS ``not_fired``, NOT ``not_evaluable``. ``bear_quiet`` is a
market this strategy has a stated opinion about — spec §0: *"a strategy that only
works in one regime is not broken; it is a strategy with a stated domain, and
firing it outside that domain is the defect"*. The bar was judged and declined.

⚠ A MASKED HIGH OR LOW SILENTLY REMOVES PIVOT CANDIDATES, and this module does
not pretend otherwise. ``swing_pivots`` skips any window containing a non-finite
value, so a quarantined bar can delete a level that would otherwise exist and the
verdict becomes ``not_fired`` on evidence that was hidden — the shape the
prevention log records against ``price_structure``'s three detectors. It is NOT
fixed by refusing the trailing 125 bars around every mask, which would be a
blast radius far larger than the defect: measured on the validated universe, the
binding rejection is the break itself (1,218,577 of 1,290,429 bars that had a
resistance above), not level availability. It is instead made VISIBLE — ATR is a
declared input and is Wilder-smoothed, so a masked bar already refuses the whole
tail of the ATR series, and ``scripts/verify_2437_s6_resistance_breakout.py
--census`` counts what each leg rejects.

⚠ WHAT THIS MODULE DOES NOT GUARD, inherited from ``indicator_series``:
quarantine and adjustment basis are the CALLER's gate. There is no database
access here.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path

import numpy as np

from app.services.indicator_series import (
    BarSeries,
    IndicatorSeries,
    Universe,
    atr_series,
)
from app.services.market_context import BENCHMARK_SYMBOL, MarketContext
from app.services.market_regime import Regime
from app.services.price_levels import PIVOT_HALF_WINDOW, LevelScan, PriceLevel
from app.services.strategy_registry import (
    NOT_EVALUABLE_REASONS,
    NotEvaluableReason,
    StrategyIdentity,
    StrategyInput,
    StrategySignal,
    evaluate,
)

S6_STRATEGY_ID = "s6-resistance-breakout-volume"

#: ⚠ FIXED, NEVER TUNED (parent §6: *"Forbidden — continuous re-optimisation"*).
#: Module constants rather than function arguments, for S-1's and S-4's reason: a
#: threshold that can be passed in is a threshold that can be swept, and
#: criterion 11 would then need every sweep value registered as its own strategy.
ATR_PERIOD = 14
VOLUME_LOOKBACK = 20
#: ⚠ Retail convention, NOT a published result. §3 says so in its own text and it
#: is repeated here because a constant with no provenance note reads as derived.
VOLUME_MULTIPLE = 1.2

#: The bracket. ⚠ ``LEVEL_STOP_MULTIPLE`` is measured from the LEVEL and
#: ``ATR_TARGET_MULTIPLE`` from the ENTRY — see the module docstring.
LEVEL_STOP_MULTIPLE = 1.0
ATR_TARGET_MULTIPLE = 3.0
MAX_HOLD_BARS = 40

#: §3's setup gate. ⚠ ``bull_volatile`` is EXCLUDED deliberately: *"breakouts
#: into a Bulge are the classic false-break regime"*. Widening this set is a new
#: strategy, not a tuning change — it is hashed into ``S6_PARAMS``.
PERMITTED_REGIMES: frozenset[Regime] = frozenset({Regime.BULL_QUIET})

#: ⚠ §3 says *"Params: 4"* and this dict carries EIGHT, recorded rather than
#: resolved by dropping four — S-3 and S-4 record the same discrepancy. §3 counts
#: FREE parameters (the numbers a sweep would move); criterion 11 hashes
#: everything that makes this a distinct strategy. ``atr_period`` is Wilder's own
#: default, but an S-6 computed on ``atr_20`` is a different strategy and must not
#: inherit this one's track record.
#:
#: ⚠⚠ ``benchmark_symbol`` and ``permitted_regimes`` are here for a stronger
#: reason: they are the only record of WHOSE market this strategy is gated on.
#: The regime rule's own version travels via ``INPUT_RULE_SETS``, but that names
#: the *classifier*, not the *series it was run over* — an S-6 gated on QQQ's
#: regime would produce different signals under an identical rule version and an
#: identical source hash. Nothing else in the identity would move.
S6_PARAMS: Mapping[str, object] = {
    "atr_period": ATR_PERIOD,
    "volume_lookback": VOLUME_LOOKBACK,
    "volume_multiple": VOLUME_MULTIPLE,
    "level_stop_multiple": LEVEL_STOP_MULTIPLE,
    "atr_target_multiple": ATR_TARGET_MULTIPLE,
    "max_hold_bars": MAX_HOLD_BARS,
    "permitted_regimes": sorted(regime.value for regime in PERMITTED_REGIMES),
    "benchmark_symbol": BENCHMARK_SYMBOL,
}

#: The first index at which every leg can be evaluated, DERIVED from the rule.
#:
#: ``atr_series`` emits its first value at ``ATR_PERIOD``. The volume ratio needs
#: ``VOLUME_LOOKBACK`` PRIOR bars and is ready at ``VOLUME_LOOKBACK``. A swing
#: pivot is not expressible at all before ``2 * PIVOT_HALF_WINDOW + 1`` bars
#: exist. 20 binds.
#:
#: ⚠ Above that floor, "no live resistance above the prior close" is a VERDICT
#: and not warm-up: the rule genuinely has no setup there. Only the fixed-window
#: legs report warm-up, and they report it by emitting ``None`` rather than by a
#: length check — an explicit ``len(series) < 20 -> refuse`` would be a second,
#: weaker copy of the same rule (S-4's argument, unchanged).
WARMUP_BARS = max(ATR_PERIOD, VOLUME_LOOKBACK, 2 * PIVOT_HALF_WINDOW + 1)


def _source_hash() -> str:
    """Hash of THIS module — the ``source_hash`` half of criterion 11."""
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


def s6_identity(*, universe: Universe, cost_model_id: str) -> StrategyIdentity:
    """The registered identity of S-6 on one universe under one cost model."""
    if not cost_model_id.strip():
        raise ValueError(
            "cost_model_id must be a non-empty declaration (criterion 11 hashes it); "
            "pass app.services.cost_model.COST_MODEL_ID rather than an empty string"
        )
    return StrategyIdentity(
        strategy_id=S6_STRATEGY_ID,
        params=S6_PARAMS,
        universe=universe,
        cost_model_id=cost_model_id,
        source_hash=_source_hash(),
    )


def _volumes(series: BarSeries) -> np.ndarray:
    """Bar volumes as floats, with a missing or negative volume as NaN.

    ⚠ A NEGATIVE volume is treated as missing rather than clamped. ``price_daily``
    stores it unconstrained and the quarantine has no axis for it, so this module
    is the boundary: a negative value cannot be an observation, and averaging one
    in would quietly lower the bar the confirmation leg has to clear.
    """
    out = np.empty(len(series), dtype=float)
    for index, row in enumerate(series.rows):
        value = row.get("volume")
        out[index] = np.nan if value is None or value < 0 else float(value)
    return out


def volume_ratio_series(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """``volume(t)`` over the mean of the ``VOLUME_LOOKBACK`` bars BEFORE ``t``.

    ⚠ ``t`` IS EXCLUDED from its own denominator — see the module docstring.

    A missing volume anywhere in the window, or at ``t`` itself, is a DATA
    refusal and is listed in ``not_evaluable_indices``: the field is present and
    empty, so the window genuinely cannot support an average. Indices with fewer
    than ``VOLUME_LOOKBACK`` bars behind them are warm-up and stay a bare
    ``None``. Collapsing the two would destroy what criterion 8 exists for.

    ⚠ A zero-volume window is a DATA refusal too, not an infinite ratio. Twenty
    consecutive sessions that all traded nothing is a halted or unlisted name,
    and dividing by it would report every subsequent bar as a spectacular volume
    surge.
    """
    volumes = _volumes(series)
    n = volumes.size
    values: list[float | None] = [None] * n
    unevaluable: list[int] = []
    for index in range(n):
        low = index - VOLUME_LOOKBACK
        if low < 0:
            continue  # fewer than VOLUME_LOOKBACK prior bars — warm-up.
        window = volumes[low:index]  # ⚠ excludes `index` itself.
        if not np.isfinite(volumes[index]) or not np.all(np.isfinite(window)):
            unevaluable.append(index)
            continue
        average = float(window.mean())
        if average <= 0.0:
            unevaluable.append(index)
            continue
        values[index] = float(volumes[index]) / average
    return IndicatorSeries(tuple(values), universe, tuple(unevaluable))


def prior_close_series(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """``close(t-1)`` — the price the chosen level must sit above.

    Index 0 has no predecessor and is warm-up. A masked ``close(t-1)`` is a data
    refusal: the level selection genuinely cannot be made without it.
    """
    closes = series.float_closes
    n = len(closes)
    values: list[float | None] = [None] * n
    unevaluable: list[int] = []
    for index in range(1, n):
        previous = closes[index - 1]
        if previous is None:
            unevaluable.append(index)
            continue
        values[index] = previous
    return IndicatorSeries(tuple(values), universe, tuple(unevaluable))


def _close_input(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """The bar closes, declared rather than relied upon transitively (S-4's note)."""
    closes = series.float_closes
    return IndicatorSeries(
        values=tuple(closes),
        universe=universe,
        not_evaluable_indices=tuple(i for i, value in enumerate(closes) if value is None),
    )


def _breakout_level(scan: LevelScan, *, prior_close: float, atr: float, index: int) -> PriceLevel | None:
    """The lowest live resistance strictly above ``prior_close`` at ``index``.

    ⚠⚠ THE SINGLE DEFINITION. ``s6_signals`` and ``s6_exit_bracket`` both call
    it, so the level the stop is anchored to is by construction the level the
    signal broke. Two call sites reading "the level" from two expressions is how
    a stop comes to be measured from a price the signal never considered.
    """
    candidates = [
        level for level in scan.at(atr=atr, index=index) if level.kind == "resistance" and level.price > prior_close
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda level: level.price)


def s6_exit_bracket(
    series: BarSeries,
    *,
    signal_index: int,
    entry_price: Decimal,
    universe: Universe,
) -> tuple[Decimal, Decimal, int]:
    """S-6's bracket: target from the entry, stop from the broken level.

    Returns ``(target, stop, max_hold_bars)`` — the order ``s4_exit_bracket``
    established, kept identical so the manifest's adapters read the same way.

    ⚠ Every input is read at ``signal_index``, never at the fill. Recomputing ATR
    or the level at ``signal_index + 1`` would let the fill bar's own range alter
    levels that had to exist before that bar was observed.
    """
    if not 0 <= signal_index < len(series):
        raise ValueError(f"signal_index {signal_index} is outside the {len(series)}-bar series")
    if entry_price <= 0:
        raise ValueError(f"entry_price must be positive, got {entry_price}")
    atr = atr_series(series, period=ATR_PERIOD, universe=universe).values[signal_index]
    if atr is None or atr <= 0:
        raise ValueError(f"ATR{ATR_PERIOD} is unavailable or non-positive at signal index {signal_index}")
    prior_close = series.float_closes[signal_index - 1] if signal_index >= 1 else None
    if prior_close is None:
        raise ValueError(f"close({signal_index - 1}) is unavailable, so S-6's level cannot be re-derived")
    scan = LevelScan.build(highs=series.array_highs, lows=series.array_lows, volumes=_volumes(series))
    level = _breakout_level(scan, prior_close=prior_close, atr=atr, index=signal_index)
    if level is None:
        raise ValueError(
            f"no live resistance above close({signal_index - 1}) at signal index {signal_index} — "
            "a bracket was requested for a bar that cannot have fired"
        )
    distance = Decimal(str(atr))
    stop = Decimal(str(level.price)) - Decimal(str(LEVEL_STOP_MULTIPLE)) * distance
    target = entry_price + Decimal(str(ATR_TARGET_MULTIPLE)) * distance
    if stop <= 0:
        raise ValueError(
            f"S-6's level-anchored stop {stop} is non-positive for level {level.price}; "
            "the bracket is not broker-orderable"
        )
    # ⚠ A level-anchored stop can sit ABOVE the fill: price gapped up through the
    # break and opened more than 1xATR above the level. Refused rather than
    # silently entered, because a "stop" above the entry is an immediate exit
    # wearing a protective order's name, and the outcome resolver would book it
    # as a loss on the fill bar. Counted by `--census` rather than assumed rare.
    if stop >= entry_price:
        raise ValueError(
            f"S-6's stop {stop} is at or above the entry {entry_price}: the fill gapped more than "
            f"{LEVEL_STOP_MULTIPLE}xATR above the broken level {level.price}"
        )
    return target, stop, MAX_HOLD_BARS


def s6_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
    market: MarketContext,
) -> list[StrategySignal]:
    """S-6's entry verdict for every bar. ⚠ ENTRIES ONLY.

    Like S-4, all three exit conditions are measured FROM THE ENTRY or from a
    level fixed at entry, so all three are position state and a pure per-bar
    verdict function has none. ``LEVEL_STOP_MULTIPLE``, ``ATR_TARGET_MULTIPLE``
    and ``MAX_HOLD_BARS`` are carried in ``S6_PARAMS`` so criterion 11 hashes
    them, and their consumer is ``s6_exit_bracket``.

    ⚠ The regime is looked up by ``series.dates``, which is why no separate date
    vector is accepted. ``BarSeries`` validates that its dates and rows align at
    construction, and ``strategy_segmented_evaluation`` builds each segment as a
    ``BarSeries`` slice carrying its own dates — so the segment's bars and the
    days they are gated on cannot come apart. A ``dates=`` parameter would
    reintroduce exactly that possibility for no caller that needs it.
    """
    if masked_reason not in NOT_EVALUABLE_REASONS:
        raise ValueError(f"unknown reason code {masked_reason!r}; must be one of {sorted(NOT_EVALUABLE_REASONS)}")

    closes = series.float_closes
    atr = atr_series(series, universe=universe, period=ATR_PERIOD)
    volume_ratio = volume_ratio_series(series, universe=universe)
    prior_close = prior_close_series(series, universe=universe)
    gate = market.gate_series(series.dates, allowed=PERMITTED_REGIMES, universe=universe)
    scan = LevelScan.build(highs=series.array_highs, lows=series.array_lows, volumes=_volumes(series))

    inputs = (
        StrategyInput(series=_close_input(series, universe=universe), reason=masked_reason),
        StrategyInput(series=atr, reason=masked_reason),
        StrategyInput(series=prior_close, reason=masked_reason),
        # ⚠ `missing_volume` is hardcoded and NOT taken from `masked_reason`,
        # departing from S-4 knowingly. The registry's rule is that the caller
        # supplies the reason because `indicator_series` "knows THAT a value is
        # unevaluable but not WHY" — but here the strategy DOES know why:
        # `load_masked_bars` masks only OHLC, so a null volume is a genuinely
        # absent field and never a quarantine mask. Passing `masked_reason` would
        # report a real missing-volume gap as `quarantined_bar`.
        StrategyInput(series=volume_ratio, reason="missing_volume"),
        StrategyInput(series=gate, reason="missing_market_context"),
    )

    def entry(index: int) -> bool:
        close = closes[index]
        previous = prior_close.values[index]
        current_atr = atr.values[index]
        ratio = volume_ratio.values[index]
        permitted = gate.values[index]
        # Not reachable through `evaluate`, which refuses the bar first. Present
        # to narrow the types and to fail loudly for a direct caller.
        assert close is not None and previous is not None and current_atr is not None
        assert ratio is not None and permitted is not None
        if permitted != 1.0:
            return False
        if ratio < VOLUME_MULTIPLE:
            return False
        level = _breakout_level(scan, prior_close=previous, atr=current_atr, index=index)
        return level is not None and close > level.price

    return evaluate(entry, inputs=inputs, n_bars=len(series), kind="entry")


__all__ = [
    "ATR_PERIOD",
    "ATR_TARGET_MULTIPLE",
    "LEVEL_STOP_MULTIPLE",
    "MAX_HOLD_BARS",
    "PERMITTED_REGIMES",
    "S6_PARAMS",
    "S6_STRATEGY_ID",
    "VOLUME_LOOKBACK",
    "VOLUME_MULTIPLE",
    "WARMUP_BARS",
    "prior_close_series",
    "s6_exit_bracket",
    "s6_identity",
    "s6_signals",
    "volume_ratio_series",
]
