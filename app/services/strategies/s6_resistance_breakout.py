"""S-6 — resistance breakout with volume confirmation. First of the S-5…S-10 set.

Parent spec: ``docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md`` §3 (S-6),
§1 (regime), §2 (levels). Registry contract: ``app/services/strategy_registry.py``.
Refs #2437.

THE RULE, VERBATIM FROM §3
-------------------------
    Setup: live resistance level; regime ∈ {``bull_quiet``}. ⚠ Excluded from
    ``bull_volatile`` deliberately — breakouts into a Bulge are the classic
    false-break regime. Signal: ``close(t) >`` level **and** ``volume(t) >= 1.2
    × 20-bar average volume``. ⚠ The 1.2 multiple is retail convention, **not**
    a published result — frozen by construction, recorded as such. Exit: stop
    ``level − 1.0 × ATR(14)`` (back inside the range = failed break); target
    ``entry + 3.0 × ATR(14)``; max hold 40 bars. Params: 4. Data: OHLC + volume.

⚠⚠ THE REGIME IS AN ARGUMENT, NOT A COMPUTATION, AND THAT IS DELIBERATE.
Regime is a property of the MARKET (SPY), not of the instrument being scanned, so
this module cannot derive it from ``series`` — the bars it is handed are the
candidate's, not the benchmark's. Passing it in keeps the function pure and
keeps one regime classification shared across every strategy in a cycle, rather
than each recomputing it and silently disagreeing at the boundary.

⚠ ``regime`` is REQUIRED with no default. A default would let a caller scan
without a regime and get the permissive behaviour by omission — which is the
exact failure ``RegimeSeries.permits`` fails closed against. Absent regime must
mean "cannot evaluate", never "no constraint".

⚠⚠ WHY THE STOP IS OFF THE LEVEL AND THE TARGET IS OFF THE ENTRY.
They are not the same anchor and making them consistent would break the rule.
The stop answers *"was this a false break?"* — which is a question about the
LEVEL: price closing back below it invalidates the setup regardless of where the
fill happened. The target answers *"has this paid enough?"* — a question about
the POSITION, so it is measured from what was actually paid. A gap-up fill can
therefore sit far above its own stop, and that is correct: the trade is wrong
when the level fails, not when the fill is retraced.

⚠ NO EXIT SIGNAL LEG, for S-4's reason. Stop, target and max-hold are all
measured from the entry, so all three are position state; a per-bar verdict
function has none. The parameters are carried in ``S6_PARAMS`` so criterion 11
hashes them into the identity, and their consumer is ``s6_exit_bracket``.
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
from app.services.market_regime import REGIME_RULE_VERSION, Regime, RegimeSeries
from app.services.price_levels import LEVEL_RULE_VERSION, LevelScan
from app.services.strategy_registry import (
    NOT_EVALUABLE_REASONS,
    NotEvaluableReason,
    StrategyIdentity,
    StrategyInput,
    StrategySignal,
    evaluate,
)

S6_STRATEGY_ID = "s6-resistance-breakout"

#: ⚠ FIXED, NEVER TUNED. Module constants rather than arguments, for S-4's
#: reason: a threshold that can be passed in is a threshold that can be swept,
#: and criterion 11 would need every swept value registered as its own strategy.
ATR_PERIOD = 14
VOLUME_LOOKBACK = 20

#: ⚠⚠ RETAIL CONVENTION, NOT A PUBLISHED RESULT. Widely quoted as "volume >= 1.2x
#: the 20-bar average confirms a break", and no peer-reviewed formulation was
#: found for it. Frozen by construction per the repo rule for that case, and
#: labelled here so nobody later cites it as evidence-backed. If a published
#: rule is found, it is a NEW VERSION, not a correction of this one.
VOLUME_CONFIRMATION_MULTIPLE = 1.2

#: The bracket, in ATR multiples at the SIGNAL bar. Declared and hashed here,
#: evaluated by ``s6_exit_bracket``.
ATR_STOP_MULTIPLE = 1.0
ATR_TARGET_MULTIPLE = 3.0
MAX_HOLD_BARS = 40

#: ⚠ ONE REGIME ONLY. §3 excludes ``bull_volatile`` on purpose: a breakout into a
#: Bollinger Bulge is the textbook false-break condition, so admitting it would
#: dilute the rule with its own worst cases. A frozenset rather than a single
#: member because the shape must accommodate S-5/S-7/S-9, which permit two.
PERMITTED_REGIMES = frozenset({Regime.BULL_QUIET})

#: ⚠ §3 says "Params: 4" and this dict carries SEVEN, for S-4's recorded reason:
#: §3 counts FREE parameters (what a sweep would move) while criterion 11 hashes
#: everything that makes this a distinct strategy. ``atr_period`` is Wilder's
#: default and the two exit multiples are read by nothing in this module — the
#: identity hash is the only thing keeping them attached to the rule.
#:
#: ⚠⚠ THE TWO SHARED RULE VERSIONS ARE IN HERE AND THEY MUST BE. This strategy is
#: a function of the regime classifier and the level constructor as much as of
#: its own constants: the same bars under a different ``LEVEL_RULE_VERSION``
#: produce different levels and therefore different signals. Omitting them would
#: let a level-rule edit silently inherit this strategy's track record, which is
#: precisely what criterion 11 exists to prevent.
S6_PARAMS: Mapping[str, object] = {
    "atr_period": ATR_PERIOD,
    "volume_lookback": VOLUME_LOOKBACK,
    "volume_confirmation_multiple": VOLUME_CONFIRMATION_MULTIPLE,
    "atr_stop_multiple": ATR_STOP_MULTIPLE,
    "atr_target_multiple": ATR_TARGET_MULTIPLE,
    "max_hold_bars": MAX_HOLD_BARS,
    "permitted_regimes": tuple(sorted(r.value for r in PERMITTED_REGIMES)),
    "level_rule_version": LEVEL_RULE_VERSION,
    "regime_rule_version": REGIME_RULE_VERSION,
}

#: DERIVED from the rule, not restated. Levels need a confirmed pivot, which
#: needs ``PIVOT_HALF_WINDOW`` bars of lead-in and ``MIN_TOUCHES`` of them; the
#: binding constraint in practice is the regime series, which needs a 200-SMA and
#: a full 126-bar BandWidth window (~326 bars) — but that is the BENCHMARK's
#: warmup, not this series', and it arrives as ``None`` in ``regime``. Locally,
#: ATR at ``ATR_PERIOD`` and the volume average at ``VOLUME_LOOKBACK - 1`` bind.
WARMUP_BARS = max(ATR_PERIOD, VOLUME_LOOKBACK - 1)


def _source_hash() -> str:
    """Hash of THIS module — the ``source_hash`` half of criterion 11."""
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


def s6_identity(*, universe: Universe, cost_model_id: str) -> StrategyIdentity:
    """The registered identity of S-6 on one universe under one cost model.

    Both arguments REQUIRED with no default, per S-4: criterion 11 makes universe
    and cost model part of the identity, so a default would silently register a
    strategy the caller never declared.
    """
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


def _close_input(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """The bar closes, in the shape the runner checks for evaluability.

    ⚠⚠ ``not_evaluable_indices`` IS THE POINT, and omitting it was a real bug
    caught by ``test_the_reason_code_reaches_the_verdict``. A bare
    ``IndicatorSeries`` of closes carries values but declares no masked bars, so
    a masked close fell through to whichever input refused first — reporting
    ``insufficient_warmup`` for a bar that was actually MASKED. The verdict was
    still "not evaluable", so nothing broke; the RECORDED REASON was simply
    wrong, which is the kind of defect that survives every test that only checks
    whether a bar fired.
    """
    closes = series.float_closes
    return IndicatorSeries(
        values=tuple(closes),
        universe=universe,
        not_evaluable_indices=tuple(i for i, value in enumerate(closes) if value is None),
    )


def _volumes(series: BarSeries) -> np.ndarray:
    """Bar volumes as float, with missing volume as NaN.

    ⚠ NaN rather than 0.0. A missing volume is UNKNOWN, and zero is a claim that
    nothing traded — which would make the confirmation test trivially fail and
    look like a considered refusal rather than absent data. NaN propagates into
    the average and the bar is refused as unevaluable, which is the truthful
    outcome.
    """
    out = np.full(len(series), np.nan)
    for i, row in enumerate(series.rows):
        vol = row.get("volume")
        if vol is not None:
            out[i] = float(vol)
    return out


def average_volume_series(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """Trailing ``VOLUME_LOOKBACK``-bar mean volume, ending at ``t`` INCLUSIVE.

    ⚠ INCLUSIVE of ``t``, unlike S-4's prior-high window which excludes it. The
    two are asymmetric for the same reason S-4 documents: the breakout LEVEL must
    not be self-referential, but the volume BASELINE is a question about the
    recent distribution that today belongs to. ⚠ Note the signal then compares
    ``volume(t)`` against an average that CONTAINS ``volume(t)`` — which makes
    the test conservative (a huge bar lifts its own baseline), never permissive.
    That direction is why it is acceptable; the reverse would not be.
    """
    vols = _volumes(series)
    n = vols.size
    values: list[float | None] = []
    for i in range(n):
        if i < VOLUME_LOOKBACK - 1:
            values.append(None)
            continue
        window = vols[i - VOLUME_LOOKBACK + 1 : i + 1]
        values.append(None if not np.all(np.isfinite(window)) else float(window.mean()))
    return IndicatorSeries(values=tuple(values), universe=universe)


def s6_exit_bracket(
    series: BarSeries,
    *,
    signal_index: int,
    entry_price: Decimal,
    universe: Universe,
) -> tuple[Decimal, Decimal, int]:
    """``(take_profit, stop_loss, max_hold_bars)`` for a fill against S-6's signal bar.

    ⚠⚠ ORDER MATCHES ``s4_exit_bracket`` — TARGET FIRST, THEN STOP. Both return
    ``tuple[Decimal, Decimal, int]``, so the two are structurally identical and
    a mismatched order would be invisible to the type checker and to review: the
    adapter would build ``ExitLevels(take_profit=stop, stop_loss=target)`` and
    every bracket would be inverted. This repo has already shipped that exact
    class of defect through positional lists (#2623). Same order, deliberately.

    ⚠⚠ THE STOP IS ANCHORED TO THE LEVEL, THE TARGET TO THE ENTRY. See the module
    docstring — this asymmetry is the rule, not an oversight, and normalising it
    would change what the strategy means.

    ⚠ ATR and the level are both read at ``signal_index``, never at the fill bar.
    Sizing a stop off the fill bar's own range leaks that bar into the decision
    that produced it.
    """
    atr = atr_series(series, universe=universe, period=ATR_PERIOD)
    atr_at_signal = atr.values[signal_index]
    if atr_at_signal is None:
        raise ValueError(f"S-6 bracket needs ATR at the signal bar; index {signal_index} is unevaluable")
    level = _resistance_below(series, index=signal_index, atr=atr_at_signal)
    if level is None:
        raise ValueError(f"S-6 bracket needs the resistance level at index {signal_index}; none is live")
    stop = Decimal(str(level - ATR_STOP_MULTIPLE * atr_at_signal))
    target = entry_price + Decimal(str(ATR_TARGET_MULTIPLE * atr_at_signal))
    return target, stop, MAX_HOLD_BARS


def _resistance_below(series: BarSeries, *, index: int, atr: float, scan: LevelScan | None = None) -> float | None:
    """The nearest live resistance level below this bar's close, or ``None``.

    ⚠⚠ NO REGIME GATE HERE, AND THAT IS THE POINT OF THE SPLIT. The gate belongs
    to the SIGNAL (`s6_signals`), not to the level lookup, because the two are
    asked at different times and only one of them is a decision.

    ``s6_exit_bracket`` needs this level to place a stop for a fill against a
    signal that ALREADY FIRED — so the regime was permitted by construction at
    signal time. Re-checking it at bracket time would let a regime that has since
    moved refuse to produce a stop for a position that is already open, which is
    the worst possible moment to have no stop. A gate that can retroactively
    invalidate an open position's exit is not a safety control.

    ⚠ Levels remain causal: only pivots CONFIRMED by ``index`` are considered,
    and the ATR sizing the cluster tolerance is the one at ``index``.

    ⚠⚠ ``scan`` REPLACES THE PER-CALL RECOMPUTE, AND IT IS NOT THE CACHE THIS
    DOCSTRING PREVIOUSLY REFUSED. The objection was exact and correct: *"a cache
    keyed by anything other than the exact bar index is the standard way lookahead
    gets reintroduced"*. ``LevelScan`` has NO KEY. It precomputes swing-pivot
    DETECTION for the whole series — legal because whether bar ``i`` is a pivot
    depends only on bars ``i-5 .. i+5`` and never on where the observer stands —
    and ``at(index)`` then filters to pivots confirmed by ``index`` on every call.
    The causal filter still runs per bar; only the detection is shared, so there is
    nothing to invalidate and no key to get wrong.

    Measured rather than asserted: ``scripts/verify_2437_level_scan.py
    --equivalence`` compares the two forms over 45,094 (instrument, bar, arm)
    comparisons — including quarantine-masked instruments and the ``volumes=None``
    arm — and reports 0 mismatches.

    ⚠ ``scan`` defaults to ``None`` and is built internally when omitted, so a
    direct caller keeps the standalone contract.
    """
    if scan is None:
        scan = LevelScan.build(highs=series.array_highs, lows=series.array_lows, volumes=_volumes(series))
    levels = scan.at(atr=atr, index=index)
    close = series.float_closes[index]
    if close is None:
        return None
    # The level being broken is the nearest resistance BELOW the close — price
    # has to have crossed it. `nearest_level` alone would also match one just
    # above, which is a level not yet broken.
    candidates = [lvl for lvl in levels if lvl.kind == "resistance" and lvl.price < close]
    if not candidates:
        return None
    return max(candidates, key=lambda lvl: lvl.price).price


def s6_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,
) -> list[StrategySignal]:
    """S-6's entry verdict for every bar. ⚠ ENTRIES ONLY — see the module docstring."""
    if masked_reason not in NOT_EVALUABLE_REASONS:
        raise ValueError(f"unknown reason code {masked_reason!r}; must be one of {sorted(NOT_EVALUABLE_REASONS)}")
    if len(regime) != len(series):
        raise ValueError(f"regime series has {len(regime)} bars against {len(series)} price bars; they must align")

    closes = series.float_closes
    atr = atr_series(series, universe=universe, period=ATR_PERIOD)
    avg_volume = average_volume_series(series, universe=universe)
    vols = _volumes(series)
    # ⚠ ONE scan for the whole series, built here and passed into every per-bar
    # lookup. Without it `_resistance_below` rebuilds whole-series pivot
    # detection on every bar, which makes a full-series evaluation quadratic —
    # 3.61 ms/bar against 0.1532 ms/bar, measured. See `_resistance_below` for
    # why this is not the cache that docstring refuses.
    scan = LevelScan.build(highs=series.array_highs, lows=series.array_lows, volumes=vols)

    inputs = (
        StrategyInput(series=_close_input(series, universe=universe), reason=masked_reason),
        StrategyInput(series=atr, reason=masked_reason),
        StrategyInput(series=avg_volume, reason=masked_reason),
    )

    def entry(index: int) -> bool:
        close = closes[index]
        atr_now = atr.values[index]
        avg = avg_volume.values[index]
        # Not reachable through `evaluate`, which refuses the bar first.
        assert close is not None and atr_now is not None and avg is not None
        volume_now = vols[index]
        if not np.isfinite(volume_now) or avg <= 0:
            return False
        if volume_now < VOLUME_CONFIRMATION_MULTIPLE * avg:
            return False
        # ⚠ THE REGIME GATE IS HERE, on the decision, not inside the level
        # lookup — see `_resistance_below`. Checked before the level work
        # because it is the cheaper refusal and the more fundamental one.
        if not regime.permits(index, PERMITTED_REGIMES):
            return False
        level = _resistance_below(series, index=index, atr=atr_now, scan=scan)
        if level is None or close <= level:
            return False
        # ⚠⚠ THE BREAK IS A CROSSING, NOT A POSITION. `close > level` alone is
        # satisfied on EVERY subsequent bar once price sits above an old level,
        # so the rule degenerates into "price is above some level and volume is
        # up" and re-fires indefinitely. Measured before this guard: 70 entries
        # per instrument over ~4 years, one every ~15 bars — a breakout is not a
        # fortnightly event, and the count is what exposed it.
        #
        # The prior close must be AT OR BELOW the level, so the signal marks the
        # bar that crossed it. §3's "close(t) > level" states the condition at
        # `t` and takes the crossing as read; stating it here is the honest
        # reading, not an extra parameter — there is nothing to tune.
        prior_close = closes[index - 1] if index > 0 else None
        return prior_close is not None and prior_close <= level

    return evaluate(entry, inputs=inputs, n_bars=len(series), kind="entry")


__all__ = [
    "ATR_PERIOD",
    "ATR_STOP_MULTIPLE",
    "ATR_TARGET_MULTIPLE",
    "MAX_HOLD_BARS",
    "PERMITTED_REGIMES",
    "S6_PARAMS",
    "S6_STRATEGY_ID",
    "VOLUME_CONFIRMATION_MULTIPLE",
    "VOLUME_LOOKBACK",
    "WARMUP_BARS",
    "average_volume_series",
    "s6_exit_bracket",
    "s6_identity",
    "s6_signals",
]
