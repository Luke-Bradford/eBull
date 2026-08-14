"""S-5 — support bounce. Second of the S-5…S-10 set.

Parent spec: ``docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md`` §3 (S-5),
§1 (regime), §2 (levels). Registry contract: ``app/services/strategy_registry.py``.
Refs #2437.

THE RULE, VERBATIM FROM §3
-------------------------
    Setup: price within 0.5 × ATR(14) of a live support level (§2); regime ∈
    {``bull_quiet``, ``bull_volatile``}. Signal: bar ``t`` closes **above** the
    level having traded below it intrabar — rejection, not a close-through.
    Exit: stop ``level − 1.0 × ATR(14)``; target ``entry + 2.0 × ATR(14)``; max
    hold 30 bars. Params: 4. Data: OHLC + volume.

⚠⚠ THE SIGNAL IS A REJECTION, AND THAT IS WHY IT READS THE LOW.
``close(t) > level`` alone is true on almost every bar of an uptrend that never
came near the level — it describes POSITION, not EVENT, which is the exact defect
S-6's first implementation shipped (70 entries per instrument per four years).
S-5 avoids it by construction rather than by a crossing guard: the bar must have
traded BELOW the level intrabar (``low(t) < level``) and closed back ABOVE it.
That pair is a wick through support that got bought — one bar, self-contained,
and it cannot be satisfied by simply being above the level.

⚠ CONTRAST WITH S-6 DELIBERATELY. S-6 needs `close(t-1) <= level` because a
close-through says nothing about the prior bar; S-5 needs no prior bar at all
because the low and the close of the SAME bar already encode the rejection. Two
rules, two shapes, same failure avoided.

⚠ PERMITS ``bull_volatile``, UNLIKE S-6. A breakout into a Bollinger Bulge is the
classic false break, so S-6 excludes it. A bounce is the opposite trade: the
volatility that makes a breakout fail is what produces the wick this rule wants.
The asymmetry is the spec's, and collapsing the two strategies onto one regime
set would quietly change both.

⚠ NO EXIT SIGNAL LEG, for S-4's reason. Stop, target and max-hold are all
measured from the entry, so all three are position state; a per-bar verdict
function has none.
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
from app.services.price_levels import LEVEL_RULE_VERSION, levels_at
from app.services.strategy_registry import (
    NOT_EVALUABLE_REASONS,
    NotEvaluableReason,
    StrategyIdentity,
    StrategyInput,
    StrategySignal,
    evaluate,
)

S5_STRATEGY_ID = "s5-support-bounce"

#: ⚠ FIXED, NEVER TUNED — a threshold that can be passed in is one that can be
#: swept, and criterion 11 would need every swept value registered separately.
ATR_PERIOD = 14

#: The bracket, in ATR multiples at the SIGNAL bar.
#:
#: ⚠ TARGET IS 2.0 HERE AND 3.0 ON S-6, and that is the spec's asymmetry, not an
#: oversight. A bounce trades back into a range and has the opposite side of that
#: range as its natural ceiling; a breakout trades out of one and has none. Same
#: 1.0 stop, different reward, because the two setups have different room.
ATR_STOP_MULTIPLE = 1.0
ATR_TARGET_MULTIPLE = 2.0
MAX_HOLD_BARS = 30

#: ⚠ TWO REGIMES, unlike S-6's one. See the module docstring: the volatility that
#: makes a breakout fail is what produces the wick a bounce needs.
PERMITTED_REGIMES = frozenset({Regime.BULL_QUIET, Regime.BULL_VOLATILE})

#: ⚠ §3 says "Params: 4"; this carries six plus the two shared rule versions, for
#: S-4's recorded reason — §3 counts FREE parameters, criterion 11 hashes
#: everything that makes this a distinct strategy.
#:
#: ⚠⚠ The shared rule versions MUST be here. The same bars under a different
#: ``LEVEL_RULE_VERSION`` produce different levels and therefore different
#: signals, so omitting them would let a level-rule edit silently inherit this
#: strategy's track record.
S5_PARAMS: Mapping[str, object] = {
    "atr_period": ATR_PERIOD,
    "atr_stop_multiple": ATR_STOP_MULTIPLE,
    "atr_target_multiple": ATR_TARGET_MULTIPLE,
    "max_hold_bars": MAX_HOLD_BARS,
    "permitted_regimes": tuple(sorted(r.value for r in PERMITTED_REGIMES)),
    "level_rule_version": LEVEL_RULE_VERSION,
    "regime_rule_version": REGIME_RULE_VERSION,
}

#: DERIVED from the rule. ATR emits its first value at ``ATR_PERIOD``; the level
#: constructor needs confirmed pivots, which arrive later but produce ``None``
#: rather than a wrong answer, so ATR binds locally.
WARMUP_BARS = ATR_PERIOD


def _source_hash() -> str:
    """Hash of THIS module — the ``source_hash`` half of criterion 11."""
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


def s5_identity(*, universe: Universe, cost_model_id: str) -> StrategyIdentity:
    """The registered identity of S-5 on one universe under one cost model."""
    if not cost_model_id.strip():
        raise ValueError(
            "cost_model_id must be a non-empty declaration (criterion 11 hashes it); "
            "pass app.services.cost_model.COST_MODEL_ID rather than an empty string"
        )
    return StrategyIdentity(
        strategy_id=S5_STRATEGY_ID,
        params=S5_PARAMS,
        universe=universe,
        cost_model_id=cost_model_id,
        source_hash=_source_hash(),
    )


def _close_input(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """The bar closes, in the shape the runner checks for evaluability.

    ⚠ ``not_evaluable_indices`` is the load-bearing part. Without it a MASKED
    close is reported as ``insufficient_warmup`` — the verdict is still "not
    evaluable" so nothing breaks, but the recorded REASON is wrong, and that
    survives every test which only checks whether a bar fired. S-6 shipped
    exactly that bug and a manifest guard caught it.
    """
    closes = series.float_closes
    return IndicatorSeries(
        values=tuple(closes),
        universe=universe,
        not_evaluable_indices=tuple(i for i, value in enumerate(closes) if value is None),
    )


def _volumes(series: BarSeries) -> np.ndarray:
    """Bar volumes as float, missing volume as NaN.

    ⚠ Used only to weight level clustering, never as a signal condition — S-5 has
    no volume test. NaN rather than 0.0 so a missing volume does not silently
    weight a pivot to zero.
    """
    out = np.full(len(series), np.nan)
    for i, row in enumerate(series.rows):
        vol = row.get("volume")
        if vol is not None:
            out[i] = float(vol)
    return out


def _support_below(series: BarSeries, *, index: int, atr: float) -> float | None:
    """The nearest live support level within tolerance of this bar's action.

    ⚠ NO REGIME GATE — the gate belongs to the signal, not the level lookup, so
    ``s5_exit_bracket`` can place a stop for an already-open position without a
    since-moved regime being able to refuse it. Same split as S-6.

    ⚠ The level must sit at or below the CLOSE and at or above the LOW: it is the
    level the bar wicked through and reclaimed. A support above the close was not
    reclaimed; one below the low was never touched.
    """
    close = series.float_closes[index]
    low = series.float_lows[index]
    if close is None or low is None:
        return None
    levels = levels_at(
        highs=series.array_highs,
        lows=series.array_lows,
        volumes=_volumes(series),
        atr=atr,
        index=index,
    )
    candidates = [lvl.price for lvl in levels if lvl.kind == "support" and low < lvl.price <= close]
    if not candidates:
        return None
    # The highest such level is the one most recently defended.
    return max(candidates)


def s5_exit_bracket(
    series: BarSeries,
    *,
    signal_index: int,
    entry_price: Decimal,
    universe: Universe,
) -> tuple[Decimal, Decimal, int]:
    """``(take_profit, stop_loss, max_hold_bars)`` for a fill against S-5's signal bar.

    ⚠⚠ ORDER MATCHES ``s4_exit_bracket`` AND ``s6_exit_bracket`` — TARGET FIRST.
    All three return ``tuple[Decimal, Decimal, int]``, so a mismatched order is
    invisible to the type checker and to review, and the adapter would build an
    inverted bracket. This repo has shipped that class through positional lists
    (#2623).

    ⚠ The stop anchors to the LEVEL, the target to the ENTRY — S-6's asymmetry
    for S-6's reason: the stop asks "did support fail?", which is a question
    about the level; the target asks "has this paid enough?", about the position.
    """
    atr = atr_series(series, universe=universe, period=ATR_PERIOD)
    atr_at_signal = atr.values[signal_index]
    if atr_at_signal is None:
        raise ValueError(f"S-5 bracket needs ATR at the signal bar; index {signal_index} is unevaluable")
    level = _support_below(series, index=signal_index, atr=atr_at_signal)
    if level is None:
        raise ValueError(f"S-5 bracket needs the support level at index {signal_index}; none is live")
    stop = Decimal(str(level - ATR_STOP_MULTIPLE * atr_at_signal))
    target = entry_price + Decimal(str(ATR_TARGET_MULTIPLE * atr_at_signal))
    return target, stop, MAX_HOLD_BARS


def s5_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,
) -> list[StrategySignal]:
    """S-5's entry verdict for every bar. ⚠ ENTRIES ONLY."""
    if masked_reason not in NOT_EVALUABLE_REASONS:
        raise ValueError(f"unknown reason code {masked_reason!r}; must be one of {sorted(NOT_EVALUABLE_REASONS)}")
    if len(regime) != len(series):
        raise ValueError(f"regime series has {len(regime)} bars against {len(series)} price bars; they must align")

    closes = series.float_closes
    lows = series.float_lows
    atr = atr_series(series, universe=universe, period=ATR_PERIOD)

    inputs = (
        StrategyInput(series=_close_input(series, universe=universe), reason=masked_reason),
        StrategyInput(series=atr, reason=masked_reason),
    )

    def entry(index: int) -> bool:
        close = closes[index]
        atr_now = atr.values[index]
        # Not reachable through `evaluate`, which refuses the bar first.
        assert close is not None and atr_now is not None
        low = lows[index]
        if low is None:
            return False
        if not regime.permits(index, PERMITTED_REGIMES):
            return False
        # `_support_below` already requires low < level <= close, so the
        # rejection is expressed once, in the level selection, rather than
        # restated here where the two could drift apart.
        return _support_below(series, index=index, atr=atr_now) is not None

    return evaluate(entry, inputs=inputs, n_bars=len(series), kind="entry")


__all__ = [
    "ATR_PERIOD",
    "ATR_STOP_MULTIPLE",
    "ATR_TARGET_MULTIPLE",
    "MAX_HOLD_BARS",
    "PERMITTED_REGIMES",
    "S5_PARAMS",
    "S5_STRATEGY_ID",
    "WARMUP_BARS",
    "s5_exit_bracket",
    "s5_identity",
    "s5_signals",
]
