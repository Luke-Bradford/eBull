"""S-8 — mean reversion in range. Fourth of the S-5…S-10 set.

Parent spec: ``docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md`` §3 (S-8),
§1 (regime). Registry contract: ``app/services/strategy_registry.py``.
Refs #2437, #2240.

THE RULE, VERBATIM FROM §3
-------------------------
    Setup: regime ∈ {``bear_quiet``, ``bull_quiet``}; ADX(14) < 20 (no trend —
    Wilder's ADX). Signal: ``close(t)`` < lower Bollinger band (20, 2) **and**
    ``close(t) > close(t-1)``. Exit: target = middle band (20-SMA); stop
    ``entry - 1.5 x ATR(14)``; max hold 15 bars. Params: 5. Data: OHLC.

WHY THIS ONE IS DIFFERENT FROM THE THREE ALREADY SHIPPED
--------------------------------------------------------
S-5, S-6 and S-9 are all *continuation or reaction at a structure* — a level, or
a volatility state. S-8 is the only one that bets on a range HOLDING, and it is
the only one permitted in a bear regime. That is deliberate breadth rather than
a fifth variant: a portfolio of shots with stated domains needs at least one
whose domain is "the market is going nowhere".

⚠⚠ EVERY GATE HERE IS A PUBLISHED FORMULATION, AND THE 20 IS WILDER'S.
ADX below 20 as "no trend" is Wilder's own reading in *New Concepts in Technical
Trading Systems* (1978) ch. 4, not a percentile of our corpus and not a number
picked to make the funnel look right. The Bollinger band is (20, 2), Bollinger's
own defaults, same as the regime's. Nothing in this module is tuned.

⚠ THE SECOND SIGNAL LEG IS THE WHOLE DIFFERENCE BETWEEN THIS AND CATCHING A
FALLING KNIFE. ``close(t) < lower band`` alone fires on every bar of a decline
once price leaves the band, which in a genuine breakdown is many bars in a row
and each one worse than the last. ``close(t) > close(t-1)`` requires the bar to
have turned up — one day of evidence that the excursion is being bought rather
than continued. It is the cheapest possible confirmation and it is the reason
the rule is mean reversion rather than momentum-in-reverse.

⚠ THE TARGET IS THE MIDDLE BAND, WHICH IS NOT AN ATR MULTIPLE, AND THAT IS THE
POINT. The band is where the rule says price is going — the mean it is reverting
to. An ATR-sized target would be a different strategy that happens to enter on
the same bar. It also means the reward is NOT fixed: a wide band gives a large
target and a narrow one gives a small target, which is the correct behaviour for
a rule whose thesis is "this excursion is too big for this regime".

⚠ NO EXIT SIGNAL LEG. Target, stop and max-hold are all measured from the entry
or from the signal bar, so all three are position state — the same shape as
S-4/S-5/S-6/S-9 and unlike S-1/S-3, which carry a per-bar exit rule.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path

from app.services.indicator_series import (
    BarSeries,
    IndicatorSeries,
    Universe,
    adx_series,
    atr_series,
    bollinger_series,
)
from app.services.market_regime import (
    BOLLINGER_NUM_STD,
    BOLLINGER_PERIOD,
    REGIME_RULE_VERSION,
    Regime,
    RegimeSeries,
)
from app.services.strategy_registry import (
    NOT_EVALUABLE_REASONS,
    NotEvaluableReason,
    StrategyIdentity,
    StrategyInput,
    StrategySignal,
    evaluate,
)

S8_STRATEGY_ID = "s8-range-mean-reversion"

#: ⚠ FIXED, NEVER TUNED.
ATR_PERIOD = 14
ADX_PERIOD = 14

#: Wilder's own "no trend" reading. ⚠ NOT a percentile of our corpus — see the
#: module docstring. Moving it is a new strategy, not a calibration.
ADX_TREND_CEILING = 20.0

#: ⚠ IMPORTED FROM ``market_regime`` RATHER THAN RESTATED. The regime's
#: volatility leg and this strategy's entry band must be the SAME band: if they
#: drifted apart, S-8 would measure an excursion against one distribution while
#: being gated on another, and the disagreement would show only at the extreme
#: — which is the only place either is consulted. S-9 restates its constants
#: deliberately (to hold a comparison against S-4 fixed); S-8 has no such
#: comparison, so the shared definition is the safer choice.
ENTRY_BAND_PERIOD = BOLLINGER_PERIOD
ENTRY_BAND_NUM_STD = BOLLINGER_NUM_STD

#: The bracket. ⚠ Only the STOP is an ATR multiple — the target is the middle
#: band, which is a price, not a distance.
ATR_STOP_MULTIPLE = 1.5
MAX_HOLD_BARS = 15

#: ⚠ THE ONLY STRATEGY IN THE SET PERMITTED IN A BEAR REGIME, and §3 says so.
#: A range is a range whichever side of the 200-SMA it sits on; what S-8 cannot
#: survive is a BULGE, because an expanding band is the market telling you the
#: excursion is not an excursion. Both volatile regimes are therefore excluded,
#: which is the same exclusion S-6 makes and for the same reason.
PERMITTED_REGIMES = frozenset({Regime.BEAR_QUIET, Regime.BULL_QUIET})

#: ⚠ §3 says "Params: 5"; criterion 11 hashes everything that makes this a
#: distinct strategy, including the shared regime rule version — the same bars
#: under a different regime boundary produce different signals.
S8_PARAMS: Mapping[str, object] = {
    "atr_period": ATR_PERIOD,
    "adx_period": ADX_PERIOD,
    "adx_trend_ceiling": ADX_TREND_CEILING,
    "entry_band_period": ENTRY_BAND_PERIOD,
    "entry_band_num_std": ENTRY_BAND_NUM_STD,
    "atr_stop_multiple": ATR_STOP_MULTIPLE,
    "max_hold_bars": MAX_HOLD_BARS,
    "permitted_regimes": tuple(sorted(r.value for r in PERMITTED_REGIMES)),
    "regime_rule_version": REGIME_RULE_VERSION,
}

#: DERIVED. ADX(14) binds by a wide margin: Wilder's index needs ``period``
#: smoothed bars and then ``period`` DX readings to seed its own average, so its
#: first value sits at ``2 * ADX_PERIOD - 1``. The band is ready at 19 and ATR
#: at 14.
WARMUP_BARS = 2 * ADX_PERIOD - 1


def _source_hash() -> str:
    """Hash of THIS module — the ``source_hash`` half of criterion 11."""
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


def s8_identity(*, universe: Universe, cost_model_id: str) -> StrategyIdentity:
    """The registered identity of S-8 on one universe under one cost model."""
    if not cost_model_id.strip():
        raise ValueError(
            "cost_model_id must be a non-empty declaration (criterion 11 hashes it); "
            "pass app.services.cost_model.COST_MODEL_ID rather than an empty string"
        )
    return StrategyIdentity(
        strategy_id=S8_STRATEGY_ID,
        params=S8_PARAMS,
        universe=universe,
        cost_model_id=cost_model_id,
        source_hash=_source_hash(),
    )


def _close_input(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """The bar closes, in the shape the runner checks for evaluability.

    ⚠ ``not_evaluable_indices`` is load-bearing: without it a MASKED close is
    recorded as ``insufficient_warmup``. The verdict stays "not evaluable" so
    nothing breaks — only the REASON is wrong, which survives every test that
    checks whether a bar fired. S-6 shipped that bug; a guard caught it.
    """
    closes = series.float_closes
    return IndicatorSeries(
        values=tuple(closes),
        universe=universe,
        not_evaluable_indices=tuple(i for i, value in enumerate(closes) if value is None),
    )


def _prior_close_input(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """``close(t-1)``, declared so the turn-up leg cannot be judged without it.

    ⚠⚠ DECLARED, NOT READ INLINE, AND THAT IS THE POINT OF THE REGISTRY.
    ``close(t) > close(t-1)`` reaches one bar back, so a masked ``close(t-1)``
    makes bar ``t`` unjudgeable even though every input AT ``t`` is fine. Reading
    it inside the body and returning ``False`` would store that bar as
    ``not_fired`` — the exact defect #2437 fixed for the regime, one input over.

    ⚠ Index 0 is a bare ``None`` (warm-up: there is no prior bar), while a
    masked prior close is listed. The two are different facts and the registry
    counts them separately.
    """
    closes = series.float_closes
    values: list[float | None] = [None] + list(closes[:-1])
    return IndicatorSeries(
        values=tuple(values),
        universe=universe,
        not_evaluable_indices=tuple(i for i in range(1, len(closes)) if closes[i - 1] is None),
    )


def s8_exit_bracket(
    series: BarSeries,
    *,
    signal_index: int,
    entry_price: Decimal,
    universe: Universe,
) -> tuple[Decimal, Decimal, int]:
    """``(take_profit, stop_loss, max_hold_bars)`` for a fill against S-8's signal bar.

    ⚠ ORDER MATCHES S-4, S-5, S-6 AND S-9 — target first. All return
    ``tuple[Decimal, Decimal, int]``, so a mismatched order is invisible to the
    type checker and would invert every bracket (#2623's shape).

    ⚠⚠ THE TARGET IS READ AT THE SIGNAL BAR AND NEVER MOVES. The middle band is
    recomputed every bar, so a target that tracked it would be a trailing exit —
    a different rule, and one whose exit price depends on bars after the entry.
    §3.5's "levels are fixed at signal time and never move" is inherited from the
    parent catalogue and is what makes the outcome attributable to the signal.

    ⚠ AN INVERTED BRACKET IS REACHABLE AND IS NOT A BUG TO PREVENT. The target
    is anchored to the signal bar's band while the stop is anchored to the FILL,
    so a gap up through the middle band on the open of ``t+1`` leaves
    ``target <= stop``. The manifest adapter refuses it as
    ``unorderable_exit_levels``, which is the truthful state: the rule's thesis
    was consumed by the gap before the position existed.
    """
    atr = atr_series(series, universe=universe, period=ATR_PERIOD)
    atr_at_signal = atr.values[signal_index]
    if atr_at_signal is None:
        raise ValueError(f"S-8 bracket needs ATR at the signal bar; index {signal_index} is unevaluable")
    bands = bollinger_series(series, universe=universe, period=ENTRY_BAND_PERIOD, num_std=ENTRY_BAND_NUM_STD)
    middle = bands.components["middle"][signal_index]
    if middle is None:
        raise ValueError(f"S-8 bracket needs the middle band at the signal bar; index {signal_index} is unevaluable")
    stop = entry_price - Decimal(str(ATR_STOP_MULTIPLE * atr_at_signal))
    target = Decimal(str(middle))
    return target, stop, MAX_HOLD_BARS


def s8_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,
) -> list[StrategySignal]:
    """S-8's entry verdict for every bar. ⚠ ENTRIES ONLY — see the module docstring."""
    if masked_reason not in NOT_EVALUABLE_REASONS:
        raise ValueError(f"unknown reason code {masked_reason!r}; must be one of {sorted(NOT_EVALUABLE_REASONS)}")
    if len(regime) != len(series):
        raise ValueError(f"regime series has {len(regime)} bars against {len(series)} price bars; they must align")

    closes = series.float_closes
    prior_closes = _prior_close_input(series, universe=universe)
    adx = adx_series(series, universe=universe, period=ADX_PERIOD)
    bands = bollinger_series(series, universe=universe, period=ENTRY_BAND_PERIOD, num_std=ENTRY_BAND_NUM_STD)
    lower = bands.components["lower"]

    inputs = (
        StrategyInput(series=_close_input(series, universe=universe), reason=masked_reason),
        StrategyInput(series=prior_closes, reason=masked_reason),
        StrategyInput(series=adx, reason=masked_reason),
        StrategyInput(series=bands, reason=masked_reason),
        # ⚠⚠ THE REGIME IS AN INPUT, NOT JUST A GATE IN THE BODY (#2437). A date
        # on which the benchmark contributed no bar cannot be judged by this
        # strategy at all; without this line it would be stored as `not_fired`.
        # ⚠ DECLARED LAST: `_unevaluable_reason_at` returns the FIRST matching
        # input's reason, and a bar that is both quarantined and missing its
        # market context is counted under the instrument's own defect.
        StrategyInput(series=regime, reason="missing_market_context"),
    )

    def entry(index: int) -> bool:
        close = closes[index]
        prior = prior_closes.values[index]
        adx_now = adx.values[index]
        band = lower[index]
        # Not reachable through `evaluate`, which refuses the bar first.
        assert close is not None and prior is not None and adx_now is not None and band is not None
        # ⚠ The regime is the cheapest refusal and the most fundamental, so it
        # is checked first — the same ordering S-6 uses.
        if not regime.permits(index, PERMITTED_REGIMES):
            return False
        # ⚠ STRICTLY BELOW the ceiling. Wilder's reading is "below 20"; `<= 20`
        # would admit the boundary, and a boundary admitted on one side of a
        # published threshold is a redefinition of it.
        if adx_now >= ADX_TREND_CEILING:
            return False
        if close >= band:
            return False
        # The turn-up. ⚠ STRICT: an unchanged close is not a turn.
        return close > prior

    return evaluate(entry, inputs=inputs, n_bars=len(series), kind="entry")


__all__ = [
    "ADX_PERIOD",
    "ADX_TREND_CEILING",
    "ATR_PERIOD",
    "ATR_STOP_MULTIPLE",
    "ENTRY_BAND_NUM_STD",
    "ENTRY_BAND_PERIOD",
    "MAX_HOLD_BARS",
    "PERMITTED_REGIMES",
    "S8_PARAMS",
    "S8_STRATEGY_ID",
    "WARMUP_BARS",
    "s8_exit_bracket",
    "s8_identity",
    "s8_signals",
]
