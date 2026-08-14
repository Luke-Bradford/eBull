"""S-7 — trend pullback. Fifth of the S-5…S-10 set.

Parent spec: ``docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md`` §3 (S-7),
§1 (regime). Registry contract: ``app/services/strategy_registry.py``.
Refs #2437, #2240.

THE RULE, VERBATIM FROM §3
-------------------------
    Setup: close > 200-SMA **and** 50-SMA > 200-SMA; regime ∈ {``bull_quiet``,
    ``bull_volatile``}. Signal: RSI(14) crosses back **above** 40 having been
    below it within 5 bars. ⚠ Wilder's RSI, smoothed his way — not a simple
    moving average of gains. 40 (not 30) because in an uptrend RSI rarely
    reaches 30; frozen by construction. Exit: stop ``entry − 2.0 × ATR(14)``;
    exit-signal on ``close < 50-SMA``; max hold 60 bars. Params: 5. Data: OHLC.

⚠⚠ THE FIRST STRATEGY WITH A STOP AND NO TARGET. S-1/S-3 carry an exit rule and
no levels at all; S-4/S-5/S-6/S-8/S-9 carry a full stop/target bracket. S-7
sits between them: the stop is position state (an ``ExitLevels`` with
``take_profit=None`` — the stop-only bracket the resolver documents), while
"the trend is over" is a per-bar verdict (``close < 50-SMA``) and therefore an
exit LEG, the same shape as S-3's. Its exit regime is the first to declare
``signal_pair`` and ``level_based`` together; ``position_builder`` evaluates
all declared close sources together and the earliest wins, so nothing new is
needed there.

⚠ THE CROSSING IS EDGE-TRIGGERED, AND THE READING IS RECORDED HERE BECAUSE §3
COMPRESSES IT INTO ONE SENTENCE. "Crosses back above 40" is taken as the bar
``t`` with ``rsi(t) > 40`` and ``rsi(t-1) <= 40`` — a crossing, not a state, for
S-6's measured reason (a state re-fires on every bar of the recovery). "Having
been below it within 5 bars" is the existential reading: some bar in
``t-5 … t-1`` with ``rsi < 40`` STRICTLY. Given the cross, ``rsi(t-1) <= 40``
already holds, so the clause binds only at the equality edge (an RSI that
touched exactly 40.0 and never dipped below) — it is stated in §3 and evaluated
literally rather than silently folded into the cross. No published formulation
exists for the pullback-depth window; 5 bars is §3's own construction, frozen
in the identity hash.

⚠ A ``None`` IN THE LOOKBACK REFUSES THE CROSS, NOT THE BAR. The bar's own
inputs decide evaluability (registry §3.1); prior bars may be masked. An
unevaluable ``rsi(t-1)`` means the crossing cannot be confirmed, so the entry
verdict is False — the same treatment as S-6's ``prior_close`` — rather than
``not_evaluable``, which would misreport a judgeable bar as unjudgeable.

⚠⚠ THE EXIT LEG DOES NOT DECLARE THE REGIME INPUT, AND THAT IS DELIBERATE.
Both legs declare the same four INSTRUMENT inputs (S-3's rule: evaluability is
a property of the strategy, so the exit does not go half-live 150 bars before
the entry). The regime is different in kind: it gates the DECISION to enter,
and ``close < 50-SMA`` reads nothing from the benchmark. Declaring it on the
exit leg would let a missing benchmark session refuse the exit verdict for a
position that is already open — the "gate that can retroactively invalidate an
open position's exit" S-6's level lookup already refuses to be.
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
    atr_series,
    rsi_series,
    sma_series,
)
from app.services.market_regime import REGIME_RULE_VERSION, Regime, RegimeSeries
from app.services.strategy_registry import (
    NOT_EVALUABLE_REASONS,
    NotEvaluableReason,
    StrategyIdentity,
    StrategyInput,
    StrategySignal,
    evaluate,
)

S7_STRATEGY_ID = "s7-trend-pullback"

#: ⚠ FIXED, NEVER TUNED. Wilder's defaults for RSI and ATR; the 50/200 pair is
#: the standard trend filter §3 states outright.
RSI_PERIOD = 14
ATR_PERIOD = 14
PULLBACK_SMA_PERIOD = 50
TREND_SMA_PERIOD = 200

#: ⚠ 40, NOT 30, and §3 says why: in an uptrend RSI rarely reaches 30, so an
#: oversold threshold tuned for all regimes would make the rule near-mute in the
#: only regimes it is permitted in. Frozen by construction.
RSI_REENTRY_THRESHOLD = 40.0

#: The dip window for "having been below it within 5 bars". ⚠ No published
#: formulation exists for a pullback-depth window; §3's own construction.
RSI_DIP_LOOKBACK_BARS = 5

#: The stop, in ATR multiples at the signal bar. ⚠ NO TARGET — see the module
#: docstring. The other close sources are the exit leg and the hold cap.
ATR_STOP_MULTIPLE = 2.0
MAX_HOLD_BARS = 60

#: Both bull regimes. A pullback needs a trend to pull back FROM, so the bear
#: regimes are excluded; unlike S-6, ``bull_volatile`` is permitted — a Bulge is
#: hostile to breakouts, not to buying a dip inside an intact uptrend, and §3
#: states the pair outright.
PERMITTED_REGIMES = frozenset({Regime.BULL_QUIET, Regime.BULL_VOLATILE})

#: ⚠ §3 says "Params: 5" (what a sweep would move); criterion 11 hashes
#: everything that makes this a distinct strategy, including the shared regime
#: rule version — the same bars under a different regime boundary produce
#: different signals.
S7_PARAMS: Mapping[str, object] = {
    "rsi_period": RSI_PERIOD,
    "atr_period": ATR_PERIOD,
    "pullback_sma_period": PULLBACK_SMA_PERIOD,
    "trend_sma_period": TREND_SMA_PERIOD,
    "rsi_reentry_threshold": RSI_REENTRY_THRESHOLD,
    "rsi_dip_lookback_bars": RSI_DIP_LOOKBACK_BARS,
    "atr_stop_multiple": ATR_STOP_MULTIPLE,
    "max_hold_bars": MAX_HOLD_BARS,
    "permitted_regimes": tuple(sorted(r.value for r in PERMITTED_REGIMES)),
    "regime_rule_version": REGIME_RULE_VERSION,
}

#: DERIVED. The 200-SMA binds by a wide margin — warm from bar 199; RSI at 14,
#: ATR at 14, the 50-SMA at 49.
WARMUP_BARS = TREND_SMA_PERIOD - 1


def _source_hash() -> str:
    """Hash of THIS module — the ``source_hash`` half of criterion 11."""
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


def s7_identity(*, universe: Universe, cost_model_id: str) -> StrategyIdentity:
    """The registered identity of S-7 on one universe under one cost model.

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
        strategy_id=S7_STRATEGY_ID,
        params=S7_PARAMS,
        universe=universe,
        cost_model_id=cost_model_id,
        source_hash=_source_hash(),
    )


def _close_input(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """The bar closes, in the shape the runner checks for evaluability.

    ⚠ ``not_evaluable_indices`` is the point — a bare series of closes carries
    values but declares no masked bars, so a masked close would fall through to
    whichever input refused first and record the WRONG reason (S-6's
    ``test_the_reason_code_reaches_the_verdict``).
    """
    closes = series.float_closes
    return IndicatorSeries(
        values=tuple(closes),
        universe=universe,
        not_evaluable_indices=tuple(i for i, value in enumerate(closes) if value is None),
    )


def s7_exit_bracket(
    series: BarSeries,
    *,
    signal_index: int,
    entry_price: Decimal,
    universe: Universe,
) -> tuple[None, Decimal, int]:
    """``(take_profit, stop_loss, max_hold_bars)`` for a fill against S-7's signal bar.

    ⚠⚠ ORDER MATCHES ``s4_exit_bracket`` — TARGET FIRST, THEN STOP — and the
    target slot is ``None`` BY TYPE, not by value: S-7 has no target, and typing
    the slot ``None`` makes assigning one a type error rather than a silent
    strategy change. The triple keeps the sibling factories' shape so a
    positional inversion stays impossible to miss (#2623) — an inverted build
    would put ``None`` in ``stop_loss`` and fail loudly at construction.

    ⚠ ATR is read at ``signal_index``, never at the fill bar. Sizing a stop off
    the fill bar's own range leaks that bar into the decision that produced it.
    ⚠ The stop is ENTRY-anchored (``entry − 2.0 × ATR``), unlike S-5/S-6's
    level-anchored stops — §3 measures S-7's risk from the position, because
    there is no level whose failure defines the trade being wrong.
    """
    atr = atr_series(series, universe=universe, period=ATR_PERIOD)
    atr_at_signal = atr.values[signal_index]
    if atr_at_signal is None:
        raise ValueError(f"S-7 bracket needs ATR at the signal bar; index {signal_index} is unevaluable")
    stop = entry_price - Decimal(str(ATR_STOP_MULTIPLE * atr_at_signal))
    return None, stop, MAX_HOLD_BARS


def s7_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,
) -> list[StrategySignal]:
    """Both legs of S-7 over ``series``: one entry verdict and one exit verdict per bar.

    Returns entries followed by exits — ``signal_ledger.resolve_fills`` keys on
    ``(signal_bar_date, kind)``, so the two legs coexist on one bar (S-3's
    recorded shape). Both legs declare the same four instrument inputs; only the
    entry declares the regime — see the module docstring for both decisions.
    """
    if masked_reason not in NOT_EVALUABLE_REASONS:
        raise ValueError(f"unknown reason code {masked_reason!r}; must be one of {sorted(NOT_EVALUABLE_REASONS)}")
    if len(regime) != len(series):
        raise ValueError(f"regime series has {len(regime)} bars against {len(series)} price bars; they must align")

    closes = series.float_closes
    rsi = rsi_series(series, universe=universe, period=RSI_PERIOD)
    pullback_sma = sma_series(series, universe=universe, period=PULLBACK_SMA_PERIOD)
    trend_sma = sma_series(series, universe=universe, period=TREND_SMA_PERIOD)

    instrument_inputs = (
        StrategyInput(series=_close_input(series, universe=universe), reason=masked_reason),
        StrategyInput(series=rsi, reason=masked_reason),
        StrategyInput(series=pullback_sma, reason=masked_reason),
        StrategyInput(series=trend_sma, reason=masked_reason),
    )
    # ⚠⚠ THE REGIME IS AN INPUT ON THE ENTRY LEG, NOT JUST A GATE IN THE BODY
    # (#2437): a date the benchmark could not classify must be refused as
    # `missing_market_context`, never recorded as a bar the strategy judged and
    # declined. ⚠ DECLARED LAST — `_unevaluable_reason_at` returns the FIRST
    # matching input's reason, and an instrument defect should headline over the
    # market-context fallback. ⚠ ABSENT from the exit leg — module docstring.
    entry_inputs = (
        *instrument_inputs,
        StrategyInput(series=regime, reason="missing_market_context"),
    )

    def entry(index: int) -> bool:
        close = closes[index]
        rsi_now = rsi.values[index]
        pullback_now = pullback_sma.values[index]
        trend_now = trend_sma.values[index]
        # Not reachable through `evaluate`, which refuses the bar first.
        assert close is not None and rsi_now is not None and pullback_now is not None and trend_now is not None
        if not regime.permits(index, PERMITTED_REGIMES):
            return False
        if close <= trend_now or pullback_now <= trend_now:
            return False
        if rsi_now <= RSI_REENTRY_THRESHOLD:
            return False
        # The crossing: prior bar at or below the threshold. An unevaluable
        # prior bar cannot confirm a cross — False, per the module docstring.
        prior_rsi = rsi.values[index - 1] if index > 0 else None
        if prior_rsi is None or prior_rsi > RSI_REENTRY_THRESHOLD:
            return False
        # "Having been below it within 5 bars": a STRICT dip somewhere in the
        # window. Masked bars contribute nothing — a dip must be observed.
        window = rsi.values[max(0, index - RSI_DIP_LOOKBACK_BARS) : index]
        return any(value is not None and value < RSI_REENTRY_THRESHOLD for value in window)

    def exit_(index: int) -> bool:
        close = closes[index]
        pullback_now = pullback_sma.values[index]
        assert close is not None and pullback_now is not None
        return close < pullback_now

    n_bars = len(series)
    return [
        *evaluate(entry, inputs=entry_inputs, n_bars=n_bars, kind="entry"),
        *evaluate(exit_, inputs=instrument_inputs, n_bars=n_bars, kind="exit"),
    ]


__all__ = [
    "ATR_PERIOD",
    "ATR_STOP_MULTIPLE",
    "MAX_HOLD_BARS",
    "PERMITTED_REGIMES",
    "PULLBACK_SMA_PERIOD",
    "RSI_DIP_LOOKBACK_BARS",
    "RSI_PERIOD",
    "RSI_REENTRY_THRESHOLD",
    "S7_PARAMS",
    "S7_STRATEGY_ID",
    "TREND_SMA_PERIOD",
    "WARMUP_BARS",
    "s7_exit_bracket",
    "s7_identity",
    "s7_signals",
]
