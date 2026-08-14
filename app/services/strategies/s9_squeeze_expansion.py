"""S-9 — volatility contraction expansion. Third of the S-5…S-10 set.

Parent spec: ``docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md`` §3 (S-9),
§1 (regime). Registry contract: ``app/services/strategy_registry.py``.
Refs #2437, #2240.

THE RULE, VERBATIM FROM §3
-------------------------
    Setup: BandWidth in **Squeeze** (lowest in 126 bars — §1's published rule).
    Signal: ``close(t) >`` highest close of bars ``t−20 … t−1``, **and** regime ∈
    {``bull_quiet``, ``bull_volatile``}. Exit: stop ``entry − 2.0 × ATR(14)``;
    target ``entry + 3.0 × ATR(14)``; max hold 40 bars. Params: 4. Data: OHLC.

⚠⚠ THIS IS DELIBERATELY ALMOST S-4, AND THAT IS THE ENTIRE POINT.
S-4 lost money on every hold-out year (2022-2026, 0 of 5 against buy-and-hold).
S-9 keeps its breakout leg IDENTICAL — ``close(t) >`` the highest close of the
prior 20 bars, same window, same exclusive boundary — and changes exactly two
things:

1. the compression test becomes Bollinger's PUBLISHED Squeeze (BandWidth lowest
   in 126 bars) instead of S-4's bottom-quartile-of-trailing-100 ATR rank;
2. a regime gate is added.

So if S-9 works where S-4 failed, ONE OF THOSE TWO IS THE REASON, and the pair is
small enough to attribute. That is a controlled comparison, not a sixth guess at
a new rule — which matters when four of four prior strategies failed and the
replication literature expects most to.

⚠ A CONSEQUENCE WORTH STATING: S-9 is NOT expected to be a better S-4 by
construction. It is expected to be an INTERPRETABLE difference from S-4. If both
lose, the shared breakout leg is implicated and the whole family should be
dropped rather than re-tuned — which is a more valuable outcome than another
marginal variant.

⚠ THE SQUEEZE IS THE INSTRUMENT'S OWN, NOT THE MARKET'S. ``market_regime`` reads
the benchmark's BandWidth to classify the market; this reads the candidate's to
find a coiled spring in that name. Same published rule, different series, and
conflating them would mean firing every name whenever the index went quiet.

⚠ NO EXIT SIGNAL LEG, for S-4's reason. Stop, target and max-hold are all
measured from the entry, so all three are position state.
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
from app.services.market_regime import (
    BOLLINGER_NUM_STD,
    BOLLINGER_PERIOD,
    REGIME_RULE_VERSION,
    SQUEEZE_LOOKBACK_BARS,
    Regime,
    RegimeSeries,
    is_squeeze,
)
from app.services.strategy_registry import (
    NOT_EVALUABLE_REASONS,
    NotEvaluableReason,
    StrategyIdentity,
    StrategyInput,
    StrategySignal,
    evaluate,
)

S9_STRATEGY_ID = "s9-squeeze-expansion"

#: ⚠ FIXED, NEVER TUNED.
ATR_PERIOD = 14

#: ⚠⚠ IDENTICAL TO S-4's ``BREAKOUT_LOOKBACK``, and it must stay identical —
#: the whole value of S-9 is that this leg is held constant while the
#: compression test and the regime gate change. Restated here rather than
#: imported from S-4: importing would couple S-9's identity hash to S-4's module
#: bytes, so an unrelated S-4 comment edit would invalidate S-9's track record.
#: The two being equal is asserted by a test, not by a shared symbol.
BREAKOUT_LOOKBACK = 20

#: The bracket, in ATR multiples at the SIGNAL bar. ⚠ Same values as S-4 (2.0 /
#: 3.0 / 40), again to hold everything but the two changed variables constant.
ATR_STOP_MULTIPLE = 2.0
ATR_TARGET_MULTIPLE = 3.0
MAX_HOLD_BARS = 40

#: ⚠ TWO REGIMES. A squeeze resolving upward is worth taking in a quiet bull and
#: in a volatile one; the setup is defined by the instrument's OWN compression,
#: so the market's Bulge is not the disqualifier it is for S-6's level breakout.
PERMITTED_REGIMES = frozenset({Regime.BULL_QUIET, Regime.BULL_VOLATILE})

#: ⚠ §3 says "Params: 4"; criterion 11 hashes everything that makes this a
#: distinct strategy, including the shared regime rule version — the same bars
#: under a different regime boundary produce different signals.
#:
#: ⚠ ``squeeze_lookback_bars`` and the Bollinger constants are declared even
#: though they come from ``market_regime``: they are INPUTS to this rule, and a
#: reader of S-9's identity should see the numbers the rule ran under without
#: chasing another module.
S9_PARAMS: Mapping[str, object] = {
    "atr_period": ATR_PERIOD,
    "breakout_lookback": BREAKOUT_LOOKBACK,
    "squeeze_lookback_bars": SQUEEZE_LOOKBACK_BARS,
    "bollinger_period": BOLLINGER_PERIOD,
    "bollinger_num_std": BOLLINGER_NUM_STD,
    "atr_stop_multiple": ATR_STOP_MULTIPLE,
    "atr_target_multiple": ATR_TARGET_MULTIPLE,
    "max_hold_bars": MAX_HOLD_BARS,
    "permitted_regimes": tuple(sorted(r.value for r in PERMITTED_REGIMES)),
    "regime_rule_version": REGIME_RULE_VERSION,
}

#: DERIVED. The Squeeze needs a full 126-bar BandWidth window, and BandWidth
#: itself needs ``BOLLINGER_PERIOD`` bars, so the first evaluable index is
#: ``BOLLINGER_PERIOD - 1 + SQUEEZE_LOOKBACK_BARS - 1``. The breakout leg is
#: ready at 20 and ATR at 14; the Squeeze binds by a wide margin.
WARMUP_BARS = BOLLINGER_PERIOD + SQUEEZE_LOOKBACK_BARS - 2


def _source_hash() -> str:
    """Hash of THIS module — the ``source_hash`` half of criterion 11."""
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


def s9_identity(*, universe: Universe, cost_model_id: str) -> StrategyIdentity:
    """The registered identity of S-9 on one universe under one cost model."""
    if not cost_model_id.strip():
        raise ValueError(
            "cost_model_id must be a non-empty declaration (criterion 11 hashes it); "
            "pass app.services.cost_model.COST_MODEL_ID rather than an empty string"
        )
    return StrategyIdentity(
        strategy_id=S9_STRATEGY_ID,
        params=S9_PARAMS,
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


def _bands(closes: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """SMA(20) ± 2 POPULATION sigma on the INSTRUMENT's closes.

    ⚠ Population sigma, matching ``indicator_series.bollinger_series`` and
    ``market_regime_provider``. A sample sigma here would put S-9's Squeeze on a
    marginally different band than the regime's, and the two would disagree
    exactly at the extreme, which is the only place either is consulted.
    """
    n = closes.size
    upper = np.full(n, np.nan)
    middle = np.full(n, np.nan)
    lower = np.full(n, np.nan)
    for i in range(BOLLINGER_PERIOD - 1, n):
        window = closes[i - BOLLINGER_PERIOD + 1 : i + 1]
        if not np.all(np.isfinite(window)):
            continue
        mean = float(window.mean())
        sigma = float(window.std())
        middle[i] = mean
        upper[i] = mean + BOLLINGER_NUM_STD * sigma
        lower[i] = mean - BOLLINGER_NUM_STD * sigma
    return upper, middle, lower


def prior_high_close_series(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """Highest close of bars ``t-BREAKOUT_LOOKBACK … t-1`` — EXCLUDING ``t``.

    ⚠⚠ THE WINDOW EXCLUDES ``t``, exactly as S-4's does, and for S-4's stated
    reason: ``close(t) > max(closes including close(t))`` is satisfiable only by
    a tie and is partly self-referential. Holding this leg identical to S-4's is
    the point of S-9, so the boundary is not a detail to re-derive.
    """
    closes = series.float_closes
    values: list[float | None] = []
    for index in range(len(closes)):
        if index < BREAKOUT_LOOKBACK:
            values.append(None)
            continue
        window = closes[index - BREAKOUT_LOOKBACK : index]
        if any(value is None for value in window):
            values.append(None)
            continue
        values.append(max(value for value in window if value is not None))
    return IndicatorSeries(values=tuple(values), universe=universe)


def s9_exit_bracket(
    series: BarSeries,
    *,
    signal_index: int,
    entry_price: Decimal,
    universe: Universe,
) -> tuple[Decimal, Decimal, int]:
    """``(take_profit, stop_loss, max_hold_bars)`` for a fill against S-9's signal bar.

    ⚠ ORDER MATCHES S-4, S-5 AND S-6 — target first. All return
    ``tuple[Decimal, Decimal, int]``, so a mismatched order is invisible to the
    type checker and would invert every bracket (#2623's shape).

    ⚠ BOTH LEGS ANCHOR TO THE ENTRY here, unlike S-5/S-6 whose stops anchor to a
    LEVEL. S-9 has no level — its setup is a volatility state, not a price — so
    there is nothing else to anchor to. Same as S-4, deliberately.
    """
    atr = atr_series(series, universe=universe, period=ATR_PERIOD)
    atr_at_signal = atr.values[signal_index]
    if atr_at_signal is None:
        raise ValueError(f"S-9 bracket needs ATR at the signal bar; index {signal_index} is unevaluable")
    stop = entry_price - Decimal(str(ATR_STOP_MULTIPLE * atr_at_signal))
    target = entry_price + Decimal(str(ATR_TARGET_MULTIPLE * atr_at_signal))
    return target, stop, MAX_HOLD_BARS


def s9_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,
) -> list[StrategySignal]:
    """S-9's entry verdict for every bar. ⚠ ENTRIES ONLY."""
    if masked_reason not in NOT_EVALUABLE_REASONS:
        raise ValueError(f"unknown reason code {masked_reason!r}; must be one of {sorted(NOT_EVALUABLE_REASONS)}")
    if len(regime) != len(series):
        raise ValueError(f"regime series has {len(regime)} bars against {len(series)} price bars; they must align")

    closes = series.float_closes
    atr = atr_series(series, universe=universe, period=ATR_PERIOD)
    prior_high = prior_high_close_series(series, universe=universe)
    upper, middle, lower = _bands(series.array_closes)

    inputs = (
        StrategyInput(series=_close_input(series, universe=universe), reason=masked_reason),
        StrategyInput(series=atr, reason=masked_reason),
        StrategyInput(series=prior_high, reason=masked_reason),
    )

    def entry(index: int) -> bool:
        close = closes[index]
        highest_prior = prior_high.values[index]
        # Not reachable through `evaluate`, which refuses the bar first.
        assert close is not None and highest_prior is not None
        if not regime.permits(index, PERMITTED_REGIMES):
            return False
        if not is_squeeze(upper, middle, lower, index):
            return False
        return close > highest_prior

    return evaluate(entry, inputs=inputs, n_bars=len(series), kind="entry")


__all__ = [
    "ATR_PERIOD",
    "ATR_STOP_MULTIPLE",
    "ATR_TARGET_MULTIPLE",
    "BREAKOUT_LOOKBACK",
    "MAX_HOLD_BARS",
    "PERMITTED_REGIMES",
    "S9_PARAMS",
    "S9_STRATEGY_ID",
    "WARMUP_BARS",
    "prior_high_close_series",
    "s9_exit_bracket",
    "s9_identity",
    "s9_signals",
]
