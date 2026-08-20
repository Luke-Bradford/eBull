"""S-3 — mean reversion within trend. The catalogue's second implemented strategy.

Parent spec: ``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md``
§4 (S-3), §3.5 (execution semantics), §4.0 (validated universe), §5 criteria 4,
8 and 11. Registry contract: ``app/services/strategy_registry.py`` (3a).
Refs #2240, #2288, #2260.

THE RULE, VERBATIM FROM §4
-------------------------
    Signal: ``rsi_14(t) < 30`` and ``close(t) > sma_200(t)`` (reversion inside an
    uptrend, not a falling knife). Exit: ``rsi_14(t) > 50``, or 10 bars elapsed,
    whichever first. Fill at ``open(t+1)``.
    Params: 3.
    Rationale: short-horizon overreaction. The trend filter is what
    distinguishes this from catching a terminal decline.

Comparisons are STRICT on all three, as written. A bar whose RSI is exactly 30,
or whose close sits exactly on ``sma_200``, or whose RSI is exactly 50, fires
nothing — the spec's own wording, not a tie-break this module gets to invent.
``TestStrictComparisons`` pins each of the three with its OWN exact-equality
fixture, because a fixture that is flat pins none of them: relax one operator
and the other conjunct still reads False, so the probe reports NOT CAUGHT.

⚠⚠ THIS STRATEGY IS DELIBERATELY CLOSE TO #2260's UNATTRIBUTED RSI<30 TRIGGER,
AND ITS NUMBERS ARE NOT TO BE TRUSTED UNTIL THAT IS RESOLVED.
§4: *"It is **not** claimed that the trend filter explains that anomaly — #2260
is still unattributed, and S-3's results are not to be trusted until it is. If
anything, S-3 is the test case."* Two further reasons this module ships without
any performance claim attached:

- §4's survivorship table grades S-3's omission bias **highest of the six**:
  *"'Oversold and kept going' is the definition of the missing population, so
  the absent signals are disproportionately the losing ones."* Every signal this
  emits over the current corpus is labelled ``universe = 'survivor_only'``.
- The 76.8% figure #2260 was filed over **does not reproduce causally**
  (2026-08-05): a causal Wilder RSI gives 51.8% on ``price_daily`` and 50.4% on
  the research corpus. This module computes the causal form via
  ``indicator_series.rsi_series``, so it is not expected to reproduce 76.8% and
  reproducing it would be evidence of a bug, not of an edge.

⚠ THIS MODULE NEVER RESOLVES A FILL, AND CANNOT.
A ``StrategySignal`` carries a bar index and no fill field (3a's module
docstring). ``signal_ledger.resolve_fills`` turns the index into ``open(t+1)``.
Nothing below reads bar ``t+1``, and there is no parameter through which it
could ask for one.

⚠⚠ THE "10 BARS ELAPSED" HALF OF THE EXIT IS NOT IN THE SIGNAL STREAM, AND
CANNOT BE. It is measured from the entry it closes, so it is position state, and
a pure per-bar verdict function has none — the same reason S-1's exit leg is
stateless. It is NOT dropped: ``MAX_HOLD_BARS`` is declared below and carried
inside ``S3_PARAMS``, so criterion 11 hashes it into the strategy identity and it
cannot drift away from this rule unnoticed. It is *enforced* by whoever pairs
entries with exits — ``outcome_resolver.ExitLevels.max_hold_bars`` (phase 4a) is
the existing field for it, and phase 5 is what calls it.

⚠ THE ``rsi_14 > 50`` LEG IS STATELESS, DELIBERATELY, exactly as S-1's is.
It fires on every such bar whether or not an entry is open, because the ledger
records DECISIONS, not positions — §7: *"Every fired signal is recorded whether
or not it was acted on."*

⚠ A MASKED CLOSE REFUSES THE WHOLE TAIL OF THE SERIES, NOT A 200-BAR WINDOW.
This is S-3's one structural difference from S-1 and it is a property of RSI,
not a choice made here: Wilder smoothing carries state forward, so
``rsi_series`` marks every index from the first NULL close onward unevaluable
rather than resuming after the gap. ``sma_series`` recovers once its window
clears the hole. The consequence is that S-3's refusal footprint on masked bars
is far larger than S-1's, which is a bias worth counting rather than asserting
away — ``scripts/verify_2240_s3_mean_reversion.py --census`` reports it over the
validated universe.

⚠ WHAT THIS MODULE DOES NOT GUARD, INHERITED FROM ``indicator_series``.
Quarantine and adjustment basis are the CALLER's gate. There is no database
access here, so bars arrive however the caller loaded them; ``close_reason``
exists because only the caller knows why a close is missing.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path

from app.services.indicator_series import (
    BarSeries,
    IndicatorSeries,
    Universe,
    rsi_series,
    sma_series,
)
from app.services.strategy_registry import (
    NOT_EVALUABLE_REASONS,
    NotEvaluableReason,
    StrategyIdentity,
    StrategyInput,
    StrategySignal,
    evaluate,
)

S3_STRATEGY_ID = "s3-mean-reversion-in-trend"

#: ⚠ FIXED, NEVER TUNED (§6: *"Forbidden — continuous re-optimisation"*). Module
#: constants rather than function arguments for the reason S-1 gives: a
#: threshold that can be passed in is a threshold that can be swept, and
#: criterion 11 would then need every sweep value registered as its own
#: strategy. Changing any of them moves ``_source_hash`` AND ``S3_PARAMS``, so
#: the identity moves and the prior track record is not inherited.
RSI_PERIOD = 14
OVERSOLD_THRESHOLD = 30.0
EXIT_THRESHOLD = 50.0
TREND_PERIOD = 200

#: The other half of §4's exit, in bars from the fill. ⚠ Declared here and
#: hashed into the identity, but NOT evaluated by ``s3_signals`` — see the module
#: docstring. Its consumer is ``outcome_resolver.ExitLevels.max_hold_bars``.
MAX_HOLD_BARS = 10

#: ⚠ §4 says *"Params: 3"* and this dict carries FIVE entries. The discrepancy is
#: recorded rather than resolved by dropping two: §4's count is of *free*
#: parameters (the two RSI thresholds and the hold cap — the numbers a sweep
#: would move), while criterion 11 hashes *everything that makes this a distinct
#: strategy*. ``rsi_period`` and ``trend_period`` are inherited conventions
#: (Wilder's default, and S-1's own slow lookback) rather than free choices, but
#: an S-3 computed on ``sma_100`` is a different strategy and must not silently
#: inherit this one's track record.
S3_PARAMS: Mapping[str, object] = {
    "rsi_period": RSI_PERIOD,
    "oversold_threshold": OVERSOLD_THRESHOLD,
    "exit_threshold": EXIT_THRESHOLD,
    "trend_period": TREND_PERIOD,
    "max_hold_bars": MAX_HOLD_BARS,
}

#: ⚠ §4 gives S-3 no explicit bar requirement the way it gives S-1 *"Needs >=200
#: bars"*, but the rule names ``sma_200``, so the requirement is the same 200 and
#: it is derived from the rule rather than restated as an independent number.
#: NOT enforced by a length check here — ``sma_series`` emits ``None`` until its
#: window is full and 3a's runner turns that into ``insufficient_warmup``. An
#: explicit ``len(series) < 200 -> refuse`` would be a second, weaker copy of the
#: same rule: it would pass a 250-bar series whose bar 210 still sits inside the
#: window of a NULL close.
WARMUP_BARS = TREND_PERIOD


def _source_hash() -> str:
    """Hash of THIS module — the ``source_hash`` half of criterion 11.

    ``StrategyIdentity.version`` mixes this with the params, the universe, the
    cost-model id and the registry's own source. Same construction as
    ``indicator_series.RULE_SET_VERSION``, and the same deliberate
    over-invalidation: editing a comment here moves the version, which makes
    previously stored signals visibly stale rather than silently mixed.
    """
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


def s3_identity(*, universe: Universe, cost_model_id: str) -> StrategyIdentity:
    """The registered identity of S-3 on one universe under one cost model.

    ⚠ Both arguments are REQUIRED and neither has a default, for the reason S-1
    states: criterion 11 makes universe and cost model part of the identity, so a
    default would silently register a strategy the caller never declared.
    ``cost_model_id`` is rejected when blank — ``str`` does not distinguish "not
    supplied" from ``""``, and a present-but-empty declaration is the #2286 shape.
    """
    if not cost_model_id.strip():
        raise ValueError(
            "cost_model_id must be a non-empty declaration (criterion 11 hashes it); "
            "pass app.services.cost_model.COST_MODEL_ID rather than an empty string"
        )
    return StrategyIdentity(
        strategy_id=S3_STRATEGY_ID,
        params=S3_PARAMS,
        universe=universe,
        cost_model_id=cost_model_id,
        source_hash=_source_hash(),
    )


def _close_input(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """The bar closes, in the shape 3a's runner checks for evaluability.

    ⚠ The close is DECLARED, not relied upon transitively — S-1's reasoning
    applies unchanged. A NULL close at bar ``i`` already makes ``sma_200(i)`` and
    ``rsi_14(i)`` unevaluable, so declaring it changes no verdict today. It is
    declared anyway because the entry rule READS it, and an undeclared input is a
    guard that exists only as a property of a different module.
    """
    closes = series.float_closes
    return IndicatorSeries(
        values=tuple(closes),
        universe=universe,
        not_evaluable_indices=tuple(i for i, value in enumerate(closes) if value is None),
    )


def s3_signals(
    series: BarSeries,
    *,
    universe: Universe,
    close_reason: NotEvaluableReason,
) -> list[StrategySignal]:
    """Both legs of S-3 over ``series``: one entry verdict and one exit verdict per bar.

    Returns entries followed by exits. ``signal_ledger.resolve_fills`` keys on
    ``(signal_bar_date, kind)``, so the two legs coexist on one bar — which is
    why ``signal_kind`` is in the ledger's uniqueness key (3-phase spec §2.1).

    ⚠⚠ BOTH LEGS DECLARE THE SAME THREE INPUTS, INCLUDING THE ``sma_200`` AND THE
    ``close`` THE EXIT DOES NOT READ. Settled by S-1 and applied here for the same
    reason, which bites harder on S-3: §3.1 makes evaluability a property of the
    STRATEGY, decided before any condition runs, precisely so the shape of the
    condition cannot change the verdict. The exit reads only ``rsi_14``, warm from
    bar 14; the entry additionally needs ``sma_200``, warm from bar 199. Declaring
    per-leg input sets would make bars 14..198 live for the exit and
    ``insufficient_warmup`` for the entry — a strategy that is half-live, on a
    rule whose data requirement is a single 200 bars.

    ⚠ It is a NARROWING, and narrowings get counted rather than asserted safe:
    the bars it moves from a computable exit verdict to ``insufficient_warmup``
    are exactly indices ``RSI_PERIOD .. TREND_PERIOD - 2`` of every series long
    enough to reach them — 185 bars, against S-1's 150.
    ``scripts/verify_2240_s3_mean_reversion.py --census`` reports that count on
    the full population rather than leaving it inferred.

    ``close_reason`` is the code recorded when a close is missing, and it comes
    from the caller because only the caller knows why: bars from
    ``load_masked_series`` are missing because the quarantine masked them
    (``quarantined_bar``), and a different loader would owe a different code.
    """
    if close_reason not in NOT_EVALUABLE_REASONS:
        raise ValueError(f"unknown reason code {close_reason!r}; must be one of {sorted(NOT_EVALUABLE_REASONS)}")

    closes = series.float_closes
    rsi = rsi_series(series, universe=universe, period=RSI_PERIOD)
    trend = sma_series(series, universe=universe, period=TREND_PERIOD)

    inputs = (
        StrategyInput(series=_close_input(series, universe=universe), reason=close_reason),
        StrategyInput(series=rsi, reason=close_reason),
        StrategyInput(series=trend, reason=close_reason),
    )

    def entry(index: int) -> bool:
        close, rsi_value, trend_value = closes[index], rsi.values[index], trend.values[index]
        # Not reachable through `evaluate`, which refuses the bar first. Present
        # to narrow the types and to fail loudly for a direct caller.
        assert close is not None and rsi_value is not None and trend_value is not None
        return rsi_value < OVERSOLD_THRESHOLD and close > trend_value

    def exit_(index: int) -> bool:
        rsi_value = rsi.values[index]
        assert rsi_value is not None
        return rsi_value > EXIT_THRESHOLD

    n_bars = len(series)
    return [
        *evaluate(entry, inputs=inputs, n_bars=n_bars, kind="entry"),
        *evaluate(exit_, inputs=inputs, n_bars=n_bars, kind="exit"),
    ]


__all__ = [
    "EXIT_THRESHOLD",
    "MAX_HOLD_BARS",
    "OVERSOLD_THRESHOLD",
    "RSI_PERIOD",
    "S3_PARAMS",
    "S3_STRATEGY_ID",
    "TREND_PERIOD",
    "WARMUP_BARS",
    "s3_identity",
    "s3_signals",
]
