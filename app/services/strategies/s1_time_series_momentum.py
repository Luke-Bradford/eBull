"""S-1 — time-series momentum (trend). The first catalogue strategy.

Parent spec: ``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md``
§4 (S-1), §3.5 (execution semantics), §4.0 (validated universe), §5 criteria 4,
8 and 11. Registry contract: ``app/services/strategy_registry.py`` (3a).
Refs #2240, #2288.

THE RULE, VERBATIM FROM §4
-------------------------
    Signal: ``close(t) > sma_200(t)`` and ``sma_50(t) > sma_200(t)``.
    Exit signal: ``close(t) < sma_50(t)``. Fill both at ``open(t+1)`` per §3.5.
    Params: 2 (the two lookbacks — fixed, never tuned).
    Rationale: persistence of returns at 3-12 month horizons; the CTA base case.
    Data: close-only. Needs >=200 bars as-of the decision date.

Comparisons are STRICT on both legs, as written. A bar where the close sits
exactly on ``sma_50`` fires neither, which is the spec's own wording and not a
tie-break this module gets to invent.

⚠ THIS MODULE NEVER RESOLVES A FILL, AND CANNOT.
A ``StrategySignal`` carries a bar index and no fill field (3a's module
docstring). ``signal_ledger.resolve_fills`` turns the index into
``open(t+1)``. Nothing below reads bar ``t+1``, and there is no parameter
through which it could ask for one.

⚠ THE EXIT LEG IS STATELESS, DELIBERATELY.
``close(t) < sma_50(t)`` fires on every such bar, whether or not an entry is
open. The ledger records DECISIONS, not positions — §7: *"Every fired signal is
recorded whether or not it was acted on … Only recording taken trades biases the
record toward periods of spare capacity."* Pairing an exit with the entry it
closes is the backtester's job (phase 5), and doing it here would need position
state, which would stop this being a pure function of the bars.

⚠ WHAT THIS MODULE DOES NOT GUARD, INHERITED FROM ``indicator_series``.
Quarantine and adjustment basis are the CALLER's gate. There is no database
access here, so bars arrive however the caller loaded them; ``close_reason``
exists because only the caller knows why a close is missing.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path

from app.services.indicator_series import BarSeries, IndicatorSeries, Universe, sma_series
from app.services.strategy_registry import (
    NOT_EVALUABLE_REASONS,
    NotEvaluableReason,
    StrategyIdentity,
    StrategyInput,
    StrategySignal,
    evaluate,
)

S1_STRATEGY_ID = "s1-time-series-momentum"

#: ⚠ FIXED, NEVER TUNED (§4: *"the two lookbacks — fixed, never tuned"*, and §6:
#: *"Forbidden — continuous re-optimisation"*). They are module constants rather
#: than function arguments on purpose: a period that can be passed in is a
#: period that can be swept, and criterion 11 would then need every sweep value
#: registered as its own strategy. Changing either constant changes
#: ``_source_hash`` AND ``S1_PARAMS``, so the identity moves and the prior track
#: record is not inherited.
FAST_PERIOD = 50
SLOW_PERIOD = 200

#: ⚠ §4's *"Needs >=200 bars as-of the decision date"*, and it is NOT enforced by
#: a length check here — ``sma_series`` emits ``None`` until its window is full,
#: and 3a's runner turns that into ``insufficient_warmup``. An explicit
#: ``len(series) < 200 -> refuse`` would be a second, weaker copy of the same
#: rule: it would pass a 250-bar series whose bar 210 still sits inside the
#: window of a NULL close.
WARMUP_BARS = SLOW_PERIOD

S1_PARAMS: Mapping[str, object] = {"fast_period": FAST_PERIOD, "slow_period": SLOW_PERIOD}


def _source_hash() -> str:
    """Hash of THIS module — the ``source_hash`` half of criterion 11.

    ``StrategyIdentity.version`` mixes this with the params, the universe, the
    cost-model id and the registry's own source. Same construction as
    ``indicator_series.RULE_SET_VERSION``, and the same deliberate
    over-invalidation: editing a comment here moves the version, which makes
    previously stored signals visibly stale rather than silently mixed.
    """
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


def s1_identity(*, universe: Universe, cost_model_id: str) -> StrategyIdentity:
    """The registered identity of S-1 on one universe under one cost model.

    ⚠ Both arguments are REQUIRED and neither has a default. Criterion 11 makes
    universe and cost model part of the identity — *"'S-1 on US stocks' and
    'S-1 on eu_equity' are two strategies and always were"* — so a default would
    silently register a strategy the caller never declared.

    ⚠ ``cost_model_id`` is rejected when blank. ``str`` does not distinguish
    "not supplied" from ``""``, and a present-but-empty declaration is the #2286
    shape: it passes every type check and records nothing.

    ⚠ The model now EXISTS — ``app.services.cost_model.COST_MODEL_ID``, stage 5b
    — so the placeholder this argument used to carry is gone and every identity
    moved when it landed, exactly as the earlier note here predicted. It stays a
    caller argument rather than becoming a module constant because §5.1 makes a
    recalibration a NEW id, and stage 5e's sensitivity arms have to be able to
    evaluate the same strategy under two of them in one process.
    """
    if not cost_model_id.strip():
        raise ValueError(
            "cost_model_id must be a non-empty declaration (criterion 11 hashes it); "
            "pass app.services.cost_model.COST_MODEL_ID rather than an empty string"
        )
    return StrategyIdentity(
        strategy_id=S1_STRATEGY_ID,
        params=S1_PARAMS,
        universe=universe,
        cost_model_id=cost_model_id,
        source_hash=_source_hash(),
    )


def _close_input(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """The bar closes, in the shape 3a's runner checks for evaluability.

    ⚠ The close is DECLARED, not relied upon transitively. A NULL close at bar
    ``i`` already makes ``sma_50(i)`` and ``sma_200(i)`` unevaluable, because
    ``sma_series``' window contains ``i`` — so declaring it changes no verdict
    today. It is declared anyway because the strategy READS it: an undeclared
    input is a guard that exists only as a property of a different module, and
    the day ``sma_series`` learns to skip NULLs, this rule would start comparing
    a missing close against a real average with nothing to say so.

    ``IndicatorSeries`` is the right carrier rather than a bare list because its
    contract is exactly what is needed here — ``len(values) == len(bars)``, and
    ``not_evaluable_indices`` separating "no value from this input" from
    warm-up, which a raw close has none of.
    """
    closes = series.float_closes
    return IndicatorSeries(
        values=tuple(closes),
        universe=universe,
        not_evaluable_indices=tuple(i for i, value in enumerate(closes) if value is None),
    )


def s1_signals(
    series: BarSeries,
    *,
    universe: Universe,
    close_reason: NotEvaluableReason,
) -> list[StrategySignal]:
    """Both legs of S-1 over ``series``: one entry verdict and one exit verdict per bar.

    Returns entries followed by exits. ``signal_ledger.resolve_fills`` keys on
    ``(signal_bar_date, kind)``, so the two legs coexist on one bar — which is
    why ``signal_kind`` is in the ledger's uniqueness key (spec §2.1: a strategy
    exiting one position and entering another on the same bar is legitimate).

    ⚠⚠ BOTH LEGS DECLARE THE SAME THREE INPUTS, INCLUDING THE ``sma_200`` THE
    EXIT DOES NOT READ. This is the one judgement call in the module.

    §3.1 makes evaluability a property of the STRATEGY, decided before any
    condition runs, precisely so the shape of the condition cannot change the
    verdict. Declaring per-leg input sets would reintroduce that by the back
    door one level up: bars 49..198 have a warm ``sma_50`` and a cold
    ``sma_200``, so a per-leg declaration would make the same bar evaluable for
    the exit and ``insufficient_warmup`` for the entry — a strategy that is
    half-live, on a rule §4 gives a single data requirement for (*"Needs >=200
    bars"*).

    ⚠ It is a NARROWING, and narrowings get counted rather than asserted safe:
    the bars it moves from a computable exit verdict to ``insufficient_warmup``
    are exactly indices ``FAST_PERIOD - 1 .. SLOW_PERIOD - 2`` of every series
    long enough to reach them. ``scripts/verify_2240_s1_momentum.py`` reports
    that count on the full population rather than leaving it inferred.

    ``close_reason`` is the code recorded when a close is missing, and it comes
    from the caller because only the caller knows why: bars from
    ``load_masked_series`` are missing because the quarantine masked them
    (``quarantined_bar``), and a different loader would owe a different code.
    3a's ``StrategyInput`` exists for exactly this — *"the knowledge is supplied
    where it exists rather than guessed where it does not"*.
    """
    if close_reason not in NOT_EVALUABLE_REASONS:
        raise ValueError(f"unknown reason code {close_reason!r}; must be one of {sorted(NOT_EVALUABLE_REASONS)}")

    closes = series.float_closes
    fast = sma_series(series, universe=universe, period=FAST_PERIOD)
    slow = sma_series(series, universe=universe, period=SLOW_PERIOD)

    inputs = (
        StrategyInput(series=_close_input(series, universe=universe), reason=close_reason),
        StrategyInput(series=fast, reason=close_reason),
        StrategyInput(series=slow, reason=close_reason),
    )

    def entry(index: int) -> bool:
        close, fast_value, slow_value = closes[index], fast.values[index], slow.values[index]
        # Not reachable through `evaluate`, which refuses the bar first. Present
        # to narrow the types and to fail loudly for a direct caller.
        assert close is not None and fast_value is not None and slow_value is not None
        return close > slow_value and fast_value > slow_value

    def exit_(index: int) -> bool:
        close, fast_value = closes[index], fast.values[index]
        assert close is not None and fast_value is not None
        return close < fast_value

    n_bars = len(series)
    return [
        *evaluate(entry, inputs=inputs, n_bars=n_bars, kind="entry"),
        *evaluate(exit_, inputs=inputs, n_bars=n_bars, kind="exit"),
    ]


__all__ = [
    "FAST_PERIOD",
    "S1_PARAMS",
    "S1_STRATEGY_ID",
    "SLOW_PERIOD",
    "WARMUP_BARS",
    "s1_identity",
    "s1_signals",
]
