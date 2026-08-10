"""S-4 — volatility compression breakout. The catalogue's third implemented strategy.

Parent spec: ``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md``
§4 (S-4), §3.5 (execution semantics), §4.0 (validated universe), §5 criteria 4,
8 and 11. Registry contract: ``app/services/strategy_registry.py`` (3a).
Refs #2240, #2288.

THE RULE, VERBATIM FROM §4
-------------------------
    Setup: ``atr_14(t)`` sits in the bottom quartile of its own trailing
    100-bar distribution, **computed on bars <= t**. Signal: ``close(t) >`` the
    highest close of bars ``t-20 .. t-1`` (**prior** 20 bars, excluding *t*
    itself — including it makes the condition partly self-referential). Fill at
    ``open(t+1)``. Exit: stop at ``entry - 2 x atr_14(t)``, profit target at
    ``entry + 3 x atr_14(t)``, hard max-hold 40 bars — whichever comes first.
    ⚠ ATR is indexed at **``t``, the signal bar**, never at ``t+1``: sizing a
    stop off the fill bar's own range leaks that bar into the decision that
    produced it. Levels are fixed at signal time and do not move. If one bar
    spans both stop and target, the outcome is ``ambiguous`` per §3.5 rule 4.
    Params: 3 (compression window, breakout lookback, ATR stop multiple).
    Data: OHLC only, but requires **complete** OHLC.

⚠⚠ THE TWO LEGS' WINDOW BOUNDARIES ARE DELIBERATELY ASYMMETRIC, AND MAKING THEM
CONSISTENT WOULD BE A SPEC VIOLATION. Compression is *"computed on bars <= t"* —
the window INCLUDES ``t``, because the question is where today's ATR sits in its
own recent distribution, and excluding today would ask about a distribution
today is not in. The breakout window EXCLUDES ``t``, and §4 says why in its own
parenthesis: ``close(t) > max(closes including close(t))`` is satisfiable only by
a tie and is *"partly self-referential"*. Two windows, two boundaries, one
sentence of spec each. ``TestWindowBoundaries`` pins both.

⚠⚠ THIS STRATEGY HAS NO EXIT SIGNAL LEG, AND CANNOT HAVE ONE.
S-1 and S-3 each exit on a per-bar price condition, so their exits are signals.
All three of S-4's exit conditions — stop, target, max-hold — are measured FROM
THE ENTRY, so all three are position state, and a pure per-bar verdict function
has none. This is S-3's ``MAX_HOLD_BARS`` reasoning applied to the whole exit
rather than to one half of it. The parameters are NOT dropped: ``ATR_STOP_MULTIPLE``,
``ATR_TARGET_MULTIPLE`` and ``MAX_HOLD_BARS`` are carried in ``S4_PARAMS``, so
criterion 11 hashes them into the identity, and their consumer is
``s4_exit_bracket`` below, which returns the strategy-owned values used to
construct the outcome runner's version-pinned level object.

⚠ THE ATR THAT SIZES THE BRACKET IS INDEXED AT THE SIGNAL BAR, AND THIS MODULE
IS THE ONE PLACE ALLOWED TO EMIT IT. A ``StrategySignal`` carries a bar index
and nothing else (3a's module docstring), so ``s4_exit_bracket`` recomputes
``atr_series`` and reads ``signal_index`` — never
``signal_index + 1``. §4 flags this as the exact trap ("sizing a stop off the fill
bar's own range leaks that bar into the decision that produced it") and it was one
of the five findings Codex's second spec round caught. The factory's API shape
is what prevents it, in the same way the absent fill field prevents a same-bar fill.

⚠ "BOTTOM QUARTILE" HAS NO PUBLISHED FORMULATION AND IS THEREFORE FIXED BY
CONSTRUCTION HERE, NOT INVENTED SILENTLY.
`.claude/CLAUDE.md`: *"Where a published formulation genuinely does NOT exist …
say so explicitly and fix the rule by construction, freezing the constants in a
version hash — do not invent a citation and do not leave the choice implicit."*
Bollinger's Squeeze has a published rule (BandWidth at its lowest in six months);
"ATR in the bottom quartile of its own trailing 100 bars" does not — it is this
spec's own construction, and §4 states the window and the quartile but not the
membership test. Sample quantiles are not one thing: NumPy ships nine
interpolation methods, and picking one would be an unstated constant doing real
work at the boundary. So membership is defined by RANK, which has no
interpolation and no free parameter:

    compression(t) := #{w in W : w < atr_14(t)} / |W|,  W = atr_14[t-99 .. t]
    the setup holds iff  compression(t) < 0.25

i.e. **the empirical CDF's left limit at today's ATR** — the fraction of the
window strictly below it. With ``|W| = 100`` and no ties, today's ATR is the
k-th smallest and ``compression = (k-1)/100``, so the setup holds for exactly
k <= 25: the bottom 25 of 100, which is what "bottom quartile" has to mean for
the count to come out right. ``TestCompressionRankRule`` pins that boundary at
k=25 and k=26.

⚠ Ties are resolved FAVOURABLY, and that is forced rather than chosen. Counting
``w <= atr(t)`` instead would make the verdict depend on how many equal values
happen to sit in the window, so two bars with IDENTICAL ATRs in the SAME window
could get different verdicts according to their index — a rule that reads
arbitrary order as signal. Counting strictly-below gives every tied value the
same fraction and therefore the same verdict. ``TestCompressionRankRule`` pins it.

⚠ The degenerate consequence, stated rather than left to be discovered: on a
window whose 100 ATRs are ALL equal, ``compression = 0.0`` and the setup holds on
every bar. A point-mass distribution has no quartiles, so no membership rule is
"right" there; this one answers "maximally compressed", which is the reading that
matches the strategy's intent. It is defused by the conjunction rather than by a
special case — the breakout leg needs ``close(t)`` STRICTLY above 20 prior
closes, which a series flat enough to hold ATR exactly constant cannot produce.
``test_a_flat_series_never_fires`` is that claim, asserted rather than argued.

⚠ A MASKED BAR REFUSES THE WHOLE TAIL OF THE SERIES, exactly as S-3's RSI does
and for the same reason: ``atr_series`` is Wilder-smoothed from the series start,
so it carries state across every bar and has no window for a hole to clear. S-4
inherits S-3's blast radius, not S-1's. Counted rather than asserted away —
``scripts/verify_2240_s4_volatility_breakout.py --census`` reports it over the
validated universe.

⚠ §4 REQUIRES COMPLETE OHLC, WHICH IS WIDER THAN S-1/S-3's CLOSE-ONLY NEED.
*"instruments with NULL high/low and any bar inside a ``price_series_break``
segment are ``not_evaluable``, not absent."* The high/low half is enforced here
transitively and correctly: ``atr_series`` refuses on a NULL high, a NULL low or
a NULL previous close, so a bar with a good close and a missing high is refused
rather than judged. The ``price_series_break`` half is the CALLER's, and
deliberately so — this module has no database access. The bounded runner splits
S-4 into independent segments, making the final pre-break bar unfillable and
restarting ATR warm-up at the new scale; the outcome resolver separately refuses
an open position that would cross the next boundary.

⚠ WHAT THIS MODULE DOES NOT GUARD, INHERITED FROM ``indicator_series``.
Quarantine and adjustment basis are the CALLER's gate. There is no database
access here, so bars arrive however the caller loaded them.
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
)
from app.services.strategy_registry import (
    NOT_EVALUABLE_REASONS,
    NotEvaluableReason,
    StrategyIdentity,
    StrategyInput,
    StrategySignal,
    evaluate,
)

S4_STRATEGY_ID = "s4-volatility-compression-breakout"

#: ⚠ FIXED, NEVER TUNED (§6: *"Forbidden — continuous re-optimisation"*). Module
#: constants rather than function arguments, for the reason S-1 and S-3 both
#: give: a threshold that can be passed in is a threshold that can be swept, and
#: criterion 11 would then need every sweep value registered as its own strategy.
#: Changing any of them moves ``_source_hash`` AND ``S4_PARAMS``, so the identity
#: moves and the prior track record is not inherited.
ATR_PERIOD = 14
COMPRESSION_WINDOW = 100
#: The "quartile" of §4's *"bottom quartile"*, as a rank fraction — see the
#: module docstring for why membership is a rank rather than an interpolated
#: sample quantile.
COMPRESSION_QUANTILE = 0.25
BREAKOUT_LOOKBACK = 20

#: The bracket, in ATR multiples at the SIGNAL bar. ⚠ Declared here and hashed
#: into the identity, but NOT evaluated by ``s4_signals`` — see the module
#: docstring. Their consumer is ``outcome_resolver.ExitLevels``.
ATR_STOP_MULTIPLE = 2.0
ATR_TARGET_MULTIPLE = 3.0
MAX_HOLD_BARS = 40

#: ⚠ §4 says *"Params: 3 (compression window, breakout lookback, ATR stop
#: multiple)"* and this dict carries SEVEN entries. Recorded rather than resolved
#: by dropping four, exactly as S-3 records the same discrepancy: §4 counts
#: *free* parameters — the numbers a sweep would move — while criterion 11 hashes
#: *everything that makes this a distinct strategy*. ``atr_period`` is Wilder's
#: own default and ``compression_quantile`` is fixed by the word "quartile", but
#: an S-4 computed on ``atr_20``, or on a tercile, is a different strategy and
#: must not silently inherit this one's track record. The two exit multiples are
#: here for the stronger reason that NOTHING IN THIS MODULE READS THEM: the
#: identity hash is the only thing keeping them attached to the rule.
S4_PARAMS: Mapping[str, object] = {
    "atr_period": ATR_PERIOD,
    "compression_window": COMPRESSION_WINDOW,
    "compression_quantile": COMPRESSION_QUANTILE,
    "breakout_lookback": BREAKOUT_LOOKBACK,
    "atr_stop_multiple": ATR_STOP_MULTIPLE,
    "atr_target_multiple": ATR_TARGET_MULTIPLE,
    "max_hold_bars": MAX_HOLD_BARS,
}

#: The first index at which BOTH legs can be evaluated, DERIVED from the rule
#: rather than restated as an independent number.
#:
#: ``atr_series`` emits its first value at index ``ATR_PERIOD`` (Wilder seeds on
#: the mean of true ranges 1..period). A compression window of 100 ending at
#: ``i`` reaches back to ``i - 99``, so it is full only from
#: ``ATR_PERIOD + COMPRESSION_WINDOW - 1 = 113``. The breakout leg needs 20 prior
#: closes and is ready at index 20. 113 binds.
#:
#: NOT enforced by a length check — the series above emit ``None`` until their
#: windows are full and 3a's runner turns that into ``insufficient_warmup``. An
#: explicit ``len(series) < 113 -> refuse`` would be a second, weaker copy of the
#: same rule: it would pass a 200-bar series whose bar 150 still sits inside the
#: tail an early masked bar refused.
WARMUP_BARS = ATR_PERIOD + COMPRESSION_WINDOW - 1


def _source_hash() -> str:
    """Hash of THIS module — the ``source_hash`` half of criterion 11.

    ``StrategyIdentity.version`` mixes this with the params, the universe, the
    cost-model id and the registry's own source. Same construction as
    ``indicator_series.RULE_SET_VERSION``, and the same deliberate
    over-invalidation: editing a comment here moves the version, which makes
    previously stored signals visibly stale rather than silently mixed.
    """
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


def s4_identity(*, universe: Universe, cost_model_id: str) -> StrategyIdentity:
    """The registered identity of S-4 on one universe under one cost model.

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
        strategy_id=S4_STRATEGY_ID,
        params=S4_PARAMS,
        universe=universe,
        cost_model_id=cost_model_id,
        source_hash=_source_hash(),
    )


def s4_exit_bracket(
    series: BarSeries,
    *,
    signal_index: int,
    entry_price: Decimal,
    universe: Universe,
) -> tuple[Decimal, Decimal, int]:
    """Build S-4's fixed bracket from ATR on the causal signal bar.

    The entry is the next bar's open, but the distance is deliberately read at
    ``signal_index``. Recomputing ATR at the fill index would let the fill bar's
    range alter levels that had to exist before that bar was observed.
    """
    if not 0 <= signal_index < len(series):
        raise ValueError(f"signal_index {signal_index} is outside the {len(series)}-bar series")
    if entry_price <= 0:
        raise ValueError(f"entry_price must be positive, got {entry_price}")
    atr = atr_series(series, period=ATR_PERIOD, universe=universe).values[signal_index]
    if atr is None or atr <= 0:
        raise ValueError(f"ATR{ATR_PERIOD} is unavailable or non-positive at signal index {signal_index}")
    distance = Decimal(str(atr))
    stop = entry_price - Decimal(str(ATR_STOP_MULTIPLE)) * distance
    target = entry_price + Decimal(str(ATR_TARGET_MULTIPLE)) * distance
    if stop <= 0:
        raise ValueError(
            f"S-4's 2xATR stop {stop} is non-positive for entry {entry_price}; the bracket is not broker-orderable"
        )
    return target, stop, MAX_HOLD_BARS


def _close_input(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """The bar closes, in the shape 3a's runner checks for evaluability.

    ⚠ The close is DECLARED, not relied upon transitively — S-1's and S-3's
    reasoning applies unchanged. A NULL close at bar ``i`` already makes
    ``atr_14(i)`` unevaluable, so declaring it changes no verdict today. It is
    declared anyway because the breakout rule READS ``close(t)``, and an
    undeclared input is a guard that exists only as a property of a different
    module.
    """
    closes = series.float_closes
    return IndicatorSeries(
        values=tuple(closes),
        universe=universe,
        not_evaluable_indices=tuple(i for i, value in enumerate(closes) if value is None),
    )


def compression_rank_series(atr: IndicatorSeries, *, universe: Universe) -> IndicatorSeries:
    """Fraction of the trailing ``COMPRESSION_WINDOW`` ATRs strictly below ATR(t).

    The empirical CDF's left limit, per the module docstring. The COMPARISON
    against ``COMPRESSION_QUANTILE`` deliberately stays in ``s4_signals``'s body
    rather than being folded in here: this series carries a measured quantity, so
    a probe that relaxes the threshold has something to relax, and ``--census``
    has a distribution to report rather than a boolean.

    ⚠ The window INCLUDES ``t`` (§4: *"computed on bars <= t"*), so ``atr(t)`` is
    always one of the values it is ranked against and the fraction can never
    reach 1.0.

    Refusal is split the way 3a requires. An index whose window overlaps ATR's
    own unevaluable set is a DATA refusal and is listed in
    ``not_evaluable_indices``; an index whose window merely reaches back into
    ATR's warm-up is left as a bare ``None``, which 3a reads as
    ``insufficient_warmup``. Collapsing the two would destroy exactly what
    criterion 8 exists for.
    """
    n = len(atr)
    values: list[float | None] = [None] * n
    unevaluable: list[int] = []
    atr_bad = set(atr.not_evaluable_indices)

    for index in range(n):
        low = index - COMPRESSION_WINDOW + 1
        if low < 0:
            continue  # fewer than COMPRESSION_WINDOW bars exist yet — warm-up.
        if any(j in atr_bad for j in range(low, index + 1)):
            unevaluable.append(index)
            continue
        window = atr.values[low : index + 1]
        if any(value is None for value in window):
            continue  # the window reaches into ATR's warm-up — still warm-up.
        current = atr.values[index]
        assert current is not None  # implied by the window check above.
        below = sum(1 for value in window if value is not None and value < current)
        values[index] = below / len(window)

    return IndicatorSeries(tuple(values), universe, tuple(unevaluable))


def prior_high_close_series(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """Highest close of the ``BREAKOUT_LOOKBACK`` bars STRICTLY BEFORE ``t``.

    ⚠ ``t`` IS EXCLUDED. §4: *"the highest close of bars ``t-20 .. t-1``
    (**prior** 20 bars, excluding *t* itself — including it makes the condition
    partly self-referential)"*. Including it would leave ``close(t) > max(...)``
    satisfiable only by a tie, i.e. never, under a strict comparison — the rule
    would silently never fire rather than fail.

    A masked close anywhere in the window is a DATA refusal, not warm-up: the
    field is present and empty, so the window genuinely cannot support a maximum.
    Indices with fewer than ``BREAKOUT_LOOKBACK`` bars behind them are warm-up.
    """
    closes = series.float_closes
    n = len(closes)
    values: list[float | None] = [None] * n
    unevaluable: list[int] = []

    for index in range(n):
        low = index - BREAKOUT_LOOKBACK
        if low < 0:
            continue  # fewer than BREAKOUT_LOOKBACK prior bars — warm-up.
        window = closes[low:index]  # ⚠ excludes `index` itself.
        if any(value is None for value in window):
            unevaluable.append(index)
            continue
        values[index] = max(value for value in window if value is not None)

    return IndicatorSeries(tuple(values), universe, tuple(unevaluable))


def s4_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
) -> list[StrategySignal]:
    """S-4's entry verdict for every bar. ⚠ ENTRIES ONLY — see the module docstring.

    ⚠ ALL FOUR INPUTS ARE DECLARED TO THE ONE LEG, including the raw ``atr``
    that the condition does not read directly. ``atr`` is declared because
    ``compression_rank_series`` is derived from it and a caller reading this
    function should see the strategy's real data requirement, not a requirement
    laundered through a helper — and because §3.1 makes evaluability a property
    of the STRATEGY, decided before any condition runs, so the set of declared
    inputs is the honest statement of what the bar needs. It changes no verdict
    (every ATR refusal already propagates into the rank series), which is why it
    is unprobeable and is called out here rather than left to look load-bearing.

    ``masked_reason`` is the code recorded when an OHLC field is missing, and it
    comes from the caller because only the caller knows why. Segment boundaries
    are structural rather than missing fields and are therefore applied by the
    runner before this function is called.
    """
    if masked_reason not in NOT_EVALUABLE_REASONS:
        raise ValueError(f"unknown reason code {masked_reason!r}; must be one of {sorted(NOT_EVALUABLE_REASONS)}")

    closes = series.float_closes
    atr = atr_series(series, universe=universe, period=ATR_PERIOD)
    compression = compression_rank_series(atr, universe=universe)
    prior_high = prior_high_close_series(series, universe=universe)

    inputs = (
        StrategyInput(series=_close_input(series, universe=universe), reason=masked_reason),
        StrategyInput(series=atr, reason=masked_reason),
        StrategyInput(series=compression, reason=masked_reason),
        StrategyInput(series=prior_high, reason=masked_reason),
    )

    def entry(index: int) -> bool:
        close = closes[index]
        rank = compression.values[index]
        highest_prior_close = prior_high.values[index]
        # Not reachable through `evaluate`, which refuses the bar first. Present
        # to narrow the types and to fail loudly for a direct caller.
        assert close is not None and rank is not None and highest_prior_close is not None
        return rank < COMPRESSION_QUANTILE and close > highest_prior_close

    return evaluate(entry, inputs=inputs, n_bars=len(series), kind="entry")


__all__ = [
    "ATR_PERIOD",
    "ATR_STOP_MULTIPLE",
    "ATR_TARGET_MULTIPLE",
    "BREAKOUT_LOOKBACK",
    "COMPRESSION_QUANTILE",
    "COMPRESSION_WINDOW",
    "MAX_HOLD_BARS",
    "S4_PARAMS",
    "S4_STRATEGY_ID",
    "WARMUP_BARS",
    "compression_rank_series",
    "prior_high_close_series",
    "s4_identity",
    "s4_signals",
]
