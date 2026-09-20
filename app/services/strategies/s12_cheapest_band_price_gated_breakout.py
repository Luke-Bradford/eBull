"""S-12 — S-4's compression breakout, gated to the cheapest CHARGED cost band.

The R5 sweep calls this candidate **S-H arm 2**; the manifest numbers its strategies
``s{N}-`` and the id has to live in the manifest, so the sequence continues at 12 and
the mapping is recorded here. Spec:
``docs/proposals/ta/2026-09-20-sh-top-band-price-gated-breakout.md``.
Refs #2840, #2832, #2437, #3238.

THE RULE
--------
    entry(t) := s4_entry(t)  and  close(t) >= CHEAPEST_BAND.lower

S-4's rule is IMPORTED, never restated, and its bracket, ATR multiples and 40-bar
hold cap are unchanged: the gate conditions ENTRY, not the exit. S-4 is left
byte-identical for S-11's reason — it is the CONTROL this candidate is measured
against, and editing it would move its ``_source_hash()`` and orphan its stored
results from the rule they ran under.

⚠⚠ THE THRESHOLD IS THE CHEAPEST **CHARGED** BAND, NOT ``BANDS[-1]``.
``BANDS[-1]`` is guaranteed only to be the highest-PRICED band. The hypothesis is
"trade where the cost model charges least", so the gate reads
``min(BANDS, key=p75_spread_pct)`` — the mirror of ``cost_model``'s own
``UNKNOWN_NOMINAL_PRICE_BAND = max(BANDS, key=...)``. They are the same band today
(``>=$100`` at 0.322% against 0.509 / 0.571 / 1.450) and ``_check_gate_is_expressible``
asserts at IMPORT that they still are.

⚠⚠ AND THAT ASSERTION IS NOT PEDANTRY. A recalibration that made a BOUNDED band the
cheapest one would leave every line below running happily while the rule silently
stopped expressing the hypothesis: ``close >= lower`` would then admit a dearer band
above the cheap one. Import is the right place because there is no verdict to return
— the strategy has no honest answer under that table, and a module that cannot state
its own rule must not load.

⚠ THE THRESHOLD IS NEVER TYPED AS A LITERAL — ``_cheapest_band`` takes the table as
an argument so ``test_the_threshold_follows_a_recalibrated_band_table`` can prove the
edge MOVES with it, which a source grep for ``100`` could not.
The edge is read from the band table so the gate and the charge can never disagree,
and ``price_band_lower`` is hashed into ``S12_PARAMS`` so narrowing it after a look
mints a different strategy rather than re-reading this one's evidence.

⚠ THE GATE IS A BODY CONDITION, AND THAT IS LEGAL HERE FOR A REASON S-11 COULD NOT
USE. S-11 had to declare its regime as a fifth ``StrategyInput`` because a missing
regime is an ABSENT INPUT — ``not_evaluable / missing_market_context`` — and writing
it as a body check would have collapsed that into ``not_fired``, which is the bug S-6
shipped. The close is ALREADY one of S-4's four declared inputs, so a priced bar S-4
can evaluate is one S-12 can evaluate and the gate introduces no new unevaluable
state. It can only turn a ``fired`` into a ``not_fired``, never a refusal into either.

⚠ THE PRICE MUST BE AS TRADED, AND THIS MODULE CANNOT CHECK THAT.
A ``>= $100`` gate is a nominal-price gate only on an as-traded corpus.
``BACKTEST_UNIVERSE`` is ``survivorship_free``, pinned to an archive whose declared
``adjustment_basis`` is ``unadjusted``, and #3238 made that the live cost basis. The
coupling is asserted in two places OUTSIDE this module — a config test on the pinned
archive, and a run-time assertion on the actual run's ``cost_price_basis`` in the
step-3 measurement script — because the manifest's signals callable has no
price-basis channel and because mapping universe to basis HERE would be wrong on the
scan path, whose prices are live candles rather than the pinned archive. The spec's
"price basis" section carries what each guard does and does not catch.

⚠ ``s4_source_hash`` IS IN ``S12_PARAMS`` AND IS LOAD-BEARING, for S-11's reason
verbatim: this module imports S-4's rule, so an edit to S-4 changes what S-12 DOES,
and without S-4's hash in these params S-12's own ``_source_hash()`` would not move
and the changed rule would silently inherit this one's track record. The hash is
RECOMPUTED from S-4's module path rather than imported, because
``s4_volatility_compression_breakout._source_hash`` is private and exporting it would
change S-4's file bytes — i.e. bump the S-4 version, which is the orphaning above.
``test_s12_params_carry_s4s_live_source_hash`` asserts the two agree.

⚠ ``WARMUP_BARS`` IS RE-EXPORTED FROM S-4 (113) AND IS S-12's WARM-UP UNCHANGED —
unlike S-11, whose regime input warms up on a different series with no fixed offset.
The gate reads one already-declared input on the bar itself and adds no window.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path

from app.services.cost_model import BANDS, PriceBand
from app.services.indicator_series import BarSeries, IndicatorSeries, Universe, atr_series
from app.services.strategies import s4_volatility_compression_breakout as s4
from app.services.strategies.s4_volatility_compression_breakout import (
    ATR_PERIOD,
    ATR_STOP_MULTIPLE,
    ATR_TARGET_MULTIPLE,
    BREAKOUT_LOOKBACK,
    COMPRESSION_QUANTILE,
    COMPRESSION_WINDOW,
    MAX_HOLD_BARS,
    WARMUP_BARS,
    compression_rank_series,
    prior_high_close_series,
    s4_exit_bracket,
)
from app.services.strategy_registry import (
    NOT_EVALUABLE_REASONS,
    NotEvaluableReason,
    StrategyIdentity,
    StrategyInput,
    StrategySignal,
    evaluate,
)

S12_STRATEGY_ID = "s12-cheapest-band-price-gated-breakout"


def _cheapest_band(bands: tuple[PriceBand, ...]) -> PriceBand:
    """The band charged the least — the mirror of ``UNKNOWN_NOMINAL_PRICE_BAND``'s ``max``.

    ⚠ TAKES THE TABLE AS AN ARGUMENT rather than closing over ``BANDS``, so a test
    can prove the threshold FOLLOWS the table under a recalibration instead of
    monkeypatching the params dict — which would only prove the digest reads that
    dict, not that the dict describes what the rule does.
    """
    return min(bands, key=lambda band: band.p75_spread_pct)


#: The hypothesis under test: the band the cost model charges LEAST for.
CHEAPEST_BAND: PriceBand = _cheapest_band(BANDS)


def _check_gate_is_expressible(band: PriceBand) -> Decimal:
    """The gate's edge — or a refusal, if ``close >= band.lower`` no longer states it.

    Checked at import for the reason ``cost_model._check_bands_are_total`` is: a
    band table that no longer supports the rule must fail where it is read, not
    produce a plausible verdict under a rule nobody declared.

    ⚠ RETURNS THE EDGE rather than just raising, so the one function that proves
    ``lower`` is not ``None`` is also the one that hands it on. Splitting the two
    would leave every consumer re-narrowing a ``Decimal | None`` that this check has
    already settled — and the obvious way to silence that is an ``assert``, which is
    a second, weaker copy of this rule.
    """
    if band.lower is None:
        raise ValueError(
            f"cheapest band {band.label} is open BELOW, so every price clears it and the gate is not a gate"
        )
    if band.upper is not None:
        raise ValueError(
            f"cheapest band {band.label} is bounded above ({band.upper}), so `close >= {band.lower}` would admit "
            "the dearer bands above it; the hypothesis needs an interval rule and a new strategy version"
        )
    return band.lower


#: The edge itself, in the table's own type. Hashed into ``S12_PARAMS``.
GATE_EDGE: Decimal = _check_gate_is_expressible(CHEAPEST_BAND)

#: The gate's threshold. ⚠ A ``float`` because the comparison runs once per bar over
#: tens of millions of them and ``series.float_closes`` is already float. What
#: licenses that is a measurement, not an assumption: 0 of 75,972,669
#: ``research_price_daily`` open/close values sit within 1e-9 of a band edge without
#: equalling it (#3238, recorded at ``backtest_run.py:1479``), so no stored price can
#: land in the interval where the float and ``Decimal`` answers could differ.
#: ``test_the_float_gate_agrees_with_band_for`` pins the two together anyway.
PRICE_FLOOR: float = float(GATE_EDGE)


def _s4_source_hash() -> str:
    """S-4's file hash, recomputed rather than imported — see the module docstring."""
    return hashlib.sha256(Path(s4.__file__).read_bytes()).hexdigest()[:12]


#: ⚠ WRITTEN OUT KEY BY KEY rather than merged from ``S4_PARAMS`` — S-11's reasoning:
#: a merge has a collision direction to get wrong and hides which keys this strategy
#: actually declares.
#:
#: ⚠ ``price_band_lower`` is the Decimal's ``str``, not the float. The identity digest
#: needs a canonical form, and ``str(Decimal("100"))`` is stable where a float repr is
#: a property of the binary value.
#:
#: ⚠ The band keys are BELT-AND-BRACES, not the primary protection. ``cost_model``'s
#: own rule is that a change to what is charged is a new ``COST_MODEL_ID``, and that
#: id is hashed into every ``StrategyIdentity`` — so a compliant recalibration already
#: rotates this version. What these add is cover for a spread-only edit that moved a
#: p75 without moving a label, plus a statement in the identity of which threshold
#: this rule used.
S12_PARAMS: Mapping[str, object] = {
    "atr_period": ATR_PERIOD,
    "compression_window": COMPRESSION_WINDOW,
    "compression_quantile": COMPRESSION_QUANTILE,
    "breakout_lookback": BREAKOUT_LOOKBACK,
    "atr_stop_multiple": ATR_STOP_MULTIPLE,
    "atr_target_multiple": ATR_TARGET_MULTIPLE,
    "max_hold_bars": MAX_HOLD_BARS,
    "price_band_label": CHEAPEST_BAND.label,
    "price_band_lower": str(GATE_EDGE),
    "s4_source_hash": _s4_source_hash(),
}


def _source_hash() -> str:
    """Hash of THIS module — the ``source_hash`` half of criterion 11."""
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


def s12_identity(*, universe: Universe, cost_model_id: str) -> StrategyIdentity:
    """The registered identity of S-12 on one universe under one cost model."""
    if not cost_model_id.strip():
        raise ValueError(
            "cost_model_id must be a non-empty declaration (criterion 11 hashes it); "
            "pass app.services.cost_model.COST_MODEL_ID rather than an empty string"
        )
    return StrategyIdentity(
        strategy_id=S12_STRATEGY_ID,
        params=S12_PARAMS,
        universe=universe,
        cost_model_id=cost_model_id,
        source_hash=_source_hash(),
    )


def _close_input(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """The bar closes, in the shape the runner checks for evaluability.

    A LOCAL COPY, matching what S-1, S-3, S-4, S-5 and S-11 each already carry —
    the package's convention is one per module, and S-4's is private and must not be
    exported (that would bump S-4's version).
    ``test_s12_close_input_matches_s4`` compares the two.
    """
    closes = series.float_closes
    return IndicatorSeries(
        values=tuple(closes),
        universe=universe,
        not_evaluable_indices=tuple(i for i, value in enumerate(closes) if value is None),
    )


def s12_exit_bracket(
    series: BarSeries,
    *,
    signal_index: int,
    entry_price: Decimal,
    universe: Universe,
) -> tuple[Decimal, Decimal, int]:
    """S-4's bracket, unchanged. The gate conditions entry, never the exit.

    Delegated rather than reimplemented so the two can never disagree — and so the
    ATR-at-signal-bar rule S-4's docstring calls out as the trap is enforced in
    exactly one place.
    """
    return s4_exit_bracket(series, signal_index=signal_index, entry_price=entry_price, universe=universe)


def s12_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
) -> list[StrategySignal]:
    """S-12's entry verdict for every bar. ENTRIES ONLY, as S-4.

    S-4's four inputs are rebuilt here rather than obtained by calling ``s4_signals``
    and filtering. Post-filtering a returned signal list is the construction S-11's
    docstring rejects: it turns the runner's own verdict into an input to a second
    pass, and the next gate written that way WILL be one whose condition can be
    unevaluable. Rebuilding keeps every strategy in this package the same shape.
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
        # Not reachable through `evaluate`, which refuses the bar first.
        assert close is not None and rank is not None and highest_prior_close is not None
        # ⚠ INCLUSIVE, matching `PriceBand.contains`: the band the charge would
        # select on this level is the band the gate admits.
        if close < PRICE_FLOOR:
            return False
        return rank < COMPRESSION_QUANTILE and close > highest_prior_close

    return evaluate(entry, inputs=inputs, n_bars=len(series), kind="entry")


__all__ = [
    "ATR_PERIOD",
    "GATE_EDGE",
    "ATR_STOP_MULTIPLE",
    "ATR_TARGET_MULTIPLE",
    "BREAKOUT_LOOKBACK",
    "CHEAPEST_BAND",
    "COMPRESSION_QUANTILE",
    "COMPRESSION_WINDOW",
    "MAX_HOLD_BARS",
    "PRICE_FLOOR",
    "S12_PARAMS",
    "S12_STRATEGY_ID",
    "WARMUP_BARS",
    "s12_exit_bracket",
    "s12_identity",
    "s12_signals",
]
