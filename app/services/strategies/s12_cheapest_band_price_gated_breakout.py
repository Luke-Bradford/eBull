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

⚠⚠ THE PRICE MUST BE AS TRADED, AND THAT IS ENFORCED BY THE ``price_basis`` INPUT.
A ``>= $100`` gate is a nominal-price gate only on an as-traded corpus, so a
``PriceBasisSeries`` is declared FIRST among this rule's inputs and ``evaluate``
refuses an uncertified bar before the body runs.

⚠⚠ IT USED TO BE A UNIVERSE TOKEN, AND THE TOKEN IS GONE (#2840 §6 item 3).
``AS_TRADED_UNIVERSES = {"survivorship_free"}`` refused every bar on any other
universe — a SERIES-LEVEL NAME standing in for a PER-BAR FACT, wrong in both
directions. It refused the live scan unconditionally (all 5,791 stored S-12
observations are that one refusal) while a ``survivorship_free`` run whose archive
policy was WITHHELD passed with no refusal anywhere, because
``backtest_run._resolve_liquidity_policy`` returns ``None`` rather than blocking.
The carrier reads the fact the name stood in for, so the name is not needed.

⚠ WHAT THE TOKEN WAS ALSO DOING, and where that work moved. On the scan path
``survivor_only`` is served by ``price_daily``, whose history the provider
back-adjusts at fetch time and whose #2066 split-cliff guard HEALS a mixed series
onto the back-adjusted basis rather than preserving the traded one
(``market_data.py:751``). That fact has not gone away — it is now the reason the
scan declares NO source at all (``strategy_price_basis.from_undeclared_source``),
which is a statement about the provenance rather than about the universe's name.

⚠ WHY A REFUSAL AND NOT AN EXCLUSION FROM THE SCAN. S-12 is the first strategy in
this package with an ABSOLUTE price threshold — every other rule compares prices to
prices, so a uniformly re-based series gives it the same answer and back-adjustment
is invisible to it. A scan catching up across a split would hand this rule pre-split
bars on the post-split scale, and scan rows are terminal. So the bar is refused
rather than judged: no wrong verdict is recorded, the strategy stays non-retired
(retirement would also remove it from the BACKTEST, which is the evidence #2840
arm 2 actually needs), and the scan keeps running.

⚠ NO GATE HERE VALIDATES THE PAYLOAD. A stored ``unadjusted`` label on rescaled
data satisfies the carrier, ``cost_price_basis`` and the config test alike. That is
the corpus's invariant (``sql/249``, ``sql/251``, ``sql/305``), and the spec's
"price basis" section states the division.

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
from app.services.strategy_price_basis import PRICE_BASIS_RULE_VERSION, PriceBasisSeries
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

#: ⚠ ``missing_market_context`` and not a new code. It is one of OUR four reasons
#: (``OUR_ADDITIONAL_REASON_CODES``), so its meaning is ours to state: the bar
#: exists and its fields are present, but the CONTEXT this rule needs — a price on
#: the scale it would have traded at — is not available on this universe. Minting a
#: fifth code would mean editing a closed vocabulary that is written out in four
#: places including ``sql/255``'s CHECK, which is the drift defect the registry's
#: own comment carries from #2218.
PRICE_BASIS_REFUSAL_REASON: NotEvaluableReason = "missing_market_context"

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
    # ⚠⚠ THE PRICE-BASIS RULE IS VERSIONED HERE AND NOT IN ``INPUT_RULE_SETS``
    # (#2840). That mapping is hashed into EVERY ``StrategyIdentity``
    # (``strategy_registry.py:279``), so installing a sixth entry would rotate all
    # 11 strategies and detach their stored evidence across 24 identity-carrying
    # tables — to version a rule with exactly one consumer.
    #
    # ⚠ This is an EXCEPTION to the registry's own preference, and it is bounded
    # rather than argued away: ``INPUT_RULE_SETS`` exists because author-maintained
    # per-strategy coverage drifts once a second consumer appears.
    # ``test_s12_is_the_only_consumer_while_the_rule_stays_out_of_input_rule_sets``
    # fails the moment another strategy imports the module.
    #
    # ⚠⚠ THE TRIGGER IS A SECOND CONSUMER, NOT "#2840 §6 item 3" — CORRECTED HERE.
    # Both this comment and that test used to name item 3 (this change, which
    # removed ``AS_TRADED_UNIVERSES``) as the moment the global entry was owed. It
    # is not: item 3 added no reader. Every other ``PerSeriesSignals`` /
    # ``MemberStager`` adapter takes ``price_basis`` and DISCARDS it (12 of them
    # carry ``noqa: ARG001`` in ``strategy_manifest``), and ``segmented_signals`` /
    # ``segmented_member`` only slice it. The consumer set is still {S-12}.
    #
    # ⚠ What IS still owed, and is not this: the carrier's SOURCE SELECTION sits
    # outside every identity hash, so rewiring which constructor the scan calls
    # moves verdicts without moving this version. Pre-existing (#2840's carrier
    # shipped it); the spec's §8 records it open.
    "price_basis_rule": PRICE_BASIS_RULE_VERSION,
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
    price_basis: PriceBasisSeries,
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
    # ⚠⚠ THE LENGTH CHECK IS A BACKSTOP AND IT IS LOAD-BEARING (#2840, Codex
    # checkpoint 1). ``evaluate`` returns ``no_fill_bar`` for the LAST bar before
    # reading any input, so a carrier one element SHORT is never looked up and
    # passes silently — the misalignment then shifts every certification by one
    # with nothing raising. Production callers build the carrier FROM this series
    # so the class cannot arise there; this covers the hand-built caller, which is
    # how S-12 is reached in tests and scripts.
    if len(price_basis) != len(series):
        raise ValueError(f"price_basis has {len(price_basis)} bars against {len(series)} price bars; they must align")
    # ⚠⚠ THE CARRIER'S RULE VERSION MUST BE THE ONE THIS IDENTITY CLAIMS (#2840,
    # Codex checkpoint 1). ``S12_PARAMS["price_basis_rule"]`` hashes
    # ``PRICE_BASIS_RULE_VERSION`` into the identity, so a carrier built under an
    # older rule would execute under a version that asserts the newer one — the
    # stored verdict would name a rule that did not produce it. Now that this is
    # the SOLE gate the mismatch is not survivable, so it raises rather than
    # refusing: a caller holding a stale carrier has a wiring bug, and a refusal
    # would write that bug into the ledger as a data condition.
    #
    if price_basis.rule_set_version != PRICE_BASIS_RULE_VERSION:
        raise ValueError(
            f"price_basis was built under rule {price_basis.rule_set_version!r} but this identity claims "
            f"{PRICE_BASIS_RULE_VERSION!r}; a stale carrier cannot certify a bar for the current rule"
        )
    # ⚠⚠ LENGTH EQUALITY DOES NOT BIND A CARRIER TO THESE BARS (#2840 §8b, closed
    # here). Measured before the fix: a carrier built at ``n_bars=len(series_B)``
    # was accepted against series A and this rule FIRED on it —
    # ``Counter({'not_evaluable': 114, 'not_fired': 60, 'fired': 1})``. The length
    # check above cannot see it, and neither can the two dispatchers'.
    #
    # ⚠ It is a SECOND check, not a replacement: the length message documents the
    # off-by-one hazard (``evaluate`` returns ``no_fill_bar`` for the last bar
    # before reading any input, so a one-short carrier is never looked up), and
    # deleting it because a stronger check subsumes it would lose that reasoning.
    #
    # ⚠ It RAISES rather than refusing, for the same reason the rule-version
    # mismatch does: a caller holding a foreign carrier has a wiring bug, and a
    # refusal would write that bug into the ledger as a data condition.
    if (mismatch := price_basis.binding_mismatch(series)) is not None:
        raise ValueError(mismatch)

    # ⚠⚠ SHORT-CIRCUIT, AND IT IS A COMPLEXITY FIX RATHER THAN A SECOND RULE.
    # ``_unevaluable_reason_at`` tests ``index in series.not_evaluable_indices``
    # on a TUPLE, so an all-refused carrier of n bars costs O(n²) — and n here is
    # the whole corpus, on the new withheld-policy path. Widening that field to a
    # set means editing ``strategy_registry``, whose bytes are hashed into EVERY
    # strategy identity, so the short-circuit lives here instead.
    #
    # ⚠ It must not be able to diverge from the declared-input path:
    # ``test_the_short_circuit_matches_the_declared_input_path`` asserts the two
    # produce identical verdicts, because a fast path that disagrees with the
    # rule is worse than the cost it saves.
    #
    # ⚠⚠ AND THE LAST BAR IS ``no_fill_bar``, NOT THE BASIS REFUSAL. ``evaluate``
    # returns it BEFORE reading any input (``strategy_registry.py:474``), so a
    # uniformly-refusing list would disagree with the path it replaces on
    # exactly one bar per series.
    #
    # ⚠⚠ "LAST BAR" IS THE LAST BAR OF THE **SEGMENT**, NOT OF THE SERIES.
    # ``segmented_signals`` calls this once per price-scale segment with a fresh
    # ``BarSeries`` indexed from zero, so this stamps ``no_fill_bar`` at every
    # segment terminus. That is what every peer strategy already stores there —
    # and it is the one verdict the removed ``AS_TRADED_UNIVERSES`` gate got
    # wrong, since it refused uniformly and never routed through ``evaluate`` at
    # all. Measured before removing it: 0 of the 5,791 stored S-12 observations
    # sit at a segment terminus (anti-join on ``price_series_break`` at the bar
    # after each one), so the correction moved no stored row.
    if price_basis.certifies_nothing():
        return [
            StrategySignal(
                verdict="not_evaluable",
                signal_index=index,
                kind="entry",
                reason="no_fill_bar" if index == len(series) - 1 else PRICE_BASIS_REFUSAL_REASON,
            )
            for index in range(len(series))
        ]

    closes = series.float_closes
    atr = atr_series(series, universe=universe, period=ATR_PERIOD)
    compression = compression_rank_series(atr, universe=universe)
    prior_high = prior_high_close_series(series, universe=universe)

    inputs = (
        # ⚠⚠ FIRST, AND THE ORDER IS THE RULE RATHER THAN A STYLE (#2840).
        # ``_unevaluable_reason_at`` returns the FIRST declared input's reason
        # among competing data reasons, so a bar that is both quarantined and
        # uncertified reports whichever is declared earlier. The basis belongs
        # first: the other four say *this bar's data is unusable*, this one says
        # *this rule may not read this corpus at all*, and the precondition is
        # the more informative answer to store.
        StrategyInput(series=price_basis, reason=PRICE_BASIS_REFUSAL_REASON),
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
    "PRICE_BASIS_REFUSAL_REASON",
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
