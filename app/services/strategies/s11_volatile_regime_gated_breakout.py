"""S-11 — S-4's compression breakout, gated to the two volatile regimes.

The R5 sweep calls this candidate **S-H**; the manifest numbers its strategies
``s{N}-`` and the id has to live in the manifest, so the sequence continues at 11
and the mapping is recorded here. Spec:
``docs/proposals/ta/2026-08-22-sh-volatile-regime-gated-breakout.md``.
Refs #2840, #2832, #2437.

THE RULE
--------
    entry(t) := s4_entry(t)  and  regime(t) in {bear_volatile, bull_volatile}

S-4's rule is IMPORTED, never restated — ``compression_rank_series``,
``prior_high_close_series`` and every constant come from
``s4_volatility_compression_breakout``. The bracket, the two ATR multiples and
the 40-bar hold cap are S-4's, unchanged: the gate conditions ENTRY, not the exit.

⚠⚠ S-4 IS LEFT BYTE-IDENTICAL, AND THAT IS THE WHOLE REASON THIS IS A NEW
STRATEGY ID RATHER THAN A NEW S-4 VERSION.
#2840 step 1 asks for "a NEW strategy_version". Editing
``s4_volatility_compression_breakout.py`` would move its ``_source_hash()`` and
therefore the S-4 identity, so every stored S-4 result — the CONTROL this
candidate is measured against — would stop being interpretable against the rule
it ran under, for a change S-4's own rule did not undergo.

⚠⚠ ``s4_source_hash`` IS IN ``S11_PARAMS`` AND IS LOAD-BEARING.
This module imports S-4's rule rather than copying it, so an edit to S-4 changes
what S-11 DOES. Without S-4's hash in these params, S-11's own ``_source_hash()``
would not move and the changed rule would silently inherit this one's track
record. Same construction, and the same reason, as S-5 hashing
``LEVEL_RULE_VERSION`` and ``REGIME_RULE_VERSION`` rather than trusting its own
file hash.

⚠ THE HASH IS RECOMPUTED FROM S-4's MODULE PATH RATHER THAN IMPORTED.
``s4_volatility_compression_breakout._source_hash`` is private and absent from
that module's ``__all__``. Adding it there would change S-4's file bytes — i.e.
bump the S-4 version, which is precisely the orphaning described above. So the
one-line construction is repeated here and
``test_s11_hash_tracks_s4`` asserts the two agree, which is what stops the copy
drifting.

⚠⚠ THE REGIME IS A DECLARED INPUT, NOT A CHECK IN THE BODY, AND THIS MODULE
CANNOT BE A POST-FILTER OVER ``s4_signals``.
Filtering S-4's returned signal list would collapse "the regime says no"
(``not_fired``) and "there is no regime at all" (``not_evaluable /
missing_market_context``) into the same non-firing bar — criterion 8's
distinction destroyed at the last step, and the bug S-6 shipped. So the four S-4
inputs are rebuilt here and the ``RegimeSeries`` is declared alongside them,
LAST, exactly as ``s5_signals`` declares it.

⚠ DECLARED LAST ON PURPOSE. ``evaluate`` refuses on the first unevaluable input,
so S-4's own OHLC and warm-up refusals take precedence over the regime's. That is
the honest order: a bar S-4 cannot evaluate is not a bar whose regime matters.

⚠ THE PERMITTED SET IS THE HYPOTHESIS, NOT A TUNABLE.
It is the two cohorts #2840 named. It is frozen in ``S11_PARAMS`` and hashed into
the identity, so moving it mints a different strategy rather than re-reading this
one's evidence. The measured premise behind it — including the 4x cohort
double-count the ticket's headline figures contained, and the fact that
``bull_volatile`` flips sign in the ``admitted`` quarantine arm — is in the spec's
"Measured premise, corrected" section. It is NOT narrowed to ``bear_volatile``
here: doing that would fit the hypothesis to the hold-out cohort that suggested it.

⚠ ``WARMUP_BARS`` IS RE-EXPORTED FROM S-4 (113) AND IS NOT S-11's EFFECTIVE
WARM-UP. The regime has its own — a 200-bar SMA and a 126-bar BandWidth window —
so no S-11 bar is judgeable before the BENCHMARK has warmed up, which is a
property of a different series and has no fixed offset into this one. Re-exporting
S-4's is honest only because nothing enforces it as a length check: the series
emit ``None`` until their windows fill and ``evaluate`` turns that into
``insufficient_warmup`` structurally, exactly as S-4's own docstring describes. A
caller that treats this constant as "the first judgeable index" would be wrong for
S-11 in a way it is not wrong for S-4.

⚠ REGIME BOUNDARY CONVENTIONS ARE INHERITED FROM ``market_regime``, NOT DECIDED
HERE. ``close > SMA`` classifies equality as bearish and a tied Bulge counts as
volatile. Both are frozen in ``REGIME_RULE_VERSION``, which this identity hashes,
so a change to either is a new S-11 rather than a silent re-reading of this one.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path

from app.services.indicator_series import BarSeries, IndicatorSeries, Universe, atr_series
from app.services.market_regime import REGIME_RULE_VERSION, Regime, RegimeSeries
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

S11_STRATEGY_ID = "s11-volatile-regime-gated-breakout"

#: The hypothesis under test. ⚠ BOTH volatile regimes — see the module docstring.
PERMITTED_REGIMES = frozenset({Regime.BEAR_VOLATILE, Regime.BULL_VOLATILE})


def _s4_source_hash() -> str:
    """S-4's file hash, recomputed rather than imported — see the module docstring."""
    return hashlib.sha256(Path(s4.__file__).read_bytes()).hexdigest()[:12]


#: ⚠ WRITTEN OUT KEY BY KEY rather than merged from ``S4_PARAMS``. A merge has a
#: collision direction to get wrong and hides which keys this strategy actually
#: declares; there are seven of them and they are cheap to name.
#:
#: ⚠ ``permitted_regimes`` is a sorted tuple of enum VALUES, not the frozenset —
#: ``s5_support_bounce``'s serialisation, because a set has no canonical JSON
#: form and the identity digest needs one.
S11_PARAMS: Mapping[str, object] = {
    "atr_period": ATR_PERIOD,
    "compression_window": COMPRESSION_WINDOW,
    "compression_quantile": COMPRESSION_QUANTILE,
    "breakout_lookback": BREAKOUT_LOOKBACK,
    "atr_stop_multiple": ATR_STOP_MULTIPLE,
    "atr_target_multiple": ATR_TARGET_MULTIPLE,
    "max_hold_bars": MAX_HOLD_BARS,
    "permitted_regimes": tuple(sorted(r.value for r in PERMITTED_REGIMES)),
    "regime_rule_version": REGIME_RULE_VERSION,
    "s4_source_hash": _s4_source_hash(),
}


def _source_hash() -> str:
    """Hash of THIS module — the ``source_hash`` half of criterion 11."""
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


def s11_identity(*, universe: Universe, cost_model_id: str) -> StrategyIdentity:
    """The registered identity of S-11 on one universe under one cost model."""
    if not cost_model_id.strip():
        raise ValueError(
            "cost_model_id must be a non-empty declaration (criterion 11 hashes it); "
            "pass app.services.cost_model.COST_MODEL_ID rather than an empty string"
        )
    return StrategyIdentity(
        strategy_id=S11_STRATEGY_ID,
        params=S11_PARAMS,
        universe=universe,
        cost_model_id=cost_model_id,
        source_hash=_source_hash(),
    )


def _close_input(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """The bar closes, in the shape the runner checks for evaluability.

    ⚠ A LOCAL COPY, matching what S-1, S-3, S-4 and S-5 each already carry — the
    package's convention is one per module. S-4's is private and this module must
    not edit S-4's file to export it (that would bump S-4's version, per the
    module docstring). ``test_s11_close_input_matches_s4`` compares the two.
    """
    closes = series.float_closes
    return IndicatorSeries(
        values=tuple(closes),
        universe=universe,
        not_evaluable_indices=tuple(i for i, value in enumerate(closes) if value is None),
    )


def s11_exit_bracket(
    series: BarSeries,
    *,
    signal_index: int,
    entry_price: Decimal,
    universe: Universe,
) -> tuple[Decimal, Decimal, int]:
    """S-4's bracket, unchanged. The gate conditions entry, never the exit.

    Delegated rather than reimplemented so the two can never disagree — and so
    the ATR-at-signal-bar rule S-4's docstring calls out as the trap is enforced
    in exactly one place.
    """
    return s4_exit_bracket(series, signal_index=signal_index, entry_price=entry_price, universe=universe)


def s11_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,
) -> list[StrategySignal]:
    """S-11's entry verdict for every bar. ENTRIES ONLY, as S-4.

    The four S-4 inputs are rebuilt here rather than obtained by calling
    ``s4_signals`` — see the module docstring on why a post-filter is wrong.
    """
    if masked_reason not in NOT_EVALUABLE_REASONS:
        raise ValueError(f"unknown reason code {masked_reason!r}; must be one of {sorted(NOT_EVALUABLE_REASONS)}")
    if len(regime) != len(series):
        raise ValueError(f"regime series has {len(regime)} bars against {len(series)} price bars; they must align")

    closes = series.float_closes
    atr = atr_series(series, universe=universe, period=ATR_PERIOD)
    compression = compression_rank_series(atr, universe=universe)
    prior_high = prior_high_close_series(series, universe=universe)

    inputs = (
        StrategyInput(series=_close_input(series, universe=universe), reason=masked_reason),
        StrategyInput(series=atr, reason=masked_reason),
        StrategyInput(series=compression, reason=masked_reason),
        StrategyInput(series=prior_high, reason=masked_reason),
        # ⚠⚠ LAST, and with its own reason code. See the module docstring: a
        # benchmark hole is `missing_market_context`, while a benchmark bar that
        # exists but is not yet classifiable stays `insufficient_warmup`, which
        # `evaluate` derives structurally from the bare `None`.
        StrategyInput(series=regime, reason="missing_market_context"),
    )

    def entry(index: int) -> bool:
        close = closes[index]
        rank = compression.values[index]
        highest_prior_close = prior_high.values[index]
        # Not reachable through `evaluate`, which refuses the bar first.
        assert close is not None and rank is not None and highest_prior_close is not None
        if not regime.permits(index, PERMITTED_REGIMES):
            return False
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
    "PERMITTED_REGIMES",
    "S11_PARAMS",
    "S11_STRATEGY_ID",
    "WARMUP_BARS",
    "s11_exit_bracket",
    "s11_identity",
    "s11_signals",
]
