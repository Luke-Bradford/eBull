"""S-10 — relative-strength leader. The set's second cross-sectional strategy.

Parent spec: ``docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md`` §S-10.
Plan + Codex ckpt-1 resolutions:
``docs/proposals/ta/2026-08-14-s10-implementation-plan.md``. Contract:
``app/services/strategy_registry.py`` (``admissible_indices`` /
``mandatory_indices`` were added FOR this module — S-2 predates and ignores
them). Refs #2437.

THE RULE, VERBATIM FROM §S-10
-----------------------------
    Setup: regime ∈ {bull_quiet}; universe ranked by 63-bar return.
    Signal: enter the top decile that ALSO closes above its own 50-SMA;
    rebalance monthly.
    Exit: leaves the top three deciles, or closes below 50-SMA; no fixed stop
    (cross-sectional).

⚠⚠ THE EXIT CADENCE IS MONTHLY, PINNED BY THE SPEC'S OWN GATE, NOT BY TASTE.
§S-10 orders the turnover measurement FIRST and disqualifies above
~50%/month (Novy-Marx/Velikov). Measured on the full validated universe with
the exact semantics this module ships (``scripts/verify_2437_s10_turnover.py``
v4, posted on #2437): both exit legs at rebalance only → **36.5%/month**
steady-state; the 50-SMA exit checked daily → **83.0%/month**. The daily
reading is not a viable variant of this strategy; the cadence is frozen in
``S10_PARAMS`` and hashed into the identity.

TWO LEGS, TWO PANELS — the denominators differ and that is a stated choice
---------------------------------------------------------------------------
- The ENTRY panel carries S-2's §9 Q3 price floor (``close >= $1``, evaluated
  as-of the decision bar): momentum-style ranks concentrate tick-quantised
  penny names in exactly the decile this strategy buys. ⚠ S-2's caveat
  applies verbatim: on split-adjusted closes this is an ADJUSTED-price floor,
  and the deviation runs one way (reverse-split survivors of a sub-$1 past
  pass a floor they would have failed at the time).
- The EXIT panel is floor-free: a held name that fell under $1 must still be
  exitable, and the retention band is cut on the panel that contains it.
- "The top decile that ALSO closes above its own 50-SMA" is read as: the
  decile is cut on the whole (floored) ranking, then the SMA condition
  filters the winners WITHOUT backfilling the slots —
  ``admissible_indices``' exact semantics, built for this sentence.
- "Leaves the top three deciles" is the band ``ordered[3 * N // 10 :]`` of
  the exit panel; "closes below 50-SMA" fires REGARDLESS of the band —
  ``mandatory_indices``' exact semantics.

⚠ THE EXIT LEG IS EVALUABLE ONLY WHEN THE BAR IS RANKABLE (score, SMA and
close all present). The below-SMA exit does not logically need the rank, but
a second evaluability regime for one condition inside one leg would be two
contracts wearing one kind; an unrankable rebalance bar refuses visibly
(``not_evaluable``) and the position carries to the next evaluable rebalance.
Counted by the census, never assumed rare.

⚠ THE EXIT LEG DOES NOT DECLARE THE REGIME — S-7's rule: a missing benchmark
session must never refuse the exit verdict for an open position. Exits run in
every regime; only entries are gated.

⚠ NO ``max_hold_bars``, NO LEVELS, NO CALENDAR CLOSE — the exit regime is
``signal_pair`` alone (S-1/S-3's shape). ``rebalance_dates`` stays ``None``
deliberately: the position layer's C4 closes at "the next rebalance NOT
RESELECTED", which is entry-set retention — S-10's retention is the WIDER
top-three-decile band, and the exit leg carries it. Declaring both would
close band-surviving positions a month early.

⚠ PRICE RETURNS, NOT TOTAL RETURNS — S-2's note applies unchanged: ``close``
is the split-adjusted series consistent with OHLC (sql/251); the ranking
systematically understates high-yield names over a 63-bar lookback.

⚠ THIS MODULE NEVER RESOLVES A FILL, AND CANNOT — S-2's note, unchanged.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Set
from datetime import date
from pathlib import Path

from app.services.indicator_series import BarSeries, IndicatorSeries, Universe, sma_series
from app.services.market_regime import REGIME_RULE_VERSION, Regime, RegimeSeries
from app.services.strategy_registry import (
    NOT_EVALUABLE_REASONS,
    CrossSectionalMember,
    NotEvaluableReason,
    StrategyIdentity,
    StrategyInput,
    StrategySignal,
    evaluate_cross_sectional,
)

S10_STRATEGY_ID = "s10-relative-strength-leader"

#: §S-10's four free parameters. ⚠ FIXED, NEVER TUNED (§6 of the catalogue
#: parent: *"Forbidden — continuous re-optimisation"*). Module constants for
#: S-1's reason: a period that can be passed in is a period that can be swept.
LOOKBACK_BARS = 63
ENTRY_DECILE = 10
EXIT_BAND_DECILES = 3
SMA_BARS = 50

#: S-2's §9 Q3 floor, entry panel only — module docstring.
MIN_CLOSE = 1.0

#: The smallest cross-section either leg ranks on — BY CONSTRUCTION, and much
#: larger than S-2's 10 on purpose. S-2's floor protects a one-shot decile
#: cut; S-10's retention BAND is evaluated against the panel the book was
#: formed from, and a decile band cut on a sliver panel is a calendar hole
#: wearing a rank. Measured while gating (#2437): ``price_daily`` carries
#: weekend rows for ~11 instruments and one corpus-hole Friday with 243, while
#: every real panel ran 2,084–5,747. 1000 refuses every observed junk date
#: and no observed real one; a thin date refuses BOTH legs
#: (``thin_cross_section``) and holdings simply carry.
MIN_CROSS_SECTION = 1000

#: Entries only in the quiet bull — §S-10's one-regime setup, narrower than
#: S-7's pair by the spec's own text.
PERMITTED_REGIMES = frozenset({Regime.BULL_QUIET})

#: ⚠ §S-10 says "Params: 4" (what a sweep would move) — the first four. The
#: rest are by-construction constants recorded so the identity hash moves if
#: any is edited. ``exit_cadence`` is the measured pin from the module
#: docstring: it is a parameter precisely so that a daily-exit variant could
#: never share this identity.
S10_PARAMS: Mapping[str, object] = {
    "lookback_bars": LOOKBACK_BARS,
    "entry_decile": ENTRY_DECILE,
    "exit_band_deciles": EXIT_BAND_DECILES,
    "sma_bars": SMA_BARS,
    "min_close": MIN_CLOSE,
    "min_cross_section": MIN_CROSS_SECTION,
    "exit_cadence": "rebalance_only",
    "permitted_regimes": tuple(sorted(r.value for r in PERMITTED_REGIMES)),
    "regime_rule_version": REGIME_RULE_VERSION,
}


def _source_hash() -> str:
    """Hash of THIS module — the ``source_hash`` half of criterion 11."""
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


def s10_identity(*, universe: Universe, cost_model_id: str) -> StrategyIdentity:
    """The registered identity of S-10 on one universe under one cost model."""
    if not cost_model_id.strip():
        raise ValueError(
            "cost_model_id must be a non-empty declaration (criterion 11 hashes it); "
            "pass app.services.cost_model.COST_MODEL_ID rather than an empty string"
        )
    return StrategyIdentity(
        strategy_id=S10_STRATEGY_ID,
        params=S10_PARAMS,
        universe=universe,
        cost_model_id=cost_model_id,
        source_hash=_source_hash(),
    )


def s10_rebalance_dates(calendar: Iterable[date]) -> frozenset[date]:
    """First WEEKDAY bar of each new month, from the panel's union calendar.

    S-2's causal first-bar-of-the-new-month rule, with one cut applied first:
    Saturdays and Sundays are dropped before the month rule runs. Source rule:
    the validated universe is US-listed stock (§4.0), and US equity venues
    hold no regular weekend sessions (NYSE/Nasdaq holiday-and-hours
    calendars) — yet ``price_daily`` carries weekend rows for ~11 instruments,
    and because the FIRST qualifying bar takes the month, an unfiltered union
    calendar hands entire months' rebalances to an 11-name Sunday and the
    real first trading day then never rebalances at all (measured on #2437;
    S-2 shares the exposure and that is noted there, not silently fixed here
    — editing its calendar would move its identity).

    ⚠ A corpus-hole WEEKDAY (243 of ~3,500 names on 2023-12-01) still takes
    its month and is then refused by ``MIN_CROSS_SECTION`` — that month
    simply does not rebalance. Measured incidence: one date in 43 months;
    self-healing as the corpus fills; preferred over teaching this pure
    function participation counts it cannot verify.
    """
    weekdays = [when for when in sorted(set(calendar)) if when.weekday() < 5]
    return frozenset(
        when
        for previous, when in zip(weekdays, weekdays[1:], strict=False)
        if (when.year, when.month) != (previous.year, previous.month)
    )


def _close_input(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """The bar closes, declared — S-2's reasoning, unchanged."""
    closes = series.float_closes
    return IndicatorSeries(
        values=tuple(closes),
        universe=universe,
        not_evaluable_indices=tuple(i for i, value in enumerate(closes) if value is None),
    )


def return_series(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """``close(t) / close(t-63) - 1`` per bar — 63 intervals, 64 observations.

    S-2's interval convention (its 252/21 are interval counts), pinned here
    because "63-bar return" reads either way. Two kinds of absence, kept
    apart exactly as ``momentum_series`` keeps them: warm-up bars are ``None``
    and NOT in ``not_evaluable_indices`` (→ ``insufficient_warmup``); bars
    with a missing or non-positive endpoint are refused with the caller's
    reason. The non-positive guard is S-2's, for S-2's measured reason: a
    negative close ranks as a plausible number, which is worse than a crash.
    """
    closes = series.float_closes
    values: list[float | None] = []
    unevaluable: list[int] = []
    for index in range(len(closes)):
        if index < LOOKBACK_BARS:
            values.append(None)
            continue
        past = closes[index - LOOKBACK_BARS]
        now = closes[index]
        if past is None or now is None or past <= 0.0 or now <= 0.0:
            values.append(None)
            unevaluable.append(index)
            continue
        values.append(now / past - 1.0)
    return IndicatorSeries(values=tuple(values), universe=universe, not_evaluable_indices=tuple(unevaluable))


def s10_entry_member(
    series: BarSeries,
    *,
    panel_decision_dates: Set[date],
    universe: Universe,
    close_reason: NotEvaluableReason,
    regime: RegimeSeries,
) -> CrossSectionalMember:
    """One instrument's contribution to the ENTRY panel.

    ⚠ THE PRICE FLOOR IS AN ELIGIBILITY RULE, NOT AN EVALUABILITY ONE — S-2's
    words: a sub-$1 bar is simply not a decision bar and its verdict is
    ``not_fired``. It is therefore also not in this panel's denominator,
    which is what "the entry panel carries the floor" means.

    ⚠ THE REGIME IS A DECLARED INPUT, LAST — S-7's two rules: an unclassified
    date refuses as ``missing_market_context`` rather than reading as a
    judged decline, and it is declared last so an instrument defect headlines
    over the market-context fallback. A CLASSIFIED bar outside
    ``PERMITTED_REGIMES`` is inadmissible — ``not_fired`` even if the rank
    selects it, per the resolution rule.
    """
    if close_reason not in NOT_EVALUABLE_REASONS:
        raise ValueError(f"unknown reason code {close_reason!r}; must be one of {sorted(NOT_EVALUABLE_REASONS)}")
    if len(regime) != len(series):
        raise ValueError(f"regime series has {len(regime)} bars against {len(series)} price bars; they must align")

    closes = series.float_closes
    score = return_series(series, universe=universe)
    sma = sma_series(series, universe=universe, period=SMA_BARS)
    decision = frozenset(
        index
        for index, when in enumerate(series.dates)
        if when in panel_decision_dates and (close := closes[index]) is not None and close >= MIN_CLOSE
    )
    admissible = frozenset(
        index
        for index in decision
        if (close := closes[index]) is not None
        and (mean := sma.values[index]) is not None
        and close > mean
        and regime.permits(index, PERMITTED_REGIMES)
    )
    return CrossSectionalMember(
        dates=series.dates,
        inputs=(
            StrategyInput(series=_close_input(series, universe=universe), reason=close_reason),
            StrategyInput(series=score, reason=close_reason),
            StrategyInput(series=sma, reason=close_reason),
            StrategyInput(series=regime, reason="missing_market_context"),
        ),
        score=score,
        decision_indices=decision,
        admissible_indices=admissible,
    )


def s10_exit_member(
    series: BarSeries,
    *,
    panel_decision_dates: Set[date],
    universe: Universe,
    close_reason: NotEvaluableReason,
) -> CrossSectionalMember:
    """One instrument's contribution to the EXIT panel.

    Floor-free, regime-free — module docstring. Every rebalance bar is a
    decision bar; the below-SMA condition is MANDATORY (fires regardless of
    the band), the band itself comes from ``s10_exit_select``.
    """
    if close_reason not in NOT_EVALUABLE_REASONS:
        raise ValueError(f"unknown reason code {close_reason!r}; must be one of {sorted(NOT_EVALUABLE_REASONS)}")

    closes = series.float_closes
    score = return_series(series, universe=universe)
    sma = sma_series(series, universe=universe, period=SMA_BARS)
    decision = frozenset(index for index, when in enumerate(series.dates) if when in panel_decision_dates)
    mandatory = frozenset(
        index
        for index in decision
        if (close := closes[index]) is not None and (mean := sma.values[index]) is not None and close < mean
    )
    return CrossSectionalMember(
        dates=series.dates,
        inputs=(
            StrategyInput(series=_close_input(series, universe=universe), reason=close_reason),
            StrategyInput(series=score, reason=close_reason),
            StrategyInput(series=sma, reason=close_reason),
        ),
        score=score,
        decision_indices=decision,
        mandatory_indices=mandatory,
    )


def _ordered(scores: Mapping[int, float]) -> list[int]:
    """S-2's frozen total order: score descending, instrument id ascending."""
    return [key for key, _ in sorted(scores.items(), key=lambda item: (-item[1], item[0]))]


def s10_entry_select(when: date, scores: Mapping[int, float]) -> frozenset[int]:
    """The top decile of the entry panel — ``N // 10``, S-2's cut verbatim.

    The SMA condition is deliberately NOT here: selection sees scores only,
    and the winners it names are filtered by ``admissible_indices`` without
    backfilling — module docstring. ``when`` is in the signature for the
    contract's reason, unused by the rule.
    """
    count = len(scores) // ENTRY_DECILE
    if count <= 0:
        return frozenset()
    return frozenset(_ordered(scores)[:count])


def s10_exit_select(when: date, scores: Mapping[int, float]) -> frozenset[int]:
    """Everyone OUTSIDE the top three deciles — the set that fires the exit.

    The complement of the retention band ``ordered[: 3 * N // 10]``. Returned
    directly as the firing set (rather than returning the band and inverting
    at the caller) because ``select`` names winners, and for an exit leg the
    winners ARE the leavers.
    """
    keep = EXIT_BAND_DECILES * len(scores) // ENTRY_DECILE
    return frozenset(_ordered(scores)[keep:])


def s10_signals(
    panel: Mapping[int, BarSeries],
    *,
    universe: Universe,
    close_reason: NotEvaluableReason,
    regime_by_member: Mapping[int, RegimeSeries],
) -> dict[int, list[StrategySignal]]:
    """S-10 over a whole panel: both legs, one verdict list per member per leg.

    ⚠ HOLDS THE WHOLE PANEL IN MEMORY — S-2's warning verbatim: the right
    entry point for a bounded panel (a test, a watchlist); a full-corpus
    sweep streams members through the public pieces instead.
    """
    calendar = {when for series in panel.values() for when in series.dates}
    dates = s10_rebalance_dates(calendar)
    entries = evaluate_cross_sectional(
        members={
            key: s10_entry_member(
                series,
                panel_decision_dates=dates,
                universe=universe,
                close_reason=close_reason,
                regime=regime_by_member[key],
            )
            for key, series in panel.items()
        },
        select=s10_entry_select,
        min_participants=MIN_CROSS_SECTION,
        kind="entry",
    )
    exits = evaluate_cross_sectional(
        members={
            key: s10_exit_member(
                series,
                panel_decision_dates=dates,
                universe=universe,
                close_reason=close_reason,
            )
            for key, series in panel.items()
        },
        select=s10_exit_select,
        min_participants=MIN_CROSS_SECTION,
        kind="exit",
    )
    return {key: entries[key] + exits[key] for key in panel}


__all__ = [
    "ENTRY_DECILE",
    "EXIT_BAND_DECILES",
    "LOOKBACK_BARS",
    "MIN_CLOSE",
    "MIN_CROSS_SECTION",
    "PERMITTED_REGIMES",
    "S10_PARAMS",
    "S10_STRATEGY_ID",
    "SMA_BARS",
    "return_series",
    "s10_entry_member",
    "s10_entry_select",
    "s10_exit_member",
    "s10_exit_select",
    "s10_identity",
    "s10_rebalance_dates",
    "s10_signals",
]
