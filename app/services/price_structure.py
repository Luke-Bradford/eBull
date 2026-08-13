"""
Pure price-structure primitives — swings, levels, Fibonacci, anchored VWAP,
volatility regime.

Phase 2b of the TA strategy platform (#2279, parent #2240). Spec:
``docs/proposals/ta/2026-08-05-price-structure-primitives.md``.

All functions are pure — no DB, no I/O. They take oldest-first bars and return
plain values. This is the same contract ``technical_analysis.py`` holds, and it
is deliberate: the difference between the two modules is not purity, it is that
``technical_analysis`` answers "what is this number on the last N closes" while
this one produces objects with **identity over time** — a swing has a bar, a
level has a touch count, a leg has two anchors.

Three properties are load-bearing and none of them is an implementation detail:

1. **Confirmation lag is a constant.** A pivot at bar ``i`` is emitted with
   ``confirmed_index = i + n``, always. That is why the N-bar fractal was chosen
   over a percentage-deviation ZigZag, whose lag is data-dependent and was
   measured at up to 38 bars on our own panel (spec §2c). A consumer that gates
   on ``confirmed_index`` cannot leak look-ahead; one reading a ZigZag has to
   remember to, and a backtest that forgets looks excellent and is fiction.

2. **Every result carries a tri-state**, never a bare empty collection.
   "No swings in this series" and "this series is too short to say" are
   different facts, and collapsing them corrupts a win-rate denominator. This is
   the vacuous-truth class already in the prevention log and in the parent
   design's §5 decision 5.

3. **A masked bar cannot confirm anything.** Bars quarantined by
   ``price_quarantine`` arrive here with the offending fields set to ``None``
   (per-field, not per-bar — see ``StructureBar``). A pivot candidate at or
   beside a masked bar is *not evaluable*, never "no pivot": a spurious wick to
   0.010 must not be able to suppress a real swing low by looking like a lower
   low.

⚠ ``universe`` is a required keyword on every entry point and has no default.
The research corpus is survivor-only (#2284: every free source returns 0/259 on
the committed Form 25 cohort), and the corpus skill makes labelling mandatory.
A default would let a caller omit the label by accident, which is the one way a
label fails.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Literal

from app.services.technical_analysis import OHLCVRow, atr, bollinger_bands, sma

# ---------------------------------------------------------------------------
# Rule-set version
# ---------------------------------------------------------------------------
# Same pattern as price_quarantine.py, and for the same reason: an integer
# version cannot tell you whether two stored rows came from the same code, and a
# source hash can. Any edit to this module changes the version.
#
# ⚠ That over-invalidates — a comment edit bumps it too. This is the inherited,
# deliberate trade (price_quarantine.py: "makes every previously stored verdict
# visibly stale rather than silently mixed"). The alternative, hashing only the
# constants below, under-invalidates the moment a rule moves into a helper,
# which fails in the silent direction. Tests assert the version is derived from
# module source, NOT that it changes only on rule changes — the latter is both
# untestable and false.
RULE_SET_ID = "price-structure-v1"


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


RULE_SET_VERSION = f"{RULE_SET_ID}+{_code_hash()}"


# ---------------------------------------------------------------------------
# Frozen rule constants
# ---------------------------------------------------------------------------
# These are MODELLING constants, chosen from the calendar before any outcome
# existed to fit them to. They are not derived and they are not tuned. The guard
# against a later tuning pass is procedural, not intrinsic: they are hashed into
# RULE_SET_VERSION, so changing one invalidates every dependent signal visibly.
#
# ⚠ Do NOT read the ladder as "N cannot be fitted". It can — N changes signal
# frequency and timing as readily as a percentage threshold does. The claim is
# only that this ladder was not fitted here. (Spec §3, corrected after Codex.)
SWING_LADDER: dict[str, int] = {
    "short": 5,  # ~1 trading week either side
    "medium": 21,  # ~1 trading month either side
    "long": 63,  # ~1 trading quarter either side
}

#: Cluster / band tolerance as a multiple of ATR(14) at the governing bar.
#: Half a day's true range. ATR-relative rather than a percentage because a
#: fixed percentage means different things on a $3 stock and a $600 one, and
#: different things on the same stock in 2008 and 2017.
CLUSTER_ATR_K = 0.5

ATR_PERIOD = 14

#: Bollinger's Squeeze/Bulge is BandWidth at its lowest/highest in SIX MONTHS
#: (Bollinger, *Bollinger on Bollinger Bands*, ch. 21) — not a percentile cut.
#: Six months = 126 trading days.
BANDWIDTH_WINDOW = 20
BANDWIDTH_LOOKBACK = 126

FIB_RATIOS: tuple[float, ...] = (0.236, 0.382, 0.5, 0.618, 0.786)


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

State = Literal["fired", "not_fired", "not_evaluable"]

#: Provenance of the price series every result was computed over. Required, no
#: default — see the module docstring.
Universe = Literal["survivor_only", "survivorship_free"]

SwingKind = Literal["high", "low"]
LevelKind = Literal["resistance", "support"]
Interaction = Literal["break_up", "break_down", "touch", "none", "not_evaluable"]
Regime = Literal["compression", "expansion", "normal", "not_evaluable"]


@dataclass(frozen=True)
class StructureBar:
    """One bar, with quarantined fields already masked to ``None``.

    Masking is **per field**, because the quarantine carries two independent
    verdicts and they mean different things:

    - ``range_usable = False`` masks ``high`` and ``low`` (a spurious wick),
    - ``return_usable = False`` masks ``close`` (a bad print on the close).

    ``open`` has no verdict and no primitive here reads it for a decision; it is
    carried only so ``technical_analysis.atr`` can be reused unchanged rather
    than re-implemented (``atr`` reads high/low/close only, so masking the open
    cannot move a tolerance).

    ⚠ ``open`` IS ``Decimal | None`` since #2354, and the annotation was the
    defect. Both source columns are nullable, and the loader now masks a
    non-positive open per ``price_quarantine.rule_b1`` — but this field was
    declared non-optional, so every consumer type-checked as though a usable
    price were guaranteed while the runtime value could be ``None`` or ``0``.
    ``signal_ledger.resolve_fills`` was one of those consumers.
    """

    bar_date: date
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: int | None


@dataclass(frozen=True)
class Swing:
    """A confirmed N-bar fractal pivot.

    ``confirmed_index`` is the whole look-ahead defence: the pivot happened at
    ``index`` but was not knowable until ``index + n``. Both are emitted so a
    consumer never has to reconstruct the lag.
    """

    index: int
    bar_date: date
    kind: SwingKind
    price: float
    n: int
    confirmed_index: int
    confirmed_date: date


@dataclass(frozen=True)
class SwingSeries:
    state: State
    swings: tuple[Swing, ...]
    n: int
    bars_evaluated: int
    #: Indices where a pivot could not be decided because the candidate bar, or
    #: a bar in its comparison window, was masked. Without this, "masked" and
    #: "no swing here" are indistinguishable.
    not_evaluable_indices: tuple[int, ...]
    universe: Universe
    rule_set_version: str = RULE_SET_VERSION


@dataclass(frozen=True)
class Level:
    kind: LevelKind
    price_low: float
    price_high: float
    #: Unweighted arithmetic mean of the clustered swing prices. Not
    #: volume-weighted (5.16% of corpus bars have zero volume) and not
    #: time-decayed (that would be a second free parameter).
    price_mean: float
    touches: int
    first_touch_date: date
    last_touch_date: date


@dataclass(frozen=True)
class LevelSet:
    state: State
    levels: tuple[Level, ...]
    #: Swings whose ATR tolerance was not evaluable (a masked bar in the ATR
    #: window). Reported rather than merged on a fallback tolerance.
    unclustered: tuple[Swing, ...]
    universe: Universe
    rule_set_version: str = RULE_SET_VERSION


@dataclass(frozen=True)
class Leg:
    """An ordered swing pair — the anchor of a Fibonacci retracement."""

    start: Swing
    end: Swing
    direction: Literal["up", "down"]


@dataclass(frozen=True)
class FibRetracement:
    state: State
    direction: Literal["up", "down"] | None
    levels: dict[float, float]
    #: Not readable before this bar: the leg itself is not knowable until both
    #: anchors are confirmed.
    usable_from_index: int | None
    universe: Universe
    rule_set_version: str = RULE_SET_VERSION


@dataclass(frozen=True)
class AnchoredVwap:
    state: State
    value: float | None
    #: Bars with strictly positive volume in the anchored window. This is the
    #: denominator; a zero here is why the result is not_evaluable rather than
    #: 0.0.
    bars_with_volume: int
    anchor_index: int
    #: Distinct from ``anchor_index`` whenever the anchor is a swing: the sum
    #: starts at the pivot bar, but the *choice* of anchor was not knowable
    #: until the pivot was confirmed. A consumer that ignores this leaks
    #: look-ahead.
    usable_from_index: int
    universe: Universe
    rule_set_version: str = RULE_SET_VERSION


@dataclass(frozen=True)
class VolatilityRegime:
    regime: Regime
    bandwidth: float | None
    #: Fraction (0-100) of the trailing lookback BandWidths strictly less than
    #: the current one, current bar included. Weak percentile; diagnostic only —
    #: the classification uses Bollinger's own min/max rule.
    bandwidth_pct_rank: float | None
    universe: Universe
    rule_set_version: str = RULE_SET_VERSION


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _atr_at(bars: Sequence[StructureBar], index: int, period: int = ATR_PERIOD) -> float | None:
    """ATR(period) evaluated at ``index``, or None if the window is unusable.

    Fail-closed on masking: ``technical_analysis.atr`` needs non-null OHLC over
    ``period + 1`` bars, so a single masked bar anywhere in the window makes the
    tolerance not evaluable. The alternative — substituting a fallback — would
    silently widen or narrow a band by an unknown amount at exactly the bars the
    quarantine flagged as untrustworthy.
    """
    start = index - period
    if start < 0:
        return None
    window: list[OHLCVRow] = []
    for bar in bars[start : index + 1]:
        if bar.high is None or bar.low is None or bar.close is None:
            return None
        window.append(
            {
                # ⚠ `atr` reads high/low/close only, and the three are checked
                # non-None above. The open is carried for shape alone, so a
                # masked one cannot reach an arithmetic — the ignore is the same
                # one the four sibling call sites already carry.
                "open": bar.open,  # type: ignore[typeddict-item]
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            }
        )
    return atr(window, period)


def _state_for(found: bool, *, blinded: bool = False) -> State:
    """Tri-state from "did anything fire" plus "was any evidence hidden".

    ``blinded`` is the half that is easy to forget and expensive to get wrong.
    Finding nothing while some of the evidence was masked is NOT a negative — it
    is "cannot say" — and reporting it as ``not_fired`` feeds a quarantined bar
    into a win-rate denominator as an observed miss. Codex caught this module
    making exactly that mistake in three separate places on the first pass,
    which is why the rule lives in one helper now instead of at each call site.

    A positive result is still ``fired`` even when something was masked: the
    structure that WAS found is real, and the partial evidence is reported
    separately (``not_evaluable_indices``, ``unclustered``).
    """
    if found:
        return "fired"
    return "not_evaluable" if blinded else "not_fired"


# ---------------------------------------------------------------------------
# 1. Swings
# ---------------------------------------------------------------------------


def detect_swings(
    bars: Sequence[StructureBar],
    n: int,
    *,
    universe: Universe,
) -> SwingSeries:
    """N-bar fractal pivot highs and lows.

    A pivot high at ``i`` requires ``high[i]`` **strictly greater** than all 2n
    neighbours. A plateau (two equal highs in one window) therefore yields NO
    pivot rather than two: an equal high is the absence of a new extreme, and
    emitting both would cluster into a level with an inflated touch count — the
    count is the only thing a level asserts, so double-counting it is the one
    error that matters.

    Compared against ``scipy.signal.argrelextrema(order=n)``, which agrees to
    within boundary handling (spec §2). scipy is not a dependency here: it would
    be a new one for ~10 lines whose tie behaviour we have to pin down anyway.
    """
    if n < 1:
        raise ValueError("n must be >= 1")

    if len(bars) < 2 * n + 1:
        return SwingSeries(
            state="not_evaluable",
            swings=(),
            n=n,
            # 0, NOT len(bars). `bars_evaluated` counts bars at which a pivot
            # could be DECIDED, and a series shorter than the window decides
            # none. An earlier version returned len(bars) here while the normal
            # path returns len(bars) - 2n, so a consumer computing yield per
            # evaluated bar got two incomparable denominators depending on a
            # branch it could not see.
            bars_evaluated=0,
            not_evaluable_indices=(),
            universe=universe,
        )

    swings: list[Swing] = []
    not_evaluable: list[int] = []

    for i in range(n, len(bars) - n):
        undecided = False
        for kind in ("high", "low"):
            pivot = _pivot_at(bars, i, n, kind)  # type: ignore[arg-type]
            if pivot is None:
                undecided = True
            elif pivot:
                swings.append(
                    Swing(
                        index=i,
                        bar_date=bars[i].bar_date,
                        kind=kind,  # type: ignore[arg-type]
                        price=float(bars[i].high if kind == "high" else bars[i].low),  # type: ignore[arg-type]
                        n=n,
                        confirmed_index=i + n,
                        confirmed_date=bars[i + n].bar_date,
                    )
                )
        if undecided:
            not_evaluable.append(i)

    return SwingSeries(
        state=_state_for(bool(swings), blinded=bool(not_evaluable)),
        swings=tuple(swings),
        n=n,
        bars_evaluated=len(bars) - 2 * n,
        not_evaluable_indices=tuple(not_evaluable),
        universe=universe,
    )


def _pivot_at(
    bars: Sequence[StructureBar],
    i: int,
    n: int,
    kind: SwingKind,
) -> bool | None:
    """True/False/None (= not evaluable) for one pivot candidate.

    None whenever the candidate bar OR any bar in its comparison window is
    masked on the relevant field. A masked neighbour "cannot refute", so the
    honest answer is that we do not know — not that a pivot exists. Reporting a
    pivot here would be asserting a swing over a bar the quarantine says is not
    a price.
    """
    centre = bars[i].high if kind == "high" else bars[i].low
    if centre is None:
        return None
    for j in range(i - n, i + n + 1):
        if j == i:
            continue
        other = bars[j].high if kind == "high" else bars[j].low
        if other is None:
            return None
        if kind == "high":
            if other >= centre:
                return False
        elif other <= centre:
            return False
    return True


# ---------------------------------------------------------------------------
# 2. Level clustering
# ---------------------------------------------------------------------------


def cluster_levels(
    bars: Sequence[StructureBar],
    swings: SwingSeries,
    *,
    universe: Universe,
    k: float = CLUSTER_ATR_K,
) -> LevelSet:
    """Group swings into support / resistance levels.

    No published formulation exists for this (spec §0), so the rule is fixed by
    construction: single-linkage agglomeration on price, with an ATR-relative
    tolerance evaluated at the later swing's bar.

    Highs and lows cluster **separately**. A level asserts which side price
    approached it from, and merging swing highs with swing lows makes that
    unstateable.

    ``touches`` is emitted, never filtered on: a minimum-touch threshold baked
    in here would hide the denominator from the consumer that needs it.

    ⚠ Takes the whole ``SwingSeries``, NOT a bare ``Sequence[Swing]``, and that
    is a correctness requirement rather than convenience. With a bare sequence
    this function cannot tell "the detector found no swings" from "the detector
    could not evaluate the series at all" — both arrive as ``()`` — so it would
    report ``not_fired`` for a series that was too short or fully masked,
    reintroducing the tri-state collapse one level up the pipeline. Caught by
    the review bot after the same defect had already been fixed in three places
    inside this module; the type is now what prevents it, not vigilance.
    """
    # Upstream blinding travels: if the detector itself could not evaluate, no
    # level derived from its (empty) output is a negative result.
    if swings.state == "not_evaluable":
        return LevelSet(
            state="not_evaluable",
            levels=(),
            unclustered=(),
            universe=universe,
        )

    if not swings.swings:
        return LevelSet(
            state=_state_for(False, blinded=bool(swings.not_evaluable_indices)),
            levels=(),
            unclustered=(),
            universe=universe,
        )

    levels: list[Level] = []
    unclustered: list[Swing] = []

    for kind, level_kind in (("high", "resistance"), ("low", "support")):
        members = [s for s in swings.swings if s.kind == kind]
        tolerances: dict[int, float] = {}
        clusterable: list[Swing] = []
        for swing in members:
            bar_atr = _atr_at(bars, swing.index)
            if bar_atr is None:
                unclustered.append(swing)
                continue
            tolerances[swing.index] = k * bar_atr
            clusterable.append(swing)

        for group in _single_linkage(clusterable, tolerances):
            prices = [s.price for s in group]
            dates = sorted(s.bar_date for s in group)
            levels.append(
                Level(
                    kind=level_kind,  # type: ignore[arg-type]
                    price_low=min(prices),
                    price_high=max(prices),
                    price_mean=sum(prices) / len(prices),
                    touches=len(group),
                    first_touch_date=dates[0],
                    last_touch_date=dates[-1],
                )
            )

    levels.sort(key=lambda lv: (lv.kind, lv.price_mean))
    return LevelSet(
        # Every swing unclusterable (masked ATR window, or a series shorter than
        # the ATR warm-up) is "could not evaluate", not "no levels here".
        state=_state_for(
            bool(levels),
            blinded=bool(unclustered) or bool(swings.not_evaluable_indices),
        ),
        levels=tuple(levels),
        unclustered=tuple(unclustered),
        universe=universe,
    )


def _single_linkage(
    swings: Sequence[Swing],
    tolerances: dict[int, float],
) -> list[list[Swing]]:
    """Chain price-sorted swings into clusters.

    The tolerance for joining a pair is the one belonging to the **later** swing
    by index — the spec fixes this because "the ATR at the swing's bar" is
    ambiguous for a pair, and an unstated choice here is a silent parameter.
    """
    if not swings:
        return []
    ordered = sorted(swings, key=lambda s: s.price)
    groups: list[list[Swing]] = [[ordered[0]]]
    for prev, cur in zip(ordered, ordered[1:], strict=False):
        later = cur if cur.index >= prev.index else prev
        if cur.price - prev.price <= tolerances[later.index]:
            groups[-1].append(cur)
        else:
            groups.append([cur])
    return groups


# ---------------------------------------------------------------------------
# 3. Level interaction
# ---------------------------------------------------------------------------


def classify_interaction(
    level: Level,
    bars: Sequence[StructureBar],
    index: int,
    *,
    k: float = CLUSTER_ATR_K,
) -> Interaction:
    """Touch / break / none for one bar against one level.

    **Close-through, not wick-through.** A break needs the *close* strictly
    beyond the widened band; a wick beyond it with the close inside is a touch.
    Same call S7 (#2247) made for the quarantine rules and for the same reason —
    a wick is the part of a bar most likely to be a bad print, which is why
    ``range_usable = False`` exists at all (XPER 2024-06-03 closed at 8.298 with
    a low of 0.010).

    Equality is fail-closed: a close exactly on the band edge is INSIDE the band
    and reads as a touch, because a break requires strict inequality.
    """
    bar = bars[index]
    band = _atr_at(bars, index)
    if band is None or bar.close is None:
        return "not_evaluable"

    # Decimal(str(k)) * Decimal(str(band)), NOT Decimal(str(k * band)): the
    # latter performs the multiplication in float and only then converts, so the
    # band edge inherits float rounding error at exactly the boundary this
    # function compares with strict inequality.
    # ⚠ Residual, stated rather than papered over: `band` comes from
    # technical_analysis.atr, which returns a float, so the edge is still only
    # as precise as that. This removes the compounding, not the source.
    tol = Decimal(str(k)) * Decimal(str(band))
    band_low = Decimal(str(level.price_low)) - tol
    band_high = Decimal(str(level.price_high)) + tol

    if bar.close > band_high:
        return "break_up"
    if bar.close < band_low:
        return "break_down"

    if bar.high is None or bar.low is None:
        return "not_evaluable"
    if bar.low <= band_high and bar.high >= band_low:
        return "touch"
    return "none"


@dataclass(frozen=True)
class BreakRetest:
    direction: Literal["up", "down"]
    break_index: int
    retest_index: int
    confirm_index: int


@dataclass(frozen=True)
class BreakRetestSet:
    state: State
    patterns: tuple[BreakRetest, ...]
    universe: Universe
    rule_set_version: str = RULE_SET_VERSION


def find_break_and_retest(
    level: Level,
    bars: Sequence[StructureBar],
    *,
    universe: Universe,
    max_retest_bars: int,
    k: float = CLUSTER_ATR_K,
) -> BreakRetestSet:
    """Break → retest → confirmation, as an explicit state machine.

    Every transition is stated because an unstated one is a silent parameter:

    - **break** — the first close strictly beyond the band.
    - **retest** — the first later bar whose [low, high] re-intersects the band,
      within ``max_retest_bars`` of the break.
    - **confirm** — the first later close again strictly beyond the band on the
      break side.
    - **invalidation** — a close strictly beyond the band on the OPPOSITE side
      at any point before confirmation voids the pattern; so does a retest that
      does not confirm within ``max_retest_bars`` of itself. The machine resets
      either way, so one level can yield several independent patterns.
    - **gap-over** — a bar that gaps entirely past the band simply is not a
      retest; the window continues. Running out of window is ``not_fired``, NOT
      ``not_evaluable``: the absence was observed, not unmeasurable.

    ``max_retest_bars`` is derived from the ladder rung the level was built at
    (2N), never a new constant.
    """
    patterns: list[BreakRetest] = []
    seen_evaluable = False
    blinded = False

    state: Literal["idle", "broken", "retested"] = "idle"
    direction: Literal["up", "down"] = "up"
    break_index = retest_index = 0

    for i in range(len(bars)):
        verdict = classify_interaction(level, bars, i, k=k)
        if verdict == "not_evaluable":
            # ⚠ A hidden bar INSIDE an active sequence voids it. Skipping past
            # one and carrying the state forward would emit a break→retest→
            # confirm spanning a bar the quarantine says is not a price — and
            # that bar could have been the opposite-side close that invalidated
            # the whole setup. Emitting a positive signal across hidden evidence
            # is the worst direction this module can fail in, so the sequence
            # resets and the series is marked blinded.
            if state != "idle":
                state = "idle"
                blinded = True
            continue
        seen_evaluable = True

        # ⚠ Expire a stale sequence BEFORE interpreting this bar, not after.
        # The timeout branches used to reset and `continue`, which swallowed a
        # break landing ON the timeout bar — while the opposite-verdict branch
        # below correctly kept one. Two rules for the same event, differing only
        # by which branch happened to see it first. Expiring up front means one
        # rule: a bar is interpreted against the state that is live when it
        # arrives, and an expired sequence is simply not live.
        if state == "broken" and i - break_index > max_retest_bars:
            state = "idle"
        elif state == "retested" and i - retest_index > max_retest_bars:
            state = "idle"

        if state == "idle":
            if verdict in ("break_up", "break_down"):
                state = "broken"
                direction = "up" if verdict == "break_up" else "down"
                break_index = i
            continue

        opposite = "break_down" if direction == "up" else "break_up"
        same = "break_up" if direction == "up" else "break_down"

        if verdict == opposite:
            # A close through the other side voids the pattern outright. Reset
            # to broken-in-the-new-direction rather than idle: this bar IS a
            # break, and dropping it would lose a real occurrence.
            state = "broken"
            direction = "up" if verdict == "break_up" else "down"
            break_index = i
            continue

        if state == "broken":
            # A further break in the SAME direction while already broken is not
            # a new occurrence — the level was broken at the first one, and
            # re-anchoring here would let price walking away from the level
            # extend the retest window indefinitely.
            if verdict == "touch":
                state = "retested"
                retest_index = i
            continue

        # state == "retested" — and still inside the window, since an expired
        # one was reset above. So a same-direction close IS the confirmation.
        if verdict == same:
            patterns.append(
                BreakRetest(
                    direction=direction,
                    break_index=break_index,
                    retest_index=retest_index,
                    confirm_index=i,
                )
            )
            state = "idle"

    if not seen_evaluable:
        return BreakRetestSet(state="not_evaluable", patterns=(), universe=universe)
    return BreakRetestSet(
        state=_state_for(bool(patterns), blinded=blinded),
        patterns=tuple(patterns),
        universe=universe,
    )


# ---------------------------------------------------------------------------
# 4. Fibonacci retracement
# ---------------------------------------------------------------------------


def select_leg(swings: Sequence[Swing]) -> Leg | None:
    """The most recent completed leg: last swing, plus the last opposite before it.

    Fractals do NOT alternate — a run of three swing highs with no low between
    them is ordinary — so "the last high and the last low" does not determine a
    leg. Taking the last swing as the leg end and walking back to the most
    recent swing of the opposite kind is deterministic under any such run, and
    it is the charting convention (the most recent completed major swing).
    """
    if len(swings) < 2:
        return None
    ordered = sorted(swings, key=lambda s: s.index)
    end = ordered[-1]
    for candidate in reversed(ordered[:-1]):
        if candidate.kind != end.kind:
            return Leg(
                start=candidate,
                end=end,
                direction="up" if end.kind == "high" else "down",
            )
    return None


def fib_levels(leg: Leg | None, *, universe: Universe) -> FibRetracement:
    """Retracement levels on a leg.

    Direction decides the arithmetic, and an earlier draft of the spec omitted
    it — two anchors alone do not determine whether levels are measured down
    from the high or up from the low:

    - up-leg (leg ends on a high): ``high - r * (high - low)``
    - down-leg (leg ends on a low): ``low + r * (high - low)``

    ``usable_from_index`` is the later of the two anchors' confirmations. The
    retracement is not knowable before it, whatever the arithmetic says.
    """
    if leg is None:
        return FibRetracement(
            state="not_evaluable",
            direction=None,
            levels={},
            usable_from_index=None,
            universe=universe,
        )

    high = max(leg.start.price, leg.end.price)
    low = min(leg.start.price, leg.end.price)
    span = high - low
    if span <= 0:
        return FibRetracement(
            state="not_evaluable",
            direction=leg.direction,
            levels={},
            usable_from_index=None,
            universe=universe,
        )

    if leg.direction == "up":
        levels = {r: high - r * span for r in FIB_RATIOS}
    else:
        levels = {r: low + r * span for r in FIB_RATIOS}

    return FibRetracement(
        state="fired",
        direction=leg.direction,
        levels=levels,
        usable_from_index=max(leg.start.confirmed_index, leg.end.confirmed_index),
        universe=universe,
    )


# ---------------------------------------------------------------------------
# 5. Anchored VWAP
# ---------------------------------------------------------------------------


def anchored_vwap(
    bars: Sequence[StructureBar],
    anchor: Swing | int,
    *,
    universe: Universe,
    end_index: int | None = None,
) -> AnchoredVwap:
    """Cumulative VWAP from an anchor bar, typical price = (H + L + C) / 3.

    Typical price is a **choice**, not a standard — HLC3 is the daily-bar
    convention (TradingView, Sierra Chart). A tick-level VWAP would use trade
    prices, which daily OHLCV cannot supply.

    **Two indices come back, and they differ whenever the anchor is a swing.**
    The sum starts at the pivot bar, which is economically the anchor. But the
    *choice* of that anchor was not knowable until the pivot was confirmed, so
    ``usable_from_index`` is the confirmation. A consumer reading ``value`` at a
    bar before it has leaked look-ahead.

    Zero-volume bars contribute nothing AND are excluded from
    ``bars_with_volume``. That count is what makes the difference observable: if
    it reaches zero the result is ``not_evaluable``, never ``0.0``. Volume is
    never NULL in the research corpus (measured, 25,818,944 of 25,818,944), but
    NULL is treated as no-volume rather than as zero anyway — this module is not
    promised only that corpus.
    """
    anchor_index = anchor.index if isinstance(anchor, Swing) else anchor
    usable_from = anchor.confirmed_index if isinstance(anchor, Swing) else anchor_index
    last = len(bars) - 1 if end_index is None else end_index

    if not bars or anchor_index < 0 or last >= len(bars) or last < anchor_index:
        return AnchoredVwap(
            state="not_evaluable",
            value=None,
            bars_with_volume=0,
            anchor_index=anchor_index,
            usable_from_index=usable_from,
            universe=universe,
        )

    numerator = Decimal(0)
    denominator = Decimal(0)
    counted = 0
    for bar in bars[anchor_index : last + 1]:
        if bar.volume is None or bar.volume <= 0:
            continue
        if bar.high is None or bar.low is None or bar.close is None:
            continue
        typical = (bar.high + bar.low + bar.close) / Decimal(3)
        volume = Decimal(bar.volume)
        numerator += typical * volume
        denominator += volume
        counted += 1

    if counted == 0 or denominator == 0:
        return AnchoredVwap(
            state="not_evaluable",
            value=None,
            bars_with_volume=0,
            anchor_index=anchor_index,
            usable_from_index=usable_from,
            universe=universe,
        )

    return AnchoredVwap(
        state="fired",
        value=float(numerator / denominator),
        bars_with_volume=counted,
        anchor_index=anchor_index,
        usable_from_index=usable_from,
        universe=universe,
    )


# ---------------------------------------------------------------------------
# 6. Volatility regime
# ---------------------------------------------------------------------------


def volatility_regime(
    closes: Sequence[Decimal | None],
    *,
    universe: Universe,
    window: int = BANDWIDTH_WINDOW,
    lookback: int = BANDWIDTH_LOOKBACK,
) -> VolatilityRegime:
    """Bollinger Squeeze / Bulge on BandWidth.

    ⚠ This implements **Bollinger's own rule**: the Squeeze is BandWidth at its
    lowest value in six months and the Bulge is its highest (*Bollinger on
    Bollinger Bands*, ch. 21). It is NOT a 20th/80th-percentile cut — an earlier
    draft of the spec invented that, and inventing a threshold where the source
    states one is exactly what "source rule before design" forbids. Six months =
    126 trading days.

    The percentile is still returned, as a continuous diagnostic. It is a *weak*
    percentile (strictly-less), current bar included — the tie method changes
    the number, so it is stated rather than left to the reader.

    Warm-up is ``window + lookback - 1``: the first BandWidth needs ``window``
    closes and each later one needs one more, so ``lookback`` BandWidths
    including the current one need 145 closes at the defaults.
    """
    needed = window + lookback - 1
    usable = [c for c in closes[-needed:]] if len(closes) >= needed else []
    if len(usable) < needed or any(c is None for c in usable):
        return VolatilityRegime(
            regime="not_evaluable",
            bandwidth=None,
            bandwidth_pct_rank=None,
            universe=universe,
        )

    series: list[Decimal] = [c for c in usable if c is not None]
    widths: list[float] = []
    for end in range(window, len(series) + 1):
        chunk = series[end - window : end]
        bands = bollinger_bands(chunk, window)
        middle = sma(chunk, window)
        if bands is None or middle is None or middle <= 0:
            # A non-positive middle is a real state on this corpus (2 bars close
            # at or below zero, 9,351 sub-penny) and dividing by it is nonsense.
            return VolatilityRegime(
                regime="not_evaluable",
                bandwidth=None,
                bandwidth_pct_rank=None,
                universe=universe,
            )
        widths.append((bands[0] - bands[1]) / middle)

    current = widths[-1]
    rank = 100.0 * sum(1 for w in widths if w < current) / len(widths)

    if current == min(widths):
        regime: Regime = "compression"
    elif current == max(widths):
        regime = "expansion"
    else:
        regime = "normal"

    return VolatilityRegime(
        regime=regime,
        bandwidth=current,
        bandwidth_pct_rank=rank,
        universe=universe,
    )


__all__ = [
    "ATR_PERIOD",
    "BANDWIDTH_LOOKBACK",
    "BANDWIDTH_WINDOW",
    "CLUSTER_ATR_K",
    "FIB_RATIOS",
    "RULE_SET_ID",
    "RULE_SET_VERSION",
    "SWING_LADDER",
    "AnchoredVwap",
    "BreakRetest",
    "BreakRetestSet",
    "FibRetracement",
    "Leg",
    "Level",
    "LevelSet",
    "State",
    "StructureBar",
    "Swing",
    "SwingSeries",
    "Universe",
    "VolatilityRegime",
    "anchored_vwap",
    "classify_interaction",
    "cluster_levels",
    "detect_swings",
    "fib_levels",
    "find_break_and_retest",
    "select_leg",
    "volatility_regime",
]
