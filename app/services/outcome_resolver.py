"""Phase 4a — the outcome resolver.

Spec: ``docs/proposals/ta/2026-08-06-outcome-resolver.md``. Predecessors:
``app/services/strategy_registry.py`` (3a), ``app/services/signal_ledger.py``
(3c). Refs #2240, #2245, #2288.

The ledger records what fired and where it filled. This module answers what
happened NEXT, for one filled long entry under a TP/SL bracket with a max-hold
expiry: ``tp_hit`` / ``sl_hit`` / ``expired`` / ``ambiguous`` (design-doc §3
decision 1), plus ``unresolved`` — ours, argued for in the spec's §3.4.

⚠⚠ THIS MODULE READS BARS AFTER THE FILL. That is its job, and it is why the
guard here is different in kind from phase 3's.

Phase 3 made look-ahead UNEXPRESSIBLE: a ``StrategySignal`` carries a bar index
and no fill field, so a strategy cannot request a same-bar fill. Nothing of the
sort is available here — every bar this module touches is in the future of the
decision. So the discipline is instead:

1. **The fill bar is inside the window and the entry is its OPEN**, both
   supplied, both validated against each other. This module resolves nothing
   about the entry; phase 3c owns that.
2. **A max-hold exit fills at the NEXT open**, never at the window's last close
   — parent §3.5.1 applies the fill rule to "entries and exits alike", and
   booking the last close would be the same-bar fill the whole phase exists to
   prevent.
3. **Refusals are counted, never absorbed.** ``ambiguous`` is never resolved
   favourably (§3.5.4), and a window whose evidence runs out is ``unresolved``
   rather than ``expired``.

⚠ LONG ONLY. ``stop_loss < entry`` is validated, and ``entry < take_profit``
whenever a target exists. That is the repo's v1 risk posture
(``.claude/CLAUDE.md``), not an oversight — a short bracket inverts every
comparison below and would be a different rule set with a different version,
not a flag on this one.

⚠ A bracket may be STOP-ONLY (``take_profit is None``). S-7 is the case: §3
gives it a stop, an exit signal and a hold cap, and no target — the first
strategy between the signal-pair shape (S-1/S-3, no levels at all) and the
full-bracket shape (S-4/S-5/S-6/S-8/S-9). With no target, precedence rows 2, 3
and 5 below are simply unreachable — nothing reorders — and the close comes
from the stop, the position layer's exit signal, or the hold cap.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Literal, get_args

from app.services.indicator_series import BarSeries

# Same construction as indicator_series / price_quarantine / price_structure: a
# stable id plus a hash of THIS MODULE'S SOURCE. Criterion 11 makes the
# execution assumption part of identity, and the two constructions this module
# makes (gap-through resolves at the open; max-hold exits at the next open) live
# nowhere else — so they are versioned here and stamped on every outcome.
RULE_SET_ID = "outcome-resolver-v1"


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


RULE_SET_VERSION = f"{RULE_SET_ID}+{_code_hash()}"


#: The parent's four (design-doc §3 decision 1) plus ``unresolved``.
OutcomeClass = Literal["tp_hit", "sl_hit", "expired", "ambiguous", "unresolved"]

#: ⚠ CLOSED vocabulary, for criterion 9's "measure what you reject": free text
#: cannot be counted. ``series_break`` and ``quarantined_bar`` are criterion 8's
#: own names and are pinned against ``strategy_registry`` by test.
#: ``window_truncated``, ``missing_bar_data`` and ``unorderable_exit_levels``
#: are OURS and are flagged as additions rather than smuggled in — see the
#: spec's §3.4.
UnresolvedReason = Literal[
    "window_truncated",
    "series_break",
    "quarantined_bar",
    "missing_bar_data",
    "unorderable_exit_levels",
]

#: ⚠ One member in v1, and it is not decoration. S5 (#2245) established that a
#: historical bar can NEVER be resolved intraday — the eToro candle endpoint has
#: no date parameter, no offset and no cursor — while a forward-going signal can
#: be, inside a ~2-session window. Without this stamp a later intraday-backed
#: resolution would mix silently into the same statistics.
ResolutionMethod = Literal["daily_bar"]

# ⚠ DERIVED from the Literals, never restated — the closed-vocabulary-in-N-places
# defect the prevention log carries from #2218.
OUTCOME_CLASSES: frozenset[str] = frozenset(get_args(OutcomeClass))
UNRESOLVED_REASONS: frozenset[str] = frozenset(get_args(UnresolvedReason))
RESOLUTION_METHODS: frozenset[str] = frozenset(get_args(ResolutionMethod))

#: Ours, kept as an explicit subtraction so adopting a parent code later cannot
#: silently land on our side of the line. Same construction as
#: ``strategy_registry.OUR_ADDITIONAL_REASON_CODES``.
OUR_ADDITIONAL_REASONS: frozenset[str] = frozenset({"window_truncated", "missing_bar_data", "unorderable_exit_levels"})
INHERITED_REASONS: frozenset[str] = UNRESOLVED_REASONS - OUR_ADDITIONAL_REASONS

#: Outcomes that book a trade. The other two — ``ambiguous`` and ``unresolved``
#: — carry no exit price and no return, because §3.5.4 excludes ambiguous
#: outcomes from the win rate and a populated return column is a column
#: something eventually averages.
_BOOKED: frozenset[str] = frozenset({"tp_hit", "sl_hit", "expired"})


@dataclass(frozen=True)
class ExitLevels:
    """The bracket, as absolute prices, plus the max hold.

    ⚠ NOT computed here. These are strategy parameters and criterion 11 puts
    every parameter inside the strategy identity hash, so a resolver that
    invented them would put the same numbers in two places that can disagree.

    ⚠ S5 corrected the parent ticket's premise on this:
    ``entry_timing._compute_take_profit`` reads a thesis ``base_value`` — a
    fundamental valuation target — and returns ``None`` without one. It is not
    ATR-based and does not apply to a TA strategy.

    ⚠ ``take_profit is None`` is a STOP-ONLY bracket (see the module
    docstring), not an omission: the field has no default, so a caller states
    the ``None`` visibly (#2288 — a field with a default is a field a writer
    can forget). A stop is still mandatory; a strategy with neither is not
    level-based and never constructs this class (S-3's recorded shape).
    """

    take_profit: Decimal | None
    stop_loss: Decimal
    max_hold_bars: int

    def __post_init__(self) -> None:
        if self.max_hold_bars < 1:
            raise ValueError(f"max_hold_bars must be >= 1, got {self.max_hold_bars}")
        if self.stop_loss <= 0:
            raise ValueError(f"stop_loss must be > 0, got {self.stop_loss}")
        if self.take_profit is not None and self.stop_loss >= self.take_profit:
            raise ValueError(
                f"stop_loss {self.stop_loss} is not below take_profit {self.take_profit} "
                "— this resolver is long-only (.claude/CLAUDE.md risk posture)"
            )


@dataclass(frozen=True)
class Outcome:
    """What happened to one filled entry.

    The validation below is the same shape as ``signal_ledger.LedgerRow``'s: it
    states the invariants a stored row must satisfy so a bad one fails at
    construction with a message naming the field, rather than reaching a
    reporting query.
    """

    outcome: OutcomeClass
    resolution_method: ResolutionMethod
    rule_set_version: str
    #: Required exactly when ``outcome == "unresolved"``, forbidden otherwise.
    reason: UnresolvedReason | None = None
    #: ⚠ The DATE is recorded alongside the index. The ledger keys on dates, and
    #: an index is not durable across a corpus rebuild or a re-segmentation.
    exit_index: int | None = None
    exit_bar_date: date | None = None
    exit_price: Decimal | None = None
    #: ``exit_index - fill_index``. ⚠ **0 for a TP/SL touched on the fill bar**,
    #: which is correct as a bar count and is NOT exposure time — criterion 7's
    #: exposure metric is phase 5's and must be defined there, not read off this.
    bars_held: int | None = None
    #: ⚠ GROSS. Criterion 2 requires costs per trade and this number has none;
    #: the name is the guard against something downstream averaging it as
    #: performance.
    gross_return_pct: Decimal | None = None

    def __post_init__(self) -> None:
        if self.outcome not in OUTCOME_CLASSES:
            raise ValueError(f"unknown outcome {self.outcome!r}; must be one of {sorted(OUTCOME_CLASSES)}")
        if self.resolution_method not in RESOLUTION_METHODS:
            raise ValueError(
                f"unknown resolution_method {self.resolution_method!r}; must be one of {sorted(RESOLUTION_METHODS)}"
            )
        if self.reason is not None and self.reason not in UNRESOLVED_REASONS:
            raise ValueError(f"unknown reason {self.reason!r}; must be one of {sorted(UNRESOLVED_REASONS)}")
        if (self.outcome == "unresolved") != (self.reason is not None):
            raise ValueError(
                f"outcome {self.outcome!r} and reason {self.reason!r} disagree: "
                "a reason is required exactly when the outcome is unresolved"
            )

        # ⚠ COUNTED, not ANDed — the exact defect Codex caught at phase 3c's
        # checkpoint 2 and which is now in the prevention log. `a is not None
        # and b is not None` reads as "has a value" and silently admits HALF a
        # value: an unresolved row carrying an exit_index with no date scores
        # False on that expression, matches `outcome == "unresolved"`, and
        # passes. Counting makes the two fields move together or not at all.
        located = (self.exit_index is not None) + (self.exit_bar_date is not None) + (self.bars_held is not None)
        if located != (0 if self.outcome == "unresolved" else 3):
            raise ValueError(
                f"outcome {self.outcome!r} carries a partial exit location "
                f"{(self.exit_index, self.exit_bar_date, self.bars_held)!r}: an unresolved outcome has none of "
                "the three and every other outcome has all three"
            )

        booked = (self.exit_price is not None) + (self.gross_return_pct is not None)
        if booked != (2 if self.outcome in _BOOKED else 0):
            raise ValueError(
                f"outcome {self.outcome!r} carries {(self.exit_price, self.gross_return_pct)!r}: "
                "a price and a return exist exactly for tp_hit / sl_hit / expired, and move together"
            )

        if self.bars_held is not None and self.bars_held < 0:
            raise ValueError(f"bars_held must be non-negative, got {self.bars_held} — the exit precedes the fill")


def _unresolved(reason: UnresolvedReason) -> Outcome:
    return Outcome(
        outcome="unresolved",
        resolution_method="daily_bar",
        rule_set_version=RULE_SET_VERSION,
        reason=reason,
    )


def _booked(
    outcome: OutcomeClass,
    *,
    series: BarSeries,
    fill_index: int,
    exit_index: int,
    exit_price: Decimal,
    entry_price: Decimal,
) -> Outcome:
    return Outcome(
        outcome=outcome,
        resolution_method="daily_bar",
        rule_set_version=RULE_SET_VERSION,
        exit_index=exit_index,
        exit_bar_date=series.dates[exit_index],
        exit_price=exit_price,
        bars_held=exit_index - fill_index,
        gross_return_pct=(exit_price - entry_price) / entry_price,
    )


def resolve_outcome(
    *,
    series: BarSeries,
    fill_index: int,
    entry_price: Decimal,
    levels: ExitLevels,
    masked_bar_reasons: Mapping[int, UnresolvedReason],
    segment_end_index: int | None,
) -> Outcome:
    """Classify one filled long entry.

    ``fill_index`` is bar ``f``, the bar whose OPEN was the fill (phase 3c
    resolved it as ``signal_index + 1``). The holding window is
    ``f … f + max_hold_bars - 1``, **inclusive of f** — we bought at its open,
    so the rest of that bar's range is real forward information and excluding it
    would understate both levels. Excluding it is the more common error and the
    flattering one for tight stops.

    Per bar, in this order (spec §3.2), with ``S`` the stop and ``T`` the target::

        1. open <= S            -> sl_hit  at open
        2. open >= T            -> tp_hit  at open
        3. low <= S and high>=T -> ambiguous
        4. low <= S             -> sl_hit  at S
        5. high >= T            -> tp_hit  at T

    ⚠ Rules 1 and 2 are NOT a tie-break heuristic. **The open is the first price
    of the bar** — definitional in OHLC, not a chosen rule — so a bar that gaps
    through a level has a KNOWN touch order and is not ambiguous. They cannot
    both hold, because ``S < T``. Neither can fire on the fill bar itself, since
    the entry IS ``open[f]`` and ``S < E < T``: on that bar the bracket is placed
    at the open, simultaneously with entry, so there is no window in which it
    could have been gapped through.

    ⚠ Rule 3 requires the whole bar and must never be weakened by a proximity
    argument. §3.5.4: *"the order of touch is unknowable from OHLC … Silently
    resolving them favourably is how backtests manufacture edge."* S5 measured
    the class at 0.83% of signals at a 1.0×ATR target — ~212,000 of 25.5M —
    which is small and is not nothing.

    ⚠ And never "assume SL first for conservatism". S5: *"It is not
    conservative, it is a different bias — it makes TP-first strategies look
    systematically worse, and the distortion scales with how tight the TP is."*

    ⚠ A STOP-ONLY bracket (``take_profit is None``) reaches rules 1 and 4 only;
    rules 2, 3 and 5 test a target that does not exist and cannot fire. With one
    level there is no unknowable touch order, so ``ambiguous`` is unreachable
    for such a bracket by construction — not by a resolution policy.

    REFUSALS, and why each is not ``expired``
    -----------------------------------------
    Checked per bar before the rules, in this order: past the end of the series
    (``window_truncated``), past ``segment_end_index`` (``series_break``), then
    a ``NULL`` field we must read — reported as whatever ``masked_bar_reasons``
    declares for that bar, or ``missing_bar_data`` if it declares nothing.

    ⚠ Only reachable while the window is UNDECIDED. A target touched on bar 3 of
    a window that is later truncated is ``tp_hit``; a shorter window cannot
    un-hit a level that was already hit, and a masked bar after the decisive one
    is irrelevant.

    ⚠ ``expired`` must not absorb ``window_truncated``. Booking a return for a
    window that never ran is a one-directional bias, and it lands on every open
    trade at the corpus edge — the most recent and most operator-relevant slice.

    MASKING IS PER FIELD (spec §3.5)
    --------------------------------
    Adopted from ``research_price_structure_store.load_masked_series``, which is
    the settled treatment: *"`range_usable = False` is a bad wick (masks
    high/low), `return_usable = False` is a bad close (masks close). Masking the
    whole bar on either verdict would discard good data."* So a masked-range bar
    arrives with ``high``/``low`` already ``None`` and still serves as a valid
    **expiry exit** through its ``open``, which a whole-bar rejection would have
    thrown away.

    ⚠ A touch test needs ``open``, ``high`` AND ``low`` — not just the range.
    Without the open we cannot tell rule 1 from rule 4, and those disagree on
    the exit PRICE; nor rule 1 from rule 3, and those disagree on the class.

    ⚠⚠ ``masked_bar_reasons`` ANNOTATES; it does not refuse. **The absent fields
    do the refusing.** That split is what keeps the per-field rule intact: a
    map that refused whole bars would throw away the good ``open`` of a
    range-masked bar, which is precisely the *"masking the whole bar on either
    verdict would discard good data"* the loader warns against. So a caller
    that does not trust a field masks that field, and uses this map only to say
    WHY — the same split phase 3a made for ``StrategyInput.reason``, and for the
    same criterion-8 reason: a declared reason wins over the ``missing_bar_data``
    fallback so a quarantine verdict and a data gap never collapse into one
    another.

    ⚠ Consequence, stated because it is a footgun otherwise: **annotating a bar
    whose fields are all present is a no-op.** To refuse such a bar — a `B1`/`B4`
    sentinel whose OPEN is untrustworthy too, a `provisional` part-session bar —
    the caller masks the field it does not trust. ⚠ ``load_masked_series`` masks
    ``high``/``low``/``close`` on the two quarantine axes, and **since #2354 it
    also masks a non-positive open** — ``rule_b1``'s own clause applied to the
    field neither axis covers. This paragraph used to hand that job to the
    caller (*"a caller reading `B4` bars must mask the open itself"*) and no
    caller ever did it, which is the prevention log's *"an obligation written in
    the docstring of the module that CANNOT discharge it is the weakest form of
    a rule there is"*. A caller loading bars by some OTHER route still owes the
    mask; ``signal_ledger.resolve_fills`` refuses one independently.

    ⚠ ``masked_bar_reasons`` and ``segment_end_index`` are REQUIRED and have no
    defaults. #2288's lesson: a field with a default is a field a writer can
    forget. Forgetting ``segment_end_index`` spans a level break; forgetting the
    reason map collapses every quarantine verdict into ``missing_bar_data``,
    which is exactly the countability criterion 8 exists to protect. A caller
    with nothing to declare passes ``{}`` and ``None``, visibly.

    ⚠ Two loader obligations this module cannot check: the bars must come from a
    FAIL-CLOSED loader (the quarantine tables are sparse, so absence of a row
    means "clean" OR "never evaluated"), and ``provisional`` part-session bars —
    whose high and low are partial counts — must be MASKED by the caller, not
    merely annotated.

    A BREAK IS A SEGMENT BOUNDARY, NOT A BAD BAR
    --------------------------------------------
    ⚠ ``price_series_break.break_date`` is *"the bar at the NEW scale"* and
    ``price_transition_quarantine`` is *"keyed on the LATER bar"*, because *"the
    defect lives on a transition, not a bar … Bars either side of a level break
    are valid prices in their own unit regime."* A trade entered on or after the
    break is entirely within the new scale and is perfectly resolvable — masking
    the break bar would reject it. Hence ``segment_end_index``, which is
    design-doc decision 10's own *"per-instrument segment model"*, rather than an
    extra entry in ``masked_bar_reasons``.
    """
    n_bars = len(series)
    if not 0 <= fill_index < n_bars:
        raise ValueError(f"fill_index {fill_index} is outside the {n_bars}-bar series")
    if entry_price <= 0:
        raise ValueError(f"entry_price must be > 0, got {entry_price} — gross_return_pct divides by it")

    fill_open = series.rows[fill_index].get("open")
    if fill_open is None:
        raise ValueError(
            f"bar {fill_index} ({series.dates[fill_index]}) has no open, so it cannot be a fill bar "
            "— phase 3c records no_fill_bar for exactly this case and never produces a fill here"
        )
    # ⚠ The entry is PASSED IN and checked, not re-read. Passing it catches a
    # caller that computed a fill some other way; checking it catches a stored
    # ledger row being resolved against a corpus that has since been rebuilt or
    # re-adjusted, where a silent re-read would quietly reinterpret a recorded
    # decision (phase 3 spec §2.1).
    if entry_price != fill_open:
        raise ValueError(
            f"entry_price {entry_price} disagrees with open[{fill_index}] = {fill_open} on "
            f"{series.dates[fill_index]} — the fill and the series must come from the same corpus"
        )
    if levels.stop_loss >= entry_price:
        raise ValueError(
            f"stop {levels.stop_loss} is not below the entry {entry_price}: "
            "a stop at or above entry triggers immediately and is not a trade"
        )
    if levels.take_profit is not None and entry_price >= levels.take_profit:
        raise ValueError(
            f"target {levels.take_profit} is not above the entry {entry_price}: "
            "a target at or below entry triggers immediately and is not a trade"
        )
    if segment_end_index is not None and segment_end_index < fill_index:
        raise ValueError(
            f"segment_end_index {segment_end_index} precedes fill_index {fill_index} "
            "— the fill bar must be inside its own segment"
        )

    stop, target = levels.stop_loss, levels.take_profit

    def structural_refusal(index: int) -> UnresolvedReason | None:
        """Why bar ``index`` is out of reach, before any field is read."""
        if index >= n_bars:
            return "window_truncated"
        if segment_end_index is not None and index > segment_end_index:
            return "series_break"
        return None

    def masked(index: int) -> Outcome:
        """A field we must read is absent. The caller names why, or we say so."""
        return _unresolved(masked_bar_reasons.get(index, "missing_bar_data"))

    for index in range(fill_index, fill_index + levels.max_hold_bars):
        reason = structural_refusal(index)
        if reason is not None:
            return _unresolved(reason)

        row = series.rows[index]
        bar_open, high, low = row.get("open"), row.get("high"), row.get("low")
        if bar_open is None or high is None or low is None:
            return masked(index)

        if bar_open <= stop:
            return _booked(
                "sl_hit",
                series=series,
                fill_index=fill_index,
                exit_index=index,
                exit_price=bar_open,
                entry_price=entry_price,
            )
        if target is not None and bar_open >= target:
            return _booked(
                "tp_hit",
                series=series,
                fill_index=fill_index,
                exit_index=index,
                exit_price=bar_open,
                entry_price=entry_price,
            )
        touched_stop = low <= stop
        touched_target = target is not None and high >= target
        if touched_stop and touched_target:
            return Outcome(
                outcome="ambiguous",
                resolution_method="daily_bar",
                rule_set_version=RULE_SET_VERSION,
                exit_index=index,
                exit_bar_date=series.dates[index],
                bars_held=index - fill_index,
            )
        if touched_stop:
            return _booked(
                "sl_hit",
                series=series,
                fill_index=fill_index,
                exit_index=index,
                exit_price=stop,
                entry_price=entry_price,
            )
        if touched_target:
            assert target is not None  # touched_target requires it
            return _booked(
                "tp_hit",
                series=series,
                fill_index=fill_index,
                exit_index=index,
                exit_price=target,
                entry_price=entry_price,
            )

    # ⚠ THE EXPIRY EXIT FILLS AT THE OPEN OF THE BAR AFTER THE WINDOW, never at
    # the window's last close. §3.5.1 applies the fill rule to "entries and
    # exits alike"; a max-hold exit is a DECISION taken on a close, so it fills
    # at the next open exactly like an entry. TP and SL differ legitimately —
    # they are resting orders placed in advance, not decisions taken on a close.
    # Booking the last close would be a same-bar fill.
    #
    # ⚠ This is a CONSTRUCTION from §3.5.1, not a citation of it: §3.5.1 speaks
    # of signal-driven exits and a max-hold liquidation is generated here. It is
    # flagged as constructed in the spec's §4 and covered by RULE_SET_VERSION.
    #
    # ⚠ This exit reads the OPEN alone, so a range-masked bar — high and low
    # already None, open carried through — serves it perfectly well. That is the
    # per-field rule paying for itself; a whole-bar refusal would have thrown
    # the exit away and reported unresolved.
    exit_index = fill_index + levels.max_hold_bars
    reason = structural_refusal(exit_index)
    if reason is not None:
        return _unresolved(reason)
    exit_open = series.rows[exit_index].get("open")
    if exit_open is None:
        return masked(exit_index)
    return _booked(
        "expired",
        series=series,
        fill_index=fill_index,
        exit_index=exit_index,
        exit_price=exit_open,
        entry_price=entry_price,
    )
