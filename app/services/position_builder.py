"""Phase 5a — position construction.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §3 (the three exit
regimes, the pyramiding rule, the four close sources) and §8 (stages). Parent:
``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`` §3.5, §7,
criteria 8/9/11. Inputs: ``app/services/signal_ledger.py`` (3c) and
``app/services/outcome_ledger.py`` (4b). Refs #2240, #2288.

⚠⚠ THIS IS THE FIRST STATEFUL LAYER IN THE EPIC, AND IT IS STATEFUL BECAUSE THE
LEDGER IS DELIBERATELY NOT.

Parent §7: *"Every fired signal is recorded whether or not it was acted on …
Only recording taken trades biases the record toward periods of spare
capacity."* So S-1's exit leg fires on every bar whose close is below
``sma_50``, open position or not, and its entry leg fires on every bar of a
sustained uptrend. Pairing an entry with the exit that closes it — and
collapsing a run of entries into ONE hold — is this module's job and nothing
else's.

⚠⚠ ENTRIES ARE STATES, NOT CROSSOVERS (spec §3.1). S-1's entry is
``close > sma_200 AND sma_50 > sma_200``; S-3's is ``rsi_14 < 30 AND
close > sma_200``. Neither is edge-triggered. A naive "one position per fired
entry" therefore opens a position on EVERY day of a trend and multiplies every
downstream statistic by the length of the run. ``superseded_open_position``
below is the collapse, and it is COUNTED rather than asserted harmless
(criterion 9: *"measure what you reject"*).

⚠ NO PRICE SERIES IS EVER USED AS A FILL-PRICE SOURCE. Every close this module
emits is either a stored ``(date, price)`` pair from a ledger row, or the OPEN
of a named bar — which is §3.5 rule 1's own fill price. Spec §2.1: the
simulator is handed events indexed on the FILL bar, and a library's default
fill semantics *"are not a detail discovered during implementation; they are
the defect"*.

⚠ COSTS ARE NOT APPLIED HERE. Stage 5b owns the cost model, and every price
below is the raw stored one. A net return computed from these prices without
5b's half-spread adjustment is a gross return wearing a net return's name.
"""

from __future__ import annotations

import hashlib
from bisect import bisect_right
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Literal, get_args

from app.services.indicator_series import BarSeries
from app.services.outcome_resolver import OUTCOME_CLASSES, UNRESOLVED_REASONS, OutcomeClass, UnresolvedReason

# Same construction as `indicator_series` / `outcome_resolver`: a stable id plus
# a hash of THIS MODULE'S SOURCE. Parent criterion 11 makes the execution
# assumption part of strategy identity, and the constructions below — the
# pyramiding collapse, same-bar exit-before-entry, `unresolved` suppressing the
# max-hold close — live nowhere else. Stage 5c hashes this into the result
# identity for exactly that reason.
RULE_SET_ID = "position-builder-v1"


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


RULE_SET_VERSION = f"{RULE_SET_ID}+{_code_hash()}"


#: Spec §3.2's four sources, plus ``ambiguous`` — which is listed apart from
#: ``level`` because it breaks that source's ``(date, price)`` shape: ``sql/256``
#: gives an ambiguous outcome a date and withholds the price BY CONSTRAINT, so
#: the position closes on a known bar with an unknown return (§3.4).
#: ⚠ ``series_termination`` is EMITTED BY ``backtest_run``, never by this
#: builder: a terminating series' still-open ``window_end`` positions are
#: converted to realised closes at the series' last admissible close ×
#: ``series_termination.terminal_value_fraction`` (#2721 step 3). It lives in
#: this vocabulary because ``Position.__post_init__`` validates against it.
CloseSource = Literal["signal_pair", "level", "max_hold", "calendar", "ambiguous", "series_termination"]

#: Why a position is still open at the end of the window. ⚠ These are counted
#: SEPARATELY (§3.2 rule 5) because they say different things: one is a window
#: that ended, one is a resolver that refused to judge.
#:
#: ⚠ ``close_bar_unfillable`` and ``series_break`` are OURS — spec §3.2 rule
#: 5 names only the other two — and they are flagged as additions rather than
#: smuggled in, the same way ``strategy_registry`` flags ``no_fill_bar``. The
#: former exists because a forced close bar can be present and UNPRICED. The
#: latter refuses to compare prices on opposite sides of a known scale break.
#: ⚠ ``termination_price_unlocatable`` is SET BY ``backtest_run``, never by
#: this builder: a ``window_end`` open on a terminating series whose span from
#: fill to terminal bar carries no admissible close cannot be realised by the
#: termination rule and is re-labelled so the census can count it apart from an
#: ordinary window-end open (#2721 step 3).
OpenReason = Literal[
    "unresolved_outcome", "series_break", "window_end", "close_bar_unfillable", "termination_price_unlocatable"
]

#: Kept as an explicit subtraction so adopting a spec reason later cannot
#: silently land on our side of the line — ``strategy_registry``'s construction.
OUR_ADDITIONAL_OPEN_REASONS: frozenset[str] = frozenset(
    {"close_bar_unfillable", "series_break", "termination_price_unlocatable"}
)

# ⚠ DERIVED from the Literals, never restated — the closed-vocabulary-in-N-places
# defect the prevention log carries from #2218.
CLOSE_SOURCES: frozenset[str] = frozenset(get_args(CloseSource))
OPEN_REASONS: frozenset[str] = frozenset(get_args(OpenReason))
SPEC_OPEN_REASONS: frozenset[str] = OPEN_REASONS - OUR_ADDITIONAL_OPEN_REASONS

#: ``series_termination`` is OUR addition to the spec's close vocabulary
#: (#2721 step 3), declared apart exactly as the open reasons are — adopting a
#: spec source later cannot silently land on our side of the line.
OUR_ADDITIONAL_CLOSE_SOURCES: frozenset[str] = frozenset({"series_termination"})
SPEC_CLOSE_SOURCES: frozenset[str] = CLOSE_SOURCES - OUR_ADDITIONAL_CLOSE_SOURCES

#: Which candidate a tie resolves to. ``level`` wins over ``max_hold`` because
#: spec §3.2 names ``outcome_resolver.py:470`` as the reference for the expiry
#: bar — the two are redundant for S-4 by design and must agree.
#:
#: ⚠⚠ THIS NOW DECIDES THE PRICE, NOT ONLY THE LABEL (#2779), and the change is
#: deliberate. It previously ran only after every tied candidate had been checked
#: to agree on the price, so it chose a label over identical numbers. That check
#: is now scoped to the genuinely redundant pair, so a tie between sources the
#: spec never expected to coincide resolves HERE.
#:
#: ⚠ Spec §3.2 does NOT settle that case, and this comment says so rather than
#: implying a citation. Its table assigns C1 to S-1/S-3 and C2 to S-4, so a
#: signal-pair/level tie could not arise when it was written; the hybrid regime
#: (#2723) created it.
#:
#: ⚠⚠ THIS ORDER RUNS *WITHIN* AN EXECUTION MOMENT, NOT ACROSS ONE — see
#: ``_executes_at_open``. An earlier construction had ``level`` precede
#: ``signal_pair`` on the reasoning that "an intraday level touch closed the
#: position before any close-based exit fill could". That premise is FALSE:
#: ``signal_ledger.resolve_fills`` states its rule outright — *"the fill price is
#: that bar's OPEN. There is no other path"* — and it does not distinguish entry
#: fills from exit fills. A signal-pair exit therefore executes at the FIRST tick
#: of its bar, before any intraday path exists to touch a level. The real case
#: from #2779 is unambiguous: signal 7694 on 2011-06-16 had ``signal_pair=326.9``
#: (that bar's open) against ``level=318.874053244667719`` (a stop), and booking
#: the stop records a ~2.5% loss on a position that had already closed at the
#: open.
#:
#: ``RULE_SET_VERSION`` hashes this module's source, so this ordering is frozen
#: into strategy identity by construction — changing it moves every strategy
#: version, which is the intended cost of changing it.
_SOURCE_PRECEDENCE: tuple[CloseSource, ...] = ("level", "ambiguous", "max_hold", "calendar", "signal_pair")

#: Sources whose close executes at a bar's OPEN, so they precede anything that
#: needs an intraday path. ``signal_pair`` is here because
#: ``signal_ledger.resolve_fills`` prices EVERY fill at ``open(signal_index + 1)``;
#: ``max_hold`` and ``calendar`` because spec §3.2's table says "its **open**" for
#: both.
_OPEN_PRICED_SOURCES: frozenset[CloseSource] = frozenset({"signal_pair", "max_hold", "calendar"})


def _executes_at_open(candidate: _Candidate) -> bool:
    """Whether this close happens at its bar's open rather than somewhere inside it.

    ⚠⚠ THE ORDER OF EVENTS INSIDE ONE BAR IS open → intraday path → close, and
    that is the whole basis for resolving a tie the spec does not settle. A
    source that executes at the open cannot be preempted by one that needs the
    price to travel somewhere first.

    ``level`` is on BOTH sides of this line, which is why the test is not a bare
    source lookup. An ``expired`` outcome exits at the next bar's OPEN
    (``outcome_resolver``: *"a max-hold exit fills at the NEXT open"*), and it is
    exactly the ``expired`` case that carries ``redundant_with_max_hold``. A
    ``tp``/``sl`` touch is intraday.

    ⚠ A gap-through ``tp``/``sl`` also resolves at the open and is NOT detectable
    here — but it prices AT that open, so it ties with the open-priced sources on
    the number and can only differ in the label. Sorting it late is therefore
    price-neutral, which is the property that matters.
    """
    return candidate.source in _OPEN_PRICED_SOURCES or candidate.redundant_with_max_hold


@dataclass(frozen=True)
class EntryFill:
    """One fired ENTRY row from ``strategy_signals``, already filled.

    ⚠ ``signal_bar_date`` is carried even though the fill is what positions are
    keyed on. Two things need it: spec §2.1's assertion 3
    (``fill_bar_date > signal_bar_date`` over every row consumed, asserted here
    at construction rather than left to a later sweep), and S-2's calendar close,
    which asks whether the name was RESELECTED at a rebalance — a property of
    the decision bar, not of the fill.
    """

    signal_id: int
    instrument_id: int
    signal_bar_date: date
    fill_bar_date: date
    fill_price: Decimal

    def __post_init__(self) -> None:
        if self.fill_bar_date <= self.signal_bar_date:
            raise ValueError(
                f"signal {self.signal_id}: fill_bar_date {self.fill_bar_date} is not after signal_bar_date "
                f"{self.signal_bar_date} — the look-ahead sql/255's CHECK exists to prevent"
            )
        if self.fill_price <= 0:
            raise ValueError(f"signal {self.signal_id}: fill_price must be > 0, got {self.fill_price}")


@dataclass(frozen=True)
class ExitFill:
    """One fired EXIT row from ``strategy_signals``, already filled.

    ⚠ Stateless by construction (S-1's and S-3's exit legs fire whether or not a
    position is open), so an exit row is a candidate close for AT MOST one
    position and may match none at all.
    """

    instrument_id: int
    signal_bar_date: date
    fill_bar_date: date
    fill_price: Decimal

    def __post_init__(self) -> None:
        if self.fill_bar_date <= self.signal_bar_date:
            raise ValueError(
                f"exit on {self.signal_bar_date}: fill_bar_date {self.fill_bar_date} is not after it "
                "— the look-ahead sql/255's CHECK exists to prevent"
            )
        if self.fill_price <= 0:
            raise ValueError(f"exit on {self.signal_bar_date}: fill_price must be > 0, got {self.fill_price}")


@dataclass(frozen=True)
class ResolvedOutcome:
    """One ``strategy_outcomes`` row, at ONE pinned version pair.

    The validation mirrors ``sql/256``'s CHECKs, as ``sql/256``'s own comment
    frames them: a pair of nullity EQUALITIES, so a bad row fails at
    construction with a message naming the field. ⚠ The projection carries ONE
    location field and one price field rather than 4b's three, so there is no
    half-location to count here — the counted-not-ANDed guard that matters at
    this layer is ``Position``'s, where a close date and a bar count do move
    together.
    """

    signal_id: int
    rule_set_version: str
    input_rule_set_version: str
    outcome: OutcomeClass
    exit_bar_date: date | None
    exit_price: Decimal | None
    reason: UnresolvedReason | None = None
    #: First stored bar on the new scale. This is runner-only structural
    #: metadata rather than a booked exit and therefore is not projected into
    #: ``exit_bar_date`` (an unresolved ledger outcome has no exit location).
    unresolved_until_bar_date: date | None = None

    def __post_init__(self) -> None:
        if self.outcome not in OUTCOME_CLASSES:
            raise ValueError(f"unknown outcome {self.outcome!r}; must be one of {sorted(OUTCOME_CLASSES)}")
        if not self.rule_set_version or not self.input_rule_set_version:
            raise ValueError(
                f"blank version on signal {self.signal_id}: "
                f"{(self.rule_set_version, self.input_rule_set_version)!r} — both key members must be stated"
            )
        located = self.exit_bar_date is not None
        if located == (self.outcome == "unresolved"):
            raise ValueError(
                f"outcome {self.outcome!r} on signal {self.signal_id} carries exit_bar_date "
                f"{self.exit_bar_date!r}: an unresolved outcome has none and every other outcome — "
                "including ambiguous, whose bar is known — has one"
            )
        booked = self.exit_price is not None
        if booked != (self.outcome in {"tp_hit", "sl_hit", "expired"}):
            raise ValueError(
                f"outcome {self.outcome!r} on signal {self.signal_id} carries exit_price "
                f"{self.exit_price!r}: a price exists exactly for tp_hit / sl_hit / expired"
            )
        if self.reason is not None and self.reason not in UNRESOLVED_REASONS:
            raise ValueError(f"unknown unresolved reason {self.reason!r}; must be one of {sorted(UNRESOLVED_REASONS)}")
        if (self.outcome == "unresolved") != (self.reason is not None):
            raise ValueError(
                f"outcome {self.outcome!r} on signal {self.signal_id} and reason {self.reason!r} disagree: "
                "a reason is required exactly for an unresolved outcome"
            )
        if (self.reason == "series_break") != (self.unresolved_until_bar_date is not None):
            raise ValueError(
                f"reason {self.reason!r} on signal {self.signal_id} and unresolved boundary "
                f"{self.unresolved_until_bar_date!r} disagree: a boundary is required exactly for series_break"
            )


@dataclass(frozen=True)
class OutcomePin:
    """The ``(rule_set_version, input_rule_set_version)`` pair the outcomes were read at.

    ⚠⚠ SPEC §3.2 RULE 1 — *"Version pinning is mandatory … an unpinned join
    double-counts every signal once per resolver version present."* ``sql/256``
    makes the pair a key member precisely so two resolver versions COEXIST, so
    a mixed outcome set is not a hypothetical: it is the default result of a
    query that forgets one predicate.

    A pure function cannot see the query, so the pin is declared and every
    supplied outcome is checked against it. That turns the silent double-count
    into a raise naming the offending signal.
    """

    rule_set_version: str
    input_rule_set_version: str

    def __post_init__(self) -> None:
        if not self.rule_set_version or not self.input_rule_set_version:
            raise ValueError(
                f"both pin members must be stated, got {(self.rule_set_version, self.input_rule_set_version)!r}"
            )


@dataclass(frozen=True)
class ExitRegime:
    """Which of the four close sources this strategy declares.

    ⚠ EVERY FIELD IS REQUIRED AND NONE HAS A DEFAULT (#2288's lesson: a field
    with a default is a field a writer can forget). Spec §3's table, verbatim:

    ==========  ===========  ============  ==============  ================
    strategy    signal_pair  level_based   max_hold_bars   rebalance_dates
    ==========  ===========  ============  ==============  ================
    S-1         True         False         None            None
    S-2         False        False         None            the panel's
    S-3         True         False         10              None
    S-4         False        True          40              None
    ==========  ===========  ============  ==============  ================

    ⚠⚠ S-3 carries ``max_hold_bars`` here and NOT ``level_based``, which is the
    correction spec §3 makes to the catalogue's own prose: S-3 has no stop and
    no target — its exit is ``rsi_14 > 50`` — so ``ExitLevels`` cannot be
    constructed for it and its ``MAX_HOLD_BARS = 10`` is today enforceable by
    nothing (#2348). Max-hold expiry is therefore a close source owned HERE, not
    exclusively by the resolver.

    ⚠ S-1 declares no close source but ``signal_pair``, and that is not an
    omission: §4 gives it no holding bound. Spec §5.3 flags the consequence for
    the walk-forward embargo as a genuine open problem and recommends giving S-1
    a declared bound — a new strategy version, and stage 5e's to make.
    """

    signal_pair: bool
    level_based: bool
    max_hold_bars: int | None
    rebalance_dates: frozenset[date] | None

    def __post_init__(self) -> None:
        if self.max_hold_bars is not None and self.max_hold_bars < 1:
            raise ValueError(f"max_hold_bars must be >= 1, got {self.max_hold_bars} — 0 would exit at the fill bar")
        if self.rebalance_dates is not None and not self.rebalance_dates:
            raise ValueError(
                "rebalance_dates is empty — pass None for a strategy with no calendar close, so that "
                "'no calendar' and 'a calendar with no dates' cannot be confused"
            )
        if not (self.signal_pair or self.level_based or self.max_hold_bars or self.rebalance_dates):
            raise ValueError(
                "no close source declared: every position would stay open forever. Declare at least one of "
                "signal_pair / level_based / max_hold_bars / rebalance_dates"
            )


@dataclass(frozen=True)
class Window:
    """The evaluation window, inclusive at both ends.

    ⚠ THE WINDOW IS A NAMESPACE, NOT A FILTER, AND IT IS APPLIED TO THE **FILL**
    DATE. Spec §5.2: *"A signal whose signal_bar_date is in-sample but whose
    fill_bar_date is on or after the boundary is purged — it is neither, because
    acting on it needs a price from the withheld side."* Selecting entries on
    the fill date is exactly that purge, so it is done here rather than left to
    each caller's WHERE clause.

    ⚠ Consequence, stated because it is a real boundary artefact: a position
    opened BEFORE ``start`` belongs to the previous namespace and is not
    supplied here, so it cannot suppress an entry just inside the window. The
    first hold of a window can therefore be one the previous namespace was
    already in. Splitting the position instead would put one namespace's prices
    into the other's statistics, which is the error §5.2 rejects outright.
    """

    start: date
    end: date

    def __post_init__(self) -> None:
        if self.end < self.start:
            raise ValueError(f"window end {self.end} precedes start {self.start}")

    def contains(self, when: date) -> bool:
        return self.start <= when <= self.end


@dataclass(frozen=True)
class Position:
    """One hold: an entry fill, and the close that ended it — or the mark that did not.

    ⚠ ``close_price is None`` while ``close_source == "ambiguous"`` is the one
    priced-close exception, and it is a state ``sql/256`` produces on purpose:
    the bar is known, the touch order is not. §3.4 gives it a two-armed
    sensitivity treatment at stage 5d; it is NOT resolved favourably and NOT
    assumed to be a stop, which parent §3.5 rule 4 and spike S5 both reject —
    *"it is not conservative, it is a different bias"*.
    """

    strategy_id: str
    strategy_version: str
    instrument_id: int
    entry_signal_id: int
    entry_signal_bar_date: date
    entry_fill_bar_date: date
    entry_fill_price: Decimal
    close_source: CloseSource | None
    close_bar_date: date | None
    close_price: Decimal | None
    bars_held: int | None
    open_reason: OpenReason | None
    #: Unrealised mark for an open position: the last usable close of the window
    #: (§3.2 rule 5). ⚠ Stage 5b still owes it ONE side of the cost model — the
    #: exit that has not happened.
    mark_price: Decimal | None

    def __post_init__(self) -> None:
        if self.close_source is not None and self.close_source not in CLOSE_SOURCES:
            raise ValueError(f"unknown close source {self.close_source!r}; must be one of {sorted(CLOSE_SOURCES)}")
        if self.open_reason is not None and self.open_reason not in OPEN_REASONS:
            raise ValueError(f"unknown open reason {self.open_reason!r}; must be one of {sorted(OPEN_REASONS)}")
        if (self.close_source is None) == (self.open_reason is None):
            raise ValueError(
                f"position on signal {self.entry_signal_id} is {'both' if self.close_source else 'neither'} "
                "closed and open: exactly one of close_source / open_reason is set"
            )
        # ⚠ COUNTED, not ANDed — the mirror defect from #2240 3c. A closed
        # position with a date and no bars_held is half a close, and reads as
        # "no location" under `a is not None and b is not None`.
        located = (self.close_bar_date is not None) + (self.bars_held is not None)
        if located != (2 if self.close_source is not None else 0):
            raise ValueError(
                f"position on signal {self.entry_signal_id} carries a partial close location "
                f"{(self.close_bar_date, self.bars_held)!r}: a closed position has both, an open one neither"
            )
        if self.close_source is not None:
            assert self.close_bar_date is not None and self.bars_held is not None  # narrowed by the count above
            # ⚠ `>=`, NOT `>`. A tp/sl can be touched on the FILL BAR itself —
            # `outcome_resolver` scans from `fill_index` inclusive and
            # `sql/256` says bars_held = 0 is legal. Requiring a strict
            # inequality here would reject the resolver's own output.
            if self.close_bar_date < self.entry_fill_bar_date:
                raise ValueError(
                    f"position on signal {self.entry_signal_id} closes {self.close_bar_date} before its fill "
                    f"{self.entry_fill_bar_date}"
                )
            if self.bars_held < 0:
                raise ValueError(f"bars_held must be non-negative, got {self.bars_held}")
            if (self.close_price is None) != (self.close_source == "ambiguous"):
                raise ValueError(
                    f"close source {self.close_source!r} carries close_price {self.close_price!r}: a price is "
                    "absent exactly for ambiguous, whose bar is known and whose touch order is not"
                )
            if self.mark_price is not None:
                raise ValueError("a closed position has a realised close, not a mark")
        elif self.close_price is not None:
            raise ValueError("an open position has no close price")
        if self.open_reason == "series_break" and self.mark_price is not None:
            raise ValueError("a position crossing a series break cannot be marked on the new price scale")


@dataclass(frozen=True)
class PositionSet:
    """The positions, and every narrowing that produced them.

    ⚠ THE COUNTERS ARE NOT DIAGNOSTICS — they are parent criterion 8's report.
    Spec §9's C8 requires the ``superseded_open_position`` count and §3.3's
    panel-vs-instrument calendar divergence *"reported alongside, since both are
    narrowings phase 5 introduces"*.

    ⚠ Counts, not id lists, and deliberately: entries are states (§3.1), so on
    the full corpus the suppressed set is larger than the position set by orders
    of magnitude and holding its ids would dominate the run's memory.
    """

    positions: tuple[Position, ...]
    #: §3.1 — an entry arriving while this strategy version already holds the name.
    superseded_open_position: int
    #: Entries whose FILL fell outside the window (§5.2's purge).
    entries_outside_window: int
    #: §3.3 — a rebalance date on which the instrument's own series has no bar,
    #: so the calendar close lands later than the panel's. The divergence
    #: between the panel calendar and the instrument calendar, counted.
    halted_at_rebalance: int
    #: A max-hold or calendar close whose bar exists but whose OPEN does not, so
    #: the close could not be priced and the position stayed open.
    close_bar_unfillable: int
    #: An open position with no usable close at or after its fill, so no mark
    #: could be taken. Never silently zero — §3.2 rule 5 forbids dropping it.
    marks_unavailable: int


def _bar_index(series: BarSeries, when: date, index: Mapping[date, int], *, what: str) -> int:
    """Locate a STORED date in ``series``, or say the corpus moved.

    Same guard as ``outcome_ledger.locate_fill_index`` and for the same reason —
    a stored date absent from the series means the corpus was rebuilt or
    re-adjusted, and silently re-reading "whatever bar sits there now" is how a
    ledger stops being a record of what was decided. It takes a prebuilt index
    rather than calling ``.index()`` because this runs once per position and
    ``list.index`` is linear: on a 16,236-bar series with thousands of entries
    that is quadratic.
    """
    position = index.get(when)
    if position is None:
        raise ValueError(
            f"{what} {when} is not in the {len(series)}-bar series "
            f"({series.dates[0] if series.dates else 'empty'} … "
            f"{series.dates[-1] if series.dates else 'empty'}) — the stored row and the series must come "
            "from the same corpus"
        )
    return position


def _next_bar_after(series: BarSeries, when: date) -> int | None:
    """Index of the first bar strictly after ``when``, or None past the series end.

    ⚠ The instrument's OWN series, never ``when + 1 day``. Calendar gaps are
    normal and date arithmetic would invent a fill on a day it did not trade —
    ``sql/255``'s ``fill_bar_date`` comment, applied to a calendar close.
    """
    index = bisect_right(series.dates, when)
    return index if index < len(series) else None


def _mark_price(series: BarSeries, *, window: Window, not_before: date) -> Decimal | None:
    """The last usable close at or before the window end, for an open position.

    §3.2 rule 5. ⚠ Bounded BELOW by the fill date as well: a series whose every
    close after the fill is masked would otherwise mark the position at a price
    from before it was opened, which is not an unrealised return — it is a
    fabricated one.
    """
    for index in range(len(series) - 1, -1, -1):
        when = series.dates[index]
        if when > window.end:
            continue
        if when < not_before:
            return None
        close = series.rows[index].get("close")
        if close is not None:
            return close
    return None


@dataclass(frozen=True)
class _Candidate:
    when: date
    price: Decimal | None
    source: CloseSource
    #: ⚠ True ONLY for a C2 outcome of ``expired``, which is the one candidate
    #: the spec calls redundant with C3's max-hold expiry. ``tp_hit`` / ``sl_hit``
    #: are NOT redundant with anything — they are an intraday touch, not a
    #: recomputation of the same window — so they must not carry it.
    redundant_with_max_hold: bool = False


def _pick_close(candidates: Sequence[_Candidate], *, signal_id: int) -> _Candidate:
    """The earliest close, with a disagreement on the same bar treated as a failure.

    Spec §3.2: the four sources *"are evaluated together and the earliest date
    wins; they are not alternatives selected by strategy"*. And on C2-vs-C3 for
    S-4, where the two are redundant by design: *"a disagreement is a failure,
    not a tie-break — it means the resolver and position construction disagree
    about the window"*.

    ⚠⚠ THE EQUALITY CHECK IS SCOPED TO THE REDUNDANT PAIR, NOT GENERALISED
    (#2779). It used to fire on ANY two tied sources carrying different prices,
    which contradicts the general rule quoted above — *the earliest date wins* —
    and says nothing about prices agreeing. Only C2's ``expired`` and C3 are
    redundant: both compute the same max-hold window, so disagreeing means the
    resolver and position construction disagree, which is a real defect.

    C1-vs-C2 is NOT that. A level exit is an intraday touch priced AT the level;
    a signal-pair exit is a fill priced at the bar. Both can be true on one bar
    and there is no reason they should be equal. The generalisation made that
    ordinary case fatal, and it killed a 13.6-hour full-set run at
    ``s7-trend-pullback`` — the first hybrid ``signal_pair`` + ``level_based``
    regime (#2723), a combination §3.2's table predates.

    ⚠ The generalisation also made ``_SOURCE_PRECEDENCE`` unreachable for every
    case it was written for: an ordering over tied sources only means anything
    if tied sources may disagree.

    ⚠⚠ THE TIE IS BROKEN BY WHEN THE CLOSE EXECUTED, NOT BY SOURCE ALONE — see
    ``_executes_at_open``. A signal-pair exit is priced at its bar's OPEN
    (``signal_ledger.resolve_fills``: *"the fill price is that bar's OPEN. There
    is no other path"*), so it closes the position at the first tick and no
    intraday level touch on the same bar can reach it. #2779's own reported case
    is the proof: ``signal_pair=326.9`` (the open) against ``level=318.87`` (a
    stop) on 2011-06-16 — preferring the stop books a loss on a position that no
    longer existed.
    """
    earliest = min(candidate.when for candidate in candidates)
    tied = [candidate for candidate in candidates if candidate.when == earliest]
    redundant = [candidate for candidate in tied if candidate.redundant_with_max_hold or candidate.source == "max_hold"]
    if len({candidate.price for candidate in redundant}) > 1:
        detail = ", ".join(
            f"{candidate.source}={candidate.price}" for candidate in sorted(redundant, key=lambda c: c.source)
        )
        raise ValueError(
            f"signal {signal_id}: the max-hold expiry disagrees with the resolver on {earliest} ({detail}) — "
            "C2's `expired` and C3 compute the same window, so a disagreement is a failure, not a tie-break"
        )
    # ⚠ TWO-TIER, and the tiers are not interchangeable. The first asks WHEN in
    # the bar the close executed, which is a fact about the market; the second is
    # the declared label preference among sources that executed at the same
    # moment, which is a convention. Collapsing them into one tuple is what
    # produced the cycle: `level` must precede `max_hold` (spec §3.2 names
    # `outcome_resolver.py:470` as the expiry reference, and the two are
    # price-equal by the check above), `signal_pair` must precede an intraday
    # `level`, and a single total order cannot hold both while keeping
    # `max_hold` ahead of `signal_pair`.
    return min(
        tied,
        key=lambda candidate: (
            0 if _executes_at_open(candidate) else 1,
            _SOURCE_PRECEDENCE.index(candidate.source),
        ),
    )


def build_positions(
    *,
    strategy_id: str,
    strategy_version: str,
    entries: Sequence[EntryFill],
    exits: Sequence[ExitFill],
    outcomes: Sequence[ResolvedOutcome],
    outcome_pin: OutcomePin | None,
    series: Mapping[int, BarSeries],
    regime: ExitRegime,
    window: Window,
) -> PositionSet:
    """Collapse one strategy version's fills into positions. Pure; reads no database.

    ⚠ ONE STRATEGY VERSION PER CALL, and required to be — the same rule
    ``outcome_ledger.select_pending_fills`` states. §3.1's suppression is scoped
    to *"the same strategy version"*, so a mixed batch would let one version's
    open hold suppress another's entry, and criterion 11 says those are
    different strategies.

    THE FOUR CLOSE SOURCES (§3.2), plus the fifth that breaks their shape
    ------------------------------------------------------------------------
    ==  ============  =====================================================
    C1  signal_pair   the next fired EXIT fill STRICTLY AFTER the entry fill
    C2  level         the pinned ``strategy_outcomes`` row's exit
    C3  max_hold      ``entry fill index + max_hold_bars``, at that bar's OPEN
    C4  calendar      the next rebalance at which the name is NOT reselected
    --  ambiguous     terminal, with a date and NO price (``sql/256``)
    ==  ============  =====================================================

    ⚠ C1 IS STRICTLY AFTER, AND THAT IS RULE 4 — same-bar ordering is exit
    before entry. An exit fill sharing a bar with a new entry closes the OLDER
    position and never the one opened that bar. §3.5 rule 1's justification for
    keying the ledger on ``signal_kind`` is that *"a strategy exiting one
    position and entering another on the same bar for the same instrument is
    legitimate"*, so the order has to be stated rather than left to sort
    stability.

    ⚠⚠ AN ``unresolved`` OUTCOME SUPPRESSES C3 AND LEAVES THE POSITION OPEN.
    This is forced by §3.2 rule 5 — *"positions left open by an ``unresolved``
    outcome are counted separately from those open because the window ended"* —
    which is only satisfiable if ``unresolved`` beats the max-hold close.
    Substantively: the resolver refuses when it cannot tell whether a level was
    touched, and booking that window's expiry return anyway would report a
    result over bars it just said it could not judge. It does NOT suppress C1 or
    C4: those are decisions the strategy actually made, not inferences about
    what the price did.

    ⚠ A level-based strategy with a MISSING outcome raises rather than falling
    through to C3. "Not resolved yet" and "resolved as nothing" are different
    states, and the second is the one that quietly books returns.

    S-2's CALENDAR CLOSE (§3.3)
    ---------------------------
    Reselection is read from the ENTRY FILLS, not from the positions: the entry
    at the next rebalance is suppressed by §3.1 (the name is already held), so
    consulting the positions would report every consecutive hold as a drop-out
    and charge two sides of the cost model for a hold that never ended. A name
    dropped at a rebalance closes at that rebalance's fill even though NO exit
    signal exists for it, which is why the close clause cannot be "the entry
    that supersedes it".

    ⚠ Everything that is not a fired entry at that rebalance counts as NOT
    reselected — ``not_fired``, ``not_evaluable`` and a missing row alike. A
    name the panel could not evaluate is a name the strategy did not choose to
    hold, and holding it would be a decision nobody made.
    """
    if regime.level_based and outcome_pin is None:
        raise ValueError("a level-based regime must declare the outcome version pin it read (spec §3.2 rule 1)")
    if outcomes and not regime.level_based:
        raise ValueError(
            f"{len(outcomes)} outcomes supplied for a regime that is not level-based — they would be read by "
            "nothing, and a close source nobody applies is a close source nobody notices is missing"
        )
    # ⚠ NO third check for "outcomes without a pin". It would be unreachable:
    # a non-empty `outcomes` forces `level_based` through the check above, and
    # `level_based` forces a non-null pin through the one above that. Written
    # out because the absence looks like an omission — the pin IS required
    # whenever outcomes are supplied, by those two checks composed.
    if exits and not regime.signal_pair:
        raise ValueError(
            f"{len(exits)} exit fills supplied for a regime that declares no exit leg — the ledger and the "
            "declared regime disagree about what this strategy is"
        )

    by_signal: dict[int, ResolvedOutcome] = {}
    for outcome in outcomes:
        if outcome_pin is not None and (
            outcome.rule_set_version != outcome_pin.rule_set_version
            or outcome.input_rule_set_version != outcome_pin.input_rule_set_version
        ):
            raise ValueError(
                f"signal {outcome.signal_id}: outcome at "
                f"{(outcome.rule_set_version, outcome.input_rule_set_version)!r} does not match the pin "
                f"{(outcome_pin.rule_set_version, outcome_pin.input_rule_set_version)!r} — an unpinned join "
                "double-counts every signal once per resolver version present"
            )
        if outcome.signal_id in by_signal:
            raise ValueError(
                f"two outcomes for signal {outcome.signal_id} at one version pin — sql/256's uniqueness key "
                "makes this unreachable from the table, so the batch was assembled wrongly"
            )
        by_signal[outcome.signal_id] = outcome

    exits_by_instrument: dict[int, list[ExitFill]] = {}
    for exit_fill in exits:
        exits_by_instrument.setdefault(exit_fill.instrument_id, []).append(exit_fill)
    for instrument_exits in exits_by_instrument.values():
        instrument_exits.sort(key=lambda fill: fill.fill_bar_date)

    #: Every rebalance at which each instrument WAS selected, from the raw
    #: entries — including those the window excludes, so a hold that continues
    #: past the window edge is not reported as a drop-out.
    reselected: set[tuple[int, date]] = {(entry.instrument_id, entry.signal_bar_date) for entry in entries}
    rebalances = sorted(regime.rebalance_dates) if regime.rebalance_dates else []

    entries_by_instrument: dict[int, list[EntryFill]] = {}
    outside = 0
    for entry in entries:
        if not window.contains(entry.fill_bar_date):
            outside += 1
            continue
        entries_by_instrument.setdefault(entry.instrument_id, []).append(entry)
    for instrument_entries in entries_by_instrument.values():
        instrument_entries.sort(key=lambda fill: fill.fill_bar_date)

    positions: list[Position] = []
    superseded = 0
    halted = 0
    unfillable = 0
    marks_missing = 0

    for instrument_id, instrument_entries in sorted(entries_by_instrument.items()):
        bars = series.get(instrument_id)
        if bars is None:
            raise ValueError(
                f"no series supplied for instrument {instrument_id}, which has {len(instrument_entries)} "
                "entry fills in the window — the close bar and the window-end mark both need it"
            )
        bar_index = {when: index for index, when in enumerate(bars.dates)}
        instrument_exits = exits_by_instrument.get(instrument_id, [])
        exit_dates = [fill.fill_bar_date for fill in instrument_exits]

        # None means "no position open"; a date means the open one closes then.
        # ⚠ A closed position whose close date equals a later entry's fill bar
        # does NOT suppress it — rule 4, exit before entry.
        open_until: date | None = None
        holding = False

        for entry in instrument_entries:
            if holding and (open_until is None or entry.fill_bar_date < open_until):
                superseded += 1
                continue

            entry_index = _bar_index(bars, entry.fill_bar_date, bar_index, what="fill_bar_date")
            candidates: list[_Candidate] = []
            unresolved = False
            unresolved_reason: UnresolvedReason | None = None
            unresolved_until: date | None = None

            if regime.signal_pair:
                after = bisect_right(exit_dates, entry.fill_bar_date)
                if after < len(instrument_exits):
                    exit_fill = instrument_exits[after]
                    candidates.append(_Candidate(exit_fill.fill_bar_date, exit_fill.fill_price, "signal_pair"))

            if regime.level_based:
                outcome = by_signal.get(entry.signal_id)
                if outcome is None:
                    raise ValueError(
                        f"signal {entry.signal_id} is a level-based entry with no outcome at the pinned "
                        "version pair — resolve it before building positions rather than falling through to "
                        "the max-hold close"
                    )
                if outcome.outcome == "unresolved":
                    unresolved = True
                    unresolved_reason = outcome.reason
                    unresolved_until = outcome.unresolved_until_bar_date
                else:
                    assert outcome.exit_bar_date is not None  # narrowed by ResolvedOutcome's own check
                    source: CloseSource = "ambiguous" if outcome.outcome == "ambiguous" else "level"
                    candidates.append(
                        _Candidate(
                            outcome.exit_bar_date,
                            outcome.exit_price,
                            source,
                            # ⚠ §3.2: C3 "for S-4 is redundant with C2's `expired`
                            # and must agree with it". Only `expired` — a tp/sl
                            # touch is an intraday event, not a second reading of
                            # the max-hold window, so it is redundant with nothing.
                            redundant_with_max_hold=outcome.outcome == "expired",
                        )
                    )

            # ⚠⚠ `ceiling` IS THE BAR ON WHICH THE HOLD IS FORCED TO END, and it
            # binds whether or not that bar can be priced. C3 and C4 are not
            # opinions the way C1 and C2 are: a declared `max_hold_bars` and a
            # rebalance drop-out both END the position by construction, so no
            # later source may close it. Without this, a max-hold bar whose OPEN
            # is masked contributes no candidate and a LATER exit signal books
            # the trade — holding S-3 for 13 bars against its declared 10 and
            # pricing it off a bar the strategy could never have reached.
            # (Codex, checkpoint 2.) When the forced bar IS priced the ceiling
            # changes nothing, because `_pick_close` already takes the earliest.
            ceiling: date | None = None

            if regime.max_hold_bars is not None and not unresolved:
                expiry = entry_index + regime.max_hold_bars
                if expiry < len(bars):
                    ceiling = bars.dates[expiry]
                    expiry_open = bars.rows[expiry].get("open")
                    if expiry_open is None:
                        unfillable += 1
                    else:
                        candidates.append(_Candidate(bars.dates[expiry], expiry_open, "max_hold"))

            if rebalances:
                for when in rebalances:
                    if when <= entry.signal_bar_date:
                        continue
                    if (instrument_id, when) in reselected:
                        continue
                    if when not in bar_index:
                        halted += 1
                    close_index = _next_bar_after(bars, when)
                    if close_index is not None:
                        forced = bars.dates[close_index]
                        ceiling = forced if ceiling is None else min(ceiling, forced)
                        close_open = bars.rows[close_index].get("open")
                        if close_open is None:
                            unfillable += 1
                        else:
                            candidates.append(_Candidate(forced, close_open, "calendar"))
                    break

            # ⚠ A close after the window end is not this namespace's close: the
            # position is open AT the window end, and dropping it would bias
            # toward positions that closed — which happens faster in trending
            # regimes (§3.2 rule 5).
            #
            # ⚠ The window bound is applied FIRST when the two disagree: a
            # ceiling beyond the window end never happened inside this
            # namespace, so such a position is open because the WINDOW ended,
            # not because a close bar could not be priced.
            limit = window.end if ceiling is None else min(ceiling, window.end)
            candidates = [candidate for candidate in candidates if candidate.when <= limit]

            if candidates:
                chosen = _pick_close(candidates, signal_id=entry.signal_id)
                close_index = _bar_index(bars, chosen.when, bar_index, what="close_bar_date")
                positions.append(
                    Position(
                        strategy_id=strategy_id,
                        strategy_version=strategy_version,
                        instrument_id=instrument_id,
                        entry_signal_id=entry.signal_id,
                        entry_signal_bar_date=entry.signal_bar_date,
                        entry_fill_bar_date=entry.fill_bar_date,
                        entry_fill_price=entry.fill_price,
                        close_source=chosen.source,
                        close_bar_date=chosen.when,
                        close_price=chosen.price,
                        bars_held=close_index - entry_index,
                        open_reason=None,
                        mark_price=None,
                    )
                )
                open_until = chosen.when
            else:
                mark = (
                    None
                    if unresolved_reason == "series_break"
                    else _mark_price(bars, window=window, not_before=entry.fill_bar_date)
                )
                if mark is None:
                    marks_missing += 1
                if unresolved_reason == "series_break":
                    open_reason: OpenReason = "series_break"
                elif unresolved:
                    open_reason = "unresolved_outcome"
                elif ceiling is not None and ceiling <= window.end:
                    open_reason = "close_bar_unfillable"
                else:
                    open_reason = "window_end"
                positions.append(
                    Position(
                        strategy_id=strategy_id,
                        strategy_version=strategy_version,
                        instrument_id=instrument_id,
                        entry_signal_id=entry.signal_id,
                        entry_signal_bar_date=entry.signal_bar_date,
                        entry_fill_bar_date=entry.fill_bar_date,
                        entry_fill_price=entry.fill_price,
                        close_source=None,
                        close_bar_date=None,
                        close_price=None,
                        bars_held=None,
                        open_reason=open_reason,
                        mark_price=mark,
                    )
                )
                # ⚠ A position left open by an UNPRICEABLE forced close is
                # unbookable, not eternal: the hold ended at the ceiling by
                # construction. Suppressing every later entry in that instrument
                # would silently drop the rest of its history over one masked
                # bar, which is a narrowing far larger than the defect.
                open_until = (
                    ceiling
                    if open_reason == "close_bar_unfillable"
                    else unresolved_until
                    if open_reason == "series_break"
                    else None
                )
            holding = True

    return PositionSet(
        positions=tuple(positions),
        superseded_open_position=superseded,
        entries_outside_window=outside,
        halted_at_rebalance=halted,
        close_bar_unfillable=unfillable,
        marks_unavailable=marks_missing,
    )


__all__ = [
    "CLOSE_SOURCES",
    "OPEN_REASONS",
    "OUR_ADDITIONAL_CLOSE_SOURCES",
    "OUR_ADDITIONAL_OPEN_REASONS",
    "RULE_SET_ID",
    "RULE_SET_VERSION",
    "SPEC_CLOSE_SOURCES",
    "SPEC_OPEN_REASONS",
    "CloseSource",
    "EntryFill",
    "ExitFill",
    "ExitRegime",
    "OpenReason",
    "OutcomePin",
    "Position",
    "PositionSet",
    "ResolvedOutcome",
    "Window",
    "build_positions",
]
