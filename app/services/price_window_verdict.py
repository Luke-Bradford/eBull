"""Verdict-aware transport for consumers that evaluate a window over raw ``price_daily``.

Spec: ``docs/proposals/ta/2026-09-15-3046-day-change-window-verdict.md``.
Contract: ``docs/proposals/ta/2026-09-15-3046-transition-verdict-contract.md``. Refs
#3046, #2261, #3031.

⚠⚠ THIS MODULE DEFINES NO RULE. ``rule_w1`` and ``rule_w2`` live in
``app.services.price_quarantine`` and are CALLED here, never mirrored — the
measurement script ``verify_3046_consumer_exposure`` made exactly that mirroring
mistake once with ``rule_w1`` and had it deleted. What is new is the transport (a
batch loader) and the COMPOSITION (which clauses a windowed consumer owes).

WHY IT EXISTS. The contract found four damage kinds that a windowed return consumer
needs to hold at once, and two of them — ``rule_w1`` (the window SPANS a
non-return) and ``rule_w2`` (the window's HORIZON is stretched) — had **zero
production callers**. They were written, unit-tested and versioned, and nothing read
them. This is the carrier.

⚠ FOUR CLAUSES, AND THEY COMPOSE. They are not alternatives a consumer picks between:

    1  a bar FIELD is unusable            B1/B4   ``return_unusable_bars``
    2  the two sides cannot be joined     T3      ``unresolved_breaks``
    3  the window SPANS a non-return      W1      ``quarantined_transitions``
    4  the window's HORIZON is stretched  W2      window bounds + stored bar count

⚠ CONTAINMENT, NEVER REPAIR. Every verdict here is a refusal or a flag. There is
nothing to repair towards: for a level break both bars are valid in their own unit
regime (``sql/247:83-88``), and for T1/T2 the cause is unknowable
(``price_quarantine.py:5-6`` — *"It NEVER identifies a cause"*).

⚠ IDENTITY. Nothing here is stored — the assessment is computed per request and
written to no table, column or ledger row — so there is no artefact to replay and no
version to carry. **The first caller that STORES an assessment, or that sits on a
strategy path, owes this module an ``INPUT_RULE_SETS`` entry** in
``strategy_registry`` (#3031). Today's only caller is an API display path.

⚠ RESOLVED BREAKS ARE DATA HERE, NOT A FILTER. ``sql/246:4-10`` reclassifies a
quarantined transition with an active adjustment, and ``price_segments`` honours that
because its callers apply the adjustment factor. A caller that divides two RAW closes
must not: ``price_daily`` is raw vendor data (``sql/247:4-6``; the only
``UPDATE price_daily`` in the tree writes derived feature columns), so a resolved
1 -> 10 break still divides to +900%. ``unresolved_breaks`` is clause 2's operand;
clause 3's ``quarantined_transitions`` is deliberately NOT filtered by resolution,
and a caller that applies adjustments should filter it itself.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import psycopg

from app.services.price_quarantine import (
    RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION,
)
from app.services.price_quarantine import (
    ClassParams,
    rule_w1,
    rule_w2,
)

#: The window is clean on every clause, inside an evaluated interval.
VERDICT_OK = "ok"
#: Nothing said the window is bad; nothing established that it is good either.
#: ``sql/247:23-33`` — *"Everything else reads UNKNOWN, which is not the same as
#: usable."* What a consumer DOES with it is the consumer's policy, not this
#: module's: a scan drops it, a display surface may render it flagged.
VERDICT_UNVERIFIED = "unverified"
#: At least one clause fired. The quantity computed over this window is not the
#: quantity it claims to be.
VERDICT_QUARANTINED = "quarantined"

#: Closed vocabulary. The first four suppress; the rest are UNKNOWN reasons that
#: are kept DISTINCT even though today's consumer treats them alike, so a later
#: consumer can split them without re-deriving the distinction.
REASON_BAR_RETURN_UNUSABLE = "bar_return_unusable"  # clause 1
REASON_UNRESOLVED_BREAK = "unresolved_break"  # clause 2
REASON_QUARANTINED_TRANSITION = "quarantined_transition"  # clause 3
REASON_HORIZON_STRETCHED = "horizon_stretched"  # clause 4
REASON_COVERAGE_MISSING = "coverage_missing"
REASON_COVERAGE_BEFORE_FIRST_BAR = "coverage_before_first_bar"
REASON_COVERAGE_AFTER_LAST_BAR = "coverage_after_last_bar"
REASON_VERDICT_DEFERRED = "verdict_deferred"

_QUARANTINING_REASONS = frozenset(
    {
        REASON_BAR_RETURN_UNUSABLE,
        REASON_UNRESOLVED_BREAK,
        REASON_QUARANTINED_TRANSITION,
        REASON_HORIZON_STRETCHED,
    }
)

#: ``rule_w2`` re-asked in TRADING-DAY units. ⚠⚠ BOTH SIDES MOVE, OR THE WEEKEND IS
#: DEDUCTED TWICE: ``ClassParams.calendar_days_per_bar = 1.4`` IS ``7/5`` and already
#: carries the weekend allowance, so shortening the observed span by its weekend days
#: while keeping 1.4 as the nominal excuses genuinely stretched windows (Codex
#: checkpoint 2 on ``verify_3046_contract_decision``: 20 bars over 60 calendar days /
#: 44 weekdays returned "explained"). ``1`` is the rule set's own ``_SEVEN_DAY``
#: value and is the correct nominal for a span that no longer contains weekends.
#:
#: ⚠ ``magnitude_threshold`` and ``hole_days`` are UNREAD by ``rule_w2`` — it takes
#: ``calendar_days_per_bar`` alone. They are deliberately IMPOSSIBLE rather than
#: plausible: if ``rule_w2`` ever grows a dependency on either, a 0x magnitude gate
#: and a 0-day hole tolerance fail loudly instead of quietly re-classifying windows.
_TRADING_DAY_PARAMS = ClassParams(magnitude_threshold=Decimal(0), hole_days=0, calendar_days_per_bar=Decimal(1))

#: Days of an instrument's OWN history, ending at its own last stored bar, over which
#: the weekend-session habit is measured. Ends at the instrument's last bar rather
#: than at ``current_date`` so a delisted or dormant name is judged on its own final
#: year instead of on a window in which it has no bars at all.
WEEKEND_HABIT_DAYS = 365

#: ⚠⚠ FIXED BY CONSTRUCTION, NOT TUNED — no published formulation exists for "does
#: this venue treat Saturday as a session", so the rule is built and its constant
#: frozen here (repo rule: invent no citation, leave no threshold implicit).
#:
#: CONSTRUCTION: a genuinely seven-day product trades BOTH weekend days, so its
#: weekend share of bars tends to 2/7 = 0.2857. A five-day venue tends to 0. The cut
#: is HALF of the seven-day signature — at least half the weekend days in the lookback
#: carry a bar — which is 1/7 = 0.1429.
#:
#: ⚠ The cut lands inside a MEASURED EMPTY BAND, so no instrument sits near it.
#: Reproduce with the ratio histogram in
#: ``scripts/verify_3046_day_change_verdict.py --weekend-habit``: on 12,157
#: instruments with >= 20 bars in their lookback, 11,802 have ratio <= 0.0238 and 307
#: have ratio >= 0.2256, and the band 0.1250 -> 0.2256 is EMPTY.
#:
#: ⚠ A boolean ("any weekend bar in a year") was measured and REJECTED: it admits FX
#: names carrying a handful of incidental Sunday bars — reintroducing the exact
#: false positive Codex checkpoint 1 removed.
WEEKEND_SESSION_RATIO = Decimal(1) / Decimal(7)


@dataclass(frozen=True)
class WindowInputs:
    """Everything the four clauses need for ONE instrument, over one date bound.

    ⚠ ``coverage`` is an INTERVAL, not an existence test. ``sql/247:23-33`` makes a
    bar usable only if it falls inside ``[first_bar, last_bar]``, so a date-only
    mapping cannot express a partially-covered history — which is the correction that
    withdrew the first attempt at a consumer (``e6c8d6ee``).

    ⚠ ``coverage is None`` does NOT empty the other fields. An instrument can carry
    verdict rows with no current coverage row, and discarding them in order to report
    UNKNOWN would lose exactly the evidence the precedence rule in ``assess_window``
    is about (Codex checkpoint 1 on the spec).
    """

    coverage: tuple[date, date] | None
    quarantined_transitions: tuple[date, ...]
    deferred_transitions: tuple[date, ...]
    unresolved_breaks: tuple[date, ...]
    return_unusable_bars: tuple[date, ...]
    weekend_bar_dates: frozenset[date]
    """Saturdays and Sundays on which THIS instrument has a stored bar.

    ⚠ Used ONLY for a five-day venue, and only to REFUSE a deduction: a day the
    instrument actually printed on is not deducted from the observed span, because
    shortening the span below what was observed is how a genuinely gappy window
    (Wednesday to Sunday, two stored bars) is talked out of firing. Measured: 6 live
    instruments turn on exactly this.
    """

    trades_weekends: bool
    """Whether THIS instrument's venue treats weekends as sessions.

    ⚠⚠ A HABIT, AND IT ANSWERS A DIFFERENT QUESTION FROM ``weekend_bar_dates``.
    Presence answers "should a day this instrument printed on be deducted?"; the
    habit answers "is an ABSENT Saturday a closure or a hole?". The first version
    had presence alone, so an absent Saturday was always a closure — true for an
    equity, FALSE for a 24/7 product, where it is exactly the hole ``rule_w2``
    exists to catch. That returned ``ok`` on a stretched window (Codex checkpoint 2).

    ⚠ Replacing presence with the habit instead of composing them was measured and
    REJECTED: it fixed the seven-day case and lost 6 five-day suppressions, trading
    one defect for a larger one.

    Measured over the instrument's own history, so the asset class is never asked —
    see ``WEEKEND_SESSION_RATIO`` for the construction and the empty band it sits in.
    """


@dataclass(frozen=True)
class WindowAssessment:
    verdict: str
    reasons: tuple[str, ...]

    @property
    def is_quarantined(self) -> bool:
        return self.verdict == VERDICT_QUARANTINED


#: ⚠ ONE QUERY, NOT SIX. Every field shares one snapshot by construction — under READ
#: COMMITTED a second statement sees a later commit, and a quarantine refresh running
#: between two of them would pair a verdict with prices it did not evaluate. It is
#: also why this is not an N+1: the ``unnest`` drives, so every requested instrument
#: gets exactly one row whether or not it has coverage or verdicts.
_LOAD_SQL = """
    SELECT ids.instrument_id,
           cov.first_bar,
           cov.last_bar,
           COALESCE(qt.dates, '{}')  AS quarantined_transitions,
           COALESCE(dft.dates, '{}') AS deferred_transitions,
           COALESCE(brk.dates, '{}') AS unresolved_breaks,
           COALESCE(bar.dates, '{}') AS return_unusable_bars,
           COALESCE(wkd.dates, '{}') AS weekend_bar_dates,
           COALESCE(wke.trades_weekends, false) AS trades_weekends
    FROM unnest(%(instrument_ids)s::bigint[], %(window_starts)s::date[])
           AS ids(instrument_id, since)
    LEFT JOIN price_quarantine_coverage cov
      ON cov.instrument_id = ids.instrument_id
     AND cov.rule_set_version = %(quarantine_version)s
    LEFT JOIN LATERAL (
        SELECT array_agg(q.price_date ORDER BY q.price_date) AS dates
        FROM price_transition_quarantine q
        WHERE q.instrument_id = ids.instrument_id
          AND q.rule_set_version = %(quarantine_version)s
          AND q.price_date >= ids.since
          AND cardinality(q.rules) > 0
    ) qt ON TRUE
    LEFT JOIN LATERAL (
        SELECT array_agg(q.price_date ORDER BY q.price_date) AS dates
        FROM price_transition_quarantine q
        WHERE q.instrument_id = ids.instrument_id
          AND q.rule_set_version = %(quarantine_version)s
          AND q.price_date >= ids.since
          AND q.provisional
          AND cardinality(q.rules) = 0
    ) dft ON TRUE
    LEFT JOIN LATERAL (
        SELECT array_agg(b.break_date ORDER BY b.break_date) AS dates
        FROM price_series_break b
        WHERE b.instrument_id = ids.instrument_id
          AND b.break_date >= ids.since
          AND b.resolved_by IS NULL
    ) brk ON TRUE
    LEFT JOIN LATERAL (
        SELECT array_agg(q.price_date ORDER BY q.price_date) AS dates
        FROM price_bar_quarantine q
        WHERE q.instrument_id = ids.instrument_id
          AND q.rule_set_version = %(quarantine_version)s
          AND q.price_date >= ids.since
          AND q.return_usable = false
    ) bar ON TRUE
    LEFT JOIN LATERAL (
        SELECT array_agg(d.price_date ORDER BY d.price_date) AS dates
        FROM price_daily d
        WHERE d.instrument_id = ids.instrument_id
          AND d.price_date >= ids.since
          AND extract(isodow FROM d.price_date) >= 6
    ) wkd ON TRUE
    LEFT JOIN LATERAL (
        -- ⚠ The weekend-session HABIT, over the instrument's own last
        -- ``weekend_habit_days`` of history — NOT the window, and NOT ``since``.
        -- Asking the window ("is there a bar this Saturday?") cannot tell a closed
        -- venue from a hole in a 24/7 series; asking the habit can.
        SELECT coalesce(
                   count(*) FILTER (WHERE extract(isodow FROM d.price_date) >= 6)::numeric
                     / nullif(count(*), 0),
                   0
               ) >= %(weekend_session_ratio)s AS trades_weekends
        FROM price_daily d
        WHERE d.instrument_id = ids.instrument_id
          AND d.price_date > (
              SELECT max(x.price_date) FROM price_daily x
              WHERE x.instrument_id = ids.instrument_id
          ) - %(weekend_habit_days)s
    ) wke ON TRUE
"""


def load_window_inputs(
    conn: psycopg.Connection[Any],
    window_starts: Mapping[int, date],
) -> Mapping[int, WindowInputs]:
    """Load every clause operand for each instrument, bounded at ITS OWN window start.

    ``window_starts`` maps instrument id -> the earliest date that instrument's
    caller will assess. Each id's bound must be on or before its own
    ``window_start``: a verdict before it is invisible to every clause.

    ⚠⚠ THE BOUND IS PER-INSTRUMENT, NOT A BATCH MINIMUM, AND THE DIFFERENCE IS
    MEASURED. The first version took one ``since`` for the whole batch, so a single
    instrument with an old window dragged every other instrument's scan back with
    it. On the worst 200-id page in the corpus (the 199 densest histories plus the
    instrument holding the oldest window, 2020-12-21) that cost **45.2 ms against
    10.9 ms** — a 4x difference on a page the list endpoint really serves.
    ⚠ It was very nearly dismissed on a CONFOUNDED comparison: two DIFFERENT pages,
    one of which happened to be faster. Only the same ids with the bound alternated
    isolates it (review bot NITPICK 2, upheld at Codex checkpoint 3).

    Returns an entry for EVERY requested id, including ids with no coverage row and
    no verdicts. A missing key therefore means "not requested"; ``assess_window``
    still handles ``None`` defensively, and ``coverage is None`` — not absence — is
    how "never evaluated" is reported.
    """
    pairs = sorted((int(i), d) for i, d in window_starts.items())
    if not pairs:
        return {}
    rows = conn.execute(
        _LOAD_SQL,
        {
            "instrument_ids": [i for i, _ in pairs],
            "window_starts": [d for _, d in pairs],
            "quarantine_version": QUARANTINE_RULE_SET_VERSION,
            "weekend_habit_days": timedelta(days=WEEKEND_HABIT_DAYS),
            "weekend_session_ratio": WEEKEND_SESSION_RATIO,
        },
    ).fetchall()
    out: dict[int, WindowInputs] = {}
    for instrument_id, first_bar, last_bar, trans, deferred, breaks, bars, weekend, habit in rows:
        out[int(instrument_id)] = WindowInputs(
            coverage=(first_bar, last_bar) if first_bar is not None else None,
            quarantined_transitions=tuple(trans),
            deferred_transitions=tuple(deferred),
            unresolved_breaks=tuple(breaks),
            return_unusable_bars=tuple(bars),
            weekend_bar_dates=frozenset(weekend),
            trades_weekends=bool(habit),
        )
    return out


def _stripped_end(
    window_start: date,
    window_end: date,
    *,
    trades_weekends: bool,
    weekend_bar_dates: frozenset[date],
) -> date:
    """``window_end`` moved back by the NON-SESSION weekend days the span contains.

    ⚠⚠ THE ASSET CLASS CANNOT ANSWER THIS, AND ASKING IT IS A MEASURED DEFECT.
    The obvious gate is ``ClassParams.calendar_days_per_bar != 1`` ("is this a
    five-day class"). The rule set declares ``fx``, ``commodity`` and ``index``
    SEVEN-day — for hole tolerance, not because those venues trade Saturdays — so
    that gate denies them the weekend allowance and fires W2 on an ordinary
    Friday-to-Monday pair. Measured on the full live corpus: **28 of 63 FX
    day-changes (44%)** would have been suppressed. It fails in the other direction
    too: eToro's ``.24-7`` synthetics are typed ``us_equity`` and DO trade weekends.

    ⚠⚠ THE WINDOW ALONE CANNOT ANSWER IT EITHER, WHICH IS THE SUBTLER ERROR. The
    second version asked only "does this instrument have a bar on THIS Saturday" and
    stripped the day when it did not. That reads an ABSENT bar as a CLOSED venue —
    sound for an equity, and wrong for a seven-day product, where an absent Saturday
    is a HOLE. It deleted the very gap ``rule_w2`` is there to find and returned
    ``ok`` (Codex checkpoint 2).

    ⚠⚠ SO BOTH SIGNALS ARE USED, BECAUSE THEY ANSWER DIFFERENT QUESTIONS — and using
    either alone is a measured defect:

    - the HABIT decides whether an ABSENT weekend day is a closure or a hole. A
      seven-day venue strips nothing, so its missing Saturday stretches the span and
      W2 sees it.
    - PRESENCE decides whether a day the instrument actually PRINTED on may be
      deducted. It may not: deducting it shortens the span below what was observed,
      which is how a Wednesday-to-Sunday window with two stored bars is talked out of
      firing. Dropping this and keeping the habit alone was measured: it lost 6 live
      suppressions to buy the seven-day fix.

    The span is ``(window_start, window_end]``, matching ``(window_end -
    window_start).days``.
    """
    if trades_weekends:
        return window_end
    stripped = sum(
        1
        for offset in range(1, (window_end - window_start).days + 1)
        if (day := window_start + timedelta(days=offset)).isoweekday() >= 6 and day not in weekend_bar_dates
    )
    return window_end - timedelta(days=stripped)


def assess_window(
    inputs: WindowInputs | None,
    *,
    window_start: date,
    window_end: date,
    bar_count: int,
) -> WindowAssessment:
    """Compose all four clauses over one window. Pure; no I/O.

    ``bar_count`` is the number of STORED bars in ``[window_start, window_end]`` —
    never a rank and never a calendar estimate. ``rule_w2`` takes ``bar_count - 1`` as
    its interval count, and the ``by_rank[r]`` off-by-one that read ``r + 1`` made W2
    under-fire by 6x on the corpus scan.

    ⚠ A FIRED CLAUSE OUTRANKS A COVERAGE REASON, and every clause is evaluated
    regardless of coverage: positive evidence beats absence, and an instrument with
    no coverage row can still carry verdict rows. Both reason lists are returned
    either way, so nothing is lost by the precedence.
    """
    if window_end < window_start:
        raise ValueError(f"window_end {window_end} precedes window_start {window_start}")
    if bar_count < 1:
        raise ValueError(f"bar_count must be >= 1, got {bar_count}")

    reasons: list[str] = []
    if inputs is None or inputs.coverage is None:
        reasons.append(REASON_COVERAGE_MISSING)
    else:
        first_bar, last_bar = inputs.coverage
        if window_start < first_bar:
            reasons.append(REASON_COVERAGE_BEFORE_FIRST_BAR)
        if window_end > last_bar:
            reasons.append(REASON_COVERAGE_AFTER_LAST_BAR)

    if inputs is not None:
        # Clause 1 is INCLUSIVE of both endpoints: a condemned bar anywhere in the
        # window damages a quantity computed over it, including the operands.
        if any(window_start <= d <= window_end for d in inputs.return_unusable_bars):
            reasons.append(REASON_BAR_RETURN_UNUSABLE)
        # Clauses 2, 3 and the deferred check are `(start, end]` — `rule_w1`'s own
        # convention, because the transition INTO the first bar happened before the
        # window opened and does not contaminate it.
        if rule_w1(window_start, window_end, inputs.unresolved_breaks):
            reasons.append(REASON_UNRESOLVED_BREAK)
        if rule_w1(window_start, window_end, inputs.quarantined_transitions):
            reasons.append(REASON_QUARANTINED_TRANSITION)
        if rule_w1(window_start, window_end, inputs.deferred_transitions):
            reasons.append(REASON_VERDICT_DEFERRED)

    # ⚠ Absent inputs means the habit is unknown. Defaulting to False (weekends are
    # non-sessions) keeps the span SHORTER, so an unknown instrument is not quarantined
    # on an assumption — the UNKNOWN reasons above already carry that state honestly.
    trades_weekends = inputs.trades_weekends if inputs is not None else False
    weekend_bars = inputs.weekend_bar_dates if inputs is not None else frozenset()
    if rule_w2(
        window_start,
        _stripped_end(
            window_start,
            window_end,
            trades_weekends=trades_weekends,
            weekend_bar_dates=weekend_bars,
        ),
        bar_count,
        _TRADING_DAY_PARAMS,
    ):
        reasons.append(REASON_HORIZON_STRETCHED)

    ordered = tuple(sorted(set(reasons)))
    if _QUARANTINING_REASONS & set(ordered):
        return WindowAssessment(VERDICT_QUARANTINED, ordered)
    if ordered:
        return WindowAssessment(VERDICT_UNVERIFIED, ordered)
    return WindowAssessment(VERDICT_OK, ())


__all__ = [
    "WEEKEND_HABIT_DAYS",
    "WEEKEND_SESSION_RATIO",
    "REASON_BAR_RETURN_UNUSABLE",
    "REASON_COVERAGE_AFTER_LAST_BAR",
    "REASON_COVERAGE_BEFORE_FIRST_BAR",
    "REASON_COVERAGE_MISSING",
    "REASON_HORIZON_STRETCHED",
    "REASON_QUARANTINED_TRANSITION",
    "REASON_UNRESOLVED_BREAK",
    "REASON_VERDICT_DEFERRED",
    "VERDICT_OK",
    "VERDICT_QUARANTINED",
    "VERDICT_UNVERIFIED",
    "WindowAssessment",
    "WindowInputs",
    "assess_window",
    "load_window_inputs",
]
