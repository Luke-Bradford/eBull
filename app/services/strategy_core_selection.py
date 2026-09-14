"""Evidence-approved instrument for the deterministic core/cash sleeve (#2833)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Final, Literal

import psycopg

from app.services.market_session_support import session_support_reason

CORE_SELECTION_CANDIDATE_IDS: Final = (3417, 3434, 3075)
CORE_SELECTION_REQUIRED_TRADING_DAYS: Final = 5
CORE_SELECTION_MAX_COST_BPS: Final = 60
# #2833's corrected prospective declaration excludes the already-seen
# 2026-08-24 observations.  The operator surface must count the same population
# the sealed verifier will open, or it reports progress that is not evidence.
CORE_SELECTION_EVIDENCE_NOT_BEFORE: Final = datetime(2026, 8, 25, tzinfo=UTC)
#: Frozen with the construction :func:`earliest_possible_verdict_at` applies, because no
#: published formulation exists for "when can a five-common-session window close": the
#: venue calendars are published, the composition rule is ours.  v1 fixes, BY
#: CONSTRUCTION: weekends are the only non-sessions modelled, the walk starts at the
#: later of today and the day after the last common date (never before the prospective
#: boundary), and the window closes at the 00:00 UTC boundary AFTER its fifth session.
CORE_SELECTION_VERDICT_BOUND_VERSION: Final = "core-verdict-bound-v1"

#: ``date.weekday()`` is Monday-0, so 5 and 6 are the weekend.  Named because the
#: comparison is the whole of what this module models about a venue calendar.
_SATURDAY: Final = 5

# Populated only by the reviewed #2833 verdict. Choosing here before the
# prospective five-day gate completes would be adoption before measurement.
SELECTED_CORE_INSTRUMENT_ID: Final[int | None] = None
SELECTED_CORE_EVIDENCE_REF: Final[str | None] = None

CoreSelectionState = Literal["evidence_collecting", "ready", "unavailable"]


class CoreSelectionError(RuntimeError):
    """An enabled mandate does not match an evidence-approved core sleeve."""


@dataclass(frozen=True)
class CoreCandidateCoverage:
    instrument_id: int
    symbol: str
    observed_trading_days: int
    first_observed_date: date | None
    last_observed_date: date | None
    asset_class: str | None
    """``exchanges.asset_class`` for the candidate's venue, or ``None`` when the
    instrument has no ``exchanges`` row.  Carried so the selected sleeve can be
    checked against what the core EXECUTION path can actually session-check;
    ``None`` is a refusal, not a pass."""


@dataclass(frozen=True)
class CoreSelection:
    state: CoreSelectionState
    selected_instrument_id: int | None
    selected_symbol: str | None
    evidence_ref: str | None
    required_trading_days: int
    observed_trading_days: int
    max_cost_bps: int
    candidates: tuple[CoreCandidateCoverage, ...]
    missing_candidate_ids: tuple[int, ...]
    configuration_error: str | None
    earliest_possible_verdict_at: datetime
    """#2833's lower bound, recomputed by every ``load_core_selection`` call -- never
    a completion ETA, and never the same twice across a day boundary.  A FIELD rather
    than the property it replaced because it is EVIDENCE: stamped from the same read
    that produced the counts beside it, so a row cannot report 3/5 next to a bound
    derived from a later population."""

    @property
    def ready(self) -> bool:
        return self.state == "ready"


def earliest_possible_verdict_at(
    *,
    observed_trading_days: int,
    last_common_observed_date: date | None,
    verdict_window_close_date: date | None,
    now: datetime,
) -> datetime:
    """The soonest instant #2833's five-common-session window COULD close.

    A lower bound, never an ETA, and it moves: the hardcoded ``2026-09-02`` this
    replaced was derived by hand on 2026-08-24, was passed on 2026-09-02 with the
    window still at 1/5 (this ticket's 2026-09-12 gap comment says so), and was still
    what ``/strategies`` served on 2026-09-14 -- rendering "Earliest verdict" twelve
    days in the PAST under the subtitle "Lower bound if all five common sessions
    complete".  ``.claude/CLAUDE.md``: never hardcode a derived statistic, compute it.

    ⚠⚠ **Weekends are the only non-sessions modelled, and that is deliberate.** Both
    venues' holiday calendars are published -- LSE's E&W bank-holiday rule is
    researched and cited on #2312, NYSE's is on nyse.com/trade/hours-calendars -- but
    neither is encoded in this repo yet (#2312), and a holiday can only push a session
    LATER.  So omitting them keeps the answer a valid lower bound and never an
    overstatement, which is the one direction this number must not be wrong in.  The
    cost is precision: on the 2026-08-24 inputs this returns 2026-09-01, one day
    earlier than the hand-derived 2026-09-02, because 31 August is an LSE bank
    holiday.  Encoding the calendars (#2312) tightens it; nothing else needs to change.

    ⚠ **Skipping a weekend is safe in the OTHER direction too, and that is not
    self-evident -- it was challenged at Codex checkpoint 3.** If a weekend date could
    become a common date, skipping it would push the bound LATER than possible, which
    is the overstatement the paragraph above forbids.  It cannot: an ``observed`` row
    needs a quote younger than ``strategy_core_quote_observation.MAX_QUOTE_AGE`` (one
    hour, the tick cadence), and a shut venue only re-serves its last quote -- the
    module's own dev-verified note, 2026-08-23, a Sunday.  Measured on the FULL stored
    population 2026-09-14: by ISO weekday, ``observed`` rows exist on Mon/Tue/Wed only;
    Saturday is 20 rows and Sunday 420, and **every one of them is ``invalid``**.  A
    hand-written weekend fixture proves nothing about what the collector can produce.

    The walk starts at the LATER of today and the day after the last common date, and
    never before ``CORE_SELECTION_EVIDENCE_NOT_BEFORE`` -- a session before the
    prospective boundary cannot enter the population the sealed verifier opens.  Today
    itself counts when it is a weekday, including when its session has already closed
    un-observed: optimistic in the same safe direction as the holiday omission.

    ⚠ Starting at TODAY assumes a date earlier than today cannot later become common,
    which holds because ``sample_bucket`` truncates the OBSERVATION instant -- the
    collector can only ever write the hour it is running in, so there is no backfill
    path that adds an older common date behind the walk.

    ⚠⚠ **A COMPLETE window freezes on its FIFTH common date, not its latest one.**
    ``scripts/verify_2833_core_selection.py:166`` takes ``common_dates[:required_dates]``
    -- the sealed verifier's window is the first five, and a sixth observation cannot
    move a boundary that has already opened.  Anchoring on ``max(common_dates)`` instead
    (the first draft, caught by Codex at checkpoint 2) advances "Earliest verdict" every
    further session, telling the operator an already-open verdict is still pending.
    ``verdict_window_close_date`` is therefore the REQUIRED-th common date and is
    ``None`` until one exists -- which is also why the completed branch tests it rather
    than ``observed_trading_days``: the date's existence IS the completeness fact.
    """
    if now.tzinfo is None:
        # Every other clock-taking module here refuses a naive stamp rather than
        # letting `astimezone` silently read the HOST timezone -- `sample_bucket` and
        # `observe_core_sleeve` both do.  A bound computed in the wrong zone is off by
        # a day at exactly the boundary the whole function exists to name.
        raise ValueError("now must be timezone-aware")
    if verdict_window_close_date is not None:
        return _midnight_after(verdict_window_close_date)

    # Clamped, and the clamp is the loop's termination guarantee rather than a fudge:
    # reaching here means the REQUIRED-th common date does not exist, so fewer than
    # REQUIRED were counted and `remaining >= 1` already holds -- both numbers come from
    # one `common_dates` CTE.  If that ever stops being true, a non-positive `remaining`
    # would spin this walk forever inside a read path, which is the one failure mode an
    # unbounded loop must not have.
    remaining = max(CORE_SELECTION_REQUIRED_TRADING_DAYS - observed_trading_days, 1)
    today = now.astimezone(UTC).date()
    day = max(today, CORE_SELECTION_EVIDENCE_NOT_BEFORE.date())
    if last_common_observed_date is not None:
        day = max(day, last_common_observed_date + timedelta(days=1))
    counted = 0
    while True:
        if day.weekday() < _SATURDAY:
            counted += 1
            if counted == remaining:
                return _midnight_after(day)
        day += timedelta(days=1)


def _midnight_after(day: date) -> datetime:
    """The 00:00 UTC boundary at which a window ending on ``day`` closes."""
    return datetime.combine(day + timedelta(days=1), time.min, tzinfo=UTC)


_COVERAGE_SQL: Final = """
WITH candidate_dates AS (
    SELECT o.instrument_id,
           (o.sample_bucket AT TIME ZONE 'UTC')::date AS observation_date
    FROM strategy_core_quote_observations o
    WHERE o.instrument_id = ANY(%s)
      AND o.sample_bucket >= %s
      AND o.observation_status = 'observed'
    GROUP BY o.instrument_id, (o.sample_bucket AT TIME ZONE 'UTC')::date
), common_dates AS (
    SELECT observation_date
    FROM candidate_dates
    GROUP BY observation_date
    HAVING count(*) = cardinality(%s::bigint[])
)
SELECT i.instrument_id, i.symbol,
       count(d.observation_date) AS observed_days,
       min(d.observation_date) AS first_observed_date,
       max(d.observation_date) AS last_observed_date,
       (SELECT count(*) FROM common_dates) AS common_observed_days,
       e.asset_class,
       (SELECT max(observation_date) FROM common_dates) AS last_common_date,
       -- The REQUIRED-th common date, or NULL until one exists.  NOT `max(...)`:
       -- `verify_2833_core_selection.evaluate` freezes the window at
       -- `common_dates[:required_dates]`, so a sixth observation must not move a
       -- boundary that has already opened.
       (SELECT observation_date FROM common_dates
         ORDER BY observation_date OFFSET %s LIMIT 1) AS verdict_window_close_date
FROM instruments i
LEFT JOIN exchanges e ON e.exchange_id = i.exchange
LEFT JOIN candidate_dates d ON d.instrument_id = i.instrument_id
WHERE i.instrument_id = ANY(%s)
GROUP BY i.instrument_id, i.symbol, e.asset_class
ORDER BY array_position(%s::bigint[], i.instrument_id)
"""
# ⚠ The exchange join is `e.exchange_id = i.exchange` -- `exchanges` is keyed by a
# TEXT id that `instruments` stores in a column of a DIFFERENT name, and `i` has no
# `exchange_id` column at all.  Same join `strategy_core_preflight._PREFLIGHT_SQL`
# uses, and a LEFT JOIN so a candidate with no `exchanges` row yields NULL and is
# refused below rather than disappearing from the coverage list.


def load_core_selection(conn: psycopg.Connection[Any], *, now: datetime | None = None) -> CoreSelection:
    """Return reviewed selection and descriptive coverage, never infer a verdict.

    ``now`` only feeds :func:`earliest_possible_verdict_at`; nothing about the
    evidence population depends on it.  Defaulted rather than required so the four
    existing call sites keep their shape, and injectable so the bound is table-testable
    without freezing a clock.
    """
    rows = conn.execute(
        _COVERAGE_SQL,
        (
            list(CORE_SELECTION_CANDIDATE_IDS),
            CORE_SELECTION_EVIDENCE_NOT_BEFORE,
            list(CORE_SELECTION_CANDIDATE_IDS),
            # OFFSET is zero-based, so the REQUIRED-th row sits at REQUIRED - 1.
            CORE_SELECTION_REQUIRED_TRADING_DAYS - 1,
            list(CORE_SELECTION_CANDIDATE_IDS),
            list(CORE_SELECTION_CANDIDATE_IDS),
        ),
    ).fetchall()
    candidates = tuple(
        CoreCandidateCoverage(
            instrument_id=int(row[0]),
            symbol=str(row[1]),
            observed_trading_days=int(row[2]),
            first_observed_date=row[3],
            last_observed_date=row[4],
            asset_class=row[6],
        )
        for row in rows
    )
    coverage_by_id = {candidate.instrument_id: candidate for candidate in candidates}
    missing_candidate_ids = tuple(
        instrument_id for instrument_id in CORE_SELECTION_CANDIDATE_IDS if instrument_id not in coverage_by_id
    )
    # The verdict window is the first five dates observed by EVERY candidate,
    # not the minimum of three independent date counts.  The latter can reach
    # 5/5 while the common-date intersection is still only four days.
    observed_days = int(rows[0][5]) if rows and not missing_candidate_ids else 0
    # Read from the same row as `observed_days` and gated on the same condition: a
    # candidate missing from `instruments` means the population is not describable, and
    # a last-common-date without its count would let the two disagree.
    last_common_date: date | None = rows[0][7] if rows and not missing_candidate_ids else None
    window_close_date: date | None = rows[0][8] if rows and not missing_candidate_ids else None
    selected = SELECTED_CORE_INSTRUMENT_ID
    selected_candidate = None if selected is None else coverage_by_id.get(selected)
    evidence_ref = SELECTED_CORE_EVIDENCE_REF
    selection_declared = selected is not None or evidence_ref is not None
    selection_complete = (
        selected in CORE_SELECTION_CANDIDATE_IDS
        and selected_candidate is not None
        and evidence_ref is not None
        and bool(evidence_ref.strip())
    )
    configuration_error = None
    # A sleeve the core EXECUTION path would refuse must not read as `ready` here.
    # Both `require_selected_core_instrument` callers (the mandate writer and the
    # executor) key on `ready`, so refusing at DECLARATION time moves the failure
    # from the operator-attended session -- the most expensive moment available --
    # to the moment somebody writes the verdict constant.  Measured 2026-09-14:
    # `CSPX.L` (3434) and `IUSA.L` (3075) are exchange `7` = LSE / `uk_equity`, and
    # `decide_core_preflight` refuses both `core_unsupported_market_session` with
    # every other input healthy.
    if selection_complete and selected_candidate is not None:
        unsupported_venue = session_support_reason(selected_candidate.asset_class)
        if unsupported_venue is not None:
            selection_complete = False
            # The wrap adds ONLY the identity half -- which selection is at fault.  The
            # consequence is already the last clause of `unsupported_venue`, and saying
            # it twice is how an operator string starts drifting from the predicate.
            configuration_error = (
                f"the reviewed core selection names {selected_candidate.symbol} "
                f"(instrument {selected}): {unsupported_venue}"
            )
    if selection_declared and not selection_complete and configuration_error is None:
        configuration_error = "the reviewed core selection must name a declared candidate and a non-blank evidence ref"
    return CoreSelection(
        state=(
            "unavailable"
            if missing_candidate_ids or configuration_error is not None
            else "ready"
            if selection_complete
            else "evidence_collecting"
        ),
        selected_instrument_id=selected if selection_complete else None,
        selected_symbol=selected_candidate.symbol if selection_complete and selected_candidate is not None else None,
        evidence_ref=evidence_ref if selection_complete else None,
        required_trading_days=CORE_SELECTION_REQUIRED_TRADING_DAYS,
        observed_trading_days=observed_days,
        max_cost_bps=CORE_SELECTION_MAX_COST_BPS,
        candidates=candidates,
        missing_candidate_ids=missing_candidate_ids,
        configuration_error=configuration_error,
        earliest_possible_verdict_at=earliest_possible_verdict_at(
            observed_trading_days=observed_days,
            last_common_observed_date=last_common_date,
            verdict_window_close_date=window_close_date,
            now=datetime.now(UTC) if now is None else now,
        ),
    )


def require_selected_core_instrument(conn: psycopg.Connection[Any], *, instrument_id: int) -> CoreSelection:
    """Require the reviewed sleeve selection below every mandate writer."""
    selection = load_core_selection(conn)
    if not selection.ready or selection.selected_instrument_id is None:
        raise CoreSelectionError(
            "the core sleeve cannot be enabled until #2833 completes its five-trading-day cost verdict"
        )
    if instrument_id != selection.selected_instrument_id:
        raise CoreSelectionError(
            f"instrument {instrument_id} is not the evidence-approved core sleeve ({selection.selected_instrument_id})"
        )
    return selection


__all__ = [
    "CORE_SELECTION_CANDIDATE_IDS",
    "CORE_SELECTION_EVIDENCE_NOT_BEFORE",
    "CORE_SELECTION_MAX_COST_BPS",
    "CORE_SELECTION_REQUIRED_TRADING_DAYS",
    "CORE_SELECTION_VERDICT_BOUND_VERSION",
    "CoreCandidateCoverage",
    "CoreSelection",
    "CoreSelectionError",
    "earliest_possible_verdict_at",
    "load_core_selection",
    "require_selected_core_instrument",
]
