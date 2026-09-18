"""May a core rebalance be submitted against the WORLD right now? (#2603 item 3, step 3b-1).

Step 3a (:mod:`app.services.strategy_core_submission_gate`) decides whether a stored
*verdict* may become an order -- supersession, mandate revision, in-flight
suppression, eligibility.  It closes by naming what it does NOT carry: the kill
switch, ``enable_auto_trading``, the execution block, market-session state, quote
availability and staleness, account-risk availability, broker minimums, cost
assessment and broker rejection.

This module is the **DB-and-clock** half of that list.  Every input is a table read
or the wall clock, so it holds no broker handle and cannot mutate broker state.

The **broker** half -- account-risk availability, what-if cost assessment, the
broker minimum, broker rejection -- stays with step 3b-2.  The split is on "does
this need a broker", which is the line between a refusal provable in a pure test
and one observable only against a live account.

The attended core executor now calls this preflight after the stored-intent gate.
It writes nothing and authorises nothing alone; the executor composes its verdict
with broker preflight before durable order authority can be recorded.

⚠ NOT the complete vocabulary either.  It is the complete DB-and-clock vocabulary.
A reader who takes this module plus step 3a for the whole set will miss the
broker's own view of the account. The executor's next layer,
``strategy_core_broker_preflight``, performs those reads.

Spec: ``docs/proposals/ta/2026-08-14-core-submission-preflight.md``
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Final, Literal, LiteralString

import psycopg

from app.services.market_session_support import (
    session_support_reason,
    venue_local_now,
    venue_session_is_open,
)
from app.services.runtime_config import RuntimeConfigCorrupt, get_runtime_config
from app.services.strategy_core_mandate import CORE_MANDATE_ADVISORY_LOCK
from app.services.strategy_core_submission_gate import (
    CORE_SUBMISSION_ADVISORY_LOCK,
    core_lock_held,
)
from app.services.strategy_halt_identity import INSTRUMENT_HALT_SYMBOL_SQL
from app.services.strategy_halts import MAX_SOURCE_LAG

#: Frozen with the rule set it stamps.  v2 fixes, BY CONSTRUCTION: the two
#: freshness bounds below (their INTERVAL is derived from each producer's cadence,
#: their POSITION in that interval is a choice -- see the constants), the
#: ``us_equity`` session allow-list, the refusal precedence order, and the shared
#: versioned Nasdaq/eToro halt-symbol identity rule.
#:
#: v3 (#3157): ``CORE_MAX_QUOTE_AGE_SECONDS`` is re-derived from
#: ``core_candidate_quote_refresh`` (300 s, one lost fire tolerated) rather than from
#: ``quotes_refresh`` (3600 s), moving it 5400 -> 750 s.  Nothing else in the rule set
#: moves: the halt bound, the ``us_equity`` allow-list, the precedence order and the
#: halt-symbol identity rule are byte-identical.
CORE_PREFLIGHT_POLICY_VERSION: Final = "core-preflight-v3"

#: The venue allow-list and the session predicate both live in
#: ``app/services/market_session_support.py``.
#:
#: ⚠ The allow-list moved there (#2312, 2026-09-14) so the #2833 SELECTION path
#: could refuse a sleeve this module would refuse at submission time.  It could not
#: import the predicate from here: ``strategy_core_selection`` -> this module ->
#: ``strategy_core_mandate`` -> ``strategy_core_selection`` is a real cycle.  A
#: second copy of the allow-list would drift, and the drift direction is "selection
#: says ready, submission refuses" -- discovered only in an operator-attended session.
#:
#: ⚠ The session PREDICATE followed it (#2312, 2026-09-16): this module's
#: ``_session_is_open`` hardcoded ``America/New_York`` and the NYSE hours, which was
#: correct while ``us_equity`` was the only admitted class and is not a shape that
#: generalises.  ``venue_session_is_open`` dispatches on ``asset_class``; for
#: ``us_equity`` it is the same calendar, the same inclusive-open/exclusive-close
#: boundaries and the same result.  ⚠⚠ ``SESSION_SUPPORTED_ASSET_CLASSES`` is
#: unchanged at ``{"us_equity"}`` -- it is now DERIVED as "has a calendar AND has
#: halt coverage", and the UK has only the first, because ``_PREFLIGHT_SQL`` below
#: reads halts from ``nasdaq_trader_rss`` alone.  So
#: ``CORE_PREFLIGHT_POLICY_VERSION`` does not move: the admission set it freezes is
#: byte-identical.

#: Tolerated clock skew on a producer timestamp, matching
#: ``strategy_paper_executor._age_ok``.  A row stamped further into the future than
#: this is refused rather than treated as maximally fresh -- without it a corrupted
#: future timestamp never ages out.
_FUTURE_SKEW: Final = timedelta(seconds=5)


def _freshness_bound(period_seconds: int, *, tolerated_missed_fires: int = 0) -> int:
    """The freshness bound derived from a producer's nominal cadence.

    A bound BELOW one period refuses a healthy state in the tail of every cycle:
    immediately before each refresh the newest possible row is one full period old,
    so such a bound is a recurring false refusal by construction.  A bound at or
    above TWO periods does not make a stopped producer undetectable, but defers
    detection by a further full period.  So the usable interval is
    ``[period, 2 * period)``.

    ⚠ THAT INTERVAL ASSUMES EVERY SCHEDULED FIRE LANDS, and #3157 measured that it
    does not: ``core_candidate_quote_refresh`` wrote no ``job_runs`` row at all for
    2 of 352 slots in its first 29 hours, so its realised inter-arrival reaches
    600 s against a 300 s cadence.  ``tolerated_missed_fires`` (``k``) admits that:
    with ``k`` consecutive losses tolerated the same reasoning gives
    ``[(k + 1) * period, (k + 2) * period)`` -- below it refuses whenever ``k``
    fires are lost, at or above it defers detection by a further period.

    ⚠ WHERE IN THAT INTERVAL IS A CHOICE, NOT A DERIVATION, and so is ``k``.  The
    midpoint tolerates half a period of scheduler lateness (lane ticks, prerequisite
    skips and ``catch_up_on_boot`` all make lateness real) while capping detection
    latency below the next whole period.  Nothing in the producer settles either
    trade-off, so both are frozen in ``CORE_PREFLIGHT_POLICY_VERSION`` rather than
    presented as derived.  ``k`` is a POLICY tolerance informed by an observed loss
    rate; it is not a prediction of the maximum future loss.

    ⚠ Derived from NOMINAL cadence only.  Dispatch latency, fetch duration, retries
    and lane contention are not modelled.  What this bounds is the AGE of the row a
    verdict is reached on; it is not producer-health detection (``quotes`` has other
    writers -- see ``CORE_MAX_QUOTE_AGE_SECONDS``) and it is not an SLO.  The
    coupling tests prove configuration agreement, not that a producer lands a row
    inside the bound.
    """
    if period_seconds <= 0:
        raise ValueError(f"period_seconds must be positive, got {period_seconds}")
    if tolerated_missed_fires < 0:
        raise ValueError(f"tolerated_missed_fires must be >= 0, got {tolerated_missed_fires}")
    return period_seconds * (2 * tolerated_missed_fires + 3) // 2


#: ``core_candidate_quote_refresh`` is ``Cadence.every_n_minutes(interval=5)``
#: (``scheduler.CORE_QUOTE_REFRESH_INTERVAL_MINUTES``) -> period 300 s, with ONE
#: lost fire tolerated -> 750 s.  #3157.
#:
#: ⚠ It was ``_freshness_bound(3600)`` = 5400 s until #3157, derived from
#: ``quotes_refresh``.  #3118 gave the core cohort its own five-minute producer and
#: deliberately did NOT move this constant, because tightening a bound ahead of any
#: run history for the producer it newly depends on can refuse for a reason nobody
#: has observed.  The history now exists and is what sets ``k = 1``: 2 of 352
#: intervals reached 600 s, each a slot for which NO ``job_runs`` row of any status
#: was written, so ``k = 0`` (450 s) would refuse a healthy state for ~150 s after
#: each loss -- the "refuses on the clock rather than on the market" defect #3118
#: exists to remove.
#:
#: ⚠⚠ THIS IS NOT PRODUCER-HEALTH DETECTION, and reading it as such overstates it.
#: ``quotes`` is also written by ``quotes_refresh`` (hourly) and by
#: ``etoro_websocket.upsert_quote``, so a dead ``core_candidate_quote_refresh`` does
#: not age this row without limit -- the hourly write keeps it under 3600 s and
#: admission reopens briefly after each one.  What the bound guarantees is narrower
#: and is the property a verdict actually needs: an ADMITTED submission is admitted
#: on a row at most this old.
#:
#: ⚠ It does NOT bound the row behind the quote-QUALITY refusals.
#: ``core_quote_price_invalid``, ``core_quote_crossed`` and
#: ``core_quote_spread_flagged`` are returned BEFORE ``_age_ok`` runs (see the
#: precedence order in ``_decide``), so a stale and crossed quote reports
#: ``core_quote_crossed``, not ``core_quote_stale``.  Left that way deliberately:
#: every one of those branches refuses, so the safety property is identical and only
#: the diagnosis label differs, and the return ORDER is itself a frozen contract
#: (``CorePreflightRefusal``) that #3157 has no evidence to re-open.
CORE_MAX_QUOTE_AGE_SECONDS: Final = _freshness_bound(300, tolerated_missed_fires=1)

#: ``strategy_halt_feed_refresh`` is ``Cadence.every_n_minutes(interval=5)``
#: -> period 300 s, with NO lost fire tolerated.
#:
#: ⚠ Deliberately NOT moved by #3157, which tightened the quote bound beside it.
#: That job's in-session successful-run gaps reach 1,349-1,508 s, so it is exposed
#: to the same lost-fire defect -- but raising this constant WIDENS a halt gate,
#: which is the opposite direction of harm from tightening a quote bound, and wants
#: its own evidence and its own ticket.  Recorded rather than folded in.
#:
#: ⚠⚠ That ticket was #3159, and it left this constant BYTE-IDENTICAL on purpose.
#: The gaps were a PRODUCER defect, not a bound defect: ``strategy_halt_feed_refresh``
#: sat on the general execution lane sharing one permit with 49 other jobs, so a fire
#: parked on an unbounded semaphore acquire and suppressed its own successors.  The
#: fix moved the producer to the reserved lane (``_CORE_PREFLIGHT_FRESHNESS_PRODUCERS``);
#: widening the halt gate was rejected, because a wider gate admits a trade this bound
#: exists to refuse.  Reproduce the before/after with
#: ``scripts/measure_3159_halt_feed_lane.py``.
#:
#: ⚠ Only meaningful INSIDE an open session, which is why the session check
#: precedes it.  That job carries ``prerequisite=_strategy_halt_collection_due``
#: and runs only from 09:00 ET to the close plus 15 minutes; outside that window
#: its producer is deliberately idle and the feed is legitimately hours stale.
CORE_MAX_HALT_FEED_AGE_SECONDS: Final = _freshness_bound(300)

#: The real age of the halt INFORMATION behind an admitted core order (#3189
#: finding 1), as distinct from the age of our fetch.
#:
#: The check below ages ``fetched_at``, deliberately — see the comment at the
#: call site — and that is one of TWO bounds. The other lives in another module:
#: ``strategy_halts.store_halt_snapshot`` refuses a publication stamp more than
#: ``MAX_SOURCE_LAG`` behind the fetch it arrived with. Composed, an admitted
#: order can rest on halt information published up to this many seconds ago.
#:
#: ⚠ IT IS COMPUTED, NOT TYPED. The composition was previously stated only in a
#: prose comment, so widening either half silently widened the real ceiling while
#: the comment went on asserting the old one. Importing the ingest constant makes
#: the coupling structural, and ``tests/test_2603_core_preflight.py`` asserts both
#: halves so the change cannot be silent.
#:
#: ⚠ NOT a new refusal, and deliberately so. With both halves enforced the
#: composition CANNOT be exceeded, so a check against it would be unfireable
#: today — and tightening it is a policy choice on a path that now places real
#: demo orders, with its own refusal risk and its own evidence. What this
#: constant does is name the guarantee and make it break loudly if either half
#: moves.
#:
#: ⚠ It equals ``CORE_MAX_QUOTE_AGE_SECONDS`` by COINCIDENCE (750 = 450 + 300 and
#: 750 = _freshness_bound(300, tolerated_missed_fires=1)). Different derivations,
#: different producers — do not unify them.
CORE_MAX_HALT_SOURCE_AGE_SECONDS: Final = CORE_MAX_HALT_FEED_AGE_SECONDS + int(MAX_SOURCE_LAG.total_seconds())

CorePreflightRefusal = Literal[
    "core_runtime_config_corrupt",
    "core_auto_trading_disabled",
    "core_kill_switch_active_or_missing",
    "core_execution_block_active",
    "core_instrument_missing",
    "core_instrument_not_tradable",
    "core_unsupported_market_session",
    "core_market_session_closed",
    "core_halt_feed_missing",
    "core_halt_feed_stale",
    "core_instrument_halted",
    "core_quote_missing",
    "core_quote_price_invalid",
    "core_quote_crossed",
    "core_quote_spread_flagged",
    "core_quote_stale",
]
"""The closed vocabulary of reasons a core submission is refused by the world.

A ``Literal`` rather than ``str`` so pyright checks every ``return`` site and a code
cannot be introduced without appearing here (step 3a's device, and the allocator's
before it).

⚠ PRECEDENCE IS THE DECLARATION ORDER and it is load-bearing: an input can be
kill-switched, closed AND quote-less at once, and without a fixed order the recorded
explanation moves with a refactor.

⚠ ``core_auto_trading_disabled`` and ``core_kill_switch_active_or_missing`` are
SEPARATE codes on purpose.  ``docs/settled-decisions.md`` ("Execution guard
semantics -> Config controls"): *"`enable_auto_trading` is not the same as
`enable_live_trading`; both may be checked; neither replaces the kill switch."*
Collapsing them would report a disabled config flag as a kill-switch trip.

⚠ Feed health precedes ``core_instrument_halted``.  Both refuse, so the order
changes only which code is RECORDED -- but reporting "halted" while the feed
supporting that claim is stale blames the instrument when the actionable fault is
the infrastructure.
"""


class StrategyCorePreflightError(RuntimeError):
    """A caller contract breach -- not a refusal, and deliberately not returnable.

    Raised when the serialising advisory locks are not held, and when ``action`` is
    not one of the two the allocator emits.  Neither is a state of the world;
    returning them as verdicts would let a caller log the problem and carry on into
    the race, or size a trade off a side nobody chose.
    """


@dataclass(frozen=True)
class CorePreflightVerdict:
    """One preflight verdict, carrying the price the caller would otherwise re-read.

    ⚠ ``price`` IS A TIME-OF-CHECK VALUE AND THIS VERDICT HAS NO SHELF LIFE.
    ``admitted`` means "not refused at ``observed_at``, under this lock hold" -- it
    never means "safe to submit later".  The kill switch, the execution block and
    the quote can all move the instant the lock is released.  3b-2 submits inside
    the SAME hold of :func:`core_submission_lock` or re-runs this.
    """

    admitted: bool
    reason_code: CorePreflightRefusal | None
    detail: str | None
    """⚠ NOT decoration.  Several refusals collapse causes the caller cannot
    re-derive: which side of the quote was invalid, how stale a producer is, and
    which asset class was seen instead of a supported one."""

    core_instrument_id: int
    action: Literal["buy_core", "sell_core"]
    price: Decimal | None
    """The side's price -- ``ask`` for ``buy_core``, ``bid`` for ``sell_core``.
    ``None`` on every refusal, including refusals reached after the quote was read:
    a price that accompanied a refusal would eventually be used."""

    quoted_at: datetime | None
    observed_at: datetime
    policy_version: str


@dataclass(frozen=True)
class CorePreflightObservation:
    """Every DB fact the verdict rests on, from ONE snapshot.

    Separated from the decision so the decision is a pure function of it: the
    interesting half of this module is a 16-way precedence order, and a precedence
    order tested through a database is tested once per fixture rather than once per
    pair.

    ``instrument_present`` is carried explicitly rather than inferred from
    ``symbol is None``, because a NULL symbol on a row that EXISTS is a different
    fault from an absent row and must not silently become one.
    """

    instrument_present: bool
    symbol: str | None
    is_tradable: bool | None
    asset_class: str | None
    kill_switch_active: bool | None
    """``None`` means NO ``kill_switch`` row -- not an inactive one."""
    execution_blocked: bool
    is_halted: bool
    halt_feed_at: datetime | None
    quoted_at: datetime | None
    bid: Any
    ask: Any
    spread_flag: bool | None


# One statement, so the kill switch, execution block, instrument, exchange, halt
# state, halt feed and quote all come from ONE MVCC snapshot.  Reading them
# separately lets a verdict be assembled from several different worlds, which is an
# incoherence with no symptom.
#
# ⚠ ANCHORED ON THE PARAMETER, NOT ON `instruments`, and that is the point: with
# `instruments` as the FROM, a missing instrument returns NO ROW and the kill switch
# and execution block become unreadable in exactly the branch where an emergency
# stop still has to be reported.  A one-row anchor with LEFT JOINs always returns
# one row, so absence is a NULL column rather than a missing result.
#
# ⚠ `%(core_instrument_id)s::bigint` -- an uncast parameter compared against a
# nullable column raises psycopg3 `AmbiguousParameter`.
#
# ⚠ The instrument join is `i.exchange`, NOT `i.exchange_id` -- `exchanges` is keyed
# by a TEXT id that `instruments` stores in a column of a different name.
_PREFLIGHT_SQL: Final[LiteralString] = f"""
SELECT (i.instrument_id IS NOT NULL) AS instrument_present,
       i.symbol,
       i.is_tradable,
       e.asset_class,
       (SELECT is_active FROM kill_switch WHERE id = true) AS kill_switch_active,
       EXISTS (SELECT 1 FROM strategy_execution_blocks b WHERE b.active) AS execution_blocked,
       EXISTS (
           SELECT 1 FROM strategy_market_halts mh
           WHERE mh.source = 'nasdaq_trader_rss'
             AND mh.symbol = {INSTRUMENT_HALT_SYMBOL_SQL} AND mh.resumed_at IS NULL
       ) AS is_halted,
       (SELECT fetched_at FROM strategy_halt_feed_state WHERE source = 'nasdaq_trader_rss')
           AS halt_feed_at,
       q.quoted_at, q.bid, q.ask, q.spread_flag
FROM (SELECT %(core_instrument_id)s::bigint AS target) t
LEFT JOIN instruments i ON i.instrument_id = t.target
LEFT JOIN exchanges e ON e.exchange_id = i.exchange
LEFT JOIN quotes q ON q.instrument_id = i.instrument_id
"""


def _age_ok(observed_at: datetime, *, now: datetime, max_seconds: int) -> bool:
    """Is ``observed_at`` neither implausibly future nor older than the bound?

    Same shape and same skew allowance as the executor's, for the same reason: a
    future-stamped row must refuse rather than read as maximally fresh, or a
    corrupted timestamp never ages out.
    """
    return observed_at <= now + _FUTURE_SKEW and observed_at >= now - timedelta(seconds=max_seconds)


def _usable_price(value: Any) -> Decimal | None:
    """Coerce a stored NUMERIC to a positive finite ``Decimal``, or ``None``.

    Returns ``None`` rather than raising on a malformed value: a DB value this
    module cannot interpret is a refusal (``core_quote_price_invalid``), not a
    crash.  ``InvalidOperation``/``TypeError``/``ValueError`` all mean the same
    thing here -- the quote cannot be trusted.
    """
    if value is None:
        return None
    try:
        price = Decimal(str(value))
    except InvalidOperation, TypeError, ValueError:
        return None
    if not price.is_finite() or price <= 0:
        return None
    return price


def _require_known_action(action: str) -> None:
    """A ``Literal`` is a static promise, not a runtime invariant.

    The action decides which side of the book a trade is sized off, so there is no
    safe default to fall back to.  Raises rather than returning a refusal: an
    unrecognised action is a caller bug, and a refusal is something a caller logs
    and carries on from.
    """
    if action not in ("buy_core", "sell_core"):
        raise StrategyCorePreflightError(f"unknown core rebalance action: {action!r}")


def _refused(
    reason_code: CorePreflightRefusal,
    *,
    core_instrument_id: int,
    action: Literal["buy_core", "sell_core"],
    now: datetime,
    detail: str | None = None,
) -> CorePreflightVerdict:
    return CorePreflightVerdict(
        admitted=False,
        reason_code=reason_code,
        detail=detail,
        core_instrument_id=core_instrument_id,
        action=action,
        price=None,
        quoted_at=None,
        observed_at=now,
        policy_version=CORE_PREFLIGHT_POLICY_VERSION,
    )


def preflight_core_submission(
    conn: psycopg.Connection[Any],
    *,
    core_instrument_id: int,
    action: Literal["buy_core", "sell_core"],
    now: datetime,
) -> CorePreflightVerdict:
    """May a core rebalance of ``action`` be submitted for this instrument now?

    Returns a verdict for every input and NEVER raises to signal a refusal -- the
    allocator's posture, for the allocator's reason: a caller that must catch in
    order to learn the kill switch is on will eventually catch too broadly.  The
    only raises are :class:`StrategyCorePreflightError` for an unheld lock or an
    unknown ``action``, both caller contract breaches.

    ⚠ Reads only.  Writes nothing and calls no broker.

    ⚠ THE LOCKS MUST BE HELD, and this asserts rather than documents it (step 3a's
    device, for step 3a's reason).  Without them the kill switch and execution
    block are read at a moment with no defined relationship to the submission, and
    a caller can log an admission and walk into the race the lock exists to
    prevent.

    ⚠ The runtime config is read FIRST and SEPARATELY from the table snapshot,
    because ``get_runtime_config`` raises ``RuntimeConfigCorrupt`` and that is a
    refusal rather than an exception to leak.  Its window is accepted and stated,
    not hidden: it precedes the snapshot rather than sharing it.

    ⚠ ``now`` is the caller's clock and must be timezone-aware; every comparison
    here is against a ``TIMESTAMPTZ``.  This module holds no clock of its own so a
    verdict can be attributed to one instant shared with the observation and the
    intent.
    """
    # Before the locks and before any DB work: an unknown action cannot be made
    # safe by anything the rest of this function reads.
    _require_known_action(action)
    if not core_lock_held(conn, CORE_SUBMISSION_ADVISORY_LOCK):
        raise StrategyCorePreflightError(
            "core submission preflight requires core_submission_lock to be held; "
            "without it the kill switch and execution block are read at a moment with no "
            "defined relationship to the submission"
        )
    if not core_lock_held(conn, CORE_MANDATE_ADVISORY_LOCK):
        raise StrategyCorePreflightError(
            "core submission preflight requires the core mandate advisory lock to be held; "
            "without it the mandate naming this instrument can be replaced between this check "
            "and the trade INSERT"
        )

    def refuse(code: CorePreflightRefusal, detail: str | None = None) -> CorePreflightVerdict:
        return _refused(code, core_instrument_id=core_instrument_id, action=action, now=now, detail=detail)

    try:
        runtime = get_runtime_config(conn)
    except RuntimeConfigCorrupt as exc:
        return refuse("core_runtime_config_corrupt", str(exc))
    if not runtime.enable_auto_trading:
        return refuse("core_auto_trading_disabled")

    row = conn.execute(_PREFLIGHT_SQL, {"core_instrument_id": core_instrument_id}).fetchone()
    if row is None:  # pragma: no cover - a one-row anchor cannot return no row
        raise StrategyCorePreflightError("core preflight observation returned no row")
    observation = CorePreflightObservation(
        instrument_present=bool(row[0]),
        symbol=row[1],
        is_tradable=row[2],
        asset_class=row[3],
        kill_switch_active=row[4],
        execution_blocked=bool(row[5]),
        is_halted=bool(row[6]),
        halt_feed_at=row[7],
        quoted_at=row[8],
        bid=row[9],
        ask=row[10],
        spread_flag=row[11],
    )
    return decide_core_preflight(observation, core_instrument_id=core_instrument_id, action=action, now=now)


def decide_core_preflight(
    observation: CorePreflightObservation,
    *,
    core_instrument_id: int,
    action: Literal["buy_core", "sell_core"],
    now: datetime,
) -> CorePreflightVerdict:
    """The precedence order, as a pure function of one observation and one clock.

    Split out from :func:`preflight_core_submission` so the ordering is testable
    without a database.  ⚠ The ORDER of the returns below IS the contract -- see
    :data:`CorePreflightRefusal`.

    ⚠ ``action`` is re-validated here even though the DB entry point validates it
    first: this is a public entry point too, and a guard that only exists on the
    other caller's path is not a guard.
    """
    _require_known_action(action)

    def refuse(code: CorePreflightRefusal, detail: str | None = None) -> CorePreflightVerdict:
        return _refused(code, core_instrument_id=core_instrument_id, action=action, now=now, detail=detail)

    # `is_active IS NULL` means no kill_switch row at all.  An ABSENT kill switch is
    # not an inactive one -- same rule as strategy_paper_executor.py:1077.  Read
    # BEFORE the instrument, so an emergency stop is never reported as a missing
    # instrument.
    if observation.kill_switch_active is None or observation.kill_switch_active:
        return refuse(
            "core_kill_switch_active_or_missing",
            "no kill_switch row" if observation.kill_switch_active is None else "kill switch is active",
        )
    if observation.execution_blocked:
        return refuse("core_execution_block_active")
    if not observation.instrument_present:
        return refuse("core_instrument_missing", f"instrument_id={core_instrument_id}")
    if not observation.is_tradable:
        return refuse("core_instrument_not_tradable", f"symbol={observation.symbol}")
    unsupported_venue = session_support_reason(observation.asset_class)
    if unsupported_venue is not None:
        return refuse("core_unsupported_market_session", unsupported_venue)
    # ⚠ Reached only for an ADMITTED venue -- `session_support_reason` is checked
    # immediately above -- so the dispatch below always finds a calendar.  The
    # detail reports the VENUE's own civil time, not New York's: on a UK refusal
    # a New York stamp is three hours of arithmetic the operator has to do to see
    # why a 16:30 London cutoff had passed.
    if not venue_session_is_open(observation.asset_class, now):
        return refuse("core_market_session_closed", venue_local_now(observation.asset_class, now).isoformat())

    # Feed health BEFORE the halt itself: reporting "halted" on the strength of a
    # feed we have just failed to trust blames the instrument for an infrastructure
    # fault.  `fetched_at` is the right column -- `strategy_halts.store_halt_snapshot`
    # raises unless `|source_pub_at - fetched_at| <= 5 min`, so a stored row's
    # `fetched_at` transitively bounds source recency.  ⚠ #3049 relaxed the companion
    # monotonicity rule: a regressed stamp carrying UNCHANGED halt content is now
    # accepted as a CDN re-serve.  The bound above survives it, because the stored
    # `source_pub_at` is held at the maximum (`GREATEST`) and every accepted stamp
    # passed the `MAX_SOURCE_LAG` check against the `fetched_at` written with it.
    #
    # ⚠ "Transitively bounds" is a COMPOSITION, and the composed number is
    # `CORE_MAX_HALT_SOURCE_AGE_SECONDS` (#3189 finding 1) -- this bound plus the
    # ingest lag allowance. That is the figure to quote when asked how old the
    # halt information behind an admitted order can be; this one answers only how
    # recently we asked.
    if observation.halt_feed_at is None:
        return refuse("core_halt_feed_missing")
    if not _age_ok(observation.halt_feed_at, now=now, max_seconds=CORE_MAX_HALT_FEED_AGE_SECONDS):
        return refuse("core_halt_feed_stale", f"fetched_at={observation.halt_feed_at.isoformat()}")
    if observation.is_halted:
        return refuse("core_instrument_halted", f"symbol={observation.symbol}")

    if observation.quoted_at is None:
        # ⚠ Reachable and NOT transient for a legal mandate whose instrument the
        # quote producer does not cover.  `quotes_refresh` gained a core-mandate
        # scope arm in this same change precisely so this is not permanent; if it
        # is seen persistently, that arm is the thing to check.
        return refuse("core_quote_missing", f"symbol={observation.symbol}")
    # BOTH sides, not merely the side being traded: corruption of the untraded side
    # is evidence the quote is incoherent, and sizing off the other half of an
    # incoherent quote is not safer for being arithmetically possible.
    bid_price = _usable_price(observation.bid)
    ask_price = _usable_price(observation.ask)
    if bid_price is None or ask_price is None:
        invalid = "bid" if bid_price is None else "ask"
        return refuse("core_quote_price_invalid", f"{invalid} is not a positive finite number")
    if bid_price > ask_price:
        # ⚠ NOT covered by `spread_flag`.  `compute_spread_pct` is
        # `(ask - bid) / mid * 100`, so a crossed book yields a NEGATIVE spread that
        # cannot exceed `max_spread_pct`, and `market_data.py:1080` leaves the flag
        # FALSE.  Relying on the flag alone fails open on the one quote shape that
        # most clearly means "do not trade on this".
        return refuse("core_quote_crossed", f"bid={bid_price} > ask={ask_price}")
    if observation.spread_flag:
        return refuse("core_quote_spread_flagged")
    if not _age_ok(observation.quoted_at, now=now, max_seconds=CORE_MAX_QUOTE_AGE_SECONDS):
        return refuse("core_quote_stale", f"quoted_at={observation.quoted_at.isoformat()}")

    return CorePreflightVerdict(
        admitted=True,
        reason_code=None,
        detail=None,
        core_instrument_id=core_instrument_id,
        action=action,
        price=ask_price if action == "buy_core" else bid_price,
        quoted_at=observation.quoted_at,
        observed_at=now,
        policy_version=CORE_PREFLIGHT_POLICY_VERSION,
    )


__all__ = [
    "CORE_MAX_HALT_FEED_AGE_SECONDS",
    "CORE_MAX_HALT_SOURCE_AGE_SECONDS",
    "CORE_MAX_QUOTE_AGE_SECONDS",
    "CORE_PREFLIGHT_POLICY_VERSION",
    "CorePreflightObservation",
    "CorePreflightRefusal",
    "CorePreflightVerdict",
    "StrategyCorePreflightError",
    "decide_core_preflight",
    "preflight_core_submission",
]
