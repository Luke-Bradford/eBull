"""Which venue classes this repo can time a submission against (#2312, #2603).

A LEAF module on purpose. The statement "we have a trading-session calendar for
this venue" is needed by the core SUBMISSION path
(``strategy_core_preflight.decide_core_preflight``) and by the core SELECTION
path (``strategy_core_selection.load_core_selection``), and those two cannot
import each other: ``strategy_core_selection`` -> ``strategy_core_preflight`` ->
``strategy_core_mandate`` -> ``strategy_core_selection`` is a real cycle,
measured 2026-09-14. Two copies of the allow-list would drift silently, and the
direction they would drift in is "selection says ready, submission refuses" --
which is only discovered in an operator-attended session.

⚠ It is no longer import-free (#2312, 2026-09-16): it imports the two calendar
modules so the session PREDICATE lives here too, next to the allow-list that
decides which venue may use it. Both calendars are themselves leaves (stdlib +
pandas only), so the cycle above is unaffected.

Source rule: ``app/services/market_calendar.py`` (NYSE published holidays +
early closes, https://www.nyse.com/markets/hours-calendars) and
``app/services/lse_market_calendar.py`` (England & Wales bank holidays per GOV.UK,
LSE published SETS hours + the 12:30 December early close). Each module carries
its own citations.

⚠⚠ **Two things must both be true before a venue is admitted, and only one of
them is a calendar.** ``strategy_core_preflight._PREFLIGHT_SQL`` reads BOTH the
halt flag and the halt-feed freshness from ``source = 'nasdaq_trader_rss'``. For a
non-US instrument ``is_halted`` is FALSE because that feed carries no such
identity, and ``halt_feed_at`` is fresh because it is polling something else -- so
``core_instrument_halted`` and ``core_halt_feed_stale`` BOTH fail open. Admitting a
venue on the strength of a calendar alone would hand it a halt gate that cannot
see it. Hence the intersection below.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time
from types import MappingProxyType
from typing import Final
from zoneinfo import ZoneInfo

from app.services.lse_market_calendar import (
    LSE_HALF_DAY_CLOSE,
    LSE_SESSION_CLOSE,
    LSE_SESSION_OPEN,
    LSE_TZ,
    LseCalendarHorizonError,
    lse_market_reason,
    lse_market_status,
)
from app.services.market_calendar import us_market_reason, us_market_status

#: NYSE regular session, in ``America/New_York`` civil time. Open INCLUSIVE at
#: 09:30, close EXCLUSIVE at 16:00 (13:00 on a half day) so a submission at the
#: closing bell refuses.
#:
#: ⚠ These moved here from ``strategy_core_preflight._session_is_open`` (#2312).
#: That function's own docstring recorded it as a SHAPE COPY of
#: ``strategy_paper_executor._session_is_open`` -- and a hand-copied predicate has
#: no compiler (``docs/review-prevention-log.md``). The executor's copy stays,
#: deliberately: #2603 scope item 5 is "explicitly NO alpha input", so the core arm
#: must not import from a 1,300-line alpha executor. Two copies remain; the
#: difference is that this one is now the single copy the CORE path uses, and the
#: reason for the other is written down rather than rediscovered.
_NYSE_TZ: Final = ZoneInfo("America/New_York")
_NYSE_SESSION_OPEN: Final = time(9, 30)
_NYSE_SESSION_CLOSE: Final = time(16, 0)
_NYSE_HALF_DAY_CLOSE: Final = time(13, 0)


@dataclass(frozen=True)
class VenueCalendar:
    """One venue's published session rules, resolved from ``exchanges.asset_class``.

    ``status`` and ``reason`` take a venue-LOCAL civil date, never a UTC one --
    both calendars are keyed that way, and handing either a UTC date silently
    shifts every boundary near midnight.
    """

    calendar_id: str
    tz: ZoneInfo
    session_open: time
    session_close: time
    half_day_close: time
    status: Callable[[date], str]
    reason: Callable[[date], str | None]


_NYSE = VenueCalendar(
    calendar_id="nyse",
    tz=_NYSE_TZ,
    session_open=_NYSE_SESSION_OPEN,
    session_close=_NYSE_SESSION_CLOSE,
    half_day_close=_NYSE_HALF_DAY_CLOSE,
    status=us_market_status,
    reason=us_market_reason,
)

_LSE = VenueCalendar(
    calendar_id="lse",
    tz=LSE_TZ,
    session_open=LSE_SESSION_OPEN,
    session_close=LSE_SESSION_CLOSE,
    half_day_close=LSE_HALF_DAY_CLOSE,
    status=lse_market_status,
    reason=lse_market_reason,
)

#: ``exchanges.asset_class`` -> the venue calendar that answers for it.
#:
#: ⚠ ``uk_equity`` is exactly four LSE-family exchanges (``7 LSE``, ``42 LSE_AIM``,
#: ``43 LSE AIM Auction``, ``44 LSE Auction``, all ``country = 'GB'``), which is why
#: one entry can serve the class. It would NOT be honest for ``eu_equity``: that is
#: 13 distinct venues with different calendars and different early-close times.
#:
#: ⚠ Exchange ``33`` (``.RTH``, 595 instruments, 595 of them ``.RTH``-suffixed)
#: needs no entry: #2312 clause 4 measured its underlyings uniformly ``us_equity``
#: (552 name-matched twins, zero non-``us_equity``), so ``asset_class`` keying
#: already resolves the product wrapper to its underlying's calendar. A schema of
#: literal per-exchange hours would have had to invent hours for it.
_ASSET_CLASS_CALENDARS: Final = MappingProxyType({"us_equity": _NYSE, "uk_equity": _LSE})

#: Venues for which ``strategy_market_halts`` actually carries evidence. Sourced to
#: ``strategy_core_preflight._PREFLIGHT_SQL``, which reads
#: ``source = 'nasdaq_trader_rss'`` for both the halt flag and the feed freshness.
#: ⚠ This is the SECOND of the two conditions, and #2312 owns closing it for the UK.
#:
#: ⚠⚠ **DO NOT SUBSTITUTE THE ELIGIBILITY PROOF FOR THIS.** #2312's own research
#: comment (2026-09-16) recommended admitting a calendar-only venue on a TIGHTER
#: FRESHNESS BOUND over ``strategy_core_eligibility_proofs.allow_open_position``,
#: reasoning that eToro refusing to open a name is sufficient to refuse it, so
#: re-proving nearer the submission closes the halt window. What is measured, and
#: what it does and does not settle:
#:
#: * ⚠ **The bit does not move for a state we KNOW obtains.** Labelling every
#:   ``uk_equity`` proof with this module's own :func:`venue_session_is_open` puts
#:   **26 of 26** observations taken while the LSE was CLOSED at ``True``,
#:   including 22:20Z on a Sunday. This is the load-bearing measurement because it
#:   is PAIRED -- the same instruments, the same bit, spanning open and closed
#:   sessions -- so selection cannot explain it. eToro's own documentation says
#:   this bit conflates market-closed with three other causes; it does not reflect
#:   the one of those four we can observe.
#: * ⚠ The whole-table ``(True, 148, 25)`` is WEAKER than it looks and must not be
#:   cited as "the bit is constant". It is a SELECTED population -- instruments are
#:   proved because they are core candidates -- and ``docs/etoro-api-reference.md``
#:   records a demo census where **1 of 8** instruments returned
#:   ``allowOpenPosition=false``. The bit does vary; it does not vary with session.
#: * ⚠⚠ What is NOT established: halt INSENSITIVITY. No halt exists anywhere in our
#:   corpus, so nothing here measures halt sensitivity in either direction. The
#:   defensible claim is *"eligibility has no demonstrated halt sensitivity"*, not
#:   *"eligibility cannot detect halts"*. It is enough to refuse the substitution --
#:   an unevidenced gate must not widen admission -- and not enough to call it inert.
#: * ⚠ Separately fatal to the recommendation as written: *false suffices to refuse*
#:   does not imply *true suffices to admit*, and freshness cannot supply the missing
#:   implication. An OPEN-permission bit also says nothing about a rebalance SELL.
#:
#: See ``docs/review-prevention-log.md``, "A substitute gate must be shown to vary
#: with the state it stands in for".
#:
#: ⚠ What DOES track the venue, and is the design the next session should cost:
#: ``quotes.quoted_at`` is eToro's own ``date`` field (``etoro.py`` ~line 708), not
#: our fetch time. Measured 2026-09-16 04:43Z with the LSE shut 13.2h, every ``.L``
#: quote was stamped ``15:29Z`` = 16:29 London, one minute before the 16:30 close,
#: and ``SPY.RTH`` ``19:59Z`` = 15:59 ET, one minute before the 16:00 NYSE close.
#: Both gaps are far longer than any producer cadence, so neither is the collector's
#: own period -- unlike the ~21-minute US extended-hours reading taken the same
#: instant, which matches the 04:23 write and is confounded.
#:
#: ⚠⚠ Three limits, before anyone builds on it. (1) It is BROKER-supplied, not
#: exchange-origin: a scheduled broker freeze, a cached snapshot or a delayed feed
#: all fit the same observation. (2) ``etoro.py`` falls back to ``now()`` when
#: ``date`` is absent or unparseable, which MANUFACTURES freshness on exactly the
#: responses a liveness gate must not trust. (3) Session-associated staleness is not
#: halt detection -- illiquidity, a delayed opening or a dropped subscription stop
#: the clock without a halt, and an auction or an indicative quote may keep it
#: moving through one.
#:
#: ⚠ And it is not reachable at today's cadence. The only SCHEDULED producer is
#: ``quotes_refresh`` (hourly @ :23) -- ``etoro_websocket.upsert_quote`` also writes
#: this table, but only for whatever is on the operator's screen, so it is no
#: unattended producer for a core instrument. ``CORE_MAX_QUOTE_AGE_SECONDS`` is
#: 5400s, derived from that cadence and NOT from any halt-risk bound. A halt-relevant
#: bound under an hourly producer does not refuse permanently -- it admits only in a
#: brief window after each write and refuses the rest of the hour, which for a
#: submission that must happen at a chosen moment is the same problem wearing a
#: better name. Specify the risk bound first, then show collection can meet it.
_HALT_COVERED_ASSET_CLASSES: Final = frozenset({"us_equity"})

#: ⚠ An ALLOW-list, and that direction is the whole point: ``exchanges.asset_class``
#: is a CHECK vocabulary that has already grown once (``mena_equity``, added by
#: ``sql/068`` over ``sql/067``'s original nine). A value added later lands on the
#: REFUSE side with no code change here. An exclusion list would have admitted it.
#:
#: ⚠⚠ DERIVED, never hand-listed (#2312, 2026-09-16). Adding a calendar cannot
#: admit a venue on its own, and neither can adding halt coverage; a venue is
#: admitted only where both exist. The failure this shape makes unrepresentable is
#: the one #2312's park comment warned about -- widening the set ahead of the
#: machinery, which reads as progress and is not.
#:
#: ⚠ This does NOT contradict ``docs/settled-decisions.md`` ("core allocation
#: (#2603) -- a non-US-listed core instrument is permitted if its eligibility proof
#: passes"). That governs what a mandate may DECLARE; this governs what we can
#: session-check. A non-US core instrument is a legal mandate whose submissions
#: refuse until its venue has both halves.
SESSION_SUPPORTED_ASSET_CLASSES: Final = frozenset(_ASSET_CLASS_CALENDARS) & _HALT_COVERED_ASSET_CLASSES


def venue_calendar_for(asset_class: str | None) -> VenueCalendar | None:
    """The calendar that answers for ``asset_class``, or ``None`` if we have none.

    ⚠ A calendar existing does NOT mean the venue is admitted -- see
    :data:`SESSION_SUPPORTED_ASSET_CLASSES`. This is the calendar question alone.
    """
    if asset_class is None:
        return None
    return _ASSET_CLASS_CALENDARS.get(asset_class)


def session_support_reason(asset_class: str | None) -> str | None:
    """``None`` when this venue class can be session-checked, else why it cannot.

    ⚠ ``None`` as the ARGUMENT (no ``exchanges`` row for the instrument) is a
    refusal, not a pass -- an unknown venue is exactly the case an allow-list
    exists to catch.

    ⚠ The returned string opens with ``asset_class=<repr>`` so the ``detail`` that
    ``core_unsupported_market_session`` records keeps the shape it already had;
    the explanation after it is additive.

    ⚠ Two distinct refusals, and the distinction is the operator-visible payoff of
    #2312: "no calendar at all" and "calendar, but no halt evidence for this
    venue" are different amounts of remaining work, and reporting the first when
    the second is true points the next session at work already done.
    """
    if asset_class in SESSION_SUPPORTED_ASSET_CLASSES:
        return None
    supported = ", ".join(sorted(SESSION_SUPPORTED_ASSET_CLASSES))
    calendar = venue_calendar_for(asset_class)
    if calendar is not None:
        return (
            f"asset_class={asset_class!r} has a trading-session calendar "
            f"({calendar.calendar_id}) but no halt feed covers that venue -- "
            "strategy_market_halts is fetched from 'nasdaq_trader_rss', which carries "
            "no identity for it, so the halt refusals would pass on evidence that "
            "cannot see the instrument"
        )
    return (
        f"asset_class={asset_class!r} has no trading-session calendar in this repo "
        f"(supported: {supported}), so no "
        "submission on that venue can be timed against its own market hours"
    )


def _require_aware(now: datetime) -> None:
    """A naive instant would be read as the HOST's local time by ``astimezone``.

    That is the one failure a session predicate must not have: it would answer
    about a different clock than the caller meant, and be right most of the time.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")


def venue_local_now(asset_class: str | None, now: datetime) -> datetime:
    """``now`` in the venue's own civil time, for an operator-facing detail.

    Falls back to UTC for a venue with no calendar so a refusal DETAIL can still
    be rendered; the refusal itself is decided by :func:`session_support_reason`,
    never by this.
    """
    _require_aware(now)
    calendar = venue_calendar_for(asset_class)
    return now.astimezone(calendar.tz) if calendar is not None else now


def venue_session_is_open(asset_class: str | None, now: datetime) -> bool:
    """Is this venue's regular continuous session open at ``now``?

    ``False`` for a venue we have no calendar for -- an unanswerable session
    question is a closed one on a submission path.

    ⚠ ``LseCalendarHorizonError`` (a date outside the calendar's verified
    population) is caught and reported as CLOSED. That is deliberate and is the
    safe direction: outside its verified horizon the LSE calendar may be missing a
    royal or commemorative closure that no rule derives, and an un-transcribed
    closure would otherwise render as a regular session.
    """
    _require_aware(now)
    calendar = venue_calendar_for(asset_class)
    if calendar is None:
        return False
    local = now.astimezone(calendar.tz)
    try:
        status = calendar.status(local.date())
    except LseCalendarHorizonError:
        return False
    if status == "closed":
        return False
    close_at = calendar.half_day_close if status == "half_day" else calendar.session_close
    return calendar.session_open <= local.time().replace(tzinfo=None) < close_at


__all__ = [
    "SESSION_SUPPORTED_ASSET_CLASSES",
    "VenueCalendar",
    "session_support_reason",
    "venue_calendar_for",
    "venue_local_now",
    "venue_session_is_open",
]
