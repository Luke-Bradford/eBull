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
