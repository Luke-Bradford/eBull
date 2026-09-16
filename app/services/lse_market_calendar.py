"""London Stock Exchange trading calendar (#2312 clauses 1-3).

The second trading-session calendar in this repo. It exists because #2834 ARM A's
tilt sleeve is made of LSE listings (`R1VL.L`, `IUMO.L`, `IUQA.L`) and
``market_session_support`` could only session-check ``us_equity``, so every one of
them refused ``core_unsupported_market_session``.

⚠⚠ **A calendar alone does NOT admit a venue to the submission path**, and this
module must not be read as doing so. ``strategy_core_preflight._PREFLIGHT_SQL``
reads BOTH the halt flag and the halt-feed freshness from
``source = 'nasdaq_trader_rss'`` -- a US feed that carries no ``.L`` identity, so
for an LSE name ``is_halted`` is FALSE because the feed cannot see it and
``halt_feed_at`` is fresh because it is polling something else. Both halt refusals
fail OPEN for the UK. See ``market_session_support`` for the intersection that
keeps ``uk_equity`` out until an LSE halt feed exists.

Source rule -- three LSE-published documents, none of them inferred from our bars
(#2312 clause 2 forbids that):

* **Bank holidays.** LSE recognises the public and bank holidays of **England &
  Wales** (https://www.londonstockexchange.com/equities-trading/business-days).
  The authoritative dated list is GOV.UK https://www.gov.uk/bank-holidays.json,
  division ``england-and-wales``, frozen at
  ``tests/fixtures/gov_uk_bank_holidays_england_and_wales.json``.
* **Hours.** LSE Market Notice N18/19 attachment 1, *Consultation on market
  structure and trading hours*, published SETS timetable row
  ``SETS 07:50 08:00 16:30 16:35 08:45`` (opening-auction call / continuous start
  / closing-auction call / end / duration), and its option E *"Maintain the
  current time of 08:00 - 16:30 London time"*. Corroborated on a 2025 document:
  LSEG *Turquoise Trading Calendar 2025*
  (``docs.londonstockexchange.com/.../lse-turquoise-calendar-2025.pdf``), header
  *"Continuous trading ... 08:00 / Market Close ... 16:30"* in **London times**.
* **Half days.** LSE Service Announcement 001/17122014: *"SETS, International
  Order Book (IOB), Order Book for Retail Bonds (ORB), SETSqx & International
  Board closing auctions will all commence at 12:30GMT"* on 24 & 31 December. The
  same Turquoise calendar marks ``Wed 24-Dec-25`` and ``Wed 31-Dec-25`` Early
  Close in its **London Stock Exchange** column, with an *"Early Closing Time"* of
  **12:30**.

⚠ Stated rather than papered over: the GOVERNING artefact for timings is the
*Millennium Exchange & TRADEcho Business Parameters* document, which MIT201 Issue
15.8 §4.4 delegates to and which is not publicly downloadable. The citations above
are LSE-published and dated 2019 and 2025; MIT201 15.8 (effective 2026-01-19)
still describes the same session sequence. That is the strongest available basis
without a customer login.

⚠ The ``08:00-16:30`` window is the **SETS** timetable. Bank holidays are
venue-wide and apply to all four LSE-family exchanges (``7 LSE``, ``42 LSE_AIM``,
``43 LSE AIM Auction``, ``44 LSE Auction``), but AIM and SETSqx instruments can run
different market models, so the HOURS half is verified for SETS only. Inert today
because nothing is admitted; it must be checked before exchange 42/43/44 is.

⚠⚠ This is a **scheduled-window** claim, never a trading-state guarantee.
Volatility auctions, price-monitoring extensions, EDSP auctions and suspensions all
occur inside the window (MIT201 §4.4, §7.2). The gate that would catch those is
halt coverage -- which the UK does not have, which is why the allow-list is shut.

**Substitute-day rule, and the one place copying the NYSE module would be wrong.**
UK bank-holiday substitutes always move **FORWARD** to the next free weekday;
``market_calendar``'s ``nearest_workday`` moves a Saturday holiday **BACK** onto
the preceding Friday. Using the NYSE observance here would be wrong every December
and every Saturday New Year's Day. ``next_monday_or_tuesday`` is pandas' rule for
exactly the "second of two adjacent holidays" case that Boxing Day is.

**Full-population verification.** ``(derived | additions) - suppressions`` is
asserted equal to the frozen GOV.UK population -- **by date AND by name** -- over
2019-01-01..2028-12-26, 83 dates, in ``tests/test_lse_market_calendar.py``. 78 of
83 derive from the rules; 5 royal/commemorative days cannot be derived by any rule
and are transcribed; 2 rule-derived dates are suppressed because their holiday was
MOVED, not added. ⚠ The suppression half is load-bearing: modelling a moved
holiday as an extra one leaves two ordinary trading Mondays wrongly closed.

⚠ Names are checked because the date set can be right while the labels are wrong:
``next_monday``/``next_monday_or_tuesday`` label 2022-12-26 *Christmas Day* and
2022-12-27 *Boxing Day*, and GOV.UK has them the other way round. Hence
``_REASON_OVERRIDES``.

**Supported horizon.** GOV.UK publishes a rolling window and this calendar is
verified over 2019-2028 only. Outside it every entry point RAISES
``LseCalendarHorizonError`` rather than answering. A submission gate must fail
closed on an unverified year, and a raise is loud where a silently-wrong ``open``
is not -- which is also what stops the frozen fixture going green while going
stale. When the horizon is reached, refresh the fixture and re-run the
full-population test; the constants below are the only thing to move.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime, time
from pathlib import Path
from types import MappingProxyType
from typing import Final, Literal, cast
from zoneinfo import ZoneInfo

from dateutil.relativedelta import MO
from pandas import Series, Timestamp
from pandas.tseries.holiday import (
    AbstractHolidayCalendar,
    EasterMonday,
    GoodFriday,
    Holiday,
    next_monday,
    next_monday_or_tuesday,
    weekend_to_monday,
)
from pandas.tseries.offsets import DateOffset

from app.services.market_calendar import MarketYear

RULE_SET_ID: Final = "lse-market-calendar-v1"


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


RULE_SET_VERSION: Final = f"{RULE_SET_ID}+{_code_hash()}"

#: The venue's own civil timezone. ⚠ NOT fixed GMT -- London is BST from late
#: March to late October, so the same 08:00-16:30 local window is 07:00-15:30 UTC
#: in summer and 08:00-16:30 UTC in winter. The half-day citation says "12:30GMT"
#: because it was written for a December date, when the two coincide.
LSE_TZ: Final = ZoneInfo("Europe/London")

#: Continuous trading. Open INCLUSIVE, close EXCLUSIVE -- same shape as the NYSE
#: gate's 09:30/16:00, so a submission at the closing bell refuses. The
#: 16:30-16:35 closing auction and the CPX session after it are deliberately
#: outside: an auction is not a continuous-trading submission.
LSE_SESSION_OPEN: Final = time(8, 0)
LSE_SESSION_CLOSE: Final = time(16, 30)
LSE_HALF_DAY_CLOSE: Final = time(12, 30)

#: The GOV.UK population this calendar has actually been verified against.
VERIFIED_FROM_YEAR: Final = 2019
VERIFIED_THROUGH_YEAR: Final = 2028


class LseCalendarHorizonError(ValueError):
    """A date outside the verified GOV.UK horizon was asked about.

    Deliberately an error and not a best guess: outside the verified range the
    rules may be right and may be missing a royal or commemorative day that no
    rule derives, and an un-transcribed closure renders as a regular session --
    i.e. it fails OPEN. Callers on the submission path must map this to a refusal.
    """


class _LseHolidayCalendar(AbstractHolidayCalendar):
    """England & Wales bank-holiday rules, as LSE non-business days.

    ⚠⚠ Every observance moves FORWARD. ``weekend_to_monday`` for New Year's Day
    (Sat 2022-01-01 -> Mon 2022-01-03, Sat 2028-01-01 -> Mon 2028-01-03);
    ``next_monday`` for Christmas; ``next_monday_or_tuesday`` for Boxing Day,
    which is pandas' rule for the second of two adjacent holidays and yields the
    Sat/Sun -> Mon/Tue cascade the UK actually uses.
    """

    rules = [
        Holiday("New Year's Day", month=1, day=1, observance=weekend_to_monday),
        GoodFriday,
        EasterMonday,
        Holiday("Early May bank holiday", month=5, day=1, offset=DateOffset(weekday=MO(1))),
        Holiday("Spring bank holiday", month=5, day=31, offset=DateOffset(weekday=MO(-1))),
        Holiday("Summer bank holiday", month=8, day=31, offset=DateOffset(weekday=MO(-1))),
        Holiday("Christmas Day", month=12, day=25, observance=next_monday),
        Holiday("Boxing Day", month=12, day=26, observance=next_monday_or_tuesday),
    ]


_CALENDAR = _LseHolidayCalendar()
_CACHE: dict[int, MarketYear] = {}

# Royal / commemorative closures no rule derives, transcribed from GOV.UK's
# england-and-wales division. Names are GOV.UK's own titles.
_EXTRAORDINARY_CLOSURE_NAMES: Final[dict[date, str]] = {
    date(2020, 5, 8): "Early May bank holiday (VE day)",
    date(2022, 6, 2): "Spring bank holiday",
    date(2022, 6, 3): "Platinum Jubilee bank holiday",
    date(2022, 9, 19): "Bank Holiday for the State Funeral of Queen Elizabeth II",
    date(2023, 5, 8): "Bank holiday for the coronation of King Charles III",
}

# ⚠ The dates the two MOVED holidays above were moved FROM. The scheduled rules
# still produce them, and without this they would close two ordinary trading
# Mondays. A moved holiday is not an extra holiday.
_RULE_SUPPRESSIONS: Final[dict[date, str]] = {
    date(2020, 5, 4): "Early May bank holiday moved to 2020-05-08 for VE Day 75",
    date(2022, 5, 30): "Spring bank holiday moved to 2022-06-02 for the Platinum Jubilee",
}

# ⚠ Dates the rules place correctly but LABEL wrongly. In 2022 Christmas Day fell
# on the Sunday and Boxing Day on the Monday, so Boxing Day is NOT moved and the
# Christmas substitute lands on the Tuesday -- the opposite assignment to the one
# ``next_monday`` / ``next_monday_or_tuesday`` produce. The date SET is identical
# either way, which is why the test asserts names too.
_REASON_OVERRIDES: Final[dict[date, str]] = {
    date(2022, 12, 26): "Boxing Day",
    date(2022, 12, 27): "Christmas Day",
}


def _scheduled_closure_names(year: int) -> dict[date, str]:
    """Observed scheduled E&W bank holidays landing in ``year`` -> holiday name.

    The query window straddles the adjacent year boundaries for the same reason
    ``market_calendar._scheduled_closure_names`` does. UK substitutes only move
    forward and never leave the calendar year in practice (the latest possible is
    Boxing Day -> Dec 28), but the straddle costs nothing and guards a future rule
    that does spill.
    """
    named = cast(
        Series,
        _CALENDAR.holidays(start=date(year - 1, 12, 15), end=date(year + 1, 1, 15), return_name=True),
    )
    out: dict[date, str] = {}
    for ts, name in named.items():
        stamp = cast(Timestamp, ts)
        if stamp.year == year:
            out[stamp.date()] = str(name)
    return out


def _half_day_names_for_year(year: int, full_closures: frozenset[date]) -> dict[date, str]:
    """The 12:30-London early closes for ``year``, minus any that are full
    closures (closure always wins).

    24 and 31 December when they are weekdays. Neither can collide with a
    substitute -- Christmas/Boxing substitutes reach 28 December at the latest and
    a New Year substitute lands in January -- but the closure-wins subtraction is
    applied anyway rather than reasoned around.
    """
    candidates = {
        date(year, 12, 24): "Christmas Eve",
        date(year, 12, 31): "New Year's Eve",
    }
    return {d: name for d, name in candidates.items() if d.weekday() < 5 and d not in full_closures}


def _require_supported_year(year: int) -> None:
    if not VERIFIED_FROM_YEAR <= year <= VERIFIED_THROUGH_YEAR:
        raise LseCalendarHorizonError(
            f"year {year} is outside the verified GOV.UK horizon "
            f"{VERIFIED_FROM_YEAR}-{VERIFIED_THROUGH_YEAR}; refresh "
            "tests/fixtures/gov_uk_bank_holidays_england_and_wales.json and re-run "
            "tests/test_lse_market_calendar.py before extending it"
        )


def lse_market_specials(year: int) -> MarketYear:
    """LSE full closures + half days for ``year`` (cached; immutable).

    Raises :class:`LseCalendarHorizonError` outside the verified horizon.
    """
    _require_supported_year(year)
    cached = _CACHE.get(year)
    if cached is None:
        scheduled = _scheduled_closure_names(year)
        # Suppress BEFORE anything downstream reads the set, so a reopened Monday
        # carries no holiday reason either -- `reasons` is built from the surviving
        # dates, never patched afterwards.
        surviving = {d: name for d, name in scheduled.items() if d not in _RULE_SUPPRESSIONS}
        extraordinary = {d: name for d, name in _EXTRAORDINARY_CLOSURE_NAMES.items() if d.year == year}
        closures = frozenset(surviving) | frozenset(extraordinary)
        half_day_names = _half_day_names_for_year(year, closures)
        reasons = {**surviving, **extraordinary, **half_day_names}
        reasons.update({d: name for d, name in _REASON_OVERRIDES.items() if d in reasons})
        cached = MarketYear(
            year=year,
            full_closures=closures,
            half_days=frozenset(half_day_names),
            reasons=MappingProxyType(reasons),
        )
        _CACHE[year] = cached
    return cached


LseMarketStatus = Literal["open", "half_day", "closed"]


def lse_market_status(d: date) -> LseMarketStatus:
    """LSE trading status for a ``Europe/London`` civil date.

    ``closed`` on weekends and bank holidays; ``half_day`` on a 12:30-London early
    close; ``open`` otherwise. The argument is a London-local date -- the caller
    maps the instant to a London civil date first, exactly as the NYSE counterpart
    expects a New York one.
    """
    specials = lse_market_specials(d.year)
    if d.weekday() >= 5 or d in specials.full_closures:
        return "closed"
    if d in specials.half_days:
        return "half_day"
    return "open"


def lse_session_is_open(now: datetime) -> bool:
    """Is LSE continuous trading open at ``now``?

    Open inclusive at 08:00 London, close exclusive at 16:30 (12:30 on a half
    day). ``now`` must be timezone-aware: a naive instant would be silently read
    as local time by ``astimezone``, which is the one failure this predicate must
    not have.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    local = now.astimezone(LSE_TZ)
    status = lse_market_status(local.date())
    if status == "closed":
        return False
    close_at = LSE_HALF_DAY_CLOSE if status == "half_day" else LSE_SESSION_CLOSE
    return LSE_SESSION_OPEN <= local.time().replace(tzinfo=None) < close_at


def lse_market_reason(d: date) -> str | None:
    """Operator-facing reason an LSE civil date is not a regular session, or
    ``None`` on a normal open day.

    ``"Weekend"`` on Sat/Sun, checked first -- E&W substitutes always land on a
    weekday, so a special date never falls on a weekend.
    """
    if d.weekday() >= 5:
        return "Weekend"
    return lse_market_specials(d.year).reasons.get(d)


def _assert_override_integrity() -> None:
    """Fail at import if an override cannot do what it claims.

    A suppression that matches no scheduled date is a typo, and its failure mode
    is silence -- the holiday stays closed and nothing says so. Checked over the
    verified horizon, which is the only range the overrides claim to describe.
    """
    overlap = set(_EXTRAORDINARY_CLOSURE_NAMES) & set(_RULE_SUPPRESSIONS)
    if overlap:
        raise AssertionError(f"a date is both added and suppressed: {sorted(overlap)}")
    derived: set[date] = set()
    for year in range(VERIFIED_FROM_YEAR, VERIFIED_THROUGH_YEAR + 1):
        derived |= set(_scheduled_closure_names(year))
    inert = set(_RULE_SUPPRESSIONS) - derived
    if inert:
        raise AssertionError(f"suppression matches no rule-derived date: {sorted(inert)}")


_assert_override_integrity()


__all__ = [
    "LSE_HALF_DAY_CLOSE",
    "LSE_SESSION_CLOSE",
    "LSE_SESSION_OPEN",
    "LSE_TZ",
    "RULE_SET_ID",
    "RULE_SET_VERSION",
    "VERIFIED_FROM_YEAR",
    "VERIFIED_THROUGH_YEAR",
    "LseCalendarHorizonError",
    "LseMarketStatus",
    "lse_market_reason",
    "lse_market_specials",
    "lse_market_status",
    "lse_session_is_open",
]
