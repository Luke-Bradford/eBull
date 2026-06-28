"""NYSE trading-calendar gate for intraday chart session shading (#609 Phase A).

The instrument-page intraday chart tints pre-market / after-hours / closed
bands. Doing that correctly for US equities needs the **NYSE** trading
calendar — full closures and 13:00 ET early-close ("half") days — which is
distinct from the US **federal** calendar in
``app.providers.implementations.sec_calendar``: NYSE closes on **Good Friday**
(not a federal holiday) and stays open on Columbus + Veterans Day (which are
federal). So this is a separate calendar, not a reuse.

Source rule: **NYSE published holidays + early-closings**
(https://www.nyse.com/markets/hours-calendars). Full closures are composed
from ``pandas.tseries.holiday`` primitives (``pandas`` is already a top-level
dependency — see ``pyproject.toml``; no new package, per the operator's
2026-06-27 decision to favour the in-house table over ``pandas_market_calendars``
and its six transitive packages). ``AbstractHolidayCalendar`` already emits
**observed** dates (a Saturday holiday → prior Friday, Sunday → following
Monday), which is exactly what NYSE honours.

Verified against NYSE's published calendars (2026-06-27): 2025 closures =
{Jan 1, Jan 20, Feb 17, Apr 18, May 26, Jun 19, Jul 4, Sep 1, Nov 27, Dec 25};
2026 = {Jan 1, Jan 19, Feb 16, Apr 3, May 25, Jun 19, Jul 3, Sep 7, Nov 26,
Dec 25}.

**Half-day (early-close) derivation.** Early closes are irregular, so we derive
the recurring cases by rule and subtract any that are actually full closures
(closure always wins):
  * the Friday after Thanksgiving (always a half day),
  * Dec 24 when it is a weekday and not itself the observed Christmas closure,
  * Jul 3 when it is a weekday and Jul 4 is also a weekday (i.e. Jul 4 is the
    real holiday and Jul 3 is the eve) — when Jul 4 is a Saturday, Jul 3 is the
    *observed* full closure instead, removed by the minus-closures step.
Anchored to verified years: 2021 (Sat-Christmas → observed Fri Dec 24 closed,
no Dec early close — matches NYSE) and 2024/2025/2026 (Dec 24 half day).
Re-verify the half-day set annually against nyse.com.

**Extraordinary closures.** Beyond the scheduled holidays above, NYSE has
closed on a handful of ad-hoc days (9/11, Hurricane Sandy, national days of
mourning). These are transcribed in ``_EXTRAORDINARY_CLOSURES`` from the NYSE
historical record. The intraday chart (the sole Phase A consumer) only requests
recent years, but the endpoint serves the full 2000-2100 range, so we include
them for correctness. Any *future* ad-hoc closure NYSE declares must be added
here; until then such a day fails **safe** (renders as regular hours, never a
closed day shown as open).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from types import MappingProxyType
from typing import Literal, cast

from pandas import Series, Timestamp
from pandas.tseries.holiday import (
    AbstractHolidayCalendar,
    GoodFriday,
    Holiday,
    USLaborDay,
    USMartinLutherKingJr,
    USMemorialDay,
    USPresidentsDay,
    USThanksgivingDay,
    nearest_workday,
)


class _NyseHolidayCalendar(AbstractHolidayCalendar):
    """Full-closure rules for the NYSE. ``Juneteenth`` carries a
    ``start_date`` because NYSE first observed it in 2022 — without the
    bound a ``nearest_workday`` rule wrongly closes 2021-06-18."""

    rules = [
        Holiday("New Year's Day", month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday("Juneteenth", month=6, day=19, start_date=date(2022, 1, 1), observance=nearest_workday),
        Holiday("Independence Day", month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday("Christmas", month=12, day=25, observance=nearest_workday),
    ]


_CALENDAR = _NyseHolidayCalendar()
_CACHE: dict[int, MarketYear] = {}

# Ad-hoc NYSE full closures not produced by the scheduled-holiday rules,
# transcribed from the NYSE historical record. Source: NYSE holidays &
# trading-hours history (https://www.nyse.com/markets/hours-calendars) +
# press releases for national days of mourning. Each date carries its
# operator-facing reason; the closure SET is derived from this map so the two
# can never drift (the reasons map is total over the closures by construction).
_EXTRAORDINARY_CLOSURE_NAMES: dict[date, str] = {
    date(2001, 9, 11): "9/11 attacks",  # markets closed Sep 11-14
    date(2001, 9, 12): "9/11 attacks",
    date(2001, 9, 13): "9/11 attacks",
    date(2001, 9, 14): "9/11 attacks",
    date(2004, 6, 11): "Day of mourning — President Reagan",
    date(2007, 1, 2): "Day of mourning — President Ford",
    date(2012, 10, 29): "Hurricane Sandy",  # closed Oct 29-30
    date(2012, 10, 30): "Hurricane Sandy",
    date(2018, 12, 5): "Day of mourning — President G.H.W. Bush",
    date(2025, 1, 9): "Day of mourning — President Carter",
}
_EXTRAORDINARY_CLOSURES: frozenset[date] = frozenset(_EXTRAORDINARY_CLOSURE_NAMES)


@dataclass(frozen=True)
class MarketYear:
    """The NYSE special days for one calendar year, as ``America/New_York``
    civil dates. ``full_closures`` = no trading; ``half_days`` = 13:00 ET
    early close; ``reasons`` = each special date → its operator-facing name
    (holiday / early-close occasion). ``reasons`` is read-only so the cached
    instance stays effectively immutable."""

    year: int
    full_closures: frozenset[date]
    half_days: frozenset[date]
    reasons: Mapping[date, str]


def _scheduled_closure_names(year: int) -> dict[date, str]:
    """Observed scheduled NYSE closures landing in ``year`` → holiday name.

    The query window straddles the adjacent year boundaries so a New Year's
    Day that falls on a Saturday — observed on the prior Dec 31 — is
    attributed to the year its *observed* date lands in (mirrors
    ``sec_calendar._holidays_for_year``). ``return_name=True`` reuses the same
    ``pandas`` rules that produce the dates, so the names cannot drift from the
    closure set."""
    # ``return_name=True`` makes ``holidays`` return a name-valued Series keyed
    # by the observed dates; the pandas stub types it as ``DatetimeIndex`` (it
    # does not model the overload), so cast for the ``.items()`` iteration.
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
    """Rule-derived 13:00 ET early-close days → name, minus any that are full
    closures (closure wins). See module docstring for the rule set + the
    single known omission (Saturday-Christmas → Dec 23)."""
    candidates: dict[date, str] = {}

    # Friday after Thanksgiving. Thanksgiving (4th Thursday of Nov) is always
    # a full closure; the early-close day is the Thursday + 1.
    for c in full_closures:
        if c.month == 11 and c.weekday() == 3:  # the Thursday Thanksgiving
            candidates[date(c.year, 11, c.day + 1)] = "Friday after Thanksgiving"

    # Christmas Eve when a weekday.
    dec24 = date(year, 12, 24)
    if dec24.weekday() < 5:
        candidates[dec24] = "Christmas Eve"

    # Eve of Independence Day: Jul 3 when both Jul 3 and Jul 4 are weekdays.
    jul3, jul4 = date(year, 7, 3), date(year, 7, 4)
    if jul3.weekday() < 5 and jul4.weekday() < 5:
        candidates[jul3] = "Independence Day eve"

    return {d: name for d, name in candidates.items() if d not in full_closures}


def us_market_specials(year: int) -> MarketYear:
    """NYSE full closures + half days for ``year`` (cached; immutable)."""
    cached = _CACHE.get(year)
    if cached is None:
        scheduled = _scheduled_closure_names(year)
        extraordinary = {d: name for d, name in _EXTRAORDINARY_CLOSURE_NAMES.items() if d.year == year}
        closures = frozenset(scheduled) | frozenset(extraordinary)
        half_day_names = _half_day_names_for_year(year, closures)
        # Closures and half days are disjoint (closure-wins is applied above),
        # so the merged reasons map has no key collisions.
        reasons = {**scheduled, **extraordinary, **half_day_names}
        cached = MarketYear(
            year=year,
            full_closures=closures,
            half_days=frozenset(half_day_names),
            reasons=MappingProxyType(reasons),
        )
        _CACHE[year] = cached
    return cached


UsMarketStatus = Literal["open", "half_day", "closed"]


def us_market_status(d: date) -> UsMarketStatus:
    """NYSE trading status for an ``America/New_York`` civil date (#1754).

    ``closed`` on weekends and full closures; ``half_day`` on a 13:00-ET early
    close; ``open`` otherwise. The argument is a NY-local date — the caller maps
    "today"/the week to NY-local civil dates first (the calendar is keyed that
    way, like ``us_market_specials``)."""
    specials = us_market_specials(d.year)
    if d.weekday() >= 5 or d in specials.full_closures:
        return "closed"
    if d in specials.half_days:
        return "half_day"
    return "open"


def us_market_reason(d: date) -> str | None:
    """Operator-facing reason a NYSE civil date is not a regular session, or
    ``None`` for a normal open day.

    ``"Weekend"`` on Sat/Sun (checked first, matching ``us_market_status``'s
    weekend-or-closure precedence — observed NYSE closures are always shifted
    onto weekdays, so a special date never lands on a weekend). Otherwise the
    holiday / early-close name from the year's ``reasons`` map; ``None`` on a
    plain trading weekday."""
    if d.weekday() >= 5:
        return "Weekend"
    return us_market_specials(d.year).reasons.get(d)
