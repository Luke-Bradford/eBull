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

**Half-day (early-close) derivation + its one known limitation.** Early closes
are irregular, so we derive the common cases by rule and subtract any that are
actually full closures (closure always wins):
  * the Friday after Thanksgiving (always a half day),
  * Dec 24 when it is a weekday,
  * Jul 3 when it is a weekday and Jul 4 is also a weekday (i.e. Jul 4 is the
    real holiday and Jul 3 is the eve) — when Jul 4 is a Saturday, Jul 3 is the
    *observed* full closure instead, removed by the minus-closures step.
Known v1 omission: when Christmas (Dec 25) falls on a **Saturday**, NYSE
observes the closure on Fri Dec 24 and early-closes Thu Dec 23 — we do not model
that Dec-23 early close, so that rare day shades as full RTH. This fails **safe**
(a real session shown as regular hours, never a closed day shown as open) and is
refreshed by transcribing the published date if it ever matters. Re-verify the
half-day set annually against nyse.com.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

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


@dataclass(frozen=True)
class MarketYear:
    """The NYSE special days for one calendar year, as ``America/New_York``
    civil dates. ``full_closures`` = no trading; ``half_days`` = 13:00 ET
    early close."""

    year: int
    full_closures: frozenset[date]
    half_days: frozenset[date]


def _full_closures_for_year(year: int) -> frozenset[date]:
    """Observed NYSE full closures whose observed date lands in ``year``.

    The query window straddles the adjacent year boundaries so a New Year's
    Day that falls on a Saturday — observed on the prior Dec 31 — is
    attributed to the year its *observed* date lands in (mirrors
    ``sec_calendar._holidays_for_year``)."""
    stamps = _CALENDAR.holidays(start=date(year - 1, 12, 15), end=date(year + 1, 1, 15))
    return frozenset(ts.date() for ts in stamps if ts.year == year)


def _half_days_for_year(year: int, full_closures: frozenset[date]) -> frozenset[date]:
    """Rule-derived 13:00 ET early-close days, minus any that are full
    closures (closure wins). See module docstring for the rule set + the
    single known omission (Saturday-Christmas → Dec 23)."""
    candidates: set[date] = set()

    # Friday after Thanksgiving. Thanksgiving (4th Thursday of Nov) is always
    # a full closure; the early-close day is the Thursday + 1.
    for c in full_closures:
        if c.month == 11 and c.weekday() == 3:  # the Thursday Thanksgiving
            candidates.add(date(c.year, 11, c.day + 1))

    # Christmas Eve when a weekday.
    dec24 = date(year, 12, 24)
    if dec24.weekday() < 5:
        candidates.add(dec24)

    # Eve of Independence Day: Jul 3 when both Jul 3 and Jul 4 are weekdays.
    jul3, jul4 = date(year, 7, 3), date(year, 7, 4)
    if jul3.weekday() < 5 and jul4.weekday() < 5:
        candidates.add(jul3)

    return frozenset(c for c in candidates if c not in full_closures)


def us_market_specials(year: int) -> MarketYear:
    """NYSE full closures + half days for ``year`` (cached; immutable)."""
    cached = _CACHE.get(year)
    if cached is None:
        closures = _full_closures_for_year(year)
        cached = MarketYear(
            year=year,
            full_closures=closures,
            half_days=_half_days_for_year(year, closures),
        )
        _CACHE[year] = cached
    return cached
