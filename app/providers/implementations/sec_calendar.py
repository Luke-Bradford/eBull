"""US federal holiday calendar for SEC EDGAR daily-index publishing.

SEC's Archives host serves **403 (not 404)** for daily-index files that
do not exist. EDGAR publishes a daily index only on US **federal**
business days — confirmed empirically (2026-06-13, app User-Agent):
Columbus Day (2025-10-13) and Veterans Day (2025-11-11) — both NYSE-open
but federally closed — return 403, while business days return 200. So
the correct "no-publish" gate for a 403 is the **federal** holiday
calendar, not an NYSE/market calendar.

Used by ``sec_edgar.SecFilingsProvider.fetch_master_index`` and
``sec_daily_index.read_daily_index`` to tolerate a 403 on a holiday
``target_date`` (return None / empty) the same way they already tolerate
a weekend, instead of raising and wedging the per-day watermark.

``pandas`` is a declared dependency (also Required-by edgartools); its
``USFederalHolidayCalendar`` already emits **observed** dates (a Saturday
holiday → prior Friday, a Sunday holiday → following Monday), which is
exactly what EDGAR honours.
"""

from __future__ import annotations

from datetime import date

from pandas.tseries.holiday import USFederalHolidayCalendar

_CALENDAR = USFederalHolidayCalendar()
_CACHE: dict[int, frozenset[date]] = {}


def _holidays_for_year(year: int) -> frozenset[date]:
    """Observed US federal holidays whose observed date falls in ``year``.

    The query window straddles the adjacent year boundaries so a New
    Year's Day that falls on a Saturday — observed on the prior Dec 31 —
    is attributed to the year its *observed* date lands in. Cached per
    year (immutable; the planner loop calls this up to 30×/run)."""
    cached = _CACHE.get(year)
    if cached is None:
        stamps = _CALENDAR.holidays(
            start=date(year - 1, 12, 15),
            end=date(year + 1, 1, 15),
        )
        cached = frozenset(ts.date() for ts in stamps if ts.year == year)
        _CACHE[year] = cached
    return cached


def is_us_federal_holiday(d: date) -> bool:
    """True iff ``d`` is an *observed* US federal holiday.

    A Sat/Sun nominal holiday is already covered by the callers' weekend
    branch upstream; this matches the weekday observed dates EDGAR does
    not publish on (incl. observed shifts for fixed-date holidays)."""
    return d in _holidays_for_year(d.year)
