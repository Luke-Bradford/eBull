"""Tests for the SEC EDGAR federal-holiday gate (#1612).

EDGAR publishes a daily index only on US *federal* business days; the
providers tolerate a 403 on such a date instead of raising. The set must
include Columbus + Veterans Day (NYSE-open but federally closed) and
honour observed-day shifts.
"""

from __future__ import annotations

from datetime import date

import pytest

from app.providers.implementations.sec_calendar import is_us_federal_holiday

# Observed federal holidays for 2025 + 2026 (verified against
# pandas USFederalHolidayCalendar and SEC's live 403 behaviour).
_HOLIDAYS = [
    date(2025, 1, 1),  # New Year's Day
    date(2025, 1, 20),  # MLK
    date(2025, 2, 17),  # Washington's Birthday
    date(2025, 5, 26),  # Memorial Day
    date(2025, 6, 19),  # Juneteenth
    date(2025, 7, 4),  # Independence Day
    date(2025, 9, 1),  # Labor Day
    date(2025, 10, 13),  # Columbus Day (NYSE open)
    date(2025, 11, 11),  # Veterans Day (NYSE open)
    date(2025, 11, 27),  # Thanksgiving
    date(2025, 12, 25),  # Christmas
    date(2026, 5, 25),  # Memorial Day — the #1612 wedge date
    date(2026, 6, 19),  # Juneteenth
    date(2026, 7, 3),  # Independence Day observed (Jul 4 is Sat)
    date(2026, 11, 11),  # Veterans Day
]

# Adjacent business days / a nominal-Saturday holiday that must NOT match.
_NON_HOLIDAYS = [
    date(2026, 5, 26),  # Tue after Memorial Day — a business day
    date(2025, 10, 14),  # Tue after Columbus Day
    date(2026, 6, 1),  # ordinary Monday
    date(2026, 4, 20),  # ordinary Monday, no April federal holiday
    date(2026, 7, 4),  # nominal Independence Day on a Saturday — observed
    #                    date is 07-03, the weekend itself is not matched
    #                    here (callers' weekend branch handles Sat/Sun)
]


@pytest.mark.parametrize("d", _HOLIDAYS)
def test_federal_holidays_match(d: date) -> None:
    assert is_us_federal_holiday(d) is True


@pytest.mark.parametrize("d", _NON_HOLIDAYS)
def test_business_days_do_not_match(d: date) -> None:
    assert is_us_federal_holiday(d) is False


def test_cross_year_observed_new_year() -> None:
    # New Year's Day 2022-01-01 fell on a Saturday → observed on the
    # prior Friday 2021-12-31, which is in a different calendar year than
    # the nominal holiday. The per-year cache window must still catch it.
    assert is_us_federal_holiday(date(2021, 12, 31)) is True
