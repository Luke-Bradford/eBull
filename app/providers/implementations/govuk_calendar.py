"""GOV.UK ``bank-holidays.json`` — strict parser for the England-and-Wales division.

#3616 slice 3b, spec ``docs/specs/metrics/2026-10-13-3616-slice3b-tax-fx-boe.md`` (Design 2, "Parser contracts").
GOV.UK content is under the Open Government Licence v3.0. No account or key is needed.
"""

from __future__ import annotations

import json
import re
from datetime import date
from typing import Final

URL: Final = "https://www.gov.uk/bank-holidays.json"
DIVISION: Final = "england-and-wales"
#: The 8 recurring England-and-Wales bank and public holidays. A plausibility floor only: agreement with the series
#: (spec steps 4.5 and 4.7) is what checks the calendar.
MIN_HOLIDAYS_PER_YEAR: Final = 8
_ISO_DATE: Final = re.compile(r"\d{4}-\d{2}-\d{2}")  # date.fromisoformat alone also accepts compact forms


class CalendarParseError(ValueError):
    """The body does not meet the parser contract."""


def parse_bank_holidays(body: bytes) -> dict[int, frozenset[date]]:
    """England-and-Wales holidays per year, or raise ``CalendarParseError``.

    The division must be present with unique ISO dates, and every year from the first listed to the last must hold at
    least ``MIN_HOLIDAYS_PER_YEAR`` dates.
    """
    try:
        division = json.loads(body)[DIVISION]
        raw_dates = [event["date"] for event in division["events"]]
        if not all(isinstance(d, str) and _ISO_DATE.fullmatch(d) for d in raw_dates):
            raise ValueError("a date is not YYYY-MM-DD")
        dates = [date.fromisoformat(d) for d in raw_dates]
    except (ValueError, KeyError, TypeError) as exc:
        raise CalendarParseError(f"calendar is not the documented shape: {exc}") from exc
    if division.get("division") != DIVISION or not dates or len(set(dates)) != len(dates):
        raise CalendarParseError("calendar division missing, empty or holding duplicate dates")
    by_year: dict[int, set[date]] = {}
    for day in dates:
        by_year.setdefault(day.year, set()).add(day)
    for year in range(min(by_year), max(by_year) + 1):
        if len(by_year.get(year, ())) < MIN_HOLIDAYS_PER_YEAR:
            raise CalendarParseError(f"calendar year {year} holds {len(by_year.get(year, ()))} dates")
    return {year: frozenset(days) for year, days in by_year.items()}


__all__ = ["DIVISION", "MIN_HOLIDAYS_PER_YEAR", "URL", "CalendarParseError", "parse_bank_holidays"]
