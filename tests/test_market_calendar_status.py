"""Pure tests for #1754 market-status classification."""

from __future__ import annotations

from datetime import date

from app.api.calendar import _day_type
from app.services.market_calendar import us_market_status


class TestUsMarketStatus:
    def test_full_closures_are_closed(self) -> None:
        assert us_market_status(date(2026, 1, 1)) == "closed"  # New Year's Day
        assert us_market_status(date(2026, 12, 25)) == "closed"  # Christmas (Fri)
        assert us_market_status(date(2026, 7, 3)) == "closed"  # Jul 4 Sat → observed Fri closure

    def test_half_days(self) -> None:
        assert us_market_status(date(2026, 11, 27)) == "half_day"  # Fri after Thanksgiving
        assert us_market_status(date(2026, 12, 24)) == "half_day"  # Christmas Eve (Thu)

    def test_weekends_closed(self) -> None:
        assert us_market_status(date(2026, 6, 27)) == "closed"  # Saturday
        assert us_market_status(date(2026, 6, 28)) == "closed"  # Sunday

    def test_normal_weekday_open(self) -> None:
        assert us_market_status(date(2026, 6, 23)) == "open"  # plain Tuesday


class TestDayType:
    def test_us_profiles_delegate_to_nyse(self) -> None:
        assert _day_type("us_equity", date(2026, 1, 1)) == "closed"
        assert _day_type("us_equity_rth", date(2026, 6, 23)) == "open"
        assert _day_type("us_equity", date(2026, 12, 24)) == "half_day"

    def test_foreign_weekday_open_weekend_closed(self) -> None:
        # Foreign holidays NOT modelled — even Jan 1 reads "open" on a weekday.
        assert _day_type("foreign_equity", date(2026, 1, 1)) == "open"  # Thu
        assert _day_type("foreign_equity", date(2026, 6, 27)) == "closed"  # Sat

    def test_continuous_always_open(self) -> None:
        # Matches the shipped #609 classifySession (continuous = always trading).
        assert _day_type("continuous", date(2026, 6, 23)) == "open"
        assert _day_type("continuous", date(2026, 6, 27)) == "open"  # weekend too
