"""Pure tests for #1754 market-status classification."""

from __future__ import annotations

from datetime import date

from app.api.calendar import _day_reason, _day_type
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


class TestDayReason:
    """`_day_reason` (#1766) — operator-facing reason per profile/day."""

    def test_us_holiday_name(self) -> None:
        assert _day_reason("us_equity", date(2026, 1, 1)) == "New Year's Day"
        assert _day_reason("us_equity_rth", date(2026, 12, 24)) == "Christmas Eve"
        assert _day_reason("us_equity", date(2026, 7, 3)) == "Independence Day"  # observed

    def test_us_plain_weekday_none(self) -> None:
        assert _day_reason("us_equity", date(2026, 6, 23)) is None  # Tuesday

    def test_us_weekend(self) -> None:
        assert _day_reason("us_equity", date(2026, 6, 27)) == "Weekend"  # Saturday

    def test_foreign_only_weekend(self) -> None:
        # Holidays not modelled — a weekday holiday gets no reason.
        assert _day_reason("foreign_equity", date(2026, 1, 1)) is None  # Thu, but unmodelled
        assert _day_reason("foreign_equity", date(2026, 6, 27)) == "Weekend"  # Sat

    def test_continuous_and_unknown_none(self) -> None:
        assert _day_reason("continuous", date(2026, 1, 1)) is None
        assert _day_reason("continuous", date(2026, 6, 27)) is None  # weekend too
        assert _day_reason("mystery_profile", date(2026, 1, 1)) is None
