"""Pure tests for #1754 market-status classification."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from app.api.calendar import _day_reason, _day_type
from app.services.market_calendar import latest_completed_us_session, us_market_status


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


class TestLatestCompletedUsSession:
    def test_0300_utc_full_sync_names_the_prior_session(self) -> None:
        assert latest_completed_us_session(datetime(2026, 8, 11, 3, tzinfo=UTC)) == date(2026, 8, 10)

    def test_regular_session_becomes_complete_at_official_close(self) -> None:
        assert latest_completed_us_session(datetime(2026, 8, 11, 19, 59, tzinfo=UTC)) == date(2026, 8, 10)
        assert latest_completed_us_session(datetime(2026, 8, 11, 20, 0, tzinfo=UTC)) == date(2026, 8, 11)

    def test_half_day_uses_1300_new_york_close(self) -> None:
        assert latest_completed_us_session(datetime(2026, 11, 27, 17, 59, tzinfo=UTC)) == date(2026, 11, 25)
        assert latest_completed_us_session(datetime(2026, 11, 27, 18, 0, tzinfo=UTC)) == date(2026, 11, 27)

    def test_naive_datetime_is_refused(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            latest_completed_us_session(datetime(2026, 8, 11, 20, 0))


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
