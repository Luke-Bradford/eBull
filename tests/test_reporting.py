"""Tests for the reporting engine — weekly & monthly performance reports."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.workers.scheduler import Cadence, compute_next_run


class TestCadenceMonthly:
    def test_valid_monthly_cadence(self) -> None:
        c = Cadence.monthly(day=1, hour=6, minute=0)
        assert c.kind == "monthly"
        assert c.day == 1
        assert c.hour == 6
        assert c.minute == 0

    def test_monthly_day_out_of_range_high(self) -> None:
        with pytest.raises(ValueError, match="monthly day must be 1..28"):
            Cadence.monthly(day=29, hour=6)

    def test_monthly_day_out_of_range_low(self) -> None:
        with pytest.raises(ValueError, match="monthly day must be 1..28"):
            Cadence.monthly(day=0, hour=6)

    def test_monthly_label(self) -> None:
        c = Cadence.monthly(day=15, hour=9, minute=30)
        assert c.label == "monthly on day 15 at 09:30 UTC"


class TestComputeNextRunMonthly:
    def test_same_month_future(self) -> None:
        """If the fire day hasn't passed yet this month, return this month."""
        now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=UTC)
        c = Cadence.monthly(day=15, hour=7, minute=0)
        result = compute_next_run(c, now)
        assert result == datetime(2026, 4, 15, 7, 0, 0, tzinfo=UTC)

    def test_same_month_past_advances_to_next(self) -> None:
        """If fire day already passed this month, advance to next month."""
        now = datetime(2026, 4, 20, 12, 0, 0, tzinfo=UTC)
        c = Cadence.monthly(day=1, hour=7, minute=0)
        result = compute_next_run(c, now)
        assert result == datetime(2026, 5, 1, 7, 0, 0, tzinfo=UTC)

    def test_december_wraps_to_january(self) -> None:
        """December fire that's already passed wraps to January next year."""
        now = datetime(2026, 12, 15, 12, 0, 0, tzinfo=UTC)
        c = Cadence.monthly(day=1, hour=7, minute=0)
        result = compute_next_run(c, now)
        assert result == datetime(2027, 1, 1, 7, 0, 0, tzinfo=UTC)

    def test_exact_fire_time_advances(self) -> None:
        """If now is exactly on fire time, next run is next month (strictly greater)."""
        now = datetime(2026, 4, 1, 7, 0, 0, tzinfo=UTC)
        c = Cadence.monthly(day=1, hour=7, minute=0)
        result = compute_next_run(c, now)
        assert result == datetime(2026, 5, 1, 7, 0, 0, tzinfo=UTC)

    def test_february_28(self) -> None:
        """Day 28 works in February (non-leap year)."""
        now = datetime(2027, 2, 1, 0, 0, 0, tzinfo=UTC)
        c = Cadence.monthly(day=28, hour=7, minute=0)
        result = compute_next_run(c, now)
        assert result == datetime(2027, 2, 28, 7, 0, 0, tzinfo=UTC)


class TestTriggerForMonthly:
    def test_monthly_trigger(self) -> None:
        from app.jobs.runtime import _trigger_for

        c = Cadence.monthly(day=1, hour=7, minute=0)
        trigger = _trigger_for(c)
        # CronTrigger fields — verify the trigger was created without error
        assert trigger is not None
