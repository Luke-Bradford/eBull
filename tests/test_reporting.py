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


# ---------------------------------------------------------------------------
# Weekly report generator tests
# ---------------------------------------------------------------------------

from datetime import date  # noqa: E402
from decimal import Decimal  # noqa: E402
from unittest.mock import MagicMock, patch  # noqa: E402

from app.services.reporting import generate_weekly_report  # noqa: E402

_REPORTING = "app.services.reporting"


class TestGenerateWeeklyReport:
    def test_returns_weekly_report_structure(self) -> None:
        """Weekly report should contain all expected sections."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchall.return_value = []
        cursor.fetchone.return_value = {"realized": Decimal("0"), "unrealized": Decimal("0")}

        with patch(f"{_REPORTING}.compute_budget_state") as mock_budget:
            mock_budget.return_value = MagicMock(
                cash_balance=Decimal("10000"),
                deployed_capital=Decimal("5000"),
                estimated_tax_usd=Decimal("200"),
                available_for_deployment=Decimal("4800"),
            )
            report = generate_weekly_report(
                conn,
                period_start=date(2026, 4, 6),
                period_end=date(2026, 4, 12),
            )

        assert report["report_type"] == "weekly"
        assert report["period_start"] == "2026-04-06"
        assert report["period_end"] == "2026-04-12"
        assert "pnl" in report
        assert report["pnl"]["note"] == "current-state snapshot, not period delta"
        assert "top_performers" in report
        assert "bottom_performers" in report
        assert "positions_opened" in report
        assert "positions_closed" in report
        assert "upcoming_earnings" in report
        assert "score_changes" in report
        assert "budget" in report


# ---------------------------------------------------------------------------
# Monthly report generator tests
# ---------------------------------------------------------------------------

from app.services.reporting import generate_monthly_report  # noqa: E402


class TestGenerateMonthlyReport:
    def test_returns_monthly_report_structure(self) -> None:
        """Monthly report should contain all expected sections."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchall.return_value = []
        cursor.fetchone.return_value = {
            "realized": Decimal("0"),
            "unrealized": Decimal("0"),
            "positions_attributed": 0,
            "avg_gross": None,
            "avg_market": None,
            "avg_alpha": None,
        }

        with patch(f"{_REPORTING}.compute_budget_state") as mock_budget:
            mock_budget.return_value = MagicMock(
                cash_balance=Decimal("10000"),
                deployed_capital=Decimal("5000"),
                estimated_tax_usd=Decimal("200"),
                estimated_tax_gbp=Decimal("160"),
                available_for_deployment=Decimal("4800"),
                tax_year="2025/26",
            )
            report = generate_monthly_report(
                conn,
                period_start=date(2026, 3, 1),
                period_end=date(2026, 3, 31),
            )

        assert report["report_type"] == "monthly"
        assert report["period_start"] == "2026-03-01"
        assert report["period_end"] == "2026-03-31"
        assert "position_pnl" in report
        assert "win_rate" in report
        assert "avg_holding_days" in report
        assert "best_trade" in report
        assert "worst_trade" in report
        assert "attribution_summary" in report
        assert "thesis_accuracy" in report
        assert "tax_provision" in report
