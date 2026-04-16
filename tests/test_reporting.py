"""Tests for the reporting engine — weekly & monthly performance reports."""

from __future__ import annotations

import json
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


# ---------------------------------------------------------------------------
# Persistence layer tests
# ---------------------------------------------------------------------------


class TestPersistReportSnapshot:
    def test_persist_executes_upsert(self) -> None:
        """persist_report_snapshot should execute an INSERT with ON CONFLICT DO UPDATE."""
        from app.services.reporting import persist_report_snapshot

        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        report = {
            "report_type": "weekly",
            "period_start": "2026-04-06",
            "period_end": "2026-04-12",
            "pnl": {"realized_pnl": "100", "unrealized_pnl": "200"},
        }

        persist_report_snapshot(
            conn,
            report_type="weekly",
            period_start=date(2026, 4, 6),
            period_end=date(2026, 4, 12),
            snapshot=report,
        )

        cursor.execute.assert_called_once()
        sql = cursor.execute.call_args[0][0]
        params = cursor.execute.call_args[0][1]
        assert "ON CONFLICT" in sql
        assert params["report_type"] == "weekly"
        assert params["period_start"] == date(2026, 4, 6)


class TestLoadReportSnapshots:
    def test_load_returns_list(self) -> None:
        """load_report_snapshots should query by report_type."""
        from app.services.reporting import load_report_snapshots

        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchall.return_value = []

        result = load_report_snapshots(conn, report_type="weekly", limit=10)
        assert result == []
        cursor.execute.assert_called_once()


# ---------------------------------------------------------------------------
# Scheduler job registration tests
# ---------------------------------------------------------------------------


class TestReportJobsAvailability:
    """weekly_report and monthly_report used to be scheduled directly;
    since Phase 4 they are driven by the orchestrator full sync (via
    JOB_TO_LAYERS → weekly_reports / monthly_reports layers). They
    remain in _INVOKERS so POST /jobs/{name}/run continues to work."""

    def test_reports_available_via_orchestrator(self) -> None:
        from app.services.sync_orchestrator.registry import JOB_TO_LAYERS

        assert JOB_TO_LAYERS["weekly_report"] == ("weekly_reports",)
        assert JOB_TO_LAYERS["monthly_report"] == ("monthly_reports",)

    def test_reports_still_in_invokers(self) -> None:
        from app.jobs.runtime import _INVOKERS

        assert "weekly_report" in _INVOKERS
        assert "monthly_report" in _INVOKERS


# ---------------------------------------------------------------------------
# Reports API tests
# ---------------------------------------------------------------------------


class TestReportsAPI:
    def test_reports_router_exists(self) -> None:
        """The reports router should have the correct prefix."""
        from app.api.reports import router

        assert router.prefix == "/api/reports"

    def test_list_weekly_endpoint_exists(self) -> None:
        """GET /api/reports/weekly should be a registered route."""
        from app.api.reports import router

        paths = [r.path for r in router.routes if hasattr(r, "path")]  # type: ignore[union-attr]
        assert "/api/reports/weekly" in paths

    def test_list_monthly_endpoint_exists(self) -> None:
        """GET /api/reports/monthly should be a registered route."""
        from app.api.reports import router

        paths = [r.path for r in router.routes if hasattr(r, "path")]  # type: ignore[union-attr]
        assert "/api/reports/monthly" in paths

    def test_latest_endpoint_exists(self) -> None:
        """GET /api/reports/latest should be a registered route."""
        from app.api.reports import router

        paths = [r.path for r in router.routes if hasattr(r, "path")]  # type: ignore[union-attr]
        assert "/api/reports/latest" in paths


# ---------------------------------------------------------------------------
# Edge case tests
# ---------------------------------------------------------------------------


class TestWinRateEdgeCases:
    def test_no_closed_positions_returns_none(self) -> None:
        """Win rate should be None when no positions closed in the period."""
        from app.services.reporting import _win_rate_and_holding

        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchall.return_value = []

        result = _win_rate_and_holding(conn, date(2026, 4, 1), date(2026, 4, 30))
        assert result["total_closed"] == 0
        assert result["win_rate_pct"] is None
        assert result["avg_holding_days"] is None

    def test_all_winners(self) -> None:
        """100% win rate when all positions were profitable."""
        from app.services.reporting import _win_rate_and_holding

        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchall.return_value = [
            {"gross_return_pct": Decimal("0.10"), "hold_days": 30},
            {"gross_return_pct": Decimal("0.05"), "hold_days": 45},
        ]

        result = _win_rate_and_holding(conn, date(2026, 4, 1), date(2026, 4, 30))
        assert result["total_closed"] == 2
        assert result["win_rate_pct"] == "100.00"
        assert result["avg_holding_days"] == 37.5


class TestBottomPerformersEdge:
    def test_fewer_than_n_positions(self) -> None:
        """With fewer positions than N, bottom list should not duplicate top."""
        from app.services.reporting import _top_bottom_performers

        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchall.return_value = [
            {
                "instrument_id": 1,
                "symbol": "AAPL",
                "company_name": "Apple",
                "unrealized_pnl": Decimal("100"),
                "current_units": Decimal("5"),
                "avg_cost": Decimal("150"),
            },
        ]

        top, bottom = _top_bottom_performers(conn, n=3)
        assert len(top) == 1
        assert len(bottom) == 0


class TestDecHelper:
    def test_none_returns_none(self) -> None:
        from app.services.reporting import _dec

        assert _dec(None) is None

    def test_decimal_returns_string(self) -> None:
        from app.services.reporting import _dec

        assert _dec(Decimal("1.23")) == "1.23"


class TestJsonSerializability:
    """Codex-requested: verify reports can be serialized to JSON."""

    def test_weekly_report_is_json_serializable(self) -> None:
        """The weekly report dict must survive json.dumps without error."""
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

        # Must not raise
        serialized = json.dumps(report)
        assert isinstance(serialized, str)
        assert "weekly" in serialized

    def test_monthly_report_is_json_serializable(self) -> None:
        """The monthly report dict must survive json.dumps without error."""
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

        serialized = json.dumps(report)
        assert isinstance(serialized, str)
        assert "monthly" in serialized
