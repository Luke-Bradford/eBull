"""Tests for the reporting engine — weekly & monthly performance reports."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

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


def _fake_valuation() -> Any:
    """Minimal PortfolioValuation for builder tests — the heavy
    market-data/FX path is exercised separately; builder tests patch
    `compute_portfolio_valuation` at the boundary (#1596)."""
    from app.services.valuation import PortfolioValuation

    return PortfolioValuation(
        display_currency="USD",
        holdings=(),
        total_market=0.0,
        cash_balance=10000.0,
        mirror_equity=0.0,
        total_aum=10000.0,
        raw_rows=(),
        rates={},
        rates_meta={},
        mirror_breakdowns=(),
    )


_NULL_BENCHMARK = {
    "symbol": "SPX500",
    "label": "S&P 500 (price index)",
    "close_start": None,
    "close_end": None,
    "return_pct": None,
}


class TestGenerateWeeklyReport:
    def test_returns_weekly_report_structure(self) -> None:
        """Weekly report should contain all expected sections."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchall.return_value = []
        cursor.fetchone.return_value = {"realized": Decimal("0"), "unrealized": Decimal("0")}

        with (
            patch(f"{_REPORTING}.compute_budget_state") as mock_budget,
            patch(f"{_REPORTING}.compute_portfolio_valuation", return_value=_fake_valuation()),
            patch(f"{_REPORTING}._benchmark_closes", return_value=dict(_NULL_BENCHMARK)),
        ):
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
        assert report["schema_version"] == 2
        assert "pnl" in report
        assert report["pnl"]["note"] == "current-state snapshot, not period delta"
        assert "top_performers" in report
        assert "bottom_performers" in report
        assert "positions_opened" in report
        assert "positions_closed" in report
        assert "upcoming_earnings" in report
        assert "score_changes" in report
        assert "budget" in report
        # v2 sections (#1596)
        assert report["cover"]["closing_value"] == "10000.000000"
        assert report["cover"]["display_currency"] == "USD"
        # First v2 snapshot: no prior → no opening value → no return.
        assert report["cover"]["opening_value"] is None
        assert report["cover"]["period_return"] is None
        assert report["performance"]["portfolio_value"] == "10000.000000"
        assert report["performance"]["fx_mode"] == "generation_date"
        assert report["holdings"] == []


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
            "avg_sector": None,
            "avg_alpha": None,
            "avg_timing": None,
            "avg_cost_drag": None,
            "fill_count": 0,
            "fees_total": Decimal("0"),
        }

        with (
            patch(f"{_REPORTING}.compute_budget_state") as mock_budget,
            patch(f"{_REPORTING}.compute_portfolio_valuation", return_value=_fake_valuation()),
            patch(f"{_REPORTING}._benchmark_closes", return_value=dict(_NULL_BENCHMARK)),
        ):
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
        assert report["schema_version"] == 2
        assert "position_pnl" in report
        assert "win_rate" in report
        assert "avg_holding_days" in report
        assert "best_trade" in report
        assert "worst_trade" in report
        assert "attribution_summary" in report
        assert "thesis_accuracy" in report
        assert "tax_provision" in report
        # v2 sections (#1596)
        assert "cover" in report
        assert "performance" in report
        assert "holdings" in report
        assert "rolling_returns" in report
        assert set(report["rolling_returns"].keys()) == {"1m", "3m", "6m", "1y", "si"}
        assert report["income"]["items"] == []
        assert report["costs"]["fill_count"] == 0
        assert report["risk"]["insufficient_history"] is True
        assert report["thesis_summary"]["total"] == 0
        # score_changes was weekly-only pre-#1596 (committee finding).
        assert "score_changes" in report
        assert report["trade_stats"]["payoff_ratio"] is None


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
        """The reports router should have the correct prefix.

        The Vite dev proxy strips ``/api`` before forwarding to the
        backend, so the FastAPI router prefix must NOT include ``/api``
        — otherwise browser calls to ``/api/reports/...`` resolve to
        ``/reports/...`` on the backend and 404. Keep this prefix
        consistent with other routers (filings, news, instruments).
        """
        from app.api.reports import router

        assert router.prefix == "/reports"

    def test_list_weekly_endpoint_exists(self) -> None:
        from app.api.reports import router

        paths = [r.path for r in router.routes if hasattr(r, "path")]  # type: ignore[union-attr]
        assert "/reports/weekly" in paths

    def test_list_monthly_endpoint_exists(self) -> None:
        from app.api.reports import router

        paths = [r.path for r in router.routes if hasattr(r, "path")]  # type: ignore[union-attr]
        assert "/reports/monthly" in paths

    def test_latest_endpoint_exists(self) -> None:
        from app.api.reports import router

        paths = [r.path for r in router.routes if hasattr(r, "path")]  # type: ignore[union-attr]
        assert "/reports/latest" in paths


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

        with (
            patch(f"{_REPORTING}.compute_budget_state") as mock_budget,
            patch(f"{_REPORTING}.compute_portfolio_valuation", return_value=_fake_valuation()),
            patch(f"{_REPORTING}._benchmark_closes", return_value=dict(_NULL_BENCHMARK)),
        ):
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
            "avg_sector": None,
            "avg_alpha": None,
            "avg_timing": None,
            "avg_cost_drag": None,
            "fill_count": 0,
            "fees_total": Decimal("0"),
        }

        with (
            patch(f"{_REPORTING}.compute_budget_state") as mock_budget,
            patch(f"{_REPORTING}.compute_portfolio_valuation", return_value=_fake_valuation()),
            patch(f"{_REPORTING}._benchmark_closes", return_value=dict(_NULL_BENCHMARK)),
        ):
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


# ---------------------------------------------------------------------------
# Contributors computation (Slice 4 of per-stock research page spec)
# ---------------------------------------------------------------------------

from app.services.reporting import _compute_contributors  # noqa: E402


class TestComputeContributors:
    def test_no_prior_snapshot_returns_empty_lists(self) -> None:
        """First snapshot or backfilled historical (no `positions`) must
        degrade gracefully — empty contributors + drags, not None."""
        current = [
            {
                "instrument_id": 1,
                "symbol": "AAPL",
                "unrealized_pnl": "100",
                "cost_basis": "1000",
            }
        ]
        result = _compute_contributors(current, None)
        assert result == {"contributors": [], "drags": []}

    def test_computes_positive_and_negative_deltas(self) -> None:
        prior = [
            {
                "instrument_id": 1,
                "symbol": "AAPL",
                "unrealized_pnl": "50",
                "cost_basis": "1000",
            },
            {
                "instrument_id": 2,
                "symbol": "MSFT",
                "unrealized_pnl": "80",
                "cost_basis": "2000",
            },
        ]
        current = [
            {
                "instrument_id": 1,
                "symbol": "AAPL",
                "unrealized_pnl": "150",  # +100 gainer
                "cost_basis": "1000",
            },
            {
                "instrument_id": 2,
                "symbol": "MSFT",
                "unrealized_pnl": "20",  # -60 drag
                "cost_basis": "2000",
            },
        ]
        result = _compute_contributors(current, prior)
        assert len(result["contributors"]) == 1
        assert result["contributors"][0]["symbol"] == "AAPL"
        assert result["contributors"][0]["pnl_delta"] == "100"
        # pct = 100 / 1000 = 0.1
        assert result["contributors"][0]["pnl_pct"] == "0.1"

        assert len(result["drags"]) == 1
        assert result["drags"][0]["symbol"] == "MSFT"
        assert result["drags"][0]["pnl_delta"] == "-60"

    def test_new_position_with_zero_prior_cost_gets_null_pct(self) -> None:
        """Fresh position (not in prior snapshot) surfaces with full
        current P&L as delta but null pct — prevents misleading '∞%'."""
        current = [
            {
                "instrument_id": 99,
                "symbol": "NEW",
                "unrealized_pnl": "50",
                "cost_basis": "500",
            }
        ]
        prior: list[dict[str, Any]] = []
        result = _compute_contributors(current, prior)
        assert result["contributors"][0]["symbol"] == "NEW"
        assert result["contributors"][0]["pnl_delta"] == "50"
        assert result["contributors"][0]["pnl_pct"] is None

    def test_unchanged_position_not_in_either_list(self) -> None:
        """Zero delta means the position didn't contribute — omit."""
        prior = [
            {
                "instrument_id": 1,
                "symbol": "FLAT",
                "unrealized_pnl": "42",
                "cost_basis": "100",
            }
        ]
        current = [
            {
                "instrument_id": 1,
                "symbol": "FLAT",
                "unrealized_pnl": "42",
                "cost_basis": "100",
            }
        ]
        result = _compute_contributors(current, prior)
        assert result == {"contributors": [], "drags": []}

    def test_legacy_prior_without_positions_key_degrades_to_empty(self) -> None:
        """Pre-feature snapshots (backfilled without the `positions`
        key) must NOT be interpreted as 'prior had zero positions' —
        that would label every current holding as a fresh contributor.
        Passing None signals 'no comparison possible' cleanly."""
        current = [
            {
                "instrument_id": 1,
                "symbol": "AAPL",
                "unrealized_pnl": "100",
                "cost_basis": "1000",
            }
        ]
        result = _compute_contributors(current, None)
        assert result == {"contributors": [], "drags": []}

    def test_top_n_caps_each_list(self) -> None:
        """Only top_n contributors + top_n drags surface — keeps the
        UI list short and the operator focused on biggest movers.

        Fixture uses i=1..10 so no zero-delta row silently drops
        (i=0 would produce pnl=0 and be omitted, giving fragile
        "passes by accident" coverage — Codex round-2 note).
        Even i → positive delta (+i*10); odd i → negative (-i*10).
        Positives: [+20, +40, +60, +80, +100] — top 3 = S10, S8, S6.
        Negatives: [-10, -30, -50, -70, -90] — top 3 = S9, S7, S5
        (most-negative first).
        """
        prior = [
            {
                "instrument_id": i,
                "symbol": f"S{i}",
                "unrealized_pnl": "0",
                "cost_basis": "100",
            }
            for i in range(1, 11)
        ]
        current = [
            {
                "instrument_id": i,
                "symbol": f"S{i}",
                "unrealized_pnl": str((i if i % 2 == 0 else -i) * 10),
                "cost_basis": "100",
            }
            for i in range(1, 11)
        ]
        result = _compute_contributors(current, prior, top_n=3)
        assert len(result["contributors"]) == 3
        assert len(result["drags"]) == 3
        # Contributors sorted descending by delta — biggest gainer first.
        assert [r["symbol"] for r in result["contributors"]] == ["S10", "S8", "S6"]
        # Drags sorted ascending by delta — most-negative first.
        assert [r["symbol"] for r in result["drags"]] == ["S9", "S7", "S5"]


# ---------------------------------------------------------------------------
# v2 sections (#1596 — spec docs/proposals/ui/2026-06-12-report-ia.md)
# ---------------------------------------------------------------------------

from app.services.reporting import (  # noqa: E402
    _chain_link,
    _holdings_section,
    _modified_dietz,
    _risk_section,
    _thesis_summary,
)
from app.services.valuation import HoldingValuation, PortfolioValuation  # noqa: E402


def _holding(
    *,
    instrument_id: int = 1,
    symbol: str = "AAPL",
    sector: str | None = "Technology",
    cost_basis: float = 1000.0,
    market_value: float = 1100.0,
    units: float = 10.0,
) -> HoldingValuation:
    return HoldingValuation(
        instrument_id=instrument_id,
        symbol=symbol,
        company_name=f"{symbol} Inc",
        sector=sector,
        native_currency="USD",
        open_date=date(2026, 1, 5),
        source="ebull",
        updated_at=datetime(2026, 6, 1, tzinfo=UTC),
        avg_cost=cost_basis / units if units else None,
        current_units=units,
        cost_basis=cost_basis,
        current_price=market_value / units if units else None,
        market_value=market_value,
        unrealized_pnl=market_value - cost_basis,
        valuation_source="quote",
    )


def _valuation_with(holdings: tuple[HoldingValuation, ...], *, cash: float = 500.0) -> PortfolioValuation:
    total_market = sum(h.market_value for h in holdings)
    return PortfolioValuation(
        display_currency="USD",
        holdings=holdings,
        total_market=total_market,
        cash_balance=cash,
        mirror_equity=0.0,
        total_aum=total_market + cash,
        raw_rows=(),
        rates={},
        rates_meta={},
        mirror_breakdowns=(),
    )


class TestModifiedDietz:
    """Flow-adjusted period return (spec §3.4). The business boundary:
    a deposit must NEVER print as performance."""

    START = date(2026, 6, 1)
    END = date(2026, 6, 30)

    def test_no_flows_degenerates_to_simple_ratio(self) -> None:
        r = _modified_dietz(Decimal("1000"), Decimal("1100"), [], self.START, self.END)
        assert r == Decimal("0.1")

    def test_deposit_is_not_performance(self) -> None:
        """1000 → 2000 purely via a 1000 injection = 0% return, not 100%."""
        flows = [(date(2026, 6, 1), Decimal("1000"))]
        r = _modified_dietz(Decimal("1000"), Decimal("2000"), flows, self.START, self.END)
        assert r == Decimal("0")

    def test_withdrawal_is_not_a_loss(self) -> None:
        """1000 → 500 purely via a 500 withdrawal = 0% return, not −50%."""
        flows = [(date(2026, 6, 30), Decimal("-500"))]
        r = _modified_dietz(Decimal("1000"), Decimal("500"), flows, self.START, self.END)
        assert r == Decimal("0")

    def test_mid_period_flow_is_time_weighted(self) -> None:
        """A flow landing mid-period only counts for the remaining
        fraction of the period in the denominator."""
        # 30-day June: flow on the 16th → 15 days remain → w = 0.5.
        flows = [(date(2026, 6, 16), Decimal("1000"))]
        r = _modified_dietz(Decimal("1000"), Decimal("2100"), flows, self.START, self.END)
        # (2100 − 1000 − 1000) / (1000 + 1000×0.5) = 100 / 1500
        assert r == Decimal("100") / Decimal("1500")

    def test_none_opening_value_returns_none(self) -> None:
        assert _modified_dietz(None, Decimal("1000"), [], self.START, self.END) is None

    def test_non_positive_denominator_returns_none(self) -> None:
        """Account funded entirely mid-period: opening 0 → denominator
        can be ≤ 0 → a return figure would be meaningless."""
        flows = [(date(2026, 6, 30), Decimal("-2000"))]
        r = _modified_dietz(Decimal("0"), Decimal("100"), flows, self.START, self.END)
        assert r is None


class TestChainLink:
    def test_empty_returns_none(self) -> None:
        assert _chain_link([]) is None

    def test_single_return_round_trips(self) -> None:
        assert _chain_link([Decimal("0.1")]) == Decimal("0.1")

    def test_geometric_not_additive(self) -> None:
        # (1.1 × 0.9) − 1 = −0.01, NOT 0.
        r = _chain_link([Decimal("0.1"), Decimal("-0.1")])
        assert r == Decimal("-0.01")


class TestThesisSummary:
    def test_empty_rows(self) -> None:
        s = _thesis_summary([])
        assert s["total"] == 0
        assert s["buy"]["hit_rate_pct"] is None

    def test_buckets_and_not_evaluable(self) -> None:
        rows = [
            {"stance": "buy", "target_hit": "bull"},
            {"stance": "buy", "target_hit": "base"},
            {"stance": "buy", "target_hit": "between_bear_and_base"},
            {"stance": "buy", "target_hit": None},  # not yet evaluable
            {"stance": "avoid", "target_hit": "bear"},
        ]
        s = _thesis_summary(rows)
        assert s["total"] == 5
        assert s["evaluated"] == 4
        assert s["not_evaluable"] == 1
        assert s["hits"] == 2
        assert s["misses"] == 2
        assert s["buy"] == {"n": 3, "hits": 2, "hit_rate_pct": "66.67"}
        assert s["avoid"]["n"] == 1


class TestRiskSection:
    def test_insufficient_history_below_min_observations(self) -> None:
        val = _valuation_with((_holding(),))
        risk = _risk_section(val, [], Decimal("0.05"), observation_label="test")
        assert risk["insufficient_history"] is True
        assert risk["volatility"] is None
        assert risk["max_drawdown"] is None
        assert risk["observations"] == 1

    def test_concentration_and_sector_always_computable(self) -> None:
        holdings = (
            _holding(instrument_id=1, symbol="AAPL", market_value=600.0, sector="Technology"),
            _holding(instrument_id=2, symbol="JPM", market_value=300.0, sector="Financials"),
            _holding(instrument_id=3, symbol="XOM", market_value=100.0, sector=None),
        )
        val = _valuation_with(holdings, cash=0.0)
        risk = _risk_section(val, [], None, observation_label="test")
        assert risk["holding_count"] == 3
        # 3 holdings → top-5 = everything = 100%.
        assert Decimal(risk["concentration_top5_pct"]) == Decimal("1")
        assert Decimal(risk["sector_exposure"]["Technology"]) == Decimal("0.6")
        assert "Unknown" in risk["sector_exposure"]

    def test_drawdown_and_volatility_over_full_chain(self) -> None:
        chain = [
            {"period_start": date(2026, 1, 1), "period_return": Decimal(v), "display_currency": "USD"}
            for v in ("0.10", "-0.20", "0.05", "0.05", "0.05")
        ]
        val = _valuation_with((_holding(),))
        risk = _risk_section(val, chain, Decimal("0.05"), observation_label="test")
        assert risk["observations"] == 6
        assert risk["insufficient_history"] is False
        # Max drawdown: peak after +10% = 1.1, trough after −20% = 0.88
        # → 0.88/1.1 − 1 = −0.2.
        assert Decimal(risk["max_drawdown"]) == Decimal("-0.2")
        assert risk["volatility"] is not None

    def test_chain_rows_in_other_display_currency_excluded(self) -> None:
        chain = [
            {"period_start": date(2026, 1, 1), "period_return": Decimal("0.1"), "display_currency": "GBP"}
            for _ in range(10)
        ]
        val = _valuation_with((_holding(),))
        risk = _risk_section(val, chain, None, observation_label="test")
        assert risk["observations"] == 0
        assert risk["insufficient_history"] is True


class TestComputeContributorsRealizedFold:
    """#1596 spec §4.4: realised deltas fold into period contribution.
    Pre-#1596 the chart was unrealised-open-only — a position closed
    mid-period vanished; a trim read as a phantom loss."""

    def test_trim_with_realised_gain_is_not_a_phantom_drag(self) -> None:
        """Sell half at a profit: unrealised drops 50, realised rises 60
        → net +10 contributor, not a −50 drag."""
        prior = [
            {
                "instrument_id": 1,
                "symbol": "AAPL",
                "unrealized_pnl": "100",
                "cost_basis": "1000",
                "realized_pnl": "0",
            }
        ]
        current = [
            {
                "instrument_id": 1,
                "symbol": "AAPL",
                "unrealized_pnl": "50",
                "cost_basis": "500",
                "realized_pnl": "60",
            }
        ]
        realized_now = {1: {"symbol": "AAPL", "realized_pnl": Decimal("60")}}
        result = _compute_contributors(current, prior, realized_now=realized_now)
        assert len(result["contributors"]) == 1
        assert result["contributors"][0]["pnl_delta"] == "10"
        assert result["drags"] == []

    def test_position_closed_in_period_still_contributes(self) -> None:
        """Closed position: prior unrealised 100 converts to realised
        120 → net +20 contributor despite being absent from current."""
        prior = [
            {
                "instrument_id": 1,
                "symbol": "AAPL",
                "unrealized_pnl": "100",
                "cost_basis": "1000",
                "realized_pnl": "0",
            }
        ]
        realized_now = {1: {"symbol": "AAPL", "realized_pnl": Decimal("120")}}
        result = _compute_contributors([], prior, realized_now=realized_now)
        assert len(result["contributors"]) == 1
        assert result["contributors"][0]["symbol"] == "AAPL"
        assert result["contributors"][0]["pnl_delta"] == "20"

    def test_v1_prior_without_realized_keeps_legacy_behavior(self) -> None:
        """Prior snapshot rows without `realized_pnl` (v1) must not
        invent realised deltas — unrealised-only diff, closed
        positions skipped."""
        prior = [
            {"instrument_id": 1, "symbol": "AAPL", "unrealized_pnl": "100", "cost_basis": "1000"},
            {"instrument_id": 2, "symbol": "MSFT", "unrealized_pnl": "50", "cost_basis": "500"},
        ]
        current = [
            {"instrument_id": 1, "symbol": "AAPL", "unrealized_pnl": "150", "cost_basis": "1000"},
        ]
        realized_now = {
            1: {"symbol": "AAPL", "realized_pnl": Decimal("999")},
            2: {"symbol": "MSFT", "realized_pnl": Decimal("999")},
        }
        result = _compute_contributors(current, prior, realized_now=realized_now)
        assert len(result["contributors"]) == 1
        assert result["contributors"][0]["pnl_delta"] == "50"
        # MSFT closed but prior row is v1 (no realised baseline) → skipped.
        assert result["drags"] == []


class TestHoldingsSection:
    def test_weights_since_entry_and_contribution(self) -> None:
        holdings = (
            _holding(instrument_id=1, symbol="AAPL", cost_basis=800.0, market_value=900.0),
            _holding(instrument_id=2, symbol="JPM", cost_basis=100.0, market_value=100.0),
        )
        val = _valuation_with(holdings, cash=0.0)
        prior_positions = [
            {
                "instrument_id": 1,
                "symbol": "AAPL",
                "unrealized_pnl": "60",
                "cost_basis": "800",
                "realized_pnl": "0",
            }
        ]
        realized_now = {1: {"symbol": "AAPL", "realized_pnl": Decimal("0")}}
        rows = _holdings_section(val, prior_positions, realized_now, Decimal("1000"))
        assert [r["symbol"] for r in rows] == ["AAPL", "JPM"]
        aapl = rows[0]
        assert aapl["weight_pct"] == "0.900000"
        assert aapl["since_entry_return_pct"] == "0.125000"
        # Unrealised now 100 vs prior 60 → +40 period contribution.
        assert aapl["period_contribution"] == "40.000000"
        # 40 / 1000 opening value = 400 bps.
        assert aapl["period_contribution_bps"] == "400.000000"
        # New position this period: no prior row → null contribution.
        assert rows[1]["period_contribution"] is None

    def test_no_prior_and_zero_cost_are_null_safe(self) -> None:
        holdings = (_holding(cost_basis=0.0, market_value=0.0, units=0.0),)
        val = _valuation_with(holdings, cash=0.0)
        rows = _holdings_section(val, None, {}, None)
        assert rows[0]["since_entry_return_pct"] is None
        assert rows[0]["weight_pct"] is None
        assert rows[0]["period_contribution"] is None


class TestRollingWindowContiguity:
    """Count alone must not satisfy a rolling window — calendar gaps
    would let stale returns masquerade as an N-month window."""

    def test_contiguous_months_pass(self) -> None:
        from app.services.reporting import _is_contiguous_monthly

        window = [
            {"period_start": date(2026, 3, 1)},
            {"period_start": date(2026, 4, 1)},
        ]
        assert _is_contiguous_monthly(window, date(2026, 5, 1)) is True

    def test_gap_before_current_fails(self) -> None:
        from app.services.reporting import _is_contiguous_monthly

        window = [{"period_start": date(2026, 2, 1)}]
        assert _is_contiguous_monthly(window, date(2026, 5, 1)) is False

    def test_gap_inside_window_fails(self) -> None:
        from app.services.reporting import _is_contiguous_monthly

        window = [
            {"period_start": date(2026, 1, 1)},
            {"period_start": date(2026, 4, 1)},
        ]
        assert _is_contiguous_monthly(window, date(2026, 5, 1)) is False

    def test_december_january_wrap(self) -> None:
        from app.services.reporting import _is_contiguous_monthly

        window = [{"period_start": date(2025, 12, 1)}]
        assert _is_contiguous_monthly(window, date(2026, 1, 1)) is True

    def test_empty_window_is_trivially_contiguous(self) -> None:
        from app.services.reporting import _is_contiguous_monthly

        assert _is_contiguous_monthly([], date(2026, 5, 1)) is True
