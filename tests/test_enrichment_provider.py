"""
Unit tests for FMP enrichment normaliser functions.

Tests only the pure normaliser functions (_build_profile_data,
_build_earnings_event, _build_analyst_estimates) — no I/O, no HTTP.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.providers.implementations.fmp import (
    _build_analyst_estimates,
    _build_earnings_event,
    _build_profile_data,
)

# ---------------------------------------------------------------------------
# _build_profile_data
# ---------------------------------------------------------------------------


class TestBuildProfileData:
    def test_build_profile_data_full(self) -> None:
        """All FMP profile fields are correctly mapped and converted."""
        item = {
            "beta": "1.23",
            "floatShares": 15_000_000_000,
            "volAvg": 80_000_000,
            "mktCap": 2_800_000_000_000,
            "fullTimeEmployees": 164_000,
            "ipoDate": "1980-12-12",
            "isActivelyTrading": True,
        }
        result = _build_profile_data("AAPL", item)

        assert result.symbol == "AAPL"
        assert result.beta == pytest.approx(Decimal("1.23"))
        assert result.public_float == 15_000_000_000
        assert result.avg_volume_30d == 80_000_000
        assert result.market_cap == Decimal("2800000000000")
        assert result.employees == 164_000
        assert result.ipo_date == date(1980, 12, 12)
        assert result.is_actively_trading is True

    def test_build_profile_data_missing_fields(self) -> None:
        """Minimal response — all optional fields should be None."""
        result = _build_profile_data("XYZ", {})

        assert result.symbol == "XYZ"
        assert result.beta is None
        assert result.public_float is None
        assert result.avg_volume_30d is None
        assert result.market_cap is None
        assert result.employees is None
        assert result.ipo_date is None
        assert result.is_actively_trading is None

    def test_build_profile_data_invalid_ipo_date(self) -> None:
        """Unparseable ipoDate yields None, not an exception."""
        item = {"ipoDate": "not-a-date"}
        result = _build_profile_data("FOO", item)
        assert result.ipo_date is None

    def test_build_profile_data_is_actively_trading_non_bool(self) -> None:
        """String or int for isActivelyTrading should be coerced to None."""
        item = {"isActivelyTrading": "true"}
        result = _build_profile_data("BAR", item)
        assert result.is_actively_trading is None

    def test_build_profile_data_is_actively_trading_false(self) -> None:
        """Boolean False should be preserved (not treated as falsy None)."""
        item = {"isActivelyTrading": False}
        result = _build_profile_data("DELIST", item)
        assert result.is_actively_trading is False


# ---------------------------------------------------------------------------
# _build_earnings_event
# ---------------------------------------------------------------------------


class TestBuildEarningsEvent:
    def test_build_earnings_event_full(self) -> None:
        """Full FMP row including EPS, revenue, and surprise calculation."""
        row = {
            "date": "2024-09-28",
            "reportedDate": "2024-10-31",
            "epsEstimated": "1.50",
            "eps": "1.64",
            "revenueEstimated": "94_000_000_000",
            "revenue": "94_930_000_000",
        }
        event = _build_earnings_event("AAPL", row)

        assert event.symbol == "AAPL"
        assert event.fiscal_date_ending == date(2024, 9, 28)
        assert event.reporting_date == date(2024, 10, 31)
        assert event.eps_estimate == Decimal("1.50")
        assert event.eps_actual == Decimal("1.64")
        assert event.revenue_estimate == Decimal("94_000_000_000")
        assert event.revenue_actual == Decimal("94_930_000_000")
        # surprise_pct = (1.64 - 1.50) / abs(1.50) * 100
        expected_surprise = (Decimal("1.64") - Decimal("1.50")) / abs(Decimal("1.50")) * Decimal(100)
        assert event.surprise_pct == pytest.approx(expected_surprise)

    def test_build_earnings_event_missing_eps(self) -> None:
        """When EPS fields are absent, surprise should be None."""
        row = {
            "date": "2024-06-29",
            "reportedDate": "2024-07-30",
            "revenueEstimated": "85_000_000_000",
            "revenue": "85_777_000_000",
        }
        event = _build_earnings_event("AAPL", row)

        assert event.eps_estimate is None
        assert event.eps_actual is None
        assert event.surprise_pct is None
        assert event.revenue_estimate is not None
        assert event.revenue_actual is not None

    def test_build_earnings_event_zero_eps_estimate(self) -> None:
        """eps_estimate == 0 must not cause a division by zero — surprise should be None."""
        row = {
            "date": "2023-12-30",
            "epsEstimated": "0",
            "eps": "0.05",
        }
        event = _build_earnings_event("TSLA", row)

        # _decimal_or_none("0") returns Decimal("0") — the guard (eps_estimate != 0) prevents
        # division by zero and leaves surprise_pct as None
        assert event.eps_estimate == Decimal("0")
        assert event.eps_actual == Decimal("0.05")
        assert event.surprise_pct is None

    def test_build_earnings_event_missing_reported_date(self) -> None:
        """reportedDate absent should give reporting_date=None."""
        row = {"date": "2025-03-29"}
        event = _build_earnings_event("NVDA", row)

        assert event.reporting_date is None
        assert event.fiscal_date_ending == date(2025, 3, 29)

    def test_build_earnings_event_invalid_reported_date(self) -> None:
        """Unparseable reportedDate yields None, not an exception."""
        row = {"date": "2025-03-29", "reportedDate": "TBD"}
        event = _build_earnings_event("NVDA", row)

        assert event.reporting_date is None


# ---------------------------------------------------------------------------
# _build_analyst_estimates
# ---------------------------------------------------------------------------


class TestBuildAnalystEstimates:
    def test_build_analyst_estimates_full(self) -> None:
        """All three inputs present — all fields populated correctly."""
        estimates = [
            {
                "date": "2025-03-31",
                "estimatedEpsAvg": "2.35",
                "estimatedRevenueAvg": "124_000_000_000",
                "numberAnalystEstimatedEps": 25,
            }
        ]
        consensus: dict[str, object] = {
            "buy": 32,
            "hold": 8,
            "sell": 3,
        }
        price_target: dict[str, object] = {
            "targetMean": "215.50",
            "targetHigh": "260.00",
            "targetLow": "170.00",
            "numberOfAnalysts": 40,
        }

        result = _build_analyst_estimates("AAPL", estimates, consensus, price_target)

        assert result is not None
        assert result.symbol == "AAPL"
        assert result.as_of_date == date(2025, 3, 31)
        assert result.consensus_eps_fq == Decimal("2.35")
        assert result.consensus_eps_fy is None
        assert result.consensus_rev_fq == Decimal("124_000_000_000")
        assert result.consensus_rev_fy is None
        # numberOfAnalysts from price_target takes precedence over numberAnalystEstimatedEps
        assert result.analyst_count == 40
        assert result.buy_count == 32
        assert result.hold_count == 8
        assert result.sell_count == 3
        assert result.price_target_mean == Decimal("215.50")
        assert result.price_target_high == Decimal("260.00")
        assert result.price_target_low == Decimal("170.00")

    def test_build_analyst_estimates_no_data(self) -> None:
        """All inputs empty/None — must return None."""
        result = _build_analyst_estimates("NOPE", [], None, None)
        assert result is None

    def test_build_analyst_estimates_partial_estimates_only(self) -> None:
        """Only estimates present — consensus and price target fields are None."""
        estimates = [
            {
                "date": "2025-06-30",
                "estimatedEpsAvg": "3.10",
                "estimatedRevenueAvg": "135_000_000_000",
                "numberAnalystEstimatedEps": 20,
            }
        ]

        result = _build_analyst_estimates("AAPL", estimates, None, None)

        assert result is not None
        assert result.consensus_eps_fq == Decimal("3.10")
        assert result.analyst_count == 20
        assert result.buy_count is None
        assert result.hold_count is None
        assert result.sell_count is None
        assert result.price_target_mean is None
        assert result.price_target_high is None
        assert result.price_target_low is None

    def test_build_analyst_estimates_consensus_only(self) -> None:
        """Only consensus present — as_of_date falls back to today, eps/rev are None."""
        consensus: dict[str, object] = {"buy": 15, "hold": 5, "sell": 2}

        result = _build_analyst_estimates("MSFT", [], consensus, None)

        assert result is not None
        assert result.consensus_eps_fq is None
        assert result.consensus_rev_fq is None
        assert result.buy_count == 15
        assert result.hold_count == 5
        assert result.sell_count == 2
        assert result.as_of_date == date.today()

    def test_build_analyst_estimates_analyst_count_fallback(self) -> None:
        """When price_target has no numberOfAnalysts, fall back to estimates count."""
        estimates = [
            {
                "date": "2025-03-31",
                "estimatedEpsAvg": "1.00",
                "numberAnalystEstimatedEps": 12,
            }
        ]
        price_target: dict[str, object] = {
            "targetMean": "100.00",
            # numberOfAnalysts absent
        }

        result = _build_analyst_estimates("TST", estimates, None, price_target)

        assert result is not None
        assert result.analyst_count == 12
