"""Tests for YFinanceProvider (stub-based; no live Yahoo calls)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest

from app.providers.implementations.yfinance_provider import (
    YFinanceAnalystEstimates,
    YFinanceDividend,
    YFinanceKeyStats,
    YFinanceMajorHolders,
    YFinancePriceBar,
    YFinanceProfile,
    YFinanceProvider,
    YFinanceSnapshot,
)


@dataclass
class _StubTicker:
    """Minimal fake for yfinance.Ticker used in tests."""

    info: dict[str, Any]
    dividends: pd.Series | None = None
    quarterly_financials: pd.DataFrame | None = None
    quarterly_balance_sheet: pd.DataFrame | None = None
    quarterly_cashflow: pd.DataFrame | None = None
    financials: pd.DataFrame | None = None
    balance_sheet: pd.DataFrame | None = None
    cashflow: pd.DataFrame | None = None
    _history_frame: pd.DataFrame | None = None

    def history(self, period: str = "1y", interval: str = "1d") -> pd.DataFrame:  # noqa: ARG002
        return self._history_frame if self._history_frame is not None else pd.DataFrame()


def _provider_with(ticker: _StubTicker) -> YFinanceProvider:
    provider = YFinanceProvider()
    # Patch the internal _ticker factory so every lookup returns the stub.
    provider._ticker = lambda _symbol: ticker  # type: ignore[method-assign]
    return provider


# ---------------------------------------------------------------------------
# Profile
# ---------------------------------------------------------------------------


def test_profile_happy_path() -> None:
    ticker = _StubTicker(
        info={
            "longName": "Apple Inc.",
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "exchange": "NMS",
            "country": "United States",
            "currency": "USD",
            "marketCap": 3_000_000_000_000,
            "fullTimeEmployees": 150_000,
            "website": "https://apple.com",
            "longBusinessSummary": "Designs Macs and iPhones.",
        }
    )
    profile = _provider_with(ticker).get_profile("AAPL")
    assert profile == YFinanceProfile(
        symbol="AAPL",
        display_name="Apple Inc.",
        sector="Technology",
        industry="Consumer Electronics",
        exchange="NMS",
        country="United States",
        currency="USD",
        market_cap=Decimal("3000000000000"),
        employees=150_000,
        website="https://apple.com",
        long_business_summary="Designs Macs and iPhones.",
    )


def test_profile_falls_back_to_shortName_when_longName_missing() -> None:
    ticker = _StubTicker(info={"shortName": "BRK", "sector": "Financial Services"})
    profile = _provider_with(ticker).get_profile("BRK.A")
    assert profile is not None
    assert profile.display_name == "BRK"


def test_profile_returns_none_on_exception() -> None:
    provider = YFinanceProvider()
    with patch.object(
        provider,
        "_ticker",
        side_effect=RuntimeError("yahoo is down"),
    ):
        assert provider.get_profile("AAPL") is None


# ---------------------------------------------------------------------------
# Quote
# ---------------------------------------------------------------------------


def test_quote_derives_day_change_from_previous_close() -> None:
    ticker = _StubTicker(
        info={
            "regularMarketPrice": 101.5,
            "regularMarketPreviousClose": 100.0,
            "fiftyTwoWeekHigh": 120.0,
            "fiftyTwoWeekLow": 80.0,
            "currency": "USD",
        }
    )
    quote = _provider_with(ticker).get_quote("AAPL")
    assert quote is not None
    assert quote.price == Decimal("101.5")
    assert quote.day_change == Decimal("1.5")
    assert quote.day_change_pct == Decimal("1.5") / Decimal("100.0")


def test_quote_handles_missing_previous_close_gracefully() -> None:
    ticker = _StubTicker(info={"regularMarketPrice": 50.0, "currency": "GBP"})
    quote = _provider_with(ticker).get_quote("VOD.L")
    assert quote is not None
    assert quote.price == Decimal("50.0")
    assert quote.day_change is None
    assert quote.day_change_pct is None


# ---------------------------------------------------------------------------
# Key stats
# ---------------------------------------------------------------------------


def test_key_stats_happy_path() -> None:
    ticker = _StubTicker(
        info={
            "trailingPE": 25.5,
            "priceToBook": 35.0,
            "dividendYield": 0.005,
            "payoutRatio": 0.15,
            "returnOnEquity": 1.5,
            "returnOnAssets": 0.3,
            "debtToEquity": 195.0,
            "revenueGrowth": 0.08,
            "earningsGrowth": 0.12,
        }
    )
    stats = _provider_with(ticker).get_key_stats("AAPL")
    assert stats == YFinanceKeyStats(
        symbol="AAPL",
        pe_ratio=Decimal("25.5"),
        pb_ratio=Decimal("35.0"),
        dividend_yield=Decimal("0.005"),
        payout_ratio=Decimal("0.15"),
        roe=Decimal("1.5"),
        roa=Decimal("0.3"),
        debt_to_equity=Decimal("195.0"),
        revenue_growth_yoy=Decimal("0.08"),
        earnings_growth_yoy=Decimal("0.12"),
    )


# ---------------------------------------------------------------------------
# Financials
# ---------------------------------------------------------------------------


def _sample_financials_frame() -> pd.DataFrame:
    """Build a sample income statement with 2 quarters."""
    return pd.DataFrame(
        {
            pd.Timestamp("2026-03-31"): {
                "Total Revenue": 90_000_000_000,
                "Net Income": 25_000_000_000,
            },
            pd.Timestamp("2025-12-31"): {
                "Total Revenue": 120_000_000_000,
                "Net Income": 33_000_000_000,
            },
        }
    )


def test_financials_quarterly_income_happy_path() -> None:
    ticker = _StubTicker(
        info={"financialCurrency": "USD"},
        quarterly_financials=_sample_financials_frame(),
    )
    financials = _provider_with(ticker).get_financials(
        "AAPL",
        statement="income",
        period="quarterly",
    )
    assert financials is not None
    assert financials.currency == "USD"
    assert len(financials.rows) == 2
    # Rows sorted descending by period_end.
    assert financials.rows[0].period_end == date(2026, 3, 31)
    assert financials.rows[1].period_end == date(2025, 12, 31)
    assert financials.rows[0].values["Total Revenue"] == Decimal("90000000000")


def test_financials_annual_routes_to_annual_frame() -> None:
    ticker = _StubTicker(
        info={"financialCurrency": "USD"},
        financials=_sample_financials_frame(),
    )
    financials = _provider_with(ticker).get_financials(
        "AAPL",
        statement="income",
        period="annual",
    )
    assert financials is not None
    assert financials.period == "annual"


def test_financials_empty_frame_returns_none() -> None:
    ticker = _StubTicker(
        info={"financialCurrency": "USD"},
        quarterly_financials=pd.DataFrame(),
    )
    financials = _provider_with(ticker).get_financials(
        "AAPL",
        statement="income",
        period="quarterly",
    )
    assert financials is None


def test_financials_drops_nan_values() -> None:
    frame = pd.DataFrame(
        {
            pd.Timestamp("2026-03-31"): {
                "Total Revenue": 90_000_000_000,
                "Dead Concept": float("nan"),
            }
        }
    )
    ticker = _StubTicker(info={"financialCurrency": "USD"}, quarterly_financials=frame)
    financials = _provider_with(ticker).get_financials(
        "AAPL",
        statement="income",
        period="quarterly",
    )
    assert financials is not None
    assert len(financials.rows) == 1
    # NaN column should have been dropped from the row's values dict.
    assert "Dead Concept" not in financials.rows[0].values
    assert financials.rows[0].values["Total Revenue"] == Decimal("90000000000")


# ---------------------------------------------------------------------------
# Dividends
# ---------------------------------------------------------------------------


def test_dividends_returns_sorted_list() -> None:
    series = pd.Series(
        data=[0.22, 0.23, 0.24],
        index=[
            pd.Timestamp("2025-02-14"),
            pd.Timestamp("2025-05-14"),
            pd.Timestamp("2025-08-14"),
        ],
    )
    ticker = _StubTicker(info={}, dividends=series)
    dividends = _provider_with(ticker).get_dividends("AAPL")
    assert dividends == [
        YFinanceDividend(ex_date=date(2025, 8, 14), amount=Decimal("0.24")),
        YFinanceDividend(ex_date=date(2025, 5, 14), amount=Decimal("0.23")),
        YFinanceDividend(ex_date=date(2025, 2, 14), amount=Decimal("0.22")),
    ]


def test_dividends_empty_series_returns_empty_list() -> None:
    ticker = _StubTicker(info={}, dividends=pd.Series(dtype=float))
    assert _provider_with(ticker).get_dividends("AAPL") == []


# ---------------------------------------------------------------------------
# Analyst estimates + major holders
# ---------------------------------------------------------------------------


def test_analyst_estimates_happy_path() -> None:
    ticker = _StubTicker(
        info={
            "targetMeanPrice": 200.0,
            "targetHighPrice": 250.0,
            "targetLowPrice": 150.0,
            "recommendationMean": 2.1,
            "numberOfAnalystOpinions": 40,
        }
    )
    estimates = _provider_with(ticker).get_analyst_estimates("AAPL")
    assert estimates == YFinanceAnalystEstimates(
        symbol="AAPL",
        target_mean=Decimal("200.0"),
        target_high=Decimal("250.0"),
        target_low=Decimal("150.0"),
        recommendation_mean=Decimal("2.1"),
        num_analysts=40,
    )


def test_major_holders_happy_path() -> None:
    ticker = _StubTicker(
        info={
            "heldPercentInsiders": 0.001,
            "heldPercentInstitutions": 0.65,
            "numberOfInstitutionalHolders": 5200,
        }
    )
    holders = _provider_with(ticker).get_major_holders("AAPL")
    assert holders == YFinanceMajorHolders(
        symbol="AAPL",
        insiders_pct=Decimal("0.001"),
        institutions_pct=Decimal("0.65"),
        institutional_holders_count=5200,
    )


# ---------------------------------------------------------------------------
# Defensive fallbacks
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "method",
    ["get_profile", "get_quote", "get_key_stats", "get_dividends", "get_analyst_estimates", "get_major_holders"],
)
def test_every_accessor_returns_none_on_exception(method: str) -> None:
    provider = YFinanceProvider()
    with patch.object(provider, "_ticker", side_effect=RuntimeError("yahoo is down")):
        assert getattr(provider, method)("AAPL") is None


def test_get_financials_returns_none_on_exception() -> None:
    provider = YFinanceProvider()
    with patch.object(provider, "_ticker", side_effect=RuntimeError("yahoo is down")):
        assert provider.get_financials("AAPL", statement="income") is None


# ---------------------------------------------------------------------------
# NaN-safe string coercion (Codex feedback)
# ---------------------------------------------------------------------------


def test_profile_filters_nan_string_fields() -> None:
    """Yahoo occasionally returns float NaN for missing string fields.
    str(float('nan')) == 'nan' — the UI must see None, not the literal
    'nan', so the coercion treats NaN as missing."""
    ticker = _StubTicker(
        info={
            "longName": "Vodafone Group PLC",
            "sector": float("nan"),
            "industry": "nan",  # string 'nan' too, case-insensitive
            "country": "United Kingdom",
        }
    )
    profile = _provider_with(ticker).get_profile("VOD.L")
    assert profile is not None
    assert profile.display_name == "Vodafone Group PLC"
    assert profile.sector is None
    assert profile.industry is None
    assert profile.country == "United Kingdom"


# ---------------------------------------------------------------------------
# Strict period validation (Codex feedback)
# ---------------------------------------------------------------------------


def test_financials_info_failure_does_not_discard_frame() -> None:
    """If .info raises after the frame was fetched, we must still return
    the statement — currency is just display metadata, not gating data.
    Regression for PR #356 round-2 review."""

    class _RaisingInfoTicker:
        quarterly_financials = _sample_financials_frame()
        quarterly_balance_sheet = pd.DataFrame()
        quarterly_cashflow = pd.DataFrame()

        @property
        def info(self) -> dict[str, Any]:
            raise RuntimeError("info endpoint flaky")

    provider = YFinanceProvider()
    provider._ticker = lambda _symbol: _RaisingInfoTicker()  # type: ignore[method-assign,return-value]
    financials = provider.get_financials("AAPL", statement="income", period="quarterly")
    assert financials is not None
    assert financials.currency is None
    assert len(financials.rows) == 2


def test_financials_invalid_period_returns_none() -> None:
    ticker = _StubTicker(
        info={"financialCurrency": "USD"},
        quarterly_financials=_sample_financials_frame(),
    )
    financials = _provider_with(ticker).get_financials(
        "AAPL",
        statement="income",
        period="weekly",  # type: ignore[arg-type]
    )
    assert financials is None


# ---------------------------------------------------------------------------
# Price history
# ---------------------------------------------------------------------------


def _sample_history_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Open": [100.0, 101.5, 103.0],
            "High": [102.0, 103.5, 104.0],
            "Low": [99.5, 100.0, 102.0],
            "Close": [101.0, 103.0, 103.5],
            "Volume": [1_000_000, 1_500_000, 1_200_000],
        },
        index=[
            pd.Timestamp("2026-04-15"),
            pd.Timestamp("2026-04-16"),
            pd.Timestamp("2026-04-17"),
        ],
    )


def test_price_history_happy_path_sorted_oldest_first() -> None:
    ticker = _StubTicker(info={}, _history_frame=_sample_history_frame())
    bars = _provider_with(ticker).get_price_history("AAPL", period="5d")
    assert bars == [
        YFinancePriceBar(
            bar_date=date(2026, 4, 15),
            open=Decimal("100.0"),
            high=Decimal("102.0"),
            low=Decimal("99.5"),
            close=Decimal("101.0"),
            volume=1_000_000,
        ),
        YFinancePriceBar(
            bar_date=date(2026, 4, 16),
            open=Decimal("101.5"),
            high=Decimal("103.5"),
            low=Decimal("100.0"),
            close=Decimal("103.0"),
            volume=1_500_000,
        ),
        YFinancePriceBar(
            bar_date=date(2026, 4, 17),
            open=Decimal("103.0"),
            high=Decimal("104.0"),
            low=Decimal("102.0"),
            close=Decimal("103.5"),
            volume=1_200_000,
        ),
    ]


def test_price_history_empty_frame_returns_empty_list() -> None:
    ticker = _StubTicker(info={}, _history_frame=pd.DataFrame())
    bars = _provider_with(ticker).get_price_history("DELISTED")
    assert bars == []


def test_price_history_returns_none_on_exception() -> None:
    provider = YFinanceProvider()
    with patch.object(provider, "_ticker", side_effect=RuntimeError("yahoo is down")):
        assert provider.get_price_history("AAPL") is None


# ---------------------------------------------------------------------------
# Consolidated snapshot (one .info fetch for all 3 dataclasses)
# ---------------------------------------------------------------------------


def test_get_snapshot_single_info_fetch_builds_all_three() -> None:
    """Snapshot triggers exactly one .info access and derives profile +
    quote + key_stats from that single payload. Addresses Codex review
    concern that separate accessors triple Yahoo scrape pressure."""
    access_count = 0

    class _CountingTicker:
        @property
        def info(self) -> dict[str, Any]:
            nonlocal access_count
            access_count += 1
            return {
                "longName": "Apple Inc.",
                "sector": "Technology",
                "marketCap": 3_000_000_000_000,
                "regularMarketPrice": 200.5,
                "regularMarketPreviousClose": 199.0,
                "fiftyTwoWeekHigh": 250.0,
                "fiftyTwoWeekLow": 140.0,
                "currency": "USD",
                "trailingPE": 28.5,
                "priceToBook": 40.2,
            }

    provider = YFinanceProvider()
    provider._ticker = lambda _symbol: _CountingTicker()  # type: ignore[method-assign,return-value]
    snapshot = provider.get_snapshot("AAPL")
    assert access_count == 1
    assert isinstance(snapshot, YFinanceSnapshot)
    assert snapshot.profile is not None and snapshot.profile.display_name == "Apple Inc."
    assert snapshot.quote is not None and snapshot.quote.price == Decimal("200.5")
    assert snapshot.quote.day_change == Decimal("1.5")
    assert snapshot.key_stats is not None and snapshot.key_stats.pe_ratio == Decimal("28.5")


def test_get_snapshot_info_raise_returns_all_nones() -> None:
    provider = YFinanceProvider()
    with patch.object(provider, "_ticker", side_effect=RuntimeError("yahoo is down")):
        snapshot = provider.get_snapshot("AAPL")
    assert snapshot == YFinanceSnapshot(profile=None, quote=None, key_stats=None)


def test_get_snapshot_empty_info_returns_all_nones() -> None:
    class _EmptyTicker:
        info: dict[str, Any] = {}

    provider = YFinanceProvider()
    provider._ticker = lambda _symbol: _EmptyTicker()  # type: ignore[method-assign,return-value]
    snapshot = provider.get_snapshot("AAPL")
    assert snapshot == YFinanceSnapshot(profile=None, quote=None, key_stats=None)


def test_get_snapshot_info_with_only_identity_returns_null_quote_and_stats() -> None:
    """If .info has identity fields but no price or stats keys, quote and
    key_stats sections must be None — not all-None dataclasses. Addresses
    PR #358 review WARNING: contract says null sections, not shells."""

    class _IdentityOnlyTicker:
        info = {
            "longName": "Tiny Co",
            "sector": "Consumer",
        }

    provider = YFinanceProvider()
    provider._ticker = lambda _symbol: _IdentityOnlyTicker()  # type: ignore[method-assign,return-value]
    snapshot = provider.get_snapshot("TINY")
    assert snapshot.profile is not None
    assert snapshot.profile.display_name == "Tiny Co"
    assert snapshot.quote is None
    assert snapshot.key_stats is None


def test_get_snapshot_info_with_all_null_price_fields_returns_null_quote() -> None:
    """Explicit NaN or None values in price fields must collapse to quote=None."""

    class _NullPriceTicker:
        info = {
            "longName": "Ghost Co",
            "regularMarketPrice": float("nan"),
            "regularMarketPreviousClose": None,
            "fiftyTwoWeekHigh": None,
            "fiftyTwoWeekLow": float("nan"),
            # Stats still all null
        }

    provider = YFinanceProvider()
    provider._ticker = lambda _symbol: _NullPriceTicker()  # type: ignore[method-assign,return-value]
    snapshot = provider.get_snapshot("GHOST")
    assert snapshot.quote is None
    assert snapshot.key_stats is None


def test_to_decimal_nan_returns_none() -> None:
    """Direct invariant: _to_decimal(float('nan')) -> None, not Decimal('NaN').
    Pins the NaN filter that _NullPriceTicker relies on."""
    from app.providers.implementations.yfinance_provider import _to_decimal

    assert _to_decimal(float("nan")) is None
    assert _to_decimal(Decimal("NaN")) is None


def test_get_snapshot_preserves_zero_price() -> None:
    """A legitimate zero price must NOT fall through via `or` short-circuit
    to currentPrice. Addresses PR #358 BLOCKING review."""

    class _ZeroPriceTicker:
        info = {
            "longName": "Zero Co",
            # regularMarketPrice = 0 is legitimate (halted stock, distressed
            # delisted ticker); it's falsy but not missing.
            "regularMarketPrice": 0,
            "currentPrice": 5.0,  # would shadow the real price under `or`
            "regularMarketPreviousClose": 10.0,
            "fiftyTwoWeekHigh": 100.0,
            "fiftyTwoWeekLow": 0.0,
        }

    provider = YFinanceProvider()
    provider._ticker = lambda _symbol: _ZeroPriceTicker()  # type: ignore[method-assign,return-value]
    snapshot = provider.get_snapshot("ZERO")
    assert snapshot.quote is not None
    # Real zero price must survive, NOT be replaced by currentPrice=5.0.
    assert snapshot.quote.price == Decimal("0")
    # day_change = 0 - 10 = -10.
    assert snapshot.quote.day_change == Decimal("-10")
    # week_52_low=0 preserved too.
    assert snapshot.quote.week_52_low == Decimal("0")
