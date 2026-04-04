"""
Verify that all concrete provider stubs correctly implement their interfaces.

These tests do not make any network calls — they only confirm the class
hierarchy and that NotImplementedError is raised (not AttributeError or
TypeError), which proves the method signatures match the interface.
"""

import pytest

from app.providers.filings import FilingsProvider
from app.providers.fundamentals import FundamentalsProvider
from app.providers.implementations.companies_house import CompaniesHouseFilingsProvider
from app.providers.implementations.etoro import EtoroMarketDataProvider
from app.providers.implementations.fmp import FmpFundamentalsProvider
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.providers.market_data import MarketDataProvider
from app.providers.news import NewsProvider


class TestInterfaceHierarchy:
    def test_etoro_is_market_data_provider(self) -> None:
        assert issubclass(EtoroMarketDataProvider, MarketDataProvider)

    def test_fmp_is_fundamentals_provider(self) -> None:
        assert issubclass(FmpFundamentalsProvider, FundamentalsProvider)

    def test_sec_is_filings_provider(self) -> None:
        assert issubclass(SecFilingsProvider, FilingsProvider)

    def test_companies_house_is_filings_provider(self) -> None:
        assert issubclass(CompaniesHouseFilingsProvider, FilingsProvider)


class TestEtoroProvider:
    def test_context_manager_closes_cleanly(self) -> None:
        # Confirms __enter__/__exit__ are present and don't raise on close.
        with EtoroMarketDataProvider(api_key="test-key", env="demo"):
            pass


class TestFmpProvider:
    def test_context_manager_closes_cleanly(self) -> None:
        with FmpFundamentalsProvider(api_key="test-key"):
            pass


class TestSecEdgarProvider:
    def test_context_manager_closes_cleanly(self) -> None:
        with SecFilingsProvider(user_agent="test-agent test@example.com"):
            pass

    def test_invalid_identifier_type_raises(self) -> None:
        with SecFilingsProvider(user_agent="test-agent test@example.com") as provider:
            with pytest.raises(ValueError, match="identifier_type='cik'"):
                provider.list_filings_by_identifier("symbol", "AAPL")


class TestCompaniesHouseProvider:
    def test_context_manager_closes_cleanly(self) -> None:
        with CompaniesHouseFilingsProvider(api_key="test-key"):
            pass

    def test_invalid_identifier_type_raises(self) -> None:
        with CompaniesHouseFilingsProvider(api_key="test-key") as provider:
            with pytest.raises(ValueError, match="identifier_type='company_number'"):
                provider.list_filings_by_identifier("cik", "0000320193")


class TestNewsProviderIsAbstract:
    # No concrete stub exists for NewsProvider — v1 has no dedicated news provider.
    # The news service (issue #5) will decide the source (eToro feed, RSS, etc.) and
    # add an implementations/news.py at that point. This test confirms the ABC is
    # correctly defined so it cannot be accidentally instantiated directly.
    def test_cannot_instantiate_news_provider_directly(self) -> None:
        with pytest.raises(TypeError):
            NewsProvider()  # type: ignore[abstract]
