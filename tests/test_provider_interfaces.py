"""
Verify that all concrete provider stubs correctly implement their interfaces.

These tests do not make any network calls — they only confirm the class
hierarchy and that NotImplementedError is raised (not AttributeError or
TypeError), which proves the method signatures match the interface.
"""

from datetime import date

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


class TestFmpStub:
    def setup_method(self) -> None:
        self.provider = FmpFundamentalsProvider(api_key="test-key")

    def test_get_latest_snapshot_raises_not_implemented(self) -> None:
        with pytest.raises(NotImplementedError):
            self.provider.get_latest_snapshot("AAPL")

    def test_get_snapshot_history_raises_not_implemented(self) -> None:
        with pytest.raises(NotImplementedError):
            self.provider.get_snapshot_history("AAPL", date(2023, 1, 1), date(2024, 1, 1))


class TestSecEdgarStub:
    def setup_method(self) -> None:
        self.provider = SecFilingsProvider()

    def test_list_filings_raises_not_implemented(self) -> None:
        with pytest.raises(NotImplementedError):
            self.provider.list_filings("AAPL", date(2024, 1, 1), date(2024, 12, 31))

    def test_get_filing_raises_not_implemented(self) -> None:
        with pytest.raises(NotImplementedError):
            self.provider.get_filing("0001234567-24-000001")


class TestCompaniesHouseStub:
    def setup_method(self) -> None:
        self.provider = CompaniesHouseFilingsProvider(api_key="test-key")

    def test_list_filings_raises_not_implemented(self) -> None:
        with pytest.raises(NotImplementedError):
            self.provider.list_filings("BP", date(2024, 1, 1), date(2024, 12, 31))

    def test_get_filing_raises_not_implemented(self) -> None:
        with pytest.raises(NotImplementedError):
            self.provider.get_filing("MmQ1YzM4ZTliYWM4YzM2")


class TestNewsProviderIsAbstract:
    # No concrete stub exists for NewsProvider — v1 has no dedicated news provider.
    # The news service (issue #5) will decide the source (eToro feed, RSS, etc.) and
    # add an implementations/news.py at that point. This test confirms the ABC is
    # correctly defined so it cannot be accidentally instantiated directly.
    def test_cannot_instantiate_news_provider_directly(self) -> None:
        with pytest.raises(TypeError):
            NewsProvider()  # type: ignore[abstract]
