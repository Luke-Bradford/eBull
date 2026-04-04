"""
Market data provider interface.

eToro is the v1 implementation. All domain code imports this interface only —
never the concrete provider.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal


@dataclass(frozen=True)
class InstrumentRecord:
    """A tradable instrument as reported by the market data provider."""

    provider_id: str  # provider-native identifier (e.g. eToro instrument ID)
    symbol: str
    company_name: str
    exchange: str | None
    currency: str
    sector: str | None
    industry: str | None
    country: str | None
    is_tradable: bool


@dataclass(frozen=True)
class OHLCVBar:
    """A single daily OHLCV candle."""

    symbol: str
    price_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int | None


@dataclass(frozen=True)
class Quote:
    """A current best-bid/ask quote."""

    symbol: str
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    last: Decimal | None


class MarketDataProvider(ABC):
    """
    Interface for market data: tradable universe, candles, and quotes.

    v1 implementation: EtoroMarketDataProvider
    """

    @abstractmethod
    def get_tradable_instruments(self) -> list[InstrumentRecord]:
        """Return the full list of currently tradable instruments."""

    @abstractmethod
    def get_daily_candles(
        self,
        symbol: str,
        from_date: date,
        to_date: date,
    ) -> list[OHLCVBar]:
        """Return OHLCV bars for a symbol over the requested date range."""

    @abstractmethod
    def get_quote(self, symbol: str) -> Quote:
        """Return the current quote for a symbol."""
