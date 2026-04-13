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
    currency: str | None
    sector: str | None
    industry: str | None
    country: str | None
    is_tradable: bool


@dataclass(frozen=True)
class OHLCVBar:
    """A single daily OHLCV candle."""

    price_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int | None


@dataclass(frozen=True)
class Quote:
    """A current best-bid/ask quote."""

    instrument_id: int
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    last: Decimal | None
    # Instrument-currency → account-currency conversion rate (mid of bid/ask).
    # Populated by providers that embed FX data in quote responses (e.g. eToro).
    # None when the provider does not supply conversion data.
    conversion_rate: Decimal | None = None


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
        instrument_id: int,
        lookback_days: int,
    ) -> list[OHLCVBar]:
        """Return daily OHLCV bars for an instrument.

        Returns completed daily bars only — any still-forming current-day
        bar from the API is excluded.

        Ordering: oldest-first.

        lookback_days is a hint, not a guarantee. The provider returns up
        to that many trading days of data, which may be fewer calendar
        days than requested due to weekends and holidays. The eToro
        candle endpoint caps at 1000 candles per request; the current
        400-day lookback is well within that limit.
        """

    @abstractmethod
    def get_quote(self, instrument_id: int) -> Quote | None:
        """Return the current quote for a single instrument.

        Returns None if the instrument is not recognised or not
        currently quoted.
        """

    @abstractmethod
    def get_quotes(self, instrument_ids: list[int]) -> list[Quote]:
        """Batch quote fetch.

        Implementations handle any provider-specific batching limits
        internally. Callers pass the full list of IDs.

        Returns a list of Quote objects with no ordering guarantee.
        Each Quote carries instrument_id so callers match results
        by ID, not by position.

        Instruments that are not recognised or not currently quoted
        are silently omitted from the result list.
        """
