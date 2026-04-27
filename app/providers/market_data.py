"""
Market data provider interface.

eToro is the v1 implementation. All domain code imports this interface only —
never the concrete provider.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Literal


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
    # eToro instrumentTypeName ("Stock", "Crypto", "ETF", "Index",
    # "Currency", "Commodity", …). Cross-validated against
    # exchanges.asset_class downstream — a stock-typed instrument on a
    # crypto-classified exchange is a data-integrity flag (#503 PR 4).
    instrument_type: str | None = None
    # eToro instrumentTypeID — the numeric foreign key into
    # ``etoro_instrument_types`` so the frontend can render the
    # description label by joining on a stable id rather than a
    # text match. Captured alongside the name in #515 PR 1.
    instrument_type_id: int | None = None


@dataclass(frozen=True)
class ExchangeRecord:
    """An exchange entry from the provider's exchange catalogue."""

    provider_id: str  # provider-native exchange id (e.g. eToro exchangeId)
    description: str | None  # eToro's human-readable name; None if not provided


@dataclass(frozen=True)
class InstrumentTypeRecord:
    """An entry from eToro's instrument-types lookup catalogue."""

    type_id: int  # eToro instrumentTypeID
    description: str | None  # e.g. "Stocks", "ETF", "Crypto"


@dataclass(frozen=True)
class StocksIndustryRecord:
    """An entry from eToro's stocks-industries lookup catalogue."""

    industry_id: int  # eToro industryID
    name: str | None  # e.g. "Healthcare", "Technology"


@dataclass(frozen=True)
class OHLCVBar:
    """A single daily OHLCV candle."""

    price_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int | None


# Intraday candle interval tokens accepted by eToro's history endpoint.
# Mirrors the URL slot in /history/candles/{direction}/{interval}/{count}.
#
# **Sub-day only.** Daily / weekly / monthly views read from `price_daily`
# via the existing `/candles?range=...` endpoint so chart freshness is
# served by the persisted, scored series — not a parallel live-fetch
# path that would shadow it. If a long-horizon range needs a finer
# series than `price_daily` provides, the right move is to deepen the
# daily backfill (see #603), not to widen this Literal.
#
# Token set matches the documented eToro intraday options
# (docs/etoro-api-reference.md §candles).
IntradayInterval = Literal[
    "OneMinute",
    "FiveMinutes",
    "TenMinutes",
    "FifteenMinutes",
    "ThirtyMinutes",
    "OneHour",
    "FourHours",
]


@dataclass(frozen=True)
class IntradayBar:
    """A single intraday OHLCV candle.

    Distinct from `OHLCVBar` because intraday bars carry a full UTC
    timestamp (bar-open instant) rather than a calendar date. Caller is
    expected to know the interval so it can compute bar-close from
    `timestamp + interval`. No DB persistence — these flow live from
    provider through cache to the chart and never hit `price_daily`.
    """

    timestamp: datetime
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

        lookback_days is a hint, not a guarantee. The provider returns
        up to that many bars; the eToro endpoint caps at 1000 per
        request and offers no from_date pagination, so values above
        1000 silently truncate. Post-#603 the default is 1000 — about
        4 calendar years of trading-day price points, which is the
        maximum a single fetch can deliver.
        """

    @abstractmethod
    def get_intraday_candles(
        self,
        instrument_id: int,
        interval: IntradayInterval,
        count: int,
    ) -> list[IntradayBar]:
        """Return intraday OHLCV bars for an instrument.

        The provider chooses how many bars to return up to ``count``.
        Caller must supply both ``interval`` and ``count`` because the
        underlying eToro endpoint is count-based, not date-range-based.

        Bars carry full UTC timestamps (bar-open instant) and are
        ordered oldest-first. The still-forming current bar may or may
        not be included — implementations should pass through whatever
        the provider returns; consumers that need only completed bars
        should filter on ``timestamp + interval <= now``.

        Returns an empty list if the instrument has no intraday data.
        Raises on transport/auth failures so the API layer can surface
        429s as 503s with a Retry-After hint.
        """

    @abstractmethod
    def get_quote(self, instrument_id: int) -> Quote | None:
        """Return the current quote for a single instrument.

        Returns None if the instrument is not recognised or not
        currently quoted.
        """

    @abstractmethod
    def get_instrument_types(self) -> list[InstrumentTypeRecord]:
        """Return the provider's instrument-type lookup catalogue.

        eBull joins these rows on ``instruments.instrument_type_id``
        to render the human-readable label ("Stocks", "ETF",
        "Crypto", …). Implementations that don't expose a separate
        catalogue endpoint may return an empty list.
        """

    @abstractmethod
    def get_stocks_industries(self) -> list[StocksIndustryRecord]:
        """Return the provider's stocks-industries lookup catalogue.

        eBull joins on ``instruments.sector`` (which stores the
        provider's numeric industry id) to render industry names.
        Empty list permitted for providers that don't ship a
        catalogue.
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
