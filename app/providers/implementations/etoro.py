"""
eToro market data provider.

Implements MarketDataProvider against the eToro read API.
Full implementation is built in issue #2 (universe) and issue #3 (market data).
"""

from datetime import date

from app.providers.market_data import InstrumentRecord, MarketDataProvider, OHLCVBar, Quote


class EtoroMarketDataProvider(MarketDataProvider):
    """
    Reads tradable instruments, candles, and quotes from the eToro API.

    Requires ETORO_READ_API_KEY in environment settings.
    """

    def __init__(self, api_key: str, env: str = "demo") -> None:
        self._api_key = api_key
        self._env = env  # "demo" | "live"

    def get_tradable_instruments(self) -> list[InstrumentRecord]:
        raise NotImplementedError("Implemented in issue #2")

    def get_daily_candles(self, symbol: str, from_date: date, to_date: date) -> list[OHLCVBar]:
        raise NotImplementedError("Implemented in issue #3")

    def get_quote(self, symbol: str) -> Quote | None:
        raise NotImplementedError("Implemented in issue #3")
