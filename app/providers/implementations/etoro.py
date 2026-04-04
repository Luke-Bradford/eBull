"""
eToro market data provider.

Implements MarketDataProvider against the eToro read API.
Persists raw API responses before any normalisation.
"""

import json
import logging
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import TracebackType

import httpx

from app.providers.market_data import InstrumentRecord, MarketDataProvider, OHLCVBar, Quote

logger = logging.getLogger(__name__)

_ETORO_BASE_URL = "https://api.etoro.com"

# Directory for raw payload dumps (relative to process working directory)
_RAW_PAYLOAD_DIR = Path("data/raw/etoro")


def _persist_raw(tag: str, payload: object) -> None:
    """Write raw API response to disk before normalisation."""
    try:
        _RAW_PAYLOAD_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        path = _RAW_PAYLOAD_DIR / f"{tag}_{ts}.json"
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        # Never let persistence failure block the sync
        logger.warning("Failed to persist raw payload for tag=%s", tag, exc_info=True)


class EtoroMarketDataProvider(MarketDataProvider):
    """
    Reads tradable instruments, candles, and quotes from the eToro API.

    Requires ETORO_READ_API_KEY. Raw responses are persisted to
    data/raw/etoro/ before normalisation.

    Use as a context manager to ensure the HTTP client is closed:

        with EtoroMarketDataProvider(api_key=...) as provider:
            bars = provider.get_daily_candles("AAPL", from_date, to_date)
    """

    def __init__(self, api_key: str, env: str = "demo") -> None:
        self._api_key = api_key
        self._client = httpx.Client(
            base_url=_ETORO_BASE_URL,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

    def __enter__(self) -> "EtoroMarketDataProvider":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._client.close()

    # ------------------------------------------------------------------
    # Universe
    # ------------------------------------------------------------------

    def get_tradable_instruments(self) -> list[InstrumentRecord]:
        """
        Fetch the full list of tradable instruments from eToro.

        Raw response is persisted before normalisation.
        Note: pagination is not yet implemented — single request only.
        The eToro API pagination shape will be confirmed in live testing.
        """
        response = self._client.get("/v1/instruments")
        response.raise_for_status()
        raw = response.json()
        _persist_raw("instruments", raw)
        return _normalise_instruments(raw)

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    def get_daily_candles(self, symbol: str, from_date: date, to_date: date) -> list[OHLCVBar]:
        """
        Fetch daily OHLCV candles for a symbol over the requested date range.

        Raw response is persisted before normalisation.
        """
        response = self._client.get(
            "/v1/candles/day",
            params={
                "symbol": symbol,
                "from": from_date.isoformat(),
                "to": to_date.isoformat(),
            },
        )
        response.raise_for_status()
        raw = response.json()
        _persist_raw(f"candles_{symbol}", raw)
        return _normalise_candles(symbol, raw)

    def get_quote(self, symbol: str) -> Quote | None:
        """
        Return the current quote for a symbol.
        Returns None if the symbol is not recognised or not currently quoted.
        """
        response = self._client.get("/v1/quotes", params={"symbol": symbol})
        if response.status_code == 404:
            return None
        response.raise_for_status()
        raw = response.json()
        _persist_raw(f"quote_{symbol}", raw)
        return _normalise_quote(symbol, raw)


# ------------------------------------------------------------------
# Normalisers — pure functions, no I/O, unit tested with fixture data
# ------------------------------------------------------------------


def _normalise_instruments(raw: object) -> list[InstrumentRecord]:
    """
    Normalise a raw eToro instruments API response into InstrumentRecord list.

    Accepts both camelCase (InstrumentDisplayDatas) and snake_case (instruments)
    shapes while the exact live API shape is confirmed.
    """
    if not isinstance(raw, dict):
        raise ValueError(f"Expected dict from eToro instruments endpoint, got {type(raw)}")

    items: list[object] = raw.get("InstrumentDisplayDatas") or raw.get("instruments") or []

    records = []
    for item in items:
        if not isinstance(item, dict):
            continue
        record = _normalise_instrument(item)
        if record is not None:
            records.append(record)
    return records


def _normalise_instrument(item: Mapping[str, object]) -> InstrumentRecord | None:
    """
    Map a single eToro instrument dict to an InstrumentRecord.
    Returns None and logs a warning if required fields are missing.
    """
    instrument_id = item.get("InstrumentID") or item.get("instrumentId")
    symbol = item.get("SymbolFull") or item.get("symbol")

    if not instrument_id or not symbol:
        logger.warning("Skipping instrument missing ID or symbol: %s", item)
        return None

    return InstrumentRecord(
        provider_id=str(instrument_id),
        symbol=str(symbol),
        company_name=str(item.get("InstrumentDisplayName") or item.get("name") or symbol),
        exchange=_str_or_none(item.get("ExchangeID") or item.get("exchange")),
        currency=str(item.get("PriceSource") or item.get("currency") or "USD"),
        sector=_str_or_none(item.get("Sector") or item.get("sector")),
        industry=_str_or_none(item.get("Industry") or item.get("industry")),
        country=_str_or_none(item.get("Country") or item.get("country")),
        is_tradable=bool(item.get("IsActive") if "IsActive" in item else item.get("is_active", True)),
    )


def _normalise_candles(symbol: str, raw: object) -> list[OHLCVBar]:
    """
    Normalise a raw eToro candles API response into OHLCVBar list.

    Accepts both camelCase (Candles) and snake_case (candles) shapes.
    Bars with missing OHLC data are skipped with a warning.
    """
    if not isinstance(raw, dict):
        raise ValueError(f"Expected dict from eToro candles endpoint, got {type(raw)}")

    items: list[object] = raw.get("Candles") or raw.get("candles") or []

    bars = []
    for item in items:
        if not isinstance(item, dict):
            continue
        bar = _normalise_candle(symbol, item)
        if bar is not None:
            bars.append(bar)

    # Return oldest-first so callers can compute rolling windows in order
    bars.sort(key=lambda b: b.price_date)
    return bars


def _normalise_candle(symbol: str, item: Mapping[str, object]) -> OHLCVBar | None:
    """
    Map a single eToro candle dict to an OHLCVBar.
    Returns None if any required OHLC field is missing.
    """
    raw_date = item["Date"] if "Date" in item else item.get("date")
    raw_open = item["Open"] if "Open" in item else item.get("open")
    raw_high = item["High"] if "High" in item else item.get("high")
    raw_low = item["Low"] if "Low" in item else item.get("low")
    raw_close = item["Close"] if "Close" in item else item.get("close")

    if not all([raw_date, raw_open, raw_high, raw_low, raw_close]):
        logger.warning("Skipping candle missing required fields for %s: %s", symbol, item)
        return None

    try:
        price_date = date.fromisoformat(str(raw_date)[:10])
        return OHLCVBar(
            symbol=symbol,
            price_date=price_date,
            open=Decimal(str(raw_open)),
            high=Decimal(str(raw_high)),
            low=Decimal(str(raw_low)),
            close=Decimal(str(raw_close)),
            volume=_int_or_none(item.get("Volume") or item.get("volume")),
        )
    except (ValueError, ArithmeticError) as exc:
        logger.warning("Skipping malformed candle for %s: %s — %s", symbol, item, exc)
        return None


def _normalise_quote(symbol: str, raw: object) -> Quote | None:
    """
    Normalise a raw eToro quote response into a Quote.
    Returns None if bid or ask is missing.
    """
    if not isinstance(raw, dict):
        return None

    # Unwrap single-item list if needed: {"quotes": [{...}]}
    data: object = raw
    if "quotes" in raw and isinstance(raw["quotes"], list) and raw["quotes"]:
        data = raw["quotes"][0]
    elif "Quote" in raw:
        data = raw["Quote"]

    if not isinstance(data, dict):
        return None

    raw_bid = data["Bid"] if "Bid" in data else data.get("bid")
    raw_ask = data["Ask"] if "Ask" in data else data.get("ask")

    if not raw_bid or not raw_ask:
        logger.warning("Quote for %s missing bid or ask: %s", symbol, raw)
        return None

    raw_ts = data["Time"] if "Time" in data else (data["time"] if "time" in data else data.get("timestamp"))
    if raw_ts:
        try:
            quoted_at = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
        except ValueError:
            quoted_at = datetime.now(UTC)
    else:
        quoted_at = datetime.now(UTC)

    raw_last = data["Last"] if "Last" in data else data.get("last")

    return Quote(
        symbol=symbol,
        timestamp=quoted_at,
        bid=Decimal(str(raw_bid)),
        ask=Decimal(str(raw_ask)),
        last=Decimal(str(raw_last)) if raw_last else None,
    )


def _str_or_none(value: object) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _int_or_none(value: object) -> int | None:
    """Convert a raw API value to int, returning None for zero or missing."""
    if value is None:
        return None
    try:
        result = int(float(str(value)))
        return result if result != 0 else None
    except (ValueError, ArithmeticError):
        return None
