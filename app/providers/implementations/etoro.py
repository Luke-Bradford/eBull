"""
eToro market data provider.

Implements MarketDataProvider against the eToro read API.
Persists raw API responses before any normalisation.

Full candle/quote implementation is built in issue #3 (market data).
"""

import json
import logging
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from types import TracebackType

import httpx

from app.providers.market_data import InstrumentRecord, MarketDataProvider, OHLCVBar, Quote

logger = logging.getLogger(__name__)

_ETORO_BASE_URL = "https://api.etoro.com"

# Directory for raw payload dumps (relative to project root)
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

    Use as a context manager to ensure the underlying HTTP client is closed:

        with EtoroMarketDataProvider(api_key=..., env=...) as provider:
            records = provider.get_tradable_instruments()
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

    def get_tradable_instruments(self) -> list[InstrumentRecord]:
        """
        Fetch the full list of tradable instruments from eToro.

        Raw response is persisted before normalisation.
        Note: pagination is not yet implemented — single request only.
        The eToro API pagination shape will be confirmed in live testing
        and a pagination loop added at that point.
        """
        response = self._client.get("/v1/instruments")
        response.raise_for_status()
        raw = response.json()

        _persist_raw("instruments", raw)

        return _normalise_instruments(raw)

    def get_daily_candles(self, symbol: str, from_date: date, to_date: date) -> list[OHLCVBar]:
        raise NotImplementedError("Implemented in issue #3")

    def get_quote(self, symbol: str) -> Quote | None:
        raise NotImplementedError("Implemented in issue #3")


def _normalise_instruments(raw: object) -> list[InstrumentRecord]:
    """
    Normalise a raw eToro instruments API response into InstrumentRecord list.

    eToro returns instruments under a top-level key; the exact shape will be
    confirmed against the live API in issue #2 testing and this function
    updated accordingly. The normaliser is kept separate so it can be unit
    tested with fixture data without making network calls.
    """
    if not isinstance(raw, dict):
        raise ValueError(f"Expected dict from eToro instruments endpoint, got {type(raw)}")

    # eToro API shape: {"InstrumentDisplayDatas": [...]} or {"instruments": [...]}
    # Accept both while the exact shape is confirmed against live API
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


def _str_or_none(value: object) -> str | None:
    if value is None or value == "":
        return None
    return str(value)
