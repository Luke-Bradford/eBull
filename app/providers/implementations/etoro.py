"""
eToro market data provider.

Implements MarketDataProvider against the real eToro public API.
Raw API response disk dumps were retired in #471 — every structured
field lands in SQL (``instruments``, ``price_daily``, ``quotes``,
``exchanges``), and those tables are the audit trail (see
``docs/review-prevention-log.md`` §"Raw payload persistence" for
the scope-narrowed rule).

Auth: three-header scheme (x-api-key, x-user-key, x-request-id).
Base URL: https://public-api.etoro.com (configurable via settings.etoro_base_url).
"""

import logging
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from types import TracebackType
from uuid import uuid4

import httpx

from app.config import settings
from app.providers.market_data import (
    ExchangeRecord,
    InstrumentRecord,
    InstrumentTypeRecord,
    MarketDataProvider,
    OHLCVBar,
    Quote,
    StocksIndustryRecord,
)
from app.providers.resilient_client import ResilientClient

logger = logging.getLogger(__name__)

# eToro rates endpoint accepts at most 100 instrument IDs per request
# (OpenAPI spec maxItems: 100).  We use 50 to reduce blast radius when
# eToro returns 500 on a chunk containing a problematic ID.
_RATES_BATCH_SIZE = 50

# eToro rate limit: 60 GET requests per minute (rolling window).
# 1.1s inter-request interval ≈ 55 req/min — ~8% headroom.
_ETORO_READ_INTERVAL_S = 1.1


class EtoroMarketDataProvider(MarketDataProvider):
    """
    Reads tradable instruments, candles, quotes, and the exchange
    catalogue from the eToro API.

    Callers must supply both ``api_key`` and ``user_key`` (loaded from
    the encrypted broker_credentials store). Raw response disk dumps
    were retired in #471 — every structured field now lands in SQL
    (``instruments``, ``price_daily``, ``quotes``, ``exchanges``), so
    the structured tables ARE the audit trail (see
    ``docs/review-prevention-log.md`` §"Raw payload persistence",
    scope-narrowed entry).

    Use as a context manager to ensure the HTTP client is closed:

        with EtoroMarketDataProvider(api_key=..., user_key=...) as provider:
            bars = provider.get_daily_candles(12345, lookback_days=400)
    """

    def __init__(self, api_key: str, user_key: str, env: str = "demo") -> None:
        self._api_key = api_key
        self._user_key = user_key
        self._env = env
        self._client = httpx.Client(
            base_url=settings.etoro_base_url,
            headers={
                "x-api-key": self._api_key,
                "x-user-key": self._user_key,
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )
        self._http = ResilientClient(
            self._client,
            min_request_interval_s=_ETORO_READ_INTERVAL_S,
        )

    def __enter__(self) -> EtoroMarketDataProvider:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._client.close()

    def _request_headers(self) -> dict[str, str]:
        """Per-request headers — fresh UUID for x-request-id."""
        return {"x-request-id": str(uuid4())}

    # ------------------------------------------------------------------
    # Universe
    # ------------------------------------------------------------------

    def get_tradable_instruments(self) -> list[InstrumentRecord]:
        """Fetch the full list of tradable instruments from eToro."""
        response = self._http.get(
            "/api/v1/market-data/instruments",
            headers=self._request_headers(),
        )
        response.raise_for_status()
        raw = response.json()
        return _normalise_instruments(raw)

    def get_instrument_types(self) -> list[InstrumentTypeRecord]:
        """Fetch eToro's instrument-types lookup catalogue.

        Maps numeric ``instrumentTypeID`` (Forex / Commodity / CFD
        / Stocks / ETF / Bonds / …) to a human-readable
        description. Used by ``app.services.etoro_lookups.refresh_etoro_lookups``
        to populate the ``etoro_instrument_types`` table; the
        frontend joins on it to render meaningful labels instead
        of numeric ids.
        """
        response = self._http.get(
            "/api/v1/market-data/instrument-types",
            headers=self._request_headers(),
        )
        response.raise_for_status()
        return _normalise_instrument_types(response.json())

    def get_stocks_industries(self) -> list[StocksIndustryRecord]:
        """Fetch eToro's stocks-industries lookup catalogue.

        Maps numeric ``industryID`` to industry name (Basic
        Materials / Healthcare / Technology / …). Same role as
        ``get_instrument_types`` for the sector label.
        """
        response = self._http.get(
            "/api/v1/market-data/stocks-industries",
            headers=self._request_headers(),
        )
        response.raise_for_status()
        return _normalise_stocks_industries(response.json())

    def get_exchanges(self) -> list[ExchangeRecord]:
        """Fetch the eToro exchange catalogue.

        Returns every ``exchangeId`` eToro tags instruments with, plus
        the human-readable description (e.g. ``London Stock Exchange``).
        Used by ``app.services.exchanges.refresh_exchanges_metadata`` to
        populate ``exchanges.description``; ``country`` and
        ``asset_class`` stay operator-curated and untouched.
        """
        response = self._http.get(
            "/api/v1/market-data/exchanges",
            headers=self._request_headers(),
        )
        response.raise_for_status()
        raw = response.json()
        return _normalise_exchanges(raw)

    # ------------------------------------------------------------------
    # Candles
    # ------------------------------------------------------------------

    def get_daily_candles(self, instrument_id: int, lookback_days: int) -> list[OHLCVBar]:
        """Fetch daily OHLCV candles for an instrument.

        Uses ``asc`` direction so the API returns oldest-first, matching
        the interface contract. No client-side re-sort needed.
        """
        response = self._http.get(
            f"/api/v1/market-data/instruments/{instrument_id}/history/candles/asc/OneDay/{lookback_days}",
            headers=self._request_headers(),
        )
        response.raise_for_status()
        raw = response.json()
        return _normalise_candles(raw)

    # ------------------------------------------------------------------
    # Quotes
    # ------------------------------------------------------------------

    def get_quote(self, instrument_id: int) -> Quote | None:
        """Return the current quote for a single instrument."""
        quotes = self.get_quotes([instrument_id])
        quote_map = {q.instrument_id: q for q in quotes}
        return quote_map.get(instrument_id)

    def get_quotes(self, instrument_ids: list[int]) -> list[Quote]:
        """Batch quote fetch with automatic chunking.

        The eToro rates endpoint accepts up to 100 instrument IDs per
        request (OpenAPI ``maxItems: 100``).  We chunk at 50 to reduce
        blast radius.  If a chunk fails after retries, the error is
        logged and the remaining chunks continue — partial results are
        returned rather than failing the entire batch.
        """
        if not instrument_ids:
            return []

        all_quotes: list[Quote] = []
        failed_chunks = 0
        total_chunks = (len(instrument_ids) + _RATES_BATCH_SIZE - 1) // _RATES_BATCH_SIZE

        for batch_num, i in enumerate(range(0, len(instrument_ids), _RATES_BATCH_SIZE)):
            chunk = instrument_ids[i : i + _RATES_BATCH_SIZE]
            ids_param = ",".join(str(id_) for id_ in chunk)
            try:
                # Build the query string inline instead of via params={}
                # so the comma in "1181,1699" is not percent-encoded.
                # httpx encodes commas as %2C which eToro rejects with 500.
                response = self._http.get(
                    f"/api/v1/market-data/instruments/rates?instrumentIds={ids_param}",
                    headers=self._request_headers(),
                )
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                # #471: error body no longer persisted to disk per the
                # SQL-coverage-replaces-raw rule (#470). Status + body
                # snippet captured in the log line via exc_info so the
                # diagnostic survives without a separate disk file.
                logger.warning(
                    "Rates chunk %d failed (%d IDs, status %d, body=%r), skipping",
                    batch_num,
                    len(chunk),
                    exc.response.status_code,
                    exc.response.text[:500],
                    exc_info=True,
                )
                failed_chunks += 1
                continue
            except httpx.RequestError:
                # Network-level failure (timeout, connection reset) — no response to persist.
                logger.warning(
                    "Rates chunk %d network error (%d IDs), skipping",
                    batch_num,
                    len(chunk),
                    exc_info=True,
                )
                failed_chunks += 1
                continue
            raw = response.json()
            all_quotes.extend(_normalise_rates(raw))

        if failed_chunks:
            logger.warning(
                "Rates fetch: %d/%d chunks failed, returning %d partial quotes",
                failed_chunks,
                total_chunks,
                len(all_quotes),
            )

        return all_quotes


# ------------------------------------------------------------------
# Normalisers — pure functions, no I/O, unit tested with fixture data
# ------------------------------------------------------------------


def _normalise_instruments(raw: object) -> list[InstrumentRecord]:
    """Normalise a raw eToro instruments API response into InstrumentRecord list.

    Real API returns ``{ instrumentDisplayDatas: [...] }``.
    """
    if not isinstance(raw, dict):
        raise ValueError(f"Expected dict from eToro instruments endpoint, got {type(raw)}")

    items: list[object] = raw.get("instrumentDisplayDatas") or []

    records = []
    for item in items:
        if not isinstance(item, dict):
            continue
        record = _normalise_instrument(item)
        if record is not None:
            records.append(record)
    return records


def _normalise_instrument(item: Mapping[str, object]) -> InstrumentRecord | None:
    """Map a single eToro instrument dict to an InstrumentRecord.

    Returns None and logs a warning if required fields are missing or
    if ``isInternalInstrument`` is True.
    """
    # Skip internal instruments (restricted from public access)
    if item.get("isInternalInstrument") is True:
        return None

    instrument_id = item.get("instrumentID")
    symbol = item.get("symbolFull")

    if not instrument_id or not symbol:
        logger.warning("Skipping instrument missing ID or symbol: %s", item)
        return None

    return InstrumentRecord(
        provider_id=str(instrument_id),
        symbol=str(symbol),
        company_name=str(item.get("instrumentDisplayName") or symbol),
        exchange=_str_or_none(item.get("exchangeID")),
        # eToro instruments endpoint does not expose currency.
        # Return None so enrichment (FMP profile) fills the real value.
        # COALESCE upsert in universe.py preserves enriched currency.
        currency=None,
        sector=_str_or_none(item.get("stocksIndustryId")),
        industry=None,  # secondary lookup deferred
        country=None,  # not available in instruments endpoint
        is_tradable=True,  # only tradable instruments are returned by the API
        instrument_type=_str_or_none(item.get("instrumentTypeName")),
        instrument_type_id=_int_or_none(item.get("instrumentTypeID")),
    )


def _normalise_candles(raw: object) -> list[OHLCVBar]:
    """Normalise a raw eToro candles API response into OHLCVBar list.

    Real API returns ``{ candles: [{ instrumentId, candles: [...] }] }``.
    The outer list has one element per requested instrument; we flatten
    the inner candle arrays.

    The endpoint is called with ``asc`` direction, so bars arrive
    oldest-first and no re-sort is needed.
    """
    if not isinstance(raw, dict):
        raise ValueError(f"Expected dict from eToro candles endpoint, got {type(raw)}")

    outer: list[object] = raw.get("candles") or []

    bars: list[OHLCVBar] = []
    for group in outer:
        if not isinstance(group, dict):
            continue
        inner: list[object] = group.get("candles") or []
        for item in inner:
            if not isinstance(item, dict):
                continue
            bar = _normalise_candle(item)
            if bar is not None:
                bars.append(bar)

    return bars


def _normalise_candle(item: Mapping[str, object]) -> OHLCVBar | None:
    """Map a single eToro candle dict to an OHLCVBar.

    Returns None if any required OHLC field is missing.
    """
    raw_date = item.get("fromDate")
    raw_open = item.get("open")
    raw_high = item.get("high")
    raw_low = item.get("low")
    raw_close = item.get("close")

    if any(v is None or v == "" for v in (raw_date, raw_open, raw_high, raw_low, raw_close)):
        logger.warning("Skipping candle missing required fields: %s", item)
        return None

    try:
        price_date = date.fromisoformat(str(raw_date)[:10])
        return OHLCVBar(
            price_date=price_date,
            open=Decimal(str(raw_open)),
            high=Decimal(str(raw_high)),
            low=Decimal(str(raw_low)),
            close=Decimal(str(raw_close)),
            volume=_int_or_none(item.get("volume")),
        )
    except (ValueError, ArithmeticError) as exc:
        logger.warning("Skipping malformed candle: %s — %s", item, exc)
        return None


def _normalise_rates(raw: object) -> list[Quote]:
    """Normalise a raw eToro rates API response into Quote list.

    Real API returns ``{ rates: [...] }``.
    """
    if not isinstance(raw, dict):
        raise ValueError(f"Expected dict from eToro rates endpoint, got {type(raw)}")

    items: list[object] = raw.get("rates") or []

    quotes: list[Quote] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        quote = _normalise_rate(item)
        if quote is not None:
            quotes.append(quote)
    return quotes


def _normalise_rate(item: Mapping[str, object]) -> Quote | None:
    """Map a single eToro rate dict to a Quote.

    Returns None if instrument ID or bid/ask is missing or non-positive.
    """
    instrument_id = item.get("instrumentID")
    if instrument_id is None:
        logger.warning("Skipping rate missing instrumentID: %s", item)
        return None

    raw_bid = item.get("bid")
    raw_ask = item.get("ask")

    if raw_bid is None or raw_ask is None:
        logger.warning("Skipping rate missing bid/ask for instrument %s: %s", instrument_id, item)
        return None

    bid = Decimal(str(raw_bid))
    ask = Decimal(str(raw_ask))

    if bid <= 0 or ask <= 0:
        logger.warning("Rate for instrument %s has non-positive bid/ask: %s", instrument_id, item)
        return None

    raw_ts = item.get("date")
    if raw_ts:
        try:
            quoted_at = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
        except ValueError:
            quoted_at = datetime.now(UTC)
    else:
        quoted_at = datetime.now(UTC)

    raw_last = item.get("lastExecution")

    # Extract instrument-currency → account-currency conversion rate.
    # eToro returns conversionRateAsk / conversionRateBid on every rate dict;
    # the mid gives a usable FX rate for display-currency conversion.
    conversion_rate: Decimal | None = None
    raw_conv_ask = item.get("conversionRateAsk")
    raw_conv_bid = item.get("conversionRateBid")
    if raw_conv_ask is not None and raw_conv_bid is not None:
        try:
            conv_ask = Decimal(str(raw_conv_ask))
            conv_bid = Decimal(str(raw_conv_bid))
            if conv_ask > 0 and conv_bid > 0:
                conversion_rate = (conv_ask + conv_bid) / 2
        except Exception:
            logger.debug("Failed to parse conversion rate for instrument %s", instrument_id)

    return Quote(
        instrument_id=int(str(instrument_id)),
        timestamp=quoted_at,
        bid=bid,
        ask=ask,
        last=Decimal(str(raw_last)) if raw_last is not None else None,
        conversion_rate=conversion_rate,
    )


_EXCHANGES_WRAPPER_KEY = "exchangeInfo"
_INSTRUMENT_TYPES_WRAPPER_KEY = "instrumentTypes"
_STOCKS_INDUSTRIES_WRAPPER_KEY = "stocksIndustries"


def _unwrap_lookup(raw: object, wrapper_key: str) -> list[object]:
    """Shared shape-validator for the lookup endpoints.

    eToro's lookup endpoints (``exchanges`` / ``instrument-types``
    / ``stocks-industries``) all wrap a list under a single known
    key. Bare-list fallback accepted in case eToro aligns the
    live API with their portal docs in the future. Anything else
    raises so a silent schema drift fails the cron run loudly
    rather than reporting an empty feed.
    """
    if isinstance(raw, dict):
        wrapped = raw.get(wrapper_key)
        if not isinstance(wrapped, list):
            raise ValueError(
                f"eToro lookup endpoint returned a dict, but key {wrapper_key!r} "
                f"is missing or not a list. Top-level keys: {list(raw.keys())}. "
                f"If eToro renamed the wrapper key, update the lookup normaliser."
            )
        return wrapped
    if isinstance(raw, list):
        return list(raw)
    raise ValueError(
        f"Expected dict (with {wrapper_key!r} key) or list from eToro lookup endpoint, got {type(raw).__name__}."
    )


def _normalise_instrument_types(raw: object) -> list[InstrumentTypeRecord]:
    """Normalise an eToro instrument-types response into typed records."""
    items = _unwrap_lookup(raw, _INSTRUMENT_TYPES_WRAPPER_KEY)
    records: list[InstrumentTypeRecord] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        type_id = item.get("instrumentTypeID")
        if type_id is None:
            continue
        try:
            type_id_int = int(type_id)
        except TypeError, ValueError:
            continue
        records.append(
            InstrumentTypeRecord(
                type_id=type_id_int,
                description=_str_or_none(item.get("instrumentTypeDescription")),
            )
        )
    return records


def _normalise_stocks_industries(raw: object) -> list[StocksIndustryRecord]:
    """Normalise an eToro stocks-industries response into typed records."""
    items = _unwrap_lookup(raw, _STOCKS_INDUSTRIES_WRAPPER_KEY)
    records: list[StocksIndustryRecord] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        industry_id = item.get("industryID")
        if industry_id is None:
            continue
        try:
            industry_id_int = int(industry_id)
        except TypeError, ValueError:
            continue
        records.append(
            StocksIndustryRecord(
                industry_id=industry_id_int,
                name=_str_or_none(item.get("industryName")),
            )
        )
    return records


def _normalise_exchanges(raw: object) -> list[ExchangeRecord]:
    """Normalise an eToro exchanges API response into ExchangeRecord list.

    The live API wraps the list in ``{"exchangeInfo": [...]}`` even
    though the portal docs show a bare list. ``_unwrap_lookup``
    pins the shape — anything else raises ``ValueError`` so a
    silent schema drift fails loudly rather than parsing the
    wrong list and reporting a harmless-looking empty feed.
    """
    items = _unwrap_lookup(raw, _EXCHANGES_WRAPPER_KEY)
    records: list[ExchangeRecord] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        provider_id = item.get("exchangeID") or item.get("exchangeId")
        if provider_id is None:
            continue
        records.append(
            ExchangeRecord(
                provider_id=str(provider_id),
                description=_str_or_none(item.get("exchangeDescription")),
            )
        )
    return records


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
    except ValueError, ArithmeticError:
        return None
