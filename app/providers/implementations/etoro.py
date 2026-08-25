"""
eToro market data provider.

Implements MarketDataProvider against the real eToro public API.
Raw API response disk dumps were retired in #471. Durable structured
fields land in SQL (``instruments``, ``price_daily``, ``quotes``,
``exchanges``), and those tables are the audit trail outside the two bounded
ephemeral cases below (see
``docs/review-prevention-log.md`` §"Raw payload persistence" for
the scope-narrowed rule).

**Bounded ephemeral carve-outs.** ``get_intraday_candles`` returns ephemeral
chart-UI data — it does not
drive scoring, thesis, recommendations, orders, dividends, or tax,
so the SQL-as-audit-trail invariant does not apply. The pass-through
is gated by a TTL cache (``app/services/intraday_candles.py``) and
the API endpoint is auth-gated to keep external quota traceable.
Persisting intraday rows would expand the audit / sync surface
without analytical value; the no-persistence design is locked at
epic #585 and reviewed by Codex pre-implementation.

``get_broad_market_snapshot`` is the second narrow exception (#2523). It is a
two-page, collection-time screening cross-section with no per-row source
timestamp or bid/ask. Routine rows are never evidence and are not persisted;
only aggregate coverage and the existing compact fired/refused decision context
may survive. A shortlisted instrument still requires a timestamped quote. Any
additional exception requires reopening this design.

Auth: three-header scheme (x-api-key, x-user-key, x-request-id).
Base URL: https://public-api.etoro.com (configurable via settings.etoro_base_url).
"""

import logging
import math
import threading
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from types import TracebackType
from typing import Final
from uuid import uuid4

import httpx

from app.config import settings
from app.providers.market_data import (
    BroadMarketSnapshot,
    ExchangeRecord,
    InstrumentRecord,
    InstrumentTypeRecord,
    IntradayBar,
    IntradayInterval,
    MarketDataProvider,
    MarketSnapshotInstrument,
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

# eToro's market-data endpoints share 120 requests per rolling minute.  Keep
# the older conservative pacing here because other processes use the same user
# key and ResilientClient's gate is process-local, not account-global.
_ETORO_READ_INTERVAL_S = 1.1
# #2934 — market-data endpoints share one upstream request budget.  Quote
# observations now run concurrently with the long candle sweep, so every
# provider instance in this process must coordinate the same atomic clock.
# This is deliberately process-local: the conservative 1.1s floor retains
# headroom for the API process and other clients using the same account key.
_ETORO_RATE_LIMIT_CLOCK: Final[list[float]] = [0.0]
_ETORO_RATE_LIMIT_LOCK: Final[threading.Lock] = threading.Lock()

_SEARCH_PAGE_SIZE = 10_000
_SEARCH_MAX_PAGES = 2
_SEARCH_FIELDS = (
    "instrumentId,currentRate,dailyPriceChange,weeklyPriceChange,"
    "monthlyPriceChange,isCurrentlyTradable,isExchangeOpen,"
    "isActiveInPlatform,isBuyEnabled,internalIndustryId,sectorNameId,"
    "popularityUniques7Day,traders7DayChange,buyHoldingPct,sellHoldingPct"
)


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

    #: One upstream rates request per this many ids — see the base class.
    #: Callers that need get_quotes to be all-or-nothing split by this.
    quote_batch_size = _RATES_BATCH_SIZE

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
            shared_last_request=_ETORO_RATE_LIMIT_CLOCK,
            shared_throttle_lock=_ETORO_RATE_LIMIT_LOCK,
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

    def get_broad_market_snapshot(self) -> BroadMarketSnapshot:
        """Fetch eToro's projected search catalogue in complete pages.

        Live verification on 2026-08-12 found that the documented
        ``pageNumber`` parameter is ignored while ``page`` paginates.  The
        response's reported page and total are therefore checked on every
        request.  Search rows have no source timestamp or bid/ask and remain
        screening-only; callers join exact local IDs and confirm shortlisted
        candidates through ``get_quotes``.

        The result is intentionally not persisted wholesale.  Strategy code
        may retain aggregate coverage plus the compact context of a genuinely
        fired/refused candidate, avoiding a second quote/indicator warehouse.
        """
        observed_from = datetime.now(UTC)
        expected_total: int | None = None
        raw_item_count = 0
        discarded_items = 0
        records: list[MarketSnapshotInstrument] = []
        seen_ids: set[int] = set()
        page = 1

        while True:
            response = self._http.get(
                "/api/v1/market-data/search",
                params={
                    "fields": _SEARCH_FIELDS,
                    "pageSize": _SEARCH_PAGE_SIZE,
                    "page": page,
                },
                headers=self._request_headers(),
            )
            response.raise_for_status()
            raw = response.json()
            if not isinstance(raw, dict):
                raise ValueError(f"Expected dict from eToro search endpoint, got {type(raw)}")

            reported_page = raw.get("page")
            total_items = raw.get("totalItems")
            items = raw.get("items")
            if reported_page != page:
                raise ValueError(f"eToro search returned page {reported_page!r}, expected {page}")
            if not isinstance(total_items, int) or total_items < 0:
                raise ValueError(f"eToro search returned invalid totalItems {total_items!r}")
            if not isinstance(items, list):
                raise ValueError("eToro search response items must be a list")
            if expected_total is None:
                expected_total = total_items
            elif total_items != expected_total:
                raise ValueError(
                    f"eToro search totalItems changed during pagination: {expected_total} -> {total_items}"
                )

            raw_item_count += len(items)
            for item in items:
                record = _normalise_market_snapshot_instrument(item)
                if record is None:
                    discarded_items += 1
                    continue
                if record.instrument_id in seen_ids:
                    raise ValueError(f"eToro search repeated instrumentId {record.instrument_id}")
                seen_ids.add(record.instrument_id)
                records.append(record)

            pages = max(1, math.ceil(total_items / _SEARCH_PAGE_SIZE))
            if pages > _SEARCH_MAX_PAGES:
                raise ValueError(
                    f"eToro search reported {total_items} rows across {pages} pages; "
                    f"bounded adapter permits {_SEARCH_MAX_PAGES}"
                )
            if page >= pages:
                break
            page += 1

        if expected_total is None:  # pragma: no cover - first response always assigns
            raise RuntimeError("eToro search pagination did not initialise")
        if raw_item_count != expected_total:
            raise ValueError(f"eToro search pagination incomplete: received {raw_item_count} of {expected_total} rows")
        return BroadMarketSnapshot(
            observed_from=observed_from,
            observed_to=datetime.now(UTC),
            reported_total_items=expected_total,
            discarded_items=discarded_items,
            instruments=tuple(records),
        )

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

    def get_intraday_candles(
        self,
        instrument_id: int,
        interval: IntradayInterval,
        count: int,
    ) -> list[IntradayBar]:
        """Fetch intraday OHLCV bars at the requested interval.

        Same URL family as ``get_daily_candles`` but the interval slot
        is variable. eToro caps ``count`` at 1000 bars per request;
        callers in eBull stay well under that for chart use (≤600).

        Raw response shape mirrors the daily endpoint exactly — only
        the bar timestamp granularity differs. We use a sibling
        normaliser (``_normalise_intraday_candles``) that preserves the
        time component instead of truncating to a date.
        """
        response = self._http.get(
            f"/api/v1/market-data/instruments/{instrument_id}/history/candles/asc/{interval}/{count}",
            headers=self._request_headers(),
        )
        response.raise_for_status()
        raw = response.json()
        return _normalise_intraday_candles(raw)

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
        # Retained so an all-chunks-failed batch can re-raise the real cause
        # (and keep its FailureCategory) instead of returning a silent [].
        last_exc: Exception | None = None
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
                last_exc = exc
                continue
            except httpx.RequestError as exc:
                # Network-level failure (timeout, connection reset) — no response to persist.
                logger.warning(
                    "Rates chunk %d network error (%d IDs), skipping",
                    batch_num,
                    len(chunk),
                    exc_info=True,
                )
                failed_chunks += 1
                last_exc = exc
                continue
            raw = response.json()
            all_quotes.extend(_normalise_rates(raw))

        if last_exc is not None and failed_chunks == total_chunks:
            # EVERY chunk failed — that is an outage, not "these instruments
            # have no quotes", and downstream the two are indistinguishable
            # because both produce an empty list (#2271, Codex). Partial
            # failure still returns partial results, per the docstring above;
            # only TOTAL failure raises, so a caller cannot report a clean
            # no-op run while every headless reader sits on stale marks
            # (the #2218 "job reports success having done nothing" shape).
            #
            # Re-raise the original exception rather than a bespoke provider
            # error: ``classify_exception`` keys off the httpx type, so this
            # preserves AUTH_EXPIRED (401/403) / RATE_LIMITED (429) /
            # SOURCE_DOWN (5xx, transport) instead of flattening an
            # operator-actionable outage to INTERNAL_ERROR.
            logger.warning(
                "Rates fetch: ALL %d chunk(s) failed (%d instrument IDs) — re-raising the last error",
                total_chunks,
                len(instrument_ids),
            )
            raise last_exc

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
        # eToro instruments endpoint does not expose currency. Left None:
        # universe.py derives it from the operator-curated exchanges.currency
        # via the exchange join (same as country) — sql/159, #1431.
        currency=None,
        # Key is capital-ID like every sibling (instrumentID, exchangeID);
        # 0 is eToro's FX/commodity "no industry" sentinel → None (#1598).
        sector=_str_or_none(_int_or_none(item.get("stocksIndustryID"))),
        industry=None,  # secondary lookup deferred
        country=None,  # not available in instruments endpoint
        is_tradable=True,  # only tradable instruments are returned by the API
        instrument_type_id=_int_or_none(item.get("instrumentTypeID")),
    )


def _normalise_market_snapshot_instrument(item: object) -> MarketSnapshotInstrument | None:
    """Normalise one projected search row without inventing absent values."""
    if not isinstance(item, Mapping):
        return None
    instrument_id = _positive_int_or_none(item.get("instrumentId"))
    if instrument_id is None:
        return None
    current_rate = _decimal_or_none(item.get("currentRate"))
    if current_rate is not None and current_rate <= 0:
        current_rate = None
    return MarketSnapshotInstrument(
        instrument_id=instrument_id,
        current_rate=current_rate,
        daily_price_change_pct=_decimal_or_none(item.get("dailyPriceChange")),
        weekly_price_change_pct=_decimal_or_none(item.get("weeklyPriceChange")),
        monthly_price_change_pct=_decimal_or_none(item.get("monthlyPriceChange")),
        is_currently_tradable=_bool_or_none(item.get("isCurrentlyTradable")),
        is_exchange_open=_bool_or_none(item.get("isExchangeOpen")),
        is_active_in_platform=_bool_or_none(item.get("isActiveInPlatform")),
        is_buy_enabled=_bool_or_none(item.get("isBuyEnabled")),
        industry_id=_positive_int_or_none(item.get("internalIndustryId")),
        sector_id=_positive_int_or_none(item.get("sectorNameId")),
        popularity_uniques_7d=_decimal_or_none(item.get("popularityUniques7Day")),
        traders_7d_change=_decimal_or_none(item.get("traders7DayChange")),
        buy_holding_pct=_decimal_or_none(item.get("buyHoldingPct")),
        sell_holding_pct=_decimal_or_none(item.get("sellHoldingPct")),
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


def _normalise_intraday_candles(raw: object) -> list[IntradayBar]:
    """Normalise an eToro intraday-candles response into IntradayBar list.

    Same outer envelope as the daily endpoint
    (``{ candles: [{ instrumentId, candles: [...] }] }``); each inner
    candle carries a ``fromDate`` ISO timestamp with time component
    instead of date-only.
    """
    if not isinstance(raw, dict):
        raise ValueError(f"Expected dict from eToro candles endpoint, got {type(raw)}")

    outer: list[object] = raw.get("candles") or []

    bars: list[IntradayBar] = []
    for group in outer:
        if not isinstance(group, dict):
            continue
        inner: list[object] = group.get("candles") or []
        for item in inner:
            if not isinstance(item, dict):
                continue
            bar = _normalise_intraday_candle(item)
            if bar is not None:
                bars.append(bar)
    return bars


def _normalise_intraday_candle(item: Mapping[str, object]) -> IntradayBar | None:
    """Map a single eToro intraday candle dict to an IntradayBar.

    Returns None and logs a warning on missing required fields rather
    than raising — a single malformed bar should not poison a 390-bar
    series. Decimal precision preserved via ``Decimal(str(...))``.
    """
    raw_date = item.get("fromDate")
    raw_open = item.get("open")
    raw_high = item.get("high")
    raw_low = item.get("low")
    raw_close = item.get("close")

    if any(v is None or v == "" for v in (raw_date, raw_open, raw_high, raw_low, raw_close)):
        logger.warning("Skipping intraday candle missing required fields: %s", item)
        return None

    try:
        # eToro uses ISO timestamps with optional `Z` suffix; .fromisoformat
        # handles `2026-04-27T14:30:00+00:00` natively and accepts the
        # `Z` form on Python 3.11+. Coerce to UTC.
        ts_text = str(raw_date).replace("Z", "+00:00")
        ts = datetime.fromisoformat(ts_text)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        else:
            ts = ts.astimezone(UTC)
        return IntradayBar(
            timestamp=ts,
            open=Decimal(str(raw_open)),
            high=Decimal(str(raw_high)),
            low=Decimal(str(raw_low)),
            close=Decimal(str(raw_close)),
            volume=_int_or_none(item.get("volume")),
        )
    except (ValueError, ArithmeticError) as exc:
        logger.warning("Skipping malformed intraday candle: %s — %s", item, exc)
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

    # #1429: eToro returns lastExecution=0 for un-freshly-traded instruments
    # (bid/ask present, no recent trade). A non-positive last is not a real
    # trade price — persist NULL so the read-side derives a mark from bid/ask
    # rather than a fake 0 (which reads as a −100% loss, #1428).
    last_val = Decimal(str(raw_last)) if raw_last is not None else None
    if last_val is not None and last_val <= 0:
        last_val = None

    return Quote(
        instrument_id=int(str(instrument_id)),
        timestamp=quoted_at,
        bid=bid,
        ask=ask,
        last=last_val,
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


def _positive_int_or_none(value: object) -> int | None:
    result = _int_or_none(value)
    return result if result is not None and result > 0 else None


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except ValueError, ArithmeticError:
        return None
    return result if result.is_finite() else None


def _bool_or_none(value: object) -> bool | None:
    return value if isinstance(value, bool) else None
