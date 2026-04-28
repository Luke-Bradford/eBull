"""Instrument list and detail API endpoints.

Reads from:
  - instruments          (core instrument metadata)
  - quotes               (1:1 current snapshot per instrument, overwritten each refresh)
  - coverage             (1:1 coverage tier per instrument)
  - external_identifiers (1:N provider-native identifiers per instrument)

No writes (DB-side). No schema changes.

**Carve-out — intraday candles (#600).** The
``GET /instruments/{symbol}/intraday-candles`` endpoint is a
provider-backed pass-through: it loads eToro broker credentials,
calls the live eToro REST endpoint via ``EtoroMarketDataProvider``,
and serves bars through an in-process TTL cache (no DB persistence).
This is the one endpoint in this module that consumes external API
quota and writes an audit row per request (via
``load_credential_for_provider_use``). All other endpoints stay
DB-only.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Literal, get_args

import httpx
import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.config import settings
from app.db import get_conn
from app.providers.implementations.etoro import EtoroMarketDataProvider
from app.providers.market_data import IntradayInterval
from app.services.broker_credentials import (
    CredentialNotFound,
    load_credential_for_provider_use,
)
from app.services.intraday_candles import fetch_intraday_candles
from app.services.operators import (
    AmbiguousOperatorError,
    NoOperatorError,
    sole_operator_id,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/instruments", tags=["instruments"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

MAX_PAGE_LIMIT = 200


class QuoteSnapshot(BaseModel):
    """Latest quote for an instrument.

    The ``quotes`` table is a 1:1 current-snapshot table keyed by
    ``instrument_id``.  Each market-data refresh overwrites the single row
    for a given instrument, so there is never more than one quote row per
    instrument.  A LEFT JOIN on ``quotes`` is therefore fan-out-safe.
    """

    bid: float
    ask: float
    last: float | None
    spread_pct: float | None
    quoted_at: datetime


class ExternalIdentifier(BaseModel):
    provider: str
    identifier_type: str
    identifier_value: str


class InstrumentListItem(BaseModel):
    instrument_id: int
    symbol: str
    company_name: str
    exchange: str | None
    currency: str | None
    sector: str | None
    is_tradable: bool
    coverage_tier: int | None
    latest_quote: QuoteSnapshot | None


class InstrumentListResponse(BaseModel):
    items: list[InstrumentListItem]
    total: int
    offset: int
    limit: int


class InstrumentIdentity(BaseModel):
    symbol: str
    display_name: str | None
    sector: str | None
    industry: str | None
    exchange: str | None
    country: str | None
    currency: str | None
    market_cap: Decimal | None


class InstrumentPrice(BaseModel):
    current: Decimal | None
    day_change: Decimal | None
    day_change_pct: Decimal | None
    week_52_high: Decimal | None
    week_52_low: Decimal | None
    currency: str | None


# Closed set of values for `InstrumentKeyStats.field_source` entries. Mirror
# in frontend/src/api/types.ts — consumers rely on this being exhaustive.
# Per settled decision (eToro = market data, SEC = official filings),
# yfinance has no role; #498/#499 retired it.
#
# ``sec_dividend_summary`` distinguishes values sourced from
# ``instrument_dividend_summary.ttm_yield_pct`` (#426) from values
# computed against XBRL concepts directly — the dividend summary is
# *not* a raw XBRL field and conflating the two would mislead any
# audit trail that filters on provenance.
KeyStatsFieldSource = Literal[
    "sec_xbrl",
    "sec_dividend_summary",
    "unavailable",
    "sec_xbrl_price_missing",
]


class InstrumentKeyStats(BaseModel):
    pe_ratio: Decimal | None
    pb_ratio: Decimal | None
    dividend_yield: Decimal | None
    payout_ratio: Decimal | None
    roe: Decimal | None
    roa: Decimal | None
    debt_to_equity: Decimal | None
    revenue_growth_yoy: Decimal | None
    earnings_growth_yoy: Decimal | None
    field_source: dict[str, KeyStatsFieldSource] | None = None


class InstrumentFinancialRow(BaseModel):
    period_end: date
    period_type: str  # Q1/Q2/Q3/Q4/FY (from financial_periods)
    values: dict[str, Decimal | None]


class InstrumentFinancials(BaseModel):
    symbol: str
    statement: str  # "income" | "balance" | "cashflow"
    period: str  # "quarterly" | "annual"
    currency: str | None
    source: str  # "financial_periods" (local SEC XBRL) | "unavailable"
    rows: list[InstrumentFinancialRow]


class CandleBar(BaseModel):
    """One daily OHLCV bar — the minimal shape a chart library needs."""

    date: date
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: Decimal | None


class InstrumentCandles(BaseModel):
    symbol: str
    # Range token the caller asked for; echoed so the client can cache
    # by range key. `days` is the resolved lookback the server applied.
    # Field named `range` even though it shadows the Python builtin at
    # class scope — pydantic v2's alias+populate_by_name wiring kept
    # tripping pyright, and the shadow is safe inside a BaseModel
    # (the builtin is still reachable as `builtins.range`).
    range: Literal["1w", "1m", "3m", "6m", "ytd", "1y", "5y", "max"]  # noqa: A003
    days: int | None  # None when range="max"
    rows: list[CandleBar]


class IntradayBarPayload(BaseModel):
    """One intraday OHLCV bar.

    ``timestamp`` is a UTC ISO-8601 datetime — distinct from
    ``CandleBar.date`` which is YYYY-MM-DD only. Lightweight-charts
    consumers feed ``timestamp`` straight into the time scale via
    ``new Date(...).getTime() / 1000``.
    """

    timestamp: datetime
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: int | None


class InstrumentIntradayCandles(BaseModel):
    """Response shape for /instruments/{symbol}/intraday-candles.

    ``persisted`` is always False so the caller knows these bars are
    not in any DB table — they came directly from the provider via the
    in-process cache. Useful for ops dashboards and a future
    persistence-status flag if we ever cache to disk.
    """

    symbol: str
    interval: IntradayInterval
    count: int
    persisted: Literal[False] = False
    rows: list[IntradayBarPayload]


# Accepted interval tokens — derived from the IntradayInterval Literal
# so this list cannot drift from the provider contract.
_VALID_INTERVALS: frozenset[str] = frozenset(get_args(IntradayInterval))

# Hard cap mirrors eToro's documented 1000-candle ceiling. Leaving a
# small headroom so we never touch the limit.
_MAX_INTRADAY_COUNT = 1000


class CapabilityCellPayload(BaseModel):
    """One (capability × instrument) cell in the summary response.

    Mirrors ``app.services.capabilities.CapabilityCell`` — the API
    layer translates the dataclass to this Pydantic model so the
    OpenAPI schema is generated correctly.
    """

    providers: list[str]
    data_present: dict[str, bool]


class InstrumentSummary(BaseModel):
    """Per-ticker research summary.

    Sources, per the settled provider strategy (#498/#499):
      - Identity: local ``instruments`` row (sourced from eToro
        universe sync + SEC profile enrichment).
      - Price: ``quotes`` table (eToro WS / market data refresh).
      - Market cap: SEC XBRL share count × eToro close.
      - Key stats: SEC XBRL via ``financial_periods_ttm`` +
        ``instrument_dividend_summary``.

    All leaf fields are nullable so the UI can render partial data
    when SEC coverage is missing. ``source`` reports which provider
    populated each section.
    """

    instrument_id: int
    is_tradable: bool
    coverage_tier: int | None
    identity: InstrumentIdentity
    price: InstrumentPrice | None
    key_stats: InstrumentKeyStats | None
    source: dict[str, str]
    # Coverage gates — frontend uses these to hide irrelevant
    # tabs / panels rather than render an empty state for an
    # instrument the source does not cover (#503 PR 2).
    #
    # ``has_sec_cik``: True iff the instrument has a primary SEC
    # CIK in ``external_identifiers``. Gates SEC-specific panels:
    # SecProfilePanel, InsiderActivityPanel, DividendsPanel,
    # business-summary section.
    #
    # ``has_filings_coverage``: True iff any provider has filed
    # filings for the instrument (today: SEC; tomorrow:
    # Companies House / regional sources). Gates the
    # source-agnostic Filings tab + right-rail "recent filings"
    # widget. Wider than ``has_sec_cik`` so adding a non-SEC
    # provider later doesn't bake in a follow-up.
    has_sec_cik: bool
    has_filings_coverage: bool
    # Per-capability resolution (#515 PR 3). Keyed by capability
    # name (filings / fundamentals / dividends / …); each value
    # carries the operator-decided ``providers`` list and a
    # per-provider ``data_present`` dict. Frontend renders a
    # panel iff providers is non-empty AND any data_present
    # value is true. has_sec_cik / has_filings_coverage above are
    # kept for now as a thin shim during the migration window;
    # PR 3b retires them once frontend reads ``capabilities``
    # directly.
    capabilities: dict[str, CapabilityCellPayload]


class InstrumentDetail(BaseModel):
    instrument_id: int
    symbol: str
    company_name: str
    exchange: str | None
    currency: str | None
    sector: str | None
    industry: str | None
    country: str | None
    is_tradable: bool
    first_seen_at: datetime
    last_seen_at: datetime
    coverage_tier: int | None
    latest_quote: QuoteSnapshot | None
    external_identifiers: list[ExternalIdentifier]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_quote(row: dict[str, object]) -> QuoteSnapshot | None:
    """Extract a QuoteSnapshot from a joined row, or None if no quote exists.

    Guards on ``quoted_at``, ``bid``, and ``ask`` — all three must be non-None
    to produce a valid snapshot.  A partially-written quote row (e.g. quoted_at
    set but bid/ask NULL) returns None rather than crashing on ``float(None)``.
    """
    if row.get("quoted_at") is None or row.get("bid") is None or row.get("ask") is None:
        return None
    return QuoteSnapshot(
        bid=float(row["bid"]),  # type: ignore[arg-type]
        ask=float(row["ask"]),  # type: ignore[arg-type]
        last=float(row["last"]) if row.get("last") is not None else None,  # type: ignore[arg-type]
        spread_pct=float(row["spread_pct"]) if row.get("spread_pct") is not None else None,  # type: ignore[arg-type]
        quoted_at=row["quoted_at"],  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("", response_model=InstrumentListResponse)
def list_instruments(
    conn: psycopg.Connection[object] = Depends(get_conn),
    search: str | None = Query(default=None, max_length=100),
    sector: str | None = Query(default=None),
    coverage_tier: int | None = Query(default=None, ge=1, le=3),
    exchange: str | None = Query(default=None),
    has_dividend: bool | None = Query(default=None),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=MAX_PAGE_LIMIT),
) -> InstrumentListResponse:
    """Paginated instrument list with optional filters.

    Filters:
      - search: prefix match on symbol or case-insensitive substring on
        instrument_display_name (company_name)
      - sector: exact match on instruments.sector
      - coverage_tier: exact match (1/2/3); untiered instruments excluded
      - exchange: exact match on instruments.exchange
      - has_dividend: True matches instruments with a positive TTM dividend
        signal (dps_declared or dividends_paid > 0 in the last four reported
        quarters); False matches instruments with no such signal. Backed by
        the ``instrument_dividend_summary`` view (sql/050).

    Ordering: symbol ASC, instrument_id ASC (deterministic tiebreak).
    """
    # -- WHERE clause fragments (parameterised) ----------------------------
    where_clauses: list[str] = []
    filter_params: dict[str, object] = {}

    if search is not None:
        search = search.strip()
        if search:
            where_clauses.append("(i.symbol ILIKE %(search_prefix)s OR i.company_name ILIKE %(search_contains)s)")
            filter_params["search_prefix"] = f"{search}%"
            filter_params["search_contains"] = f"%{search}%"
    if sector is not None:
        where_clauses.append("i.sector = %(sector)s")
        filter_params["sector"] = sector
    if coverage_tier is not None:
        where_clauses.append("c.coverage_tier = %(coverage_tier)s")
        filter_params["coverage_tier"] = coverage_tier
    if exchange is not None:
        where_clauses.append("i.exchange = %(exchange)s")
        filter_params["exchange"] = exchange
    if has_dividend is not None:
        if has_dividend:
            where_clauses.append("COALESCE(ds.has_dividend, FALSE) = TRUE")
        else:
            where_clauses.append("COALESCE(ds.has_dividend, FALSE) = FALSE")

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    # -- COUNT query -------------------------------------------------------
    # Only join tables that the active filters require.
    # Uses filter_params only — no limit/offset keys that the COUNT has no placeholders for.
    count_needs_coverage = coverage_tier is not None
    count_needs_dividend = has_dividend is not None
    count_join = "LEFT JOIN coverage c USING (instrument_id)" if count_needs_coverage else ""
    count_dividend_join = (
        "LEFT JOIN instrument_dividend_summary ds USING (instrument_id)" if count_needs_dividend else ""
    )
    count_sql = (  # noqa: S608  — hardcoded fragments only
        f"SELECT COUNT(*) AS cnt FROM instruments i {count_join} {count_dividend_join}{where_sql}"
    )

    # -- Items query -------------------------------------------------------
    # Only join the dividend-summary view when the filter needs it. The view
    # scans every instrument with any dividend row, and adding it to an
    # unrelated query (e.g. a plain ``/instruments`` call) is pure overhead.
    items_dividend_join = count_dividend_join
    items_params: dict[str, object] = {**filter_params, "limit": limit, "offset": offset}
    items_sql = f"""SELECT i.instrument_id, i.symbol, i.company_name, i.exchange,
               i.currency, i.sector, i.is_tradable,
               c.coverage_tier,
               q.bid, q.ask, q.last, q.spread_pct, q.quoted_at
        FROM instruments i
        LEFT JOIN quotes q USING (instrument_id)
        LEFT JOIN coverage c USING (instrument_id)
        {items_dividend_join}
        {where_sql}
        ORDER BY i.symbol, i.instrument_id
        LIMIT %(limit)s OFFSET %(offset)s"""  # noqa: S608  — hardcoded fragments only

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(count_sql, filter_params)  # type: ignore[arg-type]  # SQL built from hardcoded fragments
        count_row = cur.fetchone()
        total: int = count_row["cnt"] if count_row else 0  # type: ignore[index]

        cur.execute(items_sql, items_params)  # type: ignore[arg-type]  # SQL built from hardcoded fragments
        rows = cur.fetchall()

    items = [
        InstrumentListItem(
            instrument_id=r["instrument_id"],  # type: ignore[arg-type]
            symbol=r["symbol"],  # type: ignore[arg-type]
            company_name=r["company_name"],  # type: ignore[arg-type]
            exchange=r["exchange"],  # type: ignore[arg-type]
            currency=r["currency"],  # type: ignore[arg-type]
            sector=r["sector"],  # type: ignore[arg-type]
            is_tradable=r["is_tradable"],  # type: ignore[arg-type]
            coverage_tier=r["coverage_tier"],  # type: ignore[arg-type]
            latest_quote=_parse_quote(r),
        )
        for r in rows
    ]

    return InstrumentListResponse(items=items, total=total, offset=offset, limit=limit)


# Column sets per statement for the local financial_periods read path.
# Tuple order is the preferred display order for each statement.
_INCOME_COLUMNS: tuple[str, ...] = (
    "revenue",
    "cost_of_revenue",
    "gross_profit",
    "operating_income",
    "net_income",
    "eps_basic",
    "eps_diluted",
    "research_and_dev",
    "sga_expense",
    "depreciation_amort",
    "interest_expense",
    "income_tax",
    "shares_basic",
    "shares_diluted",
    "sbc_expense",
)

_BALANCE_COLUMNS: tuple[str, ...] = (
    "total_assets",
    "total_liabilities",
    "shareholders_equity",
    "cash",
    "long_term_debt",
    "short_term_debt",
    "shares_outstanding",
    "inventory",
    "receivables",
    "payables",
    "goodwill",
    "ppe_net",
)

_CASHFLOW_COLUMNS: tuple[str, ...] = (
    "operating_cf",
    "investing_cf",
    "financing_cf",
    "capex",
    "dividends_paid",
    "dps_declared",
    "buyback_spend",
)

_STATEMENT_COLUMNS: dict[str, tuple[str, ...]] = {
    "income": _INCOME_COLUMNS,
    "balance": _BALANCE_COLUMNS,
    "cashflow": _CASHFLOW_COLUMNS,
}


def _fetch_local_financials(
    conn: psycopg.Connection[object],
    instrument_id: int,
    columns: tuple[str, ...],
    period: str,
) -> tuple[list[InstrumentFinancialRow], str | None]:
    """Read rows from ``financial_periods`` for the given statement's columns.

    ``period == 'quarterly'`` matches period_type IN ('Q1','Q2','Q3','Q4').
    ``period == 'annual'`` matches period_type = 'FY'.

    Returns (rows, currency). Empty rows = no local data; the caller
    returns an empty payload (no yfinance fallback per #498/#499 —
    SEC XBRL is the only fundamentals source for US instruments).
    """
    period_types: list[str] = ["Q1", "Q2", "Q3", "Q4"] if period == "quarterly" else ["FY"]

    # Columns whitelisted above — safe to format into SQL. period_types
    # is bound as a parameter, not formatted, so a future value added to
    # the CHECK constraint won't silently match.
    select_cols = ", ".join(columns)
    # Ordering: ``period_end_date DESC`` is the primary signal so the
    # rendered columns walk backwards through real fiscal time. The
    # endpoint is called separately for ``period_types ∈ {Q1..Q4}``
    # (quarterly view) vs ``{FY}`` (annual view) — see the call sites
    # below — so a within-row mix of FY and Q4 does not happen at the
    # API level. Tie-breakers are added defensively so a leftover
    # duplicate row that slips past the migration 076 dedupe (e.g. a
    # provider-side restatement that arrives after the one-shot purge)
    # still renders deterministically: latest ``filed_date`` wins,
    # then ``filing_event_id`` only as a final pin against a tied
    # filed_date.
    sql = f"""
        SELECT period_end_date, period_type, reported_currency, {select_cols}
        FROM financial_periods
        WHERE instrument_id = %(iid)s
          AND superseded_at IS NULL
          AND period_type = ANY(%(types)s::text[])
        ORDER BY period_end_date DESC,
                 filed_date DESC NULLS LAST,
                 fiscal_year DESC NULLS LAST,
                 fiscal_quarter DESC NULLS LAST
        LIMIT 20
    """  # noqa: S608 — columns are a hardcoded whitelist; period_types is bound

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, {"iid": instrument_id, "types": period_types})  # type: ignore[arg-type]  # SQL built from hardcoded whitelist
        db_rows = cur.fetchall()

    if not db_rows:
        return [], None

    currency = db_rows[0].get("reported_currency") if db_rows else None  # type: ignore[union-attr]
    rows: list[InstrumentFinancialRow] = [
        InstrumentFinancialRow(
            period_end=r["period_end_date"],  # type: ignore[arg-type]
            period_type=str(r["period_type"]),  # type: ignore[index]
            values={col: r.get(col) for col in columns},  # type: ignore[union-attr]
        )
        for r in db_rows
    ]
    return rows, str(currency) if currency is not None else None


@router.get("/{symbol}/financials", response_model=InstrumentFinancials)
def get_instrument_financials(
    symbol: str,
    period: Literal["quarterly", "annual"] = Query(default="quarterly"),
    statement: Literal["income", "balance", "cashflow"] = Query(default="income"),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentFinancials:
    """Per-ticker financial statement.

    Sourced exclusively from local ``financial_periods`` rows (SEC
    XBRL-derived). Per the settled provider strategy (#498/#499 +
    #532), yfinance and paid third-party providers have no role.
    Non-US issuers without regulated-source coverage in this repo
    return an empty row list — per-region integration PRs add free
    regulated providers (Companies House, ESMA, etc.).

    Returns an empty row list (not 500, not 404) when no SEC data
    exists — the UI shows "no statement data available".
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    columns = _STATEMENT_COLUMNS[statement]

    # Resolve symbol -> instrument_id for the local read. `symbol` is
    # not UNIQUE across exchanges (see migration 043), so order by
    # `is_primary_listing DESC, instrument_id ASC` to make the winner
    # deterministic on collisions.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    local_rows, local_currency = _fetch_local_financials(
        conn,
        int(inst_row["instrument_id"]),  # type: ignore[arg-type]
        columns,
        period,
    )
    if local_rows:
        return InstrumentFinancials(
            symbol=str(inst_row["symbol"]),  # type: ignore[index]
            statement=statement,
            period=period,
            currency=local_currency,
            source="financial_periods",
            rows=local_rows,
        )

    # No SEC coverage → empty payload. Frontend renders the empty-
    # state hint; no fallback to a non-canonical source.
    return InstrumentFinancials(
        symbol=str(inst_row["symbol"]),  # type: ignore[index]
        statement=statement,
        period=period,
        currency=None,
        source="unavailable",
        rows=[],
    )


_CANDLE_RANGE_DAYS: dict[str, int | None] = {
    "1w": 7,
    "1m": 30,
    "3m": 90,
    "6m": 180,
    "1y": 365,
    "5y": 365 * 5,
    "max": None,
}


def _resolve_range_days(range_: str, today: date) -> int | None:
    """Map a range token to a calendar-day lookback, or None for max.

    YTD is computed dynamically from ``today`` rather than living in
    the static dict so the lookback shrinks every Jan 1. The static
    dict still holds the fixed-window tokens.
    """
    if range_ == "ytd":
        # Days from Jan 1 of `today`'s year to `today` (inclusive of
        # today's bar). Clamp to ≥1 so January 1 returns at least
        # yesterday's bar — without the clamp, Jan 1 returns 0 and the
        # chart shows an empty state on what should be a sensible
        # "single-day-into-the-year" view.
        return max(1, (today - date(today.year, 1, 1)).days)
    return _CANDLE_RANGE_DAYS[range_]


@router.get("/{symbol}/candles", response_model=InstrumentCandles)
def get_instrument_candles(
    symbol: str,
    range_: Literal["1w", "1m", "3m", "6m", "ytd", "1y", "5y", "max"] = Query(default="1m", alias="range"),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentCandles:
    """Daily OHLCV bars for `symbol` over the requested lookback.

    Reads from `price_daily` (populated by the market-data refresh
    job). No provider fallback here — if we don't have local bars,
    return an empty row list and let the chart render an empty state.
    A 404 is reserved for an unknown symbol.

    Range resolution is the server's responsibility: the caller passes
    a range token (`1m`, `1y`, ...) and the server maps it to a day
    lookback. `max` returns every stored bar.

    Bars are returned oldest-first so chart libraries can feed the
    array directly without re-sorting.
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    # Symbol → instrument_id (primary-listing tiebreaker, see #357).
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    days = _resolve_range_days(range_, date.today())
    # Two fixed queries rather than f-string-composing a WHERE clause,
    # so there's no structural-injection footgun if the range-token
    # set grows later. `max` omits the date filter entirely.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        if days is None:
            cur.execute(
                """
                SELECT price_date, open, high, low, close, volume
                FROM price_daily
                WHERE instrument_id = %(iid)s
                ORDER BY price_date ASC
                """,
                {"iid": inst_row["instrument_id"]},
            )
        else:
            cur.execute(
                """
                SELECT price_date, open, high, low, close, volume
                FROM price_daily
                WHERE instrument_id = %(iid)s
                  AND price_date >= CURRENT_DATE - make_interval(days => %(days)s)
                ORDER BY price_date ASC
                """,
                {"iid": inst_row["instrument_id"], "days": days},
            )
        rows = cur.fetchall()

    bars = [
        CandleBar(
            date=r["price_date"],  # type: ignore[arg-type]
            open=r["open"],  # type: ignore[arg-type]
            high=r["high"],  # type: ignore[arg-type]
            low=r["low"],  # type: ignore[arg-type]
            close=r["close"],  # type: ignore[arg-type]
            volume=r["volume"],  # type: ignore[arg-type]
        )
        for r in rows
    ]
    return InstrumentCandles(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        range=range_,
        days=days,
        rows=bars,
    )


@router.get(
    "/{symbol}/intraday-candles",
    response_model=InstrumentIntradayCandles,
    dependencies=[Depends(require_session_or_service_token)],
)
def get_instrument_intraday_candles(
    symbol: str,
    interval: IntradayInterval = Query(default="OneMinute"),
    count: int = Query(default=390, ge=1, le=_MAX_INTRADAY_COUNT),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentIntradayCandles:
    """Provider-backed intraday OHLCV bars.

    **Not** read from the local DB. Each call resolves the symbol to
    an instrument id, loads eToro broker credentials, fetches bars at
    the requested interval through the in-process TTL cache, and
    returns them. Daily / longer-horizon ranges should continue to
    use ``/candles?range=...`` which reads from ``price_daily``.

    Error mapping:
      * Unknown symbol → 404
      * Missing eToro credentials → 503 (operator must run setup)
      * Provider 429 (rate limit) → 503 with Retry-After
      * Provider 5xx / network failure → 502

    The frontend owns the range → (interval, count) translation
    table. This endpoint is intentionally count-based to mirror the
    eToro REST shape exactly.
    """
    if interval not in _VALID_INTERVALS:
        # FastAPI already validates the Literal, but keep this as a
        # belt-and-braces guard against drift between the type and
        # the validator's accepted set.
        raise HTTPException(status_code=400, detail=f"Unsupported interval {interval!r}")

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    # Symbol → instrument_id (primary-listing tiebreaker, matches
    # the daily endpoint).
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    # Load eToro credentials. Each call writes an audit row tied to
    # the operator and caller name, so chart-driven external spend is
    # traceable. ``load_credential_for_provider_use`` does not commit
    # itself; we commit the audit row before the external call so a
    # network failure does not lose the audit trail.
    try:
        op_id = sole_operator_id(conn)
    except (NoOperatorError, AmbiguousOperatorError) as exc:
        logger.warning("intraday-candles: operator lookup failed: %s", exc)
        raise HTTPException(status_code=503, detail="No operator configured") from exc

    try:
        api_key = load_credential_for_provider_use(
            conn,
            operator_id=op_id,
            provider="etoro",
            label="api_key",
            environment=settings.etoro_env,
            caller="intraday_candles_endpoint",
        )
        conn.commit()
        user_key = load_credential_for_provider_use(
            conn,
            operator_id=op_id,
            provider="etoro",
            label="user_key",
            environment=settings.etoro_env,
            caller="intraday_candles_endpoint",
        )
        conn.commit()
    except CredentialNotFound as exc:
        logger.warning("intraday-candles: %s", exc)
        raise HTTPException(
            status_code=503,
            detail="eToro credentials not configured",
        ) from exc

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]

    try:
        with EtoroMarketDataProvider(api_key=api_key, user_key=user_key, env=settings.etoro_env) as provider:
            bars = fetch_intraday_candles(
                provider,
                instrument_id=instrument_id,
                interval=interval,
                count=count,
            )
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if status == 429:
            retry_after = exc.response.headers.get("Retry-After", "30")
            logger.warning(
                "intraday-candles: eToro rate-limited for %s, retry-after=%s",
                symbol_clean,
                retry_after,
            )
            raise HTTPException(
                status_code=503,
                detail="Rate limited upstream",
                headers={"Retry-After": retry_after},
            ) from exc
        logger.warning("intraday-candles: eToro returned %d for %s", status, symbol_clean)
        raise HTTPException(status_code=502, detail="Upstream provider error") from exc
    except httpx.RequestError as exc:
        logger.warning("intraday-candles: network error fetching %s: %s", symbol_clean, exc)
        raise HTTPException(status_code=502, detail="Upstream provider unreachable") from exc

    return InstrumentIntradayCandles(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        interval=interval,
        # Echo the actual number of bars returned, not the operator's
        # request — eToro can return fewer than `count` near market
        # open, on thinly-traded instruments, or after a fresh listing.
        # Callers reading `body.count` must see what they actually got.
        count=len(bars),
        rows=[
            IntradayBarPayload(
                timestamp=b.timestamp,
                open=b.open,
                high=b.high,
                low=b.low,
                close=b.close,
                volume=b.volume,
            )
            for b in bars
        ],
    )


# ---------------------------------------------------------------------------
# SEC entity profile (#427) — extracted from submissions.json
# ---------------------------------------------------------------------------


class FormerNameModel(BaseModel):
    name: str
    from_: str | None = None
    to: str | None = None


class InstrumentSecProfile(BaseModel):
    symbol: str
    cik: str
    sic: str | None
    sic_description: str | None
    owner_org: str | None
    description: str | None
    website: str | None
    investor_website: str | None
    ein: str | None
    lei: str | None
    state_of_incorporation: str | None
    state_of_incorporation_desc: str | None
    fiscal_year_end: str | None
    category: str | None
    exchanges: list[str]
    former_names: list[FormerNameModel]
    has_insider_issuer: bool | None
    has_insider_owner: bool | None


@router.get("/{symbol}/sec_profile", response_model=InstrumentSecProfile)
def get_instrument_sec_profile(
    symbol: str,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentSecProfile:
    """Return the SEC-sourced entity metadata for an instrument (#427).

    Surfaces sic / sic_description / description / website / exchanges
    / former_names / insider-activity flags from the daily submissions
    fetch. Populated for US-mapped tickers after the first
    ``fundamentals_sync`` seeds the row.

    404 when the instrument itself is unknown. 404 + ``{"detail": "no
    SEC profile"}`` when the instrument exists but no profile row has
    been seeded yet (pre-first-seed or non-US ticker without a primary
    CIK).
    """
    from app.services.sec_entity_profile import get_entity_profile

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    profile = get_entity_profile(conn, instrument_id=instrument_id)
    if profile is None:
        raise HTTPException(
            status_code=404,
            detail="no SEC profile on file for this instrument",
        )

    # Description = SEC submissions.json blurb only. Pre-#552 this
    # path preferred the authoritative 10-K Item 1 body and the
    # SecProfilePanel rendered the full multi-paragraph narrative
    # inline — that produced a wall-of-text on the instrument page
    # that pushed all other panels off-screen. The 10-K narrative now
    # lives behind the BusinessSectionsTeaser + drilldown route
    # (#552); the SecProfile description reverts to the short
    # submissions blurb that fits the instrument page panel.
    description = profile.description

    return InstrumentSecProfile(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        cik=profile.cik,
        sic=profile.sic,
        sic_description=profile.sic_description,
        owner_org=profile.owner_org,
        description=description,
        website=profile.website,
        investor_website=profile.investor_website,
        ein=profile.ein,
        lei=profile.lei,
        state_of_incorporation=profile.state_of_incorporation,
        state_of_incorporation_desc=profile.state_of_incorporation_desc,
        fiscal_year_end=profile.fiscal_year_end,
        category=profile.category,
        exchanges=profile.exchanges,
        former_names=[
            FormerNameModel(
                name=str(fn["name"]),
                from_=fn.get("from"),
                to=fn.get("to"),
            )
            for fn in profile.former_names
            if fn.get("name")
        ],
        has_insider_issuer=profile.has_insider_issuer,
        has_insider_owner=profile.has_insider_owner,
    )


# ---------------------------------------------------------------------------
# Headcount endpoint (#551 — XBRL ``dei:EntityNumberOfEmployees``)
# ---------------------------------------------------------------------------


class InstrumentHeadcount(BaseModel):
    """Most-recent reported employee count for one instrument.

    Sourced from the SEC iXBRL ``dei:EntityNumberOfEmployees`` fact
    via ``financial_facts_raw``. Block-tagging mandate post-2020 means
    near-complete coverage on US issuers — the panel is silent (404)
    when the instrument has no SEC CIK or no DEI fact ingested yet.
    """

    symbol: str
    employees: int
    period_end_date: date
    source_accession: str


@router.get("/{symbol}/employees", response_model=InstrumentHeadcount)
def get_instrument_employees(
    symbol: str,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentHeadcount:
    """Latest ``dei:EntityNumberOfEmployees`` fact for an instrument.

    Returns 404 when no fact is on file (non-SEC issuer, fundamentals
    sync hasn't seeded yet, or DEI cover-page tagging absent).
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()
    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")
    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT period_end, val, accession_number
              FROM financial_facts_raw
             WHERE instrument_id = %s
               AND concept = 'EntityNumberOfEmployees'
             ORDER BY period_end DESC, fetched_at DESC
             LIMIT 1
            """,
            (instrument_id,),
        )
        row = cur.fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail=f"No employee count on file for {symbol}")

    return InstrumentHeadcount(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        employees=int(row["val"]),  # type: ignore[arg-type]
        period_end_date=row["period_end"],  # type: ignore[arg-type]
        source_accession=str(row["accession_number"]),
    )


# ---------------------------------------------------------------------------
# 8-K structured-events endpoint (#450)
# ---------------------------------------------------------------------------


class EightKItemModel(BaseModel):
    item_code: str
    item_label: str
    severity: str | None
    body: str


class EightKExhibitModel(BaseModel):
    exhibit_number: str
    description: str | None


class EightKFilingModel(BaseModel):
    accession_number: str
    document_type: str
    is_amendment: bool
    date_of_report: date | None
    reporting_party: str | None
    signature_name: str | None
    signature_title: str | None
    signature_date: date | None
    primary_document_url: str | None
    items: list[EightKItemModel]
    exhibits: list[EightKExhibitModel]


class EightKFilingsResponse(BaseModel):
    symbol: str
    filings: list[EightKFilingModel]


# Per-endpoint allowed `provider` values. Frontend hooks pass the
# capability's resolved provider tag so the endpoint can route to the
# right backend once non-SEC sources land. Today every endpoint only
# wires a single provider; per-region integration PRs add more.
_EIGHT_K_PROVIDERS: tuple[str, ...] = ("sec_8k_events",)
_DIVIDEND_PROVIDERS: tuple[str, ...] = ("sec_dividend_summary",)
_INSIDER_PROVIDERS: tuple[str, ...] = ("sec_form4",)


def _validate_provider(provider: str | None, allowed: tuple[str, ...]) -> None:
    """Reject an unknown ``?provider=`` value.

    ``provider=None`` (param omitted) falls back to the endpoint's
    historical default behaviour for backward compatibility — every
    existing caller pre-#515 PR 3b passes nothing. New frontend hooks
    always pass the capability-resolved provider tag explicitly.
    """
    if provider is None:
        return
    if provider not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported provider {provider!r}; allowed: {list(allowed)}",
        )


@router.get("/{symbol}/eight_k_filings", response_model=EightKFilingsResponse)
def get_instrument_8k_filings(
    symbol: str,
    limit: int = 50,
    provider: str | None = Query(
        default=None,
        description="Capability provider tag. Today only 'sec_8k_events'.",
    ),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> EightKFilingsResponse:
    """Return recent 8-K filings for an instrument with full structured
    item bodies + exhibit pointers (#450)."""
    from app.services.eight_k_events import list_8k_filings

    _validate_provider(provider, _EIGHT_K_PROVIDERS)

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")
    if limit <= 0 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    if not _has_sec_cik(conn, instrument_id):
        # No SEC CIK means no canonical 8-K source — return empty
        # rather than render orphan rows from a prior bad CIK link
        # (see migration 066 / spec PR 2).
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} has no SEC coverage")
    filings = list_8k_filings(conn, instrument_id=instrument_id, limit=limit)
    return EightKFilingsResponse(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        filings=[
            EightKFilingModel(
                accession_number=f.accession_number,
                document_type=f.document_type,
                is_amendment=f.is_amendment,
                date_of_report=f.date_of_report,
                reporting_party=f.reporting_party,
                signature_name=f.signature_name,
                signature_title=f.signature_title,
                signature_date=f.signature_date,
                primary_document_url=f.primary_document_url,
                items=[
                    EightKItemModel(
                        item_code=i.item_code,
                        item_label=i.item_label,
                        severity=i.severity,
                        body=i.body,
                    )
                    for i in f.items
                ],
                exhibits=[
                    EightKExhibitModel(
                        exhibit_number=e.exhibit_number,
                        description=e.description,
                    )
                    for e in f.exhibits
                ],
            )
            for f in filings
        ],
    )


# ---------------------------------------------------------------------------
# 10-K Item 1 subsection breakdown (#449)
# ---------------------------------------------------------------------------


class BusinessCrossReferenceModel(BaseModel):
    reference_type: str
    target: str
    context: str


class BusinessTableModel(BaseModel):
    order: int
    headers: list[str]
    rows: list[list[str]]


class BusinessSectionModel(BaseModel):
    section_order: int
    section_key: str
    section_label: str
    body: str
    cross_references: list[BusinessCrossReferenceModel]
    tables: list[BusinessTableModel] = []


class BusinessSectionsResponse(BaseModel):
    """Response payload for ``/instruments/{symbol}/business_sections``.

    ``sections`` is ordered by the source 10-K layout (section_order).
    ``source_accession`` identifies the 10-K the sections were extracted
    from, so the UI can link back to the SEC filing. Empty list when
    no sections are on file (first-time instruments or no 10-K filed).
    """

    symbol: str
    source_accession: str | None
    sections: list[BusinessSectionModel]


@router.get(
    "/{symbol}/business_sections",
    response_model=BusinessSectionsResponse,
)
def get_instrument_business_sections(
    symbol: str,
    accession: str | None = Query(
        None,
        description="Specific 10-K accession; omit for the latest filing.",
    ),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> BusinessSectionsResponse:
    """Return the 10-K Item 1 subsection breakdown for an instrument (#449).

    Every subsection from the latest 10-K lands as its own row with a
    canonical ``section_key`` (stable identifier) + the verbatim
    ``section_label`` from the filing + the body text + cross-references
    to other items / exhibits / notes. Headings that don't match a known
    canonical key surface as ``section_key='other'`` with the original
    heading preserved — no silent drops.

    Pass ``?accession=<accession_number>`` to retrieve sections for a
    specific historical filing. Returns 404 when no sections exist for
    the requested accession (#559).
    """
    from app.services.business_summary import get_business_sections

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    sections = get_business_sections(conn, instrument_id=instrument_id, accession=accession)
    if accession is not None and not sections:
        raise HTTPException(
            status_code=404,
            detail=f"no 10-K sections for {symbol} accession {accession}",
        )
    source_accession = sections[0].source_accession if sections else None
    return BusinessSectionsResponse(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        source_accession=source_accession,
        sections=[
            BusinessSectionModel(
                section_order=s.section_order,
                section_key=s.section_key,
                section_label=s.section_label,
                body=s.body,
                cross_references=[
                    BusinessCrossReferenceModel(
                        reference_type=ref.reference_type,
                        target=ref.target,
                        context=ref.context,
                    )
                    for ref in s.cross_references
                ],
                tables=[
                    BusinessTableModel(
                        order=tbl.order,
                        headers=list(tbl.headers),
                        rows=[list(row) for row in tbl.rows],
                    )
                    for tbl in s.tables
                ],
            )
            for s in sections
        ],
    )


# ---------------------------------------------------------------------------
# 10-K filing history (#559)
# ---------------------------------------------------------------------------


class TenKHistoryFilingModel(BaseModel):
    accession_number: str
    filing_date: date
    filing_type: str


class TenKHistoryResponse(BaseModel):
    symbol: str
    filings: list[TenKHistoryFilingModel]


@router.get(
    "/{symbol}/filings/10-k/history",
    response_model=TenKHistoryResponse,
)
def get_instrument_tenk_history(
    symbol: str,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> TenKHistoryResponse:
    """Return the list of 10-K and 10-K/A filings for an instrument in
    reverse chronological order (#559).

    Used by the prior-10-Ks rail on the drilldown page so the user can
    navigate to any historical annual report.
    """
    from app.services.business_summary import list_10k_history

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    filings = list_10k_history(conn, instrument_id=instrument_id)
    return TenKHistoryResponse(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        filings=[
            TenKHistoryFilingModel(
                accession_number=f.accession_number,
                filing_date=f.filing_date,
                filing_type=f.filing_type,
            )
            for f in filings
        ],
    )


# ---------------------------------------------------------------------------
# Dilution + share-count history (#435)
# ---------------------------------------------------------------------------


class ShareCountPeriodModel(BaseModel):
    period_end: date
    fiscal_year: int | None
    fiscal_period: str | None
    shares_outstanding: Decimal | None
    shares_issued_new: Decimal | None
    buyback_shares: Decimal | None


class DilutionSummaryModel(BaseModel):
    latest_shares: Decimal | None
    latest_as_of: date | None
    yoy_shares: Decimal | None
    net_dilution_pct_yoy: Decimal | None
    ttm_shares_issued: Decimal | None
    ttm_buyback_shares: Decimal | None
    ttm_net_share_change: Decimal | None
    dilution_posture: Literal["dilutive", "buyback_heavy", "stable"]


class InstrumentDilution(BaseModel):
    symbol: str
    summary: DilutionSummaryModel
    history: list[ShareCountPeriodModel]


@router.get("/{symbol}/dilution", response_model=InstrumentDilution)
def get_instrument_dilution(
    symbol: str,
    limit: int = Query(default=40, ge=1, le=200),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentDilution:
    """Per-period share count + TTM dilution summary (#435).

    Source: SEC XBRL facts already ingested via the daily
    ``fundamentals_sync`` path (``StockIssuedDuringPeriodSharesNewIssues``,
    ``StockRepurchasedDuringPeriodShares``, ``CommonStockSharesOutstanding``,
    ``dei:EntityCommonStockSharesOutstanding``). Returns the
    ``stable`` empty shape for never-seeded / non-US tickers — UI
    renders an empty state without 404 handling.

    Default ``limit=40`` covers ten years of quarterly history.
    """
    from app.services.dilution import get_dilution_summary, get_share_count_history

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    summary = get_dilution_summary(conn, instrument_id=instrument_id)
    history = get_share_count_history(conn, instrument_id=instrument_id, limit=limit)

    return InstrumentDilution(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        summary=DilutionSummaryModel(
            latest_shares=summary.latest_shares,
            latest_as_of=summary.latest_as_of,
            yoy_shares=summary.yoy_shares,
            net_dilution_pct_yoy=summary.net_dilution_pct_yoy,
            ttm_shares_issued=summary.ttm_shares_issued,
            ttm_buyback_shares=summary.ttm_buyback_shares,
            ttm_net_share_change=summary.ttm_net_share_change,
            dilution_posture=summary.dilution_posture,
        ),
        history=[
            ShareCountPeriodModel(
                period_end=p.period_end,
                fiscal_year=p.fiscal_year,
                fiscal_period=p.fiscal_period,
                shares_outstanding=p.shares_outstanding,
                shares_issued_new=p.shares_issued_new,
                buyback_shares=p.buyback_shares,
            )
            for p in history
        ],
    )


# ---------------------------------------------------------------------------
# Dividend history + summary (#414 follow-up, operator ask 2026-04-24)
# ---------------------------------------------------------------------------


class DividendPeriodModel(BaseModel):
    period_end_date: date
    period_type: str
    fiscal_year: int
    fiscal_quarter: int | None
    dps_declared: Decimal | None
    dividends_paid: Decimal | None
    reported_currency: str | None


class DividendSummaryModel(BaseModel):
    has_dividend: bool
    ttm_dps: Decimal | None
    ttm_dividends_paid: Decimal | None
    ttm_yield_pct: Decimal | None
    latest_dps: Decimal | None
    latest_dividend_at: date | None
    dividend_streak_q: int
    dividend_currency: str | None


class UpcomingDividendModel(BaseModel):
    source_accession: str
    declaration_date: date | None
    ex_date: date | None
    record_date: date | None
    pay_date: date | None
    dps_declared: Decimal | None
    currency: str


class InstrumentDividends(BaseModel):
    symbol: str
    summary: DividendSummaryModel
    history: list[DividendPeriodModel]
    upcoming: list[UpcomingDividendModel]


@router.get("/{symbol}/dividends", response_model=InstrumentDividends)
def get_instrument_dividends(
    symbol: str,
    limit: int = Query(default=40, ge=1, le=400),
    provider: str | None = Query(
        default=None,
        description="Capability provider tag. Today only 'sec_dividend_summary'.",
    ),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentDividends:
    """Dividend history + TTM yield summary for a single instrument.

    Source: SEC XBRL ``us-gaap:CommonStockDividendsPerShareDeclared`` +
    ``us-gaap:PaymentsOfDividends``, already ingested via the daily
    companyfacts path. Returns ``has_dividend=False`` with an empty
    history array for instruments that have never reported a dividend
    — the UI can render an empty state without 404 handling.

    Default ``limit=40`` covers ten years of quarterly history.
    """
    from app.services.dividends import (
        get_dividend_history,
        get_dividend_summary,
        get_upcoming_dividends,
    )

    _validate_provider(provider, _DIVIDEND_PROVIDERS)

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    if not _has_sec_cik(conn, instrument_id):
        # No SEC CIK = no canonical dividend source. 404 instead
        # of returning orphan rows from a prior bad CIK link
        # (see migration 066 / spec PR 2).
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} has no SEC coverage")
    summary = get_dividend_summary(conn, instrument_id=instrument_id)
    history = get_dividend_history(conn, instrument_id=instrument_id, limit=limit)
    upcoming = get_upcoming_dividends(conn, instrument_id=instrument_id)

    return InstrumentDividends(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        summary=DividendSummaryModel(
            has_dividend=summary.has_dividend,
            ttm_dps=summary.ttm_dps,
            ttm_dividends_paid=summary.ttm_dividends_paid,
            ttm_yield_pct=summary.ttm_yield_pct,
            latest_dps=summary.latest_dps,
            latest_dividend_at=summary.latest_dividend_at,
            dividend_streak_q=summary.dividend_streak_q,
            dividend_currency=summary.dividend_currency,
        ),
        history=[
            DividendPeriodModel(
                period_end_date=p.period_end_date,
                period_type=p.period_type,
                fiscal_year=p.fiscal_year,
                fiscal_quarter=p.fiscal_quarter,
                dps_declared=p.dps_declared,
                dividends_paid=p.dividends_paid,
                reported_currency=p.reported_currency,
            )
            for p in history
        ],
        upcoming=[
            UpcomingDividendModel(
                source_accession=u.source_accession,
                declaration_date=u.declaration_date,
                ex_date=u.ex_date,
                record_date=u.record_date,
                pay_date=u.pay_date,
                dps_declared=u.dps_declared,
                currency=u.currency,
            )
            for u in upcoming
        ],
    )


class InsiderSummaryModel(BaseModel):
    """90-day insider-activity summary with two lenses (#458).

    Open-market fields (``open_market_*``) capture discretionary P/S
    trading — the strongest sentiment signal. Total-activity fields
    (``total_acquired_*`` / ``total_disposed_*``) capture every
    non-derivative transaction classified by
    ``acquired_disposed_code`` (or ``txn_code`` when SEC omitted the
    A/D flag). Operators need both: an RSU-vest month can show zero
    open-market buys alongside a large grant, and a summary that
    only reports open-market activity would misleadingly imply the
    insider disposed of shares on balance.
    """

    symbol: str
    open_market_net_shares_90d: Decimal
    open_market_buy_count_90d: int
    open_market_sell_count_90d: int
    total_acquired_shares_90d: Decimal
    total_disposed_shares_90d: Decimal
    acquisition_count_90d: int
    disposition_count_90d: int
    unique_filers_90d: int
    latest_txn_date: date | None
    # Back-compat aliases for callers built against the pre-#458 shape.
    # Mirror the primary open-market fields so existing consumers keep
    # rendering. Remove after all callers migrate.
    net_shares_90d: Decimal
    buy_count_90d: int
    sell_count_90d: int


@router.get("/{symbol}/insider_summary", response_model=InsiderSummaryModel)
def get_instrument_insider_summary(
    symbol: str,
    provider: str | None = Query(
        default=None,
        description="Capability provider tag. Today only 'sec_form4'.",
    ),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InsiderSummaryModel:
    """Return the 90-day insider-transaction summary (#429 / #458).

    Two lenses: open-market (discretionary P/S) and total-activity
    (every non-derivative transaction classified by
    ``acquired_disposed_code``). Only non-derivative trades
    contribute; derivative grants / option exercises are excluded.
    """
    from app.services.insider_transactions import get_insider_summary

    _validate_provider(provider, _INSIDER_PROVIDERS)

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    if not _has_sec_cik(conn, instrument_id):
        # Insider transactions are SEC Form 4 — no SEC CIK = no
        # canonical source. 404 instead of returning orphan rows
        # from a prior bad CIK link (see migration 066 / spec PR 2).
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} has no SEC coverage")
    summary = get_insider_summary(conn, instrument_id=instrument_id)
    return InsiderSummaryModel(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        open_market_net_shares_90d=summary.open_market_net_shares_90d,
        open_market_buy_count_90d=summary.open_market_buy_count_90d,
        open_market_sell_count_90d=summary.open_market_sell_count_90d,
        total_acquired_shares_90d=summary.total_acquired_shares_90d,
        total_disposed_shares_90d=summary.total_disposed_shares_90d,
        acquisition_count_90d=summary.acquisition_count_90d,
        disposition_count_90d=summary.disposition_count_90d,
        unique_filers_90d=summary.unique_filers_90d,
        latest_txn_date=summary.latest_txn_date,
        net_shares_90d=summary.open_market_net_shares_90d,
        buy_count_90d=summary.open_market_buy_count_90d,
        sell_count_90d=summary.open_market_sell_count_90d,
    )


class InsiderTransactionDetailModel(BaseModel):
    """Wide-shape payload for one Form 4 transaction row.

    Mirrors :class:`app.services.insider_transactions.InsiderTransactionDetail`
    one-to-one. Every structured field the XML carries is surfaced;
    the frontend picks what to render, the API does not editorialise.
    Footnote bodies are attached as a dict keyed by the XML field
    they qualify (``transactionShares``, ``transactionPricePerShare``,
    etc.) so the UI can render the explanatory text next to the
    specific cell.
    """

    accession_number: str
    txn_row_num: int
    document_type: str
    txn_date: date
    deemed_execution_date: date | None
    filer_cik: str | None
    filer_name: str
    filer_role: str | None
    security_title: str | None
    txn_code: str
    acquired_disposed_code: str | None
    shares: Decimal | None
    price: Decimal | None
    post_transaction_shares: Decimal | None
    direct_indirect: str | None
    nature_of_ownership: str | None
    is_derivative: bool
    equity_swap_involved: bool | None
    transaction_timeliness: str | None
    conversion_exercise_price: Decimal | None
    exercise_date: date | None
    expiration_date: date | None
    underlying_security_title: str | None
    underlying_shares: Decimal | None
    underlying_value: Decimal | None
    footnotes: dict[str, str]


class InsiderTransactionsListModel(BaseModel):
    symbol: str
    rows: list[InsiderTransactionDetailModel]


@router.get(
    "/{symbol}/insider_transactions",
    response_model=InsiderTransactionsListModel,
)
def get_instrument_insider_transactions(
    symbol: str,
    limit: int = 100,
    provider: str | None = Query(
        default=None,
        description="Capability provider tag. Today only 'sec_form4'.",
    ),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InsiderTransactionsListModel:
    """Return recent Form 4 insider transactions for an instrument.

    The operator-facing detail view. Covers both non-derivative
    (open-market buys/sells) and derivative (option exercises / grants)
    rows, most-recent first, up to ``limit``. Tombstoned filings
    (failed fetch / parse) are excluded.

    Every meaningful Form 4 XML field lands on the response. The UI
    decides what's worth rendering — we don't drop fields at the API
    layer (per the "every structured field queryable in SQL" rule).
    """
    from app.services.insider_transactions import list_insider_transactions

    _validate_provider(provider, _INSIDER_PROVIDERS)

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")
    if limit <= 0 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    if not _has_sec_cik(conn, instrument_id):
        # Form 4 is an SEC filing — no SEC CIK = no canonical
        # source. 404 instead of returning orphan rows from a
        # prior bad CIK link (see migration 066 / spec PR 2).
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} has no SEC coverage")
    detail_rows = list_insider_transactions(conn, instrument_id=instrument_id, limit=limit)
    return InsiderTransactionsListModel(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        rows=[
            InsiderTransactionDetailModel(
                accession_number=d.accession_number,
                txn_row_num=d.txn_row_num,
                document_type=d.document_type,
                txn_date=d.txn_date,
                deemed_execution_date=d.deemed_execution_date,
                filer_cik=d.filer_cik,
                filer_name=d.filer_name,
                filer_role=d.filer_role,
                security_title=d.security_title,
                txn_code=d.txn_code,
                acquired_disposed_code=d.acquired_disposed_code,
                shares=d.shares,
                price=d.price,
                post_transaction_shares=d.post_transaction_shares,
                direct_indirect=d.direct_indirect,
                nature_of_ownership=d.nature_of_ownership,
                is_derivative=d.is_derivative,
                equity_swap_involved=d.equity_swap_involved,
                transaction_timeliness=d.transaction_timeliness,
                conversion_exercise_price=d.conversion_exercise_price,
                exercise_date=d.exercise_date,
                expiration_date=d.expiration_date,
                underlying_security_title=d.underlying_security_title,
                underlying_shares=d.underlying_shares,
                underlying_value=d.underlying_value,
                footnotes=d.footnotes,
            )
            for d in detail_rows
        ],
    )


def _has_sec_cik(conn: psycopg.Connection[object], instrument_id: int) -> bool:
    """True if the instrument has a primary SEC CIK — the US-ticker signal
    that gates the local-SEC-XBRL preference path."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM external_identifiers "
            "WHERE instrument_id = %(iid)s AND provider = 'sec' "
            "AND identifier_type = 'cik' AND is_primary = TRUE "
            "LIMIT 1",
            {"iid": instrument_id},
        )
        return cur.fetchone() is not None


def _has_filings_coverage(conn: psycopg.Connection[object], instrument_id: int) -> bool:
    """True if any filings provider has filed filings for the instrument.

    Provider-agnostic coverage gate (#503 PR 2). Today the only
    populated provider is SEC, so this is currently equivalent to
    ``EXISTS row in filing_events``. Once Companies House / other
    regional providers are wired, the same gate keeps working
    without a frontend change.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM filing_events WHERE instrument_id = %(iid)s LIMIT 1",
            {"iid": instrument_id},
        )
        return cur.fetchone() is not None


def _fetch_local_fundamentals(
    conn: psycopg.Connection[object],
    instrument_id: int,
) -> dict[str, Decimal | None]:
    """Return the latest fundamentals_snapshot row as a dict of derivable
    fields. Missing rows yield an empty dict — callers treat it as
    'no local fundamentals'.

    Pulls:
      - eps (for PE ratio with current price)
      - book_value (per-share, for PB ratio with current price)
      - net_debt / cash / debt (for debt_to_equity with equity from
        financial_periods)

    Complemented by the latest financial_periods row (TTM-ish) for ROE,
    ROA, revenue growth — data the snapshot table doesn't carry.
    """
    out: dict[str, Decimal | None] = {}
    try:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT eps, book_value, shares_outstanding, cash, debt, net_debt, revenue_ttm
                FROM fundamentals_snapshot
                WHERE instrument_id = %(iid)s
                ORDER BY as_of_date DESC
                LIMIT 1
                """,
                {"iid": instrument_id},
            )
            snap = cur.fetchone()
            cur.execute(
                """
                SELECT net_income, shareholders_equity, total_assets, total_liabilities, revenue
                FROM financial_periods
                WHERE instrument_id = %(iid)s
                  AND superseded_at IS NULL
                  AND period_type IN ('Q1', 'Q2', 'Q3', 'Q4', 'FY')
                ORDER BY period_end_date DESC
                LIMIT 1
                """,
                {"iid": instrument_id},
            )
            fp = cur.fetchone()
    except psycopg.Error:
        # DB errors masquerading as "no local data" would be invisible
        # in the source map. Log the failure loudly; caller treats the
        # empty dict as "stats unavailable".
        logger.warning(
            "_fetch_local_fundamentals: DB query failed for instrument_id=%d",
            instrument_id,
            exc_info=True,
        )
        return {}
    if snap is not None:
        out["eps"] = snap.get("eps")  # type: ignore[union-attr]
        out["book_value"] = snap.get("book_value")  # type: ignore[union-attr]
        out["shares_outstanding"] = snap.get("shares_outstanding")  # type: ignore[union-attr]
        out["net_debt"] = snap.get("net_debt")  # type: ignore[union-attr]
        out["debt"] = snap.get("debt")  # type: ignore[union-attr]
    if fp is not None:
        out["net_income"] = fp.get("net_income")  # type: ignore[union-attr]
        out["shareholders_equity"] = fp.get("shareholders_equity")  # type: ignore[union-attr]
        out["total_assets"] = fp.get("total_assets")  # type: ignore[union-attr]
        out["total_liabilities"] = fp.get("total_liabilities")  # type: ignore[union-attr]
        out["revenue"] = fp.get("revenue")  # type: ignore[union-attr]
    return out


def _build_local_stats(
    local: dict[str, Decimal | None],
    current_price: Decimal | None,
    dividend_yield: Decimal | None,
) -> InstrumentKeyStats | None:
    """Build an :class:`InstrumentKeyStats` from SEC XBRL-derived data only.

    Per the settled provider strategy (#498/#499), yfinance is no
    longer consulted; key stats are computed exclusively from local
    SEC data:

      - pe_ratio        = current_price / eps
      - pb_ratio        = current_price / book_value
      - debt_to_equity  = debt / shareholders_equity
      - roe             = net_income / shareholders_equity
      - roa             = net_income / total_assets
      - dividend_yield  = passed in from
        ``instrument_dividend_summary.ttm_yield_pct`` (#426)

    Fields the local SEC pipeline doesn't yet produce (payout ratio,
    revenue / earnings growth YoY) return ``None`` with
    ``field_source = "unavailable"`` until those derivations land.
    """
    if not local and current_price is None and dividend_yield is None:
        return None

    field_source: dict[str, KeyStatsFieldSource] = {}

    def _pick(field: str, local_value: Decimal | None, *, source: KeyStatsFieldSource = "sec_xbrl") -> Decimal | None:
        if local_value is not None:
            field_source[field] = source
            return local_value
        field_source[field] = "unavailable"
        return None

    def _safe_div(num: Decimal | None, denom: Decimal | None) -> Decimal | None:
        if num is None or denom is None or denom == 0:
            return None
        try:
            return num / denom
        except ArithmeticError, InvalidOperation:
            return None

    local_pe = _safe_div(current_price, local.get("eps"))
    local_pb = _safe_div(current_price, local.get("book_value"))
    local_de = _safe_div(local.get("debt"), local.get("shareholders_equity"))
    local_roe = _safe_div(local.get("net_income"), local.get("shareholders_equity"))
    local_roa = _safe_div(local.get("net_income"), local.get("total_assets"))

    # When local EPS / book_value are present but current_price is not,
    # pe/pb remain unresolvable — but that's "waiting on price", not
    # "no local data". Surface that distinction so the UI can render a
    # "price missing" hint rather than an ambiguous em-dash.
    price_blocked_pe = local_pe is None and current_price is None and local.get("eps") is not None
    price_blocked_pb = local_pb is None and current_price is None and local.get("book_value") is not None

    pe_final = _pick("pe_ratio", local_pe)
    pb_final = _pick("pb_ratio", local_pb)
    div_final = _pick("dividend_yield", dividend_yield, source="sec_dividend_summary")
    payout_final = _pick("payout_ratio", None)
    roe_final = _pick("roe", local_roe)
    roa_final = _pick("roa", local_roa)
    de_final = _pick("debt_to_equity", local_de)
    rev_growth_final = _pick("revenue_growth_yoy", None)
    earn_growth_final = _pick("earnings_growth_yoy", None)

    # Rewrite pe/pb source when SEC data exists but the current price
    # is missing — distinguishes "waiting on price" from "genuinely
    # unavailable". Must run BEFORE Pydantic copies the dict into the
    # model field.
    if price_blocked_pe and pe_final is None:
        field_source["pe_ratio"] = "sec_xbrl_price_missing"
    if price_blocked_pb and pb_final is None:
        field_source["pb_ratio"] = "sec_xbrl_price_missing"

    return InstrumentKeyStats(
        pe_ratio=pe_final,
        pb_ratio=pb_final,
        dividend_yield=div_final,
        payout_ratio=payout_final,
        roe=roe_final,
        roa=roa_final,
        debt_to_equity=de_final,
        revenue_growth_yoy=rev_growth_final,
        earnings_growth_yoy=earn_growth_final,
        field_source=field_source,
    )


@router.get("/{symbol}/summary", response_model=InstrumentSummary)
def get_instrument_summary(
    symbol: str,
    conn: psycopg.Connection[object] = Depends(get_conn),
    instrument_id: int | None = Query(default=None, ge=1, alias="id"),
) -> InstrumentSummary:
    """Per-ticker research summary.

    Sources, per the settled provider strategy (#498/#499 — yfinance
    retired):

    - Identity: local ``instruments`` row.
    - Price: ``quotes`` table (eToro WS / market data refresh).
    - Market cap: SEC XBRL share count × ``quotes.last``.
    - Key stats: SEC XBRL via ``financial_periods_ttm`` +
      ``instrument_dividend_summary``.

    Fields with no canonical eToro / SEC source today (52-week range,
    day change, payout ratio, growth YoY) return ``None`` rather
    than reaching for an unsanctioned provider. The frontend renders
    "—" for those until a follow-up wires SEC-derived computations.

    A missing symbol returns 404. ``?id=<instrument_id>`` override:
    when a symbol collides across exchanges, the caller can pin a
    specific instrument_id. The server verifies that id's symbol
    matches the path symbol — a mismatch is a 404, not a silent
    wrong-instrument response. See
    docs/superpowers/specs/2026-04-20-per-stock-research-page.md §2.
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    # `symbol` is not UNIQUE across exchanges (see migration 043);
    # ORDER BY `is_primary_listing DESC, instrument_id ASC` makes the
    # winner deterministic when two listings share a ticker.
    if instrument_id is not None:
        # instrument_id is the PK so the lookup is already unique; no
        # ORDER BY / LIMIT needed.
        lookup_sql = """
            SELECT i.instrument_id, i.symbol, i.company_name, i.exchange,
                   i.currency, i.sector, i.industry, i.country,
                   i.is_tradable, c.coverage_tier,
                   q.bid, q.ask, q.last
            FROM instruments i
            LEFT JOIN coverage c USING (instrument_id)
            LEFT JOIN quotes q USING (instrument_id)
            WHERE i.instrument_id = %(id)s AND UPPER(i.symbol) = %(symbol)s
        """
        params: dict[str, object] = {"id": instrument_id, "symbol": symbol_clean}
    else:
        lookup_sql = """
            SELECT i.instrument_id, i.symbol, i.company_name, i.exchange,
                   i.currency, i.sector, i.industry, i.country,
                   i.is_tradable, c.coverage_tier,
                   q.bid, q.ask, q.last
            FROM instruments i
            LEFT JOIN coverage c USING (instrument_id)
            LEFT JOIN quotes q USING (instrument_id)
            WHERE UPPER(i.symbol) = %(symbol)s
            ORDER BY i.is_primary_listing DESC, i.instrument_id ASC
            LIMIT 1
        """
        params = {"symbol": symbol_clean}
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(lookup_sql, params)
        row = cur.fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    # Identity: local DB only. Fields the operator's universe sync
    # left null stay null — no third-party fill-in.
    identity = InstrumentIdentity(
        symbol=row["symbol"],  # type: ignore[arg-type]
        display_name=row["company_name"] or None,  # type: ignore[arg-type]
        sector=row["sector"],  # type: ignore[arg-type]
        industry=row["industry"],  # type: ignore[arg-type]
        exchange=row["exchange"],  # type: ignore[arg-type]
        country=row["country"],  # type: ignore[arg-type]
        currency=row["currency"],  # type: ignore[arg-type]
        market_cap=None,
    )

    # Price: ``quotes`` row. ``last`` is preferred, ``bid`` is the
    # fallback when last hasn't been written (some providers only
    # publish bid/ask). Day change + 52w range stay null until a
    # SEC-derived computation lands; the frontend renders "—".
    quote_last = row.get("last")
    quote_bid = row.get("bid")
    current_price: Decimal | None = None
    if quote_last is not None:
        current_price = Decimal(str(quote_last))
    elif quote_bid is not None:
        current_price = Decimal(str(quote_bid))
    price_block = (
        InstrumentPrice(
            current=current_price,
            day_change=None,
            day_change_pct=None,
            week_52_high=None,
            week_52_low=None,
            currency=row["currency"],  # type: ignore[arg-type]
        )
        if current_price is not None
        else None
    )

    instrument_id_int = int(row["instrument_id"])  # type: ignore[arg-type]
    local_fundamentals: dict[str, Decimal | None] = {}
    use_local_sec = _has_sec_cik(conn, instrument_id_int)
    if use_local_sec:
        local_fundamentals = _fetch_local_fundamentals(conn, instrument_id_int)

    # Market cap: SEC XBRL share count × eToro close (`compute_market_cap`).
    # Returns None for instruments without SEC coverage; market cap
    # then stays null on the identity rather than reaching for a
    # non-canonical source.
    try:
        from app.services.xbrl_derived_stats import compute_market_cap

        computed_cap = compute_market_cap(conn, instrument_id=instrument_id_int)
    except Exception:
        logger.warning("compute_market_cap failed", exc_info=True)
        computed_cap = None
    if computed_cap is not None:
        identity = identity.model_copy(update={"market_cap": computed_cap.value})

    # Dividend yield: SEC dividend summary (#426). Empty when the
    # instrument has never paid a dividend; key stats path falls
    # through cleanly.
    dividend_yield: Decimal | None = None
    try:
        from app.services.dividends import get_dividend_summary

        div_summary = get_dividend_summary(conn, instrument_id=instrument_id_int)
        dividend_yield = div_summary.ttm_yield_pct
    except Exception:
        logger.warning("get_dividend_summary failed", exc_info=True)

    has_local_values = any(v is not None for v in local_fundamentals.values())
    # Build the stats block when ANY input is available — SEC
    # fundamentals, dividend yield, or both. Without this gate, an
    # instrument with a dividend yield but no SEC fundamentals row
    # would silently drop the yield (Codex round 2 finding on PR for
    # #498/#499).
    if (use_local_sec and has_local_values) or dividend_yield is not None:
        stats_block = _build_local_stats(
            local_fundamentals,
            current_price=current_price,
            dividend_yield=dividend_yield,
        )
        # Block-level source label reflects which inputs actually
        # contributed: combined when both SEC fundamentals and the
        # dividend summary are populated, individual when only one
        # provided values. Avoids mislabelling a dividend-only block
        # as ``sec_xbrl`` (Codex review on PR for #499).
        if use_local_sec and has_local_values and dividend_yield is not None:
            key_stats_source = "sec_xbrl+sec_dividend_summary"
        elif use_local_sec and has_local_values:
            key_stats_source = "sec_xbrl"
        else:
            key_stats_source = "sec_dividend_summary"
    else:
        stats_block = None
        key_stats_source = "unavailable"

    source = {
        "identity": "local_db",
        "price": "quotes" if current_price is not None else "unavailable",
        "key_stats": key_stats_source,
    }

    # Coverage gates (#503 PR 2). Resolve via dedicated calls — do
    # NOT alias ``use_local_sec`` here. Today the two flags are the
    # same boolean (both come from ``_has_sec_cik``), but
    # ``use_local_sec`` is named for the local-XBRL preference path
    # and could narrow in future (e.g. "has CIK AND has ingested
    # XBRL"). The frontend gate must follow ``_has_sec_cik`` exactly,
    # not whatever predicate the local-pref path wants. Codex
    # review on PR #506 caught the aliasing risk.
    has_sec_cik = _has_sec_cik(conn, instrument_id_int)
    has_filings_coverage = _has_filings_coverage(conn, instrument_id_int)

    # Per-capability resolution (#515 PR 3). Frontend gates panels
    # on providers + data_present rather than the older has_sec_cik
    # / has_filings_coverage shim. Latter kept in the response for
    # the migration window — PR 3b retires them once frontend reads
    # ``capabilities`` directly.
    capabilities = _resolve_capabilities_payload(
        conn,
        instrument_id=instrument_id_int,
        exchange_id=identity.exchange,
    )

    return InstrumentSummary(
        instrument_id=row["instrument_id"],  # type: ignore[arg-type]
        is_tradable=row["is_tradable"],  # type: ignore[arg-type]
        coverage_tier=row["coverage_tier"],  # type: ignore[arg-type]
        identity=identity,
        price=price_block,
        key_stats=stats_block,
        source=source,
        has_sec_cik=has_sec_cik,
        has_filings_coverage=has_filings_coverage,
        capabilities=capabilities,
    )


def _resolve_capabilities_payload(
    conn: psycopg.Connection[object],
    *,
    instrument_id: int,
    exchange_id: str | None,
) -> dict[str, CapabilityCellPayload]:
    """Translate the resolver dataclass into the Pydantic payload.

    When the instrument's exchange is unknown (e.g. NULL exchange
    column from a partial universe sync), every capability comes
    back with empty ``providers`` — the resolver still runs so the
    response shape is uniform.
    """
    from app.services.capabilities import resolve_capabilities

    # Pass an unmatchable sentinel for NULL-exchange rows so the
    # resolver still runs — per-instrument augmentation via
    # ``external_identifiers`` (e.g. a SEC CIK) must flow through
    # even when the exchange row is missing. Codex round-1 finding
    # on PR #5XX: returning empty cells short-circuited the SEC
    # CIK augment and contradicted has_sec_cik on partially-synced
    # instruments.
    resolved = resolve_capabilities(
        conn,
        instrument_id=instrument_id,
        exchange_id=exchange_id if exchange_id is not None else "",
    )
    return {
        cap: CapabilityCellPayload(
            providers=list(cell.providers),
            data_present=cell.data_present,
        )
        for cap, cell in resolved.cells.items()
    }


@router.get("/{instrument_id}", response_model=InstrumentDetail)
def get_instrument(
    instrument_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentDetail:
    """Single instrument with latest quote, coverage tier, and external identifiers."""
    instrument_sql = """
        SELECT i.instrument_id, i.symbol, i.company_name, i.exchange,
               i.currency, i.sector, i.industry, i.country,
               i.is_tradable, i.first_seen_at, i.last_seen_at,
               c.coverage_tier,
               q.bid, q.ask, q.last, q.spread_pct, q.quoted_at
        FROM instruments i
        LEFT JOIN quotes q USING (instrument_id)
        LEFT JOIN coverage c USING (instrument_id)
        WHERE i.instrument_id = %(instrument_id)s
    """

    identifiers_sql = """
        SELECT provider, identifier_type, identifier_value
        FROM external_identifiers
        WHERE instrument_id = %(instrument_id)s
        ORDER BY provider, identifier_type, identifier_value
    """

    params = {"instrument_id": instrument_id}

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(instrument_sql, params)
        row = cur.fetchone()

        if row is None:
            raise HTTPException(status_code=404, detail=f"Instrument {instrument_id} not found")

        cur.execute(identifiers_sql, params)
        id_rows = cur.fetchall()

    ext_ids = [
        ExternalIdentifier(
            provider=r["provider"],  # type: ignore[arg-type]
            identifier_type=r["identifier_type"],  # type: ignore[arg-type]
            identifier_value=r["identifier_value"],  # type: ignore[arg-type]
        )
        for r in id_rows
    ]

    return InstrumentDetail(
        instrument_id=row["instrument_id"],  # type: ignore[arg-type]
        symbol=row["symbol"],  # type: ignore[arg-type]
        company_name=row["company_name"],  # type: ignore[arg-type]
        exchange=row["exchange"],  # type: ignore[arg-type]
        currency=row["currency"],  # type: ignore[arg-type]
        sector=row["sector"],  # type: ignore[arg-type]
        industry=row["industry"],  # type: ignore[arg-type]
        country=row["country"],  # type: ignore[arg-type]
        is_tradable=row["is_tradable"],  # type: ignore[arg-type]
        first_seen_at=row["first_seen_at"],  # type: ignore[arg-type]
        last_seen_at=row["last_seen_at"],  # type: ignore[arg-type]
        coverage_tier=row["coverage_tier"],  # type: ignore[arg-type]
        latest_quote=_parse_quote(row),
        external_identifiers=ext_ids,
    )
