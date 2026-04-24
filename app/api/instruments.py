"""Instrument list and detail API endpoints.

Reads from:
  - instruments          (core instrument metadata)
  - quotes               (1:1 current snapshot per instrument, overwritten each refresh)
  - coverage             (1:1 coverage tier per instrument)
  - external_identifiers (1:N provider-native identifiers per instrument)

No writes. No schema changes.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Literal

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.db import get_conn
from app.providers.implementations.yfinance_provider import YFinanceKeyStats, YFinanceProvider

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/instruments", tags=["instruments"])


def get_yfinance_provider() -> YFinanceProvider:
    """FastAPI dependency: constructs a fresh YFinanceProvider per request.

    The provider is stateless, so there is no pooling concern. Tests
    override this via ``app.dependency_overrides`` to inject a stub.
    """
    return YFinanceProvider()


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
KeyStatsFieldSource = Literal[
    "sec_xbrl",
    "yfinance",
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
    period_type: str  # Q1/Q2/Q3/Q4/FY (from financial_periods) or yfinance label
    values: dict[str, Decimal | None]


class InstrumentFinancials(BaseModel):
    symbol: str
    statement: str  # "income" | "balance" | "cashflow"
    period: str  # "quarterly" | "annual"
    currency: str | None
    source: str  # "financial_periods" (local SEC XBRL) | "yfinance"
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
    range: Literal["1w", "1m", "3m", "6m", "1y", "5y", "max"]  # noqa: A003
    days: int | None  # None when range="max"
    rows: list[CandleBar]


class InstrumentSummary(BaseModel):
    """Per-ticker research summary (Phase 2.2).

    Identity comes from the local ``instruments`` row; price + key stats
    come from yfinance. All leaf fields are nullable so the UI can render
    partial data when a provider degrades.

    ``source`` reports which provider populated each section so the
    future-spec per-field attribution (SEC EDGAR / Finnhub / yfinance)
    has a landing spot — right now everything non-identity is yfinance,
    so the map is simple; phase 2.3 will expand it.
    """

    instrument_id: int
    is_tradable: bool
    coverage_tier: int | None
    identity: InstrumentIdentity
    price: InstrumentPrice | None
    key_stats: InstrumentKeyStats | None
    source: dict[str, str]


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

    Returns (rows, currency). Empty rows = no local data, let caller fall
    back to yfinance.
    """
    period_types: list[str] = ["Q1", "Q2", "Q3", "Q4"] if period == "quarterly" else ["FY"]

    # Columns whitelisted above — safe to format into SQL. period_types
    # is bound as a parameter, not formatted, so a future value added to
    # the CHECK constraint won't silently match.
    select_cols = ", ".join(columns)
    sql = f"""
        SELECT period_end_date, period_type, reported_currency, {select_cols}
        FROM financial_periods
        WHERE instrument_id = %(iid)s
          AND superseded_at IS NULL
          AND period_type = ANY(%(types)s::text[])
        ORDER BY period_end_date DESC
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
    yfinance_provider: YFinanceProvider = Depends(get_yfinance_provider),
) -> InstrumentFinancials:
    """Per-ticker financial statement (Phase 2.3 of the 2026-04-19 refocus).

    Pull priority:
      1. Local ``financial_periods`` rows (SEC XBRL-sourced) if the
         instrument has them.
      2. yfinance fallback otherwise.

    Returns an empty row list (not 500, not 404) when neither source has
    data — the UI shows "no statement data available".
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

    # Try local first.
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

    # Fallback to yfinance.
    y_fin = yfinance_provider.get_financials(
        str(inst_row["symbol"]),  # type: ignore[index]
        statement=statement,
        period=period,
    )
    if y_fin is None:
        return InstrumentFinancials(
            symbol=str(inst_row["symbol"]),  # type: ignore[index]
            statement=statement,
            period=period,
            currency=None,
            source="yfinance",
            rows=[],
        )

    # yfinance statement rows don't carry a period_type label. For the
    # quarterly path we infer Q1-Q4 from the period_end month (fiscal
    # quarters end Mar/Jun/Sep/Dec for the vast majority of issuers).
    # Annual rows are tagged "FY". This matches the local-path labels so
    # the frontend treats both sources uniformly.
    def _yf_period_type(d: date) -> str:
        if period == "annual":
            return "FY"
        return {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}.get(d.month, "Q?")

    yf_rows = [
        InstrumentFinancialRow(
            period_end=row.period_end,
            period_type=_yf_period_type(row.period_end),
            values=dict(row.values.items()),
        )
        for row in y_fin.rows
    ]
    return InstrumentFinancials(
        symbol=str(inst_row["symbol"]),  # type: ignore[index]
        statement=statement,
        period=period,
        currency=y_fin.currency,
        source="yfinance",
        rows=yf_rows,
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


@router.get("/{symbol}/candles", response_model=InstrumentCandles)
def get_instrument_candles(
    symbol: str,
    range_: Literal["1w", "1m", "3m", "6m", "1y", "5y", "max"] = Query(default="1m", alias="range"),
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

    days = _CANDLE_RANGE_DAYS[range_]
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
    from app.services.business_summary import get_business_summary
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

    # #428: prefer the authoritative 10-K Item 1 body over the short
    # entity-level ``description`` from submissions.json. The
    # submissions description is a ~1-sentence blurb SEC surfaces in
    # their own UI; Item 1 is the multi-paragraph authoritative text
    # investors expect on an instrument page.
    item_1_body = get_business_summary(conn, instrument_id=instrument_id)
    description = item_1_body if item_1_body is not None else profile.description

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
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InsiderSummaryModel:
    """Return the 90-day insider-transaction summary (#429 / #458).

    Two lenses: open-market (discretionary P/S) and total-activity
    (every non-derivative transaction classified by
    ``acquired_disposed_code``). Only non-derivative trades
    contribute; derivative grants / option exercises are excluded.
    """
    from app.services.insider_transactions import get_insider_summary

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
        # empty dict as fall-through to yfinance.
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


def _merge_stats_with_local(
    yfinance_stats: YFinanceKeyStats | None,
    local: dict[str, Decimal | None],
    current_price: Decimal | None,
) -> InstrumentKeyStats | None:
    """Merge local SEC-derived stats onto a yfinance KeyStats object.

    Local data wins per-field when present:
      - pe_ratio    = current_price / eps
      - pb_ratio    = current_price / book_value
      - debt_to_equity = debt / shareholders_equity
      - roe         = net_income / shareholders_equity
      - roa         = net_income / total_assets

    Fields the local snapshot can't produce (dividend yield, payout ratio,
    revenue_growth_yoy, earnings_growth_yoy) always fall back to yfinance.
    """
    if yfinance_stats is None and not local:
        return None

    field_source: dict[str, KeyStatsFieldSource] = {}

    def _pick(field: str, local_value: Decimal | None, yfinance_value: Decimal | None) -> Decimal | None:
        if local_value is not None:
            field_source[field] = "sec_xbrl"
            return local_value
        if yfinance_value is not None:
            field_source[field] = "yfinance"
            return yfinance_value
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
    # "no local data". Surface that distinction in field_source so the
    # UI can render a "price missing" hint instead of an ambiguous em-dash.
    price_blocked_pe = local_pe is None and current_price is None and local.get("eps") is not None
    price_blocked_pb = local_pb is None and current_price is None and local.get("book_value") is not None

    pe_final = _pick("pe_ratio", local_pe, yfinance_stats.pe_ratio if yfinance_stats else None)
    pb_final = _pick("pb_ratio", local_pb, yfinance_stats.pb_ratio if yfinance_stats else None)
    div_final = _pick("dividend_yield", None, yfinance_stats.dividend_yield if yfinance_stats else None)
    payout_final = _pick("payout_ratio", None, yfinance_stats.payout_ratio if yfinance_stats else None)
    roe_final = _pick("roe", local_roe, yfinance_stats.roe if yfinance_stats else None)
    roa_final = _pick("roa", local_roa, yfinance_stats.roa if yfinance_stats else None)
    de_final = _pick(
        "debt_to_equity",
        local_de,
        yfinance_stats.debt_to_equity if yfinance_stats else None,
    )
    rev_growth_final = _pick(
        "revenue_growth_yoy",
        None,
        yfinance_stats.revenue_growth_yoy if yfinance_stats else None,
    )
    earn_growth_final = _pick(
        "earnings_growth_yoy",
        None,
        yfinance_stats.earnings_growth_yoy if yfinance_stats else None,
    )

    # Rewrite field_source for pe/pb when local SEC data exists but the
    # current price is missing — distinguishes "waiting on price" from
    # "genuinely unavailable" so the UI can render an actionable hint.
    # Rewrite BEFORE constructing the Pydantic model since Pydantic v2
    # copies the dict into the field, severing post-construction mutation.
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
    yfinance_provider: YFinanceProvider = Depends(get_yfinance_provider),
    instrument_id: int | None = Query(default=None, ge=1, alias="id"),
) -> InstrumentSummary:
    """Per-ticker research summary (Phase 2.2 of the 2026-04-19 refocus).

    Merges local identity/tier data with yfinance-sourced price + key
    stats. A missing symbol returns 404. yfinance failures return null
    sections rather than 500 — the UI renders what it has.

    `?id=<instrument_id>` override: when a symbol collides across
    exchanges, the caller can pin a specific instrument_id. The server
    verifies that id's symbol matches the path symbol — a mismatch is a
    404, not a silent wrong-instrument response. See
    docs/superpowers/specs/2026-04-20-per-stock-research-page.md §2.
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    # `symbol` is not UNIQUE across exchanges (see migration 043);
    # ORDER BY `is_primary_listing DESC, instrument_id ASC` makes the
    # winner deterministic when two listings share a ticker. See
    # docs/superpowers/specs/2026-04-20-per-stock-research-page.md §2.
    if instrument_id is not None:
        # instrument_id is the PK so the lookup is already unique; no
        # ORDER BY / LIMIT needed.
        lookup_sql = """
            SELECT i.instrument_id, i.symbol, i.company_name, i.exchange,
                   i.currency, i.sector, i.industry, i.country,
                   i.is_tradable, c.coverage_tier
            FROM instruments i
            LEFT JOIN coverage c USING (instrument_id)
            WHERE i.instrument_id = %(id)s AND UPPER(i.symbol) = %(symbol)s
        """
        params: dict[str, object] = {"id": instrument_id, "symbol": symbol_clean}
    else:
        lookup_sql = """
            SELECT i.instrument_id, i.symbol, i.company_name, i.exchange,
                   i.currency, i.sector, i.industry, i.country,
                   i.is_tradable, c.coverage_tier
            FROM instruments i
            LEFT JOIN coverage c USING (instrument_id)
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

    # One yfinance .info call instead of three — Codex review caught that
    # get_profile / get_quote / get_key_stats each triggered their own
    # network fetch. get_snapshot fetches .info once and derives all three.
    snapshot = yfinance_provider.get_snapshot(row["symbol"])  # type: ignore[arg-type]
    profile = snapshot.profile
    quote = snapshot.quote
    stats = snapshot.key_stats

    # Local DB is authoritative for every identity field that has a non-null
    # value — yfinance fills only the gaps. company_name is schema-non-null,
    # so display_name falls to yfinance only if the local row somehow has an
    # empty string (defence-in-depth). SEC-sourced entity metadata
    # (description, SIC, exchanges, former names) is exposed via the
    # dedicated ``GET /instruments/{symbol}/sec_profile`` endpoint (#427);
    # the frontend consumes both endpoints in parallel so this handler's
    # query pattern stays unchanged.
    identity = InstrumentIdentity(
        symbol=row["symbol"],  # type: ignore[arg-type]
        display_name=row["company_name"] or (profile.display_name if profile is not None else None),  # type: ignore[arg-type]
        sector=row["sector"] or (profile.sector if profile is not None else None),  # type: ignore[arg-type]
        industry=row["industry"] or (profile.industry if profile is not None else None),  # type: ignore[arg-type]
        exchange=row["exchange"] or (profile.exchange if profile is not None else None),  # type: ignore[arg-type]
        country=row["country"] or (profile.country if profile is not None else None),  # type: ignore[arg-type]
        currency=row["currency"] or (profile.currency if profile is not None else None),  # type: ignore[arg-type]
        market_cap=profile.market_cap if profile is not None else None,
    )

    price_block = (
        InstrumentPrice(
            current=quote.price,
            day_change=quote.day_change,
            day_change_pct=quote.day_change_pct,
            week_52_high=quote.week_52_high,
            week_52_low=quote.week_52_low,
            currency=quote.currency,
        )
        if quote is not None
        else None
    )

    # Prefer local SEC XBRL fields for US tickers (#357). Local values
    # override yfinance per-field; fields the snapshot can't compute
    # (dividend yield, growth) fall back to yfinance cleanly.
    instrument_id_int = int(row["instrument_id"])  # type: ignore[arg-type]
    local_fundamentals: dict[str, Decimal | None] = {}
    use_local_sec = _has_sec_cik(conn, instrument_id_int)
    if use_local_sec:
        local_fundamentals = _fetch_local_fundamentals(conn, instrument_id_int)

    # #432 (yfinance retire, batch 1): prefer compute-from-XBRL market
    # cap over yfinance when SEC has a fresh share count. Runs after
    # the existing local-fundamentals block so its cursor slot is the
    # LAST in the sequence — keeps the test harness shape stable.
    # Returns None for non-US / pre-seed; we then fall back to
    # whatever yfinance provided on the identity built above.
    try:
        from app.services.xbrl_derived_stats import compute_market_cap

        computed_cap = compute_market_cap(conn, instrument_id=instrument_id_int)
    except Exception:
        logger.warning("compute_market_cap failed", exc_info=True)
        computed_cap = None
    if computed_cap is not None:
        identity = identity.model_copy(update={"market_cap": computed_cap.value})

    # Treat a dict of all-None as "no local data" — the fundamentals_snapshot
    # table may exist with sparse rows during bootstrap.
    has_local_values = any(v is not None for v in local_fundamentals.values())
    if use_local_sec and has_local_values:
        stats_block = _merge_stats_with_local(
            stats,
            local_fundamentals,
            current_price=(quote.price if quote is not None else None),
        )
        key_stats_source = "local_sec_xbrl+yfinance"
    elif stats is not None:
        stats_block = InstrumentKeyStats(
            pe_ratio=stats.pe_ratio,
            pb_ratio=stats.pb_ratio,
            dividend_yield=stats.dividend_yield,
            payout_ratio=stats.payout_ratio,
            roe=stats.roe,
            roa=stats.roa,
            debt_to_equity=stats.debt_to_equity,
            revenue_growth_yoy=stats.revenue_growth_yoy,
            earnings_growth_yoy=stats.earnings_growth_yoy,
            field_source=None,
        )
        key_stats_source = "yfinance"
    else:
        stats_block = None
        key_stats_source = "unavailable"

    source = {
        "identity": "local_db+yfinance",
        "price": "yfinance" if quote is not None else "unavailable",
        "key_stats": key_stats_source,
    }

    return InstrumentSummary(
        instrument_id=row["instrument_id"],  # type: ignore[arg-type]
        is_tradable=row["is_tradable"],  # type: ignore[arg-type]
        coverage_tier=row["coverage_tier"],  # type: ignore[arg-type]
        identity=identity,
        price=price_block,
        key_stats=stats_block,
        source=source,
    )


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
