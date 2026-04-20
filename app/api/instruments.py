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
    # Per-field provenance map — present only when stats came from a mixed
    # local+yfinance merge. None when the whole block is yfinance-only.
    field_source: dict[str, str] | None = None


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

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    # -- COUNT query -------------------------------------------------------
    # Only join tables that the active filters require.
    # Uses filter_params only — no limit/offset keys that the COUNT has no placeholders for.
    count_needs_coverage = coverage_tier is not None
    count_join = "LEFT JOIN coverage c USING (instrument_id)" if count_needs_coverage else ""
    count_sql = f"SELECT COUNT(*) AS cnt FROM instruments i {count_join}{where_sql}"  # noqa: S608  — hardcoded fragments only

    # -- Items query -------------------------------------------------------
    items_params: dict[str, object] = {**filter_params, "limit": limit, "offset": offset}
    items_sql = f"""SELECT i.instrument_id, i.symbol, i.company_name, i.exchange,
               i.currency, i.sector, i.is_tradable,
               c.coverage_tier,
               q.bid, q.ask, q.last, q.spread_pct, q.quoted_at
        FROM instruments i
        LEFT JOIN quotes q USING (instrument_id)
        LEFT JOIN coverage c USING (instrument_id)
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

    # Resolve symbol -> instrument_id for the local read.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT instrument_id, symbol FROM instruments WHERE UPPER(symbol) = %(s)s LIMIT 1",
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

    field_source: dict[str, str] = {}

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
) -> InstrumentSummary:
    """Per-ticker research summary (Phase 2.2 of the 2026-04-19 refocus).

    Merges local identity/tier data with yfinance-sourced price + key
    stats. A missing symbol returns 404. yfinance failures return null
    sections rather than 500 — the UI renders what it has.
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    lookup_sql = """
        SELECT i.instrument_id, i.symbol, i.company_name, i.exchange,
               i.currency, i.sector, i.industry, i.country,
               i.is_tradable, c.coverage_tier
        FROM instruments i
        LEFT JOIN coverage c USING (instrument_id)
        WHERE UPPER(i.symbol) = %(symbol)s
        LIMIT 1
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(lookup_sql, {"symbol": symbol_clean})
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
    # empty string (defence-in-depth).
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
