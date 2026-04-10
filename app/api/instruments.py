"""Instrument list and detail API endpoints.

Reads from:
  - instruments          (core instrument metadata)
  - quotes               (1:1 current snapshot per instrument, overwritten each refresh)
  - coverage             (1:1 coverage tier per instrument)
  - external_identifiers (1:N provider-native identifiers per instrument)

No writes. No schema changes.
"""

from __future__ import annotations

from datetime import datetime

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.db import get_conn

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
