"""Filings feed API endpoint.

Reads from:
  - filing_events  (per-instrument filings with summary, risk score, document link)
  - instruments     (symbol, company_name for display context)

No writes. No schema changes.

Filing identity is provider-scoped (settled decision).  The API exposes
``provider`` and ``filing_type`` for display but does not expose
``provider_filing_id`` or ``raw_payload_json``.
"""

from __future__ import annotations

from datetime import date, datetime

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.api._helpers import parse_optional_float
from app.db import get_conn

router = APIRouter(prefix="/filings", tags=["filings"])

MAX_PAGE_LIMIT = 200


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class FilingItem(BaseModel):
    """Single filing event for an instrument."""

    filing_event_id: int
    instrument_id: int
    filing_date: date
    filing_type: str | None
    provider: str
    source_url: str | None
    primary_document_url: str | None
    extracted_summary: str | None
    red_flag_score: float | None
    created_at: datetime


class FilingsListResponse(BaseModel):
    instrument_id: int
    symbol: str | None
    items: list[FilingItem]
    total: int
    offset: int
    limit: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_filing_item(row: dict[str, object]) -> FilingItem:
    return FilingItem(
        filing_event_id=row["filing_event_id"],  # type: ignore[arg-type]
        instrument_id=row["instrument_id"],  # type: ignore[arg-type]
        filing_date=row["filing_date"],  # type: ignore[arg-type]
        filing_type=row["filing_type"],  # type: ignore[arg-type]
        provider=row["provider"],  # type: ignore[arg-type]
        source_url=row["source_url"],  # type: ignore[arg-type]
        primary_document_url=row["primary_document_url"],  # type: ignore[arg-type]
        extracted_summary=row["extracted_summary"],  # type: ignore[arg-type]
        red_flag_score=parse_optional_float(row, "red_flag_score"),
        created_at=row["created_at"],  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/{instrument_id}", response_model=FilingsListResponse)
def list_filings(
    instrument_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
    filing_type: str | None = Query(default=None),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=MAX_PAGE_LIMIT),
) -> FilingsListResponse:
    """Filing events for an instrument, ordered by filing_date DESC.

    Optional ``filing_type`` filter for narrowing to e.g. ``10-K``, ``10-Q``.

    Returns 404 if the instrument does not exist.
    """
    # Resolve instrument symbol for the response envelope.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT symbol FROM instruments WHERE instrument_id = %(id)s",
            {"id": instrument_id},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail="Instrument not found")

    symbol: str = inst_row["symbol"]  # type: ignore[assignment]

    # Build dynamic WHERE.
    where_clauses: list[str] = ["instrument_id = %(instrument_id)s"]
    filter_params: dict[str, object] = {"instrument_id": instrument_id}

    if filing_type is not None:
        where_clauses.append("filing_type = %(filing_type)s")
        filter_params["filing_type"] = filing_type

    where_sql = " AND ".join(where_clauses)

    # COUNT query — separate cursor, separate params dict.
    # where_sql is built from hardcoded clause strings only — not user input.
    count_sql = f"SELECT COUNT(*) AS cnt FROM filing_events WHERE {where_sql}"  # noqa: S608
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(count_sql, filter_params)  # type: ignore[arg-type]
        # COUNT always returns exactly one row; the column value is 0 when empty.
        count_row = cur.fetchone()
        total: int = count_row["cnt"]  # type: ignore[index]

    # Items query — separate cursor, separate params dict.
    items_params: dict[str, object] = {
        **filter_params,
        "limit": limit,
        "offset": offset,
    }
    items_sql = f"""SELECT filing_event_id, instrument_id, filing_date,
                       filing_type, provider,
                       source_url, primary_document_url,
                       extracted_summary, red_flag_score,
                       created_at
                FROM filing_events
                WHERE {where_sql}
                ORDER BY filing_date DESC, filing_event_id DESC
                LIMIT %(limit)s OFFSET %(offset)s"""  # noqa: S608  — where_sql is hardcoded clauses only
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(items_sql, items_params)  # type: ignore[arg-type]
        rows = cur.fetchall()

    items = [_parse_filing_item(r) for r in rows]
    return FilingsListResponse(
        instrument_id=instrument_id,
        symbol=symbol,
        items=items,
        total=total,
        offset=offset,
        limit=limit,
    )
