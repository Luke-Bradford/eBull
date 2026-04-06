"""News feed API endpoint.

Reads from:
  - news_events   (per-instrument news with sentiment, importance, snippet)
  - instruments    (symbol, company_name for display context)

No writes. No schema changes.

Deduplication:
  The ``(instrument_id, url_hash)`` unique constraint in the DB guarantees
  deduplicated results without additional query logic.

Pagination:
  Cursor-style by ``event_time``.  Default window is the last 30 days.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.api._helpers import parse_optional_float
from app.db import get_conn

router = APIRouter(prefix="/news", tags=["news"])

MAX_PAGE_LIMIT = 200
_DEFAULT_DAYS = 30


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class NewsItem(BaseModel):
    """Single news event for an instrument."""

    news_event_id: int
    instrument_id: int
    event_time: datetime
    source: str | None
    headline: str
    category: str | None
    sentiment_score: float | None
    importance_score: float | None
    snippet: str | None
    url: str | None


class NewsListResponse(BaseModel):
    instrument_id: int
    symbol: str | None
    items: list[NewsItem]
    total: int
    offset: int
    limit: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_news_item(row: dict[str, object]) -> NewsItem:
    return NewsItem(
        news_event_id=row["news_event_id"],  # type: ignore[arg-type]
        instrument_id=row["instrument_id"],  # type: ignore[arg-type]
        event_time=row["event_time"],  # type: ignore[arg-type]
        source=row["source"],  # type: ignore[arg-type]
        headline=row["headline"],  # type: ignore[arg-type]
        category=row["category"],  # type: ignore[arg-type]
        sentiment_score=parse_optional_float(row, "sentiment_score"),
        importance_score=parse_optional_float(row, "importance_score"),
        snippet=row["snippet"],  # type: ignore[arg-type]
        url=row["url"],  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/{instrument_id}", response_model=NewsListResponse)
def list_news(
    instrument_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
    since: datetime | None = Query(default=None),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=MAX_PAGE_LIMIT),
) -> NewsListResponse:
    """Recent news events for an instrument.

    Defaults to the last 30 days if ``since`` is not provided.
    Results are ordered by ``event_time DESC`` (newest first).

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

    # Coerce naive datetime to UTC; PostgreSQL rejects mixed-offset comparisons on TIMESTAMPTZ.
    if since is not None:
        since_ts = since if since.tzinfo is not None else since.replace(tzinfo=UTC)
    else:
        since_ts = datetime.now(UTC) - timedelta(days=_DEFAULT_DAYS)

    # COUNT query — separate cursor, separate params dict.
    count_params: dict[str, object] = {
        "instrument_id": instrument_id,
        "since": since_ts,
    }
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """SELECT COUNT(*) AS cnt
               FROM news_events
               WHERE instrument_id = %(instrument_id)s
                 AND event_time >= %(since)s""",
            count_params,
        )
        # COUNT always returns exactly one row; the column value is 0 when empty.
        count_row = cur.fetchone()
        total: int = count_row["cnt"]  # type: ignore[index]

    # Items query — separate cursor, separate params dict.
    items_params: dict[str, object] = {
        "instrument_id": instrument_id,
        "since": since_ts,
        "limit": limit,
        "offset": offset,
    }
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """SELECT news_event_id, instrument_id, event_time,
                      source, headline, category,
                      sentiment_score, importance_score,
                      snippet, url
               FROM news_events
               WHERE instrument_id = %(instrument_id)s
                 AND event_time >= %(since)s
               ORDER BY event_time DESC
               LIMIT %(limit)s OFFSET %(offset)s""",
            items_params,
        )
        rows = cur.fetchall()

    items = [_parse_news_item(r) for r in rows]
    return NewsListResponse(
        instrument_id=instrument_id,
        symbol=symbol,
        items=items,
        total=total,
        offset=offset,
        limit=limit,
    )
