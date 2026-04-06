"""Thesis API endpoints.

Reads from:
  - theses       (append-only versioned thesis rows per instrument)
  - instruments   (existence check for 404 on history endpoint)

No writes. No schema changes.

Note: the issue (#52) mentions ``conviction_score`` but the theses table
has ``confidence_score``.  This module uses the actual schema column name.
"""

from __future__ import annotations

from datetime import datetime

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.db import get_conn

router = APIRouter(prefix="/theses", tags=["theses"])

MAX_PAGE_LIMIT = 200

# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class ThesisDetail(BaseModel):
    """Single thesis row with all columns including critic output."""

    thesis_id: int
    instrument_id: int
    thesis_version: int
    thesis_type: str
    stance: str
    confidence_score: float | None
    buy_zone_low: float | None
    buy_zone_high: float | None
    base_value: float | None
    bull_value: float | None
    bear_value: float | None
    break_conditions_json: list[str] | None
    memo_markdown: str
    critic_json: dict[str, object] | None
    created_at: datetime


class ThesisHistoryResponse(BaseModel):
    instrument_id: int
    items: list[ThesisDetail]
    total: int
    offset: int
    limit: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_optional_float(row: dict[str, object], key: str) -> float | None:
    """Safely cast a nullable numeric DB column to float."""
    val = row.get(key)
    if val is None:
        return None
    return float(val)  # type: ignore[arg-type]


def _parse_thesis(row: dict[str, object]) -> ThesisDetail:
    return ThesisDetail(
        thesis_id=row["thesis_id"],  # type: ignore[arg-type]
        instrument_id=row["instrument_id"],  # type: ignore[arg-type]
        thesis_version=row["thesis_version"],  # type: ignore[arg-type]
        thesis_type=row["thesis_type"],  # type: ignore[arg-type]
        stance=row["stance"],  # type: ignore[arg-type]
        confidence_score=_parse_optional_float(row, "confidence_score"),
        buy_zone_low=_parse_optional_float(row, "buy_zone_low"),
        buy_zone_high=_parse_optional_float(row, "buy_zone_high"),
        base_value=_parse_optional_float(row, "base_value"),
        bull_value=_parse_optional_float(row, "bull_value"),
        bear_value=_parse_optional_float(row, "bear_value"),
        break_conditions_json=row["break_conditions_json"],  # type: ignore[arg-type]
        memo_markdown=row["memo_markdown"],  # type: ignore[arg-type]
        critic_json=row["critic_json"],  # type: ignore[arg-type]
        created_at=row["created_at"],  # type: ignore[arg-type]
    )


_THESIS_COLUMNS = """
    t.thesis_id, t.instrument_id, t.thesis_version,
    t.thesis_type, t.stance, t.confidence_score,
    t.buy_zone_low, t.buy_zone_high,
    t.base_value, t.bull_value, t.bear_value,
    t.break_conditions_json, t.memo_markdown, t.critic_json,
    t.created_at
"""


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/{instrument_id}", response_model=ThesisDetail)
def get_latest_thesis(
    instrument_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> ThesisDetail:
    """Latest thesis for an instrument, ordered by created_at then version."""
    sql = f"""
        SELECT {_THESIS_COLUMNS}
        FROM theses t
        WHERE t.instrument_id = %(instrument_id)s
        ORDER BY t.created_at DESC, t.thesis_version DESC
        LIMIT 1
    """  # safe: _THESIS_COLUMNS is a module-level constant, not user input
    params = {"instrument_id": instrument_id}

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"No thesis found for instrument {instrument_id}",
        )

    return _parse_thesis(row)


@router.get("/{instrument_id}/history", response_model=ThesisHistoryResponse)
def get_thesis_history(
    instrument_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=MAX_PAGE_LIMIT),
) -> ThesisHistoryResponse:
    """Paginated thesis history for an instrument, newest first.

    Returns 404 if the instrument does not exist.
    Returns 200 with empty items if the instrument exists but has no theses.
    """
    # Check instrument existence first.
    exists_sql = """
        SELECT 1 FROM instruments WHERE instrument_id = %(instrument_id)s
    """
    exists_params = {"instrument_id": instrument_id}

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(exists_sql, exists_params)
        if cur.fetchone() is None:
            raise HTTPException(
                status_code=404,
                detail=f"Instrument {instrument_id} not found",
            )

        # COUNT then SELECT is a TOCTOU window, but theses is append-only
        # so total can only grow between queries — never shrink.
        # Separate params dict (prevention log: shared params).
        count_sql = """
            SELECT COUNT(*) AS cnt
            FROM theses t
            WHERE t.instrument_id = %(instrument_id)s
        """
        count_params = {"instrument_id": instrument_id}
        cur.execute(count_sql, count_params)
        # Aggregate SELECT always returns exactly one row; guard the column.
        count_row = cur.fetchone()
        total: int = int(count_row["cnt"])  # type: ignore[index,arg-type]

        if total == 0:
            return ThesisHistoryResponse(
                instrument_id=instrument_id,
                items=[],
                total=0,
                offset=offset,
                limit=limit,
            )

        # Data query — separate params dict with limit/offset.
        data_sql = f"""
            SELECT {_THESIS_COLUMNS}
            FROM theses t
            WHERE t.instrument_id = %(instrument_id)s
            ORDER BY t.created_at DESC, t.thesis_version DESC
            LIMIT %(limit)s OFFSET %(offset)s
        """  # safe: _THESIS_COLUMNS is a module-level constant, not user input
        data_params = {"instrument_id": instrument_id, "limit": limit, "offset": offset}
        cur.execute(data_sql, data_params)
        rows = cur.fetchall()

    items = [_parse_thesis(r) for r in rows]
    return ThesisHistoryResponse(
        instrument_id=instrument_id,
        items=items,
        total=total,
        offset=offset,
        limit=limit,
    )
