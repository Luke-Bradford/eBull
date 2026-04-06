"""Recommendations API endpoints.

Reads from:
  - trade_recommendations  (append-oriented recommendation history)
  - instruments             (symbol, company_name for display)
  - scores                  (score_id FK — joined for total_score on detail)

No writes. No schema changes.

HOLD dedup semantics:
  The list endpoint returns raw history but filters out consecutive HOLD rows
  for the same instrument.  If instrument X has HOLD at t1, HOLD at t2,
  HOLD at t3, only the latest (t3) is returned.  Non-HOLD actions are
  always returned.  This is implemented via a window function:
  ROW_NUMBER() OVER (PARTITION BY instrument_id, action ORDER BY created_at DESC)
  filtered to rn=1 only for HOLD rows.

Ordering: created_at DESC, recommendation_id DESC (newest first, deterministic).
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.api._helpers import parse_optional_float, parse_optional_int
from app.db import get_conn

router = APIRouter(prefix="/recommendations", tags=["recommendations"])

MAX_PAGE_LIMIT = 200

Action = Literal["BUY", "ADD", "HOLD", "EXIT"]
Status = Literal["proposed", "approved", "rejected", "executed"]


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class RecommendationListItem(BaseModel):
    recommendation_id: int
    instrument_id: int
    symbol: str
    company_name: str
    action: str
    status: str
    rationale: str
    score_id: int | None
    model_version: str | None
    suggested_size_pct: float | None
    target_entry: float | None
    cash_balance_known: bool | None
    created_at: datetime


class RecommendationsListResponse(BaseModel):
    items: list[RecommendationListItem]
    total: int
    offset: int
    limit: int


class RecommendationDetail(BaseModel):
    recommendation_id: int
    instrument_id: int
    symbol: str
    company_name: str
    action: str
    status: str
    rationale: str
    score_id: int | None
    model_version: str | None
    suggested_size_pct: float | None
    target_entry: float | None
    cash_balance_known: bool | None
    total_score: float | None
    created_at: datetime


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_list_item(row: dict[str, object]) -> RecommendationListItem:
    return RecommendationListItem(
        recommendation_id=row["recommendation_id"],  # type: ignore[arg-type]
        instrument_id=row["instrument_id"],  # type: ignore[arg-type]
        symbol=row["symbol"],  # type: ignore[arg-type]
        company_name=row["company_name"],  # type: ignore[arg-type]
        action=row["action"],  # type: ignore[arg-type]
        status=row["status"],  # type: ignore[arg-type]
        rationale=row["rationale"],  # type: ignore[arg-type]
        score_id=parse_optional_int(row, "score_id"),
        model_version=row["model_version"],  # type: ignore[arg-type]
        suggested_size_pct=parse_optional_float(row, "suggested_size_pct"),
        target_entry=parse_optional_float(row, "target_entry"),
        cash_balance_known=row["cash_balance_known"],  # type: ignore[arg-type]
        created_at=row["created_at"],  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


# The CTE deduplicates consecutive HOLDs: for each (instrument_id, action='HOLD')
# group, only the latest row (by created_at DESC, recommendation_id DESC) is kept.
# Non-HOLD rows always pass through (rn is set to 1 unconditionally).
_DEDUPED_CTE = """
WITH deduped AS (
    SELECT r.*,
           CASE
               WHEN r.action = 'HOLD' THEN
                   ROW_NUMBER() OVER (
                       PARTITION BY r.instrument_id, r.action
                       ORDER BY r.created_at DESC, r.recommendation_id DESC
                   )
               ELSE 1
           END AS rn
    FROM trade_recommendations r
)
"""


@router.get("", response_model=RecommendationsListResponse)
def list_recommendations(
    conn: psycopg.Connection[object] = Depends(get_conn),
    action: Action | None = Query(default=None),
    status: Status | None = Query(default=None),
    instrument_id: int | None = Query(default=None),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=MAX_PAGE_LIMIT),
) -> RecommendationsListResponse:
    """Paginated recommendation history with HOLD dedup.

    Filters:
      - action: exact match (BUY, ADD, HOLD, EXIT)
      - status: exact match (proposed, approved, rejected, executed)
      - instrument_id: exact match

    Ordering: created_at DESC, recommendation_id DESC (newest first).

    HOLD dedup: for each instrument, only the latest HOLD is returned.
    Non-HOLD actions are always returned.
    """
    where_clauses: list[str] = ["d.rn = 1"]
    filter_params: dict[str, object] = {}

    if action is not None:
        where_clauses.append("d.action = %(action)s")
        filter_params["action"] = action
    if status is not None:
        where_clauses.append("d.status = %(status)s")
        filter_params["status"] = status
    if instrument_id is not None:
        where_clauses.append("d.instrument_id = %(instrument_id)s")
        filter_params["instrument_id"] = instrument_id

    where_sql = " WHERE " + " AND ".join(where_clauses)

    # -- COUNT query (separate params — no limit/offset) -------------------
    # S608 safe: all SQL fragments are hardcoded; user input is parameterised via %(...)s.
    count_sql = f"""{_DEDUPED_CTE}
        SELECT COUNT(*) AS cnt
        FROM deduped d
        JOIN instruments i USING (instrument_id)
        {where_sql}"""  # noqa: S608

    # -- Items query -------------------------------------------------------
    items_params: dict[str, object] = {
        **filter_params,
        "limit": limit,
        "offset": offset,
    }
    # S608 safe: all SQL fragments are hardcoded; user input is parameterised via %(...)s.
    items_sql = f"""{_DEDUPED_CTE}
        SELECT d.recommendation_id, d.instrument_id,
               i.symbol, i.company_name,
               d.action, d.status, d.rationale,
               d.score_id, d.model_version,
               d.suggested_size_pct, d.target_entry,
               d.cash_balance_known, d.created_at
        FROM deduped d
        JOIN instruments i USING (instrument_id)
        {where_sql}
        ORDER BY d.created_at DESC, d.recommendation_id DESC
        LIMIT %(limit)s OFFSET %(offset)s"""  # noqa: S608

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(count_sql, filter_params)  # type: ignore[arg-type]
        count_row = cur.fetchone()
        # COUNT() always returns exactly one row.
        total: int = count_row["cnt"] if count_row else 0  # type: ignore[index]

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(items_sql, items_params)  # type: ignore[arg-type]
        rows = cur.fetchall()

    items = [_parse_list_item(r) for r in rows]

    return RecommendationsListResponse(
        items=items,
        total=total,
        offset=offset,
        limit=limit,
    )


@router.get("/{recommendation_id}", response_model=RecommendationDetail)
def get_recommendation(
    recommendation_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> RecommendationDetail:
    """Single recommendation detail with linked score total_score."""
    sql = """
        SELECT r.recommendation_id, r.instrument_id,
               i.symbol, i.company_name,
               r.action, r.status, r.rationale,
               r.score_id, r.model_version,
               r.suggested_size_pct, r.target_entry,
               r.cash_balance_known, r.created_at,
               s.total_score
        FROM trade_recommendations r
        JOIN instruments i USING (instrument_id)
        LEFT JOIN scores s USING (score_id)
        WHERE r.recommendation_id = %(recommendation_id)s
    """
    params = {"recommendation_id": recommendation_id}

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Recommendation {recommendation_id} not found",
        )

    return RecommendationDetail(
        recommendation_id=row["recommendation_id"],  # type: ignore[arg-type]
        instrument_id=row["instrument_id"],  # type: ignore[arg-type]
        symbol=row["symbol"],  # type: ignore[arg-type]
        company_name=row["company_name"],  # type: ignore[arg-type]
        action=row["action"],  # type: ignore[arg-type]
        status=row["status"],  # type: ignore[arg-type]
        rationale=row["rationale"],  # type: ignore[arg-type]
        score_id=parse_optional_int(row, "score_id"),
        model_version=row["model_version"],  # type: ignore[arg-type]
        suggested_size_pct=parse_optional_float(row, "suggested_size_pct"),
        target_entry=parse_optional_float(row, "target_entry"),
        cash_balance_known=row["cash_balance_known"],  # type: ignore[arg-type]
        total_score=parse_optional_float(row, "total_score"),
        created_at=row["created_at"],  # type: ignore[arg-type]
    )
