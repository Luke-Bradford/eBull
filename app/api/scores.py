"""Scores and rankings API endpoints.

Reads from:
  - scores      (per-instrument per-run score breakdown, rank, rank_delta)
  - instruments  (symbol, company_name, sector for display)
  - coverage     (coverage_tier)
  - theses       (stance — only when stance filter is active)

No writes. No schema changes.

Latest-run semantics:
  ``compute_rankings`` writes all rows for a single run with one coherent
  ``scored_at`` timestamp inside a single transaction.  The rankings
  endpoint identifies the latest run via ``MAX(scored_at)`` for the
  requested ``model_version``.  If no run exists, both endpoints return
  an empty list with ``total=0``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from app.db import get_conn

router = APIRouter(prefix="/rankings", tags=["rankings"])

MAX_PAGE_LIMIT = 200

Stance = Literal["buy", "hold", "watch", "avoid"]

_DEFAULT_MODEL_VERSION = "v1.1-balanced"

# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class RankingItem(BaseModel):
    """Single instrument's score from the latest scoring run."""

    instrument_id: int
    symbol: str
    company_name: str
    sector: str | None
    coverage_tier: int | None
    rank: int | None
    rank_delta: int | None
    total_score: float | None
    raw_total: float | None
    quality_score: float | None
    value_score: float | None
    turnaround_score: float | None
    momentum_score: float | None
    sentiment_score: float | None
    confidence_score: float | None
    penalties_json: list[dict[str, object]] | None
    explanation: str | None
    model_version: str
    scored_at: datetime


class RankingsListResponse(BaseModel):
    items: list[RankingItem]
    total: int
    offset: int
    limit: int
    model_version: str
    scored_at: datetime | None


class ScoreHistoryItem(BaseModel):
    """One point in an instrument's score history."""

    scored_at: datetime
    total_score: float | None
    raw_total: float | None
    quality_score: float | None
    value_score: float | None
    turnaround_score: float | None
    momentum_score: float | None
    sentiment_score: float | None
    confidence_score: float | None
    penalties_json: list[dict[str, object]] | None
    explanation: str | None
    rank: int | None
    rank_delta: int | None
    model_version: str


class ScoreHistoryResponse(BaseModel):
    instrument_id: int
    items: list[ScoreHistoryItem]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_optional_float(row: dict[str, object], key: str) -> float | None:
    """Safely cast a nullable numeric DB column to float."""
    val = row.get(key)
    if val is None:
        return None
    return float(val)  # type: ignore[arg-type]


def _parse_optional_int(row: dict[str, object], key: str) -> int | None:
    """Safely cast a nullable integer DB column to int."""
    val = row.get(key)
    if val is None:
        return None
    return int(val)  # type: ignore[arg-type]


def _parse_ranking_item(row: dict[str, object]) -> RankingItem:
    return RankingItem(
        instrument_id=row["instrument_id"],  # type: ignore[arg-type]
        symbol=row["symbol"],  # type: ignore[arg-type]
        company_name=row["company_name"],  # type: ignore[arg-type]
        sector=row["sector"],  # type: ignore[arg-type]
        coverage_tier=_parse_optional_int(row, "coverage_tier"),
        rank=_parse_optional_int(row, "rank"),
        rank_delta=_parse_optional_int(row, "rank_delta"),
        total_score=_parse_optional_float(row, "total_score"),
        raw_total=_parse_optional_float(row, "raw_total"),
        quality_score=_parse_optional_float(row, "quality_score"),
        value_score=_parse_optional_float(row, "value_score"),
        turnaround_score=_parse_optional_float(row, "turnaround_score"),
        momentum_score=_parse_optional_float(row, "momentum_score"),
        sentiment_score=_parse_optional_float(row, "sentiment_score"),
        confidence_score=_parse_optional_float(row, "confidence_score"),
        penalties_json=row["penalties_json"],  # type: ignore[arg-type]
        explanation=row["explanation"],  # type: ignore[arg-type]
        model_version=row["model_version"],  # type: ignore[arg-type]
        scored_at=row["scored_at"],  # type: ignore[arg-type]
    )


def _parse_history_item(row: dict[str, object]) -> ScoreHistoryItem:
    return ScoreHistoryItem(
        scored_at=row["scored_at"],  # type: ignore[arg-type]
        total_score=_parse_optional_float(row, "total_score"),
        raw_total=_parse_optional_float(row, "raw_total"),
        quality_score=_parse_optional_float(row, "quality_score"),
        value_score=_parse_optional_float(row, "value_score"),
        turnaround_score=_parse_optional_float(row, "turnaround_score"),
        momentum_score=_parse_optional_float(row, "momentum_score"),
        sentiment_score=_parse_optional_float(row, "sentiment_score"),
        confidence_score=_parse_optional_float(row, "confidence_score"),
        penalties_json=row["penalties_json"],  # type: ignore[arg-type]
        explanation=row["explanation"],  # type: ignore[arg-type]
        rank=_parse_optional_int(row, "rank"),
        rank_delta=_parse_optional_int(row, "rank_delta"),
        model_version=row["model_version"],  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("", response_model=RankingsListResponse)
def list_rankings(
    conn: psycopg.Connection[object] = Depends(get_conn),
    model_version: str = Query(default=_DEFAULT_MODEL_VERSION),
    coverage_tier: int | None = Query(default=None, ge=1, le=3),
    sector: str | None = Query(default=None),
    stance: Stance | None = Query(default=None),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=MAX_PAGE_LIMIT),
) -> RankingsListResponse:
    """Latest scored and ranked instruments for a given model version.

    Identifies the most recent scoring run by ``MAX(scored_at)`` for the
    requested ``model_version``.  All rows in a single run share the same
    ``scored_at`` timestamp (written atomically by ``compute_rankings``).

    If no scoring run exists, returns an empty list with ``total=0``.

    Filters:
      - coverage_tier: exact match (1/2/3); untiered instruments excluded
      - sector: exact match on instruments.sector
      - stance: latest thesis stance for each instrument (adds LATERAL join only when used)
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # Step 1: find the latest run timestamp for this model_version.
        cur.execute(
            "SELECT MAX(scored_at) AS latest FROM scores WHERE model_version = %(mv)s",
            {"mv": model_version},
        )
        # MAX() always returns exactly one row; the value is None when no rows match.
        ts_row = cur.fetchone()
        latest_scored_at = ts_row["latest"]  # type: ignore[index]

        if latest_scored_at is None:
            return RankingsListResponse(
                items=[],
                total=0,
                offset=offset,
                limit=limit,
                model_version=model_version,
                scored_at=None,
            )

        # Step 2: build dynamic WHERE / JOIN fragments.
        where_clauses: list[str] = [
            "s.model_version = %(mv)s",
            "s.scored_at = %(scored_at)s",
        ]
        filter_params: dict[str, object] = {
            "mv": model_version,
            "scored_at": latest_scored_at,
        }

        extra_joins: list[str] = []

        if coverage_tier is not None:
            where_clauses.append("c.coverage_tier = %(coverage_tier)s")
            filter_params["coverage_tier"] = coverage_tier

        if sector is not None:
            where_clauses.append("i.sector = %(sector)s")
            filter_params["sector"] = sector

        # Stance filter: LATERAL join to latest thesis only when needed.
        if stance is not None:
            extra_joins.append(
                """LEFT JOIN LATERAL (
                    SELECT th.stance
                    FROM theses th
                    WHERE th.instrument_id = s.instrument_id
                    ORDER BY th.created_at DESC
                    LIMIT 1
                ) lt ON TRUE"""
            )
            where_clauses.append("lt.stance = %(stance)s")
            filter_params["stance"] = stance

        where_sql = " WHERE " + " AND ".join(where_clauses)
        joins_sql = "\n".join(extra_joins)

        # Step 3: COUNT query — separate params dict (no limit/offset keys).
        count_sql = f"""SELECT COUNT(*) AS cnt
            FROM scores s
            JOIN instruments i USING (instrument_id)
            LEFT JOIN coverage c USING (instrument_id)
            {joins_sql}
            {where_sql}"""  # noqa: S608  — hardcoded fragments only

        cur.execute(count_sql, filter_params)  # type: ignore[arg-type]
        count_row = cur.fetchone()
        total: int = count_row["cnt"] if count_row else 0  # type: ignore[index]

        # Step 4: data query — adds limit/offset in a separate params dict.
        items_params: dict[str, object] = {
            **filter_params,
            "limit": limit,
            "offset": offset,
        }
        items_sql = f"""SELECT s.instrument_id, i.symbol, i.company_name, i.sector,
                   c.coverage_tier,
                   s.rank, s.rank_delta,
                   s.total_score, s.raw_total,
                   s.quality_score, s.value_score, s.turnaround_score,
                   s.momentum_score, s.sentiment_score, s.confidence_score,
                   s.penalties_json, s.explanation,
                   s.model_version, s.scored_at
            FROM scores s
            JOIN instruments i USING (instrument_id)
            LEFT JOIN coverage c USING (instrument_id)
            {joins_sql}
            {where_sql}
            ORDER BY s.rank ASC NULLS LAST, s.total_score DESC
            LIMIT %(limit)s OFFSET %(offset)s"""  # noqa: S608  — hardcoded fragments only

        cur.execute(items_sql, items_params)  # type: ignore[arg-type]
        rows = cur.fetchall()

    items = [_parse_ranking_item(r) for r in rows]
    return RankingsListResponse(
        items=items,
        total=total,
        offset=offset,
        limit=limit,
        model_version=model_version,
        scored_at=latest_scored_at,  # type: ignore[arg-type]
    )


@router.get("/history/{instrument_id}", response_model=ScoreHistoryResponse)
def get_score_history(
    instrument_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
    model_version: str = Query(default=_DEFAULT_MODEL_VERSION),
    limit: int = Query(default=30, ge=1, le=MAX_PAGE_LIMIT),
) -> ScoreHistoryResponse:
    """Score trend over time for a single instrument.

    Returns the most recent ``limit`` score rows (newest first) for the
    requested ``model_version``.  If the instrument has never been scored,
    returns an empty list.
    """
    sql = """
        SELECT s.scored_at,
               s.total_score, s.raw_total,
               s.quality_score, s.value_score, s.turnaround_score,
               s.momentum_score, s.sentiment_score, s.confidence_score,
               s.penalties_json, s.explanation,
               s.rank, s.rank_delta,
               s.model_version
        FROM scores s
        WHERE s.instrument_id = %(instrument_id)s
          AND s.model_version = %(mv)s
        ORDER BY s.scored_at DESC
        LIMIT %(limit)s
    """
    params = {"instrument_id": instrument_id, "mv": model_version, "limit": limit}

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    items = [_parse_history_item(r) for r in rows]
    return ScoreHistoryResponse(instrument_id=instrument_id, items=items)
