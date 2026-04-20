"""Thesis API endpoints.

Reads from:
  - theses       (append-only versioned thesis rows per instrument)
  - instruments   (existence check for 404 on history endpoint)

Writes from POST /instruments/{symbol}/thesis (Phase 2.4) via the
existing ``generate_thesis`` service — 24h-cached per-ticker.

Note: the issue (#52) mentions ``conviction_score`` but the theses table
has ``confidence_score``.  This module uses the actual schema column name.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta

import anthropic
import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.db import get_conn
from app.services.thesis import generate_thesis

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/theses", tags=["theses"])


# Separate router for the symbol-based POST; kept under /instruments so
# the research page can POST to a single resource prefix.
instrument_thesis_router = APIRouter(prefix="/instruments", tags=["instruments"])


def get_anthropic_client() -> anthropic.Anthropic:
    """FastAPI dependency: constructs an Anthropic client per request.

    Raises ``HTTPException(503)`` when ANTHROPIC_API_KEY is unset — the
    thesis endpoint is the only caller that needs it, so failing here
    keeps the rest of the API unaffected by missing credentials.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(
            status_code=503,
            detail="ANTHROPIC_API_KEY not configured — thesis generation unavailable",
        )
    return anthropic.Anthropic(api_key=api_key)


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


# ---------------------------------------------------------------------------
# Symbol-keyed thesis endpoint (Phase 2.4)
# ---------------------------------------------------------------------------


THESIS_CACHE_WINDOW = timedelta(hours=24)


class GenerateThesisResponse(BaseModel):
    """Result of POST /instruments/{symbol}/thesis.

    ``cached`` reports whether the returned thesis came from the 24h
    cache (no Anthropic spend for this request) or was freshly
    generated this call.
    """

    cached: bool
    thesis: ThesisDetail


@instrument_thesis_router.post("/{symbol}/thesis", response_model=GenerateThesisResponse)
def generate_instrument_thesis(
    symbol: str,
    conn: psycopg.Connection[object] = Depends(get_conn),
    client: anthropic.Anthropic = Depends(get_anthropic_client),
) -> GenerateThesisResponse:
    """Generate or return the cached thesis for a ticker.

    Phase 2.4 of the 2026-04-19 research-tool refocus. On-demand only —
    no scheduled thesis refresh. Cache window is 24h per ticker: a POST
    within 24h of the last thesis returns the cached row without
    calling Anthropic; after 24h the endpoint regenerates.

    Returns:
      - 404 if the symbol is not in the local instruments table
      - 503 if ANTHROPIC_API_KEY is not configured
      - 200 with the thesis (cached or fresh)
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT instrument_id FROM instruments WHERE UPPER(symbol) = %(s)s LIMIT 1",
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")
    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]

    # Cache check: latest thesis for this instrument within 24h.
    latest_sql = f"""
        SELECT {_THESIS_COLUMNS}
        FROM theses t
        WHERE t.instrument_id = %(iid)s
          AND t.created_at >= %(since)s
        ORDER BY t.created_at DESC, t.thesis_version DESC
        LIMIT 1
    """  # noqa: S608 — _THESIS_COLUMNS is a module-level constant
    now = datetime.now(UTC)
    cache_cutoff = now - THESIS_CACHE_WINDOW
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(latest_sql, {"iid": instrument_id, "since": cache_cutoff})
        cached_row = cur.fetchone()

    if cached_row is not None:
        logger.info(
            "POST /instruments/%s/thesis: cache hit (created_at=%s)",
            symbol_clean,
            cached_row["created_at"],  # type: ignore[index]
        )
        return GenerateThesisResponse(cached=True, thesis=_parse_thesis(cached_row))

    # Cache miss — call the existing generate_thesis service. It handles
    # its own DB transaction + Anthropic calls. We must NOT wrap this in
    # our own transaction (see generate_thesis caller contract).
    logger.info("POST /instruments/%s/thesis: cache miss, generating", symbol_clean)
    try:
        generate_thesis(instrument_id, conn, client)
    except Exception as exc:
        logger.exception("POST /instruments/%s/thesis: generation failed", symbol_clean)
        raise HTTPException(
            status_code=502,
            detail=f"thesis generation failed: {type(exc).__name__}",
        ) from exc

    # Re-read the just-inserted thesis via the same columns shape so the
    # response format is stable.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT {_THESIS_COLUMNS}
            FROM theses t
            WHERE t.instrument_id = %(iid)s
            ORDER BY t.created_at DESC, t.thesis_version DESC
            LIMIT 1
            """,  # noqa: S608
            {"iid": instrument_id},
        )
        fresh_row = cur.fetchone()

    if fresh_row is None:
        # Shouldn't happen — generate_thesis just inserted. Defensive.
        raise HTTPException(status_code=500, detail="thesis row missing after generation")

    return GenerateThesisResponse(cached=False, thesis=_parse_thesis(fresh_row))
