"""Attribution API — return decomposition data for the dashboard."""

from __future__ import annotations

from typing import Any

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, Query

from app.api.auth import require_session_or_service_token
from app.config import settings

router = APIRouter(
    prefix="/attribution",
    tags=["attribution"],
    dependencies=[Depends(require_session_or_service_token)],
)


@router.get("")
def list_attributions(
    limit: int = Query(default=50, le=1000),
) -> list[dict[str, Any]]:
    """Return the most recent attribution rows."""
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT ra.*, i.symbol, i.sector
                FROM return_attribution ra
                JOIN instruments i USING (instrument_id)
                ORDER BY ra.computed_at DESC
                LIMIT %(limit)s
                """,
                {"limit": limit},
            )
            return cur.fetchall()


@router.get("/summary")
def list_summaries(
    limit: int = Query(default=10, le=1000),
) -> list[dict[str, Any]]:
    """Return the most recent attribution summaries."""
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT *
                FROM return_attribution_summary
                ORDER BY computed_at DESC
                LIMIT %(limit)s
                """,
                {"limit": limit},
            )
            return cur.fetchall()
