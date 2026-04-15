"""Reports API — periodic performance report snapshots."""

from __future__ import annotations

from typing import Any

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, Query

from app.api.auth import require_session_or_service_token
from app.db import get_conn

router = APIRouter(
    prefix="/api/reports",
    tags=["reports"],
    dependencies=[Depends(require_session_or_service_token)],
)


@router.get("/weekly")
def list_weekly_reports(
    conn: psycopg.Connection[object] = Depends(get_conn),
    limit: int = Query(default=10, ge=1, le=100),
) -> list[dict[str, Any]]:
    """Return the most recent weekly report snapshots."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT snapshot_id, report_type, period_start, period_end,
                   snapshot_json, computed_at
            FROM report_snapshots
            WHERE report_type = 'weekly'
            ORDER BY period_start DESC
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )
        return cur.fetchall()


@router.get("/monthly")
def list_monthly_reports(
    conn: psycopg.Connection[object] = Depends(get_conn),
    limit: int = Query(default=10, ge=1, le=100),
) -> list[dict[str, Any]]:
    """Return the most recent monthly report snapshots."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT snapshot_id, report_type, period_start, period_end,
                   snapshot_json, computed_at
            FROM report_snapshots
            WHERE report_type = 'monthly'
            ORDER BY period_start DESC
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )
        return cur.fetchall()


@router.get("/latest")
def get_latest_report(
    conn: psycopg.Connection[object] = Depends(get_conn),
    report_type: str = Query(pattern="^(weekly|monthly)$"),
) -> dict[str, Any] | None:
    """Return the single most recent report of the given type."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT snapshot_id, report_type, period_start, period_end,
                   snapshot_json, computed_at
            FROM report_snapshots
            WHERE report_type = %(report_type)s
            ORDER BY period_start DESC
            LIMIT 1
            """,
            {"report_type": report_type},
        )
        return cur.fetchone()
