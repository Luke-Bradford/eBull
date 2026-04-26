"""Business-summary failure dashboard endpoints (#533).

Read-only operator surface for the per-instrument 10-K Item 1
parse failure tracking. Two endpoints:

- ``GET /admin/business-summary-failures`` — reason histogram +
  per-instrument detail (paginated). Lets operators see at a glance
  whether the queue is dominated by ``no_item_1_marker`` (10-K/A
  Part-III amendments — fixable via #534), ``fetch_http_5xx``
  (transient SEC outage), or genuine parser bugs.

- ``POST /admin/business-summary-failures/{instrument_id}/reset`` —
  zeroes ``attempt_count`` and clears ``next_retry_at`` so the
  ingester re-attempts on its next run. Used after a parser fix
  lands or when the operator manually verifies a filing should
  re-parse.

Auth: both routes require operator auth via
``require_session_or_service_token``. Failure data reveals
pipeline-internal state, must not be public.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

import psycopg
import psycopg.rows
import psycopg.sql
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.db import get_conn

router = APIRouter(
    prefix="/admin/business-summary-failures",
    tags=["admin", "business-summary"],
    dependencies=[Depends(require_session_or_service_token)],
)


class FailureReasonCount(BaseModel):
    """One row in the reason histogram."""

    reason: str
    count: int
    quarantined_count: int


class FailureRow(BaseModel):
    """One failing instrument's detail."""

    instrument_id: int
    symbol: str
    company_name: str | None
    source_accession: str
    attempt_count: int
    last_failure_reason: str | None
    last_parsed_at: datetime
    next_retry_at: datetime | None
    is_quarantined: bool


class FailureListResponse(BaseModel):
    """Reason histogram + paginated detail."""

    checked_at: datetime
    histogram: list[FailureReasonCount]
    total_failing: int
    rows: list[FailureRow]
    limit: int
    offset: int


class ResetResponse(BaseModel):
    """Result of a manual reset action."""

    instrument_id: int
    cleared: bool


_QUARANTINE_REASON_FILTER = Literal[
    "all",
    "quarantined",
    "active",
]


@router.get("", response_model=FailureListResponse)
def list_failures(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    reason: str | None = Query(default=None, description="Filter to one failure reason (e.g. no_item_1_marker)."),
    state: _QUARANTINE_REASON_FILTER = Query(default="all"),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> FailureListResponse:
    """List instruments whose ``instrument_business_summary`` row has
    a non-NULL ``next_retry_at`` (i.e. a failure has been recorded).

    The histogram is computed over the same filter set so the totals
    line up with the visible rows when an operator is filtering by
    reason / state."""
    where_clauses = ["bs.next_retry_at IS NOT NULL"]
    params: dict[str, object] = {"limit": limit, "offset": offset}
    if reason is not None:
        where_clauses.append("bs.last_failure_reason = %(reason)s")
        params["reason"] = reason
    if state == "quarantined":
        where_clauses.append("bs.attempt_count >= 4")
    elif state == "active":
        where_clauses.append("bs.attempt_count < 4")
    where_sql = " AND ".join(where_clauses)

    histogram: list[FailureReasonCount] = []
    rows: list[FailureRow] = []
    total = 0

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            psycopg.sql.SQL(  # pyright: ignore[reportArgumentType]
                f"""
            SELECT bs.last_failure_reason AS reason,
                   COUNT(*) AS count,
                   COUNT(*) FILTER (WHERE bs.attempt_count >= 4) AS quarantined_count
              FROM instrument_business_summary bs
             WHERE {where_sql}
             GROUP BY bs.last_failure_reason
             ORDER BY count DESC
            """
            ),
            params,
        )
        for r in cur.fetchall():
            histogram.append(
                FailureReasonCount(
                    reason=str(r["reason"]) if r["reason"] is not None else "unknown",
                    count=int(r["count"]),  # type: ignore[arg-type]
                    quarantined_count=int(r["quarantined_count"]),  # type: ignore[arg-type]
                )
            )

        cur.execute(
            psycopg.sql.SQL(  # pyright: ignore[reportArgumentType]
                f"""
            SELECT COUNT(*) AS total
              FROM instrument_business_summary bs
             WHERE {where_sql}
            """
            ),
            params,
        )
        total_row = cur.fetchone()
        total = int(total_row["total"]) if total_row else 0  # type: ignore[arg-type]

        cur.execute(
            psycopg.sql.SQL(  # pyright: ignore[reportArgumentType]
                f"""
            SELECT bs.instrument_id,
                   i.symbol,
                   i.company_name,
                   bs.source_accession,
                   bs.attempt_count,
                   bs.last_failure_reason,
                   bs.last_parsed_at,
                   bs.next_retry_at
              FROM instrument_business_summary bs
              JOIN instruments i ON i.instrument_id = bs.instrument_id
             WHERE {where_sql}
             ORDER BY bs.attempt_count DESC, bs.last_parsed_at DESC
             LIMIT %(limit)s OFFSET %(offset)s
            """
            ),
            params,
        )
        for r in cur.fetchall():
            rows.append(
                FailureRow(
                    instrument_id=int(r["instrument_id"]),  # type: ignore[arg-type]
                    symbol=str(r["symbol"]),
                    company_name=str(r["company_name"]) if r["company_name"] is not None else None,
                    source_accession=str(r["source_accession"]),
                    attempt_count=int(r["attempt_count"]),  # type: ignore[arg-type]
                    last_failure_reason=str(r["last_failure_reason"]) if r["last_failure_reason"] is not None else None,
                    last_parsed_at=r["last_parsed_at"],  # type: ignore[arg-type]
                    next_retry_at=r["next_retry_at"],  # type: ignore[arg-type]
                    is_quarantined=int(r["attempt_count"]) >= 4,  # type: ignore[arg-type]
                )
            )

    return FailureListResponse(
        checked_at=datetime.now().astimezone(),
        histogram=histogram,
        total_failing=total,
        rows=rows,
        limit=limit,
        offset=offset,
    )


@router.post("/{instrument_id}/reset", response_model=ResetResponse)
def reset_failure(
    instrument_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> ResetResponse:
    """Clear failure tracking for one instrument and re-queue it.

    Resets ``attempt_count = 0`` and ``last_failure_reason = NULL``,
    and sets ``next_retry_at = NOW()`` so the ingester's candidate
    query picks the row up on the next run via the
    ``next_retry_at <= NOW()`` predicate. Body and source_accession
    are untouched so any prior successful narrative is preserved.

    Returns 404 when the instrument has no failure on file
    (``next_retry_at IS NULL``) — guards against false positives
    where an operator resets a healthy row by mistake.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE instrument_business_summary
               SET attempt_count       = 0,
                   last_failure_reason = NULL,
                   next_retry_at       = NOW()
             WHERE instrument_id  = %s
               AND next_retry_at IS NOT NULL
            """,
            (instrument_id,),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail=f"No failure row for instrument_id={instrument_id}")
        conn.commit()
    return ResetResponse(instrument_id=instrument_id, cleared=True)
