"""Operator-facing ingest health endpoints (#793, Batch 4 of #788).

Three GETs + one POST surface the data feeding the
``/admin/ingest-health`` page:

  * ``GET /api/operator/ingest-status`` — grouped-provider rollup
    with per-group state + per-source last-run summary + queue
    backlog counts. Drives the operator card grid.
  * ``GET /api/operator/ingest-failures`` — last-7-days failed /
    partial runs for the "needs attention" list. Bounded by
    ``limit`` so a flap-storm can't swamp the UI.
  * ``GET /api/operator/ingest-backfill-queue`` — queue rows for
    the per-pipeline drilldown view (current status, attempts,
    last_error). Bounded by ``limit``.
  * ``POST /api/operator/ingest-backfill`` — enqueue a backfill
    request from an operator click. Idempotent.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.db.snapshot import snapshot_read
from app.services import ingest_status

router = APIRouter(
    prefix="/operator",
    tags=["operator"],
    # Operator-only: ingest run history + failure text + a backfill
    # enqueue POST. Public exposure leaks internal error messages and
    # lets anyone trigger ingest work. Codex pre-push review (Batch 4
    # of #788) flagged the prior bare router.
    dependencies=[Depends(require_session_or_service_token)],
)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class _SourceSummaryModel(BaseModel):
    source: str
    last_success_at: datetime | None
    last_attempt_at: datetime | None
    last_attempt_status: str | None
    failures_24h: int
    rows_upserted_total: int


class _GroupModel(BaseModel):
    key: Literal[
        "sec_fundamentals",
        "sec_ownership",
        "etoro",
        "fundamentals_other",
        "other",
    ]
    label: str
    description: str
    state: Literal["never_run", "green", "amber", "red"]
    sources: list[_SourceSummaryModel]
    backlog_pending: int
    backlog_running: int
    backlog_failed: int


class IngestStatusResponse(BaseModel):
    groups: list[_GroupModel]
    queue_total: int
    queue_running: int
    queue_failed: int
    computed_at: datetime


class _FailureModel(BaseModel):
    source: str
    started_at: datetime
    finished_at: datetime | None
    error: str | None
    rows_upserted: int


class IngestFailuresResponse(BaseModel):
    failures: list[_FailureModel]


class _QueueRowModel(BaseModel):
    instrument_id: int
    symbol: str | None
    pipeline_name: str
    priority: int
    status: Literal["pending", "running", "complete", "failed"]
    queued_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    attempts: int
    last_error: str | None
    triggered_by: Literal["system", "operator", "migration", "consumer"]


class BackfillQueueResponse(BaseModel):
    rows: list[_QueueRowModel]


class EnqueueBackfillRequest(BaseModel):
    instrument_id: int
    pipeline_name: str = Field(min_length=1, max_length=128)
    priority: int = Field(default=100, ge=1, le=1000)
    triggered_by: Literal["system", "operator", "migration", "consumer"] = "operator"


class EnqueueBackfillResponse(BaseModel):
    instrument_id: int
    pipeline_name: str
    status: Literal["queued"]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/ingest-status", response_model=IngestStatusResponse)
def get_ingest_status_endpoint(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> IngestStatusResponse:
    """Grouped-provider ingest rollup. Drives the operator
    ``/admin/ingest-health`` card grid.

    Reads run inside ``snapshot_read`` so the per-group state, queue
    backlog, and per-source summaries reconcile against one
    REPEATABLE READ snapshot (otherwise a concurrent run that flips
    a source from ``running`` to ``success`` mid-rollup could leave
    the queue counts and the per-source summaries inconsistent on
    the rendered page)."""
    with snapshot_read(conn):
        report = ingest_status.get_ingest_status(conn)
    return IngestStatusResponse(
        groups=[
            _GroupModel(
                key=g.key,
                label=g.label,
                description=g.description,
                state=g.state,
                sources=[
                    _SourceSummaryModel(
                        source=s.source,
                        last_success_at=s.last_success_at,
                        last_attempt_at=s.last_attempt_at,
                        last_attempt_status=s.last_attempt_status,
                        failures_24h=s.failures_24h,
                        rows_upserted_total=s.rows_upserted_total,
                    )
                    for s in g.sources
                ],
                backlog_pending=g.backlog_pending,
                backlog_running=g.backlog_running,
                backlog_failed=g.backlog_failed,
            )
            for g in report.groups
        ],
        queue_total=report.queue_total,
        queue_running=report.queue_running,
        queue_failed=report.queue_failed,
        computed_at=report.computed_at,
    )


@router.get("/ingest-failures", response_model=IngestFailuresResponse)
def get_ingest_failures_endpoint(
    limit: int = Query(default=50, ge=1, le=500),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> IngestFailuresResponse:
    """Recent failures (last 7 days). Bounded by ``limit`` so a
    flap-storm can't swamp the UI; default 50, max 500."""
    with snapshot_read(conn):
        failures = ingest_status.get_recent_failures(conn, limit=limit)
    return IngestFailuresResponse(
        failures=[
            _FailureModel(
                source=f.source,
                started_at=f.started_at,
                finished_at=f.finished_at,
                error=f.error,
                rows_upserted=f.rows_upserted,
            )
            for f in failures
        ]
    )


@router.get("/ingest-backfill-queue", response_model=BackfillQueueResponse)
def get_backfill_queue_endpoint(
    limit: int = Query(default=200, ge=1, le=1000),
    status_filter: Literal["all", "pending", "running", "complete", "failed"] = Query(default="all", alias="status"),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> BackfillQueueResponse:
    """Backfill queue rows for the per-pipeline drilldown.

    ``status_filter`` defaults to ``all`` so the operator sees the
    full picture; the page filters client-side. Bounded by ``limit``
    to keep the payload small even when the queue grows to thousands
    of rows over time."""
    where_clauses = []
    params: list[object] = []
    if status_filter != "all":
        where_clauses.append("q.status = %s")
        params.append(status_filter)
    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    params.append(limit)
    with snapshot_read(conn):
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                f"""
                SELECT q.instrument_id, i.symbol, q.pipeline_name,
                       q.priority, q.status, q.queued_at, q.started_at,
                       q.completed_at, q.attempts, q.last_error,
                       q.triggered_by
                FROM ingest_backfill_queue q
                LEFT JOIN instruments i USING (instrument_id)
                {where_sql}
                ORDER BY
                    CASE q.status
                        WHEN 'running' THEN 0
                        WHEN 'failed' THEN 1
                        WHEN 'pending' THEN 2
                        ELSE 3
                    END,
                    q.priority,
                    q.queued_at DESC
                LIMIT %s
                """,  # noqa: S608 — where_sql / status_filter is a closed enum
                params,
            )
            rows = cur.fetchall()
    return BackfillQueueResponse(
        rows=[
            _QueueRowModel(
                instrument_id=int(row["instrument_id"]),  # type: ignore[arg-type]
                symbol=(str(row["symbol"]) if row.get("symbol") else None),
                pipeline_name=str(row["pipeline_name"]),  # type: ignore[arg-type]
                priority=int(row["priority"]),  # type: ignore[arg-type]
                status=row["status"],  # type: ignore[arg-type]
                queued_at=row["queued_at"],  # type: ignore[arg-type]
                started_at=row.get("started_at"),  # type: ignore[arg-type]
                completed_at=row.get("completed_at"),  # type: ignore[arg-type]
                attempts=int(row["attempts"]),  # type: ignore[arg-type]
                last_error=(str(row["last_error"]) if row.get("last_error") is not None else None),
                triggered_by=row["triggered_by"],  # type: ignore[arg-type]
            )
            for row in rows
        ]
    )


@router.post("/ingest-backfill", response_model=EnqueueBackfillResponse)
def enqueue_backfill_endpoint(
    request: EnqueueBackfillRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> EnqueueBackfillResponse:
    """Enqueue an operator-triggered backfill. Idempotent — re-clicking
    the button on the same (instrument, pipeline) refreshes the row
    instead of inserting a duplicate."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM instruments WHERE instrument_id = %s",
            (request.instrument_id,),
        )
        if cur.fetchone() is None:
            raise HTTPException(
                status_code=404,
                detail=f"Instrument {request.instrument_id} not found",
            )
    ingest_status.enqueue_backfill(
        conn,
        instrument_id=request.instrument_id,
        pipeline_name=request.pipeline_name,
        priority=request.priority,
        triggered_by=request.triggered_by,
    )
    conn.commit()
    return EnqueueBackfillResponse(
        instrument_id=request.instrument_id,
        pipeline_name=request.pipeline_name,
        status="queued",
    )
