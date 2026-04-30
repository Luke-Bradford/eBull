"""Job trigger and recent-runs endpoints (#13, durable queue rewrite #719).

Surface:

* ``POST /jobs/{job_name}/run`` — publish a manual run of *job_name* to
  the durable queue (`pending_job_requests` + `pg_notify`). Returns
  202 with the new ``request_id`` so the operator can correlate via
  ``GET /jobs/requests?request_id=N``. The jobs process listener
  picks up the NOTIFY (or its 5s poll fallback) and dispatches.
* ``GET  /jobs/runs`` — most recent ``job_runs`` rows, optionally
  filtered by job_name.
* ``GET  /jobs/requests`` — most recent ``pending_job_requests`` rows
  with status / job_name / request_kind / request_id filters. Added
  in #719 so a request rejected before any ``job_runs`` row exists
  is still visible to the operator.

Auth: any signed-in operator (or service-token holder for scripts /
tests).

Trigger response shape:

* ``202 Accepted`` — the request was written to the durable queue.
  Body: ``{"request_id": N}`` so the operator polls
  ``/jobs/requests?request_id=N`` for status without waiting on a
  ``job_runs`` row that may never appear (e.g. unknown job name
  rejected by the listener).
* ``404 Not Found`` — ``job_name`` is not in the invoker registry.
  Validated server-side against the imported registry before the
  INSERT, so an unknown name never lands in the queue.

The 409 path the in-process design returned is gone in #719. The
existing 202-and-trust-the-worker semantics already covered the
"another scheduled fire holds the advisory lock" race; cross-process
makes that the canonical path.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Literal

import psycopg
import psycopg.rows
import psycopg.sql
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.jobs.runtime import VALID_JOB_NAMES
from app.services.sync_orchestrator.dispatcher import publish_manual_job_request

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs", tags=["jobs"])


# ---------------------------------------------------------------------------
# Recent runs response model
# ---------------------------------------------------------------------------


class JobRunResponse(BaseModel):
    """One row from ``job_runs``.

    Mirrors the table directly so the frontend can render started/finished/
    duration without joining anything else. ``run_id`` is exposed so the
    frontend can use it as a stable React key. ``linked_request_id``
    (#719) lets the operator pivot from a run to its triggering queue
    row when the run was operator-initiated.
    """

    run_id: int
    job_name: str
    started_at: datetime
    finished_at: datetime | None
    status: Literal["running", "success", "failure", "skipped"]
    row_count: int | None
    error_msg: str | None
    linked_request_id: int | None = None


class JobRunsListResponse(BaseModel):
    items: list[JobRunResponse]
    # ``count`` is the number of rows in *this response*, not a total
    # of all matching rows in the table. Named ``count`` rather than
    # ``total`` to avoid confusion with the paginated ``total`` field
    # used by other list endpoints.
    count: int
    limit: int
    job_name: str | None  # echo of the filter, for client display


# ---------------------------------------------------------------------------
# Queue-request response model (#719)
# ---------------------------------------------------------------------------


class JobRequestResponse(BaseModel):
    """One row from ``pending_job_requests`` (#719)."""

    request_id: int
    request_kind: Literal["manual_job", "sync"]
    job_name: str | None
    payload: dict[str, Any] | None
    requested_at: datetime
    requested_by: str | None
    status: Literal["pending", "claimed", "dispatched", "completed", "rejected"]
    claimed_at: datetime | None
    error_msg: str | None


class JobRequestsListResponse(BaseModel):
    items: list[JobRequestResponse]
    count: int
    limit: int


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------


class JobRunQueuedResponse(BaseModel):
    """202 body returned by ``POST /jobs/{name}/run``."""

    request_id: int


@router.post(
    "/{job_name}/run",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobRunQueuedResponse,
    dependencies=[Depends(require_session_or_service_token)],
)
def run_job(job_name: str, request: Request) -> Response:
    """Publish a manual run of *job_name* to the durable queue.

    The jobs process picks the request up via NOTIFY (or its 5s poll
    fallback) and dispatches on its own thread. Returns 202 with the
    queue ``request_id`` so the operator can poll
    ``/jobs/requests?request_id=N`` for outcome.

    Validates ``job_name`` against the imported invoker registry
    BEFORE the INSERT — unknown names return 404 and never write a
    queue row.
    """
    if job_name not in VALID_JOB_NAMES:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown job: {job_name}",
        )

    requested_by = _identify_requestor(request)
    request_id = publish_manual_job_request(job_name, requested_by=requested_by)
    return Response(
        content=JobRunQueuedResponse(request_id=request_id).model_dump_json(),
        media_type="application/json",
        status_code=status.HTTP_202_ACCEPTED,
    )


def _identify_requestor(request: Request) -> str:
    """Best-effort caller-identity tag for the queue row.

    Used only for operator visibility on ``/jobs/requests`` — never as
    an authentication signal. Falls back to ``"unknown"`` when neither
    operator id nor service-token marker is on the request state.
    """
    operator_id = getattr(request.state, "operator_id", None)
    if operator_id is not None:
        return f"operator:{operator_id}"
    if getattr(request.state, "service_token", False):
        return "service-token"
    return "unknown"


# ---------------------------------------------------------------------------
# Recent runs
# ---------------------------------------------------------------------------


@router.get(
    "/runs",
    response_model=JobRunsListResponse,
    dependencies=[Depends(require_session_or_service_token)],
)
def list_job_runs(
    job_name: str | None = Query(
        default=None,
        description="Optional job name filter; case-sensitive exact match against job_runs.job_name.",
    ),
    limit: int = Query(default=50, ge=1, le=200),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> JobRunsListResponse:
    """Return the most recent ``job_runs`` rows, newest-first."""
    try:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            if job_name is None:
                cur.execute(
                    """
                    SELECT run_id, job_name, started_at, finished_at,
                           status, row_count, error_msg, linked_request_id
                    FROM job_runs
                    ORDER BY started_at DESC
                    LIMIT %(limit)s
                    """,
                    {"limit": limit},
                )
            else:
                cur.execute(
                    """
                    SELECT run_id, job_name, started_at, finished_at,
                           status, row_count, error_msg, linked_request_id
                    FROM job_runs
                    WHERE job_name = %(job_name)s
                    ORDER BY started_at DESC
                    LIMIT %(limit)s
                    """,
                    {"job_name": job_name, "limit": limit},
                )
            rows = cur.fetchall()
    except psycopg.Error as exc:
        logger.exception("list_job_runs: query failed")
        raise HTTPException(status_code=503, detail="job run history unavailable") from exc

    items = [
        JobRunResponse(
            run_id=row["run_id"],
            job_name=row["job_name"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            status=row["status"],
            row_count=row["row_count"],
            error_msg=row["error_msg"],
            linked_request_id=row.get("linked_request_id"),
        )
        for row in rows
    ]
    return JobRunsListResponse(items=items, count=len(items), limit=limit, job_name=job_name)


# ---------------------------------------------------------------------------
# Queue requests (#719)
# ---------------------------------------------------------------------------


@router.get(
    "/requests",
    response_model=JobRequestsListResponse,
    dependencies=[Depends(require_session_or_service_token)],
)
def list_job_requests(
    request_id: int | None = Query(
        default=None,
        description="Exact request_id filter (returns 0 or 1 row).",
    ),
    status_filter: Literal["pending", "claimed", "dispatched", "completed", "rejected"] | None = Query(
        default=None, alias="status", description="Filter by queue status."
    ),
    job_name: str | None = Query(default=None, description="Filter by manual_job request's job_name."),
    request_kind: Literal["manual_job", "sync"] | None = Query(default=None, description="Filter by request_kind."),
    limit: int = Query(default=50, ge=1, le=200),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> JobRequestsListResponse:
    """Return rows from ``pending_job_requests`` newest-first.

    Operator visibility for the durable trigger queue (#719). A request
    that the listener rejects (unknown job name, malformed payload,
    executor refused) lives ONLY in this table — there is no
    ``job_runs`` / ``sync_runs`` row to reflect it. Without this view
    the operator's experience of "I clicked Run and nothing happened"
    has no diagnostic surface.
    """
    # Build the WHERE clause from a fixed set of literal predicate
    # snippets (no caller-controlled identifiers) so the composed
    # query satisfies pyright's LiteralString contract.
    where_parts: list[psycopg.sql.SQL] = []
    params: dict[str, Any] = {"limit": limit}
    if request_id is not None:
        where_parts.append(psycopg.sql.SQL("request_id = %(request_id)s"))
        params["request_id"] = request_id
    if status_filter is not None:
        where_parts.append(psycopg.sql.SQL("status = %(status_filter)s"))
        params["status_filter"] = status_filter
    if job_name is not None:
        where_parts.append(psycopg.sql.SQL("job_name = %(job_name)s"))
        params["job_name"] = job_name
    if request_kind is not None:
        where_parts.append(psycopg.sql.SQL("request_kind = %(request_kind)s"))
        params["request_kind"] = request_kind

    where_clause: psycopg.sql.Composable
    if where_parts:
        where_clause = psycopg.sql.SQL("WHERE ") + psycopg.sql.SQL(" AND ").join(where_parts)
    else:
        where_clause = psycopg.sql.SQL("")

    query = psycopg.sql.SQL("""
        SELECT request_id, request_kind, job_name, payload,
               requested_at, requested_by, status, claimed_at, error_msg
        FROM pending_job_requests
        {where_clause}
        ORDER BY requested_at DESC
        LIMIT %(limit)s
    """).format(where_clause=where_clause)

    try:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    except psycopg.Error as exc:
        logger.exception("list_job_requests: query failed")
        raise HTTPException(status_code=503, detail="job request history unavailable") from exc

    items = [
        JobRequestResponse(
            request_id=row["request_id"],
            request_kind=row["request_kind"],
            job_name=row["job_name"],
            payload=row["payload"],
            requested_at=row["requested_at"],
            requested_by=row["requested_by"],
            status=row["status"],
            claimed_at=row["claimed_at"],
            error_msg=row["error_msg"],
        )
        for row in rows
    ]
    return JobRequestsListResponse(items=items, count=len(items), limit=limit)
