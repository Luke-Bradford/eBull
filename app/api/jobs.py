"""Job trigger and recent-runs endpoints (issue #13, PR A + PR B).

Surface:

* ``POST /jobs/{job_name}/run`` -- queue a manual run of *job_name*
  on the in-process JobRuntime. (PR A)
* ``GET  /jobs/runs`` -- the most recent rows from ``job_runs``,
  optionally filtered by job_name. (PR B)

The declared schedule + computed next-run-time view lives at
``GET /system/jobs`` (issue #57); the admin page polls that
endpoint for the per-job overview and this one for the recent-runs
table. The two are kept separate so a partial failure of the
heavier history query does not blank the lighter overview, and
because ``/system/jobs`` already exists with the right contract.

Pipeline trigger and catch-up status are PR C.

Auth: any signed-in operator (or service-token holder for scripts /
tests). Per the design notes in #13 we are not adding a separate
"may run jobs" role in v1 -- the operator surface is single-user.

Trigger response shape:

* ``202 Accepted`` -- the job was accepted into the executor. The
  body is intentionally empty (no run_id). The operator polls
  ``GET /jobs/runs`` (or the dashboard) for outcome.
* ``404 Not Found`` -- ``job_name`` is not in the invoker registry.
  Now that PR B wires every declared job, this can only fire on a
  typo from the caller, but the status is still correct.
* ``409 Conflict`` -- another instance of this job is already
  running (the per-job advisory lock is held).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Literal

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.jobs.locks import JobAlreadyRunning
from app.jobs.runtime import JobRuntime, UnknownJob

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs", tags=["jobs"])


# ---------------------------------------------------------------------------
# Recent runs response model
# ---------------------------------------------------------------------------


class JobRunResponse(BaseModel):
    """One row from ``job_runs``.

    Mirrors the table directly so the frontend can render started/finished/
    duration without joining anything else. ``run_id`` is exposed so the
    frontend can use it as a stable React key and so a future ``GET
    /jobs/runs/{run_id}`` (PR C) does not need to invent an alternate id.
    """

    run_id: int
    job_name: str
    started_at: datetime
    finished_at: datetime | None
    status: Literal["running", "success", "failure"]
    row_count: int | None
    error_msg: str | None


class JobRunsListResponse(BaseModel):
    items: list[JobRunResponse]
    # ``count`` is the number of rows in *this response*, not a total
    # of all matching rows in the table. Named ``count`` rather than
    # ``total`` to avoid confusion with the paginated ``total`` field
    # used by other list endpoints (instruments, recommendations) --
    # this endpoint deliberately does not paginate; the operator's
    # use case is "show me the latest N runs", not "page through every
    # run since the dawn of time". If pagination becomes a real
    # requirement, add ``offset`` + ``total_matching`` then; do not
    # repurpose this field.
    count: int
    limit: int
    job_name: str | None  # echo of the filter, for client display


def _get_runtime(request: Request) -> JobRuntime:
    """Read the JobRuntime off ``app.state``.

    Raises 503 if the runtime is missing -- which only happens if
    lifespan startup never completed (e.g. the test harness drove
    the router without going through TestClient's context manager).
    Surfaces as a clear configuration fault rather than a 500.
    """
    runtime: JobRuntime | None = getattr(request.app.state, "job_runtime", None)
    if runtime is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="job runtime not started",
        )
    return runtime


@router.post(
    "/{job_name}/run",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_session_or_service_token)],
)
def run_job(
    job_name: str,
    request: Request,
) -> Response:
    """Queue a manual run of *job_name*.

    See module docstring for status semantics.
    """
    runtime = _get_runtime(request)
    try:
        runtime.trigger(job_name)
    except UnknownJob as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown job: {exc.job_name}",
        ) from exc
    except JobAlreadyRunning as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"job already running: {exc.job_name}",
        ) from exc
    return Response(status_code=status.HTTP_202_ACCEPTED)


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
    """Return the most recent ``job_runs`` rows, newest-first.

    The query orders by ``started_at DESC`` so the result reflects the
    real chronology of when each job kicked off, not when it finished.
    A run that is still in progress (status='running') therefore stays
    at the top of the list until it completes -- which matches the
    operator's mental model of "what's happening right now".

    The ``job_name`` filter is parameterised; never interpolated.

    Errors at the report-build level (e.g. DB unreachable) raise 503;
    we never leak driver text in the detail.
    """
    # Narrow to ``psycopg.Error``: the auth dependency runs *before*
    # this handler body, so an HTTPException raised by auth never
    # passes through this try block. The narrow catch is still cheaper
    # than a broad one and makes the intent obvious -- this try is for
    # DB-layer faults only, not for swallowing arbitrary control flow.
    try:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT run_id, job_name, started_at, finished_at,
                       status, row_count, error_msg
                FROM job_runs
                WHERE %(job_name)s IS NULL OR job_name = %(job_name)s
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
        )
        for row in rows
    ]
    return JobRunsListResponse(items=items, count=len(items), limit=limit, job_name=job_name)
