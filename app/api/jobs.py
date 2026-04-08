"""Manual job trigger endpoint (issue #13, PR A).

The minimum surface needed to prove the runtime works:

* ``POST /jobs/{job_name}/run`` -- queue a manual run of *job_name*
  on the in-process JobRuntime.

PR A intentionally ships *only* this one endpoint. The listing
endpoint (``GET /jobs``), per-run lookup, and admin UI are PR B.
The pipeline trigger and catch-up status are PR C.

Auth: any signed-in operator (or service-token holder for scripts /
tests). Per the design notes in #13 we are not adding a separate
"may run jobs" role in v1 -- the operator surface is single-user.

Response shape:

* ``202 Accepted`` -- the job was accepted into the executor. The
  body is intentionally empty (no run_id). The operator polls
  ``/system/status`` for outcome; PR B's listing endpoint will
  return a richer shape.
* ``404 Not Found`` -- ``job_name`` is not in the invoker registry.
  This is the correct status: a job that is *declared* in
  ``SCHEDULED_JOBS`` but not yet *wired* in this PR is just as
  unknown as a typo.
* ``409 Conflict`` -- another instance of this job is already
  running (the per-job advisory lock is held).
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from app.api.auth import require_session_or_service_token
from app.jobs.locks import JobAlreadyRunning
from app.jobs.runtime import JobRuntime, UnknownJob

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs", tags=["jobs"])


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
