"""First-run setup endpoints (issue #106).

Routes:
  GET  /auth/setup-status   -- {"needs_setup": bool}
  POST /auth/setup          -- create the first operator + session

Both endpoints are public and unauthenticated. ``POST /auth/setup`` is
the only mutating endpoint that accepts an unauthenticated request, and
even then only when (a) ``operators`` is empty and (b) the bootstrap
authorization check passes (Mode A loopback / Mode B token).

Generic 404 discipline:
  Every failure mode -- already set up, wrong token, missing token,
  missing username, short password -- returns the same generic 404 with
  detail "Not Found". Callers cannot distinguish failure modes. The
  frontend maps any non-2xx to the fixed string "Setup unavailable or
  invalid token." (per Ticket G).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, Field

from app.api.auth_session import _set_session_cookie  # reuse cookie helper
from app.db import get_conn
from app.services.operator_setup import SetupOutcome, perform_setup

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class SetupStatusResponse(BaseModel):
    needs_setup: bool


class SetupRequest(BaseModel):
    username: str = Field(min_length=1, max_length=255)
    # min_length=1 here is just "the field is present"; the real
    # 12-char floor is enforced inside perform_setup so the rejection
    # path is identical for "missing", "too short", and "wrong token".
    password: str = Field(min_length=1, max_length=1024)
    setup_token: str | None = None


class SetupResponseOperator(BaseModel):
    id: UUID
    username: str


class SetupResponse(BaseModel):
    operator: SetupResponseOperator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _not_found() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Not Found",
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/setup-status", response_model=SetupStatusResponse)
def setup_status(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> SetupStatusResponse:
    """Return whether the application is in first-run setup mode.

    True iff the ``operators`` table currently has zero rows. The
    response shape is identical regardless of bootstrap mode -- it does
    not leak whether a token would be required, only whether a setup is
    needed.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM operators LIMIT 1")
        empty = cur.fetchone() is None
    return SetupStatusResponse(needs_setup=empty)


@router.post("/setup", response_model=SetupResponse)
def setup(
    body: SetupRequest,
    request: Request,
    response: Response,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> SetupResponse:
    """Create the first operator and immediately issue a session cookie.

    Every failure mode returns generic 404. The frontend maps any
    non-2xx to a single fixed string.
    """
    client = request.client
    request_host = client.host if client else None
    request_ip = client.host if client else None
    user_agent = request.headers.get("user-agent")

    outcome, success = perform_setup(
        conn,
        username=body.username,
        password=body.password,
        submitted_token=body.setup_token,
        request_host=request_host,
        user_agent=user_agent,
        request_ip=request_ip,
    )

    if outcome is not SetupOutcome.OK or success is None:
        raise _not_found()

    _set_session_cookie(
        response,
        session_id=success.session_id,
        max_age_seconds=_max_age_seconds(success.expires_at),
    )
    return SetupResponse(
        operator=SetupResponseOperator(
            id=success.operator_id,
            username=success.username,
        ),
    )


def _max_age_seconds(expires_at: datetime) -> int:
    """Cookie max-age derived from the persisted DB expiry.

    Mirrors the pattern in app/api/auth_session.py:login -- the cookie
    lifetime is anchored to the DB row, not recomputed from a parallel
    delta.
    """
    return max(0, int((expires_at - datetime.now(UTC)).total_seconds()))
