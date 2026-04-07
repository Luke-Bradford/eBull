"""Operator management endpoints (issue #106 / ADR 0002).

Routes (all session-only -- never service_token):
  GET    /operators        -- list
  POST   /operators        -- create another operator
  DELETE /operators/{id}   -- delete (self or other; last-operator block)

Service-token auth is intentionally not accepted on this router. The
operator-management surface is operator-only by design: a script with
the service token must not be able to create or delete browser
identities.
"""

from __future__ import annotations

import logging
from datetime import datetime
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, Field

from app.api.auth import require_session
from app.api.auth_session import _clear_session_cookie
from app.db import get_conn
from app.security.sessions import SessionRow
from app.services.operators import (
    CreateOutcome,
    DeleteOutcome,
    create_operator,
    delete_operator,
    list_operators,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/operators", tags=["operators"])


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class OperatorView(BaseModel):
    id: UUID
    username: str
    created_at: datetime
    last_login_at: datetime | None
    is_self: bool


class CreateOperatorRequest(BaseModel):
    username: str = Field(min_length=1, max_length=255)
    password: str = Field(min_length=1, max_length=1024)


class CreateOperatorResponse(BaseModel):
    operator: OperatorView


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("", response_model=list[OperatorView])
def list_(
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> list[OperatorView]:
    """Return every operator with an ``is_self`` marker for the caller."""
    rows = list_operators(conn)
    return [
        OperatorView(
            id=row.operator_id,
            username=row.username,
            created_at=row.created_at,
            last_login_at=row.last_login_at,
            is_self=(row.operator_id == session.operator_id),
        )
        for row in rows
    ]


@router.post("", response_model=CreateOperatorResponse, status_code=status.HTTP_201_CREATED)
def create(
    body: CreateOperatorRequest,
    request: Request,
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> CreateOperatorResponse:
    """Create a new operator. Does NOT log the new operator in.

    The calling operator stays signed in. The new operator must log in
    via /auth/login like any other.
    """
    client = request.client
    user_agent = request.headers.get("user-agent")
    request_ip = client.host if client else None

    outcome, row = create_operator(
        conn,
        actor_operator_id=session.operator_id,
        actor_username=session.username,
        new_username=body.username,
        new_password=body.password,
        request_ip=request_ip,
        user_agent=user_agent,
    )

    if outcome is CreateOutcome.BAD_USERNAME or outcome is CreateOutcome.BAD_PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid username or password",
        )
    if outcome is CreateOutcome.DUPLICATE:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Username already exists",
        )
    if row is None:
        # Defensive: the service contract is (OK -> row, anything else
        # -> None), so this branch is unreachable today. Use an
        # explicit raise rather than ``assert`` so the guard survives
        # ``python -O`` and produces a real 500 instead of an
        # AssertionError if the contract is ever broken.
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal Server Error",
        )
    return CreateOperatorResponse(
        operator=OperatorView(
            id=row.operator_id,
            username=row.username,
            created_at=row.created_at,
            last_login_at=row.last_login_at,
            is_self=False,
        )
    )


@router.delete("/{operator_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete(
    operator_id: UUID,
    request: Request,
    response: Response,
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> Response:
    """Delete an operator.

    Self-delete is permitted only when at least one other operator
    exists. On a successful self-delete the caller's session row is
    destroyed in the same transaction as the operator row, the cookie
    is cleared on the response, and the response is 204.
    """
    client = request.client
    user_agent = request.headers.get("user-agent")
    request_ip = client.host if client else None

    outcome = delete_operator(
        conn,
        actor_operator_id=session.operator_id,
        actor_username=session.username,
        actor_session_id=session.session_id,
        target_operator_id=operator_id,
        request_ip=request_ip,
        user_agent=user_agent,
    )

    if outcome is DeleteOutcome.NOT_FOUND:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Operator not found",
        )
    if outcome is DeleteOutcome.LAST_OPERATOR:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Cannot delete the only operator",
        )

    if outcome is DeleteOutcome.OK_SELF:
        _clear_session_cookie(response)

    response.status_code = status.HTTP_204_NO_CONTENT
    return response
