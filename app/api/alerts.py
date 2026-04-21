"""Alerts API (#315 Phase 3).

Guard-rejection alerts strip. Scope is intentionally narrow — this is
the execution-guard read surface only. Thesis breaches (#394) and
filings-status drops (#395) are deferred; #396 wires them into the
same strip once their event persistence lands.

Cursor model: operators.alerts_last_seen_decision_id (BIGINT). See
``docs/superpowers/specs/2026-04-21-alerts-strip-guard-rejections.md``
for why a decision_id cursor rather than decision_time.

Routes:
  GET  /alerts/guard-rejections   — 7-day window, 500-row cap, ORDER BY decision_id DESC
  POST /alerts/seen               — body {seen_through_decision_id}, monotonic GREATEST + LEAST clamp
  POST /alerts/dismiss-all        — no body, atomic MAX-in-window advance, no-op on empty window
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.services.operators import AmbiguousOperatorError, NoOperatorError, sole_operator_id

router = APIRouter(
    prefix="/alerts",
    tags=["alerts"],
    dependencies=[Depends(require_session_or_service_token)],
)

GuardAction = Literal["BUY", "ADD", "HOLD", "EXIT"]


class GuardRejection(BaseModel):
    decision_id: int
    decision_time: datetime
    instrument_id: int | None
    symbol: str | None
    action: GuardAction | None
    explanation: str


class GuardRejectionsResponse(BaseModel):
    alerts_last_seen_decision_id: int | None
    unseen_count: int
    rejections: list[GuardRejection]


class MarkSeenRequest(BaseModel):
    seen_through_decision_id: int = Field(gt=0)


def _resolve_operator(conn: psycopg.Connection[object]) -> UUID:
    try:
        return sole_operator_id(conn)
    except NoOperatorError as exc:
        raise HTTPException(status_code=503, detail="no operator configured") from exc
    except AmbiguousOperatorError as exc:
        raise HTTPException(
            status_code=501,
            detail="multiple operators present — alerts require a per-session operator context",
        ) from exc


@router.get("/guard-rejections", response_model=GuardRejectionsResponse)
def get_guard_rejections(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> GuardRejectionsResponse:
    _operator_id = _resolve_operator(conn)  # used in Task 3 query
    # Implementation in Task 3.
    return GuardRejectionsResponse(
        alerts_last_seen_decision_id=None,
        unseen_count=0,
        rejections=[],
    )


@router.post("/seen", status_code=status.HTTP_204_NO_CONTENT)
def mark_seen(
    body: MarkSeenRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    _operator_id = _resolve_operator(conn)  # used in Task 4 UPDATE
    # Implementation in Task 4.


@router.post("/dismiss-all", status_code=status.HTTP_204_NO_CONTENT)
def dismiss_all(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    _operator_id = _resolve_operator(conn)  # used in Task 5 UPDATE
    # Implementation in Task 5.
