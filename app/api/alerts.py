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
    operator_id = _resolve_operator(conn)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # 1. Read operator's cursor.
        cur.execute(
            "SELECT alerts_last_seen_decision_id FROM operators WHERE operator_id = %(op)s",
            {"op": operator_id},
        )
        op_row = cur.fetchone()
        last_seen: int | None = op_row["alerts_last_seen_decision_id"] if op_row else None

        # 2. Count unseen in-window rows (uncapped).
        cur.execute(
            """
            SELECT COUNT(*) AS unseen_count
            FROM decision_audit
            WHERE pass_fail = 'FAIL'
              AND stage = 'execution_guard'
              AND decision_time >= now() - INTERVAL '7 days'
              AND (%(last_id)s::BIGINT IS NULL OR decision_id > %(last_id)s::BIGINT)
            """,
            {"last_id": last_seen},
        )
        count_row = cur.fetchone()
        assert count_row is not None, "COUNT(*) always returns a row"
        unseen_count: int = int(count_row["unseen_count"])

        # 3. Fetch the list (capped at 500). Ordering is by decision_id DESC
        # (the PK sequence), not decision_time DESC — decision_time is app-supplied
        # via _utcnow() and can be clock-skewed, which would break the invariant
        # that rejections[0].decision_id === MAX(decision_id).
        cur.execute(
            """
            SELECT
                da.decision_id,
                da.decision_time,
                da.instrument_id,
                i.symbol,
                tr.action,
                da.explanation
            FROM decision_audit da
            LEFT JOIN instruments i ON i.instrument_id = da.instrument_id
            LEFT JOIN trade_recommendations tr ON tr.recommendation_id = da.recommendation_id
            WHERE da.pass_fail = 'FAIL'
              AND da.stage = 'execution_guard'
              AND da.decision_time >= now() - INTERVAL '7 days'
            ORDER BY da.decision_id DESC
            LIMIT 500
            """
        )
        rows = cur.fetchall()

    return GuardRejectionsResponse(
        alerts_last_seen_decision_id=last_seen,
        unseen_count=unseen_count,
        rejections=[GuardRejection.model_validate(r) for r in rows],
    )


@router.post("/seen", status_code=status.HTTP_204_NO_CONTENT)
def mark_seen(
    body: MarkSeenRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    operator_id = _resolve_operator(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE operators
            SET alerts_last_seen_decision_id = GREATEST(
                COALESCE(alerts_last_seen_decision_id, 0),
                LEAST(
                    %(seen_through_decision_id)s,
                    COALESCE((
                        SELECT MAX(decision_id)
                        FROM decision_audit
                        WHERE pass_fail = 'FAIL'
                          AND stage = 'execution_guard'
                          AND decision_time >= now() - INTERVAL '7 days'
                    ), 0)
                )
            )
            WHERE operator_id = %(op)s
            """,
            {
                "seen_through_decision_id": body.seen_through_decision_id,
                "op": operator_id,
            },
        )
    conn.commit()


@router.post("/dismiss-all", status_code=status.HTTP_204_NO_CONTENT)
def dismiss_all(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    operator_id = _resolve_operator(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE operators AS op
            SET alerts_last_seen_decision_id = GREATEST(
                COALESCE(op.alerts_last_seen_decision_id, 0),
                m.max_id
            )
            FROM (
                SELECT MAX(decision_id) AS max_id
                FROM decision_audit
                WHERE pass_fail = 'FAIL'
                  AND stage = 'execution_guard'
                  AND decision_time >= now() - INTERVAL '7 days'
            ) AS m
            WHERE op.operator_id = %(op)s
              AND m.max_id IS NOT NULL
            """,
            {"op": operator_id},
        )
    conn.commit()
