"""Alerts API — dashboard strip read + cursor endpoints.

Provides three independent alert feeds sharing the same dashboard strip shape:

1. Execution-guard rejections (#315 Phase 3 / PR #394):
   - GET  /alerts/guard-rejections
   - POST /alerts/seen               (body: {seen_through_decision_id})
   - POST /alerts/dismiss-all

2. Position alerts (SL/TP/thesis breach episodes, #396):
   - GET  /alerts/position-alerts
   - POST /alerts/position-alerts/seen          (body: {seen_through_position_alert_id})
   - POST /alerts/position-alerts/dismiss-all

3. Coverage status drops from 'analysable' (#397):
   - GET  /alerts/coverage-status-drops
   - POST /alerts/coverage-status-drops/seen    (body: {seen_through_event_id})
   - POST /alerts/coverage-status-drops/dismiss-all

Each feed maintains its own BIGSERIAL cursor column on ``operators`` and a
7-day window. Cursor semantics are identical across feeds: strict ``>``
comparison, GREATEST+COALESCE monotonicity, LEAST clamp on /seen, MAX
advance on /dismiss-all, and ``m.max_id IS NOT NULL`` empty-window guard.
See specs at ``docs/superpowers/specs/2026-04-21-alerts-strip-guard-rejections.md``
(guard), ``docs/superpowers/specs/2026-04-21-position-alert-persistence.md``
(position), and ``docs/superpowers/specs/2026-04-22-coverage-status-transition-log.md``
(coverage).

Known divergence between the guard /seen endpoint and the other two: guard
``/alerts/seen`` writes ``0`` as the cursor on an empty window + NULL cursor
(see #395 tech-debt). Position and coverage /seen endpoints do not — they
use the ``m.max_id IS NOT NULL`` guard as dismiss-all to preserve
``NULL = never acknowledged``.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.db.snapshot import snapshot_read
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


AlertType = Literal["sl_breach", "tp_breach", "thesis_break"]


class PositionAlert(BaseModel):
    alert_id: int
    alert_type: AlertType
    instrument_id: int
    symbol: str
    opened_at: datetime
    resolved_at: datetime | None
    detail: str
    current_bid: Decimal | None


class PositionAlertsResponse(BaseModel):
    alerts_last_seen_position_alert_id: int | None
    unseen_count: int
    alerts: list[PositionAlert]


class PositionAlertsMarkSeenRequest(BaseModel):
    seen_through_position_alert_id: int = Field(gt=0)


class CoverageStatusDrop(BaseModel):
    event_id: int
    instrument_id: int
    symbol: str
    changed_at: datetime
    old_status: str
    new_status: str | None


class CoverageStatusDropsResponse(BaseModel):
    alerts_last_seen_coverage_event_id: int | None
    unseen_count: int
    drops: list[CoverageStatusDrop]


class CoverageStatusDropsMarkSeenRequest(BaseModel):
    seen_through_event_id: int = Field(gt=0)


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
    # #395: three sequential reads must agree on a single snapshot,
    # otherwise a concurrent guard FAIL between Q2 and Q3 lets the
    # list contain N+1 rows while unseen_count reports N (pill lags
    # by one). REPEATABLE READ over the whole handler closes the
    # window. Read-only block — no writes inside.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur, snapshot_read(conn):
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


@router.post("/position-alerts/seen", status_code=status.HTTP_204_NO_CONTENT)
def mark_position_alerts_seen(
    body: PositionAlertsMarkSeenRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    operator_id = _resolve_operator(conn)
    with conn.cursor() as cur:
        # The m.max_id IS NOT NULL guard makes this a no-op on an empty
        # window — without it, LEAST(client_posted, NULL) would short-circuit
        # to NULL and GREATEST(COALESCE(cursor, 0), NULL) would itself be NULL
        # (PostgreSQL GREATEST ignores NULL arguments), but the simpler reading
        # is: we never want to materialise a cursor value when no rows exist.
        cur.execute(
            """
            UPDATE operators AS op
            SET alerts_last_seen_position_alert_id = GREATEST(
                COALESCE(op.alerts_last_seen_position_alert_id, 0),
                LEAST(%(seen_through_position_alert_id)s, m.max_id)
            )
            FROM (
                SELECT MAX(alert_id) AS max_id
                FROM position_alerts
                WHERE opened_at >= now() - INTERVAL '7 days'
            ) AS m
            WHERE op.operator_id = %(op)s
              AND m.max_id IS NOT NULL
            """,
            {
                "seen_through_position_alert_id": body.seen_through_position_alert_id,
                "op": operator_id,
            },
        )
    conn.commit()


@router.post("/position-alerts/dismiss-all", status_code=status.HTTP_204_NO_CONTENT)
def dismiss_all_position_alerts(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    operator_id = _resolve_operator(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE operators AS op
            SET alerts_last_seen_position_alert_id = GREATEST(
                COALESCE(op.alerts_last_seen_position_alert_id, 0),
                m.max_id
            )
            FROM (
                SELECT MAX(alert_id) AS max_id
                FROM position_alerts
                WHERE opened_at >= now() - INTERVAL '7 days'
            ) AS m
            WHERE op.operator_id = %(op)s
              AND m.max_id IS NOT NULL
            """,
            {"op": operator_id},
        )
    conn.commit()


@router.get("/position-alerts", response_model=PositionAlertsResponse)
def get_position_alerts(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> PositionAlertsResponse:
    operator_id = _resolve_operator(conn)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # 1. Read operator's cursor.
        cur.execute(
            "SELECT alerts_last_seen_position_alert_id FROM operators WHERE operator_id = %(op)s",
            {"op": operator_id},
        )
        op_row = cur.fetchone()
        last_seen: int | None = op_row["alerts_last_seen_position_alert_id"] if op_row else None

        # 2. Count unseen in-window rows (uncapped).
        cur.execute(
            """
            SELECT COUNT(*) AS unseen_count
            FROM position_alerts
            WHERE opened_at >= now() - INTERVAL '7 days'
              AND (%(last_id)s::BIGINT IS NULL OR alert_id > %(last_id)s::BIGINT)
            """,
            {"last_id": last_seen},
        )
        count_row = cur.fetchone()
        assert count_row is not None, "COUNT(*) always returns a row"
        unseen_count: int = int(count_row["unseen_count"])

        # 3. Fetch the list (capped at 500). ORDER BY alert_id DESC —
        # BIGSERIAL PK is the race-safe ordering (clock-skew irrelevant;
        # single-threaded writer guarantees monotonicity). Matches #394
        # rationale for decision_id.
        cur.execute(
            """
            SELECT
                pa.alert_id,
                pa.alert_type,
                pa.instrument_id,
                i.symbol,
                pa.opened_at,
                pa.resolved_at,
                pa.detail,
                pa.current_bid
            FROM position_alerts pa
            JOIN instruments i ON i.instrument_id = pa.instrument_id
            WHERE pa.opened_at >= now() - INTERVAL '7 days'
            ORDER BY pa.alert_id DESC
            LIMIT 500
            """
        )
        rows = cur.fetchall()

    return PositionAlertsResponse(
        alerts_last_seen_position_alert_id=last_seen,
        unseen_count=unseen_count,
        alerts=[PositionAlert.model_validate(r) for r in rows],
    )


@router.get("/coverage-status-drops", response_model=CoverageStatusDropsResponse)
def get_coverage_status_drops(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> CoverageStatusDropsResponse:
    operator_id = _resolve_operator(conn)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # 1. Read operator's cursor.
        cur.execute(
            "SELECT alerts_last_seen_coverage_event_id FROM operators WHERE operator_id = %(op)s",
            {"op": operator_id},
        )
        op_row = cur.fetchone()
        last_seen: int | None = op_row["alerts_last_seen_coverage_event_id"] if op_row else None

        # 2. Count unseen in-window drops (uncapped).
        cur.execute(
            """
            SELECT COUNT(*) AS unseen_count
            FROM coverage_status_events
            WHERE old_status = 'analysable'
              AND new_status IS DISTINCT FROM 'analysable'
              AND changed_at >= now() - INTERVAL '7 days'
              AND (%(last_id)s::BIGINT IS NULL OR event_id > %(last_id)s::BIGINT)
            """,
            {"last_id": last_seen},
        )
        count_row = cur.fetchone()
        assert count_row is not None, "COUNT(*) always returns a row"
        unseen_count: int = int(count_row["unseen_count"])

        # 3. Fetch list capped at 500. ORDER BY event_id DESC — BIGSERIAL PK
        # is race-safe (advisory xact lock in migration 047's trigger
        # serializes concurrent coverage writers, matching #396 rationale).
        cur.execute(
            """
            SELECT
                e.event_id,
                e.instrument_id,
                i.symbol,
                e.changed_at,
                e.old_status,
                e.new_status
            FROM coverage_status_events e
            JOIN instruments i ON i.instrument_id = e.instrument_id
            WHERE e.old_status = 'analysable'
              AND e.new_status IS DISTINCT FROM 'analysable'
              AND e.changed_at >= now() - INTERVAL '7 days'
            ORDER BY e.event_id DESC
            LIMIT 500
            """
        )
        rows = cur.fetchall()

    return CoverageStatusDropsResponse(
        alerts_last_seen_coverage_event_id=last_seen,
        unseen_count=unseen_count,
        drops=[CoverageStatusDrop.model_validate(r) for r in rows],
    )


@router.post("/coverage-status-drops/seen", status_code=status.HTTP_204_NO_CONTENT)
def mark_coverage_status_drops_seen(
    body: CoverageStatusDropsMarkSeenRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    operator_id = _resolve_operator(conn)
    with conn.cursor() as cur:
        # m.max_id IS NOT NULL guard preserves NULL cursor on empty window.
        # Matches /alerts/position-alerts/seen (post-#395 correct shape) rather
        # than guard /alerts/seen (pre-#395 divergent shape).
        cur.execute(
            """
            UPDATE operators AS op
            SET alerts_last_seen_coverage_event_id = GREATEST(
                COALESCE(op.alerts_last_seen_coverage_event_id, 0),
                LEAST(%(seen_through_event_id)s, m.max_id)
            )
            FROM (
                SELECT MAX(event_id) AS max_id
                FROM coverage_status_events
                WHERE old_status = 'analysable'
                  AND new_status IS DISTINCT FROM 'analysable'
                  AND changed_at >= now() - INTERVAL '7 days'
            ) AS m
            WHERE op.operator_id = %(op)s
              AND m.max_id IS NOT NULL
            """,
            {
                "seen_through_event_id": body.seen_through_event_id,
                "op": operator_id,
            },
        )
    conn.commit()


@router.post("/coverage-status-drops/dismiss-all", status_code=status.HTTP_204_NO_CONTENT)
def dismiss_all_coverage_status_drops(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    operator_id = _resolve_operator(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE operators AS op
            SET alerts_last_seen_coverage_event_id = GREATEST(
                COALESCE(op.alerts_last_seen_coverage_event_id, 0),
                m.max_id
            )
            FROM (
                SELECT MAX(event_id) AS max_id
                FROM coverage_status_events
                WHERE old_status = 'analysable'
                  AND new_status IS DISTINCT FROM 'analysable'
                  AND changed_at >= now() - INTERVAL '7 days'
            ) AS m
            WHERE op.operator_id = %(op)s
              AND m.max_id IS NOT NULL
            """,
            {"op": operator_id},
        )
    conn.commit()
