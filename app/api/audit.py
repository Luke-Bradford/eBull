"""Execution audit trail API endpoints.

Reads from:
  - decision_audit  (one row per execution guard / order client invocation)
  - instruments      (symbol, company_name for display)

No writes. No schema changes.

Note on terminology:
  The issue (#54) refers to filtering by "decision (approve/reject)".
  The actual DB column is ``pass_fail`` with values ``PASS`` / ``FAIL``.
  This endpoint uses the DB-native vocabulary.

evidence_json shape varies by stage:
  - execution_guard: list of {"rule": str, "passed": bool, "detail": str}
  - order_client:    {"order_id": ..., "raw_payload": ...}
  The column is JSONB, so the response type is ``object | None``.

Ordering: decision_time DESC, decision_id DESC (newest first, deterministic).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.api._helpers import parse_optional_int
from app.db import get_conn

router = APIRouter(prefix="/audit", tags=["audit"])

MAX_PAGE_LIMIT = 200

PassFail = Literal["PASS", "FAIL"]
Stage = Literal["execution_guard", "order_client"]


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class AuditListItem(BaseModel):
    decision_id: int
    decision_time: datetime
    instrument_id: int | None
    symbol: str | None
    company_name: str | None
    recommendation_id: int | None
    stage: Stage
    model_version: str | None
    pass_fail: PassFail
    explanation: str


class AuditListResponse(BaseModel):
    items: list[AuditListItem]
    total: int
    offset: int
    limit: int


class AuditDetail(BaseModel):
    decision_id: int
    decision_time: datetime
    instrument_id: int | None
    symbol: str | None
    company_name: str | None
    recommendation_id: int | None
    stage: Stage
    model_version: str | None
    pass_fail: PassFail
    explanation: str
    evidence_json: object | None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_list_item(row: dict[str, Any]) -> AuditListItem:
    return AuditListItem(
        decision_id=row["decision_id"],
        decision_time=row["decision_time"],
        instrument_id=parse_optional_int(row, "instrument_id"),
        symbol=row["symbol"],
        company_name=row["company_name"],
        recommendation_id=parse_optional_int(row, "recommendation_id"),
        stage=row["stage"],
        model_version=row["model_version"],
        pass_fail=row["pass_fail"],
        explanation=row["explanation"],
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("", response_model=AuditListResponse)
def list_audit(
    conn: psycopg.Connection[object] = Depends(get_conn),
    instrument_id: int | None = Query(default=None),
    pass_fail: PassFail | None = Query(default=None),
    stage: Stage | None = Query(default=None),
    date_from: datetime | None = Query(
        default=None,
        description="Inclusive lower bound on decision_time",
    ),
    date_to: datetime | None = Query(
        default=None,
        description="Exclusive upper bound on decision_time",
    ),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=MAX_PAGE_LIMIT),
) -> AuditListResponse:
    """Paginated execution audit log.

    Filters:
      - instrument_id: exact match
      - pass_fail: PASS or FAIL
      - stage: execution_guard or order_client
      - date_from: inclusive lower bound on decision_time (>=)
      - date_to: exclusive upper bound on decision_time (<)

    Ordering: decision_time DESC, decision_id DESC (newest first).
    """
    where_clauses: list[str] = []
    filter_params: dict[str, object] = {}

    if instrument_id is not None:
        where_clauses.append("da.instrument_id = %(instrument_id)s")
        filter_params["instrument_id"] = instrument_id
    if pass_fail is not None:
        where_clauses.append("da.pass_fail = %(pass_fail)s")
        filter_params["pass_fail"] = pass_fail
    if stage is not None:
        where_clauses.append("da.stage = %(stage)s")
        filter_params["stage"] = stage
    if date_from is not None:
        where_clauses.append("da.decision_time >= %(date_from)s")
        filter_params["date_from"] = date_from
    if date_to is not None:
        where_clauses.append("da.decision_time < %(date_to)s")
        filter_params["date_to"] = date_to

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    # -- COUNT query (separate params — no limit/offset) -------------------
    # S608 safe: all SQL fragments are hardcoded; user input is parameterised.
    count_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM decision_audit da
        LEFT JOIN instruments i USING (instrument_id)
        {where_sql}"""  # noqa: S608

    # -- Items query -------------------------------------------------------
    items_params: dict[str, object] = {
        **filter_params,
        "limit": limit,
        "offset": offset,
    }
    # S608 safe: all SQL fragments are hardcoded; user input is parameterised.
    items_sql = f"""
        SELECT da.decision_id, da.decision_time,
               da.instrument_id,
               i.symbol, i.company_name,
               da.recommendation_id,
               da.stage, da.model_version,
               da.pass_fail, da.explanation
        FROM decision_audit da
        LEFT JOIN instruments i USING (instrument_id)
        {where_sql}
        ORDER BY da.decision_time DESC, da.decision_id DESC
        LIMIT %(limit)s OFFSET %(offset)s"""  # noqa: S608

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(count_sql, filter_params)  # type: ignore[arg-type]
        count_row = cur.fetchone()
        # COUNT() always returns exactly one row; guard on column value.
        total: int = count_row["cnt"] if count_row else 0  # type: ignore[index]

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(items_sql, items_params)  # type: ignore[arg-type]
        rows = cur.fetchall()

    items = [_parse_list_item(r) for r in rows]

    return AuditListResponse(
        items=items,
        total=total,
        offset=offset,
        limit=limit,
    )


@router.get("/{decision_id}", response_model=AuditDetail)
def get_audit(
    decision_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> AuditDetail:
    """Single audit row with full evidence_json."""
    sql = """
        SELECT da.decision_id, da.decision_time,
               da.instrument_id,
               i.symbol, i.company_name,
               da.recommendation_id,
               da.stage, da.model_version,
               da.pass_fail, da.explanation,
               da.evidence_json
        FROM decision_audit da
        LEFT JOIN instruments i USING (instrument_id)
        WHERE da.decision_id = %(decision_id)s
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, {"decision_id": decision_id})
        row = cur.fetchone()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Audit row {decision_id} not found",
        )

    return AuditDetail(
        decision_id=row["decision_id"],
        decision_time=row["decision_time"],
        instrument_id=parse_optional_int(row, "instrument_id"),
        symbol=row["symbol"],
        company_name=row["company_name"],
        recommendation_id=parse_optional_int(row, "recommendation_id"),
        stage=row["stage"],
        model_version=row["model_version"],
        pass_fail=row["pass_fail"],
        explanation=row["explanation"],
        evidence_json=row["evidence_json"],
    )
