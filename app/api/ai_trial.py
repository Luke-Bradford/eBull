"""#3514 — ``GET /ai-trial/status``: the AI trial's readiness, every no-op state named.

A read over stored rows (``app.services.ai_trial_status``). It grants nothing and triggers
nothing; the trial's controls stay with the supervisor runbook on #3471.

#3515: ``?arm=&version=`` selects a registered trial version (default v1); an unregistered one
is a 404, never another version's status.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.services.ai_trial_status import (
    DecisionState,
    ExecutionState,
    JobFire,
    TrialState,
    load_trial_status,
)
from app.services.ai_trial_version import V1, trial_version

router = APIRouter(
    prefix="/ai-trial",
    tags=["ai-trial"],
    dependencies=[Depends(require_session_or_service_token)],
)


class JobFireResponse(BaseModel):
    job_name: str
    next_fire_at: datetime
    last_started_at: datetime | None
    last_finished_at: datetime | None
    last_status: str | None
    last_note: str | None


class DecisionResponse(BaseModel):
    state: DecisionState
    label: str
    legs: int
    refusal_reason: str | None
    decision_refusals: dict[str, int]


class ExecutionResponse(BaseModel):
    state: ExecutionState
    label: str
    count: int
    reason: str | None


class SessionResponse(BaseModel):
    session_date: date
    decision: DecisionResponse
    execution: list[ExecutionResponse]


class OpenLegResponse(BaseModel):
    leg: str
    pair_seq: int
    strategy_trade_id: int
    symbol: str
    trade_status: str
    entry_price: Decimal | None
    requested_stop: Decimal | None
    requested_target: Decimal | None
    broker_observed: bool
    broker_stop: Decimal | None
    broker_target: Decimal | None
    pnl_usd: Decimal | None
    exit_deadline_session: date | None


class LegLossResponse(BaseModel):
    leg: str
    pnl_usd: Decimal
    unmeasured: int
    limit_usd: Decimal
    headroom_usd: Decimal


class TrialStatusResponse(BaseModel):
    arm_strategy_id: str
    strategy_version: str
    state: TrialState
    declaration_id: int | None
    state_reason: str | None
    state_at: datetime | None
    decision_job: JobFireResponse
    execute_job: JobFireResponse
    sessions: list[SessionResponse]
    open_legs: list[OpenLegResponse]
    loss: list[LegLossResponse]


def _job(fire: JobFire) -> JobFireResponse:
    return JobFireResponse(
        job_name=fire.job_name,
        next_fire_at=fire.next_fire_at,
        last_started_at=fire.last_started_at,
        last_finished_at=fire.last_finished_at,
        last_status=fire.last_status,
        last_note=fire.last_note,
    )


@router.get("/status", response_model=TrialStatusResponse)
def get_ai_trial_status(
    arm: str = Query(V1.arm_strategy_id, max_length=64),
    version: str = Query(V1.strategy_version, max_length=16),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> TrialStatusResponse:
    try:
        trial = trial_version(arm, version)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    status = load_trial_status(conn, version=trial)
    return TrialStatusResponse(
        arm_strategy_id=status.arm_strategy_id,
        strategy_version=status.strategy_version,
        state=status.state,
        declaration_id=status.declaration_id,
        state_reason=status.state_reason,
        state_at=status.state_at,
        decision_job=_job(status.decision_job),
        execute_job=_job(status.execute_job),
        sessions=[
            SessionResponse(
                session_date=session.session_date,
                decision=DecisionResponse(
                    state=session.decision.state,
                    label=session.decision.label,
                    legs=session.decision.legs,
                    refusal_reason=session.decision.refusal_reason,
                    decision_refusals=dict(session.decision.decision_refusals),
                ),
                execution=[
                    ExecutionResponse(state=item.state, label=item.label, count=item.count, reason=item.reason)
                    for item in session.execution
                ],
            )
            for session in status.sessions
        ],
        open_legs=[OpenLegResponse(**vars(leg)) for leg in status.open_legs],
        loss=[
            LegLossResponse(
                leg=item.leg,
                pnl_usd=item.pnl_usd,
                unmeasured=item.unmeasured,
                limit_usd=item.limit_usd,
                headroom_usd=item.headroom_usd,
            )
            for item in status.loss
        ],
    )
