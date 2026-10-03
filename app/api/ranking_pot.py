"""#2842 slice 7 — ``GET /ranking-pot/status`` and ``GET /ranking-pot/readout``; #3592 slice 4c — the same pair for
ranking-pot-v2 under ``/ranking-pot/v2/`` (``app.services.ranking_pot_status_v2``).

Reads over stored rows (``app.services.ranking_pot_status``). They grant nothing and trigger nothing; the pot's
freeze and activation stay with the supervisor scripts. The readout is its own route because it walks every step
row (K-wide control columns), so the positions never wait on it.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.api.ai_trial import JobFireResponse
from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.services import ranking_pot_status_v2 as status_v2
from app.services.ranking_pot_exec import Status
from app.services.ranking_pot_status import load_readout, load_status

router = APIRouter(
    prefix="/ranking-pot",
    tags=["ranking-pot"],
    dependencies=[Depends(require_session_or_service_token)],
)


class DeclarationResponse(BaseModel):
    declaration_id: int
    frozen_at: datetime
    state: str | None
    state_reason: str | None
    state_at: datetime | None
    pot_capital: Decimal | None


class RebalanceResponse(BaseModel):
    month: date
    target_session: date
    outcome: str
    refusal: str | None
    fired_at: datetime
    executed_state: str | None
    entries_allowed: bool | None
    v1_active: bool | None


class StepResponse(BaseModel):
    latest_session: date | None
    refusal_session: date | None
    refusal_reason: str | None
    refusal_at: datetime | None


class LookResponse(BaseModel):
    look_months: int
    endpoint_session: date
    kind: str
    verdict: str
    harm: bool
    reasons: list[str]
    computed_at: datetime


class PositionResponse(BaseModel):
    lifecycle_id: int
    slot: int
    instrument_id: int
    symbol: str | None
    status: Status
    trade_status: str | None
    target_session: date
    funding_reason: str | None
    exit_session: date | None
    exit_reason: str | None
    amount: Decimal | None
    ask: Decimal | None
    quote_at: datetime | None
    sent_stop_loss: Decimal | None
    sent_take_profit: Decimal | None
    held_observed_at: datetime | None
    held_stop_loss: Decimal | None
    held_take_profit: Decimal | None
    held_no_stop_loss: bool | None
    held_no_take_profit: bool | None
    realized_pnl_usd: Decimal | None
    unrealized_pnl_usd: Decimal | None
    marked_on: date | None
    ticket: dict[str, Any]
    ticket_verified: bool


class PotStatusResponse(BaseModel):
    strategy_id: str
    strategy_version: str
    build_complete: bool
    declaration: DeclarationResponse | None
    jobs: list[JobFireResponse]
    rebalances: list[RebalanceResponse]
    step: StepResponse | None
    looks: list[LookResponse]
    held: list[PositionResponse]
    recent: list[PositionResponse]


class PotReadoutResponse(BaseModel):
    declaration_id: int | None
    endpoint: date | None
    readout: dict[str, Any] | None
    reason: str | None


class V2LookResponse(BaseModel):
    look_id: int
    look_months: int
    endpoint: date
    v2_verdict: str
    harm: bool
    reasons: list[str]
    reference_condition: bool | None
    turnover_condition: bool | None
    invalidated_by: int | None
    invalidated_note: str | None


class V2HoldingResponse(BaseModel):
    instrument_id: int
    symbol: str | None
    state: str
    slot: int | None
    entry_session: date
    entry_fill: Decimal | None
    invested: Decimal | None
    value: Decimal | None
    last_close: Decimal | None
    last_close_session: date | None
    stop_loss: Decimal | None
    take_profit: Decimal | None
    reasons: dict[str, Any] | None


class PotStatusV2Response(BaseModel):
    strategy_id: str
    strategy_version: str
    build_complete: bool
    declaration: DeclarationResponse | None
    jobs: list[JobFireResponse]
    rebalances: list[RebalanceResponse]
    step: StepResponse | None
    shadow_nav: Decimal | None
    looks: list[V2LookResponse]
    looks_withheld: bool
    holdings: list[V2HoldingResponse]
    holdings_withheld: bool


@router.get("/status", response_model=PotStatusResponse)
def get_ranking_pot_status(conn: psycopg.Connection[object] = Depends(get_conn)) -> PotStatusResponse:
    return PotStatusResponse.model_validate(asdict(load_status(conn)))  # type: ignore[arg-type]


@router.get("/readout", response_model=PotReadoutResponse)
def get_ranking_pot_readout(conn: psycopg.Connection[object] = Depends(get_conn)) -> PotReadoutResponse:
    return PotReadoutResponse.model_validate(asdict(load_readout(conn)))  # type: ignore[arg-type]


@router.get("/v2/status", response_model=PotStatusV2Response)
def get_ranking_pot_v2_status(conn: psycopg.Connection[object] = Depends(get_conn)) -> PotStatusV2Response:
    return PotStatusV2Response.model_validate(asdict(status_v2.load_status(conn)))  # type: ignore[arg-type]


@router.get("/v2/readout", response_model=PotReadoutResponse)
def get_ranking_pot_v2_readout(conn: psycopg.Connection[object] = Depends(get_conn)) -> PotReadoutResponse:
    return PotReadoutResponse.model_validate(asdict(status_v2.load_readout(conn)))  # type: ignore[arg-type]
