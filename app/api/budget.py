"""Budget API endpoints (issue #203).

Endpoints:
  - GET   /budget              — compute and return full budget state snapshot
  - GET   /budget/events       — paginated list of capital events
  - POST  /budget/events       — record an operator capital injection or withdrawal
  - GET   /budget/config       — current budget config (cash_buffer_pct, cgt_scenario)
  - PATCH /budget/config       — partial update of budget config

All endpoints require operator auth.

Fail-closed posture:
  - Missing ``budget_config`` singleton row → 503 (configuration corrupt).
    Never auto-recreated and never substituted with default values.

Commit ownership:
  - POST /budget/events and PATCH /budget/config must commit explicitly.
    The service functions (record_capital_event, update_budget_config) do
    not commit — the API handler owns the commit boundary.
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import Literal

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, model_validator

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.services.budget import (
    BudgetConfigCorrupt,
    compute_budget_state,
    get_budget_config,
    list_capital_events,
    record_capital_event,
    update_budget_config,
)
from app.services.transaction_cost import (
    TransactionCostConfigCorrupt,
    get_transaction_cost_config,
    update_transaction_cost_config,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/budget",
    tags=["budget"],
    dependencies=[Depends(require_session_or_service_token)],
)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class BudgetStateResponse(BaseModel):
    cash_balance: float | None
    deployed_capital: float
    mirror_equity: float
    working_budget: float | None
    estimated_tax_gbp: float
    estimated_tax_usd: float
    gbp_usd_rate: float | None
    cash_buffer_reserve: float
    available_for_deployment: float | None
    cash_buffer_pct: float
    cgt_scenario: str
    tax_year: str


class CapitalEventResponse(BaseModel):
    event_id: int
    event_time: datetime
    event_type: str
    amount: float
    currency: str
    source: str
    note: str | None
    created_by: str | None


class BudgetConfigResponse(BaseModel):
    cash_buffer_pct: float
    cgt_scenario: str
    updated_at: datetime
    updated_by: str
    reason: str


class CostConfigResponse(BaseModel):
    max_total_cost_bps: float
    min_return_vs_cost_ratio: float
    default_hold_days: int
    updated_at: datetime
    updated_by: str
    reason: str


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class CreateCapitalEventRequest(BaseModel):
    event_type: Literal["injection", "withdrawal"]
    amount: float = Field(gt=0)
    currency: Literal["USD", "GBP"] = "USD"
    note: str | None = None


class UpdateBudgetConfigRequest(BaseModel):
    cash_buffer_pct: float | None = Field(default=None, ge=0, le=0.50)
    cgt_scenario: Literal["basic", "higher"] | None = None
    updated_by: str
    reason: str

    @model_validator(mode="after")
    def _at_least_one_field(self) -> UpdateBudgetConfigRequest:
        if self.cash_buffer_pct is None and self.cgt_scenario is None:
            raise ValueError("at least one of cash_buffer_pct or cgt_scenario must be provided")
        return self


class UpdateCostConfigRequest(BaseModel):
    max_total_cost_bps: float | None = Field(default=None, gt=0, le=1000)
    min_return_vs_cost_ratio: float | None = Field(default=None, ge=1.0)
    default_hold_days: int | None = Field(default=None, gt=0, le=365)
    updated_by: str
    reason: str

    @model_validator(mode="after")
    def _at_least_one_field(self) -> UpdateCostConfigRequest:
        if self.max_total_cost_bps is None and self.min_return_vs_cost_ratio is None and self.default_hold_days is None:
            raise ValueError(
                "at least one of max_total_cost_bps, min_return_vs_cost_ratio, or default_hold_days must be provided"
            )
        return self


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


@router.get("", response_model=BudgetStateResponse)
def get_budget(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> BudgetStateResponse:
    """Compute and return a full budget state snapshot."""
    try:
        state = compute_budget_state(conn)
    except BudgetConfigCorrupt:
        logger.exception("budget config corrupt — cannot compute budget state")
        raise HTTPException(status_code=503, detail="budget configuration unavailable")

    return BudgetStateResponse(
        cash_balance=float(state.cash_balance) if state.cash_balance is not None else None,
        deployed_capital=float(state.deployed_capital),
        mirror_equity=float(state.mirror_equity),
        working_budget=float(state.working_budget) if state.working_budget is not None else None,
        estimated_tax_gbp=float(state.estimated_tax_gbp),
        estimated_tax_usd=float(state.estimated_tax_usd),
        gbp_usd_rate=float(state.gbp_usd_rate) if state.gbp_usd_rate is not None else None,
        cash_buffer_reserve=float(state.cash_buffer_reserve),
        available_for_deployment=(
            float(state.available_for_deployment) if state.available_for_deployment is not None else None
        ),
        cash_buffer_pct=float(state.cash_buffer_pct),
        cgt_scenario=state.cgt_scenario,
        tax_year=state.tax_year,
    )


@router.get("/events", response_model=list[CapitalEventResponse])
def get_capital_events(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> list[CapitalEventResponse]:
    """Return a paginated list of capital events ordered by event_time descending."""
    events = list_capital_events(conn, limit=limit, offset=offset)
    return [
        CapitalEventResponse(
            event_id=e.event_id,
            event_time=e.event_time,
            event_type=e.event_type,
            amount=float(e.amount),
            currency=e.currency,
            source=e.source,
            note=e.note,
            created_by=e.created_by,
        )
        for e in events
    ]


@router.post("/events", response_model=CapitalEventResponse, status_code=201)
def create_capital_event(
    body: CreateCapitalEventRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> CapitalEventResponse:
    """Record an operator capital injection or withdrawal."""
    event = record_capital_event(
        conn,
        event_type=body.event_type,
        amount=Decimal(str(body.amount)),
        currency=body.currency,
        note=body.note,
        created_by="operator",
        source="operator",
    )
    conn.commit()
    return CapitalEventResponse(
        event_id=event.event_id,
        event_time=event.event_time,
        event_type=event.event_type,
        amount=float(event.amount),
        currency=event.currency,
        source=event.source,
        note=event.note,
        created_by=event.created_by,
    )


@router.get("/config", response_model=BudgetConfigResponse)
def get_budget_config_endpoint(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> BudgetConfigResponse:
    """Return the current budget configuration."""
    try:
        config = get_budget_config(conn)
    except BudgetConfigCorrupt:
        logger.exception("budget config corrupt — cannot load budget config")
        raise HTTPException(status_code=503, detail="budget configuration unavailable")

    return BudgetConfigResponse(
        cash_buffer_pct=float(config.cash_buffer_pct),
        cgt_scenario=config.cgt_scenario,
        updated_at=config.updated_at,
        updated_by=config.updated_by,
        reason=config.reason,
    )


@router.patch("/config", response_model=BudgetConfigResponse)
def patch_budget_config(
    body: UpdateBudgetConfigRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> BudgetConfigResponse:
    """Partial update of budget configuration.

    Only fields provided as non-None are changed.  Both ``updated_by`` and
    ``reason`` are required for every mutation so the audit trail always
    carries attribution.
    """
    cash_buffer_decimal = Decimal(str(body.cash_buffer_pct)) if body.cash_buffer_pct is not None else None
    try:
        config = update_budget_config(
            conn,
            cash_buffer_pct=cash_buffer_decimal,
            cgt_scenario=body.cgt_scenario,
            updated_by=body.updated_by,
            reason=body.reason,
        )
    except BudgetConfigCorrupt:
        logger.exception("budget config corrupt — cannot update budget config")
        raise HTTPException(status_code=503, detail="budget configuration unavailable")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    conn.commit()
    return BudgetConfigResponse(
        cash_buffer_pct=float(config.cash_buffer_pct),
        cgt_scenario=config.cgt_scenario,
        updated_at=config.updated_at,
        updated_by=config.updated_by,
        reason=config.reason,
    )


@router.get("/cost-config", response_model=CostConfigResponse)
def get_cost_config(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> CostConfigResponse:
    """Return the current transaction cost configuration."""
    try:
        config = get_transaction_cost_config(conn)
    except TransactionCostConfigCorrupt:
        logger.exception("transaction cost config corrupt")
        raise HTTPException(status_code=503, detail="transaction cost configuration unavailable")

    return CostConfigResponse(
        max_total_cost_bps=float(config["max_total_cost_bps"]),
        min_return_vs_cost_ratio=float(config["min_return_vs_cost_ratio"]),
        default_hold_days=config["default_hold_days"],
        updated_at=config["updated_at"],
        updated_by=config["updated_by"],
        reason=config["reason"],
    )


@router.patch("/cost-config", response_model=CostConfigResponse)
def patch_cost_config(
    body: UpdateCostConfigRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> CostConfigResponse:
    """Partial update of transaction cost configuration.

    Only fields provided as non-None are changed.  Both ``updated_by`` and
    ``reason`` are required for every mutation so the audit trail always
    carries attribution.
    """
    try:
        config = update_transaction_cost_config(
            conn,
            max_total_cost_bps=Decimal(str(body.max_total_cost_bps)) if body.max_total_cost_bps is not None else None,
            min_return_vs_cost_ratio=(
                Decimal(str(body.min_return_vs_cost_ratio)) if body.min_return_vs_cost_ratio is not None else None
            ),
            default_hold_days=body.default_hold_days,
            updated_by=body.updated_by,
            reason=body.reason,
        )
    except TransactionCostConfigCorrupt:
        logger.exception("transaction cost config corrupt")
        raise HTTPException(status_code=503, detail="transaction cost configuration unavailable")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    conn.commit()
    return CostConfigResponse(
        max_total_cost_bps=float(config["max_total_cost_bps"]),
        min_return_vs_cost_ratio=float(config["min_return_vs_cost_ratio"]),
        default_hold_days=config["default_hold_days"],
        updated_at=config["updated_at"],
        updated_by=config["updated_by"],
        reason=config["reason"],
    )
