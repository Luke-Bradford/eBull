"""Tax & CGT read API (#1905).

Passive read surface over the UK tax engine (``app/services/tax_ledger.py``).
The engine owns every CGT treatment (same-day / bed-&-breakfast / s104
matching, era-dependent scenario rates, £3,000 annual exempt); these handlers
only shape its output for the UI — no tax logic lives here.

Endpoints (all operator-auth):
  - GET /tax/summary?tax_year=YYYY/YY   — tax-year totals + CGT scenario estimates
  - GET /tax/disposals?tax_year=YYYY/YY — disposal-match rows for the year
  - GET /tax/pools                      — current s104 pool state per instrument
  - GET /tax/tax-years                  — {current, available[]} for the UI selector

``tax_year`` defaults to the current UK tax year and is validated by
``tax_ledger.valid_tax_year`` (shape + suffix arithmetic) — a malformed or
impossible label is a 422, never a silently-empty summary.

Read-only: no writes, no commit boundary. Each handler wraps its (multi-query)
read in ``snapshot_read`` for a consistent snapshot.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.db.snapshot import snapshot_read
from app.services.tax_ledger import (
    available_tax_years,
    current_tax_year,
    disposals_for_tax_year,
    s104_pool_rows,
    tax_year_summary,
    valid_tax_year,
)

router = APIRouter(
    prefix="/tax",
    tags=["tax"],
    dependencies=[Depends(require_session_or_service_token)],
)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class TaxSummaryResponse(BaseModel):
    tax_year: str
    total_gains_gbp: float
    total_losses_gbp: float
    net_gain_gbp: float
    dividend_total_gbp: float
    disposals_same_day: int
    disposals_bed_and_breakfast: int
    disposals_s104: int
    annual_exempt_gbp: float
    exempt_remaining_gbp: float
    estimated_cgt_basic_scenario: float
    estimated_cgt_higher_scenario: float


class TaxDisposalResponse(BaseModel):
    match_id: int
    instrument_id: int
    symbol: str
    matching_rule: str
    matched_units: float
    acquisition_cost_gbp: float
    disposal_proceeds_gbp: float
    gain_or_loss_gbp: float
    disposal_uk_date: date
    tax_year: str
    disposal_tax_lot_id: int
    acquisition_tax_lot_id: int | None
    matched_at: datetime


class S104PoolResponse(BaseModel):
    instrument_id: int
    symbol: str
    pool_units: float
    pool_cost_gbp: float
    pool_avg_cost_gbp: float
    updated_at: datetime


class TaxYearsResponse(BaseModel):
    current: str
    available: list[str]


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def _f(d: Decimal) -> float:
    return float(d)


def _resolve_tax_year(tax_year: str | None) -> str:
    """Default to the current UK tax year; 422 on a malformed/impossible label."""
    ty = tax_year or current_tax_year()
    if not valid_tax_year(ty):
        raise HTTPException(
            status_code=422,
            detail=f"invalid tax_year '{ty}'; expected UK tax-year label like '2026/27'",
        )
    return ty


@router.get("/summary", response_model=TaxSummaryResponse)
def get_tax_summary(
    tax_year: str | None = Query(default=None),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> TaxSummaryResponse:
    """Tax-year gains/losses, dividends, exempt-remaining and CGT scenario estimates."""
    ty = _resolve_tax_year(tax_year)
    with snapshot_read(conn):
        s = tax_year_summary(conn, ty)
    return TaxSummaryResponse(
        tax_year=s.tax_year,
        total_gains_gbp=_f(s.total_gains_gbp),
        total_losses_gbp=_f(s.total_losses_gbp),
        net_gain_gbp=_f(s.net_gain_gbp),
        dividend_total_gbp=_f(s.dividend_total_gbp),
        disposals_same_day=s.disposals_same_day,
        disposals_bed_and_breakfast=s.disposals_bed_and_breakfast,
        disposals_s104=s.disposals_s104,
        annual_exempt_gbp=_f(s.annual_exempt_gbp),
        exempt_remaining_gbp=_f(s.exempt_remaining_gbp),
        estimated_cgt_basic_scenario=_f(s.estimated_cgt_basic_scenario),
        estimated_cgt_higher_scenario=_f(s.estimated_cgt_higher_scenario),
    )


@router.get("/disposals", response_model=list[TaxDisposalResponse])
def get_tax_disposals(
    tax_year: str | None = Query(default=None),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> list[TaxDisposalResponse]:
    """Every disposal match in the tax year (disposal → matched acquisition, rule, gain/loss)."""
    ty = _resolve_tax_year(tax_year)
    with snapshot_read(conn):
        rows = disposals_for_tax_year(conn, ty)
    return [
        TaxDisposalResponse(
            match_id=r.match_id,
            instrument_id=r.instrument_id,
            symbol=r.symbol,
            matching_rule=r.matching_rule,
            matched_units=_f(r.matched_units),
            acquisition_cost_gbp=_f(r.acquisition_cost_gbp),
            disposal_proceeds_gbp=_f(r.disposal_proceeds_gbp),
            gain_or_loss_gbp=_f(r.gain_or_loss_gbp),
            disposal_uk_date=r.disposal_uk_date,
            tax_year=r.tax_year,
            disposal_tax_lot_id=r.disposal_tax_lot_id,
            acquisition_tax_lot_id=r.acquisition_tax_lot_id,
            matched_at=r.matched_at,
        )
        for r in rows
    ]


@router.get("/pools", response_model=list[S104PoolResponse])
def get_tax_pools(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> list[S104PoolResponse]:
    """Current s104 (section-104) pool state per instrument, largest cost first."""
    with snapshot_read(conn):
        rows = s104_pool_rows(conn)
    return [
        S104PoolResponse(
            instrument_id=r.instrument_id,
            symbol=r.symbol,
            pool_units=_f(r.pool_units),
            pool_cost_gbp=_f(r.pool_cost_gbp),
            pool_avg_cost_gbp=_f(r.pool_avg_cost_gbp),
            updated_at=r.updated_at,
        )
        for r in rows
    ]


@router.get("/tax-years", response_model=TaxYearsResponse)
def get_tax_years(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> TaxYearsResponse:
    """Tax years with data (newest first) + the current year, for the UI selector."""
    with snapshot_read(conn):
        available = available_tax_years(conn)
    return TaxYearsResponse(current=current_tax_year(), available=available)
