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

``GET /tax/view?tax_year=`` (#3616 slice 3d-4a, Design 6) reads the effective
matching run. The scheduled jobs (``tax_broker_evidence_refresh``,
``daily_tax_reconciliation``) publish the evidence and the matching run; this
endpoint only reads, and derives the annual view on each request without
storing it. Amounts are exact decimal strings in GBP; no exchange rate is
returned. The legacy endpoints above read tables with no writer and are
removed with the page switch (slice 3d-4a part C).
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Literal

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, PlainSerializer

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.db.snapshot import snapshot_read
from app.services.tax_assumptions import ASSUMPTIONS
from app.services.tax_ledger import (
    available_tax_years,
    current_tax_year,
    disposals_for_tax_year,
    s104_pool_rows,
    tax_year_summary,
    valid_tax_year,
)
from app.services.tax_matching import TaxRunUnavailable
from app.services.tax_view_reader import FundDisposalDetail, Identification, ShareDisposalDetail, resolve_and_read
from app.services.tax_year_view import Flag, Reading, supported_tax_years, year_view

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
    disposals_total: int
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
        disposals_total=s.disposals_total,
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


# ---------------------------------------------------------------------------
# GET /tax/view (#3616 slice 3d-4a, Design 6)
# ---------------------------------------------------------------------------

LABELS: tuple[str, ...] = ("demo", "account_only_hypothetical")

# Exact amounts as plain decimal strings ("0.00000000000000", never "0E-14").
DecimalStr = Annotated[Decimal, PlainSerializer(lambda d: format(d, "f"), return_type=str)]


class ReadingResponse(BaseModel):
    gains: DecimalStr
    losses: DecimalStr
    net_current: DecimalStr
    current_losses_carried_forward: DecimalStr
    aea: DecimalStr
    aea_used: DecimalStr
    aea_remaining: DecimalStr
    brought_forward_opening: DecimalStr
    brought_forward_used: DecimalStr
    brought_forward_remaining: DecimalStr
    losses_carried_forward: DecimalStr
    taxable: DecimalStr
    tax_basic: DecimalStr
    tax_higher: DecimalStr


class FlagRowResponse(BaseModel):
    ref: str
    detail: list[str]
    # None: the row can affect every year.
    years: list[str] | None


class FlagResponse(BaseModel):
    code: str
    omission: bool
    count: int
    rows: list[FlagRowResponse]


class TaxYearFiguresResponse(BaseModel):
    tax_year: str
    income_reading: ReadingResponse
    capital_reading: ReadingResponse | None
    offshore_income_gains: DecimalStr
    unresolved_gains: DecimalStr
    complete: bool
    flags: list[FlagResponse]
    figure_class: Literal["provisional"]
    reasons: list[str]
    assumption_ids: list[str]


class IdentificationResponse(BaseModel):
    seq: int
    rule: str
    acquisition_uk_date: date | None
    acquisition_lot_keys: list[str]
    units: DecimalStr
    cost_gbp: DecimalStr | None
    missing_lot_keys: list[str]
    reasons: list[str]


class TaxViewDisposalResponse(BaseModel):
    kind: Literal["share", "fund"]
    tax_security_id: int
    uk_date: date
    seq: int | None
    lot_key: str | None
    executed_at: datetime | None
    symbols: list[str]
    units: DecimalStr
    proceeds_gbp: DecimalStr | None
    cost_gbp: DecimalStr | None
    status: str
    missing_lot_keys: list[str]
    reasons: list[str]
    # Shares.
    gain_gbp: DecimalStr | None
    # Funds: NULL exactly where the matching run stores NULL (an unresolved split stays NULL).
    computed_gain_gbp: DecimalStr | None
    basic_gain_gbp: DecimalStr | None
    gain_character: str | None
    offshore_income_gain_gbp: DecimalStr | None
    chargeable_gain_gbp: DecimalStr | None
    member_of: Literal["both_readings", "capital_reading_only", "neither"]
    identifications: list[IdentificationResponse]


class FundBalanceResponse(BaseModel):
    lot_key: str
    units_left: DecimalStr
    cost_left_gbp: DecimalStr | None
    missing_lot_keys: list[str]
    reasons: list[str]


class HoldingResponse(BaseModel):
    tax_security_id: int
    symbols: list[str]
    holding_basis: str
    units: DecimalStr | None
    cost_gbp: DecimalStr | None
    missing_lot_keys: list[str]
    figure_class: Literal["provisional"]
    reasons: list[str]
    window_open_since: date | None
    fund_balances: list[FundBalanceResponse]


class WithheldResponse(BaseModel):
    tax_security_id: int
    symbols: list[str]
    reasons: list[str]


class TaxYearEntry(BaseModel):
    tax_year: str
    # None when no run is available: unknown, not absent.
    has_disposal: bool | None


class TaxViewResponse(BaseModel):
    status: Literal["available", "unavailable"]
    unavailable_reason: TaxRunUnavailable | None
    matching_run_id: int | None
    evidence_run_id: int | None
    matching_policy_id: str | None
    class_policy_id: str | None
    character_policy_id: str | None
    labels: list[str]
    current_year: str
    years: list[TaxYearEntry]
    year: TaxYearFiguresResponse | None
    disposals: list[TaxViewDisposalResponse]
    holdings: list[HoldingResponse]
    withheld: list[WithheldResponse]
    assumption_definitions: dict[str, str]


def _reading(r: Reading) -> ReadingResponse:
    return ReadingResponse(**{name: getattr(r, name) for name in ReadingResponse.model_fields})


def _flag(f: Flag) -> FlagResponse:
    return FlagResponse(
        code=f.code,
        omission=f.omission,
        count=f.count,
        rows=[
            FlagRowResponse(ref=r.ref, detail=list(r.detail), years=None if r.years is None else list(r.years))
            for r in f.rows
        ],
    )


def _identifications(ids: tuple[Identification, ...]) -> list[IdentificationResponse]:
    return [
        IdentificationResponse(
            seq=i.seq,
            rule=i.rule,
            acquisition_uk_date=i.acquisition_uk_date,
            acquisition_lot_keys=list(i.acquisition_lot_keys),
            units=i.units,
            cost_gbp=i.cost_gbp,
            missing_lot_keys=list(i.missing_lot_keys),
            reasons=list(i.reasons),
        )
        for i in ids
    ]


def _share_disposal(d: ShareDisposalDetail, symbols: list[str]) -> TaxViewDisposalResponse:
    r = d.row
    return TaxViewDisposalResponse(
        kind="share",
        tax_security_id=r.tax_security_id,
        uk_date=r.uk_date,
        seq=None,
        lot_key=None,
        executed_at=None,
        symbols=symbols,
        units=d.units,
        proceeds_gbp=d.proceeds_gbp,
        cost_gbp=d.cost_gbp,
        status=r.status,
        missing_lot_keys=list(r.missing_lot_keys),
        reasons=list(r.reasons),
        gain_gbp=r.gain_gbp,
        computed_gain_gbp=None,
        basic_gain_gbp=None,
        gain_character=None,
        offshore_income_gain_gbp=None,
        chargeable_gain_gbp=None,
        member_of="both_readings" if r.status == "computed" and r.gain_gbp else "neither",
        identifications=_identifications(d.identifications),
    )


def _fund_disposal(d: FundDisposalDetail, symbols: list[str]) -> TaxViewDisposalResponse:
    r = d.row
    member_of: Literal["both_readings", "capital_reading_only", "neither"] = "neither"
    if r.status == "computed" and r.gain_character == "capital_loss":
        member_of = "both_readings"
    elif r.status == "computed" and r.gain_character == "oig_or_capital_unresolved":
        member_of = "capital_reading_only"
    return TaxViewDisposalResponse(
        kind="fund",
        tax_security_id=r.tax_security_id,
        uk_date=r.uk_date,
        seq=r.seq,
        lot_key=r.lot_key,
        executed_at=d.executed_at,
        symbols=symbols,
        units=d.units,
        proceeds_gbp=d.proceeds_gbp,
        cost_gbp=d.cost_gbp,
        status=r.status,
        missing_lot_keys=list(r.missing_lot_keys),
        reasons=list(r.reasons),
        gain_gbp=None,
        computed_gain_gbp=d.computed_gain_gbp,
        basic_gain_gbp=r.basic_gain_gbp,
        gain_character=r.gain_character,
        offshore_income_gain_gbp=r.offshore_income_gain_gbp,
        chargeable_gain_gbp=r.chargeable_gain_gbp,
        member_of=member_of,
        identifications=_identifications(d.identifications),
    )


@router.get("/view", response_model=TaxViewResponse)
def get_tax_view(
    tax_year: str | None = Query(default=None),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> TaxViewResponse:
    """One tax year of the effective matching run: readings, income lines, flags, disposals and holdings."""
    ty = _resolve_tax_year(tax_year)
    current = current_tax_year()
    with snapshot_read(conn):
        resolution, data = resolve_and_read(conn)
    supported = supported_tax_years(current)
    if data is None:
        return TaxViewResponse(
            status="unavailable",
            unavailable_reason=resolution.reason,
            matching_run_id=None,
            evidence_run_id=None,
            matching_policy_id=None,
            class_policy_id=None,
            character_policy_id=None,
            labels=list(LABELS),
            current_year=current,
            years=[TaxYearEntry(tax_year=y, has_disposal=None) for y in supported],
            year=None,
            disposals=[],
            holdings=[],
            withheld=[],
            assumption_definitions={},
        )
    rows = data.rows
    with_disposal = {d.tax_year for d in rows.share_disposals} | {d.tax_year for d in rows.fund_disposals}
    view = year_view(rows, ty, current)
    sym = {sid: list(s) for sid, s in data.symbols.items()}
    disposals = [
        _share_disposal(d, sym.get(d.row.tax_security_id, [])) for d in data.share_details if d.row.tax_year == ty
    ]
    disposals += [
        _fund_disposal(d, sym.get(d.row.tax_security_id, [])) for d in data.fund_details if d.row.tax_year == ty
    ]
    disposals.sort(key=lambda d: (d.uk_date, d.tax_security_id, d.seq or 0))
    return TaxViewResponse(
        status="available",
        unavailable_reason=None,
        matching_run_id=data.matching_run_id,
        evidence_run_id=data.evidence_run_id,
        matching_policy_id=data.matching_policy_id,
        class_policy_id=data.class_policy_id,
        character_policy_id=data.character_policy_id,
        labels=list(LABELS),
        current_year=current,
        years=[TaxYearEntry(tax_year=y, has_disposal=y in with_disposal) for y in supported],
        year=TaxYearFiguresResponse(
            tax_year=view.tax_year,
            income_reading=_reading(view.income_reading),
            capital_reading=None if view.capital_reading is None else _reading(view.capital_reading),
            offshore_income_gains=view.offshore_income_gains,
            unresolved_gains=view.unresolved_gains,
            complete=view.complete,
            flags=[_flag(f) for f in view.flags],
            figure_class=view.figure_class,
            reasons=list(view.reasons),
            assumption_ids=list(view.assumption_ids),
        ),
        disposals=disposals,
        holdings=[
            HoldingResponse(
                tax_security_id=h.tax_security_id,
                symbols=list(h.symbols),
                holding_basis=h.holding_basis,
                units=h.units,
                cost_gbp=h.cost_gbp,
                missing_lot_keys=list(h.missing_lot_keys),
                figure_class="provisional",
                reasons=list(h.reasons),
                window_open_since=h.window_open_since,
                fund_balances=[
                    FundBalanceResponse(
                        lot_key=b.lot_key,
                        units_left=b.units_left,
                        cost_left_gbp=b.cost_left_gbp,
                        missing_lot_keys=list(b.missing_lot_keys),
                        reasons=list(b.reasons),
                    )
                    for b in h.fund_balances
                ],
            )
            for h in data.holdings
        ],
        withheld=[
            WithheldResponse(tax_security_id=w.tax_security_id, symbols=list(w.symbols), reasons=list(w.reasons))
            for w in rows.withheld
        ],
        assumption_definitions={a: ASSUMPTIONS[a] for a in view.assumption_ids},
    )
