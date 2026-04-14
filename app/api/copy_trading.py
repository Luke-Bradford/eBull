"""Copy-trading browsing endpoint (Track 1.5 — issue #188).

Read-only surface over the copy_traders, copy_mirrors, and
copy_mirror_positions tables delivered by Track 1a (#183).

Returns per-trader cards with mirror-level aggregates and nested
position breakdowns, all converted to the operator's display currency.

No writes. No schema changes.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.api._helpers import parse_optional_float
from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.services.fx import FxRateNotFound, convert, load_live_fx_rates_with_metadata
from app.services.runtime_config import get_runtime_config

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/portfolio/copy-trading",
    tags=["portfolio"],
    dependencies=[Depends(require_session_or_service_token)],
)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class MirrorPositionItem(BaseModel):
    position_id: int
    instrument_id: int
    symbol: str | None
    company_name: str | None
    is_buy: bool
    units: float
    amount: float
    open_rate: float
    open_conversion_rate: float
    open_date_time: datetime
    current_price: float | None
    market_value: float
    unrealized_pnl: float


class MirrorSummary(BaseModel):
    mirror_id: int
    active: bool
    initial_investment: float
    deposit_summary: float
    withdrawal_summary: float
    available_amount: float
    closed_positions_net_profit: float
    mirror_equity: float
    position_count: int
    positions: list[MirrorPositionItem]
    started_copy_date: datetime
    closed_at: datetime | None


class CopyTraderSummary(BaseModel):
    parent_cid: int
    parent_username: str
    mirrors: list[MirrorSummary]
    total_equity: float


class CopyTradingResponse(BaseModel):
    traders: list[CopyTraderSummary]
    total_mirror_equity: float
    display_currency: str


class MirrorDetailResponse(BaseModel):
    parent_username: str
    mirror: MirrorSummary
    display_currency: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _compute_position_mtm(
    row: dict[str, Any],
    display_currency: str,
    rates: dict[tuple[str, str], Decimal],
) -> MirrorPositionItem:
    """Build a MirrorPositionItem with mark-to-market from a joined row.

    Price hierarchy: quote.last → price_daily.close → open_rate (fallback).
    All monetary values are in USD natively (eToro copy-trading positions).
    Convert to display_currency at the end.
    """
    units = float(row["units"])
    amount = float(row["amount"])
    open_rate = float(row["open_rate"])
    open_conv = float(row["open_conversion_rate"])
    is_buy = row["is_buy"]
    direction = 1.0 if is_buy else -1.0

    # Price hierarchy for the instrument's native currency price
    quote_last = parse_optional_float(row, "quote_last")
    daily_close = parse_optional_float(row, "daily_close")

    if quote_last is not None:
        native_price = quote_last
    elif daily_close is not None:
        native_price = daily_close
    else:
        native_price = open_rate  # fallback — P&L will be zero

    # P&L in USD: direction * units * (current - entry) * fx_at_open
    pnl_usd = direction * units * (native_price - open_rate) * open_conv
    market_value_usd = amount + pnl_usd

    # current_price in display terms: native_price converted, but only
    # if we have a real price signal (not the open_rate fallback)
    if quote_last is not None or daily_close is not None:
        current_price_usd: float | None = native_price * open_conv
    else:
        current_price_usd = None

    # Convert USD → display_currency
    if "USD" != display_currency:
        try:
            market_value = float(convert(Decimal(str(market_value_usd)), "USD", display_currency, rates))
            pnl = float(convert(Decimal(str(pnl_usd)), "USD", display_currency, rates))
            if current_price_usd is not None:
                current_price: float | None = float(
                    convert(Decimal(str(current_price_usd)), "USD", display_currency, rates)
                )
            else:
                current_price = None
            amount_display = float(convert(Decimal(str(amount)), "USD", display_currency, rates))
        except FxRateNotFound:
            logger.warning("FX rate USD→%s not found; skipping conversion", display_currency)
            market_value = market_value_usd
            pnl = pnl_usd
            current_price = current_price_usd
            amount_display = amount
    else:
        market_value = market_value_usd
        pnl = pnl_usd
        current_price = current_price_usd
        amount_display = amount

    return MirrorPositionItem(
        position_id=row["position_id"],
        instrument_id=row["instrument_id"],
        symbol=row.get("symbol"),
        company_name=row.get("company_name"),
        is_buy=is_buy,
        units=units,
        amount=amount_display,
        open_rate=open_rate,
        open_conversion_rate=open_conv,
        open_date_time=row["open_date_time"],
        current_price=current_price,
        market_value=market_value,
        unrealized_pnl=pnl,
    )


def _convert_usd(
    value: float,
    display_currency: str,
    rates: dict[tuple[str, str], Decimal],
) -> float:
    """Convert a USD value to display_currency, returning original on failure."""
    if display_currency == "USD":
        return value
    try:
        return float(convert(Decimal(str(value)), "USD", display_currency, rates))
    except FxRateNotFound:
        logger.warning("FX rate USD→%s not found; returning unconverted value", display_currency)
        return value


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.get("", response_model=CopyTradingResponse)
def get_copy_trading(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> CopyTradingResponse:
    """Per-trader copy-trading overview with nested position breakdowns.

    Returns all copy traders (active mirrors first, then closed), with
    per-mirror equity computed from the same three-tier pricing hierarchy
    as the main portfolio endpoint.

    All monetary values are converted to the operator's display currency.
    """
    config = get_runtime_config(conn)
    display_currency = config.display_currency
    rates_meta = load_live_fx_rates_with_metadata(conn)
    rates: dict[tuple[str, str], Decimal] = {k: v["rate"] for k, v in rates_meta.items()}

    # -- Query 1: traders + mirrors ----------------------------------------
    mirrors_sql = """
        SELECT ct.parent_cid, ct.parent_username,
               cm.mirror_id, cm.active,
               cm.initial_investment, cm.deposit_summary,
               cm.withdrawal_summary, cm.available_amount,
               cm.closed_positions_net_profit,
               cm.started_copy_date, cm.closed_at
        FROM copy_traders ct
        JOIN copy_mirrors cm USING (parent_cid)
        ORDER BY cm.active DESC, ct.parent_username, cm.mirror_id
    """

    # -- Query 2: all positions with instrument + pricing info -------------
    # One query across all mirrors avoids N+1.  Instrument join is LEFT
    # because mirrors may hold instruments outside the eBull universe.
    positions_sql = """
        SELECT cmp.mirror_id, cmp.position_id, cmp.instrument_id,
               i.symbol, i.company_name,
               cmp.is_buy, cmp.units, cmp.amount,
               cmp.open_rate, cmp.open_conversion_rate,
               cmp.open_date_time,
               q.last AS quote_last,
               pd.close AS daily_close
        FROM copy_mirror_positions cmp
        LEFT JOIN instruments i ON i.instrument_id = cmp.instrument_id
        LEFT JOIN quotes q ON q.instrument_id = cmp.instrument_id
        LEFT JOIN LATERAL (
            SELECT close
            FROM price_daily
            WHERE instrument_id = cmp.instrument_id
              AND close IS NOT NULL
            ORDER BY price_date DESC
            LIMIT 1
        ) pd ON TRUE
        ORDER BY cmp.mirror_id, cmp.amount DESC
    """

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(mirrors_sql)
        mirror_rows = cur.fetchall()

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(positions_sql)
        position_rows = cur.fetchall()

    # -- Group positions by mirror_id --------------------------------------
    positions_by_mirror: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in position_rows:
        positions_by_mirror[row["mirror_id"]].append(row)

    # -- Assemble per-trader summaries -------------------------------------
    traders_map: dict[int, CopyTraderSummary] = {}

    for mr in mirror_rows:
        parent_cid: int = mr["parent_cid"]
        mirror_id: int = mr["mirror_id"]

        # Build position items for this mirror
        raw_positions = positions_by_mirror.get(mirror_id, [])
        position_items = [_compute_position_mtm(p, display_currency, rates) for p in raw_positions]

        # Per-mirror equity = available_amount + sum(position market values)
        available_usd = float(mr["available_amount"])
        positions_mv_display = sum(p.market_value for p in position_items)
        available_display = _convert_usd(available_usd, display_currency, rates)
        mirror_equity = available_display + positions_mv_display

        mirror_summary = MirrorSummary(
            mirror_id=mirror_id,
            active=mr["active"],
            initial_investment=_convert_usd(float(mr["initial_investment"]), display_currency, rates),
            deposit_summary=_convert_usd(float(mr["deposit_summary"]), display_currency, rates),
            withdrawal_summary=_convert_usd(float(mr["withdrawal_summary"]), display_currency, rates),
            available_amount=available_display,
            closed_positions_net_profit=_convert_usd(float(mr["closed_positions_net_profit"]), display_currency, rates),
            mirror_equity=mirror_equity,
            position_count=len(position_items),
            positions=position_items,
            started_copy_date=mr["started_copy_date"],
            closed_at=mr["closed_at"],
        )

        if parent_cid not in traders_map:
            traders_map[parent_cid] = CopyTraderSummary(
                parent_cid=parent_cid,
                parent_username=mr["parent_username"],
                mirrors=[],
                total_equity=0.0,
            )
        traders_map[parent_cid].mirrors.append(mirror_summary)

    # Compute per-trader total equity
    traders = list(traders_map.values())
    for trader in traders:
        trader.total_equity = sum(m.mirror_equity for m in trader.mirrors)

    total_mirror_equity = sum(t.total_equity for t in traders)

    return CopyTradingResponse(
        traders=traders,
        total_mirror_equity=total_mirror_equity,
        display_currency=display_currency,
    )


@router.get("/{mirror_id}", response_model=MirrorDetailResponse)
def get_mirror_detail(
    mirror_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> MirrorDetailResponse:
    """Single-mirror detail: stats and component positions.

    Returns the mirror's metadata, financial summary, and all
    component positions with MTM valuation — the drill-down from
    a mirror row in the dashboard positions table.
    """
    config = get_runtime_config(conn)
    display_currency = config.display_currency
    rates_meta = load_live_fx_rates_with_metadata(conn)
    rates: dict[tuple[str, str], Decimal] = {k: v["rate"] for k, v in rates_meta.items()}

    # -- Load the single mirror + trader metadata -------------------------
    mirror_sql = """
        SELECT ct.parent_username,
               cm.mirror_id, cm.active,
               cm.initial_investment, cm.deposit_summary,
               cm.withdrawal_summary, cm.available_amount,
               cm.closed_positions_net_profit,
               cm.started_copy_date, cm.closed_at
        FROM copy_mirrors cm
        JOIN copy_traders ct USING (parent_cid)
        WHERE cm.mirror_id = %(mirror_id)s
    """

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(mirror_sql, {"mirror_id": mirror_id})
        mr = cur.fetchone()

    if mr is None:
        raise HTTPException(status_code=404, detail=f"Mirror {mirror_id} not found")

    # -- Load positions for this mirror -----------------------------------
    positions_sql = """
        SELECT cmp.mirror_id, cmp.position_id, cmp.instrument_id,
               i.symbol, i.company_name,
               cmp.is_buy, cmp.units, cmp.amount,
               cmp.open_rate, cmp.open_conversion_rate,
               cmp.open_date_time,
               q.last AS quote_last,
               pd.close AS daily_close
        FROM copy_mirror_positions cmp
        LEFT JOIN instruments i ON i.instrument_id = cmp.instrument_id
        LEFT JOIN quotes q ON q.instrument_id = cmp.instrument_id
        LEFT JOIN LATERAL (
            SELECT close
            FROM price_daily
            WHERE instrument_id = cmp.instrument_id
              AND close IS NOT NULL
            ORDER BY price_date DESC
            LIMIT 1
        ) pd ON TRUE
        WHERE cmp.mirror_id = %(mirror_id)s
        ORDER BY cmp.amount DESC
    """

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(positions_sql, {"mirror_id": mirror_id})
        position_rows = cur.fetchall()

    position_items = [_compute_position_mtm(p, display_currency, rates) for p in position_rows]

    # Per-mirror equity = available_amount + sum(position market values)
    available_usd = float(mr["available_amount"])
    positions_mv_display = sum(p.market_value for p in position_items)
    available_display = _convert_usd(available_usd, display_currency, rates)
    mirror_equity = available_display + positions_mv_display

    mirror_summary = MirrorSummary(
        mirror_id=mr["mirror_id"],
        active=mr["active"],
        initial_investment=_convert_usd(float(mr["initial_investment"]), display_currency, rates),
        deposit_summary=_convert_usd(float(mr["deposit_summary"]), display_currency, rates),
        withdrawal_summary=_convert_usd(float(mr["withdrawal_summary"]), display_currency, rates),
        available_amount=available_display,
        closed_positions_net_profit=_convert_usd(float(mr["closed_positions_net_profit"]), display_currency, rates),
        mirror_equity=mirror_equity,
        position_count=len(position_items),
        positions=position_items,
        started_copy_date=mr["started_copy_date"],
        closed_at=mr["closed_at"],
    )

    return MirrorDetailResponse(
        parent_username=mr["parent_username"],
        mirror=mirror_summary,
        display_currency=display_currency,
    )
