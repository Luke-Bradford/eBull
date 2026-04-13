"""Portfolio API endpoint.

Reads from:
  - positions   (1:1 per instrument — current holdings)
  - instruments  (symbol, company_name for display)
  - quotes       (1:1 current snapshot — for mark-to-market valuation)
  - cash_ledger  (append-only — SUM for cash balance)

No writes. No schema changes.

Mark-to-market semantics:
  market_value = current_units * quote.last   when a quote with a last price exists
  market_value = cost_basis                   when no quote exists (fallback)
  unrealized_pnl = market_value - cost_basis  when a quote exists
  unrealized_pnl = 0                          when falling back to cost_basis (no price signal)

Zero-unit positions: excluded via WHERE filter. A position with current_units = 0
is fully liquidated and should not appear in the portfolio view.

AUM = SUM(market_value across all positions) + cash_balance + mirror_equity.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.api._helpers import parse_optional_float
from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.domain.positions import PositionSource
from app.services.fx import FxRateNotFound, convert, load_live_fx_rates_with_metadata
from app.services.portfolio import _load_mirror_equity
from app.services.runtime_config import get_runtime_config

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/portfolio",
    tags=["portfolio"],
    dependencies=[Depends(require_session_or_service_token)],
)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class PositionItem(BaseModel):
    instrument_id: int
    symbol: str
    company_name: str
    open_date: date | None
    avg_cost: float | None
    current_price: float | None
    current_units: float
    cost_basis: float
    market_value: float
    unrealized_pnl: float
    valuation_source: str  # "quote", "daily_close", or "cost_basis"
    source: PositionSource
    updated_at: datetime


class PortfolioResponse(BaseModel):
    positions: list[PositionItem]
    position_count: int
    total_aum: float
    cash_balance: float | None
    mirror_equity: float = 0.0
    display_currency: str = "GBP"
    fx_rates_used: dict[str, dict[str, object]] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _convert_value(
    value: float,
    from_ccy: str,
    to_ccy: str,
    rates: dict[tuple[str, str], Decimal],
) -> float:
    """Convert a float value between currencies, returning the original on FxRateNotFound."""
    if from_ccy == to_ccy:
        return value
    try:
        return float(convert(Decimal(str(value)), from_ccy, to_ccy, rates))
    except FxRateNotFound:
        logger.warning("FX rate %s→%s not found; skipping conversion", from_ccy, to_ccy)
        return value


def _parse_position(
    row: dict[str, object],
    display_currency: str,
    rates: dict[tuple[str, str], Decimal],
) -> PositionItem:
    cost_basis = float(row["cost_basis"])  # type: ignore[arg-type]
    current_units = float(row["current_units"])  # type: ignore[arg-type]
    native_currency: str = str(row.get("currency") or "USD")  # fallback for un-enriched

    # Mark-to-market hierarchy: quote.last → price_daily.close → cost_basis.
    # valuation_source tells the dashboard which tier produced the value.
    last_price = parse_optional_float(row, "last")
    daily_close = parse_optional_float(row, "daily_close")

    if last_price is not None:
        current_price: float | None = last_price
        market_value = current_units * last_price
        unrealized_pnl = market_value - cost_basis
        valuation_source = "quote"
    elif daily_close is not None:
        current_price = daily_close
        market_value = current_units * daily_close
        unrealized_pnl = market_value - cost_basis
        valuation_source = "daily_close"
    else:
        current_price = None
        market_value = cost_basis
        unrealized_pnl = 0.0
        valuation_source = "cost_basis"

    # Convert monetary values to display currency.
    if native_currency != display_currency:
        try:
            market_value = float(convert(Decimal(str(market_value)), native_currency, display_currency, rates))
            cost_basis = float(convert(Decimal(str(cost_basis)), native_currency, display_currency, rates))
            unrealized_pnl = float(convert(Decimal(str(unrealized_pnl)), native_currency, display_currency, rates))
        except FxRateNotFound:
            logger.warning(
                "FX rate %s→%s not found; skipping conversion for position",
                native_currency,
                display_currency,
            )

    avg_cost = parse_optional_float(row, "avg_cost")
    if native_currency != display_currency:
        if avg_cost is not None:
            try:
                avg_cost = float(convert(Decimal(str(avg_cost)), native_currency, display_currency, rates))
            except FxRateNotFound:
                pass  # warning already logged above
        if current_price is not None:
            try:
                current_price = float(convert(Decimal(str(current_price)), native_currency, display_currency, rates))
            except FxRateNotFound:
                pass

    return PositionItem(
        instrument_id=row["instrument_id"],  # type: ignore[arg-type]
        symbol=row["symbol"],  # type: ignore[arg-type]
        company_name=row["company_name"],  # type: ignore[arg-type]
        open_date=row["open_date"],  # type: ignore[arg-type]
        avg_cost=avg_cost,
        current_price=current_price,
        current_units=current_units,
        cost_basis=cost_basis,
        market_value=market_value,
        unrealized_pnl=unrealized_pnl,
        valuation_source=valuation_source,
        source=row["source"],  # type: ignore[arg-type]
        updated_at=row["updated_at"],  # type: ignore[arg-type]
    )


def _build_fx_rates_used(
    pos_rows: list[dict[str, Any]],
    has_cash: bool,
    mirror_equity: float,
    display_currency: str,
    rates_meta: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, dict[str, object]]:
    """Build the fx_rates_used metadata from the source currencies actually consumed.

    Keys are source currencies (e.g. "USD"). Values include rate and quoted_at.
    Only includes currencies that differ from display_currency.
    """
    source_currencies: set[str] = set()

    for row in pos_rows:
        native = str(row.get("currency") or "USD")
        if native != display_currency:
            source_currencies.add(native)

    # Cash and mirror_equity are always USD for eToro.
    # Include USD when we have cash OR non-trivial mirror equity.
    has_usd_component = has_cash or abs(mirror_equity) > 1e-9
    if has_usd_component and "USD" != display_currency:
        source_currencies.add("USD")

    result: dict[str, dict[str, object]] = {}
    for ccy in sorted(source_currencies):
        key = (ccy, display_currency)
        inv_key = (display_currency, ccy)
        if key in rates_meta:
            meta = rates_meta[key]
            quoted_at = meta["quoted_at"]
            result[ccy] = {
                "rate": float(meta["rate"]),
                "quoted_at": quoted_at.isoformat() if hasattr(quoted_at, "isoformat") else str(quoted_at),
            }
        elif inv_key in rates_meta:
            meta = rates_meta[inv_key]
            quoted_at = meta["quoted_at"]
            result[ccy] = {
                "rate": float(Decimal("1") / meta["rate"]),
                "quoted_at": quoted_at.isoformat() if hasattr(quoted_at, "isoformat") else str(quoted_at),
            }
        # If no rate found, skip — conversion was skipped for those positions too.

    return result


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.get("", response_model=PortfolioResponse)
def get_portfolio(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> PortfolioResponse:
    """Current portfolio: positions with mark-to-market valuation, cash balance, and AUM.

    Ordering: market_value DESC, instrument_id ASC (largest positions first,
    deterministic tiebreak).

    Mark-to-market uses the latest quote ``last`` price when available.
    When no quote exists, market_value falls back to cost_basis and
    unrealized_pnl is reported as 0 (no price signal).

    Zero-unit positions are excluded (fully liquidated).

    AUM = sum of all position market_values + cash_balance + mirror_equity.
    If cash_balance is unknown (empty cash_ledger), AUM uses positions only
    and cash_balance is null. mirror_equity is always a float (default 0.0).
    """
    # -- Load display currency and FX rates ----------------------------------
    config = get_runtime_config(conn)
    display_currency = config.display_currency
    rates_meta = load_live_fx_rates_with_metadata(conn)
    rates: dict[tuple[str, str], Decimal] = {k: v["rate"] for k, v in rates_meta.items()}

    # -- Positions query ---------------------------------------------------
    # quotes is 1:1 keyed by instrument_id (PRIMARY KEY) — LEFT JOIN is fan-out-safe.
    # Zero-unit positions are excluded: fully liquidated positions should not
    # appear in the portfolio view or inflate AUM.
    # i.currency: the instrument's native currency for FX conversion.
    # LEFT JOIN quotes (1:1 by PK) and latest price_daily (DISTINCT ON
    # avoids fan-out — one row per instrument, most recent date).
    positions_sql = """
        SELECT p.instrument_id, i.symbol, i.company_name, i.currency,
               p.open_date, p.avg_cost, p.current_units, p.cost_basis,
               p.source, p.updated_at,
               q.last,
               pd.close AS daily_close
        FROM positions p
        JOIN instruments i USING (instrument_id)
        LEFT JOIN quotes q USING (instrument_id)
        LEFT JOIN LATERAL (
            SELECT close
            FROM price_daily
            WHERE instrument_id = p.instrument_id
              AND close IS NOT NULL
            ORDER BY price_date DESC
            LIMIT 1
        ) pd ON true
        WHERE p.current_units > 0
        ORDER BY p.cost_basis DESC, p.instrument_id ASC
    """

    # -- Cash query --------------------------------------------------------
    # SUM on empty table returns NULL (one row, NULL value) — not zero rows.
    cash_sql = "SELECT SUM(amount) AS cash_balance FROM cash_ledger"

    # Use separate cursors for logically independent queries to avoid
    # relying on psycopg v3 cursor reuse semantics after fetchall().
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(positions_sql)
        pos_rows = cur.fetchall()

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(cash_sql)
        cash_row = cur.fetchone()
        # SUM() always returns exactly one row; the value is None when the table is empty.
        raw_cash = cash_row["cash_balance"] if cash_row else None  # type: ignore[index]

    positions = [_parse_position(r, display_currency, rates) for r in pos_rows]
    cash_balance = float(raw_cash) if raw_cash is not None else None  # type: ignore[arg-type]

    # Convert cash_balance — always USD for eToro.
    if cash_balance is not None:
        cash_balance = _convert_value(cash_balance, "USD", display_currency, rates)

    # AUM: sum of position market_values + cash (if known) + mirror_equity.
    total_market = sum(p.market_value for p in positions)
    raw_mirror_equity = _load_mirror_equity(conn)

    # Convert mirror_equity — always USD for eToro.
    mirror_equity = _convert_value(raw_mirror_equity, "USD", display_currency, rates)

    total_aum = total_market + (cash_balance if cash_balance is not None else 0.0) + mirror_equity

    # Re-sort by market_value DESC (computed value, not a DB column) with stable tiebreak.
    positions.sort(key=lambda p: (-p.market_value, p.instrument_id))

    # Build fx_rates_used from source currencies actually consumed.
    fx_rates_used = _build_fx_rates_used(
        pos_rows, raw_cash is not None, raw_mirror_equity, display_currency, rates_meta
    )

    return PortfolioResponse(
        positions=positions,
        position_count=len(positions),
        total_aum=total_aum,
        cash_balance=cash_balance,
        mirror_equity=mirror_equity,
        display_currency=display_currency,
        fx_rates_used=fx_rates_used,
    )
