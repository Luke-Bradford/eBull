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

AUM = SUM(market_value across all positions) + cash_balance.
"""

from __future__ import annotations

from datetime import date, datetime

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.api._helpers import parse_optional_float
from app.db import get_conn

router = APIRouter(prefix="/portfolio", tags=["portfolio"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class PositionItem(BaseModel):
    instrument_id: int
    symbol: str
    company_name: str
    open_date: date | None
    avg_cost: float | None
    current_units: float
    cost_basis: float
    market_value: float
    unrealized_pnl: float
    updated_at: datetime


class PortfolioResponse(BaseModel):
    positions: list[PositionItem]
    position_count: int
    total_aum: float
    cash_balance: float | None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_position(row: dict[str, object]) -> PositionItem:
    cost_basis = float(row["cost_basis"])  # type: ignore[arg-type]
    current_units = float(row["current_units"])  # type: ignore[arg-type]

    # Mark-to-market: use quote last price when available, else fall back to cost_basis.
    last_price = parse_optional_float(row, "last")
    if last_price is not None:
        # market_value (GBP) = units (count) * last price (GBP/unit)
        market_value = current_units * last_price
        unrealized_pnl = market_value - cost_basis
    else:
        # No quote — fall back to cost_basis; no P&L signal.
        market_value = cost_basis
        unrealized_pnl = 0.0

    return PositionItem(
        instrument_id=row["instrument_id"],  # type: ignore[arg-type]
        symbol=row["symbol"],  # type: ignore[arg-type]
        company_name=row["company_name"],  # type: ignore[arg-type]
        open_date=row["open_date"],  # type: ignore[arg-type]
        avg_cost=parse_optional_float(row, "avg_cost"),
        current_units=current_units,
        cost_basis=cost_basis,
        market_value=market_value,
        unrealized_pnl=unrealized_pnl,
        updated_at=row["updated_at"],  # type: ignore[arg-type]
    )


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

    AUM = sum of all position market_values + cash_balance.
    If cash_balance is unknown (empty cash_ledger), AUM uses positions only
    and cash_balance is null.
    """
    # -- Positions query ---------------------------------------------------
    # quotes is 1:1 keyed by instrument_id (PRIMARY KEY) — LEFT JOIN is fan-out-safe.
    # Zero-unit positions are excluded: fully liquidated positions should not
    # appear in the portfolio view or inflate AUM.
    positions_sql = """
        SELECT p.instrument_id, i.symbol, i.company_name,
               p.open_date, p.avg_cost, p.current_units, p.cost_basis,
               p.updated_at,
               q.last
        FROM positions p
        JOIN instruments i USING (instrument_id)
        LEFT JOIN quotes q USING (instrument_id)
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

    positions = [_parse_position(r) for r in pos_rows]
    cash_balance = float(raw_cash) if raw_cash is not None else None  # type: ignore[arg-type]

    # AUM: sum of position market_values + cash (if known).
    total_market = sum(p.market_value for p in positions)
    total_aum = total_market + (cash_balance if cash_balance is not None else 0.0)

    # Re-sort by market_value DESC (computed value, not a DB column) with stable tiebreak.
    positions.sort(key=lambda p: (-p.market_value, p.instrument_id))

    return PortfolioResponse(
        positions=positions,
        position_count=len(positions),
        total_aum=total_aum,
        cash_balance=cash_balance,
    )
