"""Shared portfolio valuation — the single source of the AUM figure.

`compute_portfolio_valuation` is consumed by BOTH the dashboard
endpoint (`app/api/portfolio.py::get_portfolio`) and the report
builders (`app/services/reporting.py`), so the report cover and the
dashboard headline cannot drift — they are one code path (#1596,
spec §3.3 of docs/proposals/ui/2026-06-12-report-ia.md).

Valuation basis (unchanged from the pre-#1596 endpoint):

    total_aum = Σ position market_value + cash_balance + mirror_equity

Mark-to-market hierarchy per position: live quote (``last`` > 0 →
bid/ask mid) → latest positive ``price_daily.close`` → cost basis
(#1428: a non-positive mark is not a valid price). All monetary
values convert to the operator's display currency; a missing FX rate
leaves the native value in place (same degrade as the dashboard).

``resolve_quote_price`` / ``parse_optional_float`` moved here from
``app/api/_helpers`` (which re-exports them) so the service layer
does not import from the API layer.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows

from app.domain.positions import PositionSource
from app.services.fx import FxRateNotFound, convert, load_live_fx_rates_with_metadata
from app.services.portfolio import MirrorBreakdown, load_mirror_breakdowns
from app.services.runtime_config import get_runtime_config

logger = logging.getLogger(__name__)


def parse_optional_float(row: dict[str, object], key: str) -> float | None:
    """Safely cast a nullable numeric DB column to float."""
    val = row.get(key)
    if val is None:
        return None
    return float(val)  # type: ignore[arg-type]


def resolve_quote_price(
    last: float | None,
    bid: float | None,
    ask: float | None,
) -> float | None:
    """Return a usable live-quote price, or ``None`` if none is available.

    Rule: a usable mark is the trade ``last`` when strictly positive; else
    the bid/ask mid when the book is two-sided and both sides are positive.

    A non-positive ``last`` is treated as missing. eToro persists
    ``quotes.last = 0.00`` for instruments not freshly traded (bid/ask
    present, no recent trade). Using 0 as the mark values a position at 0 →
    fake −100% P&L (#1428). Callers supply their own downstream fallback
    (daily_close → cost basis / open_rate) when this returns ``None``.
    """
    if last is not None and last > 0:
        return last
    if bid is not None and bid > 0 and ask is not None and ask > 0:
        return (bid + ask) / 2.0
    return None


@dataclass(frozen=True)
class HoldingValuation:
    """One open position, marked and converted to display currency."""

    instrument_id: int
    symbol: str
    company_name: str
    sector: str | None
    native_currency: str
    open_date: date | None
    source: PositionSource  # positions.source is NOT NULL + CHECK-constrained
    updated_at: datetime  # positions.updated_at is NOT NULL
    avg_cost: float | None
    current_units: float
    cost_basis: float
    current_price: float | None
    market_value: float
    unrealized_pnl: float
    valuation_source: str  # "quote" | "daily_close" | "cost_basis"


@dataclass(frozen=True)
class PortfolioValuation:
    """Aggregate valuation + the per-holding detail it was built from."""

    display_currency: str
    holdings: tuple[HoldingValuation, ...]
    total_market: float
    cash_balance: float | None  # None = empty cash_ledger (unknown)
    mirror_equity: float
    total_aum: float
    # Raw position rows (dict_row) in the same order as `holdings`, for
    # callers that need columns beyond the valuation (the dashboard's
    # broker-trade price lookup + fx_rates_used derivation).
    raw_rows: tuple[dict[str, Any], ...]
    # FX metadata loaded once; shared so callers don't re-query.
    rates: dict[tuple[str, str], Decimal]
    rates_meta: dict[tuple[str, str], dict[str, Any]]
    # Mirror breakdowns loaded once (dashboard renders per-mirror rows).
    mirror_breakdowns: tuple[MirrorBreakdown, ...]


_POSITIONS_SQL = """
    SELECT p.instrument_id, i.symbol, i.company_name, i.currency, i.sector,
           p.open_date, p.avg_cost, p.current_units, p.cost_basis,
           p.source, p.updated_at,
           q.last, q.bid, q.ask,
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

# SUM on empty table returns NULL (one row, NULL value) — not zero rows.
_CASH_SQL = "SELECT SUM(amount) AS cash_balance FROM cash_ledger"


def _convert_value(
    value: float,
    from_ccy: str,
    to_ccy: str,
    rates: dict[tuple[str, str], Decimal],
) -> float:
    """Float convenience wrapper over fx.convert; missing rate → unchanged."""
    if from_ccy == to_ccy:
        return value
    try:
        return float(convert(Decimal(str(value)), from_ccy, to_ccy, rates))
    except FxRateNotFound:
        logger.warning("FX rate %s→%s not found; value left in native currency", from_ccy, to_ccy)
        return value


def _holding_from_row(
    row: dict[str, Any],
    display_currency: str,
    rates: dict[tuple[str, str], Decimal],
) -> HoldingValuation:
    cost_basis = float(row["cost_basis"])
    current_units = float(row["current_units"])
    native_currency = str(row.get("currency") or "USD")

    quote_price = resolve_quote_price(
        parse_optional_float(row, "last"),
        parse_optional_float(row, "bid"),
        parse_optional_float(row, "ask"),
    )
    daily_close = parse_optional_float(row, "daily_close")

    if quote_price is not None:
        current_price: float | None = quote_price
        market_value = current_units * quote_price
        unrealized_pnl = market_value - cost_basis
        valuation_source = "quote"
    elif daily_close is not None and daily_close > 0:
        current_price = daily_close
        market_value = current_units * daily_close
        unrealized_pnl = market_value - cost_basis
        valuation_source = "daily_close"
    else:
        current_price = None
        market_value = cost_basis
        unrealized_pnl = 0.0
        valuation_source = "cost_basis"

    # Convert all monetary values to display currency in a single block
    # so they either all convert or all stay in native currency.
    avg_cost = parse_optional_float(row, "avg_cost")
    if native_currency != display_currency:
        try:
            market_value = float(convert(Decimal(str(market_value)), native_currency, display_currency, rates))
            cost_basis = float(convert(Decimal(str(cost_basis)), native_currency, display_currency, rates))
            unrealized_pnl = float(convert(Decimal(str(unrealized_pnl)), native_currency, display_currency, rates))
            if current_price is not None:
                current_price = float(convert(Decimal(str(current_price)), native_currency, display_currency, rates))
            if avg_cost is not None:
                avg_cost = float(convert(Decimal(str(avg_cost)), native_currency, display_currency, rates))
        except FxRateNotFound:
            logger.warning(
                "FX rate %s→%s not found; skipping conversion for position",
                native_currency,
                display_currency,
            )

    return HoldingValuation(
        instrument_id=row["instrument_id"],
        symbol=row["symbol"],
        company_name=row["company_name"],
        sector=row.get("sector"),
        native_currency=native_currency,
        open_date=row["open_date"],
        source=row["source"],
        updated_at=row["updated_at"],
        avg_cost=avg_cost,
        current_units=current_units,
        cost_basis=cost_basis,
        current_price=current_price,
        market_value=market_value,
        unrealized_pnl=unrealized_pnl,
        valuation_source=valuation_source,
    )


def compute_portfolio_valuation(conn: psycopg.Connection[Any]) -> PortfolioValuation:
    """Mark-to-market valuation of the whole account, display currency.

    One snapshot of: open positions (marked per the #1428 hierarchy),
    cash (signed sum of `cash_ledger`), and mirror equity. The
    dashboard endpoint and the report builders both call this — see
    module docstring.
    """
    config = get_runtime_config(conn)
    display_currency = config.display_currency
    rates_meta = load_live_fx_rates_with_metadata(conn)
    rates: dict[tuple[str, str], Decimal] = {k: v["rate"] for k, v in rates_meta.items()}

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_POSITIONS_SQL)
        pos_rows = cur.fetchall()

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_CASH_SQL)
        cash_row = cur.fetchone()
        # SUM() always returns exactly one row; value is None on empty table.
        raw_cash = cash_row["cash_balance"] if cash_row else None

    holdings = tuple(_holding_from_row(r, display_currency, rates) for r in pos_rows)
    total_market = sum(h.market_value for h in holdings)

    cash_balance = float(raw_cash) if raw_cash is not None else None
    # Cash is always USD for eToro.
    if cash_balance is not None:
        cash_balance = _convert_value(cash_balance, "USD", display_currency, rates)

    mirror_breakdowns = tuple(load_mirror_breakdowns(conn))
    raw_mirror_equity = sum(mb.mirror_equity_usd for mb in mirror_breakdowns)
    mirror_equity = _convert_value(raw_mirror_equity, "USD", display_currency, rates)

    total_aum = total_market + (cash_balance if cash_balance is not None else 0.0) + mirror_equity

    return PortfolioValuation(
        display_currency=display_currency,
        holdings=holdings,
        total_market=total_market,
        cash_balance=cash_balance,
        mirror_equity=mirror_equity,
        total_aum=total_aum,
        raw_rows=tuple(pos_rows),
        rates=rates,
        rates_meta=rates_meta,
        mirror_breakdowns=mirror_breakdowns,
    )
