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
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.api._helpers import parse_optional_float
from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.domain.positions import PositionSource
from app.services.fx import FxRateNotFound, convert, load_live_fx_rates_with_metadata
from app.services.portfolio import load_mirror_breakdowns
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


class BrokerPositionItem(BaseModel):
    """Individual eToro position (one trade) within a stock holding."""

    position_id: int
    is_buy: bool
    units: float
    amount: float
    open_rate: float
    open_date_time: datetime
    current_price: float | None
    market_value: float
    unrealized_pnl: float
    stop_loss_rate: float | None
    take_profit_rate: float | None
    is_tsl_enabled: bool
    leverage: int
    total_fees: float


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
    trades: list[BrokerPositionItem] = []


class PortfolioMirrorItem(BaseModel):
    mirror_id: int
    parent_username: str
    active: bool
    funded: float  # initial_investment + deposits - withdrawals (display currency)
    mirror_equity: float  # available_amount + sum(position market values) (display currency)
    unrealized_pnl: float  # mirror_equity - funded (display currency)
    position_count: int
    started_copy_date: datetime


class PortfolioResponse(BaseModel):
    positions: list[PositionItem]
    mirrors: list[PortfolioMirrorItem] = []
    position_count: int
    total_aum: float
    cash_balance: float | None
    mirror_equity: float = 0.0
    display_currency: str = "GBP"
    fx_rates_used: dict[str, dict[str, object]] = {}


class NativeTradeItem(BaseModel):
    """Individual trade in the instrument's native currency."""

    position_id: int
    is_buy: bool
    units: float
    amount: float  # invested — native currency
    open_rate: float  # entry price — native currency
    open_date_time: datetime
    current_price: float | None  # native currency
    market_value: float  # native currency
    unrealized_pnl: float  # native currency
    stop_loss_rate: float | None  # native currency
    take_profit_rate: float | None  # native currency
    is_tsl_enabled: bool
    leverage: int
    total_fees: float


class InstrumentPositionDetail(BaseModel):
    """Drill-through view for one instrument — all values in native currency."""

    instrument_id: int
    symbol: str
    company_name: str
    currency: str  # native currency code (e.g. "USD")
    current_price: float | None
    total_units: float
    avg_entry: float | None
    total_invested: float
    total_value: float
    total_pnl: float
    trades: list[NativeTradeItem]


class RollingPnlPeriod(BaseModel):
    """One row of the rolling P&L strip on the dashboard."""

    period: str  # "1d" | "1w" | "1m"
    pnl: float  # cumulative unrealised change in display currency
    pnl_pct: float | None  # pnl / cost_basis_at_start, None when denominator is 0
    coverage: int  # positions that contributed (had a prior close available)


class RollingPnlResponse(BaseModel):
    display_currency: str
    periods: list[RollingPnlPeriod]


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

    # -- Broker positions (individual trades per instrument) ----------------
    broker_sql = """
        SELECT bp.position_id, bp.instrument_id, bp.is_buy,
               bp.units, bp.amount, bp.open_rate,
               bp.open_date_time,
               bp.stop_loss_rate, bp.take_profit_rate,
               bp.is_tsl_enabled, bp.leverage, bp.total_fees,
               i.currency
        FROM broker_positions bp
        JOIN instruments i USING (instrument_id)
        WHERE bp.units > 0
        ORDER BY bp.instrument_id, bp.amount DESC
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(broker_sql)
        broker_rows = cur.fetchall()

    # Build instrument_id → current_price lookup from the positions query so
    # each individual trade can compute its own market_value / pnl.
    price_by_instrument: dict[int, tuple[float | None, str]] = {}
    for r in pos_rows:
        iid = r["instrument_id"]
        last_p = parse_optional_float(r, "last")
        daily_c = parse_optional_float(r, "daily_close")
        price_by_instrument[iid] = (last_p if last_p is not None else daily_c, str(r.get("currency") or "USD"))

    # Group broker positions by instrument_id.
    trades_by_instrument: dict[int, list[BrokerPositionItem]] = defaultdict(list)
    for br in broker_rows:
        iid = br["instrument_id"]
        native_ccy = str(br.get("currency") or "USD")
        cp_raw, _ = price_by_instrument.get(iid, (None, native_ccy))
        units = float(br["units"])
        amount = float(br["amount"])

        is_buy = br["is_buy"]
        open_rate_raw = float(br["open_rate"])

        if cp_raw is not None:
            if is_buy:
                # Long: invested capital + leveraged price delta.
                # Equivalent to units * cp_raw only when leverage == 1.
                mv_native = amount + units * (cp_raw - open_rate_raw)
                pnl_native = mv_native - amount
            else:
                # Short: profit when price drops below open_rate.
                mv_native = amount + units * (open_rate_raw - cp_raw)
                pnl_native = mv_native - amount
        else:
            mv_native = amount
            pnl_native = 0.0

        # Convert to display currency.
        cp_display = cp_raw
        mv_display = mv_native
        pnl_display = pnl_native
        amount_display = amount
        sl = parse_optional_float(br, "stop_loss_rate")
        tp = parse_optional_float(br, "take_profit_rate")
        open_rate = open_rate_raw
        if native_ccy != display_currency:
            try:
                mv_display = float(convert(Decimal(str(mv_native)), native_ccy, display_currency, rates))
                pnl_display = float(convert(Decimal(str(pnl_native)), native_ccy, display_currency, rates))
                amount_display = float(convert(Decimal(str(amount)), native_ccy, display_currency, rates))
                if cp_display is not None:
                    cp_display = float(convert(Decimal(str(cp_display)), native_ccy, display_currency, rates))
                open_rate = float(convert(Decimal(str(open_rate)), native_ccy, display_currency, rates))
                if sl is not None:
                    sl = float(convert(Decimal(str(sl)), native_ccy, display_currency, rates))
                if tp is not None:
                    tp = float(convert(Decimal(str(tp)), native_ccy, display_currency, rates))
            except FxRateNotFound:
                pass

        trades_by_instrument[iid].append(
            BrokerPositionItem(
                position_id=br["position_id"],
                is_buy=br["is_buy"],
                units=units,
                amount=amount_display,
                open_rate=open_rate,
                open_date_time=br["open_date_time"],
                current_price=cp_display,
                market_value=mv_display,
                unrealized_pnl=pnl_display,
                stop_loss_rate=sl,
                take_profit_rate=tp,
                is_tsl_enabled=br["is_tsl_enabled"],
                leverage=br["leverage"],
                total_fees=float(br["total_fees"]),
            )
        )

    positions = [_parse_position(r, display_currency, rates) for r in pos_rows]

    # Attach individual trades to their parent position.
    for pos in positions:
        pos.trades = trades_by_instrument.get(pos.instrument_id, [])

    cash_balance = float(raw_cash) if raw_cash is not None else None  # type: ignore[arg-type]

    # Convert cash_balance — always USD for eToro.
    if cash_balance is not None:
        cash_balance = _convert_value(cash_balance, "USD", display_currency, rates)

    # AUM: sum of position market_values + cash (if known) + mirror_equity.
    total_market = sum(p.market_value for p in positions)

    # Per-mirror breakdowns — derive total mirror_equity from these so we
    # load mirror data once instead of running two separate queries.
    mirror_breakdowns = load_mirror_breakdowns(conn)
    raw_mirror_equity = sum(mb.mirror_equity_usd for mb in mirror_breakdowns)

    # Convert each mirror's monetary values from USD to display currency.
    mirrors: list[PortfolioMirrorItem] = []
    for mb in mirror_breakdowns:
        mirrors.append(
            PortfolioMirrorItem(
                mirror_id=mb.mirror_id,
                parent_username=mb.parent_username,
                active=mb.active,
                funded=_convert_value(mb.funded_usd, "USD", display_currency, rates),
                mirror_equity=_convert_value(mb.mirror_equity_usd, "USD", display_currency, rates),
                unrealized_pnl=_convert_value(mb.unrealized_pnl_usd, "USD", display_currency, rates),
                position_count=mb.position_count,
                started_copy_date=mb.started_copy_date,
            )
        )

    # Convert total mirror_equity — always USD for eToro.
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
        mirrors=mirrors,
        position_count=len(positions),
        total_aum=total_aum,
        cash_balance=cash_balance,
        mirror_equity=mirror_equity,
        display_currency=display_currency,
        fx_rates_used=fx_rates_used,
    )


# ---------------------------------------------------------------------------
# Instrument position detail — native currency
# ---------------------------------------------------------------------------


@router.get(
    "/instruments/{instrument_id}",
    response_model=InstrumentPositionDetail,
)
def get_instrument_positions(
    instrument_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentPositionDetail:
    """Drill-through: all broker positions for one instrument in native currency."""
    instrument_sql = """
        SELECT i.instrument_id, i.symbol, i.company_name, i.currency,
               q.last AS quote_last,
               pd.close AS daily_close
        FROM instruments i
        LEFT JOIN quotes q USING (instrument_id)
        LEFT JOIN LATERAL (
            SELECT close FROM price_daily
            WHERE instrument_id = i.instrument_id AND close IS NOT NULL
            ORDER BY price_date DESC LIMIT 1
        ) pd ON TRUE
        WHERE i.instrument_id = %(iid)s
    """
    trades_sql = """
        SELECT bp.position_id, bp.is_buy, bp.units, bp.amount,
               bp.open_rate, bp.open_date_time,
               bp.stop_loss_rate, bp.take_profit_rate,
               bp.is_tsl_enabled, bp.leverage, bp.total_fees
        FROM broker_positions bp
        WHERE bp.instrument_id = %(iid)s AND bp.units > 0
        ORDER BY bp.amount DESC
    """

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(instrument_sql, {"iid": instrument_id})
        inst = cur.fetchone()
        if inst is None:
            raise HTTPException(status_code=404, detail=f"Instrument {instrument_id} not found")
        cur.execute(trades_sql, {"iid": instrument_id})
        trade_rows = cur.fetchall()

    # Current price in native currency (no FX conversion)
    quote_last = parse_optional_float(inst, "quote_last")
    daily_close = parse_optional_float(inst, "daily_close")
    current_price = quote_last if quote_last is not None else daily_close
    native_ccy = str(inst.get("currency") or "USD")

    trades: list[NativeTradeItem] = []
    total_units = 0.0
    total_invested = 0.0
    total_value = 0.0
    total_pnl = 0.0
    weighted_open_rate = 0.0  # sum(units * open_rate) for avg entry

    for tr in trade_rows:
        units = float(tr["units"])
        amount = float(tr["amount"])
        open_rate = float(tr["open_rate"])
        is_buy = tr["is_buy"]

        if current_price is not None:
            if is_buy:
                # Long: invested capital + leveraged price delta.
                mv = amount + units * (current_price - open_rate)
                pnl = mv - amount
            else:
                # Short: profit when price drops below open_rate.
                mv = amount + units * (open_rate - current_price)
                pnl = mv - amount
        else:
            mv = amount
            pnl = 0.0

        total_units += units
        total_invested += amount
        total_value += mv
        total_pnl += pnl
        weighted_open_rate += units * open_rate

        trades.append(
            NativeTradeItem(
                position_id=tr["position_id"],
                is_buy=tr["is_buy"],
                units=units,
                amount=amount,
                open_rate=open_rate,
                open_date_time=tr["open_date_time"],
                current_price=current_price,
                market_value=mv,
                unrealized_pnl=pnl,
                stop_loss_rate=parse_optional_float(tr, "stop_loss_rate"),
                take_profit_rate=parse_optional_float(tr, "take_profit_rate"),
                is_tsl_enabled=tr["is_tsl_enabled"],
                leverage=tr["leverage"],
                total_fees=float(tr["total_fees"]),
            )
        )

    avg_entry = weighted_open_rate / total_units if total_units > 0 else None

    return InstrumentPositionDetail(
        instrument_id=instrument_id,
        symbol=str(inst["symbol"]),
        company_name=str(inst["company_name"]),
        currency=native_ccy,
        current_price=current_price,
        total_units=total_units,
        avg_entry=avg_entry,
        total_invested=total_invested,
        total_value=total_value,
        total_pnl=total_pnl,
        trades=trades,
    )


# ---------------------------------------------------------------------------
# Rolling P&L (#315 Phase 2)
# ---------------------------------------------------------------------------

# Period → (label, days-back). Trading days ≠ calendar days but the
# dashboard wants calendar-week / calendar-month labels the operator
# thinks in; rolling P&L "since 7 days ago" is the right semantic
# for a long-horizon fund.
_ROLLING_PERIODS: tuple[tuple[str, int], ...] = (
    ("1d", 1),
    ("1w", 7),
    ("1m", 30),
)


@router.get("/rolling-pnl", response_model=RollingPnlResponse)
def get_rolling_pnl(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> RollingPnlResponse:
    """Unrealised P&L deltas at 1d / 1w / 1m lookbacks, in display currency.

    Per period and per open position:
        delta_native = (latest_close − close_at_or_before(anchor − N days)) * current_units
        delta_display = FX-convert(delta_native, native_ccy → display_ccy)
    Sum over positions. Anchor is each position's own `latest_close`
    price_date (NOT wall-clock `CURRENT_DATE`) so a stale candle
    store or market-closed day doesn't collapse the 1d bucket to zero
    (Codex #387 phase-2 finding).

    Positions without a prior close at that lookback (recent listings,
    fresh holdings) contribute zero to `pnl` AND zero to the cost-basis
    denominator, so they aren't wrongly attributed and don't dilute the
    percentage. `coverage` reports how many positions contributed.

    FX: converts each position's native-currency delta to display
    currency using live FX rates (same path as GET /portfolio). If a
    rate is missing the position skips — logged at WARNING, does not
    fail the endpoint.
    """
    runtime = get_runtime_config(conn)
    display_currency = runtime.display_currency
    # `load_live_fx_rates_with_metadata` returns {(from,to): {rate, quoted_at}};
    # `convert()` expects the raw Decimal, so unwrap — matches the
    # pattern in `get_portfolio` above.
    rates_meta = load_live_fx_rates_with_metadata(conn)
    rates: dict[tuple[str, str], Decimal] = {k: v["rate"] for k, v in rates_meta.items()}

    periods: list[RollingPnlPeriod] = []
    for label, days in _ROLLING_PERIODS:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT
                    p.instrument_id,
                    i.currency AS native_currency,
                    p.current_units,
                    curr.close AS curr_close,
                    curr.price_date AS curr_date,
                    prior.close AS prior_close
                FROM positions p
                JOIN instruments i USING (instrument_id)
                LEFT JOIN LATERAL (
                    SELECT close, price_date FROM price_daily
                    WHERE instrument_id = p.instrument_id
                      AND close IS NOT NULL
                    ORDER BY price_date DESC
                    LIMIT 1
                ) curr ON TRUE
                LEFT JOIN LATERAL (
                    SELECT close FROM price_daily
                    WHERE instrument_id = p.instrument_id
                      AND close IS NOT NULL
                      AND price_date <= curr.price_date - make_interval(days => %(days)s::int)
                    ORDER BY price_date DESC
                    LIMIT 1
                ) prior ON TRUE
                WHERE p.current_units > 0
                  AND curr.close IS NOT NULL
                """,
                {"days": days},
            )
            rows = cur.fetchall()

        total_pnl = Decimal("0")
        total_cost = Decimal("0")
        coverage = 0
        for row in rows:
            prior_close: Decimal | None = row["prior_close"]
            if prior_close is None:
                continue
            curr_close: Decimal = row["curr_close"]
            units = Decimal(str(row["current_units"]))
            native_ccy = str(row["native_currency"] or display_currency)
            delta_native = (curr_close - prior_close) * units
            cost_native = prior_close * units
            if native_ccy != display_currency:
                try:
                    delta_native = convert(delta_native, native_ccy, display_currency, rates)
                    cost_native = convert(cost_native, native_ccy, display_currency, rates)
                except FxRateNotFound:
                    logger.warning(
                        "rolling-pnl: FX %s→%s missing; skipping instrument_id=%s",
                        native_ccy,
                        display_currency,
                        row["instrument_id"],
                    )
                    continue
            total_pnl += delta_native
            total_cost += cost_native
            coverage += 1

        pnl_pct = float(total_pnl / total_cost) if total_cost != 0 else None
        periods.append(
            RollingPnlPeriod(
                period=label,
                pnl=float(total_pnl),
                pnl_pct=pnl_pct,
                coverage=coverage,
            )
        )

    return RollingPnlResponse(
        display_currency=display_currency,
        periods=periods,
    )


# ---------------------------------------------------------------------------
# Portfolio value history (#204)
# ---------------------------------------------------------------------------

# Range → days-back. `max` is intentionally capped at 5y (1825 days,
# same as the "5y" row) to keep the daily generate_series loop cheap;
# decade-scale history would need either materialised snapshots or an
# expression index on fills.filled_at::date / cash_ledger.event_time::date.
_VALUE_HISTORY_RANGES: dict[str, int] = {
    "1m": 30,
    "3m": 90,
    "6m": 180,
    "1y": 365,
    "5y": 1825,
    "max": 1825,
}


class ValueHistoryPoint(BaseModel):
    date: date
    value: float


ValueHistoryRange = Literal["1m", "3m", "6m", "1y", "5y", "max"]


class ValueHistoryResponse(BaseModel):
    display_currency: str
    range: ValueHistoryRange
    days: int
    # Today's FX rates applied to every historical date. A multi-
    # currency portfolio's past values are therefore approximate — a
    # proper historical-FX series lives in `fx_rates` (tax ledger)
    # but only covers dates with tax events. Callers that care about
    # historical-FX accuracy should treat this chart as directional,
    # not forensic.
    fx_mode: str = "live"
    # Distinct FX pairs we had to drop because the live snapshot didn't
    # have them. Lets the FE distinguish "truly no history" from
    # "all-skipped due to missing FX", without inflating the count by
    # (instruments × days).
    fx_skipped: int = 0
    points: list[ValueHistoryPoint]


@router.get("/value-history", response_model=ValueHistoryResponse)
def get_value_history(
    range: ValueHistoryRange = "1y",  # noqa: A002 — URL param
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> ValueHistoryResponse:
    """Daily portfolio total value (positions + cash) over a rolling window.

    Value at day D is reconstructed as:
        sum over each instrument:
            signed_units_at_D * close_at_D_or_prior   (converted to display ccy)
        plus sum over each currency:
            net_cash_ledger_at_D                       (converted to display ccy)

    Signed units replay the fills ledger by order action (BUY/ADD add,
    SELL/EXIT subtract). If a position had no `price_daily` close on
    or before D (e.g. new listing with stale local store), that bar
    is skipped for that day — conservatively under-states value
    rather than inventing a zero.

    FX conversion uses the **live** snapshot only (`live_fx_rates`).
    Historical daily FX is only populated on tax-event dates today, so
    reusing it would leave coverage gaps worse than the current
    approximation. Flagged via `fx_mode` in the response.
    """
    days = _VALUE_HISTORY_RANGES[range]

    runtime = get_runtime_config(conn)
    display_currency = runtime.display_currency
    rates_meta = load_live_fx_rates_with_metadata(conn)
    rates: dict[tuple[str, str], Decimal] = {k: v["rate"] for k, v in rates_meta.items()}

    # 1. Pull signed position-value points per (date, instrument).
    #    Uses a correlated subquery for close-at-or-before so the query
    #    self-contained — no separate price lookup loop in Python.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            WITH dates AS (
                SELECT generate_series(
                    CURRENT_DATE - make_interval(days => %(days)s::int),
                    CURRENT_DATE,
                    '1 day'::interval
                )::date AS d
            ),
            fills_signed AS (
                -- Explicit whitelist rather than default-to-negative so
                -- a future action code (e.g. a corporate-action type)
                -- can't silently flip the NAV sign. Unknown actions
                -- fall through to NULL and are dropped by the SUM.
                SELECT
                    f.filled_at::date AS fill_date,
                    o.instrument_id,
                    CASE
                        WHEN o.action IN ('BUY', 'ADD') THEN f.units
                        WHEN o.action IN ('SELL', 'EXIT') THEN -f.units
                        ELSE NULL
                    END AS units
                FROM fills f
                JOIN orders o ON o.order_id = f.order_id
                WHERE o.action IN ('BUY', 'ADD', 'SELL', 'EXIT')
            ),
            units_per_day AS (
                -- Long-only invariant (CLAUDE.md, eBull non-negotiables
                -- "Long only in v1. No shorting."). We intentionally
                -- drop zero and negative net-units: zero = fully
                -- closed (contributes nothing), negative = should
                -- not exist in this product and is treated as
                -- corrupt data rather than silently priced.
                SELECT
                    d.d,
                    fs.instrument_id,
                    SUM(fs.units) AS units_at_date
                FROM dates d
                JOIN fills_signed fs ON fs.fill_date <= d.d
                GROUP BY d.d, fs.instrument_id
                HAVING SUM(fs.units) > 0
            )
            SELECT
                u.d AS point_date,
                u.instrument_id,
                i.currency AS native_currency,
                u.units_at_date,
                (
                    SELECT close FROM price_daily
                    WHERE instrument_id = u.instrument_id
                      AND price_date <= u.d
                      AND close IS NOT NULL
                    ORDER BY price_date DESC
                    LIMIT 1
                ) AS close_at_date
            FROM units_per_day u
            JOIN instruments i USING (instrument_id)
            """,
            {"days": days},
        )
        position_rows = cur.fetchall()

    # 2. Pull daily cumulative cash balance per currency.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            WITH dates AS (
                SELECT generate_series(
                    CURRENT_DATE - make_interval(days => %(days)s::int),
                    CURRENT_DATE,
                    '1 day'::interval
                )::date AS d
            )
            SELECT
                d.d AS point_date,
                cl.currency,
                SUM(cl.amount) AS balance
            FROM dates d
            JOIN cash_ledger cl ON cl.event_time::date <= d.d
            GROUP BY d.d, cl.currency
            """,
            {"days": days},
        )
        cash_rows = cur.fetchall()

    # 3. Aggregate into one value per day in display currency.
    # cash_ledger semantics: every INSERT site (orders.py, order_client.py,
    # portfolio_sync.py) writes a *delta* row, never an absolute snapshot.
    # SUM(amount) is therefore the running balance — correct per-call-site
    # invariant, not a coincidence of the test fixtures.
    per_day: dict[date, Decimal] = defaultdict(lambda: Decimal("0"))
    # Track missing FX as a set of (from, to) pairs so the operator-
    # facing count reflects distinct gaps, not N * days of duplicates.
    fx_missing_pairs: set[tuple[str, str]] = set()

    for row in position_rows:
        close_raw = row["close_at_date"]
        if close_raw is None:
            continue  # no close on or before this date → skip, not zero
        # psycopg3 returns NUMERIC as Decimal in practice, but wrap
        # defensively so we never mix Decimal with a float if a driver
        # or column-type change ever slips in.
        close = Decimal(str(close_raw))
        units = Decimal(str(row["units_at_date"]))
        raw_ccy = row["native_currency"]
        if raw_ccy is None:
            # Instrument missing a currency is a data-quality bug, not
            # a display-currency position. Log and skip so we don't
            # silently attribute foreign value to display-ccy NAV.
            logger.warning(
                "value-history: instrument_id=%s has NULL currency; skipping on %s",
                row["instrument_id"],
                row["point_date"],
            )
            continue
        native_ccy = str(raw_ccy)
        value_native = close * units
        if native_ccy != display_currency:
            try:
                value_native = convert(value_native, native_ccy, display_currency, rates)
            except FxRateNotFound:
                fx_missing_pairs.add((native_ccy, display_currency))
                logger.warning(
                    "value-history: FX %s→%s missing; skipping instrument_id=%s on %s",
                    native_ccy,
                    display_currency,
                    row["instrument_id"],
                    row["point_date"],
                )
                continue
        per_day[row["point_date"]] += value_native

    for row in cash_rows:
        balance = Decimal(str(row["balance"]))
        raw_ccy = row["currency"]
        if raw_ccy is None:
            # Mirrors the positions-loop guard: cash without a currency
            # is a data-quality bug, not a display-currency balance.
            logger.warning(
                "value-history: cash_ledger row has NULL currency on %s; skipping",
                row["point_date"],
            )
            continue
        native_ccy = str(raw_ccy)
        if native_ccy != display_currency:
            try:
                balance = convert(balance, native_ccy, display_currency, rates)
            except FxRateNotFound:
                fx_missing_pairs.add((native_ccy, display_currency))
                logger.warning(
                    "value-history: FX %s→%s missing for cash on %s",
                    native_ccy,
                    display_currency,
                    row["point_date"],
                )
                continue
        per_day[row["point_date"]] += balance

    points = [ValueHistoryPoint(date=d, value=float(v)) for d, v in sorted(per_day.items())]
    return ValueHistoryResponse(
        display_currency=display_currency,
        range=range,
        days=days,
        fx_skipped=len(fx_missing_pairs),
        points=points,
    )
