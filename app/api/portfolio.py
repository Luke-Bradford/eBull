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
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.api._helpers import parse_optional_float, resolve_quote_price
from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.domain.positions import PositionSource
from app.services.fx import FxRateNotFound, convert, load_live_fx_rates_with_metadata
from app.services.runtime_config import get_runtime_config
from app.services.valuation import HoldingValuation, compute_portfolio_valuation

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
    # Union of every instrument_id rendered (or contributing to a
    # rendered total) on the portfolio page: held positions plus the
    # underlying instruments inside every active mirror. The frontend
    # feeds this set to its page-level LiveQuoteProvider so that
    # mirror equity / pnl figures update as the underlying ticks
    # come in, not just held-position rows.
    live_quote_instrument_ids: list[int] = []


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


def _position_item(h: HoldingValuation) -> PositionItem:
    """API shape for one valued holding.

    All marking + FX math lives in
    `app.services.valuation.compute_portfolio_valuation` (#1596) — this
    is a pure field mapping.
    """
    return PositionItem(
        instrument_id=h.instrument_id,
        symbol=h.symbol,
        company_name=h.company_name,
        open_date=h.open_date,
        avg_cost=h.avg_cost,
        current_price=h.current_price,
        current_units=h.current_units,
        cost_basis=h.cost_basis,
        market_value=h.market_value,
        unrealized_pnl=h.unrealized_pnl,
        valuation_source=h.valuation_source,
        source=h.source,
        updated_at=h.updated_at,
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
    # -- Shared valuation (#1596) -------------------------------------------
    # Positions mark-to-market, cash, mirror equity, and total_aum all come
    # from compute_portfolio_valuation — the same helper the report
    # builders use, so the report cover and this headline cannot drift.
    val = compute_portfolio_valuation(conn)
    display_currency = val.display_currency
    rates = val.rates
    rates_meta = val.rates_meta
    pos_rows = list(val.raw_rows)

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
        quote_p = resolve_quote_price(
            parse_optional_float(r, "last"),
            parse_optional_float(r, "bid"),
            parse_optional_float(r, "ask"),
        )
        daily_c = parse_optional_float(r, "daily_close")
        # Live quote → positive daily_close → None (caller falls back to amount).
        if quote_p is not None:
            mark: float | None = quote_p
        elif daily_c is not None and daily_c > 0:
            mark = daily_c
        else:
            mark = None
        price_by_instrument[iid] = (mark, str(r.get("currency") or "USD"))

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

    positions = [_position_item(h) for h in val.holdings]

    # Attach individual trades to their parent position.
    for pos in positions:
        pos.trades = trades_by_instrument.get(pos.instrument_id, [])

    cash_balance = val.cash_balance

    # Per-mirror breakdowns — loaded once inside the valuation helper;
    # the per-mirror display rows still convert each figure here.
    mirror_breakdowns = val.mirror_breakdowns
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

    mirror_equity = val.mirror_equity
    total_aum = val.total_aum

    # Re-sort by market_value DESC (computed value, not a DB column) with stable tiebreak.
    positions.sort(key=lambda p: (-p.market_value, p.instrument_id))

    # Build fx_rates_used from source currencies actually consumed.
    fx_rates_used = _build_fx_rates_used(
        pos_rows, cash_balance is not None, raw_mirror_equity, display_currency, rates_meta
    )

    # Union of every instrument_id the page should subscribe to live
    # ticks for: held position ids + underlying instrument ids inside
    # every active mirror. Mirror rows render an aggregated equity,
    # but their underlying tickers must still feed the live-tick
    # stream so the displayed mirror_equity recomputes as ticks land.
    held_ids = {p.instrument_id for p in positions}
    mirror_underlying_ids = _load_mirror_underlying_instrument_ids(conn)
    live_quote_instrument_ids = sorted(held_ids | set(mirror_underlying_ids))

    return PortfolioResponse(
        positions=positions,
        mirrors=mirrors,
        position_count=len(positions),
        total_aum=total_aum,
        cash_balance=cash_balance,
        mirror_equity=mirror_equity,
        display_currency=display_currency,
        fx_rates_used=fx_rates_used,
        live_quote_instrument_ids=live_quote_instrument_ids,
    )


def _load_mirror_underlying_instrument_ids(conn: psycopg.Connection[object]) -> list[int]:
    """Distinct instrument ids open across every active mirror.

    Used by ``get_portfolio`` to feed the page-level
    ``LiveQuoteProvider`` with mirror underlyings so the operator
    sees mirror equity recompute as the underlying tickers tick —
    not only when they navigate into a copy-trader detail page.
    Empty when no active mirror has any open positions.
    """
    sql = """
        SELECT DISTINCT cmp.instrument_id
        FROM copy_mirror_positions cmp
        JOIN copy_mirrors m USING (mirror_id)
        WHERE m.active
          AND cmp.instrument_id IS NOT NULL
        ORDER BY cmp.instrument_id
    """
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute(sql)
        return [int(row[0]) for row in cur.fetchall()]


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
               q.last AS quote_last, q.bid AS quote_bid, q.ask AS quote_ask,
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
               bp.open_rate, bp.open_conversion_rate, bp.open_date_time,
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

    # Current price in native currency (no FX conversion). Live quote
    # (last>0 → bid/ask mid) → positive daily_close. A non-positive mark is
    # not a valid price (#1428).
    quote_price = resolve_quote_price(
        parse_optional_float(inst, "quote_last"),
        parse_optional_float(inst, "quote_bid"),
        parse_optional_float(inst, "quote_ask"),
    )
    daily_close = parse_optional_float(inst, "daily_close")
    if quote_price is not None:
        current_price: float | None = quote_price
    elif daily_close is not None and daily_close > 0:
        current_price = daily_close
    else:
        current_price = None
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
        # eToro stores ``amount`` in USD but ``open_rate`` and the
        # current quote in the instrument's native currency.
        # ``open_conversion_rate`` (native→USD at open) reconciles the
        # native price delta back into USD before adding to ``amount``
        # so non-USD positions value correctly. Same pattern as the
        # copy-trading aggregate at app/services/portfolio.py:225-230.
        # USD positions store conversion_rate=1 → no-op.
        open_conv = float(tr["open_conversion_rate"])
        is_buy = tr["is_buy"]

        if current_price is not None:
            if is_buy:
                # Long: invested capital + leveraged price delta.
                mv = amount + units * (current_price - open_rate) * open_conv
                pnl = mv - amount
            else:
                # Short: profit when price drops below open_rate.
                mv = amount + units * (open_rate - current_price) * open_conv
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

# Range → days-back. None on "max" means "from the earliest row in
# fills/cash_ledger to today" (parallels the candles API `max`). The
# generate_series loop stays bounded by real data; a fund with 10y of
# trades gets 10y of points, not 5y of truncation.
_VALUE_HISTORY_RANGES: dict[str, int | None] = {
    "1m": 30,
    "3m": 90,
    "6m": 180,
    "1y": 365,
    "5y": 1825,
    "max": None,
}


class ValueHistoryPoint(BaseModel):
    date: date
    value: float


class ValueHistoryEvent(BaseModel):
    """A buy/sell visible on the value-history chart (#1594 markers).

    `source="fill"` rows are exact (our own order path).
    `source="position_open"` rows are broker-synced position opens —
    units are TODAY's units back-dated to the open (same approximation
    as the hybrid units basis). Broker-side sells are unrecorded until
    the #1593 ledger, so SELL events only ever come from fills.
    """

    date: date
    symbol: str
    side: Literal["BUY", "SELL"]
    units: float
    source: Literal["fill", "position_open"]


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
    # Buy/sell chart markers, ascending by date (#1594).
    events: list[ValueHistoryEvent] = []


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

    units_at_D is a hybrid (#1594 v1, pre-trade-ledger):

    - Instruments with `fills` rows replay the fills ledger by order
      action (BUY/ADD add, SELL/EXIT subtract) — exact through our own
      opens AND closes.
    - Broker-synced holdings carry no fills; each open position
      contributes its CURRENT units from its `open_date` forward.
      Known approximations until the #1593 ledger: partial adds/trims
      before today are backdated to open_date, and closed broker
      positions drop out of history entirely (the sync never recorded
      them).
    - Mirror/copy-portfolio equity is EXCLUDED — there is no history
      source for it; the dashboard headline includes it, so this
      series reads below `total_aum` by the live mirror equity.

    If a position had no `price_daily` close on or before D (e.g. new
    listing with stale local store), that bar is skipped for that day —
    conservatively under-states value rather than inventing a zero.

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

    # Resolve the start date in Python so both ledger queries below
    # use the same literal, not duplicated CTEs — a prior iteration
    # had `max` diverge from `5y` because the bound was computed in
    # two places. For range=max, pull the earliest ledger row; fall
    # back to CURRENT_DATE so an empty ledger produces a single-point
    # series rather than failing.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        if days is None:
            cur.execute(
                """
                SELECT COALESCE(
                    LEAST(
                        (SELECT MIN(filled_at::date) FROM fills),
                        (SELECT MIN(event_time::date) FROM cash_ledger),
                        -- Broker-synced holdings start history at their
                        -- position open_date (#1594 hybrid basis).
                        (SELECT MIN(open_date) FROM positions WHERE current_units > 0)
                    ),
                    CURRENT_DATE
                ) AS start_date
                """
            )
        else:
            cur.execute(
                "SELECT (CURRENT_DATE - make_interval(days => %(days)s::int))::date AS start_date",
                {"days": days},
            )
        # A SELECT without FROM always returns exactly one row in
        # Postgres; the `or` fallback keeps a driver-level anomaly
        # from raising an AssertionError in a live request path.
        row = cur.fetchone()
        start_date: date = row["start_date"] if row else date.today()

    # 1. Pull signed position-value points per (date, instrument).
    #    Uses a correlated subquery for close-at-or-before so the query
    #    is self-contained — no separate price lookup loop in Python.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            WITH dates AS (
                SELECT generate_series(
                    %(start)s::date,
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
            fills_units AS (
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
            ),
            fills_open_now AS (
                -- Instruments whose fills replay nets OPEN units today.
                -- Only these suppress the position basis below: an
                -- instrument with old fills that net to zero (closed
                -- via our order path) and a CURRENT broker-synced
                -- position row is a broker re-open — the position
                -- basis must price it or the holding goes invisible
                -- (Codex ckpt-2 finding). Overlap between a closed
                -- fills lifecycle and a position whose open_date
                -- predates the fills close would double-count that
                -- span; that requires contradictory data and resolves
                -- properly with the #1593 ledger.
                SELECT instrument_id
                FROM fills_signed
                GROUP BY instrument_id
                HAVING SUM(units) > 0
            ),
            position_units AS (
                -- Broker-synced holdings have no fills rows: back-fill
                -- units from each open position's open_date at TODAY's
                -- units (#1594 hybrid basis; see endpoint docstring for
                -- the documented approximations). NULL open_date
                -- contributes from the window start (held since before
                -- we tracked it) — note these holdings get NO buy
                -- marker in `events` (an unknowable date pinned to the
                -- window start would shift with the selected range).
                SELECT
                    d.d,
                    p.instrument_id,
                    SUM(p.current_units) AS units_at_date
                FROM dates d
                JOIN positions p
                  ON COALESCE(p.open_date, '-infinity'::date) <= d.d
                WHERE p.current_units > 0
                  AND p.instrument_id NOT IN (SELECT instrument_id FROM fills_open_now)
                GROUP BY d.d, p.instrument_id
                HAVING SUM(p.current_units) > 0
            ),
            units_per_day AS (
                SELECT d, instrument_id, units_at_date FROM fills_units
                UNION ALL
                SELECT d, instrument_id, units_at_date FROM position_units
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
            {"start": start_date},
        )
        position_rows = cur.fetchall()

    # 2. Pull daily cumulative cash balance per currency.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            WITH dates AS (
                SELECT generate_series(
                    %(start)s::date,
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
            -- Filter NULL-currency rows at the SQL level so they
            -- don't collapse into a single (date, NULL) group that
            -- undercounts the data-quality signal. The Python loop
            -- keeps a defence-in-depth guard in case this filter
            -- ever gets dropped.
            WHERE cl.currency IS NOT NULL
            GROUP BY d.d, cl.currency
            """,
            {"start": start_date},
        )
        cash_rows = cur.fetchall()

    # 3. Buy/sell events for chart markers (#1594). Mirrors the hybrid
    #    units basis for positions with KNOWN open dates: fills rows
    #    are exact events; no-open-fills broker positions contribute
    #    their open as a BUY. NULL-open holdings price into the curve
    #    but get no marker (no knowable date to pin it to), and the
    #    position-basis suppression keys on fills that net OPEN today
    #    — same rule as `fills_open_now` above.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT f.filled_at::date AS event_date,
                   i.symbol,
                   CASE WHEN o.action IN ('BUY', 'ADD') THEN 'BUY' ELSE 'SELL' END AS side,
                   f.units,
                   'fill' AS source
            FROM fills f
            JOIN orders o ON o.order_id = f.order_id
            JOIN instruments i ON i.instrument_id = o.instrument_id
            WHERE o.action IN ('BUY', 'ADD', 'SELL', 'EXIT')
              AND f.filled_at::date >= %(start)s
            UNION ALL
            SELECT p.open_date AS event_date,
                   i.symbol,
                   'BUY' AS side,
                   p.current_units AS units,
                   'position_open' AS source
            FROM positions p
            JOIN instruments i ON i.instrument_id = p.instrument_id
            WHERE p.current_units > 0
              AND p.open_date IS NOT NULL
              AND p.open_date >= %(start)s
              AND p.instrument_id NOT IN (
                  SELECT o2.instrument_id
                  FROM fills f2
                  JOIN orders o2 ON o2.order_id = f2.order_id
                  WHERE o2.action IN ('BUY', 'ADD', 'SELL', 'EXIT')
                  GROUP BY o2.instrument_id
                  HAVING SUM(
                      CASE WHEN o2.action IN ('BUY', 'ADD') THEN f2.units ELSE -f2.units END
                  ) > 0
              )
            ORDER BY event_date, symbol
            """,
            {"start": start_date},
        )
        event_rows = cur.fetchall()

    events = [
        ValueHistoryEvent(
            date=row["event_date"],
            symbol=str(row["symbol"]),
            side=row["side"],
            units=float(row["units"]),
            source=row["source"],
        )
        for row in event_rows
    ]

    # 4. Aggregate into one value per day in display currency.
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

    # For `max` the actual lookback depends on the earliest ledger
    # row, not the input bucket — surface the effective span so the
    # operator / charting code can label the axis correctly without
    # needing to subtract the first point's date client-side.
    if days is None:
        effective_days = (points[-1].date - points[0].date).days if len(points) >= 2 else 0
    else:
        effective_days = days

    return ValueHistoryResponse(
        display_currency=display_currency,
        range=range,
        days=effective_days,
        fx_skipped=len(fx_missing_pairs),
        points=points,
        events=events,
    )


# ---------------------------------------------------------------------------
# GET /portfolio/activity — broker-observed trade ledger (#1593 PR-2)
# ---------------------------------------------------------------------------


class ActivityEventItem(BaseModel):
    """One trade_events row, render-ready.

    Money fields (fees_usd, realized_pnl_usd) are USD account-currency;
    price is in the instrument's native currency. ``symbol`` is None
    when the instrument is absent from the current universe (deep
    history) — the FE falls back to ``#<etoro_instrument_id>``.
    """

    event_id: int
    position_id: int
    event_kind: Literal["open", "close"]
    side: Literal["buy", "sell"]
    symbol: str | None
    etoro_instrument_id: int
    units: float
    price: float | None
    executed_at: datetime
    fees_usd: float | None
    realized_pnl_usd: float | None
    # Close events only: days between this position's open event and
    # the close (fractional). None for opens or when no open is on file.
    holding_period_days: float | None
    source: Literal["etoro_sync", "etoro_history"]
    is_mirror: bool


class ActivityResponse(BaseModel):
    events: list[ActivityEventItem]
    # Total rows matching the filter (events is capped at `limit`).
    total: int
    include_mirrors: bool


@router.get("/activity", response_model=ActivityResponse)
def get_activity(
    limit: int = Query(default=100, ge=1, le=500),
    include_mirrors: bool = False,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> ActivityResponse:
    """Trade ledger feed, newest first.

    Mirror-originated rows (``social_trade_id != 0``) are excluded by
    default, consistent with the value-history chart's own-portfolio
    basis; ``include_mirrors=true`` widens the filter.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        total_row = cur.execute(
            """
            SELECT COUNT(*) AS total
            FROM trade_events
            WHERE %(include_mirrors)s OR COALESCE(social_trade_id, 0) = 0
            """,
            {"include_mirrors": include_mirrors},
        ).fetchone()
        total = int(total_row["total"]) if total_row else 0

        rows = cur.execute(
            """
            SELECT te.event_id, te.position_id, te.event_kind, te.side,
                   te.units, te.price, te.executed_at, te.fees_usd,
                   te.realized_pnl_usd, te.source, te.etoro_instrument_id,
                   te.social_trade_id, i.symbol,
                   CASE
                       WHEN te.event_kind = 'close' AND o.opened_at IS NOT NULL
                       THEN GREATEST(
                           0,
                           EXTRACT(EPOCH FROM te.executed_at - o.opened_at) / 86400.0
                       )
                   END AS holding_period_days
            FROM trade_events te
            LEFT JOIN instruments i ON i.instrument_id = te.instrument_id
            LEFT JOIN (
                SELECT position_id, executed_at AS opened_at
                FROM trade_events
                WHERE event_kind = 'open'
            ) o ON o.position_id = te.position_id AND te.event_kind = 'close'
            WHERE %(include_mirrors)s OR COALESCE(te.social_trade_id, 0) = 0
            ORDER BY te.executed_at DESC, te.event_id DESC
            LIMIT %(limit)s
            """,
            {"include_mirrors": include_mirrors, "limit": limit},
        ).fetchall()

    events = [
        ActivityEventItem(
            event_id=row["event_id"],
            position_id=row["position_id"],
            event_kind=row["event_kind"],
            side=row["side"],
            symbol=row["symbol"],
            etoro_instrument_id=row["etoro_instrument_id"],
            units=float(row["units"]),
            price=parse_optional_float(row, "price"),
            executed_at=row["executed_at"],
            fees_usd=parse_optional_float(row, "fees_usd"),
            realized_pnl_usd=parse_optional_float(row, "realized_pnl_usd"),
            holding_period_days=parse_optional_float(row, "holding_period_days"),
            source=row["source"],
            is_mirror=bool(row["social_trade_id"]),
        )
        for row in rows
    ]
    return ActivityResponse(events=events, total=total, include_mirrors=include_mirrors)
