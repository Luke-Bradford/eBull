"""End-of-day portfolio equity snapshots (``portfolio_eod_snapshots``).

#1594 PR-A. Captures the operator's total equity (positions + cash) once
per closed trading session and persists it — an auditable, dated record
that the value-history chart reads instead of recomputing from scratch
(PR-B). Forward-only: it records the portfolio as it stands at compute
time, stamped to the latest session with price data; it does NOT
reconstruct history from the trade ledger (that is PR-B).

Reverses #393's informal "no NAV snapshot table" posture (operator-
approved 2026-06-12 roadmap; ``/api/v1/balances/history`` 403s on the
demo key). See spec docs/proposals/etl/2026-06-13-portfolio-value-v2-fx-eod.md.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows

from app.db.snapshot import snapshot_read
from app.services.fx import FxRateNotFound, convert
from app.services.fx_history import ensure_fx_history, load_fx_rates_for_date
from app.services.runtime_config import get_runtime_config

logger = logging.getLogger(__name__)

PriceStatus = str  # 'priced' | 'no_price' | 'no_fx'


@dataclass(frozen=True)
class PositionInput:
    position_id: int
    instrument_id: int
    units: Decimal
    native_ccy: str | None
    close: Decimal | None  # native-ccy close (the mark) on/before snapshot_date; None if none
    # Mark-to-market inputs — the snapshot records EQUITY, not notional exposure.
    # Equity = amount ± units*(mark - open_rate), mirroring GET /portfolio so the
    # snapshot agrees with the dashboard and is correct for leveraged/short rows
    # (close*units only equals equity for unleveraged long). amount is the
    # invested/margin-adjusted capital (native ccy); is_buy long vs short.
    amount: Decimal
    open_rate: Decimal
    is_buy: bool


@dataclass(frozen=True)
class PositionResult:
    position_id: int
    instrument_id: int
    units: Decimal
    native_ccy: str | None
    close: Decimal | None
    value_display: Decimal | None
    price_status: PriceStatus


@dataclass(frozen=True)
class EodEquity:
    positions_value: Decimal
    cash_value: Decimal
    total_value: Decimal
    positions_total: int
    positions_priced: int
    positions_no_price: int
    positions_no_fx: int
    cash_no_fx_currencies: int
    position_results: list[PositionResult] = field(default_factory=list)


def resolve_snapshot_date(price_dates: list[date], fallback: date) -> date:
    """The latest closed session we have prices for — data-anchored, not wall-clock.

    Idempotent: a run after midnight UTC / on a weekend / a manual retry all
    return the same date until new ``price_daily`` rows land (spec §10 B2).
    """
    return max(price_dates) if price_dates else fallback


def compute_eod_equity(
    positions: list[PositionInput],
    cash_balances: list[tuple[str | None, Decimal]],
    display_ccy: str,
    rates: dict[tuple[str, str], Decimal],
) -> EodEquity:
    """Aggregate positions + cash into display-currency equity — pure, table-tested.

    Per-position outcome is a closed set (``priced`` / ``no_price`` /
    ``no_fx``); a missing close or missing FX under-states value, never
    invents a zero (mirrors value-history's skip-not-zero).
    """
    positions_value = Decimal("0")
    priced = no_price = no_fx = 0
    results: list[PositionResult] = []

    for p in positions:
        if p.close is None:
            no_price += 1
            results.append(
                PositionResult(p.position_id, p.instrument_id, p.units, p.native_ccy, None, None, "no_price")
            )
            continue
        # Mark-to-market equity, mirroring app/api/portfolio.py:348-357.
        # Long: invested + leveraged price gain; short: invested + gain on a
        # fall. Equals close*units only for unleveraged long (the v1 universe),
        # but stays correct if a leveraged/short row ever appears.
        if p.is_buy:
            value_native = p.amount + p.units * (p.close - p.open_rate)
        else:
            value_native = p.amount + p.units * (p.open_rate - p.close)
        if p.native_ccy is None:
            # No currency to convert from → cannot price into display ccy.
            no_fx += 1
            results.append(
                PositionResult(p.position_id, p.instrument_id, p.units, p.native_ccy, p.close, None, "no_fx")
            )
            continue
        try:
            value_display = (
                value_native if p.native_ccy == display_ccy else convert(value_native, p.native_ccy, display_ccy, rates)
            )
        except FxRateNotFound:
            no_fx += 1
            results.append(
                PositionResult(p.position_id, p.instrument_id, p.units, p.native_ccy, p.close, None, "no_fx")
            )
            continue
        positions_value += value_display
        priced += 1
        results.append(
            PositionResult(p.position_id, p.instrument_id, p.units, p.native_ccy, p.close, value_display, "priced")
        )

    cash_value = Decimal("0")
    cash_no_fx = 0
    for ccy, balance in cash_balances:
        if ccy is None:
            cash_no_fx += 1
            continue
        try:
            cash_value += balance if ccy == display_ccy else convert(balance, ccy, display_ccy, rates)
        except FxRateNotFound:
            cash_no_fx += 1

    return EodEquity(
        positions_value=positions_value,
        cash_value=cash_value,
        total_value=positions_value + cash_value,
        positions_total=len(positions),
        positions_priced=priced,
        positions_no_price=no_price,
        positions_no_fx=no_fx,
        cash_no_fx_currencies=cash_no_fx,
        position_results=results,
    )


def _read_today(conn: psycopg.Connection[Any]) -> date:
    with conn.cursor() as cur:
        cur.execute("SELECT CURRENT_DATE")
        row = cur.fetchone()
    return row[0] if row else date.min


def _resolve_snapshot_date(conn: psycopg.Connection[Any], fallback: date) -> date:
    """MAX(price_daily.price_date) across currently-held instruments."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(pd.price_date)
            FROM price_daily pd
            WHERE pd.instrument_id IN (
                SELECT DISTINCT instrument_id
                FROM broker_positions
                WHERE position_id >= 0 AND units > 0
            )
            """
        )
        row = cur.fetchone()
    return row[0] if row and row[0] is not None else fallback


def _read_positions(conn: psycopg.Connection[Any], snapshot_date: date) -> list[PositionInput]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT
                b.position_id,
                b.instrument_id,
                b.units,
                b.amount,
                b.open_rate,
                b.is_buy,
                i.currency AS native_ccy,
                (
                    SELECT close FROM price_daily
                    WHERE instrument_id = b.instrument_id
                      AND price_date <= %(d)s
                      AND close IS NOT NULL
                    ORDER BY price_date DESC
                    LIMIT 1
                ) AS close
            FROM broker_positions b
            JOIN instruments i USING (instrument_id)
            WHERE b.position_id >= 0 AND b.units > 0
            """,
            {"d": snapshot_date},
        )
        rows = cur.fetchall()
    return [
        PositionInput(
            position_id=int(r["position_id"]),
            instrument_id=int(r["instrument_id"]),
            units=Decimal(str(r["units"])),
            native_ccy=str(r["native_ccy"]) if r["native_ccy"] is not None else None,
            close=Decimal(str(r["close"])) if r["close"] is not None else None,
            amount=Decimal(str(r["amount"])),
            open_rate=Decimal(str(r["open_rate"])),
            is_buy=bool(r["is_buy"]),
        )
        for r in rows
    ]


def _read_cash(conn: psycopg.Connection[Any], snapshot_date: date) -> list[tuple[str | None, Decimal]]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT currency, SUM(amount) AS balance
            FROM cash_ledger
            -- Interpret the instant in UTC before truncating to a date, so the
            -- as-of-date boundary is independent of the session timezone (a
            -- non-UTC session would otherwise bleed a late-UTC deposit into the
            -- wrong day). snapshot_date is itself a UTC trading-day (price_date).
            WHERE (event_time AT TIME ZONE 'UTC')::date <= %(d)s
            GROUP BY currency
            """,
            {"d": snapshot_date},
        )
        rows = cur.fetchall()
    return [(str(r["currency"]) if r["currency"] is not None else None, Decimal(str(r["balance"]))) for r in rows]


def compute_and_store_eod_snapshot(conn: psycopg.Connection[Any]) -> EodEquity:
    """Capture today's equity, persist the snapshot + per-position rows.

    Idempotent: re-running for the same ``snapshot_date`` overwrites (ON
    CONFLICT). Owns the connection's transaction lifecycle for the job.
    """
    today = _read_today(conn)
    snapshot_date = _resolve_snapshot_date(conn, fallback=today)

    # Ensure dated FX exists up to the snapshot date (bulk on first load,
    # gap-fill thereafter). HTTP runs outside any long-held read snapshot.
    ensure_fx_history(conn, until=snapshot_date)
    conn.commit()

    runtime = get_runtime_config(conn)
    display_ccy = runtime.display_currency

    # Consistent read of the inputs (positions / cash / FX agree).
    with snapshot_read(conn):
        positions = _read_positions(conn, snapshot_date)
        cash = _read_cash(conn, snapshot_date)
        rates, fx_rate_date = load_fx_rates_for_date(conn, snapshot_date)

    equity = compute_eod_equity(positions, cash, display_ccy, rates)

    with conn.transaction():
        _write_snapshot(conn, snapshot_date, display_ccy, fx_rate_date, equity)

    logger.info(
        "eod_snapshot %s: total=%s priced=%d/%d no_price=%d no_fx=%d fx_date=%s",
        snapshot_date,
        equity.total_value,
        equity.positions_priced,
        equity.positions_total,
        equity.positions_no_price,
        equity.positions_no_fx,
        fx_rate_date,
    )
    return equity


def _write_snapshot(
    conn: psycopg.Connection[Any],
    snapshot_date: date,
    display_ccy: str,
    fx_rate_date: date | None,
    equity: EodEquity,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO portfolio_eod_snapshots (
                snapshot_date, display_currency, total_value, positions_value, cash_value,
                fx_rate_date, positions_total, positions_priced, positions_no_price,
                positions_no_fx, cash_no_fx_currencies, computed_at
            ) VALUES (
                %(d)s, %(ccy)s, %(total)s, %(pos)s, %(cash)s,
                %(fxd)s, %(ptot)s, %(ppri)s, %(pnp)s, %(pnf)s, %(cnf)s, NOW()
            )
            ON CONFLICT (snapshot_date) DO UPDATE SET
                display_currency = EXCLUDED.display_currency,
                total_value = EXCLUDED.total_value,
                positions_value = EXCLUDED.positions_value,
                cash_value = EXCLUDED.cash_value,
                fx_rate_date = EXCLUDED.fx_rate_date,
                positions_total = EXCLUDED.positions_total,
                positions_priced = EXCLUDED.positions_priced,
                positions_no_price = EXCLUDED.positions_no_price,
                positions_no_fx = EXCLUDED.positions_no_fx,
                cash_no_fx_currencies = EXCLUDED.cash_no_fx_currencies,
                computed_at = NOW()
            """,
            {
                "d": snapshot_date,
                "ccy": display_ccy,
                "total": equity.total_value,
                "pos": equity.positions_value,
                "cash": equity.cash_value,
                "fxd": fx_rate_date,
                "ptot": equity.positions_total,
                "ppri": equity.positions_priced,
                "pnp": equity.positions_no_price,
                "pnf": equity.positions_no_fx,
                "cnf": equity.cash_no_fx_currencies,
            },
        )
        # Per-position rows: replace wholesale for this date (re-run overwrites).
        cur.execute(
            "DELETE FROM portfolio_eod_position_snapshots WHERE snapshot_date = %s",
            (snapshot_date,),
        )
        if equity.position_results:
            cur.executemany(
                """
                INSERT INTO portfolio_eod_position_snapshots (
                    snapshot_date, position_id, instrument_id, units,
                    close_price, native_currency, value_display, price_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        snapshot_date,
                        r.position_id,
                        r.instrument_id,
                        r.units,
                        r.close,
                        r.native_ccy,
                        r.value_display,
                        r.price_status,
                    )
                    for r in equity.position_results
                ],
            )
