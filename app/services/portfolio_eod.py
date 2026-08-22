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
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any, Final

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
    # Native-currency P&L -> broker USD conversion fixed by the broker at
    # entry.  Kept separate from display FX: strategy accounting is USD even
    # when the operator views the main portfolio in GBP/EUR.
    open_conversion_rate: Decimal = Decimal("1")
    # ⚠ `price_daily.price_date` OF `close` — the date the mark is effective, not
    # the date the snapshot is stamped.  The lookup is carry-forward
    # (`price_date <= snapshot_date ORDER BY price_date DESC`), so an instrument
    # that has not traded since is marked from an older bar and the two differ.
    # #2602 item 4: this was fetched and discarded, which is what made
    # `local_eod_effective_time_unknown` permanent.  None exactly when `close` is.
    mark_price_date: date | None = None


@dataclass(frozen=True)
class PositionResult:
    position_id: int
    instrument_id: int
    units: Decimal
    native_ccy: str | None
    close: Decimal | None
    value_display: Decimal | None
    price_status: PriceStatus
    unrealised_pnl_usd: Decimal | None = None
    #: Carried through from ``PositionInput`` unchanged — evidence, not a decision.
    mark_price_date: date | None = None


@dataclass(frozen=True)
class MarkEffectiveness:
    """When the marks behind a snapshot's ``positions_value`` were effective.

    ⚠ ``oldest_mark_date`` is a MIN, not a MAX. "As of when is this total true"
    is bounded by the STALEST input — a snapshot dated today whose worst mark is
    a week old is a week-old valuation with a today-shaped label on it, and the
    freshest mark says nothing about that.

    ⚠ ``oldest_mark_date is None`` reads two ways once the value is STORED, and
    only one of them matters:

    * ``positions_priced > 0`` — the row predates ``sql/350``, so its marks were
      never recorded. The effective time is genuinely unknown.
    * ``positions_priced == 0`` — no position contributed to ``positions_value``
      at all (all cash, or every position unpriced). Pre- and post-migration rows
      are indistinguishable here **and need not be distinguished**: neither has a
      mark, so neither has an effective time to be unknown.

    So ``positions_priced`` is the discriminator, and it is only load-bearing in
    the first case. ``account_equity_evidence.load_account_equity_evidence`` is
    the only reader that needs it.
    """

    oldest_mark_date: date | None
    stale_mark_positions: int


def summarise_marks(results: Sequence[PositionResult], snapshot_date: date) -> MarkEffectiveness:
    """Reduce per-position mark dates to the snapshot-level effectiveness bound.

    ⚠ PRICED POSITIONS ONLY. A ``no_price`` position has no mark to be stale,
    and a ``no_fx`` one — priced natively but not convertible — contributed
    nothing to ``positions_value``, so neither can bound a total they are not in.
    Both are already reported by their own counters, so nothing is hidden by
    excluding them; including them would make the bound describe a different
    number than the one it is attached to.
    """
    marks = [r.mark_price_date for r in results if r.price_status == "priced" and r.mark_price_date is not None]
    return MarkEffectiveness(
        oldest_mark_date=min(marks) if marks else None,
        stale_mark_positions=sum(1 for mark in marks if mark < snapshot_date),
    )


#: One cent of price per unit held — the rounding of the STORED mark, and the whole
#: of the positions leg of the F-0 reconciliation tolerance (#2602 item 4).
#:
#: ⚠ No published rule fixes a broker-reconciliation tolerance; searched, none exists,
#: and none is borrowed. An earlier draft derived this from SEC Reg NMS Rule 612's
#: minimum pricing increments — withdrawn, because Rule 612 governs the increments on
#: which NMS stocks may be QUOTED, which is a different question from how far two feeds
#: valuing the same holding may legitimately differ, and it does not reach the CFD and
#: non-US products this account can hold.
#:
#: So it is fixed BY CONSTRUCTION, at the tightest bound defensible without measurement:
#: both sides mark the same units with the same formula from the same venue feed, so the
#: irreducible term is the cent the mark is stored to. It deliberately does NOT absorb an
#: extended-hours print, dividend cash we have not credited, or a corporate-action
#: disagreement — those are meant to surface as `diverged`, not be swallowed.
#:
#: ⚠ Widening this requires a measured justification AND a new
#: ``account_equity_evidence.RECONCILIATION_RULE_VERSION``. Editing the constant alone
#: silently re-verdicts every past comparison.
MARK_ROUNDING_PER_UNIT: Final[Decimal] = Decimal("0.01")


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
    #: Display-currency allowance for mark rounding across the PRICED positions only.
    #: A `no_price` or `no_fx` position contributed nothing to ``positions_value``, so
    #: it cannot contribute an allowance to a total it is not in — the same rule
    #: ``summarise_marks`` applies to the mark dates.
    mark_rounding_tolerance: Decimal = Decimal("0")
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
    mark_rounding_tolerance = Decimal("0")
    priced = no_price = no_fx = 0
    results: list[PositionResult] = []

    for p in positions:
        if p.close is None:
            no_price += 1
            # `mark_price_date` is passed rather than left to its default: the
            # LATERAL filters `close IS NOT NULL`, so it is provably None here —
            # but a result that is correct only because of an invariant in
            # another function is one edit away from being wrong silently.
            results.append(
                PositionResult(
                    p.position_id,
                    p.instrument_id,
                    p.units,
                    p.native_ccy,
                    None,
                    None,
                    "no_price",
                    mark_price_date=p.mark_price_date,
                )
            )
            continue
        # Mark-to-market equity, mirroring app/api/portfolio.py:348-357.
        # Long: invested + leveraged price gain; short: invested + gain on a
        # fall. Equals close*units only for unleveraged long (the v1 universe),
        # but stays correct if a leveraged/short row ever appears.
        if p.is_buy:
            value_native = p.amount + p.units * (p.close - p.open_rate)
            pnl_native = p.units * (p.close - p.open_rate)
        else:
            value_native = p.amount + p.units * (p.open_rate - p.close)
            pnl_native = p.units * (p.open_rate - p.close)
        unrealised_pnl_usd = pnl_native * p.open_conversion_rate
        if p.native_ccy is None:
            # No currency to convert from → cannot price into display ccy.
            no_fx += 1
            results.append(
                PositionResult(
                    p.position_id,
                    p.instrument_id,
                    p.units,
                    p.native_ccy,
                    p.close,
                    None,
                    "no_fx",
                    unrealised_pnl_usd,
                    mark_price_date=p.mark_price_date,
                )
            )
            continue
        try:
            value_display = (
                value_native if p.native_ccy == display_ccy else convert(value_native, p.native_ccy, display_ccy, rates)
            )
        except FxRateNotFound:
            no_fx += 1
            results.append(
                PositionResult(
                    p.position_id,
                    p.instrument_id,
                    p.units,
                    p.native_ccy,
                    p.close,
                    None,
                    "no_fx",
                    unrealised_pnl_usd,
                    mark_price_date=p.mark_price_date,
                )
            )
            continue
        positions_value += value_display
        # Same pair, same rates dict, immediately after the value conversion
        # succeeded — so this cannot raise where the value did not, and the
        # allowance is always denominated identically to the total it guards.
        # `abs` because a short row's allowance is a magnitude too; `units > 0`
        # is filtered upstream, and an allowance that could go negative would
        # make the tolerance unsatisfiable rather than merely wrong.
        mark_rounding_tolerance += (
            abs(p.units) * MARK_ROUNDING_PER_UNIT
            if p.native_ccy == display_ccy
            else convert(abs(p.units) * MARK_ROUNDING_PER_UNIT, p.native_ccy, display_ccy, rates)
        )
        priced += 1
        results.append(
            PositionResult(
                p.position_id,
                p.instrument_id,
                p.units,
                p.native_ccy,
                p.close,
                value_display,
                "priced",
                unrealised_pnl_usd,
                mark_price_date=p.mark_price_date,
            )
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
        mark_rounding_tolerance=mark_rounding_tolerance,
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
                b.open_conversion_rate,
                b.is_buy,
                i.currency AS native_ccy,
                mark.close AS close,
                mark.price_date AS mark_price_date
            FROM broker_positions b
            JOIN instruments i USING (instrument_id)
            -- ⚠ LATERAL, not two correlated scalar subqueries. The mark and the
            -- date it is effective must come from the SAME bar; two independent
            -- subqueries would re-run the ordering and could in principle be
            -- planned against different rows, which is a defect no test would
            -- see because they agree on every ordinary corpus. #2602 item 4.
            LEFT JOIN LATERAL (
                SELECT close, price_date FROM price_daily
                WHERE instrument_id = b.instrument_id
                  AND price_date <= %(d)s
                  AND close IS NOT NULL
                ORDER BY price_date DESC
                LIMIT 1
            ) AS mark ON TRUE
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
            open_conversion_rate=Decimal(str(r["open_conversion_rate"])),
            mark_price_date=r["mark_price_date"],
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
    marks = summarise_marks(equity.position_results, snapshot_date)

    with conn.transaction():
        _write_snapshot(conn, snapshot_date, display_ccy, fx_rate_date, equity, marks)

    logger.info(
        "eod_snapshot %s: total=%s priced=%d/%d no_price=%d no_fx=%d fx_date=%s oldest_mark=%s stale_marks=%d",
        snapshot_date,
        equity.total_value,
        equity.positions_priced,
        equity.positions_total,
        equity.positions_no_price,
        equity.positions_no_fx,
        fx_rate_date,
        marks.oldest_mark_date,
        marks.stale_mark_positions,
    )
    return equity


def _write_snapshot(
    conn: psycopg.Connection[Any],
    snapshot_date: date,
    display_ccy: str,
    fx_rate_date: date | None,
    equity: EodEquity,
    marks: MarkEffectiveness,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO portfolio_eod_snapshots (
                snapshot_date, display_currency, total_value, positions_value, cash_value,
                fx_rate_date, positions_total, positions_priced, positions_no_price,
                positions_no_fx, cash_no_fx_currencies, oldest_mark_date,
                stale_mark_positions, mark_rounding_tolerance, computed_at
            ) VALUES (
                %(d)s, %(ccy)s, %(total)s, %(pos)s, %(cash)s,
                %(fxd)s, %(ptot)s, %(ppri)s, %(pnp)s, %(pnf)s, %(cnf)s, %(omd)s,
                %(smp)s, %(mrt)s, NOW()
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
                -- ⚠ A re-run must be able to move these BACK to NULL/0. Omitting
                -- them from the update set would leave a stale bound sitting
                -- beside a freshly recomputed total, which is worse than never
                -- having recorded one.
                oldest_mark_date = EXCLUDED.oldest_mark_date,
                stale_mark_positions = EXCLUDED.stale_mark_positions,
                mark_rounding_tolerance = EXCLUDED.mark_rounding_tolerance,
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
                "omd": marks.oldest_mark_date,
                "smp": marks.stale_mark_positions,
                "mrt": equity.mark_rounding_tolerance,
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
                -- ⚠ TWO POSITIONAL LISTS THAT MUST AGREE, edited separately: the
                -- column list here and the tuple below. A new column appended at
                -- a different ordinal in one of them binds silently to the wrong
                -- field, and nothing types it. Append to BOTH tails together.
                INSERT INTO portfolio_eod_position_snapshots (
                    snapshot_date, position_id, instrument_id, units,
                    close_price, native_currency, value_display, price_status,
                    unrealised_pnl_usd, mark_price_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        r.unrealised_pnl_usd,
                        r.mark_price_date,
                    )
                    for r in equity.position_results
                ],
            )
