"""Per-period FCF-yield series for the fundamentals drill page (#671, #1745).

FCF yield = TTM free cash flow / period-end market cap, rendered as a trend
overlay on the absolute-FCF line (`/instrument/:symbol/fundamentals`).

This module owns the fail-closed market-cap policy the frontend cannot
reproduce (data-eng I20 / prevention-log #1664). #1745 replaced the two
whole-series *suppressions* with per-period reconstruction:

  * **Multi-class issuers** — ``close × combined_shares`` is the structurally-wrong
    figure #1662 retired. Each period's cap is now the per-period total-company cap
    (``xbrl_derived_stats.total_company_cap_at_period``: Σ per-class price×shares +
    residual, fail-closed guards). A period with no clean cap → NULL yield for that
    point (not a whole-series suppression).
  * **Cross-currency** — FCF is in ``financial_periods.reported_currency``; price
    (hence cap) is the eToro trading currency (``instruments.currency``). A mismatch
    is normalised at the period-end FX rate (``fx_history``) before dividing; a
    period with no usable rate → NULL yield for that point.

``suppressed_reason`` is retained on the response for forward-compat but is no
longer set by either path (always ``None`` post-#1745).

Single-class, currency-coherent issuers get the per-period TTM yield:
``fcf_ttm = SUM(operating_cf) − ABS(SUM(capex))`` over 4 consecutive normalized
quarters (mirrors ``financial_periods_ttm`` sql/032:209 + the ABS(SUM) form at
sql/080:78), divided by ``period_end_shares × period_end_close``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows

from app.services.fx_history import fx_cross_rate
from app.services.xbrl_derived_stats import resolve_market_cap_basis, total_company_cap_at_period

FcfYieldSuppression = Literal["multiclass", "currency_mismatch"]
FinancialsPeriod = Literal["quarterly", "annual"]

# FX is daily; a period-end rate carried forward more than this from the nearest
# stored business day is too stale to trust for that period → NULL yield (Codex
# ckpt-1 MED-2). Generous (covers a long holiday) but bounded.
_MAX_FX_CARRY_FORWARD_DAYS = 7


@dataclass(frozen=True)
class FcfYieldRow:
    period_end: date
    period_type: str
    fcf_ttm: Decimal | None
    market_cap: Decimal | None
    fcf_yield_pct: Decimal | None
    price: Decimal | None
    price_as_of: date | None


@dataclass(frozen=True)
class FcfYieldComputation:
    suppressed_reason: FcfYieldSuppression | None
    rows: list[FcfYieldRow]


def fcf_yield_pct(fcf_ttm: Decimal | None, market_cap: Decimal | None) -> Decimal | None:
    """FCF yield % = ``fcf_ttm / market_cap × 100``.

    None when either input is absent or ``market_cap <= 0``. A NEGATIVE
    ``fcf_ttm`` is preserved (a real negative yield — cash-burning issuer), never
    clamped to zero: the operator needs to see it.
    """
    if fcf_ttm is None or market_cap is None or market_cap <= 0:
        return None
    return fcf_ttm / market_cap * 100


# Per-period TTM over 4 CONSECUTIVE normalized quarters. Mirrors
# financial_periods_ttm (sql/032:209) but PER PERIOD via a trailing-row window,
# plus a span guard financial_periods_ttm lacks.
#
# Span guard = 330 days. The (newest_end - oldest_end) span of 4 *consecutive*
# quarter-ends is ~273-275 days (3 inter-quarter gaps; <=~280d even on 53-week
# fiscal calendars — empirically max 275d across AAPL/MSFT/JPM/HD/GME on dev). A
# MISSING quarter pulls a 4th quarter from the prior year, pushing the 4-row
# window to ~364-365d. 330 cleanly separates the two: keeps every consecutive
# window (>=50d margin) and NULLs the gap windows (a silently-wrong cross-year
# "TTM"). A looser bound (e.g. 400) would ADMIT the 365d gap windows. Keep this
# `330` in sync with the spec (docs/specs/fundamentals/2026-06-26-fcf-yield-trend.md).
_QUARTERLY_SQL = """
    WITH q AS (
        SELECT
            period_end_date, period_type, shares_outstanding, reported_currency,
            SUM(operating_cf)    OVER w AS ocf_ttm,
            SUM(capex)           OVER w AS capex_ttm,
            COUNT(*)             OVER w AS n_q,
            MIN(period_end_date) OVER w AS ttm_start
        FROM financial_periods
        WHERE instrument_id = %(iid)s
          AND superseded_at IS NULL
          AND normalization_status = 'normalized'
          AND period_type IN ('Q1','Q2','Q3','Q4')
        WINDOW w AS (ORDER BY period_end_date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
    )
    SELECT
        q.period_end_date, q.period_type, q.shares_outstanding, q.reported_currency,
        CASE
            WHEN q.n_q = 4 AND (q.period_end_date - q.ttm_start) <= 330
            THEN q.ocf_ttm - ABS(COALESCE(q.capex_ttm, 0))
            ELSE NULL
        END AS fcf_ttm,
        pd.close AS price, pd.price_date AS price_as_of
    FROM q
    LEFT JOIN LATERAL (
        SELECT close, price_date FROM price_daily
        WHERE instrument_id = %(iid)s
          AND price_date <= q.period_end_date
          AND close IS NOT NULL
        ORDER BY price_date DESC
        LIMIT 1
    ) pd ON TRUE
    ORDER BY q.period_end_date DESC
    LIMIT 20
"""

# Annual: a FY row is already 12 months, so fcf = that row's own ocf − |capex|.
_ANNUAL_SQL = """
    SELECT
        fp.period_end_date, fp.period_type, fp.shares_outstanding, fp.reported_currency,
        fp.operating_cf - ABS(COALESCE(fp.capex, 0)) AS fcf_ttm,
        pd.close AS price, pd.price_date AS price_as_of
    FROM financial_periods fp
    LEFT JOIN LATERAL (
        SELECT close, price_date FROM price_daily
        WHERE instrument_id = %(iid)s
          AND price_date <= fp.period_end_date
          AND close IS NOT NULL
        ORDER BY price_date DESC
        LIMIT 1
    ) pd ON TRUE
    WHERE fp.instrument_id = %(iid)s
      AND fp.superseded_at IS NULL
      AND fp.normalization_status = 'normalized'
      AND fp.period_type = 'FY'
    ORDER BY fp.period_end_date DESC
    LIMIT 20
"""


def _trading_currency(conn: psycopg.Connection[Any], instrument_id: int) -> str | None:
    """The instrument's eToro trading currency (``None`` on a data gap). Stable per
    instrument; the per-period *reporting* currency comes from each row instead, so
    a historical reporting-currency change converts each period correctly (Codex
    ckpt-2)."""
    with conn.cursor() as cur:
        cur.execute("SELECT currency FROM instruments WHERE instrument_id = %s", (instrument_id,))
        row = cur.fetchone()
    return row[0] if row is not None else None


def _instrument_cik(conn: psycopg.Connection[Any], instrument_id: int) -> str | None:
    """10-digit primary SEC CIK, or ``None``. Same normalization as
    ``resolve_market_cap_basis`` so the curated FSDS oracle is hit identically."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT identifier_value FROM external_identifiers
            WHERE instrument_id = %s AND provider = 'sec' AND identifier_type = 'cik'
              AND is_primary = TRUE
            LIMIT 1
            """,
            (instrument_id,),
        )
        row = cur.fetchone()
    return str(row[0]).zfill(10) if row is not None and row[0] is not None else None


def _period_fx_rate(
    conn: psycopg.Connection[Any], *, period_end: date, reported_ccy: str, trading_ccy: str
) -> Decimal | None:
    """Reported→trading cross rate at ``period_end``, or ``None`` (fail-closed) if
    EITHER currency leg lacks a USD-base rate within ``_MAX_FX_CARRY_FORWARD_DAYS``
    of the period.

    Freshness is checked on each SPECIFIC leg, NOT the aggregate newest rate_date —
    a fresh unrelated pair must not let a stale ``reported``/``trading`` leg through
    (Codex ckpt-2). USD legs are unity (no stored row needed)."""
    rates: dict[tuple[str, str], Decimal] = {}
    with conn.cursor() as cur:
        for ccy in {reported_ccy, trading_ccy} - {"USD"}:
            cur.execute(
                """
                SELECT rate, rate_date FROM fx_rates_daily
                WHERE base_currency = 'USD' AND quote_currency = %(c)s AND rate_date <= %(d)s::date
                ORDER BY rate_date DESC
                LIMIT 1
                """,
                {"c": ccy, "d": period_end},
            )
            row = cur.fetchone()
            if row is None or (period_end - row[1]).days > _MAX_FX_CARRY_FORWARD_DAYS:
                return None
            rates[("USD", ccy)] = Decimal(row[0])
    return fx_cross_rate(rates, from_ccy=reported_ccy, to_ccy=trading_ccy)


def fcf_yield_series(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    period: FinancialsPeriod,
) -> FcfYieldComputation:
    """Per-period FCF-yield series. Per-period fail-closed: a period whose market
    cap or FX rate can't be sourced cleanly gets a NULL yield (its absolute FCF
    still renders); the series itself is never suppressed (#1745)."""
    basis = resolve_market_cap_basis(conn, instrument_id=instrument_id).basis
    # #1939: FPI ADR/ADS — no clean cap under any basis (ordinary shares vs
    # per-ADS price); every period's cap fails closed EXPLICITLY rather than
    # by the accident of the curated class table not covering the CIK.
    is_fpi_adr = basis == "fpi_adr_unavailable"
    is_multiclass = basis != "not_multiclass" and not is_fpi_adr
    cik = _instrument_cik(conn, instrument_id) if is_multiclass else None

    trading_ccy = _trading_currency(conn, instrument_id)

    with conn.cursor() as cur:
        cur.execute("SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date")
        today_row = cur.fetchone()
    today: date = today_row[0] if today_row is not None else date.max

    sql = _QUARTERLY_SQL if period == "quarterly" else _ANNUAL_SQL
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, {"iid": instrument_id})
        db_rows = cur.fetchall()

    rows: list[FcfYieldRow] = []
    for r in db_rows:
        period_end: date = r["period_end_date"]
        shares = r["shares_outstanding"]
        price = r["price"]
        fcf_ttm = r["fcf_ttm"]

        # Market cap: per-period total-company cap for a multi-class issuer (#1745),
        # else the single-class period_end shares × close. FPI ADR/ADS: no cap
        # (#1939) — the yield stays NULL, the absolute FCF still renders.
        if is_fpi_adr:
            market_cap = None
        elif is_multiclass:
            cap = (
                total_company_cap_at_period(conn, cik=cik, target_period_end=period_end, today=today)
                if cik is not None
                else None
            )
            market_cap = cap.value if cap is not None else None
        else:
            market_cap = shares * price if shares is not None and price is not None else None

        # FX-normalise the (reporting-currency) FCF into the trading currency the cap
        # is denominated in, before taking the ratio. The reporting currency is read
        # PER ROW (an issuer can change it over time — Codex ckpt-2). No usable rate →
        # NULL yield for this period. The displayed absolute fcf_ttm stays in its
        # reporting currency.
        reported_ccy = r["reported_currency"]
        fcf_for_yield = fcf_ttm
        if fcf_ttm is not None and trading_ccy is not None and reported_ccy is not None and reported_ccy != trading_ccy:
            rate = _period_fx_rate(conn, period_end=period_end, reported_ccy=reported_ccy, trading_ccy=trading_ccy)
            fcf_for_yield = fcf_ttm * rate if rate is not None else None

        rows.append(
            FcfYieldRow(
                period_end=period_end,
                period_type=str(r["period_type"]),
                fcf_ttm=fcf_ttm,
                market_cap=market_cap,
                fcf_yield_pct=fcf_yield_pct(fcf_for_yield, market_cap),
                price=price,
                price_as_of=r["price_as_of"],
            )
        )
    return FcfYieldComputation(suppressed_reason=None, rows=rows)
