"""Per-period FCF-yield series for the fundamentals drill page (#671).

FCF yield = TTM free cash flow / period-end market cap, rendered as a trend
overlay on the absolute-FCF line (`/instrument/:symbol/fundamentals`).

This module owns the fail-closed market-cap policy the frontend cannot
reproduce (data-eng I20 / prevention-log #1664):

  * **Multi-class issuers** — `close × combined_shares` is the structurally-wrong
    figure #1662 retired. v1 does no per-period per-class reconstruction, so any
    curated multi-class issuer (``resolve_market_cap_basis().basis !=
    "not_multiclass"``) is SUPPRESSED, not approximated.
  * **Cross-currency** — FCF is in ``financial_periods.reported_currency``; price
    is the instrument's eToro trading currency (``instruments.currency``). With no
    FX normaliser (sql/024 caveat), a proven mismatch is SUPPRESSED.

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

from app.services.xbrl_derived_stats import resolve_market_cap_basis

FcfYieldSuppression = Literal["multiclass", "currency_mismatch"]
FinancialsPeriod = Literal["quarterly", "annual"]


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


# Per-period TTM over 4 consecutive normalized quarters. Mirrors
# financial_periods_ttm (sql/032:209) but PER PERIOD via a trailing-row window;
# adds a span guard financial_periods_ttm lacks, so a MISSING quarter (4 rows
# spanning >~11 months) yields NULL rather than a silently-wrong 15-month "TTM".
_QUARTERLY_SQL = """
    WITH q AS (
        SELECT
            period_end_date, period_type, shares_outstanding,
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
        q.period_end_date, q.period_type, q.shares_outstanding,
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
        fp.period_end_date, fp.period_type, fp.shares_outstanding,
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


def _currency_mismatch(conn: psycopg.Connection[Any], instrument_id: int) -> bool:
    """True only on a PROVEN mismatch — both currencies known and different.

    A NULL trading currency (data gap) is NOT treated as a mismatch: over-
    suppressing US instruments is worse for the operator than a rare undetected
    foreign mismatch. The full-population verification (spec) reports the
    mismatch count; reported_currency is NOT NULL (sql/032:121).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT i.currency AS trading_ccy,
                   (SELECT reported_currency
                    FROM financial_periods
                    WHERE instrument_id = %(iid)s AND superseded_at IS NULL
                      AND normalization_status = 'normalized'
                    ORDER BY period_end_date DESC, filed_date DESC NULLS LAST
                    LIMIT 1) AS reported_ccy
            FROM instruments i
            WHERE i.instrument_id = %(iid)s
            """,
            {"iid": instrument_id},
        )
        row = cur.fetchone()
    if row is None:
        return False
    trading_ccy, reported_ccy = row
    return trading_ccy is not None and reported_ccy is not None and trading_ccy != reported_ccy


def fcf_yield_series(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    period: FinancialsPeriod,
) -> FcfYieldComputation:
    """Per-period FCF-yield series, or a suppression reason (empty rows)."""
    if resolve_market_cap_basis(conn, instrument_id=instrument_id).basis != "not_multiclass":
        return FcfYieldComputation(suppressed_reason="multiclass", rows=[])
    if _currency_mismatch(conn, instrument_id):
        return FcfYieldComputation(suppressed_reason="currency_mismatch", rows=[])

    sql = _QUARTERLY_SQL if period == "quarterly" else _ANNUAL_SQL
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, {"iid": instrument_id})
        db_rows = cur.fetchall()

    rows: list[FcfYieldRow] = []
    for r in db_rows:
        shares = r["shares_outstanding"]
        price = r["price"]
        fcf_ttm = r["fcf_ttm"]
        market_cap = shares * price if shares is not None and price is not None else None
        rows.append(
            FcfYieldRow(
                period_end=r["period_end_date"],
                period_type=str(r["period_type"]),
                fcf_ttm=fcf_ttm,
                market_cap=market_cap,
                fcf_yield_pct=fcf_yield_pct(fcf_ttm, market_cap),
                price=price,
                price_as_of=r["price_as_of"],
            )
        )
    return FcfYieldComputation(suppressed_reason=None, rows=rows)
