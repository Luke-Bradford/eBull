"""Dividend history + summary service.

Reads from the ``dividend_history`` + ``instrument_dividend_summary``
views (sql/050), which derive from ``financial_periods.dps_declared``
and ``financial_periods.dividends_paid`` — both already ingested from
SEC XBRL companyfacts (us-gaap:CommonStockDividendsPerShareDeclared,
us-gaap:PaymentsOfDividends) on the existing daily path.

Official free source. No rate-limit exposure beyond the once-daily
companyfacts fetch we already do.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows


@dataclass(frozen=True)
class DividendPeriod:
    """One fiscal period of dividend data for a single instrument."""

    period_end_date: date
    period_type: str  # Q1 / Q2 / Q3 / Q4 / FY
    fiscal_year: int
    fiscal_quarter: int | None
    dps_declared: Decimal | None
    dividends_paid: Decimal | None
    reported_currency: str | None


@dataclass(frozen=True)
class DividendSummary:
    """Roll-up across all periods for one instrument.

    ``has_dividend=False`` also covers the "no rows" case — callers can
    render an empty-state panel without branching on None.
    """

    has_dividend: bool
    ttm_dps: Decimal | None
    ttm_dividends_paid: Decimal | None
    ttm_yield_pct: Decimal | None
    latest_dps: Decimal | None
    latest_dividend_at: date | None
    dividend_streak_q: int
    dividend_currency: str | None


_EMPTY_SUMMARY = DividendSummary(
    has_dividend=False,
    ttm_dps=None,
    ttm_dividends_paid=None,
    ttm_yield_pct=None,
    latest_dps=None,
    latest_dividend_at=None,
    dividend_streak_q=0,
    dividend_currency=None,
)


def get_dividend_summary(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> DividendSummary:
    """Return the single-row summary for an instrument, or the
    ``has_dividend=False`` empty shape if the instrument has never
    reported a dividend."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT has_dividend,
                   ttm_dps,
                   ttm_dividends_paid,
                   ttm_yield_pct,
                   latest_dps,
                   latest_dividend_at,
                   dividend_streak_q,
                   dividend_currency
            FROM instrument_dividend_summary
            WHERE instrument_id = %s
            """,
            (instrument_id,),
        )
        row = cur.fetchone()

    if row is None:
        return _EMPTY_SUMMARY

    return DividendSummary(
        has_dividend=bool(row["has_dividend"]),
        ttm_dps=row["ttm_dps"],
        ttm_dividends_paid=row["ttm_dividends_paid"],
        ttm_yield_pct=row["ttm_yield_pct"],
        latest_dps=row["latest_dps"],
        latest_dividend_at=row["latest_dividend_at"],
        dividend_streak_q=int(row["dividend_streak_q"] or 0),
        dividend_currency=row["dividend_currency"],
    )


def get_dividend_history(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    limit: int = 40,
) -> list[DividendPeriod]:
    """Return dividend periods for an instrument, newest first.

    Default ``limit=40`` = ten years of quarterly data, enough to drive
    a per-share bar chart without paging. Caller can request a smaller
    window for spark-line renderings; capped at 400 to prevent
    accidental full-history reads.
    """
    if not 1 <= limit <= 400:
        raise ValueError(f"limit must be 1..400, got {limit}")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT period_end_date,
                   period_type,
                   fiscal_year,
                   fiscal_quarter,
                   dps_declared,
                   dividends_paid,
                   reported_currency
            FROM dividend_history
            WHERE instrument_id = %s
            ORDER BY period_end_date DESC
            LIMIT %s
            """,
            (instrument_id, limit),
        )
        rows = cur.fetchall()

    return [
        DividendPeriod(
            period_end_date=r["period_end_date"],
            period_type=str(r["period_type"]),
            fiscal_year=int(r["fiscal_year"]),
            fiscal_quarter=int(r["fiscal_quarter"]) if r["fiscal_quarter"] is not None else None,
            dps_declared=r["dps_declared"],
            dividends_paid=r["dividends_paid"],
            reported_currency=r["reported_currency"],
        )
        for r in rows
    ]
