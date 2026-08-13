"""Share-count + dilution service (#435).

Reads from views defined in sql/052 on top of facts already ingested
by the daily ``fundamentals_sync`` path (#430 expanded TRACKED_CONCEPTS
+ added DEI extraction). Zero new HTTP.

Drives:
  - instrument-page "Share count" chart + dilution badge
  - ranking-engine quality sub-score (net dilution YoY penalty)
  - live market-cap derivation (shares × close) — retires part of
    the yfinance profile path under #432.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows

DilutionPosture = Literal["dilutive", "buyback_heavy", "stable"]


@dataclass(frozen=True)
class ShareCountPeriod:
    period_end: date
    fiscal_year: int | None
    fiscal_period: str | None
    shares_outstanding: Decimal | None
    shares_issued_new: Decimal | None
    buyback_shares: Decimal | None


@dataclass(frozen=True)
class DilutionSummary:
    latest_shares: Decimal | None
    latest_as_of: date | None
    yoy_shares: Decimal | None
    net_dilution_pct_yoy: Decimal | None
    ttm_shares_issued: Decimal | None
    ttm_buyback_shares: Decimal | None
    ttm_net_share_change: Decimal | None
    dilution_posture: DilutionPosture


@dataclass(frozen=True)
class ShareCountLatest:
    latest_shares: Decimal
    as_of_date: date
    source_taxonomy: str  # 'dei' | 'us-gaap' | 'none'


_EMPTY_SUMMARY = DilutionSummary(
    latest_shares=None,
    latest_as_of=None,
    yoy_shares=None,
    net_dilution_pct_yoy=None,
    ttm_shares_issued=None,
    ttm_buyback_shares=None,
    ttm_net_share_change=None,
    dilution_posture="stable",
)


def get_share_count_history(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    limit: int = 40,
) -> list[ShareCountPeriod]:
    """Newest-first per-period share count + deltas. Default 10 years
    (40 quarters); capped at 200 to prevent runaway reads."""
    if not 1 <= limit <= 200:
        raise ValueError(f"limit must be 1..200, got {limit}")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT period_end, fiscal_year, fiscal_period,
                   shares_outstanding, shares_issued_new, buyback_shares
            FROM share_count_history
            WHERE instrument_id = %s
            ORDER BY period_end DESC
            LIMIT %s
            """,
            (instrument_id, limit),
        )
        rows = cur.fetchall()

    return [
        ShareCountPeriod(
            period_end=r["period_end"],
            fiscal_year=int(r["fiscal_year"]) if r["fiscal_year"] is not None else None,
            fiscal_period=r["fiscal_period"],
            shares_outstanding=r["shares_outstanding"],
            shares_issued_new=r["shares_issued_new"],
            buyback_shares=r["buyback_shares"],
        )
        for r in rows
    ]


def get_dilution_summary(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> DilutionSummary:
    """Roll-up across the last year. Never-paid / pre-seed returns
    ``_EMPTY_SUMMARY`` so callers render empty-state without None
    branching."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT latest_shares, latest_as_of, yoy_shares,
                   net_dilution_pct_yoy,
                   ttm_shares_issued, ttm_buyback_shares,
                   ttm_net_share_change, dilution_posture
            FROM instrument_dilution_summary
            WHERE instrument_id = %s
            """,
            (instrument_id,),
        )
        row = cur.fetchone()

    if row is None:
        return _EMPTY_SUMMARY

    posture_raw = str(row["dilution_posture"])
    if posture_raw not in ("dilutive", "buyback_heavy", "stable"):
        posture: DilutionPosture = "stable"
    else:
        posture = posture_raw  # type: ignore[assignment]

    return DilutionSummary(
        latest_shares=row["latest_shares"],
        latest_as_of=row["latest_as_of"],
        yoy_shares=row["yoy_shares"],
        net_dilution_pct_yoy=row["net_dilution_pct_yoy"],
        ttm_shares_issued=row["ttm_shares_issued"],
        ttm_buyback_shares=row["ttm_buyback_shares"],
        ttm_net_share_change=row["ttm_net_share_change"],
        dilution_posture=posture,
    )


def get_latest_share_count(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> ShareCountLatest | None:
    """Single point-in-time newest share count. Drives live market-cap
    derivation (``latest_shares × latest_close``)."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT latest_shares, as_of_date, source_taxonomy
            FROM instrument_share_count_latest
            WHERE instrument_id = %s
            """,
            (instrument_id,),
        )
        row = cur.fetchone()

    if row is None or row["latest_shares"] is None:
        return None

    return ShareCountLatest(
        latest_shares=row["latest_shares"],
        as_of_date=row["as_of_date"],
        source_taxonomy=str(row["source_taxonomy"]),
    )
