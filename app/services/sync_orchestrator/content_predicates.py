"""Per-layer content predicates (spec §4).

These live independently of the audit-age check so the new state
machine (chunk 4) can distinguish "audit is fresh but data is missing
rows" (DEGRADED via content) from "audit is stale" (DEGRADED via age).
The legacy `is_fresh` predicates in `freshness.py` combined both; once
chunk 7 retires that module these are the surviving content checks.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import psycopg


def candles_content_ok(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    """Every Tier 1/2 instrument must have a candle for the most recent trading day."""
    from app.services.market_data import _most_recent_trading_day

    trading_day = _most_recent_trading_day(date.today())
    row = conn.execute(
        """
        SELECT COUNT(*) AS missing
        FROM instruments i
        JOIN coverage c USING (instrument_id)
        WHERE c.coverage_tier IN (1, 2)
          AND COALESCE(
              (SELECT MAX(price_date) FROM price_daily p
               WHERE p.instrument_id = i.instrument_id),
              DATE '1900-01-01'
          ) < %s
        """,
        (trading_day,),
    ).fetchone()
    missing = row[0] if row else 0
    if missing > 0:
        return (
            False,
            f"{missing} T1/T2 instruments missing candle for {trading_day.isoformat()}",
        )
    return True, "all T1/T2 instruments current"


def fundamentals_content_ok(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    """Every tradable instrument must have a fundamentals_snapshot row in the current quarter."""
    today = date.today()
    quarter = (today.month - 1) // 3
    quarter_start = date(today.year, quarter * 3 + 1, 1)
    row = conn.execute(
        """
        SELECT COUNT(*) AS missing
        FROM instruments i
        WHERE i.is_tradable = TRUE
          AND NOT EXISTS (
              SELECT 1 FROM fundamentals_snapshot fs
              WHERE fs.instrument_id = i.instrument_id
                AND fs.as_of_date >= %s
          )
        """,
        (quarter_start,),
    ).fetchone()
    missing = row[0] if row else 0
    if missing > 0:
        return (
            False,
            f"{missing} tradable instruments lack fundamentals snapshot "
            f"for quarter starting {quarter_start.isoformat()}",
        )
    return True, "all tradable instruments have snapshot"
