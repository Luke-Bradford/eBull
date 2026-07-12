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
    # `i.is_tradable = TRUE` matches the filter in `daily_candle_refresh`
    # (app/workers/scheduler.py). Without it, a delisted instrument that
    # still carries tier 1/2 coverage would permanently fail this
    # content check, because the refresh job never re-fetches it.
    row = conn.execute(
        """
        SELECT COUNT(*) AS missing
        FROM instruments i
        JOIN coverage c USING (instrument_id)
        WHERE c.coverage_tier IN (1, 2)
          AND i.is_tradable = TRUE
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
    """Every instrument with normalized quarter periods must have
    fundamentals_snapshot rows (#2008 write-through consistency).

    The previous "snapshot row in the current calendar quarter" rule was
    structurally unsatisfiable — as_of_date is a fiscal period end, so no
    issuer can satisfy it for up to ~6 weeks after each calendar-quarter
    boundary (Rule 13a-13 10-Q filing deadlines). Filing-cadence
    staleness is coverage's concern; this gate checks only that the
    snapshot write-through kept up with normalization.
    """
    row = conn.execute(
        """
        SELECT COUNT(*) AS missing
        FROM (
            SELECT DISTINCT fp.instrument_id
            FROM financial_periods fp
            WHERE fp.period_type IN ('Q1','Q2','Q3','Q4')
              AND fp.superseded_at IS NULL
              AND fp.normalization_status = 'normalized'
        ) src
        WHERE NOT EXISTS (
            SELECT 1 FROM fundamentals_snapshot fs
            WHERE fs.instrument_id = src.instrument_id
        )
        """,
    ).fetchone()
    missing = row[0] if row else 0
    if missing > 0:
        return (
            False,
            f"{missing} instruments have normalized quarter periods but no "
            f"fundamentals_snapshot rows (write-through gap)",
        )
    return True, "snapshot write-through consistent with normalized periods"
