#!/usr/bin/env python3
"""Read-only live-corpus feasibility check for #2523 regime aggregates.

This is deliberately not a production loader.  It measures the current
point-in-time cohort and the existing fail-closed quarantine coverage without
writing derived history.  A candidate still owns its calendar, cohort and
coverage threshold.
"""

from __future__ import annotations

import json
import time
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.services.price_quarantine import RULE_SET_VERSION
from app.services.strategy_regime_context import (
    CompletedSessionPanel,
    RegimeMember,
    measure_completed_session_regime,
)

_SQL = """
WITH cohort AS (
    SELECT h.instrument_id, h.provider_industry_id
      FROM instrument_market_classification_history h
     WHERE h.effective_to IS NULL
       AND h.security_type = 'common_stock'
       AND h.primary_listing_market IN ('nyse', 'nasdaq')
       AND h.provider_industry_id IS NOT NULL
), sessions AS (
    -- eToro's daily bar label is not a civil NYSE date (Monday sessions can
    -- carry a Sunday date), so a weekday filter would be wrong.  Use the
    -- declared SPY provider series as the session calendar instead of a union
    -- in which a handful of sparse foreign/off-calendar bars create fake
    -- sessions.  Instrument 3000 is asserted below by the verification query.
    SELECT p.price_date
      FROM price_daily p
      JOIN price_quarantine_coverage cov
        ON cov.instrument_id = p.instrument_id
       AND cov.rule_set_version = %(rule_set_version)s
       AND p.price_date BETWEEN cov.first_bar AND cov.last_bar
      LEFT JOIN price_bar_quarantine q
        ON q.instrument_id = p.instrument_id
       AND q.price_date = p.price_date
       AND q.rule_set_version = %(rule_set_version)s
     WHERE p.instrument_id = 3000
       AND p.close > 0
       AND COALESCE(q.return_usable, TRUE)
       AND NOT COALESCE(q.provisional, FALSE)
     ORDER BY p.price_date DESC
     LIMIT 21
)
SELECT c.instrument_id, c.provider_industry_id, s.price_date,
       CASE WHEN p.close > 0
                  AND cov.instrument_id IS NOT NULL
                  AND COALESCE(q.return_usable, TRUE)
                  AND NOT COALESCE(q.provisional, FALSE)
            THEN p.close
            ELSE NULL
       END AS close,
       p.close > 0
           AND cov.instrument_id IS NOT NULL
           AND COALESCE(q.return_usable, TRUE)
           AND NOT COALESCE(q.provisional, FALSE)
           AND tq.instrument_id IS NULL
           AND b.instrument_id IS NULL AS return_link_usable
  FROM cohort c
 CROSS JOIN sessions s
  LEFT JOIN price_daily p
    ON p.instrument_id = c.instrument_id
   AND p.price_date = s.price_date
  LEFT JOIN price_quarantine_coverage cov
    ON cov.instrument_id = p.instrument_id
   AND cov.rule_set_version = %(rule_set_version)s
   AND p.price_date BETWEEN cov.first_bar AND cov.last_bar
  LEFT JOIN price_bar_quarantine q
    ON q.instrument_id = p.instrument_id
   AND q.price_date = p.price_date
   AND q.rule_set_version = %(rule_set_version)s
  LEFT JOIN price_transition_quarantine tq
    ON tq.instrument_id = c.instrument_id
   AND tq.price_date = s.price_date
   AND tq.rule_set_version = %(rule_set_version)s
  LEFT JOIN price_series_break b
    ON b.instrument_id = c.instrument_id
   AND b.break_date = s.price_date
 ORDER BY c.instrument_id, s.price_date
"""


def _load(conn: psycopg.Connection[Any]) -> CompletedSessionPanel:
    reference = conn.execute("SELECT symbol, instrument_type_id FROM instruments WHERE instrument_id = 3000").fetchone()
    if reference != ("SPY", 6):
        raise RuntimeError(f"reference calendar identity drifted: expected SPY ETF (3000, 6), got {reference!r}")
    frontiers = conn.execute(
        "SELECT (SELECT max(break_id) FROM price_series_break), "
        "       (SELECT max(adjustment_id) FROM price_adjustments)"
    ).fetchone()
    if frontiers is None:
        raise RuntimeError("could not pin price unit-regime frontiers")
    break_frontier, adjustment_frontier = frontiers
    rows = conn.execute(_SQL, {"rule_set_version": RULE_SET_VERSION}).fetchall()
    if not rows:
        raise RuntimeError("current point-in-time cohort has no usable completed-session rows")
    dates = tuple(sorted({row[2] for row in rows}))
    grouped: dict[tuple[int, int], dict[date, Decimal | None]] = {}
    links: dict[tuple[int, int], dict[date, bool]] = {}
    for instrument_id, industry_id, session_date, close, return_link_usable in rows:
        grouped.setdefault((instrument_id, industry_id), {})[session_date] = close
        links.setdefault((instrument_id, industry_id), {})[session_date] = return_link_usable
    members = tuple(
        RegimeMember(
            instrument_id=instrument_id,
            provider_industry_id=industry_id,
            closes=tuple(by_date.get(session_date) for session_date in dates),
            return_links=(False,) + tuple(links[(instrument_id, industry_id)].get(day, False) for day in dates[1:]),
        )
        for (instrument_id, industry_id), by_date in grouped.items()
    )
    return CompletedSessionPanel(
        session_dates=dates,
        members=members,
        cohort_version="current-pit-nyse-nasdaq-common-with-provider-industry+spy-calendar-3000",
        source_version=(
            f"price_daily+{RULE_SET_VERSION}+break<={break_frontier or 0}+adjustment<={adjustment_frontier or 0}"
        ),
        price_basis="quarantine_joinable_vendor_close",
    )


def main() -> None:
    started = time.perf_counter()
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        panel = _load(conn)
    loaded = time.perf_counter()
    result = measure_completed_session_regime(
        panel,
        minimum_coverage=Decimal("0.80"),
        minimum_sector_members=20,
    )
    finished = time.perf_counter()
    print(
        json.dumps(
            {
                "read_only": True,
                "formula_version": result.version,
                "latest_completed_session": result.latest_completed_session.isoformat(),
                "sessions": len(panel.session_dates),
                "expected_members": result.expected_count,
                "trend_coverage": str(result.prior_trend.coverage),
                "common_movement": {
                    "verdict": result.common_movement.verdict,
                    "coverage": str(result.common_movement.coverage),
                    "variance_share": (
                        None
                        if result.common_movement.variance_share is None
                        else str(result.common_movement.variance_share)
                    ),
                },
                "horizons": [
                    {
                        "sessions": item.horizon_sessions,
                        "verdict": item.verdict,
                        "coverage": str(item.coverage),
                    }
                    for item in result.market_horizons
                ],
                "sectors": len(result.sectors),
                "sectors_at_least_20_members": sum(sector.expected_count >= 20 for sector in result.sectors),
                "load_seconds": round(loaded - started, 3),
                "compute_seconds": round(finished - loaded, 3),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
