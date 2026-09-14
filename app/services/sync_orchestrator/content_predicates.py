"""Per-layer content predicates (spec §4).

These live independently of the audit-age check so the new state
machine (chunk 4) can distinguish "audit is fresh but data is missing
rows" (DEGRADED via content) from "audit is stale" (DEGRADED via age).
The legacy `is_fresh` predicates in `freshness.py` combined both; once
chunk 7 retires that module these are the surviving content checks.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import psycopg


def candles_content_ok(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    """Every Tier 1/2 instrument must reach the last completed US session."""
    from app.services.market_calendar import latest_completed_us_session

    trading_day = latest_completed_us_session(datetime.now(UTC))
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


def research_price_quarantine_content_ok(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    """Every archive series carries coverage at its declared quarantine policy.

    This is #3028's addendum made executable: *"a coverage RECONCILIATION
    before any scan is allowed, not a counter read afterwards"*. The counter it
    replaces — ``ScanReport.excluded_no_bars`` — reads ``0`` on a wholly empty
    loader, which is the value a healthy run prints.

    ⚠ Matches on ``(rule_set_version, quarantine_as_of)``. A version match alone
    cannot tell a declared-policy run from an operator ``--as-of`` override,
    which writes different ``provisional`` verdicts under an unchanged version.
    """
    from app.services.research_corpus_ingest import RESEARCH_ARCHIVES, uncovered_series_count

    outstanding = {archive.vendor: uncovered_series_count(conn, archive) for archive in RESEARCH_ARCHIVES}
    off_policy = {vendor: n for vendor, n in outstanding.items() if n}
    if off_policy:
        detail = ", ".join(f"{vendor}: {n} series" for vendor, n in sorted(off_policy.items()))
        return (
            False,
            f"research corpus not at the current quarantine rule set / as_of ({detail}) — "
            "masked reads return zero bars for those series until re-evaluated",
        )
    return True, f"research corpus covered at the current quarantine policy ({len(outstanding)} archives)"
