"""Per-layer freshness predicates.

All predicates return ``(fresh: bool, detail: str)``. Predicates are
pure reads from the planning connection — no writes, no I/O outside
SELECTs.

Source of truth per spec §1.3:

    fresh_by_audit   = latest job_runs row within window IS a counting row
    fresh_by_content = per-layer content check (optional)
    layer fresh iff (fresh_by_audit AND fresh_by_content)

Counting rows = status='success' OR (status='skipped' AND
error_msg LIKE 'prereq_missing:%'). **Critical:** query the LATEST
row first, then check if it counts. Filtering for counting status
BEFORE ordering would hide a newer failure behind an older success.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import psycopg

from app.services.sync_orchestrator.types import PREREQ_SKIP_MARKER

# ---------------------------------------------------------------------------
# Default model version for scoring freshness (must match app.api.scores)
# ---------------------------------------------------------------------------


_DEFAULT_MODEL_VERSION = "v1.1-balanced"


# ---------------------------------------------------------------------------
# Shared audit-watermark helper
# ---------------------------------------------------------------------------


def _fresh_by_audit(
    conn: psycopg.Connection[Any],
    job_name: str,
    window: timedelta,
) -> tuple[bool, str]:
    """Return (True, detail) iff the LATEST job_runs row for job_name is a
    counting row within window. Latest-first ordering ensures a newer
    failure invalidates an older success.

    Age comparison is done in SQL (now() - started_at) so the freshness
    window uses the same clock that the started_at column was written
    with. Python wall-clock datetime.now(UTC) would drift from the DB
    clock inside a long-lived planning transaction; for short-window
    layers (portfolio_sync, fx_rates at 5 min) that causes spurious
    flips at the boundary.
    """
    # EXTRACT(EPOCH FROM ...) returns DOUBLE PRECISION (float) — mapped
    # by psycopg3 to Python float regardless of interval-type registration.
    # Using raw interval would depend on the connection's type loaders,
    # which could return str under some adapter configurations.
    row = conn.execute(
        """
        SELECT started_at, status, error_msg,
               EXTRACT(EPOCH FROM now() - started_at) AS age_seconds
        FROM job_runs
        WHERE job_name = %s
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (job_name,),
    ).fetchone()
    if row is None:
        return False, f"no job_runs row for {job_name}"
    _started_at, status, error_msg, age_seconds = row
    age = timedelta(seconds=float(age_seconds))
    is_counting = status == "success" or (
        status == "skipped" and error_msg is not None and error_msg.startswith(PREREQ_SKIP_MARKER)
    )
    if not is_counting:
        return False, f"latest {job_name} has status={status}, not a counting row"
    if age > window:
        return (
            False,
            f"last {job_name} {_format_age(age)} ago (window {_format_age(window)})",
        )
    return True, f"last {job_name} {_format_age(age)} ago"


def _format_age(delta: timedelta) -> str:
    total_seconds = int(delta.total_seconds())
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    if days > 0:
        return f"{days}d {hours}h"
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _current_quarter_start(today: date) -> date:
    quarter = (today.month - 1) // 3
    return date(today.year, quarter * 3 + 1, 1)


# ---------------------------------------------------------------------------
# Per-layer predicates
# ---------------------------------------------------------------------------


def universe_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    # Weekly cadence (#277). eToro's /instruments endpoint has no delta
    # filter — we pull the whole list (~15k rows) every refresh. The
    # universe rarely changes day-to-day (new listings are rare, ticker
    # changes rarer still), so a daily refresh was write amplification
    # for no information gain. A 7-day window catches meaningful
    # changes without re-pulling weekly volumes of identical rows.
    return _fresh_by_audit(conn, "nightly_universe_sync", timedelta(days=7))


def cik_mapping_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    return _fresh_by_audit(conn, "daily_cik_refresh", timedelta(hours=24))


def candles_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    audit_fresh, audit_detail = _fresh_by_audit(conn, "daily_candle_refresh", timedelta(hours=24))
    if not audit_fresh:
        return False, audit_detail
    # Content check: every T1/T2 instrument must have a candle for the
    # most recent trading day. Per-instrument query avoids the false-pass
    # of global MAX(price_date) when the table is uniformly stale.
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
    return True, audit_detail


def financial_facts_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    return _fresh_by_audit(conn, "daily_financial_facts", timedelta(hours=24))


def financial_normalization_is_fresh(
    conn: psycopg.Connection[Any],
) -> tuple[bool, str]:
    # Same source job as financial_facts — the legacy
    # daily_financial_facts runs both fetch + normalization atomically.
    return _fresh_by_audit(conn, "daily_financial_facts", timedelta(hours=24))


def fundamentals_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    audit_fresh, audit_detail = _fresh_by_audit(conn, "daily_research_refresh", timedelta(hours=24))
    if not audit_fresh:
        return False, audit_detail
    # Content check: every tradable instrument must have a
    # fundamentals_snapshot row with as_of_date in the current quarter.
    quarter_start = _current_quarter_start(date.today())
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
    return True, audit_detail


def news_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    return _fresh_by_audit(conn, "daily_news_refresh", timedelta(hours=4))


def thesis_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    # K.4: thesis is refreshed by TWO independent paths —
    # daily_thesis_refresh (its own scheduled job) and cascade_refresh
    # (runs inside daily_financial_facts). Either a recent
    # daily_thesis_refresh job_runs row OR a recent successful
    # cascade_refresh ingestion run is sufficient audit evidence.
    audit_fresh, audit_detail = _fresh_by_audit(conn, "daily_thesis_refresh", timedelta(hours=24))
    if not audit_fresh:
        cascade_row = conn.execute(
            """
            SELECT finished_at, status
            FROM data_ingestion_runs
            WHERE source = 'cascade_refresh'
              AND finished_at IS NOT NULL
            ORDER BY finished_at DESC
            LIMIT 1
            """
        ).fetchone()
        if cascade_row is None or cascade_row[0] is None or cascade_row[1] != "success":
            return False, audit_detail
        finished_at = cascade_row[0]
        # data_ingestion_runs.finished_at is TIMESTAMPTZ so psycopg3
        # returns aware datetimes. Defensive coerce in case a future
        # adapter config strips tz — subtracting aware from naive
        # raises TypeError at runtime.
        if finished_at.tzinfo is None:
            finished_at = finished_at.replace(tzinfo=UTC)
        age = datetime.now(UTC) - finished_at
        if age >= timedelta(hours=24):
            return False, f"{audit_detail}; cascade_refresh last success {_format_age(age)} ago"
        audit_detail = f"cascade_refresh success {_format_age(age)} ago (daily_thesis_refresh stale)"

    from app.services.thesis import find_stale_instruments

    stale_t1 = find_stale_instruments(conn, tier=1)
    if stale_t1:
        return False, f"thesis stale for {len(stale_t1)} Tier 1 instruments"
    return True, audit_detail


def scoring_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    audit_fresh, audit_detail = _fresh_by_audit(conn, "morning_candidate_review", timedelta(hours=24))
    if not audit_fresh:
        return False, audit_detail
    # Content check: latest score for default model must be newer than
    # latest thesis AND latest candle. If it's older, upstream writes
    # invalidated the score cache.
    row = conn.execute(
        """
        SELECT
            (SELECT MAX(scored_at) FROM scores WHERE model_version = %s)  AS latest_score,
            (SELECT MAX(created_at) FROM theses)                          AS latest_thesis,
            (SELECT MAX(price_date)::timestamptz FROM price_daily)        AS latest_candle
        """,
        (_DEFAULT_MODEL_VERSION,),
    ).fetchone()
    if row is None:
        return False, "could not read score/thesis/candle watermarks"
    latest_score, latest_thesis, latest_candle = row
    if latest_score is None:
        return False, "no scores for default model"
    if latest_thesis is not None and latest_score < latest_thesis:
        return False, "latest score older than latest thesis"
    if latest_candle is not None and latest_score < latest_candle:
        return False, "latest score older than latest candle"
    return True, audit_detail


def recommendations_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    # Spec §1.3: fresh iff MAX(trade_recommendations.created_at) >
    # MAX(scores.scored_at) for default model OR latest successful
    # job_runs for morning_candidate_review within 24h.
    row = conn.execute(
        """
        SELECT
            (SELECT MAX(created_at) FROM trade_recommendations) AS latest_rec,
            (SELECT MAX(scored_at)  FROM scores
              WHERE model_version = %s)                         AS latest_score
        """,
        (_DEFAULT_MODEL_VERSION,),
    ).fetchone()
    latest_rec = row[0] if row else None
    latest_score = row[1] if row else None
    if latest_rec is not None and latest_score is not None and latest_rec > latest_score:
        return True, "latest recommendation newer than latest score"
    # Fall back to audit watermark.
    return _fresh_by_audit(conn, "morning_candidate_review", timedelta(hours=24))


def portfolio_sync_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    return _fresh_by_audit(conn, "daily_portfolio_sync", timedelta(minutes=5))


def fx_rates_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    return _fresh_by_audit(conn, "fx_rates_refresh", timedelta(minutes=5))


def cost_models_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    return _fresh_by_audit(conn, "seed_cost_models", timedelta(hours=24))


def weekly_reports_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    return _fresh_by_audit(conn, "weekly_report", timedelta(days=7))


def monthly_reports_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    return _fresh_by_audit(conn, "monthly_report", timedelta(days=31))
