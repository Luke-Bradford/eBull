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

from app.services.scoring import _DEFAULT_MODEL_VERSION
from app.services.sync_orchestrator.layer_types import Cadence
from app.services.sync_orchestrator.types import PREREQ_SKIP_MARKER

# Default model version for scoring freshness is the single source of truth in
# app.services.scoring (#1633: flipped to v1.2-balanced). Imported, not duplicated,
# so the freshness query always targets the same model_version the scoring pass
# writes — otherwise a flip silently wedges scoring as perpetually stale.


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


def fundamentals_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    # Liveness audit stays on ``daily_research_refresh`` — the job the
    # ``fundamentals`` DataLayer's refresh adapter dispatches
    # (sync_orchestrator/adapters.py::refresh_fundamentals). Audit-job and
    # refresh-job MUST stay aligned: if the orchestrator marks the layer
    # stale it re-runs daily_research_refresh, which must be able to clear
    # the liveness signal (Codex ckpt-2). The snapshot data itself is
    # produced by the scheduled fundamentals_sync → daily_financial_facts
    # write-through, and its correctness is checked by the content probe
    # below (not by this liveness row) — same producer/consumer split as
    # before #2008 (the snapshot was already made by fundamentals_sync
    # phase-1b under the default dedupe flag, not by the layer refresh).
    audit_fresh, audit_detail = _fresh_by_audit(conn, "daily_research_refresh", timedelta(hours=24))
    if not audit_fresh:
        return False, audit_detail
    # Content check (#2008): pipeline CONSISTENCY, not filing cadence —
    # every instrument with normalized quarter rows must have snapshot
    # rows (the write-through guarantees it; missing > 0 means the
    # write-through broke). The previous "as_of_date in the current
    # calendar quarter" rule was structurally unsatisfiable: as_of is a
    # fiscal period end, so for up to ~6 weeks after every calendar
    # quarter boundary (Rule 13a-13 10-Q deadlines: 40-45d) NO issuer
    # can have a row in the current quarter — measured red on the FULL
    # population (5,349/5,349 "missing", 2026-07-12). Per-instrument
    # filing-cadence staleness is coverage's job, not this gate's.
    #
    # Delegate to fundamentals_content_ok so the write-through-gap query
    # lives in exactly one place (review NITPICK — no drift between the
    # two gates). Imported at call time to avoid a package import cycle.
    from app.services.sync_orchestrator.content_predicates import fundamentals_content_ok

    content_ok, content_detail = fundamentals_content_ok(conn)
    if not content_ok:
        return False, content_detail
    return True, audit_detail


def scoring_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    audit_fresh, audit_detail = _fresh_by_audit(conn, "morning_candidate_review", timedelta(hours=24))
    if not audit_fresh:
        return False, audit_detail
    # Content check: latest score for default model must be newer than
    # latest candle. Theses are now on-demand (Phase 1.2 / 2.4) so the
    # scoring layer no longer gates on thesis freshness — scoring reads
    # whatever thesis row exists at the moment it runs.
    row = conn.execute(
        """
        SELECT
            (SELECT MAX(scored_at) FROM scores WHERE model_version = %s)  AS latest_score,
            (SELECT MAX(price_date)::timestamptz FROM price_daily)        AS latest_candle
        """,
        (_DEFAULT_MODEL_VERSION,),
    ).fetchone()
    if row is None:
        return False, "could not read score/candle watermarks"
    latest_score, latest_candle = row
    if latest_score is None:
        return False, "no scores for default model"
    if latest_candle is not None and latest_score < latest_candle:
        return False, "latest score older than latest candle"
    return True, audit_detail


def fair_value_band_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    """Content-watermark freshness for the fair-value band layer (#2009).

    Unlike the audit-watermark predicates above, the band is a pure
    DB-derived layer with no external I/O, so freshness reads the actual
    band rows (``MAX(computed_at)`` over ``fair_value_band_current`` for the
    live method version) rather than a ``job_runs`` audit row — the presence
    of freshly-written band rows IS the freshness signal, and it stays honest
    even if a manual recompute wrote rows outside the orchestrator job path.
    24h window matches the layer cadence. Age is computed in SQL as
    now() - computed_at; note computed_at is written with the Python client
    clock (``datetime.now(tz=UTC)`` in ``write_band``), NOT SQL now(), so this
    compares the DB-server clock against the client write-clock — they are
    co-located, so the skew is negligible.
    """
    from app.services.fair_value_band import METHOD_VERSION

    row = conn.execute(
        """
        SELECT EXTRACT(EPOCH FROM now() - MAX(computed_at)) AS age_seconds
        FROM fair_value_band_current
        WHERE method_version = %s
        """,
        (METHOD_VERSION,),
    ).fetchone()
    if row is None or row[0] is None:
        return False, "no fair_value_band_current rows for current method_version"
    age = timedelta(seconds=float(row[0]))
    if age > timedelta(hours=24):
        return False, f"fair-value bands last computed {_format_age(age)} ago (window 24h)"
    return True, f"fair-value bands last computed {_format_age(age)} ago"


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
    # Cadence cut from 5 min → 24 h: ECB publishes reference rates
    # once per working day at ~16:00 CET, so anything tighter than
    # daily was burning >95% as 304 Not Modified hits. The lifespan
    # bootstrap covers the empty-table case at boot (#502).
    return _fresh_by_audit(conn, "fx_rates_refresh", timedelta(hours=24))


def cost_models_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    return _fresh_by_audit(conn, "seed_cost_models", timedelta(hours=24))


def risk_metrics_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    # Weekly cadence (#591): risk metrics are recomputed from price
    # history, which only meaningfully shifts on a weekly horizon. A
    # 7-day audit window matches the layer cadence.
    return _fresh_by_audit(conn, "risk_metrics_refresh", timedelta(days=7))


def weekly_reports_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    return _fresh_by_audit(conn, "weekly_report", timedelta(days=7))


def monthly_reports_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    """Calendar-month anchored freshness for the monthly report layer (#335).

    Fresh iff the latest counting ``job_runs`` row for ``monthly_report``
    has its ``COALESCE(finished_at, started_at)`` anchor on or after
    the first day of the current calendar month in UTC.

    Two design choices to flag:

    * The month boundary is computed in Python (not via SQL
      ``date_trunc('month', now() at time zone 'UTC')``). The SQL
      form returns ``timestamp without time zone``, which Postgres
      silently coerces against a ``timestamptz`` ``started_at`` using
      the session's ``TimeZone`` setting — that would mis-classify
      runs at the boundary in any non-UTC DB session. Comparing two
      ``timestamptz`` values in Python sidesteps the coercion.

    * The freshness anchor is ``COALESCE(finished_at, started_at)``,
      matching ``layer_state.py::_latest_age_seconds_map``. Without
      that alignment, a run that started Jan 31 23:59 UTC and
      finished Feb 1 00:01 UTC would be reported STALE by this
      predicate and HEALTHY by the v2 state machine — the month-edge
      divergence Codex flagged on #335.
    """
    now = datetime.now(UTC)
    month_start_utc = Cadence(calendar_months=1).window_start(now)
    # ``anchor`` mirrors the state machine's ``_latest_age_seconds_map``
    # in ``layer_state.py`` — ``COALESCE(finished_at, started_at)``. A
    # monthly run that started Jan 31 23:59 UTC and finished Feb 1
    # 00:01 UTC counts as a Feb run for both views: predicate and v2
    # state-machine. Without aligning the anchor, /sync/layers (legacy)
    # and /sync/layers/v2 (state-machine) could disagree at the
    # month-boundary edge.
    row = conn.execute(
        """
        SELECT COALESCE(finished_at, started_at) AS anchor,
               status,
               error_msg,
               EXTRACT(EPOCH FROM now() - COALESCE(finished_at, started_at)) AS age_seconds
        FROM job_runs
        WHERE job_name = %s
        ORDER BY started_at DESC
        LIMIT 1
        """,
        ("monthly_report",),
    ).fetchone()
    if row is None:
        return False, "no job_runs row for monthly_report"
    anchor, status, error_msg, age_seconds = row
    is_counting = status == "success" or (
        status == "skipped" and error_msg is not None and error_msg.startswith(PREREQ_SKIP_MARKER)
    )
    if not is_counting:
        return False, f"latest monthly_report has status={status}, not a counting row"
    age = timedelta(seconds=float(age_seconds))
    if anchor < month_start_utc:
        return (
            False,
            f"last monthly_report {_format_age(age)} ago — before the start of the current calendar month UTC",
        )
    return True, f"last monthly_report {_format_age(age)} ago (this calendar month)"
