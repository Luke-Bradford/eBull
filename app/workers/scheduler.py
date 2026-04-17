"""Scheduled job functions and declared schedule registry.

Each function represents one scheduled job. The ``JobRuntime`` in
``app.jobs.runtime`` registers them with APScheduler and handles
catch-up, prerequisite checks, and manual triggers.

This module owns the **declared schedule registry** (``SCHEDULED_JOBS``),
which is the single source of truth for:

* job names — referenced from each ``_tracked_job(...)`` call site so the
  ``job_runs.job_name`` value cannot drift from what the system reports.
* declared cadences — APScheduler ``CronTrigger`` instances are derived
  from these via ``_trigger_for()`` in ``runtime.py``.

The ``/system/jobs`` endpoint uses live APScheduler introspection for
``next_run_time``; ``compute_next_run`` remains as a pure utility for
catch-up-on-boot and tests.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable, Generator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal

import anthropic
import psycopg
import psycopg.rows
import psycopg.sql
from psycopg.types.json import Jsonb

from app.config import settings
from app.providers.implementations.companies_house import CompaniesHouseFilingsProvider
from app.providers.implementations.etoro import EtoroMarketDataProvider
from app.providers.implementations.fmp import FmpFundamentalsProvider
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider
from app.services.broker_credentials import CredentialNotFound, load_credential_for_provider_use
from app.services.coverage import review_coverage, seed_coverage
from app.services.deferred_retry import retry_deferred_recommendations
from app.services.enrichment import refresh_enrichment
from app.services.entry_timing import evaluate_entry_conditions
from app.services.execution_guard import evaluate_recommendation
from app.services.filings import FilingsRefreshSummary, refresh_filings, upsert_cik_mapping
from app.services.fundamentals import refresh_fundamentals
from app.services.market_data import refresh_market_data
from app.services.operators import AmbiguousOperatorError, NoOperatorError, sole_operator_id
from app.services.ops_monitor import (
    check_row_count_spike,
    record_job_finish,
    record_job_skip,
    record_job_start,
)
from app.services.order_client import execute_order
from app.services.portfolio import run_portfolio_review
from app.services.portfolio_sync import sync_portfolio
from app.services.position_monitor import check_position_health
from app.services.return_attribution import (
    SUMMARY_WINDOWS,
    compute_attribution_summary,
    persist_attribution_summary,
)
from app.services.scoring import compute_rankings
from app.services.sync_orchestrator import prereq_skip_reason
from app.services.sync_orchestrator.progress import report_progress
from app.services.tax_ledger import ingest_tax_events, run_disposal_matching
from app.services.thesis import find_stale_instruments, generate_thesis
from app.services.universe import enrich_instrument_currencies, sync_universe
from app.services.watermarks import get_watermark, set_watermark

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Declared schedule registry
# ---------------------------------------------------------------------------
#
# Cadence semantics (UTC throughout):
#
#   hourly  — runs every hour at ``minute`` past the hour. ``next_run_time``
#             is the next future occurrence of that minute, regardless of
#             whether the previous run actually fired.
#   daily   — runs once per day at ``hour:minute`` UTC. ``next_run_time`` is
#             today's occurrence if it is still in the future, otherwise
#             tomorrow's.
#   weekly  — runs once per week on ``weekday`` (0=Monday … 6=Sunday) at
#             ``hour:minute`` UTC.
#
# These cadences feed the APScheduler ``CronTrigger`` registration in
# ``app.jobs.runtime``. The ``/system/jobs`` endpoint reads the live
# next-fire-time from APScheduler; ``compute_next_run`` is retained as
# a pure utility for catch-up-on-boot and tests.

CadenceKind = Literal["every_n_minutes", "hourly", "daily", "weekly", "monthly"]


@dataclass(frozen=True)
class Cadence:
    """Small typed cadence model.

    Only the fields relevant to the cadence ``kind`` are consulted; the rest
    default to zero. Constructed via the helper classmethods so call sites do
    not have to remember which fields apply where.
    """

    kind: CadenceKind
    minute: int = 0
    hour: int = 0
    weekday: int = 0  # 0=Mon (matches datetime.weekday())
    day: int = 0  # 1..28 for monthly cadence
    interval_minutes: int = 0  # every_n_minutes cadence (e.g. 5 for every 5 min)

    @classmethod
    def every_n_minutes(cls, *, interval: int) -> Cadence:
        """Cron-style sub-hourly cadence — e.g. interval=5 fires at
        :00, :05, :10, … every hour. Used by orchestrator_high_frequency_sync."""
        if interval < 1 or interval > 30:
            raise ValueError(f"every_n_minutes interval must be 1..30, got {interval}")
        if 60 % interval != 0:
            raise ValueError(f"every_n_minutes interval must divide 60 evenly, got {interval}")
        return cls(kind="every_n_minutes", interval_minutes=interval)

    @classmethod
    def hourly(cls, *, minute: int = 0) -> Cadence:
        if not 0 <= minute <= 59:
            raise ValueError(f"hourly minute must be 0..59, got {minute}")
        return cls(kind="hourly", minute=minute)

    @classmethod
    def daily(cls, *, hour: int, minute: int = 0) -> Cadence:
        if not 0 <= hour <= 23:
            raise ValueError(f"daily hour must be 0..23, got {hour}")
        if not 0 <= minute <= 59:
            raise ValueError(f"daily minute must be 0..59, got {minute}")
        return cls(kind="daily", hour=hour, minute=minute)

    @classmethod
    def weekly(cls, *, weekday: int, hour: int, minute: int = 0) -> Cadence:
        if not 0 <= weekday <= 6:
            raise ValueError(f"weekly weekday must be 0..6, got {weekday}")
        if not 0 <= hour <= 23:
            raise ValueError(f"weekly hour must be 0..23, got {hour}")
        if not 0 <= minute <= 59:
            raise ValueError(f"weekly minute must be 0..59, got {minute}")
        return cls(kind="weekly", weekday=weekday, hour=hour, minute=minute)

    @classmethod
    def monthly(cls, *, day: int, hour: int, minute: int = 0) -> Cadence:
        if not 1 <= day <= 28:
            raise ValueError(f"monthly day must be 1..28, got {day}")
        if not 0 <= hour <= 23:
            raise ValueError(f"monthly hour must be 0..23, got {hour}")
        if not 0 <= minute <= 59:
            raise ValueError(f"monthly minute must be 0..59, got {minute}")
        return cls(kind="monthly", day=day, hour=hour, minute=minute)

    @property
    def label(self) -> str:
        """Human-readable label for API responses."""
        if self.kind == "every_n_minutes":
            return f"every {self.interval_minutes}m"
        if self.kind == "hourly":
            return f"hourly at :{self.minute:02d} UTC"
        if self.kind == "daily":
            return f"daily at {self.hour:02d}:{self.minute:02d} UTC"
        if self.kind == "weekly":
            weekday_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            return f"weekly on {weekday_names[self.weekday]} at {self.hour:02d}:{self.minute:02d} UTC"
        # monthly
        return f"monthly on day {self.day} at {self.hour:02d}:{self.minute:02d} UTC"


PrerequisiteResult = tuple[bool, str]
"""(met, reason) — True if the prerequisite is satisfied; reason explains why not."""

# Type alias for prerequisite callables.  Each takes a psycopg connection
# and returns (met, reason).  The connection is opened by the caller
# (catch-up or scheduled-fire path) and closed after the check.
PrerequisiteFn = Callable[[psycopg.Connection[Any]], PrerequisiteResult]


@dataclass(frozen=True)
class ScheduledJob:
    """A registered scheduled job."""

    name: str
    description: str
    cadence: Cadence
    # When True, the job runtime will trigger this job at startup if it
    # is overdue (last successful run's next scheduled fire <= now, or
    # no successful run exists at all).  Set to False for jobs that are
    # too expensive or have side-effects that make cold-start firing
    # undesirable.
    catch_up_on_boot: bool = True
    # Optional prerequisite check.  When set, the catch-up and
    # scheduled-fire paths call this before running the job.  If
    # the check returns (False, reason), the job is skipped and a
    # ``job_runs`` row with status='skipped' is recorded.
    prerequisite: PrerequisiteFn | None = None


# Job-name constants. Every ``_tracked_job(...)`` call site below references
# one of these so the literal cannot drift from the registry / job_runs row.
JOB_NIGHTLY_UNIVERSE_SYNC = "nightly_universe_sync"
JOB_DAILY_CANDLE_REFRESH = "daily_candle_refresh"
JOB_DAILY_CIK_REFRESH = "daily_cik_refresh"
JOB_DAILY_RESEARCH_REFRESH = "daily_research_refresh"
JOB_DAILY_NEWS_REFRESH = "daily_news_refresh"
JOB_DAILY_THESIS_REFRESH = "daily_thesis_refresh"
JOB_MORNING_CANDIDATE_REVIEW = "morning_candidate_review"
JOB_WEEKLY_COVERAGE_REVIEW = "weekly_coverage_review"
JOB_DAILY_TAX_RECONCILIATION = "daily_tax_reconciliation"
JOB_DAILY_PORTFOLIO_SYNC = "daily_portfolio_sync"
JOB_EXECUTE_APPROVED_ORDERS = "execute_approved_orders"
JOB_FX_RATES_REFRESH = "fx_rates_refresh"
JOB_RETRY_DEFERRED = "retry_deferred_recommendations"
JOB_MONITOR_POSITIONS = "monitor_positions"
JOB_ATTRIBUTION_SUMMARY = "attribution_summary"
JOB_WEEKLY_REPORT = "weekly_report"
JOB_MONTHLY_REPORT = "monthly_report"
JOB_SEED_COST_MODELS = "seed_cost_models"
JOB_DAILY_FINANCIAL_FACTS = "daily_financial_facts"
JOB_ORCHESTRATOR_FULL_SYNC = "orchestrator_full_sync"
JOB_ORCHESTRATOR_HIGH_FREQUENCY_SYNC = "orchestrator_high_frequency_sync"


# ---------------------------------------------------------------------------
# Prerequisite checks
# ---------------------------------------------------------------------------
#
# Each returns (met: bool, reason: str).  The reason is recorded in the
# job_runs.error_msg column when the job is skipped and in the boot
# catch-up summary log line.


def _exists(conn: psycopg.Connection[Any], sql: psycopg.sql.SQL) -> bool:
    """Run a ``SELECT EXISTS(...)`` query and return the boolean result.

    Uses an explicit ``tuple_row`` factory so the result is always
    positional, regardless of the connection-level ``row_factory``.
    """
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        row = cur.execute(sql).fetchone()
    return row is not None and bool(row[0])


def _has_coverage_tier12(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
    """True if at least one Tier 1 or Tier 2 coverage row exists."""
    if _exists(conn, psycopg.sql.SQL("SELECT EXISTS(SELECT 1 FROM coverage WHERE coverage_tier IN (1, 2))")):
        return (True, "")
    return (False, "no Tier 1/2 coverage rows")


def _has_any_coverage(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
    """True if the coverage table has at least one row."""
    if _exists(conn, psycopg.sql.SQL("SELECT EXISTS(SELECT 1 FROM coverage)")):
        return (True, "")
    return (False, "coverage table is empty")


def _has_scores(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
    """True if at least one score row exists."""
    if _exists(conn, psycopg.sql.SQL("SELECT EXISTS(SELECT 1 FROM scores)")):
        return (True, "")
    return (False, "no scores rows")


def _has_scoreable_instruments(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
    """True if at least one tradable instrument has data to score.

    Mirrors the eligibility query in compute_rankings (scoring.py) so the
    prerequisite passes if and only if scoring would find at least one
    instrument to score.  This replaces _has_scores for
    morning_candidate_review to break the bootstrap deadlock where
    scoring cannot run because no scores exist yet.
    """
    if _exists(
        conn,
        psycopg.sql.SQL(
            """
            SELECT EXISTS(
                SELECT 1
                FROM instruments i
                JOIN coverage c ON c.instrument_id = i.instrument_id
                WHERE i.is_tradable = TRUE
                  AND (
                      EXISTS (SELECT 1 FROM theses t WHERE t.instrument_id = i.instrument_id)
                      OR EXISTS (SELECT 1 FROM fundamentals_snapshot f WHERE f.instrument_id = i.instrument_id)
                      OR EXISTS (SELECT 1 FROM price_daily p WHERE p.instrument_id = i.instrument_id)
                  )
            )
            """
        ),
    ):
        return (True, "")
    return (False, "no scoreable instruments")


def _has_actionable_recommendations(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
    """True if at least one proposed or approved recommendation exists."""
    if _exists(
        conn,
        psycopg.sql.SQL("SELECT EXISTS(SELECT 1 FROM trade_recommendations WHERE status IN ('proposed', 'approved'))"),
    ):
        return (True, "")
    return (False, "no proposed or approved recommendations")


def _has_deferred_recommendations(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
    """True if at least one timing_deferred BUY/ADD recommendation exists."""
    if _exists(
        conn,
        psycopg.sql.SQL(
            "SELECT EXISTS(SELECT 1 FROM trade_recommendations "
            "WHERE status = 'timing_deferred' AND action IN ('BUY', 'ADD'))"
        ),
    ):
        return (True, "")
    return (False, "no timing_deferred BUY/ADD recommendations")


def _has_open_positions(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
    """True if at least one open position exists."""
    if _exists(
        conn,
        psycopg.sql.SQL("SELECT EXISTS(SELECT 1 FROM positions WHERE current_units > 0)"),
    ):
        return (True, "")
    return (False, "no open positions")


def _has_tier1_stale_theses(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
    """True if at least one Tier 1 instrument exists (thesis staleness is checked by the job itself)."""
    if _exists(conn, psycopg.sql.SQL("SELECT EXISTS(SELECT 1 FROM coverage WHERE coverage_tier = 1)")):
        return (True, "")
    return (False, "no Tier 1 instruments")


def _has_tier1_coverage(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
    """True if at least one instrument has Tier 1 coverage."""
    if _exists(conn, psycopg.sql.SQL("SELECT EXISTS(SELECT 1 FROM coverage WHERE coverage_tier = 1)")):
        return (True, "")
    return (False, "no Tier 1 instruments yet")


def _has_attributions(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
    """True if at least one attributed position exists."""
    if _exists(conn, psycopg.sql.SQL("SELECT EXISTS(SELECT 1 FROM return_attribution)")):
        return (True, "")
    return (False, "no attributed positions yet")


def _has_positions_or_attributions(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
    """True if there are open positions or any attributed positions."""
    if _exists(
        conn,
        psycopg.sql.SQL(
            "SELECT EXISTS(SELECT 1 FROM positions WHERE current_units > 0) OR EXISTS(SELECT 1 FROM return_attribution)"
        ),
    ):
        return (True, "")
    return (False, "no positions or attributions to report on")


# Declared schedule. Hours/minutes are deliberate-but-arbitrary placeholders
# until APScheduler is wired — the values are stable enough for operator UI
# planning but should not be treated as the live truth. See module docstring.
SCHEDULED_JOBS: list[ScheduledJob] = [
    # -- Orchestrator triggers (Phase 4 — replaces 12 legacy cron jobs) --
    # Single daily full-DAG sync at 03:00 UTC. The orchestrator plans
    # which layers are stale and refreshes only those, in topological
    # order. Replaces the 12 removed legacy entries that mapped to
    # non-empty JOB_TO_LAYERS values (see spec §4.5). The 13th in-DAG
    # adapter is nightly_universe_sync, which was already on-demand
    # only before Phase 4 — it remains in _INVOKERS for the Admin UI.
    ScheduledJob(
        name=JOB_ORCHESTRATOR_FULL_SYNC,
        description="Orchestrator full sync — walks the DAG and refreshes stale layers.",
        cadence=Cadence.daily(hour=3, minute=0),
        # Never catch up on boot. A full sync runs ~45min (research refresh
        # dominates) and holds DB connections the HTTP layer needs. Every
        # dev-stack restart would otherwise fire a catch-up and wedge the
        # site until it finishes. If the 03:00 UTC slot is missed, the
        # operator can click "Sync now" in the admin UI.
        catch_up_on_boot=False,
    ),
    # Every-5-minutes refresh of independent high-frequency layers
    # (portfolio_sync + fx_rates). The orchestrator's partial unique
    # index gate ensures this cannot overlap with a still-running FULL
    # sync — the wrapper catches SyncAlreadyRunning and logs.
    ScheduledJob(
        name=JOB_ORCHESTRATOR_HIGH_FREQUENCY_SYNC,
        description="Orchestrator high-frequency sync — portfolio_sync + fx_rates every 5 minutes.",
        cadence=Cadence.every_n_minutes(interval=5),
        catch_up_on_boot=False,
    ),
    # -- Outside-DAG jobs (5 kept on their own cron triggers) ------------
    # These have empty JOB_TO_LAYERS entries and remain independently
    # scheduled; they do not participate in the orchestrator DAG.
    ScheduledJob(
        name=JOB_EXECUTE_APPROVED_ORDERS,
        description="Guard and execute actionable trade recommendations.",
        cadence=Cadence.daily(hour=6, minute=30),
        prerequisite=_has_actionable_recommendations,
        # Do not fire on cold boot — order execution must only happen at
        # the scheduled time, not as a surprise catch-up hours later.
        catch_up_on_boot=False,
    ),
    ScheduledJob(
        name=JOB_RETRY_DEFERRED,
        description="Re-evaluate timing_deferred recommendations with fresh TA data.",
        cadence=Cadence.hourly(minute=30),
        prerequisite=_has_deferred_recommendations,
        catch_up_on_boot=False,
    ),
    ScheduledJob(
        name=JOB_MONITOR_POSITIONS,
        description="Check open positions for SL/TP breaches and thesis breaks.",
        cadence=Cadence.hourly(minute=15),
        prerequisite=_has_open_positions,
        catch_up_on_boot=False,
    ),
    ScheduledJob(
        name=JOB_WEEKLY_COVERAGE_REVIEW,
        description="Review coverage tier assignments; promote/demote instruments.",
        cadence=Cadence.weekly(weekday=0, hour=5, minute=0),
        prerequisite=_has_any_coverage,
    ),
    ScheduledJob(
        name=JOB_ATTRIBUTION_SUMMARY,
        description="Compute and persist rolling attribution summaries (30d, 90d, 365d).",
        cadence=Cadence.weekly(weekday=6, hour=6, minute=0),
        prerequisite=_has_attributions,
        catch_up_on_boot=False,
    ),
    # -- On-demand jobs are NOT listed here.  They stay in _INVOKERS
    # (runtime.py) so "Run now" in the Admin UI works, but they are
    # not registered with APScheduler and do not participate in
    # catch-up.  Currently on-demand: daily_tax_reconciliation.
]


def compute_next_run(cadence: Cadence, now: datetime) -> datetime:
    """Return the next future occurrence of ``cadence`` after ``now``.

    The returned datetime is strictly greater than ``now`` — if ``now`` lands
    exactly on a fire time we return the *following* one, which matches how
    APScheduler reports ``next_run_time`` immediately after a fire.

    ``now`` must be timezone-aware (UTC). The result is also UTC.

    See module docstring for cadence semantics. This is a pure function so it
    can be unit-tested without DB or scheduler setup.
    """
    if now.tzinfo is None:
        raise ValueError("compute_next_run requires a timezone-aware 'now'")
    now_utc = now.astimezone(UTC)

    if cadence.kind == "every_n_minutes":
        # Next slot is the smallest k*interval minute past the current hour
        # that is strictly after now.
        interval = cadence.interval_minutes
        candidate = now_utc.replace(second=0, microsecond=0)
        next_minute = ((now_utc.minute // interval) + 1) * interval
        if next_minute >= 60:
            candidate = candidate.replace(minute=0) + timedelta(hours=1)
        else:
            candidate = candidate.replace(minute=next_minute)
        return candidate

    if cadence.kind == "hourly":
        candidate = now_utc.replace(minute=cadence.minute, second=0, microsecond=0)
        if candidate <= now_utc:
            candidate += timedelta(hours=1)
        return candidate

    if cadence.kind == "daily":
        candidate = now_utc.replace(hour=cadence.hour, minute=cadence.minute, second=0, microsecond=0)
        if candidate <= now_utc:
            candidate += timedelta(days=1)
        return candidate

    if cadence.kind == "weekly":
        candidate = now_utc.replace(hour=cadence.hour, minute=cadence.minute, second=0, microsecond=0)
        days_ahead = (cadence.weekday - candidate.weekday()) % 7
        candidate += timedelta(days=days_ahead)
        if candidate <= now_utc:
            candidate += timedelta(days=7)
        return candidate

    # monthly
    candidate = now_utc.replace(day=cadence.day, hour=cadence.hour, minute=cadence.minute, second=0, microsecond=0)
    if candidate <= now_utc:
        # Advance to next month
        if candidate.month == 12:
            candidate = candidate.replace(year=candidate.year + 1, month=1)
        else:
            candidate = candidate.replace(month=candidate.month + 1)
    return candidate


# ---------------------------------------------------------------------------
# Job tracking helper
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _tracked_job(job_name: str) -> Generator[_JobTracker]:
    """
    Context manager that records a job_runs row on entry (status=running)
    and updates it on exit (status=success or failure).

    Usage::

        with _tracked_job(JOB_NIGHTLY_UNIVERSE_SYNC) as tracker:
            # ... do work ...
            tracker.row_count = summary.inserted + summary.updated

    If the body raises, the job is recorded as failure with the error message.
    """
    tracker = _JobTracker(job_name)
    try:
        with psycopg.connect(settings.database_url) as conn:
            tracker.run_id = record_job_start(conn, job_name)
    except Exception:
        logger.error("Failed to record job start for %s", job_name, exc_info=True)
        # Still run the job even if tracking fails.
        yield tracker
        return

    try:
        yield tracker
    except Exception as exc:
        try:
            with psycopg.connect(settings.database_url) as conn:
                record_job_finish(conn, tracker.run_id, status="failure", error_msg=str(exc))
        except Exception:
            logger.error("Failed to record job failure for %s", job_name, exc_info=True)
        raise
    else:
        try:
            with psycopg.connect(settings.database_url) as conn:
                record_job_finish(
                    conn,
                    tracker.run_id,
                    status="success",
                    row_count=tracker.row_count,
                )
                # Check for row-count spikes after recording the successful run.
                # Exclude the current run_id so we compare against the *previous*
                # successful run, not the one we just wrote.
                if tracker.row_count is not None:
                    spike = check_row_count_spike(conn, job_name, tracker.row_count, exclude_run_id=tracker.run_id)
                    if spike.flagged:
                        logger.warning("Row-count spike detected: %s", spike.detail)
        except Exception:
            logger.error("Failed to record job success for %s", job_name, exc_info=True)


class _JobTracker:
    """Mutable bag passed into the tracked_job context so the caller can set row_count."""

    def __init__(self, job_name: str) -> None:
        self.job_name = job_name
        self.run_id: int = 0
        self.row_count: int | None = None


def _load_etoro_credentials(job_name: str) -> tuple[str, str] | None:
    """Load (api_key, user_key) for ``settings.etoro_env``.

    Returns ``None`` if either credential is missing. Failures are logged
    at ERROR with the specific missing label and environment.

    Each credential load is committed individually so audit rows are
    durable even if the second load fails (e.g. user_key not found
    must not silently roll back the api_key audit row).
    """
    try:
        with psycopg.connect(settings.database_url) as conn:
            op_id = sole_operator_id(conn)
            api_key = load_credential_for_provider_use(
                conn,
                operator_id=op_id,
                provider="etoro",
                label="api_key",
                environment=settings.etoro_env,
                caller=job_name,
            )
            conn.commit()  # api_key audit row durable
            user_key = load_credential_for_provider_use(
                conn,
                operator_id=op_id,
                provider="etoro",
                label="user_key",
                environment=settings.etoro_env,
                caller=job_name,
            )
            conn.commit()  # user_key audit row durable
    except (NoOperatorError, AmbiguousOperatorError) as exc:
        logger.error("%s: %s, skipping", job_name, exc)
        return None
    except CredentialNotFound as exc:
        logger.error("%s: %s, skipping", job_name, exc)
        return None
    return (api_key, user_key)


def _record_prereq_skip(job_name: str, detail: str) -> None:
    """Write a PREREQ_SKIP-marked job_runs row for a job that cannot run
    due to a missing prerequisite (credentials, API key, etc.).

    Called BEFORE entering `_tracked_job` so exactly one job_runs row is
    written. The orchestrator's fresh_by_audit rule counts this row as
    ran-to-prerequisite-check (spec §1.3) so the layer doesn't look
    stale-forever while the prerequisite is missing.
    """
    try:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            record_job_skip(conn, job_name, prereq_skip_reason(detail))
    except Exception:
        logger.error("%s: failed to write prereq-skip audit row", job_name, exc_info=True)


def _promote_held_to_tier1(conn: psycopg.Connection[Any]) -> int:
    """Promote instruments with open positions to coverage Tier 1.

    Returns the number of instruments promoted. Idempotent — instruments
    already at Tier 1 are untouched. Must be called inside an open
    transaction (the caller commits).
    """
    result = conn.execute(
        """
        UPDATE coverage
        SET coverage_tier = 1
        WHERE instrument_id IN (
            SELECT instrument_id FROM positions WHERE current_units > 0
        )
          AND coverage_tier != 1
        """
    )
    return result.rowcount


def nightly_universe_sync() -> None:
    """
    Sync the eToro tradable instrument universe to the local DB.

    Runs nightly. Idempotent — safe to re-run.
    """
    creds = _load_etoro_credentials("nightly_universe_sync")
    if creds is None:
        _record_prereq_skip(JOB_NIGHTLY_UNIVERSE_SYNC, "etoro credentials missing")
        return
    api_key, user_key = creds

    with _tracked_job(JOB_NIGHTLY_UNIVERSE_SYNC) as tracker:
        with (
            EtoroMarketDataProvider(api_key=api_key, user_key=user_key, env=settings.etoro_env) as provider,
            psycopg.connect(settings.database_url) as conn,
        ):
            # Two separate transactions so a coverage-seeding failure
            # does not roll back a completed universe sync.  Each
            # function opens its own conn.transaction() (savepoints)
            # internally; the outer transaction ensures a clean,
            # well-defined connection state for each call.
            #
            # All variable references stay inside the transaction block
            # that defines them to avoid UnboundLocalError if __exit__
            # raises (prevention-log entry from PR #148 round 1).
            # row_count accumulates across blocks without cross-block
            # reads of tracker.row_count.
            row_count = 0

            with conn.transaction():
                summary = sync_universe(provider, conn)
                row_count = summary.inserted + summary.updated
                tracker.row_count = row_count
                logger.info(
                    "Universe sync: inserted=%d updated=%d deactivated=%d",
                    summary.inserted,
                    summary.updated,
                    summary.deactivated,
                )

            # First-run bootstrap: if the coverage table is empty after a
            # successful universe sync, seed all tradable instruments at
            # Tier 3.  This is a no-op on subsequent runs (seed_coverage
            # checks for existing rows and skips if non-empty).
            with conn.transaction():
                seed_result = seed_coverage(conn)
                row_count += seed_result.seeded
                tracker.row_count = row_count
                logger.info(
                    "Coverage seed: seeded=%d already_populated=%s",
                    seed_result.seeded,
                    seed_result.already_populated,
                )

            # Enrich instrument currencies from FMP for instruments that
            # are missing currency data or haven't been enriched in 90 days.
            # Uses a separate autocommit connection so each per-instrument
            # UPDATE commits independently, and HTTP I/O (FMP API calls)
            # does not hold a transaction open.
            if settings.fmp_api_key:
                try:
                    with (
                        FmpFundamentalsProvider(api_key=settings.fmp_api_key) as fmp_provider,
                        psycopg.connect(settings.database_url, autocommit=True) as enrich_conn,
                    ):
                        enriched = enrich_instrument_currencies(fmp_provider, enrich_conn)
                        row_count += enriched
                        tracker.row_count = row_count
                        logger.info("Currency enrichment: enriched=%d", enriched)
                except Exception:
                    logger.warning(
                        "Currency enrichment failed; universe sync and coverage still committed",
                        exc_info=True,
                    )
            else:
                logger.info("Currency enrichment skipped: FMP API key not configured")

            tracker.row_count = row_count


_T3_BOOTSTRAP_BATCH_SIZE = 200
"""Max T3 instruments to include in candle refresh for bootstrap scoring.

Prevents hitting API rate limits while giving enough T3 instruments price
data to enable T3→T2 promotion via the scoring/coverage pipeline.
"""


def daily_candle_refresh() -> None:
    """
    Refresh candles for Tier 1/2 instruments and a bootstrap subset of
    Tier 3 instruments that have fundamentals data.

    T1/T2: all covered instruments (uncapped).
    T3: up to _T3_BOOTSTRAP_BATCH_SIZE instruments that already have a
    fundamentals_snapshot row, ordered by symbol for determinism.  This
    gives T3 instruments enough price data for momentum scoring, enabling
    T3→T2 promotion via the weekly coverage review.

    Fetches up to 400 daily candles per instrument (enough for 1y return
    + buffer).  Quotes are skipped (owned by the hourly job).

    Runs daily at 22:00 UTC, after US market close.
    """
    creds = _load_etoro_credentials("daily_candle_refresh")
    if creds is None:
        _record_prereq_skip(JOB_DAILY_CANDLE_REFRESH, "etoro credentials missing")
        return
    api_key, user_key = creds

    with _tracked_job(JOB_DAILY_CANDLE_REFRESH) as tracker:
        with (
            EtoroMarketDataProvider(api_key=api_key, user_key=user_key, env=settings.etoro_env) as provider,
            psycopg.connect(settings.database_url) as conn,
        ):
            # T1/T2: all covered instruments
            tier12_rows = conn.execute(
                """
                SELECT i.instrument_id, i.symbol
                FROM instruments i
                JOIN coverage c ON c.instrument_id = i.instrument_id
                WHERE i.is_tradable = TRUE
                  AND c.coverage_tier IN (1, 2)
                ORDER BY i.symbol, i.instrument_id
                """
            ).fetchall()

            # T3: bootstrap batch — instruments with fundamentals but no
            # candle data yet, capped to avoid API rate limit pressure.
            # NOT EXISTS(price_daily) is intentional: once an instrument
            # has any candle data it drops out of the bootstrap pool.
            # refresh_market_data fetches ~400 candles per instrument in
            # a single API call, so a "partial" bootstrap still gives
            # enough data for momentum scoring.  If the API call fails
            # entirely, no rows are inserted and the instrument retries
            # next run.
            t3_rows = conn.execute(
                """
                SELECT i.instrument_id, i.symbol
                FROM instruments i
                JOIN coverage c ON c.instrument_id = i.instrument_id
                WHERE i.is_tradable = TRUE
                  AND c.coverage_tier = 3
                  AND EXISTS (
                      SELECT 1 FROM fundamentals_snapshot f
                      WHERE f.instrument_id = i.instrument_id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM price_daily p
                      WHERE p.instrument_id = i.instrument_id
                  )
                ORDER BY i.symbol, i.instrument_id
                LIMIT %(limit)s
                """,
                {"limit": _T3_BOOTSTRAP_BATCH_SIZE},
            ).fetchall()

            all_rows = tier12_rows + t3_rows
            if not all_rows:
                logger.info("daily_candle_refresh: no covered instruments found, skipping")
                tracker.row_count = 0
                return

            logger.info(
                "daily_candle_refresh: %d T1/T2 + %d T3 bootstrap = %d instruments",
                len(tier12_rows),
                len(t3_rows),
                len(all_rows),
            )

            instruments = [(row[0], row[1]) for row in all_rows]
            # skip_quotes=True: quote freshness is owned by the hourly
            # fx_rates_refresh job; daily candle job must not shadow
            # those fresher values with stale end-of-day data.
            summary = refresh_market_data(provider, conn, instruments, skip_quotes=True)
        tracker.row_count = summary.candle_rows_upserted

    logger.info(
        "Market refresh complete: instruments=%d candles=%d features=%d quotes=%d quotes_skipped=%d spread_flags=%d",
        summary.instruments_refreshed,
        summary.candle_rows_upserted,
        summary.features_computed,
        summary.quotes_updated,
        summary.quotes_skipped,
        summary.spread_flags_set,
    )


def daily_cik_refresh() -> None:
    """
    Refresh SEC ticker → CIK mapping and upsert into external_identifiers.

    Runs daily. Idempotent — safe to re-run.

    Conditional-fetch path (#270): sends If-Modified-Since against the
    prior Last-Modified watermark. When the server returns 304, skips
    the upsert loop entirely — most days this is a zero-byte no-op.
    On 200 with an unchanged body (defensive — SEC could serve 200
    with identical bytes), the sha256 body-hash watermark lets us skip
    anyway. Only when the body genuinely changed do we do the full
    upsert and advance the watermark.
    """
    SOURCE = "sec.tickers"
    WATERMARK_KEY = "global"

    # Pre-bind so any reference after the inner `with` blocks is always
    # bound — a future refactor that moves these assignments deeper
    # cannot produce an UnboundLocalError under an exception path.
    upserted = 0
    mapping_size = 0

    with _tracked_job(JOB_DAILY_CIK_REFRESH) as tracker:
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as provider,
            psycopg.connect(settings.database_url) as conn,
        ):
            prior = get_watermark(conn, SOURCE, WATERMARK_KEY)
            # Explicit truthy check: an empty-string watermark from a
            # prior run where Last-Modified was absent must NOT be sent
            # as `If-Modified-Since: ` (invalid HTTP date).
            if_modified_since = prior.watermark if (prior and prior.watermark) else None

            result = provider.build_cik_mapping_conditional(
                if_modified_since=if_modified_since,
            )

            if result is None:
                # 304 — nothing changed.
                logger.info("daily_cik_refresh: 304 Not Modified, skipping upsert")
                tracker.row_count = 0
                return

            mapping_size = len(result.mapping)

            if prior and prior.response_hash == result.body_hash:
                # 200 with identical bytes. Advance fetched_at only.
                logger.info("daily_cik_refresh: 200 but body hash unchanged, skipping upsert")
                with conn.transaction():
                    set_watermark(
                        conn,
                        source=SOURCE,
                        key=WATERMARK_KEY,
                        watermark=result.last_modified or prior.watermark,
                        response_hash=result.body_hash,
                    )
                tracker.row_count = 0
                return

            rows = conn.execute(
                "SELECT symbol, instrument_id::text FROM instruments WHERE is_tradable = TRUE"
            ).fetchall()
            instrument_symbols = [(row[0], row[1]) for row in rows]

            # Upsert + watermark advance must land atomically — if the
            # watermark committed but the upserts didn't (crash), the
            # next run would skip and the data would drift.
            with conn.transaction():
                upserted = upsert_cik_mapping(conn, result.mapping, instrument_symbols)
                set_watermark(
                    conn,
                    source=SOURCE,
                    key=WATERMARK_KEY,
                    # Empty string when Last-Modified is absent is the
                    # "no validator available" sentinel — next run's
                    # truthy check above will fall back to no-header.
                    # The body_hash still works for dedup in that case.
                    watermark=result.last_modified or "",
                    response_hash=result.body_hash,
                )
        tracker.row_count = upserted

    logger.info(
        "CIK refresh complete: mapping_size=%d upserted=%d",
        mapping_size,
        upserted,
    )


def daily_research_refresh() -> None:
    """
    Refresh fundamentals and filings for all tradable instruments.

    Runs daily. Fetches:
      - SEC XBRL fundamentals (primary, free) for US instruments with a CIK
      - FMP fundamentals (fallback) for remaining instruments if API key is set
      - SEC EDGAR filing metadata for US instruments with a known CIK
      - Companies House filing metadata for UK instruments with a company_number

    No tier gate — fundamentals and filings are cheap batch operations.
    Hydrating data for the full universe lets scoring produce scores for
    T3 instruments, enabling the weekly coverage review to promote them
    to T2 on deterministic signals alone.
    """
    if not settings.companies_house_api_key:
        logger.warning("daily_research_refresh: COMPANIES_HOUSE_API_KEY not set, skipping CH filings")

    with _tracked_job(JOB_DAILY_RESEARCH_REFRESH) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            rows = conn.execute(
                """
                SELECT i.symbol, i.instrument_id::text
                FROM instruments i
                WHERE i.is_tradable = TRUE
                ORDER BY i.symbol
                """
            ).fetchall()

            # Build symbol→CIK mapping for SEC fundamentals
            cik_rows = conn.execute(
                """
                SELECT i.symbol, ei.identifier_value
                FROM external_identifiers ei
                JOIN instruments i ON i.instrument_id = ei.instrument_id
                WHERE ei.provider = 'sec'
                  AND ei.identifier_type = 'cik'
                  AND i.is_tradable = TRUE
                """
            ).fetchall()

        if not rows:
            logger.info("daily_research_refresh: no tradable instruments found, skipping")
            tracker.row_count = 0
            return

        symbols = [(row[0], row[1]) for row in rows]
        instrument_ids = [row[1] for row in rows]
        cik_map = {row[0].upper(): row[1] for row in cik_rows}
        from_date = date.today() - timedelta(days=30)
        to_date = date.today()

        total_rows = 0

        # Fundamentals — SEC XBRL (primary, free, US equities)
        sec_symbols = [(sym, iid) for sym, iid in symbols if sym.upper() in cik_map]
        if sec_symbols:
            with (
                SecFundamentalsProvider(user_agent=settings.sec_user_agent) as sec_fund,
                psycopg.connect(settings.database_url) as conn,
            ):
                sec_fund.set_cik_cache(cik_map)
                summary = refresh_fundamentals(sec_fund, conn, sec_symbols)
            total_rows += summary.snapshots_upserted
            logger.info(
                "SEC fundamentals refresh: attempted=%d upserted=%d skipped=%d",
                summary.symbols_attempted,
                summary.snapshots_upserted,
                summary.symbols_skipped,
            )
        else:
            logger.info("daily_research_refresh: no CIK mappings, skipping SEC fundamentals")

        # Fundamentals — FMP (fallback for non-US instruments)
        fmp_symbols = [(sym, iid) for sym, iid in symbols if sym.upper() not in cik_map]
        if settings.fmp_api_key:
            if fmp_symbols:
                with (
                    FmpFundamentalsProvider(api_key=settings.fmp_api_key) as fmp,
                    psycopg.connect(settings.database_url) as conn,
                ):
                    fmp_summary = refresh_fundamentals(fmp, conn, fmp_symbols)
                total_rows += fmp_summary.snapshots_upserted
                logger.info(
                    "FMP fundamentals refresh (non-US fallback): attempted=%d upserted=%d skipped=%d",
                    fmp_summary.symbols_attempted,
                    fmp_summary.snapshots_upserted,
                    fmp_summary.symbols_skipped,
                )
        elif fmp_symbols:
            logger.warning(
                "FMP_API_KEY not set; %d non-US instruments will have no fundamentals",
                len(fmp_symbols),
            )

        # Enrichment — profile, earnings, analyst estimates (FMP)
        if settings.fmp_api_key:
            try:
                with (
                    FmpFundamentalsProvider(api_key=settings.fmp_api_key) as fmp,
                    psycopg.connect(settings.database_url) as conn,
                ):
                    enrich_summary = refresh_enrichment(fmp, conn, symbols)
                    conn.commit()
                total_rows += enrich_summary.profiles_upserted + enrich_summary.earnings_upserted
                logger.info(
                    "Enrichment refresh: attempted=%d profiles=%d earnings=%d estimates=%d skipped=%d",
                    enrich_summary.symbols_attempted,
                    enrich_summary.profiles_upserted,
                    enrich_summary.earnings_upserted,
                    enrich_summary.estimates_upserted,
                    enrich_summary.symbols_skipped,
                )
            except Exception:
                logger.warning("Enrichment refresh failed", exc_info=True)

        # Filings — SEC EDGAR
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
            psycopg.connect(settings.database_url) as conn,
        ):
            sec_summary = refresh_filings(
                provider=sec,
                provider_name="sec",
                identifier_type="cik",
                conn=conn,
                instrument_ids=instrument_ids,
                start_date=from_date,
                end_date=to_date,
                filing_types=["10-K", "10-Q", "8-K"],
            )
        total_rows += sec_summary.filings_upserted
        logger.info(
            "SEC filings refresh: attempted=%d upserted=%d skipped=%d",
            sec_summary.instruments_attempted,
            sec_summary.filings_upserted,
            sec_summary.instruments_skipped,
        )

        # Filings — Companies House
        if settings.companies_house_api_key:
            with (
                CompaniesHouseFilingsProvider(api_key=settings.companies_house_api_key) as ch,
                psycopg.connect(settings.database_url) as conn,
            ):
                ch_summary: FilingsRefreshSummary = refresh_filings(
                    provider=ch,
                    provider_name="companies_house",
                    identifier_type="company_number",
                    conn=conn,
                    instrument_ids=instrument_ids,
                    start_date=from_date,
                    end_date=to_date,
                )
            total_rows += ch_summary.filings_upserted
            logger.info(
                "CH filings refresh: attempted=%d upserted=%d skipped=%d",
                ch_summary.instruments_attempted,
                ch_summary.filings_upserted,
                ch_summary.instruments_skipped,
            )

        tracker.row_count = total_rows


def daily_financial_facts() -> None:
    """Incremental SEC facts refresh driven by the daily master-index
    + per-CIK watermarks. See app.services.sec_incremental."""
    from datetime import UTC, datetime

    with _tracked_job(JOB_DAILY_FINANCIAL_FACTS) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            from app.providers.implementations.sec_edgar import SecFilingsProvider
            from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider
            from app.services.sec_incremental import execute_refresh, plan_refresh

            today = datetime.now(UTC).date()
            with (
                SecFilingsProvider(user_agent=settings.sec_user_agent) as filings,
                SecFundamentalsProvider(user_agent=settings.sec_user_agent) as fundamentals,
            ):
                plan = plan_refresh(conn, filings, today=today)
                logger.info(
                    "daily_financial_facts plan: seeds=%d refreshes=%d submissions_only=%d",
                    len(plan.seeds),
                    len(plan.refreshes),
                    len(plan.submissions_only_advances),
                )
                outcome = execute_refresh(
                    conn,
                    filings_provider=filings,
                    fundamentals_provider=fundamentals,
                    plan=plan,
                )
                logger.info(
                    "daily_financial_facts outcome: seeded=%d refreshed=%d submissions_advanced=%d failed=%d",
                    outcome.seeded,
                    outcome.refreshed,
                    outcome.submissions_advanced,
                    len(outcome.failed),
                )

            touched_ciks = list(plan.seeds) + list(plan.refreshes)
            if outcome.seeded + outcome.refreshed > 0 and touched_ciks:
                # Phase 2: normalization for CIKs we actually touched this run.
                from app.services.financial_normalization import normalize_financial_periods

                cur = conn.execute(
                    """
                    SELECT i.instrument_id
                    FROM instruments i
                    JOIN external_identifiers ei
                        ON ei.instrument_id = i.instrument_id
                        AND ei.provider = 'sec'
                        AND ei.identifier_type = 'cik'
                        AND ei.identifier_value = ANY(%s)
                        AND ei.is_primary = TRUE
                    WHERE i.is_tradable = TRUE
                    """,
                    (touched_ciks,),
                )
                instrument_ids = [row[0] for row in cur.fetchall()]
                if instrument_ids:
                    norm_summary = normalize_financial_periods(conn, instrument_ids)
                    logger.info(
                        "Normalization: %d instruments, %d raw periods, %d canonical",
                        norm_summary.instruments_processed,
                        norm_summary.periods_raw_upserted,
                        norm_summary.periods_canonical_upserted,
                    )
                    tracker.row_count = outcome.seeded + outcome.refreshed + norm_summary.periods_canonical_upserted
                else:
                    tracker.row_count = outcome.seeded + outcome.refreshed
            else:
                # No facts written — use submissions-only advance count
                # so empty days still show non-zero row_count when
                # watermarks advanced.
                tracker.row_count = outcome.submissions_advanced


def daily_news_refresh() -> None:
    """
    Fetch, deduplicate, and score news events for all active Tier 1/2 instruments.

    Runs daily (or on-demand). Idempotent — safe to re-run.
    Requires ANTHROPIC_API_KEY to be set; skips sentiment scoring otherwise.
    """
    if not settings.anthropic_api_key:
        logger.error("daily_news_refresh: ANTHROPIC_API_KEY not set, skipping")
        _record_prereq_skip(JOB_DAILY_NEWS_REFRESH, "anthropic api key missing")
        return

    # No concrete NewsProvider implementation wired in v1. The guard
    # must live OUTSIDE `_tracked_job` — otherwise a naive
    # record_job_skip inside the tracker would produce two job_runs
    # rows for one invocation (skipped + tracked-success). Wire a real
    # provider here once one is available and remove this block.
    logger.warning("daily_news_refresh: no NewsProvider implementation wired in v1 — skipping fetch")
    _record_prereq_skip(JOB_DAILY_NEWS_REFRESH, "news provider not configured")
    return


def daily_thesis_refresh() -> None:
    """
    Regenerate theses for stale Tier 1 instruments.

    An instrument is stale when:
      - it has no thesis row, or
      - its most recent thesis is older than coverage.review_frequency allows.

    Requires ANTHROPIC_API_KEY. Skips silently if not set.
    Each instrument is processed independently — a failure on one does not
    abort the rest of the batch.
    """
    if not settings.anthropic_api_key:
        logger.error("daily_thesis_refresh: ANTHROPIC_API_KEY not set, skipping")
        _record_prereq_skip(JOB_DAILY_THESIS_REFRESH, "anthropic api key missing")
        return

    with _tracked_job(JOB_DAILY_THESIS_REFRESH) as tracker:
        logger.info("daily_thesis_refresh: checking for stale Tier 1/2 instruments")
        # Previously: except Exception: log + return silent-success.
        # That left the layer looking fresh after a DB failure. Now:
        # let the exception propagate — _tracked_job records failure.
        with psycopg.connect(settings.database_url) as conn:
            # Generate theses for T1 and T2 instruments.  T2 instruments
            # need theses to be promoted to T1 (coverage.py requires
            # thesis for T2→T1).  The portfolio manager also requires a
            # thesis with stance="buy" before recommending a BUY.
            stale_t1 = find_stale_instruments(conn, tier=1)
            stale_t2 = find_stale_instruments(conn, tier=2)
            stale = stale_t1 + stale_t2

        if not stale:
            logger.info("daily_thesis_refresh: no stale Tier 1/2 instruments found")
            tracker.row_count = 0
            return

        logger.info(
            "daily_thesis_refresh: %d stale instrument(s) to refresh (T1=%d T2=%d)",
            len(stale),
            len(stale_t1),
            len(stale_t2),
        )

        claude_client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

        generated = 0
        skipped = 0
        total = len(stale)
        for idx, item in enumerate(stale, start=1):
            try:
                with psycopg.connect(settings.database_url) as conn:
                    generate_thesis(
                        instrument_id=item.instrument_id,
                        conn=conn,
                        client=claude_client,
                    )
                generated += 1
            except Exception:
                logger.warning(
                    "daily_thesis_refresh: failed for symbol=%s instrument_id=%d, skipping",
                    item.symbol,
                    item.instrument_id,
                    exc_info=True,
                )
                skipped += 1
            report_progress(idx, total)

        report_progress(total, total, force=True)
        tracker.row_count = generated

    logger.info(
        "daily_thesis_refresh complete: generated=%d skipped=%d",
        generated,
        skipped,
    )


def daily_portfolio_sync() -> None:
    """Sync positions and cash from eToro to local state.

    Runs before morning_candidate_review so recommendations use fresh
    portfolio data. Requires eToro broker credentials; skips gracefully
    if credentials are missing.
    """
    from app.providers.implementations.etoro_broker import EtoroBrokerProvider

    creds = _load_etoro_credentials(JOB_DAILY_PORTFOLIO_SYNC)
    if creds is None:
        _record_prereq_skip(JOB_DAILY_PORTFOLIO_SYNC, "etoro credentials missing")
        return

    api_key, user_key = creds
    with _tracked_job(JOB_DAILY_PORTFOLIO_SYNC) as tracker:
        with EtoroBrokerProvider(
            api_key=api_key,
            user_key=user_key,
            env=settings.etoro_env,
        ) as broker:
            portfolio = broker.get_portfolio()

        with psycopg.connect(settings.database_url) as conn:
            result = sync_portfolio(conn, portfolio)

            # Auto-promote held instruments to Tier 1 so market data,
            # FX rates, and downstream jobs fire for them. Without this,
            # newly synced positions stay at Tier 3 and all jobs skip.
            promoted = _promote_held_to_tier1(conn)
            conn.commit()

        if promoted:
            logger.info("Portfolio sync: auto-promoted %d held instruments to Tier 1", promoted)

        tracker.row_count = (
            result.positions_updated + result.positions_opened_externally + result.positions_closed_externally
        )
        logger.info(
            "Portfolio sync complete: updated=%d opened_ext=%d closed_ext=%d "
            "mirrors_up=%d mirrors_closed=%d mirror_positions_up=%d "
            "broker_cash=%.2f local_cash=%.2f delta=%.2f",
            result.positions_updated,
            result.positions_opened_externally,
            result.positions_closed_externally,
            result.mirrors_upserted,
            result.mirrors_closed,
            result.mirror_positions_upserted,
            result.broker_cash,
            result.local_cash,
            result.cash_delta,
        )


def morning_candidate_review() -> None:
    """
    Re-score, rank, and generate trade recommendations for Tier 1 candidates.

    Steps (run sequentially on the same connection for each phase):
      1. Score all eligible Tier 1 instruments (default model version).
      2. Run portfolio review to produce BUY/ADD/HOLD/EXIT recommendations.

    Each phase opens its own connection so a failure in recommendations
    does not roll back the completed scoring run.
    """
    # Restructured: do not reference `result` outside the `_tracked_job`
    # block. This matches the prevention-log entry "Unbound variable
    # after context-manager exit" added in this same PR — exception
    # paths from compute_morning_recommendations re-raise through
    # _tracked_job before the post-block code, so the pattern was
    # safe-as-written, but referencing inside the block removes the
    # fragility entirely and gives future readers a consistent shape.
    rec_result: Any | None = None
    with _tracked_job(JOB_MORNING_CANDIDATE_REVIEW) as tracker:
        result = compute_morning_recommendations()
        tracker.row_count = len(result.ranking_result.scored) + (
            len(result.review_result.recommendations) if result.review_result is not None else 0
        )
        rec_result = result.review_result  # None or PortfolioReviewResult

    # No-score path: nothing to log further, nothing to execute.
    if rec_result is None:
        return

    logger.info(
        "morning_candidate_review: recommendations=%d (BUY=%d ADD=%d HOLD=%d EXIT=%d) aum=%.2f",
        len(rec_result.recommendations),
        sum(1 for r in rec_result.recommendations if r.action == "BUY"),
        sum(1 for r in rec_result.recommendations if r.action == "ADD"),
        sum(1 for r in rec_result.recommendations if r.action == "HOLD"),
        sum(1 for r in rec_result.recommendations if r.action == "EXIT"),
        rec_result.total_aum,
    )

    # --- Pipeline trigger: if recs were generated, run the execution pipeline ---
    # Gate on kill switch and auto-trading flag before invoking the execution
    # path — the guard inside execute_approved_orders is a second line of
    # defence, not a substitute for checking at the call site.
    #
    # CRITICAL: this side-effect lives ONLY on the legacy scheduled path.
    # The sync orchestrator's morning_candidate_review adapter calls
    # compute_morning_recommendations() directly (no order execution).
    actionable_count = sum(1 for r in rec_result.recommendations if r.action in ("BUY", "ADD", "EXIT"))
    if actionable_count > 0:
        try:
            from app.services.ops_monitor import get_kill_switch_status
            from app.services.runtime_config import get_runtime_config

            with psycopg.connect(settings.database_url) as conn:
                ks = get_kill_switch_status(conn)
                config = get_runtime_config(conn)

            if ks.get("is_active"):
                logger.warning("morning_candidate_review: kill switch active, skipping pipeline trigger")
            elif not config.enable_auto_trading:
                logger.info("morning_candidate_review: auto_trading disabled, skipping pipeline trigger")
            else:
                logger.info(
                    "morning_candidate_review: %d actionable recs → triggering execute_approved_orders",
                    actionable_count,
                )
                execute_approved_orders()
        except Exception:
            logger.error(
                "morning_candidate_review: pipeline trigger to execute_approved_orders failed",
                exc_info=True,
            )


@dataclass(frozen=True)
class MorningComputeResult:
    """Result of compute_morning_recommendations.

    review_result is None when scoring produced no eligible instruments —
    portfolio review is skipped in that case (preserving the legacy
    no-score path). The orchestrator adapter maps None review_result to
    LayerOutcome.NO_WORK for the recommendations layer."""

    ranking_result: Any  # RankingResult — avoid top-level import cycle
    review_result: Any | None  # PortfolioReviewResult | None


def compute_morning_recommendations() -> MorningComputeResult:
    """Run scoring + portfolio review. Does NOT call execute_approved_orders.

    Used by the sync orchestrator's morning_candidate_review adapter,
    which must not trigger order execution as a side effect of a data
    refresh. The legacy `morning_candidate_review` scheduled job retains
    its execute trigger during Phase 1–3; Phase 4 removes that scheduled
    path entirely.

    Opens TWO separate `psycopg.connect()` blocks — one per phase — so a
    recommendation failure cannot roll back the completed scoring run.

    No-score path: if scoring produces an empty `scored` list, portfolio
    review does NOT run and `review_result` is None.
    """
    logger.info("compute_morning_recommendations: starting scoring run")
    with psycopg.connect(settings.database_url) as conn:
        score_result = compute_rankings(conn)

    if not score_result.scored:
        logger.info("compute_morning_recommendations: no eligible instruments to score")
        return MorningComputeResult(ranking_result=score_result, review_result=None)

    top5 = score_result.scored[:5]
    top5_summary = ", ".join(f"instrument_id={r.instrument_id} score={r.total_score:.3f} rank={r.rank}" for r in top5)
    logger.info(
        "compute_morning_recommendations: scored %d instruments [model=%s] top5=[%s]",
        len(score_result.scored),
        score_result.model_version,
        top5_summary,
    )

    logger.info("compute_morning_recommendations: starting portfolio review")
    with psycopg.connect(settings.database_url) as conn:
        rec_result = run_portfolio_review(conn, model_version=score_result.model_version)

    return MorningComputeResult(ranking_result=score_result, review_result=rec_result)


def _timing_error_defer(
    rec_id: int,
    instrument_id: int,
    explanation: str,
) -> bool:
    """Atomically defer a rec that could not be timing-evaluated.

    Writes a decision_audit row + sets status='timing_deferred' with
    timing_verdict='error' in one transaction.  Returns True if the
    commit succeeded, False if the fallback itself failed (rec left as
    'proposed' — logged by caller).
    """
    try:
        with psycopg.connect(settings.database_url) as conn:
            conn.execute(
                """
                INSERT INTO decision_audit
                    (decision_time, instrument_id, recommendation_id,
                     stage, pass_fail, explanation)
                VALUES
                    (NOW(), %(iid)s, %(rid)s,
                     'entry_timing', 'FAIL', %(expl)s)
                """,
                {"iid": instrument_id, "rid": rec_id, "expl": explanation},
            )
            conn.execute(
                """
                UPDATE trade_recommendations
                SET status = 'timing_deferred',
                    timing_verdict = 'error',
                    timing_rationale = %(rationale)s,
                    timing_deferred_at = COALESCE(timing_deferred_at, NOW())
                WHERE recommendation_id = %(rid)s
                """,
                {"rid": rec_id, "rationale": explanation},
            )
            conn.commit()
        return True
    except Exception:
        logger.error(
            "execute_approved_orders: failed to defer rec=%d after timing error",
            rec_id,
            exc_info=True,
        )
        return False


def execute_approved_orders() -> None:
    """Guard and execute actionable trade recommendations.

    Three phases, run sequentially:

    Phase 0 — Entry timing: evaluate TA conditions for BUY/ADD recs.
    If conditions are unfavorable, set status='timing_deferred' and
    write SL/TP + rationale.  If favorable, write SL/TP and leave as
    'proposed'.  EXIT/HOLD recs are untouched.  Inline (not a separate
    scheduled job) to guarantee ordering and eliminate scheduler race.

    Phase 1 — Guard: query all remaining ``proposed`` recommendations.
    For each, call ``evaluate_recommendation`` to run the execution
    guard.  PASS transitions to ``approved``; FAIL transitions to
    ``rejected``.

    Phase 2 — Execute: query all ``approved`` recommendations (includes
    newly approved from phase 1 plus any pre-existing approved rows).
    For each, look up the guard's ``decision_id`` from ``decision_audit``
    and call ``execute_order``.

    Each recommendation gets its own connection so a single failure
    does not roll back others.  The broker provider is opened once for
    the entire run (live mode only).
    """
    from app.providers.implementations.etoro_broker import EtoroBrokerProvider

    with _tracked_job(JOB_EXECUTE_APPROVED_ORDERS) as tracker:
        # --- Phase 0: entry timing — evaluate TA conditions for BUY/ADD ---
        # Inline timing check (not a separate scheduled job) guarantees
        # ordering: timing → guard → execute. Eliminates scheduler race.
        timing_passed = 0
        timing_deferred = 0
        timing_skipped = 0
        timing_candidates: list[tuple[Any, ...]] = []

        try:
            with psycopg.connect(settings.database_url) as conn:
                timing_candidates = conn.execute(
                    """
                    SELECT recommendation_id, action, instrument_id
                    FROM trade_recommendations
                    WHERE status = 'proposed'
                    ORDER BY recommendation_id
                    """,
                ).fetchall()
        except Exception:
            # DB failure fetching timing candidates must not kill Phase 1/2.
            logger.error(
                "execute_approved_orders: timing candidate fetch failed",
                exc_info=True,
            )

        for row in timing_candidates:
            rec_id, action, instrument_id = row[0], row[1], row[2]
            # EXIT/HOLD recs skip timing — never gate protective exits
            # (settled decision). The inner evaluate_entry_conditions also
            # handles this, but skipping here avoids opening a per-rec DB
            # connection for actions that are guaranteed to be skipped.
            if action in ("EXIT", "HOLD"):
                timing_skipped += 1
                continue
            try:
                with psycopg.connect(settings.database_url) as conn:
                    evaluation = evaluate_entry_conditions(conn, rec_id)

                    pass_fail = "DEFER" if evaluation.verdict == "defer" else "PASS"
                    new_status = "timing_deferred" if evaluation.verdict == "defer" else None

                    if evaluation.verdict in ("pass", "defer"):
                        # Atomic: audit row + rec update in one transaction.
                        # If either statement fails, both roll back and the
                        # outer except catches it → _timing_error_defer.
                        with conn.transaction():
                            conn.execute(
                                """
                                INSERT INTO decision_audit
                                    (decision_time, instrument_id, recommendation_id,
                                     stage, pass_fail, explanation, evidence_json)
                                VALUES
                                    (NOW(), %(iid)s, %(rid)s,
                                     'entry_timing', %(pf)s, %(expl)s, %(ev)s)
                                """,
                                {
                                    "iid": instrument_id,
                                    "rid": rec_id,
                                    "pf": pass_fail,
                                    "expl": evaluation.rationale,
                                    "ev": Jsonb(evaluation.condition_details),
                                },
                            )
                            update_params: dict[str, Any] = {
                                "sl": evaluation.stop_loss_rate,
                                "tp": evaluation.take_profit_rate,
                                "verdict": evaluation.verdict,
                                "rationale": evaluation.rationale,
                                "rid": rec_id,
                            }
                            if new_status is not None:
                                conn.execute(
                                    """
                                    UPDATE trade_recommendations
                                    SET status = 'timing_deferred',
                                        stop_loss_rate = %(sl)s,
                                        take_profit_rate = %(tp)s,
                                        timing_verdict = %(verdict)s,
                                        timing_rationale = %(rationale)s,
                                        timing_deferred_at = COALESCE(timing_deferred_at, NOW())
                                    WHERE recommendation_id = %(rid)s
                                    """,
                                    update_params,
                                )
                            else:
                                conn.execute(
                                    """
                                    UPDATE trade_recommendations
                                    SET stop_loss_rate = %(sl)s,
                                        take_profit_rate = %(tp)s,
                                        timing_verdict = %(verdict)s,
                                        timing_rationale = %(rationale)s
                                    WHERE recommendation_id = %(rid)s
                                    """,
                                    update_params,
                                )

                        # Commit the transaction (savepoint released above,
                        # but the outer implicit txn needs an explicit commit).
                        conn.commit()

                        # Counter increment AFTER commit (reached only if
                        # both writes succeeded).
                        if new_status is not None:
                            timing_deferred += 1
                        else:
                            timing_passed += 1

                        logger.info(
                            "execute_approved_orders: timing %s rec=%d rationale=%s",
                            pass_fail,
                            rec_id,
                            evaluation.rationale,
                        )
                    else:
                        # verdict == "skip" for a BUY/ADD rec — should not
                        # happen, but must not let an uninspected rec reach
                        # the guard. Defer it as a safety measure.
                        skip_ok = _timing_error_defer(
                            rec_id,
                            instrument_id,
                            "evaluate_entry_conditions returned 'skip' for BUY/ADD rec — deferred as safety fallback",
                        )
                        if skip_ok:
                            timing_deferred += 1
                        else:
                            # Defer failed — rec stays proposed. Log at
                            # CRITICAL: this is the last safety path before
                            # the execution guard (which is the final gate).
                            logger.critical(
                                "execute_approved_orders: rec=%d could not be deferred after skip verdict; "
                                "rec remains proposed — execution guard is the final safety net",
                                rec_id,
                            )
                            timing_skipped += 1
            except Exception:
                # Timing failure must not let an uninspected BUY/ADD rec
                # reach the guard without SL/TP. Mark as timing_deferred
                # so Phase 1 excludes it.
                logger.error(
                    "execute_approved_orders: timing evaluation failed for rec=%d, deferring",
                    rec_id,
                    exc_info=True,
                )
                deferred_ok = _timing_error_defer(
                    rec_id,
                    instrument_id,
                    "timing evaluation raised an exception; deferred to prevent SL/TP-absent execution",
                )
                if deferred_ok:
                    timing_deferred += 1
                else:
                    timing_skipped += 1

        logger.info(
            "execute_approved_orders: timing phase — candidates=%d passed=%d deferred=%d skipped=%d",
            len(timing_candidates),
            timing_passed,
            timing_deferred,
            timing_skipped,
        )

        # --- Phase 1: guard proposed recommendations ---
        # Re-query proposed recs (timing_deferred ones are now excluded).
        with psycopg.connect(settings.database_url) as conn:
            proposed = conn.execute(
                """
                SELECT recommendation_id
                FROM trade_recommendations
                WHERE status = 'proposed'
                ORDER BY recommendation_id
                """,
            ).fetchall()

        guarded = 0
        rejected = 0
        for row in proposed:
            rec_id = row[0]
            try:
                with psycopg.connect(settings.database_url) as conn:
                    result = evaluate_recommendation(conn, rec_id)
                    conn.commit()
                if result.verdict == "PASS":
                    guarded += 1
                    logger.info(
                        "execute_approved_orders: recommendation_id=%d PASS (decision_id=%d)",
                        rec_id,
                        result.decision_id,
                    )
                else:
                    rejected += 1
                    logger.info(
                        "execute_approved_orders: recommendation_id=%d FAIL rules=%s",
                        rec_id,
                        result.failed_rules,
                    )
            except Exception:
                rejected += 1
                logger.error(
                    "execute_approved_orders: guard failed for recommendation_id=%d",
                    rec_id,
                    exc_info=True,
                )

        logger.info(
            "execute_approved_orders: guard phase complete — proposed=%d approved=%d rejected=%d",
            len(proposed),
            guarded,
            rejected,
        )

        # --- Phase 2: execute approved recommendations ---
        with psycopg.connect(settings.database_url) as conn:
            # Use DISTINCT ON to pick the latest PASS decision per
            # recommendation (in case the guard was run more than once).
            approved = conn.execute(
                """
                SELECT DISTINCT ON (tr.recommendation_id)
                       tr.recommendation_id, da.decision_id
                FROM trade_recommendations tr
                JOIN decision_audit da
                    ON da.recommendation_id = tr.recommendation_id
                   AND da.stage = 'execution_guard'
                   AND da.pass_fail = 'PASS'
                WHERE tr.status = 'approved'
                ORDER BY tr.recommendation_id, da.decision_id DESC
                """,
            ).fetchall()

        if not approved:
            logger.info("execute_approved_orders: no approved recommendations to execute")
            tracker.row_count = guarded + rejected
            return

        # Open the broker provider once for the entire execution phase.
        # When credentials are absent broker stays None — execute_order
        # reads enable_live_trading from runtime_config and generates
        # synthetic fills when live trading is disabled.
        creds = _load_etoro_credentials(JOB_EXECUTE_APPROVED_ORDERS)
        broker: EtoroBrokerProvider | None = None
        broker_ctx: EtoroBrokerProvider | None = None

        if creds is not None:
            api_key, user_key = creds
            broker_ctx = EtoroBrokerProvider(
                api_key=api_key,
                user_key=user_key,
                env=settings.etoro_env,
            )
            broker = broker_ctx.__enter__()

        try:
            executed = 0
            pending = 0
            failed = 0
            for row in approved:
                rec_id, decision_id = row[0], row[1]
                try:
                    with psycopg.connect(settings.database_url) as conn:
                        result = execute_order(
                            conn,
                            recommendation_id=rec_id,
                            decision_id=decision_id,
                            broker=broker,
                        )
                        conn.commit()
                    if result.outcome == "filled":
                        executed += 1
                        logger.info(
                            "execute_approved_orders: recommendation_id=%d executed order_id=%d ref=%s",
                            rec_id,
                            result.order_id,
                            result.broker_order_ref,
                        )
                    elif result.outcome == "pending":
                        pending += 1
                        logger.info(
                            "execute_approved_orders: recommendation_id=%d pending order_id=%d ref=%s",
                            rec_id,
                            result.order_id,
                            result.broker_order_ref,
                        )
                    else:
                        failed += 1
                        logger.warning(
                            "execute_approved_orders: recommendation_id=%d failed order_id=%d explanation=%s",
                            rec_id,
                            result.order_id,
                            result.explanation,
                        )
                except Exception:
                    failed += 1
                    logger.error(
                        "execute_approved_orders: execution failed for recommendation_id=%d",
                        rec_id,
                        exc_info=True,
                    )

            tracker.row_count = (
                timing_passed + timing_deferred + timing_skipped + guarded + rejected + executed + pending + failed
            )

        finally:
            if broker_ctx is not None:
                broker_ctx.__exit__(None, None, None)

    logger.info(
        "execute_approved_orders complete: "
        "timing(passed=%d deferred=%d skipped=%d) "
        "guard(proposed=%d approved=%d rejected=%d) "
        "exec(approved=%d executed=%d pending=%d failed=%d)",
        timing_passed,
        timing_deferred,
        timing_skipped,
        len(proposed),
        guarded,
        rejected,
        len(approved),
        executed,
        pending,
        failed,
    )


def retry_deferred_recommendations_job() -> None:
    """Re-evaluate timing_deferred recommendations hourly.

    Checks kill switch and auto-trading flag before proceeding.
    Deferred recs that now pass timing are transitioned to 'proposed'
    so they enter the next execute_approved_orders cycle.
    """
    from app.services.ops_monitor import get_kill_switch_status
    from app.services.runtime_config import get_runtime_config

    with _tracked_job(JOB_RETRY_DEFERRED) as tracker:
        # Single connection for config check + service call so the kill
        # switch state cannot change between the gate and the work.
        try:
            with psycopg.connect(settings.database_url) as conn:
                # Safety gate: kill switch + auto-trading check
                ks = get_kill_switch_status(conn)
                config = get_runtime_config(conn)

                if ks.get("is_active"):
                    logger.warning("retry_deferred: kill switch active, skipping")
                    tracker.row_count = 0
                    return

                if not config.enable_auto_trading:
                    logger.info("retry_deferred: auto_trading disabled, skipping")
                    tracker.row_count = 0
                    return

                result = retry_deferred_recommendations(conn)
        except Exception:
            logger.error("retry_deferred: service call failed", exc_info=True)
            tracker.row_count = 0
            return

        tracker.row_count = result.retried + result.expired + result.errors
        logger.info(
            "retry_deferred: retried=%d re_proposed=%d re_deferred=%d expired=%d errors=%d",
            result.retried,
            result.re_proposed,
            result.re_deferred,
            result.expired,
            result.errors,
        )


def monitor_positions_job() -> None:
    """Hourly position health check.

    Detects SL/TP breaches and thesis breaks between daily sync cycles.
    Alerts are logged for now; future work may trigger out-of-cycle
    EXIT recommendations or operator notifications.

    Read-only — does not place orders or modify positions.
    """
    with _tracked_job(JOB_MONITOR_POSITIONS) as tracker:
        try:
            with psycopg.connect(settings.database_url) as conn:
                result = check_position_health(conn)
        except Exception:
            logger.error("monitor_positions: health check failed", exc_info=True)
            tracker.row_count = 0
            return

        tracker.row_count = result.positions_checked

        if result.alerts:
            for alert in result.alerts:
                logger.warning(
                    "monitor_positions: ALERT %s on %s (instrument_id=%d): %s",
                    alert.alert_type,
                    alert.symbol,
                    alert.instrument_id,
                    alert.detail,
                )
        else:
            logger.info(
                "monitor_positions: %d positions checked, no alerts",
                result.positions_checked,
            )


def weekly_coverage_review() -> None:
    """
    Review coverage tier assignments; promote/demote instruments.

    Runs weekly. Evaluates all instruments with coverage rows against
    deterministic promotion/demotion rules. Enforces Tier 1 cap.
    All changes are recorded in coverage_audit.
    """
    with _tracked_job(JOB_WEEKLY_COVERAGE_REVIEW) as tracker:
        logger.info("weekly_coverage_review: starting coverage tier review")
        try:
            with psycopg.connect(settings.database_url) as conn:
                result = review_coverage(conn)
        except Exception:
            logger.error("weekly_coverage_review: failed", exc_info=True)
            return

        tracker.row_count = len(result.promotions) + len(result.demotions)

    logger.info(
        "weekly_coverage_review complete: promotions=%d demotions=%d blocked=%d unchanged=%d",
        len(result.promotions),
        len(result.demotions),
        len(result.blocked),
        result.unchanged,
    )


def fx_rates_refresh() -> None:
    """Refresh live FX rates and quotes.

    Two-source strategy:

    1. **Frankfurter (primary FX):** Fetch ECB reference rates for all
       supported display currencies. No API key, no coverage prerequisite.
       Conditional ETag (#275): sends If-None-Match against the last
       persisted ETag. 304 responses are no-ops (ECB only publishes
       once per working day ~16:00 CET, so >95% of 5-min polls are 304).

    2. **eToro quotes (secondary):** Batch-fetch quotes for covered Tier 1/2
       instruments. Extracts eToro-specific conversion rates as a supplement,
       and upserts quotes for hourly freshness. Skips gracefully if eToro
       credentials are missing or the rates endpoint fails.

    Runs hourly at :00 (invoked by orchestrator_high_frequency_sync).
    """
    from app.providers.implementations.frankfurter import fetch_latest_rates_conditional
    from app.services.fx import upsert_live_fx_rate
    from app.services.market_data import compute_spread_pct
    from app.services.runtime_config import SUPPORTED_CURRENCIES

    FX_SOURCE = "frankfurter.latest"
    FX_WATERMARK_KEY = "global"

    with _tracked_job(JOB_FX_RATES_REFRESH) as tracker:
        fx_rows_written = 0
        quotes_updated = 0

        # --- Phase 1: Frankfurter ECB rates (conditional GET) ---
        # Fetch USD → every other supported currency.
        targets = sorted(c for c in SUPPORTED_CURRENCIES if c != "USD")
        try:
            with psycopg.connect(settings.database_url) as conn:
                prior = get_watermark(conn, FX_SOURCE, FX_WATERMARK_KEY)
                if_none_match = prior.watermark if (prior and prior.watermark) else None

            result = fetch_latest_rates_conditional("USD", targets, if_none_match=if_none_match)

            if result is None:
                # 304 — ECB hasn't published a new rate since last fetch.
                logger.info("fx_rates_refresh: Frankfurter 304 Not Modified, skipping upsert")
            else:
                # Use the ECB publication date for quoted_at so freshness
                # checks reflect when the rate was actually set, not when
                # we fetched it (matters on weekends/holidays).
                if result.ecb_date is not None:
                    ecb_quoted_at = datetime.fromisoformat(result.ecb_date).replace(tzinfo=UTC)
                else:
                    ecb_quoted_at = datetime.now(UTC)
                with psycopg.connect(settings.database_url) as conn:
                    # Upsert + watermark advance inside one transaction so
                    # a crash between them can't leave the watermark ahead
                    # of the data (next run would skip unfinished work).
                    with conn.transaction():
                        for (from_ccy, to_ccy), rate in result.rates.items():
                            upsert_live_fx_rate(
                                conn,
                                from_currency=from_ccy,
                                to_currency=to_ccy,
                                rate=rate,
                                quoted_at=ecb_quoted_at,
                            )
                            fx_rows_written += 1
                        # Always advance the watermark on 200 — prefer ETag
                        # (what Frankfurter's server actually validates),
                        # fall back to the ecb_date when ETag is absent
                        # (content-based fingerprint: next run comparing
                        # result.ecb_date against prior.watermark still
                        # detects "same publication"). An empty string is
                        # the last-ditch sentinel meaning "no validator
                        # available" — next run's truthy check skips
                        # If-None-Match altogether.
                        set_watermark(
                            conn,
                            source=FX_SOURCE,
                            key=FX_WATERMARK_KEY,
                            watermark=result.etag or result.ecb_date or "",
                        )
                logger.info(
                    "fx_rates_refresh: Frankfurter ECB rates written: %d pairs (date=%s, etag=%s)",
                    fx_rows_written,
                    result.ecb_date,
                    result.etag,
                )
        except Exception:
            logger.warning("fx_rates_refresh: Frankfurter fetch failed, continuing with eToro fallback", exc_info=True)

        # --- Phase 2: eToro quotes + conversion rates (best-effort) ---
        creds = _load_etoro_credentials("fx_rates_refresh")
        if creds is not None:
            api_key, user_key = creds
            try:
                with (
                    EtoroMarketDataProvider(api_key=api_key, user_key=user_key, env=settings.etoro_env) as provider,
                    psycopg.connect(settings.database_url) as conn,
                ):
                    rows = conn.execute(
                        """
                        SELECT i.instrument_id, i.symbol, i.currency
                        FROM instruments i
                        JOIN coverage c ON c.instrument_id = i.instrument_id
                        WHERE i.is_tradable = TRUE
                          AND c.coverage_tier IN (1, 2)
                        ORDER BY i.symbol
                        """
                    ).fetchall()

                    if rows:
                        instrument_ids = [row[0] for row in rows]
                        currency_map: dict[int, str | None] = {row[0]: row[2] for row in rows}

                        quotes = provider.get_quotes(instrument_ids)

                        # Extract eToro-specific conversion rates as a supplement.
                        fx_pairs: dict[str, tuple[Decimal, datetime]] = {}
                        for q in quotes:
                            if q.conversion_rate is None:
                                continue
                            ccy = currency_map.get(q.instrument_id)
                            if ccy is None or ccy == "USD":
                                continue
                            existing = fx_pairs.get(ccy)
                            if existing is None or q.timestamp > existing[1]:
                                fx_pairs[ccy] = (q.conversion_rate, q.timestamp)

                        with conn.transaction():
                            for ccy, (rate, ts) in fx_pairs.items():
                                upsert_live_fx_rate(
                                    conn,
                                    from_currency=ccy,
                                    to_currency="USD",
                                    rate=rate,
                                    quoted_at=ts,
                                )
                                fx_rows_written += 1
                        conn.commit()

                        # Upsert quotes for hourly freshness.
                        max_spread_pct = Decimal("1.0")
                        for q in quotes:
                            try:
                                spread_pct = compute_spread_pct(q.bid, q.ask)
                                spread_flag = spread_pct is not None and spread_pct > max_spread_pct
                                with conn.transaction():
                                    conn.execute(
                                        """
                                        INSERT INTO quotes (
                                            instrument_id, quoted_at, bid, ask, last,
                                            spread_pct, spread_flag
                                        )
                                        VALUES (
                                            %(instrument_id)s, %(quoted_at)s, %(bid)s, %(ask)s,
                                            %(last)s, %(spread_pct)s, %(spread_flag)s
                                        )
                                        ON CONFLICT (instrument_id) DO UPDATE SET
                                            quoted_at   = EXCLUDED.quoted_at,
                                            bid         = EXCLUDED.bid,
                                            ask         = EXCLUDED.ask,
                                            last        = EXCLUDED.last,
                                            spread_pct  = EXCLUDED.spread_pct,
                                            spread_flag = EXCLUDED.spread_flag
                                        """,
                                        {
                                            "instrument_id": q.instrument_id,
                                            "quoted_at": q.timestamp,
                                            "bid": q.bid,
                                            "ask": q.ask,
                                            "last": q.last,
                                            "spread_pct": spread_pct,
                                            "spread_flag": spread_flag,
                                        },
                                    )
                                    quotes_updated += 1
                            except Exception:
                                logger.warning(
                                    "fx_rates_refresh: failed to upsert quote for instrument %d",
                                    q.instrument_id,
                                    exc_info=True,
                                )
                        conn.commit()

                        if quotes_updated == 0:
                            logger.warning(
                                "fx_rates_refresh: 0 quotes written for %d covered instruments"
                                " — quote staleness will degrade mark-to-market valuations",
                                len(instrument_ids),
                            )
                    else:
                        logger.info("fx_rates_refresh: no covered instruments for eToro quotes")
            except Exception:
                logger.warning("fx_rates_refresh: eToro quote fetch failed", exc_info=True)

        tracker.row_count = fx_rows_written + quotes_updated

    logger.info(
        "fx_rates_refresh complete: fx_pairs=%d quotes=%d",
        fx_rows_written,
        quotes_updated,
    )


def daily_tax_reconciliation() -> None:
    """
    Ingest new fills into tax_lots and re-run disposal matching.

    Runs daily. Idempotent — safe to re-run. Requires fx_rates to be
    populated for any non-GBP instruments before ingestion.

    Two separate connections are used intentionally so that a matching
    failure does not roll back committed tax_lot ingestion. If the
    process crashes between ingestion and matching, disposal_matches
    will be stale until the next run — acceptable because matching is
    a full delete-and-recompute and will self-correct on re-run.
    """
    with _tracked_job(JOB_DAILY_TAX_RECONCILIATION) as tracker:
        logger.info("daily_tax_reconciliation: starting")

        try:
            with psycopg.connect(settings.database_url) as conn:
                ingestion = ingest_tax_events(conn)
        except Exception:
            logger.error("daily_tax_reconciliation: ingestion failed", exc_info=True)
            return

        logger.info(
            "daily_tax_reconciliation: ingested fills=%d cash_events=%d already_present=%d",
            ingestion.fills_ingested,
            ingestion.cash_events_ingested,
            ingestion.already_present,
        )

        try:
            with psycopg.connect(settings.database_url) as conn:
                matching = run_disposal_matching(conn)
        except Exception:
            logger.error("daily_tax_reconciliation: matching failed", exc_info=True)
            return

        tracker.row_count = ingestion.fills_ingested + matching.matches_created

    logger.info(
        "daily_tax_reconciliation complete: instruments=%d matches=%d gain=%.2f loss=%.2f",
        matching.instruments_processed,
        matching.matches_created,
        matching.total_gain_gbp,
        matching.total_loss_gbp,
    )


def attribution_summary_job() -> None:
    """Compute and persist attribution summaries for all configured windows."""
    with _tracked_job(JOB_ATTRIBUTION_SUMMARY) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            total_positions = 0
            for window in SUMMARY_WINDOWS:
                summary = compute_attribution_summary(conn, window)
                persist_attribution_summary(conn, summary)
                total_positions = max(total_positions, summary.positions_attributed)
                logger.info(
                    "attribution_summary: window=%dd positions=%d avg_alpha=%.4f",
                    window,
                    summary.positions_attributed,
                    float(summary.avg_model_alpha_pct or 0),
                )
            conn.commit()
            tracker.row_count = total_positions


def weekly_report() -> None:
    """Generate and persist the weekly performance report."""
    from app.services.reporting import generate_weekly_report, persist_report_snapshot

    with _tracked_job(JOB_WEEKLY_REPORT) as tracker:
        # Period: previous Monday through Sunday
        today = datetime.now(tz=UTC).date()
        # Saturday run → report covers Mon–Sun of the week just ended
        period_end = today - timedelta(days=(today.weekday() + 1) % 7)  # last Sunday
        period_start = period_end - timedelta(days=6)  # Monday of that week

        with psycopg.connect(settings.database_url) as conn:
            report = generate_weekly_report(conn, period_start, period_end)
            persist_report_snapshot(
                conn,
                report_type="weekly",
                period_start=period_start,
                period_end=period_end,
                snapshot=report,
            )
            conn.commit()
        tracker.row_count = 1


def monthly_report() -> None:
    """Generate and persist the monthly performance report."""
    from app.services.reporting import generate_monthly_report, persist_report_snapshot

    with _tracked_job(JOB_MONTHLY_REPORT) as tracker:
        # Period: previous full calendar month
        today = datetime.now(tz=UTC).date()
        period_end = today.replace(day=1) - timedelta(days=1)  # last day of prev month
        period_start = period_end.replace(day=1)  # first day of prev month

        with psycopg.connect(settings.database_url) as conn:
            report = generate_monthly_report(conn, period_start, period_end)
            persist_report_snapshot(
                conn,
                report_type="monthly",
                period_start=period_start,
                period_end=period_end,
                snapshot=report,
            )
            conn.commit()
        tracker.row_count = 1


def seed_cost_models() -> None:
    """Scheduled job: refresh cost_model rows from current quotes."""
    from app.services.transaction_cost import seed_cost_models_from_quotes

    with _tracked_job(JOB_SEED_COST_MODELS) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            result = seed_cost_models_from_quotes(conn)
            conn.commit()
        tracker.row_count = result["processed"]
        logger.info(
            "seed_cost_models: processed=%d skipped=%d",
            result["processed"],
            result["skipped"],
        )


def orchestrator_full_sync() -> None:
    """Scheduled job: full DAG sync via the orchestrator.

    Replaces 12 legacy cron triggers removed in Phase 4. Runs
    `run_sync(FULL, trigger='scheduled')` which plans, executes, and
    finalizes synchronously in this worker thread. Any layer failure
    is recorded in `sync_runs` / `sync_layer_progress`; the
    `_safe_run_and_finalize` wrapper ensures the partial unique index
    gate always releases, even on crash.
    """
    from app.services.sync_orchestrator import SyncScope, run_sync

    logger.info("orchestrator_full_sync: starting")
    result = run_sync(SyncScope.full(), trigger="scheduled")
    logger.info(
        "orchestrator_full_sync complete: sync_run_id=%d outcomes=%d",
        result.sync_run_id,
        len(result.outcomes),
    )


def orchestrator_high_frequency_sync() -> None:
    """Scheduled job: refresh portfolio_sync + fx_rates every 5 minutes
    via the orchestrator. Runs `run_sync(HIGH_FREQUENCY, trigger='scheduled')`.

    The orchestrator's partial unique index gate ensures this cannot
    overlap with a still-running FULL sync — it returns early via
    SyncAlreadyRunning, which this wrapper catches and logs.
    """
    from app.services.sync_orchestrator import (
        SyncAlreadyRunning,
        SyncScope,
        run_sync,
    )

    try:
        result = run_sync(SyncScope.high_frequency(), trigger="scheduled")
        logger.info(
            "orchestrator_high_frequency_sync complete: sync_run_id=%d outcomes=%d",
            result.sync_run_id,
            len(result.outcomes),
        )
    except SyncAlreadyRunning as exc:
        logger.info(
            "orchestrator_high_frequency_sync skipped: sync %s already running",
            exc.active_sync_run_id,
        )
