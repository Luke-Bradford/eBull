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
from collections.abc import Callable, Generator, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal

import anthropic
import psycopg
import psycopg.rows
import psycopg.sql
from psycopg.types.json import Jsonb

from app.config import settings
from app.jobs.sources import Lane
from app.providers.implementations.companies_house import CompaniesHouseFilingsProvider
from app.providers.implementations.etoro import EtoroMarketDataProvider
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider
from app.services.broker_credentials import CredentialNotFound, load_credential_for_provider_use
from app.services.coverage import bootstrap_missing_coverage_rows, review_coverage, seed_coverage
from app.services.deferred_retry import retry_deferred_recommendations
from app.services.entry_timing import evaluate_entry_conditions
from app.services.etoro_lookups import refresh_etoro_lookups
from app.services.exchanges import refresh_exchanges_metadata
from app.services.execution_guard import evaluate_recommendation
from app.services.filings import FilingsRefreshSummary, refresh_filings, upsert_cik_mapping
from app.services.fundamentals import refresh_fundamentals
from app.services.market_data import refresh_market_data
from app.services.mf_directory import refresh_mf_directory
from app.services.operators import AmbiguousOperatorError, NoOperatorError, sole_operator_id
from app.services.ops_monitor import (
    record_job_finish,
    record_job_skip,
    record_job_start,
)
from app.services.order_client import execute_order
from app.services.portfolio import run_portfolio_review
from app.services.portfolio_sync import sync_portfolio
from app.services.position_monitor import (
    check_position_health,
    persist_position_alerts,
)
from app.services.processes.param_metadata import ParamMetadata
from app.services.refresh_cascade import (
    demote_to_rerank_needed,
    instrument_lock,
)
from app.services.return_attribution import (
    SUMMARY_WINDOWS,
    compute_attribution_summary,
    persist_attribution_summary,
)
from app.services.scoring import compute_rankings
from app.services.sync_orchestrator import prereq_skip_reason
from app.services.sync_orchestrator.progress import report_progress
from app.services.sync_orchestrator.row_count_spikes import check_row_count_spike
from app.services.tax_ledger import ingest_tax_events, run_disposal_matching
from app.services.thesis import find_stale_instruments, generate_thesis
from app.services.universe import sync_universe
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
    # PR1a #1064 — source-level JobLock bucket. Operator-locked decision:
    # same-source jobs serialise; cross-source run parallel. Required.
    # See app/jobs/sources.py::Lane for the bucket vocabulary and
    # docs/wiki/job-registry-audit.md §1 for the per-job source mapping.
    source: Lane
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
    # PR1a #1064 — operator-exposable parameter surface. Empty tuple =
    # no operator-tunable params. Populated per audit doc §2; PR1b's
    # validate_job_params reads this; PR2's FE Advanced disclosure
    # renders one form field per entry. Consumed via the deferred
    # ParamMetadata import to avoid an import cycle (param_metadata
    # itself doesn't import scheduler).
    params_metadata: tuple[ParamMetadata, ...] = ()
    # PR1a #1064 — operator-facing label. PR4 (#1082) renders this in
    # the admin ProcessesTable next to the ⓘ tooltip; populated now to
    # avoid a separate registry-touching pass later. ``None`` falls
    # back to ``name`` at render time.
    display_name: str | None = None


# Job-name constants. Every ``_tracked_job(...)`` call site below references
# one of these so the literal cannot drift from the registry / job_runs row.
JOB_NIGHTLY_UNIVERSE_SYNC = "nightly_universe_sync"
JOB_DAILY_CANDLE_REFRESH = "daily_candle_refresh"
JOB_DAILY_CIK_REFRESH = "daily_cik_refresh"
JOB_DAILY_RESEARCH_REFRESH = "daily_research_refresh"
JOB_DAILY_NEWS_REFRESH = "daily_news_refresh"
JOB_DAILY_THESIS_REFRESH = "daily_thesis_refresh"
JOB_MORNING_CANDIDATE_REVIEW = "morning_candidate_review"
JOB_DAILY_TAX_RECONCILIATION = "daily_tax_reconciliation"
JOB_DAILY_PORTFOLIO_SYNC = "daily_portfolio_sync"
JOB_EXECUTE_APPROVED_ORDERS = "execute_approved_orders"
JOB_FX_RATES_REFRESH = "fx_rates_refresh"
JOB_RETRY_DEFERRED = "retry_deferred_recommendations"
JOB_MONITOR_POSITIONS = "monitor_positions"
JOB_ATTRIBUTION_SUMMARY = "attribution_summary"
JOB_WEEKLY_REPORT = "weekly_report"
# JOB_WEEKLY_COVERAGE_AUDIT + JOB_WEEKLY_COVERAGE_REVIEW retired in Chunk 2 of
# the 2026-04-19 research-tool refocus; their work is now part of
# JOB_FUNDAMENTALS_SYNC. See docs/superpowers/specs/2026-04-19-research-tool-refocus.md.
JOB_MONTHLY_REPORT = "monthly_report"
JOB_SEED_COST_MODELS = "seed_cost_models"
JOB_DAILY_FINANCIAL_FACTS = "daily_financial_facts"
JOB_RAW_DATA_RETENTION_SWEEP = "raw_data_retention_sweep"
JOB_ORCHESTRATOR_FULL_SYNC = "orchestrator_full_sync"
JOB_ORCHESTRATOR_HIGH_FREQUENCY_SYNC = "orchestrator_high_frequency_sync"
JOB_FUNDAMENTALS_SYNC = "fundamentals_sync"
JOB_SEC_BUSINESS_SUMMARY_BOOTSTRAP = "sec_business_summary_bootstrap"
JOB_SEC_INSIDER_TRANSACTIONS_INGEST = "sec_insider_transactions_ingest"
JOB_SEC_INSIDER_TRANSACTIONS_BACKFILL = "sec_insider_transactions_backfill"
JOB_SEC_FORM3_INGEST = "sec_form3_ingest"
JOB_SEC_DEF14A_INGEST = "sec_def14a_ingest"
JOB_SEC_DEF14A_BOOTSTRAP = "sec_def14a_bootstrap"
JOB_SEC_8K_EVENTS_INGEST = "sec_8k_events_ingest"
JOB_SEC_FILING_DOCUMENTS_INGEST = "sec_filing_documents_ingest"
JOB_CUSIP_EXTID_SWEEP = "cusip_extid_sweep"
JOB_OWNERSHIP_OBSERVATIONS_SYNC = "ownership_observations_sync"
JOB_OWNERSHIP_OBSERVATIONS_BACKFILL = "ownership_observations_backfill"
JOB_SEC_13F_FILER_DIRECTORY_SYNC = "sec_13f_filer_directory_sync"
JOB_SEC_13F_QUARTERLY_SWEEP = "sec_13f_quarterly_sweep"
JOB_SEC_NPORT_FILER_DIRECTORY_SYNC = "sec_nport_filer_directory_sync"
JOB_SEC_N_PORT_INGEST = "sec_n_port_ingest"
JOB_CUSIP_UNIVERSE_BACKFILL = "cusip_universe_backfill"
JOB_EXCHANGES_METADATA_REFRESH = "exchanges_metadata_refresh"
JOB_ETORO_LOOKUPS_REFRESH = "etoro_lookups_refresh"
# #873 — manifest worker tick. Drains pending + retryable
# ``sec_filing_manifest`` rows by dispatching the registered parser
# for each row's source. Cadence is frequent (every 5 min) so atom-
# discovered + per-CIK-polled new accessions are parsed promptly.
JOB_SEC_MANIFEST_WORKER = "sec_manifest_worker"
# #1131 — one-shot sweep that tombstones manifest rows stuck in
# ``failed`` with a pre-#1131-shape ``upsert error:...`` message older
# than 24h. Stops the legacy retry loop that hammered SEC every hour
# for deterministic constraint violations on Form 4 / 8-K / 13D/G /
# DEF 14A ingest. Operator-triggered + auto-fires once per day until
# the legacy backlog drains.
JOB_SEC_MANIFEST_TOMBSTONE_STALE = "sec_manifest_tombstone_stale"

# PR1c #1064 — promoted from bespoke wrappers in
# ``app/services/bootstrap_orchestrator.py``. Each is now a registered
# ``JobInvoker`` (params-aware) so bootstrap dispatch and operator
# manual-trigger share a single body. The bootstrap stage spec
# carries the per-stage hardcoded values via ``StageSpec.params``.
JOB_FILINGS_HISTORY_SEED = "filings_history_seed"
JOB_SEC_FIRST_INSTALL_DRAIN = "sec_first_install_drain"

# #1155 — Layer 1 / 2 / 3 freshness redesign wiring + sec_rebuild
# manual triage. Implementation entrypoints existed since #867/#868/#870
# (Layer wrappers) and the #863-#873 spec (sec_rebuild) but were never
# registered with _INVOKERS or SCHEDULED_JOBS until this PR.
JOB_SEC_ATOM_FAST_LANE = "sec_atom_fast_lane"
JOB_SEC_DAILY_INDEX_RECONCILE = "sec_daily_index_reconcile"
JOB_SEC_PER_CIK_POLL = "sec_per_cik_poll"
JOB_SEC_REBUILD = "sec_rebuild"


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

    Must include the #268 analysability gate — coverage.filings_status =
    'analysable' — to stay in lockstep with compute_rankings. Otherwise
    the prerequisite could report "scoreable" while the downstream query
    filters every candidate out, producing a confusing empty-scoring run.
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
                  AND c.filings_status = 'analysable'
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


def _all_of(*prereqs: PrerequisiteFn) -> PrerequisiteFn:
    """Compose multiple prerequisite checks into a single one.

    Returns ``(True, "")`` only if every wrapped check returns
    ``(True, "")``. The first failing check's reason is returned
    so the operator sees a deterministic explanation rather than a
    concatenation that mutates with check order.
    """

    def composed(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
        for check in prereqs:
            met, reason = check(conn)
            if not met:
                return (False, reason)
        return (True, "")

    return composed


def _bootstrap_complete(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
    """True if first-install bootstrap has finalised in the ``complete`` state.

    Spec: docs/superpowers/specs/2026-05-07-first-install-bootstrap.md.

    Returns ``(False, ...)`` while bootstrap is ``pending``,
    ``running``, or ``partial_error`` so dependent SEC / fundamentals
    / orchestrator-DAG jobs stay quiet against an empty / half-populated
    DB. Operator releases the gate by either:

      1. Running the bootstrap from the admin panel until every stage
         is ``success``, OR
      2. Pressing "Mark complete" after manually fixing the cause of
         a stage failure.

    Per #719 the connection is opened by the caller (catch-up /
    scheduled-fire path) and closed after the check.
    """
    if _exists(
        conn,
        psycopg.sql.SQL("SELECT EXISTS(SELECT 1 FROM bootstrap_state WHERE id = 1 AND status = 'complete')"),
    ):
        return (True, "")
    return (False, "first-install bootstrap not complete; visit /admin to run")


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
        display_name="Orchestrator full sync",
        source="db",
        description="Orchestrator full sync — walks the DAG and refreshes stale layers.",
        cadence=Cadence.daily(hour=3, minute=0),
        # Never catch up on boot. A full sync runs ~45min (research refresh
        # dominates) and holds DB connections the HTTP layer needs. Every
        # dev-stack restart would otherwise fire a catch-up and wedge the
        # site until it finishes. If the 03:00 UTC slot is missed, the
        # operator can click "Sync now" in the admin UI.
        catch_up_on_boot=False,
        # #996 — gated until first-install bootstrap is complete. The
        # full-sync DAG walk fires every credential-using and
        # filings-using layer; against an empty DB those layers
        # produce a flood of misleading "instruments=0" log lines.
        prerequisite=_bootstrap_complete,
    ),
    # Every-5-minutes refresh of independent high-frequency layers
    # (portfolio_sync + fx_rates). The orchestrator's partial unique
    # index gate ensures this cannot overlap with a still-running FULL
    # sync — the wrapper catches SyncAlreadyRunning and logs.
    ScheduledJob(
        name=JOB_ORCHESTRATOR_HIGH_FREQUENCY_SYNC,
        display_name="Orchestrator high-frequency sync",
        source="db",
        description="Orchestrator high-frequency sync — portfolio_sync + fx_rates every 5 minutes.",
        cadence=Cadence.every_n_minutes(interval=5),
        catch_up_on_boot=False,
    ),
    # -- Outside-DAG jobs (5 kept on their own cron triggers) ------------
    # These have empty JOB_TO_LAYERS entries and remain independently
    # scheduled; they do not participate in the orchestrator DAG.
    ScheduledJob(
        name=JOB_EXECUTE_APPROVED_ORDERS,
        display_name="Execute approved orders",
        source="etoro",
        description="Guard and execute actionable trade recommendations.",
        cadence=Cadence.daily(hour=6, minute=30),
        prerequisite=_has_actionable_recommendations,
        # Do not fire on cold boot — order execution must only happen at
        # the scheduled time, not as a surprise catch-up hours later.
        catch_up_on_boot=False,
    ),
    ScheduledJob(
        name=JOB_RETRY_DEFERRED,
        display_name="Retry deferred recommendations",
        source="db",
        description="Re-evaluate timing_deferred recommendations with fresh TA data.",
        cadence=Cadence.hourly(minute=30),
        prerequisite=_has_deferred_recommendations,
        catch_up_on_boot=False,
    ),
    ScheduledJob(
        name=JOB_MONITOR_POSITIONS,
        display_name="Monitor open positions",
        source="db",
        description="Check open positions for SL/TP breaches and thesis breaks.",
        cadence=Cadence.hourly(minute=15),
        prerequisite=_has_open_positions,
        catch_up_on_boot=False,
    ),
    ScheduledJob(
        name=JOB_FUNDAMENTALS_SYNC,
        display_name="Fundamentals research refresh",
        source="db",
        description=(
            "Daily fundamentals research refresh: re-classify every "
            "tradable instrument's coverage.filings_status, backfill "
            "eligible instruments via SEC EDGAR, then re-evaluate "
            "coverage tier promote/demote rules. Collapses the previous "
            "weekly_coverage_audit + weekly_coverage_review pair into a "
            "single job per the 2026-04-19 research-tool refocus. "
            "Cadence 02:30 UTC lands ~30 min after SEC's nightly XBRL "
            "publish window (~22:00 ET / 02:00 UTC) so new filings are "
            "picked up the same night rather than up to seven days "
            "later (#414)."
        ),
        cadence=Cadence.daily(hour=2, minute=30),
        # #996 — compose with bootstrap gate. ``_bootstrap_complete``
        # is strictly stronger than ``_has_any_coverage`` (bootstrap
        # finalises only after universe sync + Tier 3 coverage seed),
        # but keeping the original check as a defense-in-depth means
        # an operator who forces ``mark-complete`` on an empty DB
        # still doesn't get a fundamentals run that returns
        # ``instruments_attempted=0``.
        prerequisite=_all_of(_bootstrap_complete, _has_any_coverage),
        # Never catch up on boot. The job pulls SEC EDGAR data for every
        # covered CIK (tens of minutes, holds DB-pool workers and hits
        # SEC's 10 rps cap). Every dev-stack restart would otherwise
        # fire a catch-up and make the site unresponsive until it
        # finishes. A missed 02:30 run rolls forward to the next day —
        # the incremental watermark design (#420) picks up skipped
        # filings on the following run.
        catch_up_on_boot=False,
    ),
    # attribution_summary retired from scheduling in Phase 1.4 of the
    # 2026-04-19 research-tool refocus — no UI consumer today. The
    # function body stays in scheduler.py + _INVOKERS so the operator
    # can still manually fire it from Admin "Run now" if needed.
    # `sec_dividend_calendar_ingest` retired post-#1155 (#1166):
    # Layer 1/2/3 + sec_manifest_worker + manifest_parsers/eight_k.py
    # (#1158, PR #1166) carry every 8-K dividend extraction. The
    # legacy daily 03:00 UTC cron + its `ingest_dividend_events`
    # service path is full-deleted — `sec_rebuild` covers operator
    # manual backfill via re-pending 8-K manifest rows.
    # `sec_business_summary_ingest` retired post-#1155: Layer 1/2/3
    # discovery + `sec_manifest_worker` + `manifest_parsers/sec_10k.py`
    # (#1152) carry every 10-K Item 1 write to `instrument_business_summary`
    # + `instrument_business_summary_sections`. The weekly Sunday safety-net
    # bootstrap (`sec_business_summary_bootstrap` below) stays for
    # one-shot drain + operator manual backfill.
    # `sec_insider_transactions_ingest` retired from SCHEDULED_JOBS
    # post-#1155: Layer 1/2/3 + sec_manifest_worker + manifest_parsers/
    # insider_345.py (#1130) carry every Form 4 write to
    # insider_transactions + insider_filings + insider_filers. The
    # round-robin `sec_insider_transactions_backfill` cron stays
    # scheduled for the deep-historical-tail drain. Function body +
    # _INVOKERS entry preserved for Admin "Run now".
    ScheduledJob(
        name=JOB_SEC_FILING_DOCUMENTS_INGEST,
        display_name="SEC filing-documents manifest ingest",
        source="sec_rate",
        description=(
            "Parse SEC filing-index JSON (``{accession}-index.json``) "
            "into the filing_documents manifest table (#452). Captures "
            "every document in every filing (primary + exhibits + "
            "XBRL + graphics) as structured SQL rows so the long-tail "
            "disk dump under data/raw/sec/sec_filing_*.json can be "
            "retired. Bounded to 500 filings per run."
        ),
        cadence=Cadence.hourly(minute=35),
        catch_up_on_boot=False,
        prerequisite=_bootstrap_complete,  # #996 — gated until first-install bootstrap is complete
    ),
    # `sec_8k_events_ingest` retired from SCHEDULED_JOBS post-#1155:
    # Layer 1/2/3 + sec_manifest_worker + manifest_parsers/eight_k.py
    # (#1126) carry every 8-K write to eight_k_filings / eight_k_items
    # / eight_k_exhibits + (#1158, PR #1166) dividend_events. Function
    # body + _INVOKERS entry preserved — bootstrap stage 20 dispatches
    # this job_name via _INVOKERS, plus sweep-adapter sec_8k_sweep +
    # Admin "Run now" remain operator-callable.
    ScheduledJob(
        name=JOB_SEC_BUSINESS_SUMMARY_BOOTSTRAP,
        display_name="SEC business-summary bootstrap drain",
        source="sec_rate",
        description=(
            "One-shot drain of the 10-K Item 1 candidate set (#535). "
            "Loops the standard ingester at a higher chunk limit "
            "until the queue empties or the deadline (1 hour) elapses. "
            "Designed for first-time backfill of the SEC-CIK universe "
            "and operator-driven catch-up. Manual trigger via "
            "POST /jobs/sec_business_summary_bootstrap/run; auto-fires "
            "weekly Sunday 04:00 UTC as a safety net."
        ),
        cadence=Cadence.weekly(weekday=6, hour=4, minute=0),
        catch_up_on_boot=False,
        prerequisite=_bootstrap_complete,  # #996 — gated until first-install bootstrap is complete
    ),
    ScheduledJob(
        name=JOB_SEC_INSIDER_TRANSACTIONS_BACKFILL,
        display_name="SEC Form 4 round-robin backfill",
        source="sec_rate",
        description=(
            "Round-robin backfill of Form 4 filings for instruments "
            "with deep historical backlogs (#456). Complements the "
            "hourly universe-wide ingester: that job runs newest-first "
            "across every ticker, so a specific instrument with 400+ "
            "pending Form 4s can starve for days. This job picks the "
            "25 instruments with the most un-ingested candidates and "
            "clears up to 50 per instrument per run, oldest-first, so "
            "the historical tail drains predictably."
        ),
        cadence=Cadence.hourly(minute=45),
        catch_up_on_boot=False,
        prerequisite=_bootstrap_complete,  # #996 — gated until first-install bootstrap is complete
    ),
    # `sec_form3_ingest` retired from SCHEDULED_JOBS post-#1155:
    # Layer 1/2/3 + sec_manifest_worker + manifest_parsers/insider_345.py
    # (#1130) carry every Form 3 write to insider_initial_holdings.
    # Function body + _INVOKERS entry preserved — bootstrap stage 19
    # (sec_form3_ingest) dispatches via _INVOKERS[job_name], plus
    # sweep-adapter sec_form3_sweep + Admin "Run now" remain
    # operator-callable.
    # `sec_def14a_ingest` retired from SCHEDULED_JOBS post-#1155:
    # Layer 1/2/3 discovery + `sec_manifest_worker` + `manifest_parsers/
    # def14a.py` (#1128) carry every DEF 14A write to
    # `def14a_beneficial_holdings`. Weekly `sec_def14a_bootstrap` Sunday
    # safety-net stays for one-shot drain. Function body + `_INVOKERS`
    # entry + sweep-adapter `sec_def14a_sweep` kept so Admin "Run now"
    # + the per-source sweep UI remain operator-callable.
    ScheduledJob(
        name=JOB_OWNERSHIP_OBSERVATIONS_SYNC,
        display_name="Ownership repair sweep",
        source="db",
        description=(
            "Self-healing repair sweep for ownership_*_current (#892). "
            "Live ingesters now write observations + refresh _current "
            "inline (#888-#891 873.A-D), so this job is a safety net: "
            "scans for drift between _current.refreshed_at and "
            "max(observations.ingested_at), refreshes drifted "
            "instruments. On a healthy install: zero rows, <100ms. "
            "Cadence: daily 03:30 UTC."
        ),
        cadence=Cadence.daily(hour=3, minute=30),
        catch_up_on_boot=True,
        prerequisite=_bootstrap_complete,  # #996 — gated until first-install bootstrap is complete
    ),
    ScheduledJob(
        name=JOB_OWNERSHIP_OBSERVATIONS_BACKFILL,
        display_name="Legacy → observations backfill",
        source="db",
        description=(
            "One-shot legacy → ownership_*_observations backfill (#909). "
            "Phase 1 write-through (#888-#891) only fires on new "
            "ingestion; the historical rows already in legacy typed "
            "tables (insider_filings, institutional_holdings, "
            "blockholder_filings, fundamentals.treasury_shares, "
            "def14a_beneficial_holdings) never went through write-"
            "through, so observations + _current stay empty until this "
            "runs. Calls ownership_observations_sync.sync_all with no "
            "since/limit, which is idempotent on the natural keys "
            "(ON CONFLICT DO UPDATE). Operator-triggered via "
            "POST /jobs/ownership_observations_backfill/run; auto-fires "
            "weekly Sunday 03:00 UTC as a safety net so a fresh clone "
            "self-heals without a manual trigger. The 03:00 slot lands "
            "30 min after ``sec_def14a_bootstrap`` (Sun 02:30) finishes "
            "and 30 min before the daily ``ownership_observations_sync`` "
            "repair sweep (03:30) — that ordering means observations "
            "land first, the repair sweep then sees zero drift, and the "
            "two windows never overlap. Once the legacy tables are "
            "dropped post-#905, this job can also be retired."
        ),
        cadence=Cadence.weekly(weekday=6, hour=3, minute=0),
        catch_up_on_boot=False,
    ),
    ScheduledJob(
        name=JOB_CUSIP_EXTID_SWEEP,
        display_name="CUSIP rewash sweep",
        source="db",
        description=(
            "Sweep ``unresolved_13f_cusips`` for rows whose CUSIP "
            "already has a matching ``external_identifiers`` row, mark "
            "them ``resolved_via_extid``, and rewash the source 13F-HR "
            "accession so the previously-stranded holdings land in "
            "``institutional_holdings`` (#836). Closes the race-loss "
            "path between 13F ingest and the CUSIP backfill — operator "
            "audit 2026-05-03 found 119 Fortune-100 names stranded by "
            "this race. Cheap (one indexed JOIN, bounded to 1000 "
            "rows/pass); daily 04:50 UTC schedules ~15min after the "
            "DEF 14A ingest finishes."
        ),
        cadence=Cadence.daily(hour=4, minute=50),
        # Catch up on boot so a fresh deployment promotes any backlog
        # without waiting for the next 04:50 UTC. Cost is bounded —
        # the sweep is one indexed JOIN; even with the full ~119-row
        # backlog the rewash work is per-accession and the LIMIT 1000
        # cap holds.
        catch_up_on_boot=True,
    ),
    ScheduledJob(
        name=JOB_SEC_DEF14A_BOOTSTRAP,
        display_name="SEC DEF 14A bootstrap drain",
        source="sec_rate",
        description=(
            "One-shot drain of the DEF 14A candidate set (#839). "
            "Loops ``ingest_def14a`` at chunk_limit=500 until the "
            "candidate query empties or the per-run deadline (1 hour) "
            "elapses. Operator audit 2026-05-03 found "
            "``def14a_beneficial_holdings`` empty despite 44k+ DEF 14A "
            "filings on file — daily limit=100 is too slow to drain "
            "historical backlog. Manual trigger via "
            "POST /jobs/sec_def14a_bootstrap/run; auto-fires Sunday "
            "02:30 UTC as a safety net. Cadence leaves a 2-hour "
            "buffer before the daily ``sec_def14a_ingest`` fires at "
            "04:35 UTC so the two jobs cannot overlap and "
            "double-fetch from SEC (Codex pre-push review for #839 "
            "caught the prior 04:30 cadence sharing the same window "
            "as the daily ingester)."
        ),
        cadence=Cadence.weekly(weekday=6, hour=2, minute=30),
        catch_up_on_boot=False,
        prerequisite=_bootstrap_complete,  # #996 — gated until first-install bootstrap is complete
    ),
    ScheduledJob(
        name=JOB_RAW_DATA_RETENTION_SWEEP,
        display_name="Raw data retention sweep",
        source="db",
        description=(
            "Per-source compaction + age-based sweep of data/raw/**. Reclaims "
            "disk from byte-identical duplicates and (per-source) ages-out old "
            "files. Dry-run by default; operator flips settings.raw_retention_dry_run "
            "after observing one cycle."
        ),
        cadence=Cadence.daily(hour=2, minute=0),  # 02:00 UTC, before orchestrator_full_sync at 03:00
        # catch_up_on_boot=False so restarts don't trigger an expensive
        # 225 GB rehash unnecessarily — a missed window waits for the
        # next natural fire.
        catch_up_on_boot=False,
    ),
    ScheduledJob(
        name=JOB_EXCHANGES_METADATA_REFRESH,
        display_name="eToro exchanges metadata refresh",
        source="etoro",
        description=(
            "Weekly refresh of the eToro exchanges catalogue. Pulls "
            "/api/v1/market-data/exchanges and upserts ``description`` "
            "on the ``exchanges`` table. Operator-curated ``country`` / "
            "``asset_class`` are NOT touched. New exchange ids land as "
            "``asset_class='unknown'`` so the operator audit query sees "
            "them and the SEC mapper still excludes them until manually "
            "classified (#503 PR 4)."
        ),
        # Sundays 04:00 UTC — well after orchestrator_full_sync (03:00)
        # and before the working week begins. Catalogue churn is rare
        # so even a missed week is harmless; catch-up on boot is on so
        # a fresh DB picks up real descriptions instead of NULLs without
        # waiting for the next Sunday.
        cadence=Cadence.weekly(weekday=6, hour=4, minute=0),
        catch_up_on_boot=True,
    ),
    ScheduledJob(
        name=JOB_CUSIP_UNIVERSE_BACKFILL,
        display_name="CUSIP universe backfill",
        source="sec_rate",
        description=(
            "Quarterly CUSIP coverage backfill (#914 / #841 PR3). "
            "Walks SEC's Official List of Section 13(f) Securities "
            "(the canonical free regulated source — CUSIP + issuer "
            "name + description, ~12k rows per quarter), fuzzy-matches "
            "each row against ``instruments.company_name``, INSERTs "
            "confident matches into ``external_identifiers``. "
            "Post-batch ``sweep_resolvable_unresolved_cusips`` "
            "promotes previously-stranded 13F holdings into "
            "``institutional_holdings``. Closes the chain: PR1 #912 "
            "discovers filers, PR2 #913 ingests their holdings (most "
            "of which strand on unresolved CUSIP), PR3 (this job) "
            "populates the CUSIP map and drains the strand. "
            "Cadence: weekly Sunday 05:00 UTC — 30 min after "
            "ownership_observations_backfill (03:00) and 30 min after "
            "etoro_lookups_refresh (04:30). Idempotent: already-"
            "mapped instruments are filtered at SELECT; re-runs on a "
            "populated install are cheap reads."
        ),
        cadence=Cadence.weekly(weekday=6, hour=5, minute=0),
        # Catch up on boot — fresh install with empty external_identifiers
        # benefits from running this immediately so the next 13F sweep
        # has CUSIP coverage to work with. Cost is bounded: one
        # ~600KB SEC fetch + a Python-side fuzzy match over ~12k rows
        # (~10s wall-clock).
        catch_up_on_boot=True,
    ),
    # `sec_13f_quarterly_sweep` retired from SCHEDULED_JOBS post-#1155:
    # Layer 1/2/3 + sec_manifest_worker + manifest_parsers/sec_13f_hr.py
    # (#1133) carry every 13F-HR write to institutional_holdings.
    # Function body + _INVOKERS entry preserved — bootstrap stage 21
    # dispatches this job_name (with min_period_of_report +
    # source_label params) via _INVOKERS. Operator-API params now live
    # in MANUAL_TRIGGER_JOB_METADATA (`min_period_of_report`);
    # `source_label` stays in JOB_INTERNAL_KEYS (bootstrap-only).
    # Sweep-adapter `sec_13f_sweep` + Admin "Run now" remain
    # operator-callable.
    ScheduledJob(
        name=JOB_SEC_13F_FILER_DIRECTORY_SYNC,
        display_name="13F filer-directory sync",
        source="sec_rate",
        description=(
            "Discovery sweep of SEC's quarterly form.idx for every "
            "active 13F-HR filer (#912 / #841 PR1). Pre-Phase-2 the "
            "``institutional_filers`` directory holds 14 curated rows; "
            "the real US 13F-HR universe is ~5,000 filers per quarter, "
            "so AAPL institutional rollup is stuck at 5.94% (real "
            "~62%). Walks the last 4 closed quarters' form.idx, "
            "harvests every distinct 13F-HR / 13F-HR/A / 13F-NT filer "
            "CIK + canonical name, UPSERTs into ``institutional_filers``. "
            "Idempotent — re-run on the same quarter set produces "
            "zero new rows but refreshes name + last_filing_at. "
            "Cadence: weekly Sunday 04:15 UTC — staggered after the "
            "existing 04:00 UTC slot (sec_business_summary_bootstrap "
            "+ exchanges_metadata_refresh) and before "
            "etoro_lookups_refresh at 04:30 so the SEC bandwidth "
            "spike isn't aligned with the eToro slot. Does NOT "
            "ingest holdings — that's PR2 (#913)."
        ),
        cadence=Cadence.weekly(weekday=6, hour=4, minute=15),
        # Don't catch up on boot — the sweep fetches ~4×50MB of
        # form.idx text and runs a few minutes; firing on every dev
        # restart would burn SEC bandwidth + dev wall-clock for no
        # operator benefit (the directory churns slowly). A missed
        # window rolls forward to the next Sunday.
        catch_up_on_boot=False,
    ),
    ScheduledJob(
        name=JOB_SEC_NPORT_FILER_DIRECTORY_SYNC,
        display_name="N-PORT filer-directory sync",
        source="sec_rate",
        description=(
            "Discovery sweep — populate ``sec_nport_filer_directory`` "
            "from SEC's quarterly form.idx (#963). N-PORT files under "
            "RIC TRUST CIKs (Vanguard Index Funds, iShares Trust, etc.) "
            "which are disjoint from the 13F-MANAGER CIKs in "
            "``institutional_filers`` (#912). Walks the last 4 closed "
            "quarters' form.idx, harvests every distinct NPORT-P / "
            "NPORT-P/A filer CIK + canonical trust name, UPSERTs into "
            "``sec_nport_filer_directory``. Idempotent — re-run on the "
            "same quarter set produces zero new rows but refreshes "
            "fund_trust_name + last_seen_filed_at on existing rows. "
            "Cadence: weekly Sunday 04:20 UTC — staggered 5 min after "
            "``sec_13f_filer_directory_sync`` so the two SEC bandwidth "
            "spikes don't overlap. Does NOT ingest holdings — that's "
            "``sec_n_port_ingest`` reading off this directory."
        ),
        cadence=Cadence.weekly(weekday=6, hour=4, minute=20),
        # Same rationale as the 13F directory sync — directory churns
        # slowly, ~4×50MB form.idx fetches per run, no operator
        # benefit from firing on every dev restart.
        catch_up_on_boot=False,
    ),
    # `sec_n_port_ingest` retired from SCHEDULED_JOBS post-#1155:
    # Layer 1/2/3 + sec_manifest_worker + manifest_parsers/sec_n_port.py
    # (#1133) carry every NPORT-P write to ownership_funds_observations
    # + ownership_funds_current. Function body + _INVOKERS entry
    # preserved — bootstrap stage 22 dispatches this job_name via
    # _INVOKERS, plus sweep-adapter nport_sweep + Admin "Run now"
    # remain operator-callable.
    ScheduledJob(
        name=JOB_ETORO_LOOKUPS_REFRESH,
        display_name="eToro lookup catalogues refresh",
        source="etoro",
        description=(
            "Weekly refresh of eToro's instrument-types + "
            "stocks-industries lookup catalogues into "
            "``etoro_instrument_types`` / ``etoro_stocks_industries`` "
            "(#515 PR 1). Frontend joins on these tables so the "
            "instrument page renders 'Stocks' / 'Healthcare' instead "
            "of numeric ids. Catalogues rarely churn; ~10s rows total "
            "across both endpoints so the refresh is bounded."
        ),
        # Sundays 04:30 UTC — staggered 30 min after
        # exchanges_metadata_refresh so both jobs don't hit eToro
        # back-to-back. Catch-up on boot for the same reason as the
        # exchanges refresh: fresh DB picks up labels without waiting
        # a week.
        cadence=Cadence.weekly(weekday=6, hour=4, minute=30),
        catch_up_on_boot=True,
    ),
    # #873 — Manifest worker tick.
    #
    # Drains pending + retryable ``sec_filing_manifest`` rows by
    # dispatching the registered parser for each row's source. Without
    # this entry the worker is module-imported but never invoked, so
    # every atom-discovered / per-CIK-polled / daily-index-reconciled
    # manifest row sits in ``pending`` forever and the ``/coverage/
    # manifest-parsers`` audit shows the full backlog as stuck.
    #
    # Cadence: every 5 min so fresh accessions parse within minutes of
    # discovery. The worker self-bounds to ``max_rows=100`` per tick,
    # so a single fire is short. Pre-#873 nothing registered any
    # parser, so this scheduler entry was deferred until at least one
    # parser is wired.
    ScheduledJob(
        name=JOB_SEC_MANIFEST_WORKER,
        display_name="SEC manifest worker tick",
        source="sec_rate",
        description=(
            "Drain pending + retryable sec_filing_manifest rows. "
            "Per-tick dispatch fetches the body, persists raw, parses, "
            "and writes typed observation rows for every source with a "
            "registered parser. Sources without a parser are debug-"
            "skipped and surface in /coverage/manifest-parsers."
        ),
        cadence=Cadence.every_n_minutes(interval=5),
        # Catch-up on boot: a fresh dev stack would otherwise wait
        # 5 min before processing any manifest backlog. The tick is
        # bounded at max_rows=100 so the boot cost is small.
        catch_up_on_boot=True,
    ),
    # -- #1155 Layer 1 / 2 / 3 freshness redesign wiring -------------------
    ScheduledJob(
        name=JOB_SEC_ATOM_FAST_LANE,
        display_name="SEC Atom fast lane (Layer 1)",
        source="sec_rate",
        description=(
            "#1155 — Layer 1 of the #863-#873 freshness redesign. "
            "Every 5 min polls SEC's getcurrent Atom feed; filters to "
            "(cik IN data_freshness_index.cik) + (form mapped to "
            "ManifestSource); UPSERTs sec_filing_manifest rows for the "
            "manifest worker to drain. Single ~1 KB HTTP call covers the "
            "entire SEC universe — fastest discovery layer for 8-K / "
            "Form 4 / 13D/G."
        ),
        cadence=Cadence.every_n_minutes(interval=5),
        # Missed 5-min window = next fire picks up; no catch-up needed.
        catch_up_on_boot=False,
        # Gated until bootstrap completes — until then the universe is
        # empty and every Atom row's subject_resolver returns None, but
        # the gate saves the wasted HTTP fetch.
        prerequisite=_bootstrap_complete,
    ),
    ScheduledJob(
        name=JOB_SEC_DAILY_INDEX_RECONCILE,
        display_name="SEC daily-index reconcile (Layer 2)",
        source="sec_rate",
        description=(
            "#1155 — Layer 2 of the #863-#873 freshness redesign. "
            "Daily 04:00 UTC reconcile reads yesterday's daily-index "
            "master.idx (~1 MB), filters to (cik IN universe) + (form "
            "mapped to ManifestSource), UPSERTs any sec_filing_manifest "
            "rows the Atom feed missed. Safety net against transient "
            "Atom outages."
        ),
        cadence=Cadence.daily(hour=4, minute=0),
        # Catch up on boot is the ENTIRE POINT of this safety net — a
        # stack restart at 06:00 UTC after a missed 04:00 fire must
        # still reconcile yesterday's index.
        catch_up_on_boot=True,
        # NO _bootstrap_complete prereq — JobRuntime evaluates
        # catch_up_on_boot only at process start, so a prereq-blocked
        # catch-up cannot re-fire when bootstrap completes later. Daily-
        # index against an empty universe is a natural no-op
        # (subject_resolver filters every CIK). See spec #1155 §1.4.
        prerequisite=None,
    ),
    ScheduledJob(
        name=JOB_SEC_PER_CIK_POLL,
        display_name="SEC per-CIK poll (Layer 3)",
        source="sec_rate",
        description=(
            "#1155 — Layer 3 of the #863-#873 freshness redesign. "
            "Hourly per-CIK reconcile reads data_freshness_index for "
            "subjects past expected_next_at (poll path) AND past "
            "next_recheck_at (recheck path for never_filed/error rows "
            "— #1155 G13). For each due subject calls submissions.json "
            "and UPSERTs new manifest rows. Bounded total budget split "
            "2/3 poll + ~1/3 recheck (default max_subjects=100 → "
            "66+34) so error/never_filed backlog cannot starve "
            "scheduled polls."
        ),
        cadence=Cadence.hourly(minute=0),
        catch_up_on_boot=False,
        prerequisite=_bootstrap_complete,
    ),
    ScheduledJob(
        name=JOB_SEC_MANIFEST_TOMBSTONE_STALE,
        display_name="Manifest stale-failed sweep",
        source="db",
        description=(
            "#1131 backfill: scan sec_filing_manifest for rows stuck in "
            "ingest_status='failed' with a pre-#1131-shape "
            "'upsert error:...' message older than 24h, promote them to "
            "'tombstoned'. Skips post-#1131 transient-shape rows "
            "(OperationalError / SerializationFailure / DeadlockDetected) "
            "so genuine retries are not masked. Stops the legacy retry "
            "loop where deterministic constraint violations refetched the "
            "same dead XML from SEC every hour. Cadence: daily 05:30 UTC; "
            "self-deactivates once the backlog drains (zero candidates = "
            "no-op). Operator can also trigger via "
            "POST /jobs/sec_manifest_tombstone_stale/run."
        ),
        cadence=Cadence.daily(hour=5, minute=30),
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

    Prelude integration (#1071): when invoked from
    ``app.jobs.runtime.run_with_prelude`` (the standard wrapper for
    scheduled fires + queue-dispatched manual triggers), the prelude has
    already opened the ``job_runs`` row in a single tx that also acquired
    the per-process advisory lock + ran the full-wash fence check. The
    pre-allocated ``run_id`` is exposed via
    ``app.jobs.runtime.consume_prelude_run_id`` so this context manager
    reuses it instead of opening a second row (R5-W2: one writer per
    run, no double-write). When invoked outside the prelude wrappers
    (legacy direct call, tests with no runtime), the pre-allocated id
    is ``None`` and we fall back to ``record_job_start``.
    """
    tracker = _JobTracker(job_name)
    # Function-local import: app.jobs.runtime imports this module's
    # invokers at top level, so the reverse import has to be lazy.
    from app.jobs.runtime import consume_params_snapshot, consume_prelude_run_id

    pre_allocated_run_id = consume_prelude_run_id()
    # Always consume the snapshot context so a nested ``_tracked_job``
    # cannot reuse a stale value. Only used on the prelude-fallback path
    # below (``record_job_start``); the prelude branch already wrote the
    # snapshot in its own INSERT.
    fallback_params_snapshot = consume_params_snapshot()
    if pre_allocated_run_id is not None:
        tracker.run_id = pre_allocated_run_id
        # Advance straight to the body — the prelude already wrote the
        # ``status='running'`` row in its own committed tx.
        try:
            yield tracker
        except Exception as exc:
            try:
                from app.services.sync_orchestrator.exception_classifier import (
                    classify_exception,
                )

                with psycopg.connect(settings.database_url) as conn:
                    record_job_finish(
                        conn,
                        tracker.run_id,
                        status="failure",
                        error_msg=str(exc),
                        error_category=classify_exception(exc),
                    )
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
                    if tracker.row_count is not None:
                        spike = check_row_count_spike(
                            conn,
                            job_name,
                            tracker.row_count,
                            exclude_run_id=tracker.run_id,
                        )
                        if spike.flagged:
                            logger.warning("Row-count spike detected: %s", spike.detail)
            except Exception:
                logger.error("Failed to record job success for %s", job_name, exc_info=True)
        return

    try:
        with psycopg.connect(settings.database_url) as conn:
            tracker.run_id = record_job_start(
                conn,
                job_name,
                params_snapshot=dict(fallback_params_snapshot) if fallback_params_snapshot is not None else None,
            )
    except Exception:
        logger.error("Failed to record job start for %s", job_name, exc_info=True)
        # Still run the job even if tracking fails.
        yield tracker
        return

    try:
        yield tracker
    except Exception as exc:
        try:
            # Function-local import: scheduler is above classify_exception in the orchestrator graph.
            from app.services.sync_orchestrator.exception_classifier import classify_exception

            with psycopg.connect(settings.database_url) as conn:
                record_job_finish(
                    conn,
                    tracker.run_id,
                    status="failure",
                    error_msg=str(exc),
                    error_category=classify_exception(exc),
                )
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

            # #1055: auto-classify exchanges.asset_class='unknown' rows
            # from the now-populated instrument suffix patterns. The
            # one-shot migration sql/068 ran at install when the
            # instruments table was empty so its dominance computation
            # produced nothing — every exchange stayed 'unknown' and
            # downstream us_equity-cohort filters returned 0 rows.
            # Operator-curated rows are preserved (filter scopes to
            # asset_class='unknown' only).
            from app.services.exchanges import reclassify_unknown_exchanges

            with conn.transaction():
                reclass = reclassify_unknown_exchanges(conn)
                logger.info(
                    "Universe sync: reclassified %d exchanges from 'unknown'",
                    reclass.classified,
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

            # Post-bootstrap gap filler: insert Tier 3 rows for any
            # tradable instrument that joined the universe after the
            # initial seed and therefore has no coverage row. seed_coverage
            # no-ops once the table is populated; without this step, such
            # instruments would never get a coverage row and every
            # UPDATE-based coverage audit / gate would silently no-op on
            # them. See #292.
            #
            # bootstrap_missing_coverage_rows opens its own conn.transaction()
            # which becomes a savepoint under the outer connection's
            # implicit transaction. If a later step in nightly_universe_sync
            # raises, the connection context manager rolls back the outer
            # transaction — including this savepoint's inserts. That's
            # intended: coverage bootstrap is a dependent side effect of a
            # successful universe sync. A rolled-back bootstrap is harmless
            # because the missing-row predicate is idempotent and the next
            # nightly run re-inserts. The row_count contribution reflects
            # rows staged inside the connection's transaction; it is
            # accurate for "work attempted this run" even if the outer tx
            # later rolls back.
            bootstrap_result = bootstrap_missing_coverage_rows(conn)
            row_count += bootstrap_result.bootstrapped
            tracker.row_count = row_count
            if bootstrap_result.bootstrapped > 0:
                logger.info(
                    "Coverage bootstrap: inserted %d missing rows at Tier 3",
                    bootstrap_result.bootstrapped,
                )

            tracker.row_count = row_count


# Max T3 instruments to include in candle refresh for bootstrap
# scoring. Prevents hitting API rate limits while giving enough T3
# instruments price data to enable T3→T2 promotion via the
# scoring/coverage pipeline.
_T3_BOOTSTRAP_BATCH_SIZE = 200

# T3 candle bootstrap eligibility query.
# Module-level constant so the test suite imports the same SQL the
# scheduler executes — eliminates the drift risk Codex flagged on
# PR 0 (#515): a copy-pasted test SQL could stay green after a
# production regression. Tests import _T3_BOOTSTRAP_SELECT directly.
#
# Eligibility branches (post-#515 PR 0):
#   1. Tradable + tier 3 + no candles + has fundamentals (original).
#   2. OR tradable + tier 3 + no candles + non-fundamentals-bearing
#      asset class (crypto / fx / commodity / index — those classes
#      never get a fundamentals_snapshot row by design).
# Instruments on exchanges with asset_class='unknown' stay gated;
# operator curates the row first via the #503 PR 4 admin path.
_T3_BOOTSTRAP_SELECT = """
SELECT i.instrument_id, i.symbol
FROM instruments i
JOIN coverage c ON c.instrument_id = i.instrument_id
LEFT JOIN exchanges e ON e.exchange_id = i.exchange
WHERE i.is_tradable = TRUE
  AND c.coverage_tier = 3
  AND NOT EXISTS (
      SELECT 1 FROM price_daily p
      WHERE p.instrument_id = i.instrument_id
  )
  AND (
      EXISTS (
          SELECT 1 FROM fundamentals_snapshot f
          WHERE f.instrument_id = i.instrument_id
      )
      OR e.asset_class IN ('crypto', 'fx', 'commodity', 'index')
  )
ORDER BY i.symbol, i.instrument_id
LIMIT %(limit)s
"""


def daily_candle_refresh() -> None:
    """
    Refresh candles for the scoped instrument set.

    Scope (per 2026-04-19 research-tool refocus §1.3):
      1. All currently-held positions (regardless of coverage tier) —
         the operator needs current price context for anything in the
         portfolio even if it's been demoted below T2.
      2. All Tier 1/2 covered instruments (uncapped).
      3. Up to ``_T3_BOOTSTRAP_BATCH_SIZE`` Tier 3 instruments that
         already have fundamentals, ordered by symbol for determinism.
         Enables T3→T2 promotion by seeding candle history.

    Fetches up to 1000 daily candles per instrument (post-#603 — eToro's
    hard ceiling, ≈4 calendar years of trading-day price points).
    Quotes are skipped (owned by the hourly job).

    Runs daily at 22:00 UTC, after US market close. Watchlist scope
    (spec §1.3 bullet 2) lands once the watchlist table exists
    (Phase 3.2). High-frequency held-position refresh (5-min cadence
    during market hours) is Phase 4 (live quotes).
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
            # Held positions — always included, regardless of coverage
            # tier OR is_tradable status. A delisted/suspended instrument
            # still in the portfolio needs candle updates so P&L and
            # exit-timing logic keep working. If the provider 404s on
            # delisted symbols, refresh_market_data logs and continues
            # per-instrument.
            held_rows = conn.execute(
                """
                SELECT DISTINCT i.instrument_id, i.symbol
                FROM positions p
                JOIN instruments i ON i.instrument_id = p.instrument_id
                WHERE p.current_units > 0
                ORDER BY i.symbol, i.instrument_id
                """
            ).fetchall()

            # T1/T2: all covered instruments (minus any already picked up
            # via held_rows, to avoid duplicate fetches in this batch).
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

            # T3: bootstrap batch (see _T3_BOOTSTRAP_SELECT comment).
            # refresh_market_data fetches up to 1000 candles per
            # instrument in a single API call (post-#603), so a
            # "partial" bootstrap still gives enough data for momentum
            # scoring. If the API call fails entirely, no rows are
            # inserted and the instrument retries next run.
            t3_rows = conn.execute(
                _T3_BOOTSTRAP_SELECT,
                {"limit": _T3_BOOTSTRAP_BATCH_SIZE},
            ).fetchall()

            # Dedupe across scopes. A held T1 instrument must not be
            # fetched twice; set semantics keyed on instrument_id preserve
            # the symbol tuple from the first scope that introduced it.
            seen: set[int] = set()
            ordered: list[tuple[int, str]] = []
            for row in held_rows + tier12_rows + t3_rows:
                iid = int(row[0])
                if iid in seen:
                    continue
                seen.add(iid)
                ordered.append((iid, str(row[1])))

            if not ordered:
                logger.info("daily_candle_refresh: no instruments to refresh, skipping")
                tracker.row_count = 0
                return

            logger.info(
                "daily_candle_refresh: %d held + %d T1/T2 + %d T3 bootstrap = %d unique instruments",
                len(held_rows),
                len(tier12_rows),
                len(t3_rows),
                len(ordered),
            )

            instruments = ordered
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


def _cik_destination_is_empty(conn: psycopg.Connection) -> bool:  # type: ignore[type-arg]
    """Return True when ``external_identifiers`` has zero SEC CIK rows.

    Extracted for testability — daily_cik_refresh's force-full-upsert
    decision pivots on this query result. Empty destination after a
    data wipe must trigger an unconditional fetch + upsert regardless
    of any surviving watermark / body-hash. (#1056)
    """
    row = conn.execute(
        "SELECT COUNT(*) FROM external_identifiers WHERE provider = 'sec' AND identifier_type = 'cik'"
    ).fetchone()
    return row is not None and int(row[0]) == 0


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
            # #1056: detect empty destination. If the operator wiped
            # external_identifiers but the watermark survived (the
            # admin-wipe doesn't currently clear watermarks), the
            # 304/hash-skip branch below would silently no-op forever
            # and AAPL/MSFT would never get CIKs. Force a full
            # unconditional fetch + upsert when destination is empty.
            dest_empty = _cik_destination_is_empty(conn)

            prior = get_watermark(conn, SOURCE, WATERMARK_KEY)
            # Explicit truthy check: an empty-string watermark from a
            # prior run where Last-Modified was absent must NOT be sent
            # as `If-Modified-Since: ` (invalid HTTP date).
            # On dest_empty, send no conditional header so the SEC
            # cannot return 304 against a stale validator.
            if_modified_since = None if dest_empty else (prior.watermark if (prior and prior.watermark) else None)

            result = provider.build_cik_mapping_conditional(
                if_modified_since=if_modified_since,
            )

            if result is None:
                if dest_empty:
                    # Should be unreachable — when dest is empty we
                    # send no If-Modified-Since header so SEC cannot
                    # legitimately return 304. Codex pre-push MEDIUM
                    # for #1056: enforce the invariant explicitly so
                    # a future provider/refactor that silently sends
                    # IMS doesn't leave dest empty forever.
                    raise RuntimeError(
                        "daily_cik_refresh: provider returned 304 despite empty destination "
                        "(no If-Modified-Since header sent). Refusing to skip upsert — investigate."
                    )
                # 304 — nothing changed.
                logger.info("daily_cik_refresh: 304 Not Modified, skipping upsert")
                tracker.row_count = 0
                return

            mapping_size = len(result.mapping)

            if not dest_empty and prior and prior.response_hash == result.body_hash:
                # 200 with identical bytes AND destination already has
                # rows — advance fetched_at only. When destination is
                # empty we MUST upsert regardless of hash (the data
                # was wiped; the hash-skip branch would leave it empty).
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
            if dest_empty:
                logger.warning(
                    "daily_cik_refresh: destination external_identifiers (sec/cik) is empty — "
                    "forcing full upsert regardless of watermark / body hash."
                )

            # #475: Scope to US-listed exchanges only. SEC's
            # company_tickers.json only covers US-registered companies;
            # the mapper used to match every tradable instrument by
            # symbol, which stamped unrelated US-company CIKs onto
            # eToro crypto coins that happened to share a ticker
            # (e.g. BTC crypto got Grayscale Bitcoin Mini Trust's
            # CIK because both answer to "BTC").
            #
            # #503 PR 3: filter migrates from a hardcoded list to the
            # ``exchanges`` table (sql/067) so adding / correcting
            # an exchange's classification is a single row update.
            # Crypto (asset_class='crypto'), unknown ids
            # (asset_class='unknown'), and non-US classes are
            # excluded by the join.
            rows = conn.execute(
                "SELECT i.symbol, i.instrument_id::text FROM instruments i "
                "JOIN exchanges e ON e.exchange_id = i.exchange "
                "WHERE i.is_tradable = TRUE "
                "AND e.asset_class = 'us_equity'"
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

            # #1171 — bundled mutual-fund / ETF classId directory refresh.
            # Populates cik_refresh_mf_directory + external_identifiers
            # (identifier_type='class_id') for the N-CSR fund-metadata
            # parser. Logged-but-not-raised on failure: a directory-refresh
            # error MUST NOT block the equity-side CIK refresh.
            try:
                mf_result = refresh_mf_directory(conn, provider=provider)
                logger.info(
                    "mf_directory refresh: fetched=%s directory_rows=%s ext_ids=%s",
                    mf_result["fetched"],
                    mf_result["directory_rows"],
                    mf_result["external_identifier_rows"],
                )
            except Exception:  # noqa: BLE001 — fail-soft for #1171 bundling
                logger.exception("mf_directory refresh failed; equity CIK refresh result preserved")

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

            # Build symbol→CIK mapping for SEC fundamentals.
            # #540: scope to primary CIKs only so the producer cohort
            # matches the reader's freshness check (which now also
            # filters on is_primary=TRUE). Without this, a demoted
            # historical CIK row could feed the refresh against the
            # wrong issuer while the reader counted the instrument as
            # missing — silent issuer-mix corruption.
            cik_rows = conn.execute(
                """
                SELECT i.symbol, ei.identifier_value
                FROM external_identifiers ei
                JOIN instruments i ON i.instrument_id = ei.instrument_id
                WHERE ei.provider = 'sec'
                  AND ei.identifier_type = 'cik'
                  AND ei.is_primary = TRUE
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
        # Chunk #414: when ``enable_sec_fundamentals_dedupe`` is True,
        # skip this call entirely. ``fundamentals_sync`` phase 1b already
        # refreshes ``fundamentals_snapshot`` for every CIK-mapped
        # tradable instrument daily at 02:30 UTC — same data, one HTTP
        # path. Companies House filings below run regardless.
        sec_symbols = [(sym, iid) for sym, iid in symbols if sym.upper() in cik_map]
        if settings.enable_sec_fundamentals_dedupe:
            logger.info(
                "SEC fundamentals refresh: skipped (enable_sec_fundamentals_dedupe=True); "
                "relying on fundamentals_sync phase 1b for fundamentals_snapshot"
            )
        elif sec_symbols:
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

        # Filings — SEC EDGAR
        # Chunk L: when ``enable_filings_fetch_dedupe`` is True, skip
        # this call entirely. ``daily_financial_facts`` already upserts
        # every master-index entry into ``filing_events`` via
        # ``_upsert_filing_from_master_index`` — strictly broader
        # coverage (all form types, including amendments) than this
        # path's hardcoded {10-K, 10-Q, 8-K} filter. Companies House
        # filings (below) are unaffected — CH has no analogous
        # master-index path.
        if settings.enable_filings_fetch_dedupe:
            logger.info(
                "SEC filings refresh: skipped (enable_filings_fetch_dedupe=True); "
                "relying on daily_financial_facts master-index path for filing_events"
            )
        else:
            with (
                SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
                psycopg.connect(settings.database_url) as conn,
            ):
                # #1011 — daily incremental uses the same three-tier
                # allow-list as the bootstrap. Pre-fix this was
                # ``["10-K", "10-Q", "8-K"]`` (narrower than bootstrap),
                # so first-install + nightly diverged in coverage.
                # ``SEC_INGEST_KEEP_FORMS`` is the canonical union of
                # parse-and-raw + metadata-only forms.
                from app.services.filings import SEC_INGEST_KEEP_FORMS

                sec_summary = refresh_filings(
                    provider=sec,
                    provider_name="sec",
                    identifier_type="cik",
                    conn=conn,
                    instrument_ids=instrument_ids,
                    start_date=from_date,
                    end_date=to_date,
                    filing_types=sorted(SEC_INGEST_KEEP_FORMS),
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
    with _tracked_job(JOB_DAILY_FINANCIAL_FACTS) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            from app.services.fundamentals import execute_refresh, plan_refresh

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

            touched_ciks = list(plan.seeds) + [cik for cik, _ in plan.refreshes]
            if outcome.seeded + outcome.refreshed > 0 and touched_ciks:
                # Phase 2: normalization for CIKs we actually touched this run.
                from app.services.fundamentals import normalize_financial_periods

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
                # No facts written this run. Submissions-only advances are
                # watermark bookkeeping, not data ingestion — excluded from
                # row_count so ops-monitor spike detection reflects actual
                # data volume, not conditional-GET activity. status='success'
                # remains the liveness signal.
                tracker.row_count = 0

            # Phase 3: cascade refresh (#276 Chunk K.1). The bare
            # ``conn.commit()`` is reached only on the success path of
            # Phase 1 + Phase 2 — Python exception propagation skips
            # this line on any prior raise, and ``psycopg.connect()``
            # as a context manager rolls back the connection on
            # exception. The commit is required because
            # ``normalize_financial_periods`` uses savepoints, not
            # commit, and cascade reads must see committed state.
            # Cascade runs even on submissions-only days (8-K thesis
            # context update) as long as there were successful
            # non-seed CIKs.
            conn.commit()
            if settings.anthropic_api_key:
                from app.services.refresh_cascade import (
                    cascade_refresh,
                    changed_instruments_from_outcome,
                )

                # Cascade fires unconditionally when the API key is
                # set so the retry outbox (K.2) gets drained even on
                # days with zero new SEC work. ``cascade_refresh``
                # returns the empty-noop CascadeOutcome when both
                # the retry queue and instrument_ids are empty.
                changed_ids = changed_instruments_from_outcome(conn, plan, outcome)
                cascade_client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
                cascade_outcome = cascade_refresh(conn, cascade_client, changed_ids)
                # Persist any cascade-side writes before the
                # failure-surfacing raise below. compute_rankings
                # writes score rows inside a
                # ``with conn.transaction():`` block that may be
                # nested as a savepoint under this connection's
                # implicit outer tx — without this explicit
                # commit, the raise propagates to
                # psycopg.connect()'s CM rollback and discards
                # any successful ranking writes AND any retry-queue
                # mutations made by cascade's deferred-clear /
                # marker path. On the failure path where
                # compute_rankings itself rolled back (cascade_refresh's
                # inner handler), this commit is a no-op on clean
                # state. Thesis rows are already durably committed
                # by generate_thesis per #293 and are unaffected
                # either way.
                conn.commit()
                logger.info(
                    "cascade_refresh outcome: considered=%d retries_drained=%d "
                    "thesis_refreshed=%d rankings=%s failed=%d",
                    cascade_outcome.instruments_considered,
                    cascade_outcome.retries_drained,
                    cascade_outcome.thesis_refreshed,
                    cascade_outcome.rankings_recomputed,
                    len(cascade_outcome.failed),
                )
                cascade_failures: list[tuple[int, str]] = list(cascade_outcome.failed)
            else:
                logger.info(
                    "daily_financial_facts: ANTHROPIC_API_KEY not set — "
                    "skipping cascade refresh (facts + normalization still committed)"
                )
                cascade_failures = []

            # Surface every partial-failure channel in a single combined raise
            # AFTER all commits so successful CIKs' facts, rankings, and
            # retry-queue mutations all land durably. Channels:
            #   - outcome.failed        — per-CIK XBRL extract failures (#353)
            #   - plan.failed_plan_ciks — planner-phase skips (transient
            #                             submissions.json fetches that never
            #                             reached the executor)
            #   - cascade_failures      — per-instrument thesis failures AND
            #                             the -1 rerank sentinel
            # Without a combined raise, a day where 20% of CIKs fail XBRL but
            # cascade succeeds leaves tracker status='success', phase-1
            # failed_phases empty, and Admin health green — masking a real
            # partial outage. Re-entry path: the K.2 retry outbox re-queues
            # failed executor CIKs, un-advanced master-index watermarks
            # re-plan the planner-skipped CIKs, RERANK_NEEDED markers retry
            # rankings — so all three failure channels converge back to
            # green without manual intervention once the upstream source
            # recovers.
            if outcome.failed or plan.failed_plan_ciks or cascade_failures:
                raise RuntimeError(
                    "daily_financial_facts: "
                    f"xbrl_failed={len(outcome.failed)} ({outcome.failed}); "
                    f"planner_skipped={len(plan.failed_plan_ciks)} ({plan.failed_plan_ciks}); "
                    f"cascade_failed={len(cascade_failures)} ({cascade_failures}); "
                    "facts/normalization/cascade writes for successful CIKs were committed"
                )


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
        locked_skipped = 0
        total = len(stale)
        for idx, item in enumerate(stale, start=1):
            try:
                with psycopg.connect(settings.database_url) as conn:
                    with instrument_lock(conn, item.instrument_id) as acquired:
                        if not acquired:
                            logger.info(
                                "daily_thesis_refresh: LOCKED_BY_SIBLING symbol=%s instrument_id=%d",
                                item.symbol,
                                item.instrument_id,
                            )
                            locked_skipped += 1
                        else:
                            generate_thesis(
                                instrument_id=item.instrument_id,
                                conn=conn,
                                client=claude_client,
                            )
                            # Increment BEFORE demote so a demote
                            # failure can't silently under-count
                            # a successful thesis write. The thesis
                            # row is already committed by
                            # generate_thesis (#293); the demote
                            # call is a separate queue-mutation
                            # side-effect we want to best-effort.
                            generated += 1
                            # Daily's thesis write resolves any pending
                            # cascade thesis signal but does not run
                            # compute_rankings, so demote rather than
                            # delete — preserves RERANK_NEEDED rows
                            # untouched and converts thesis-failure /
                            # LOCKED_BY_SIBLING rows to RERANK_NEEDED.
                            try:
                                demote_to_rerank_needed(conn, item.instrument_id)
                            except Exception:
                                logger.exception(
                                    "daily_thesis_refresh: demote_to_rerank_needed failed "
                                    "for instrument_id=%d — queue signal stale until next run",
                                    item.instrument_id,
                                )
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
        "daily_thesis_refresh complete: generated=%d skipped=%d locked_skipped=%d",
        generated,
        skipped,
        locked_skipped,
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
    Writes one row per breach ONSET to ``position_alerts`` (#396);
    existing open episodes are resolved when the breach clears. Alerts
    also logged for operator visibility via journalctl.

    Read-only with respect to orders — does not place orders or modify
    positions. Writes only to ``position_alerts`` via
    ``persist_position_alerts``.
    """
    with _tracked_job(JOB_MONITOR_POSITIONS) as tracker:
        # Connection lifecycle: one autocommit connection shared by
        # check_position_health (read-only) and persist_position_alerts
        # (owns its own conn.transaction()). autocommit=True is required so
        # the writer's transaction block is the outer transaction (not a
        # savepoint) — see prevention log entry "conn.transaction() savepoint
        # release does not commit the outer transaction".
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            result = check_position_health(conn)
            # Report row_count BEFORE the risky persist call so that if the
            # writer raises, _tracked_job still records the count of
            # positions we actually checked alongside the failure row. The
            # exception is NOT swallowed here: it propagates out of the
            # `with _tracked_job(...)` block, which marks the job as
            # failure in job_runs (prevention: silent success on partial
            # failure hides broken alert ingestion from ops dashboards).
            tracker.row_count = result.positions_checked
            stats = persist_position_alerts(conn, result)

        if result.alerts:
            for alert in result.alerts:
                logger.warning(
                    "monitor_positions: ALERT %s on %s (instrument_id=%d): %s",
                    alert.alert_type,
                    alert.symbol,
                    alert.instrument_id,
                    alert.detail,
                )

        logger.info(
            "monitor_positions: %d checked, episodes: +%d opened / -%d resolved / %d unchanged",
            result.positions_checked,
            stats.opened,
            stats.resolved,
            stats.unchanged,
        )


def fundamentals_sync() -> None:
    """Daily fundamentals research refresh.

    Per the 2026-04-19 research-tool refocus
    (docs/superpowers/specs/2026-04-19-research-tool-refocus.md §1.1), this
    job owns the SEC research data pipeline end-to-end. Runs daily at
    02:30 UTC (#414 — landed just after the SEC nightly XBRL publish
    window so newly-submitted 10-K/10-Q/8-K filings are ingested the
    same night rather than up to seven days later).

    Four phases, in order:

    0. **CIK refresh.** Pull SEC ticker→CIK mapping and upsert into
       ``external_identifiers`` (via the legacy ``daily_cik_refresh``
       helper). Runs even if the map is unchanged — the conditional-fetch
       path short-circuits to a no-op on 304/identical-body.
    1. **SEC XBRL facts + normalization.** Pull 10-K/10-Q XBRL facts for
       eligible instruments and normalize them into ``financial_periods``
       (via the legacy ``daily_financial_facts`` helper).
    2. **Coverage audit + backfill.** Re-classify every tradable
       instrument's ``coverage.filings_status`` via the bulk audit, then
       drive any non-terminal one toward terminal state via
       ``backfill_filings``.
    3. **Tier review.** Evaluate all instruments with coverage rows
       against the deterministic promote/demote rules and enforce the
       Tier 1 cap.

    Failure isolation: phase 0 and phase 1 wrap their legacy-helper calls
    in ``try/except`` so a CIK pull blip or XBRL outage does not
    cannibalise the audit/review work. If phase 2 raises, the whole job
    fails. Phase 3 is isolated too — a transient review error leaves
    phase-2 writes committed and the job succeeded.
    """
    # Operator pause switch (#414 design goal F). Operator flips
    # ``layer_enabled[fundamentals_ingest]`` to False via the admin UI /
    # SQL to pause ingest during a demo or when SEC is rate-limiting us,
    # without restarting the server. Default behaviour is enabled — an
    # absent row counts as enabled, so first-time deploys are not gated
    # by a missing migration.
    #
    # Checked BEFORE entering ``_tracked_job`` and written as a
    # ``status='skipped'`` row via ``record_job_skip`` so the admin UI
    # can distinguish an operator-initiated pause from a regular
    # zero-row success (same pattern as ``_write_prereq_skip_row``).
    from app.services.layer_enabled import is_layer_enabled

    # Fail-open posture: if the gate read itself errors (DB unavailable,
    # table missing on a first-boot), fall through to ``_tracked_job`` so
    # the run still lands a job_runs row — either success or a real
    # failure from the body — rather than vanishing silently. Mirrors
    # the runtime's prerequisite-check policy in
    # ``app/jobs/runtime.py``.
    ingest_paused = False
    try:
        with psycopg.connect(settings.database_url, autocommit=True) as gate_conn:
            if not is_layer_enabled(gate_conn, "fundamentals_ingest"):
                ingest_paused = True
                logger.info(
                    "fundamentals_sync: skipped (layer_enabled[fundamentals_ingest]=False); "
                    "operator paused ingest — flip to True via the admin UI to resume"
                )
                try:
                    record_job_skip(
                        gate_conn,
                        JOB_FUNDAMENTALS_SYNC,
                        "paused by operator: layer_enabled[fundamentals_ingest]=False",
                    )
                except Exception:
                    logger.error(
                        "fundamentals_sync: failed to write operator-pause skip row",
                        exc_info=True,
                    )
    except Exception:
        logger.error(
            "fundamentals_sync: operator-pause gate read failed — falling open and running job",
            exc_info=True,
        )
    if ingest_paused:
        return

    with _tracked_job(JOB_FUNDAMENTALS_SYNC) as tracker:
        from app.services.coverage import BackfillOutcome, audit_all_instruments, backfill_filings

        logger.info("fundamentals_sync: starting")

        # Track phase 0/1 failures so we can raise at the end — the outer
        # _tracked_job then records the job as failed so the health
        # surfaces (/system/status, Admin UI) see the problem. Without
        # this, a CIK/XBRL outage writes an internal job_runs failure
        # but fundamentals_sync would still mark itself succeeded, and
        # there is no orchestrator SEC layer left to surface the state.
        failed_phases: list[str] = []

        # --- Phase 0: CIK refresh ----------------------------------------
        # Isolated so a failure here does not prevent the audit/review
        # from running on whatever CIK map was previously persisted.
        try:
            daily_cik_refresh()
        except Exception:
            logger.error(
                "fundamentals_sync phase 0 (CIK refresh) failed — continuing "
                "with stale CIK map; later phases may skip instruments without "
                "a mapping",
                exc_info=True,
            )
            failed_phases.append("phase 0 (CIK refresh)")

        # --- Phase 1: SEC XBRL facts + normalization ---------------------
        # Isolated for the same reason — stale XBRL still beats skipping
        # the audit and leaving the UI blind to coverage state drift.
        try:
            daily_financial_facts()
        except Exception:
            logger.error(
                "fundamentals_sync phase 1 (XBRL + normalization) failed — "
                "continuing to audit/review so coverage state still advances",
                exc_info=True,
            )
            failed_phases.append("phase 1 (XBRL + normalization)")

        # --- Phase 1b: SEC fundamentals snapshot refresh -----------------
        # Collapses the dual SEC ``companyfacts`` fetch path identified
        # in issue #414. Only runs when the operator has flipped
        # ``enable_sec_fundamentals_dedupe=True`` in settings — the
        # matching gate in ``daily_research_refresh`` skips its own SEC
        # section when the flag is on, so exactly one job per day hits
        # ``data.sec.gov/api/xbrl/companyfacts/…``.
        #
        # Isolated like phase 0/1: a transient snapshot failure must not
        # block audit/review. Coverage reads ``fundamentals_snapshot`` so
        # stale rows still beat a missed audit.
        phase1b_rows = 0
        if settings.enable_sec_fundamentals_dedupe:
            try:
                with psycopg.connect(settings.database_url) as conn:
                    # ``ei.is_primary = TRUE`` matches the phase-2 audit
                    # query. Without it, an instrument with a demoted
                    # historical SEC CIK row would appear twice in the
                    # result and the cik_map dict would non-deterministically
                    # pick whichever row came last — critical now that
                    # this query is the sole SEC snapshot driver under
                    # the dedupe flag.
                    cik_rows = conn.execute(
                        """
                        SELECT i.symbol, i.instrument_id::text, ei.identifier_value
                        FROM instruments i
                        JOIN external_identifiers ei
                            ON ei.instrument_id = i.instrument_id
                           AND ei.provider = 'sec'
                           AND ei.identifier_type = 'cik'
                           AND ei.is_primary = TRUE
                        WHERE i.is_tradable = TRUE
                        """
                    ).fetchall()
                    conn.commit()
                if cik_rows:
                    sec_symbols = [(str(row[0]), str(row[1])) for row in cik_rows]
                    cik_map = {str(row[0]).upper(): str(row[2]) for row in cik_rows}
                    with (
                        SecFundamentalsProvider(user_agent=settings.sec_user_agent) as sec_fund,
                        psycopg.connect(settings.database_url) as conn,
                    ):
                        sec_fund.set_cik_cache(cik_map)
                        snap_summary = refresh_fundamentals(sec_fund, conn, sec_symbols)
                    phase1b_rows = snap_summary.snapshots_upserted
                    logger.info(
                        "fundamentals_sync phase 1b (SEC snapshot) complete: attempted=%d upserted=%d skipped=%d",
                        snap_summary.symbols_attempted,
                        snap_summary.snapshots_upserted,
                        snap_summary.symbols_skipped,
                    )
                else:
                    logger.info("fundamentals_sync phase 1b (SEC snapshot) skipped: no CIK-mapped tradable instruments")
            except Exception:
                logger.error(
                    "fundamentals_sync phase 1b (SEC snapshot) failed — "
                    "continuing to audit/review on last-known snapshot",
                    exc_info=True,
                )
                failed_phases.append("phase 1b (SEC snapshot)")

        # --- Phase 2: coverage audit + eligibility-gated backfill --------
        outcomes: dict[BackfillOutcome, int] = {o: 0 for o in BackfillOutcome}
        eligible_count = 0
        with psycopg.connect(settings.database_url) as conn:
            pre_audit = audit_all_instruments(conn)

            eligible_rows = conn.execute(
                """
                SELECT c.instrument_id, ei.identifier_value AS cik
                FROM coverage c
                JOIN external_identifiers ei
                    ON ei.instrument_id = c.instrument_id
                   AND ei.provider = 'sec'
                   AND ei.identifier_type = 'cik'
                   AND ei.is_primary = TRUE
                WHERE c.filings_status IN ('insufficient', 'unknown', 'structurally_young')
                """
            ).fetchall()
            conn.commit()
            eligible_count = len(eligible_rows)

            with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
                for row in eligible_rows:
                    iid, cik = int(row[0]), str(row[1])
                    try:
                        result = backfill_filings(conn, provider, cik, iid)
                    except Exception:
                        logger.exception(
                            "fundamentals_sync: backfill raised for instrument_id=%d",
                            iid,
                        )
                        try:
                            conn.rollback()
                        except Exception:
                            logger.exception(
                                "fundamentals_sync: rollback failed after "
                                "instrument_id=%d — later instruments may fail",
                                iid,
                            )
                        continue
                    outcomes[result.outcome] += 1

        non_skipped_backfill_writes = sum(
            outcomes[o]
            for o in (
                BackfillOutcome.COMPLETE_OK,
                BackfillOutcome.COMPLETE_FPI,
                BackfillOutcome.STILL_INSUFFICIENT_STRUCTURALLY_YOUNG,
                BackfillOutcome.STILL_INSUFFICIENT_EXHAUSTED,
                BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR,
                BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR,
            )
        )
        audit_rows = pre_audit.total_updated + non_skipped_backfill_writes
        logger.info(
            "fundamentals_sync phase 2 (audit) complete: "
            "pre_analysable=%d eligible=%d "
            "complete_ok=%d complete_fpi=%d structurally_young=%d "
            "exhausted=%d http_err=%d parse_err=%d "
            "skipped_cap=%d skipped_backoff=%d null_anomalies=%d",
            pre_audit.analysable,
            eligible_count,
            outcomes[BackfillOutcome.COMPLETE_OK],
            outcomes[BackfillOutcome.COMPLETE_FPI],
            outcomes[BackfillOutcome.STILL_INSUFFICIENT_STRUCTURALLY_YOUNG],
            outcomes[BackfillOutcome.STILL_INSUFFICIENT_EXHAUSTED],
            outcomes[BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR],
            outcomes[BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR],
            outcomes[BackfillOutcome.SKIPPED_ATTEMPTS_CAP],
            outcomes[BackfillOutcome.SKIPPED_BACKOFF_WINDOW],
            pre_audit.null_anomalies,
        )

        # --- Phase 3: coverage tier review -------------------------------
        # Isolate phase 3 failures from phase 2 success: if review_coverage
        # raises, log and continue so the committed audit+backfill writes
        # from phase 2 still mark the job as succeeded. Mirrors the
        # "try/except + return" semantics of the retired weekly_coverage_review.
        review_rows = 0
        try:
            with psycopg.connect(settings.database_url) as conn:
                review_result = review_coverage(conn)
            review_rows = len(review_result.promotions) + len(review_result.demotions)
            logger.info(
                "fundamentals_sync phase 3 (review) complete: promotions=%d demotions=%d blocked=%d unchanged=%d",
                len(review_result.promotions),
                len(review_result.demotions),
                len(review_result.blocked),
                review_result.unchanged,
            )
        except Exception:
            logger.error("fundamentals_sync phase 3 (review) failed", exc_info=True)
            failed_phases.append("phase 3 (review)")

        # Phase 1b snapshots are counted separately from phase-2/3 rows
        # so the row-count contract (tracker = rows written / audit-
        # consistent) still holds when this job becomes the sole SEC
        # companyfacts writer under #414.
        tracker.row_count = audit_rows + review_rows + phase1b_rows

        # Raise at the end so all phases ran first, but the outer
        # _tracked_job marks the job failed and the health surfaces
        # pick it up. Phases 0, 1, 3 are isolated by try/except; phase 2
        # (audit) propagates immediately so it never reaches here if it
        # failed.
        #
        # Each failing phase has already logged its own exc_info=True
        # traceback. The message below names the subsystems so alerting
        # rules can distinguish SEC-upstream outages (phases 0/1) from
        # coverage-review logic failures (phase 3); operators grep the
        # logs by phase name to find the captured traceback.
        if failed_phases:
            raise RuntimeError(
                "fundamentals_sync completed with phase failures: "
                + "; ".join(failed_phases)
                + " (individual tracebacks logged at ERROR level; "
                + "grep logs for 'fundamentals_sync phase <N>')"
            )


def fx_rates_refresh() -> None:
    """Refresh live FX rates from Frankfurter (ECB reference rates).

    Per the visibility-driven live-prices spec
    (docs/superpowers/specs/2026-04-25-visibility-driven-live-prices-spec.md):

    - Cadence cut from hourly to once daily at 17:00 CET. ECB
      publishes reference rates once per working day ~16:00 CET, so
      hourly polling was burning >95% as 304 Not Modified hits.
      Daily matches the actual publish cadence.
    - Phase 2 (eToro batch quotes) **dropped**. The WS live-tick
      pipeline (#274) writes to the ``quotes`` table directly for
      every instrument an SSE stream subscribes to, making the
      batch-quote path redundant for visibility-driven workflows.

    Conditional ETag (#275): sends If-None-Match against the last
    persisted ETag. A 304 is a no-op upsert.

    The lifespan also runs an inline "bootstrap" call (see
    ``app.main._bootstrap_fx_rates``) when the ``live_fx_rates``
    table is empty at boot, so a fresh DB has rates available
    before the first request lands without waiting for the
    daily cron.
    """
    from app.providers.implementations.frankfurter import fetch_latest_rates_conditional
    from app.services.fx import upsert_live_fx_rate
    from app.services.runtime_config import SUPPORTED_CURRENCIES

    FX_SOURCE = "frankfurter.latest"
    FX_WATERMARK_KEY = "global"

    with _tracked_job(JOB_FX_RATES_REFRESH) as tracker:
        fx_rows_written = 0

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
            logger.warning("fx_rates_refresh: Frankfurter fetch failed", exc_info=True)

        tracker.row_count = fx_rows_written

    logger.info("fx_rates_refresh complete: fx_pairs=%d", fx_rows_written)


def exchanges_metadata_refresh() -> None:
    """Refresh ``exchanges.description`` from eToro's exchanges endpoint.

    Weekly cron — eToro's exchange catalogue rarely churns (~tens of
    rows; new exchange ids land maybe a few times a year). Operator-
    curated columns (``country``, ``asset_class``) are left alone; only
    ``description`` is upserted from the API.

    See ``app.services.exchanges.refresh_exchanges_metadata`` for the
    upsert semantics and the no-clobber-on-empty guard.
    """
    creds = _load_etoro_credentials(JOB_EXCHANGES_METADATA_REFRESH)
    if creds is None:
        _record_prereq_skip(JOB_EXCHANGES_METADATA_REFRESH, "etoro credentials missing")
        return
    api_key, user_key = creds

    with _tracked_job(JOB_EXCHANGES_METADATA_REFRESH) as tracker:
        with (
            EtoroMarketDataProvider(api_key=api_key, user_key=user_key, env=settings.etoro_env) as provider,
            psycopg.connect(settings.database_url) as conn,
        ):
            summary = refresh_exchanges_metadata(provider, conn)
            tracker.row_count = summary.inserted + summary.description_updated
            logger.info(
                "exchanges_metadata_refresh complete: fetched=%d inserted=%d description_updated=%d",
                summary.fetched,
                summary.inserted,
                summary.description_updated,
            )


def etoro_lookups_refresh() -> None:
    """Refresh ``etoro_instrument_types`` + ``etoro_stocks_industries``
    reference tables from eToro's public lookup endpoints.

    Weekly cron — both catalogues rarely churn (a few rows added
    per year at most). The refresh is bounded (10s of rows total)
    so it doesn't compete with the universe sync. See
    ``app.services.etoro_lookups.refresh_etoro_lookups`` for the
    upsert semantics.
    """
    creds = _load_etoro_credentials(JOB_ETORO_LOOKUPS_REFRESH)
    if creds is None:
        _record_prereq_skip(JOB_ETORO_LOOKUPS_REFRESH, "etoro credentials missing")
        return
    api_key, user_key = creds

    with _tracked_job(JOB_ETORO_LOOKUPS_REFRESH) as tracker:
        with (
            EtoroMarketDataProvider(api_key=api_key, user_key=user_key, env=settings.etoro_env) as provider,
            psycopg.connect(settings.database_url) as conn,
        ):
            summary = refresh_etoro_lookups(provider, conn)
            tracker.row_count = (
                summary.instrument_types_inserted
                + summary.instrument_types_updated
                + summary.industries_inserted
                + summary.industries_updated
            )
            logger.info(
                "etoro_lookups_refresh complete: types=%d/%d/%d industries=%d/%d/%d (fetched/inserted/updated)",
                summary.instrument_types_fetched,
                summary.instrument_types_inserted,
                summary.instrument_types_updated,
                summary.industries_fetched,
                summary.industries_inserted,
                summary.industries_updated,
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

    #1078 — admin control hub PR6. Listener-dispatched manual_job
    runs (operator clicks Iterate / Full-wash on the
    `orchestrator_full_sync` row) plumb `(linked_request_id, mode)`
    via the invoker contextvar so `_start_sync_run`'s fence-check can
    bypass on `mode='full_wash'` (the run IS the fence holder).
    Scheduled fires + tests pass `(None, None)` → defaults apply.
    """
    from app.jobs.runtime import consume_invoker_request_context
    from app.services.sync_orchestrator import SyncScope, run_sync

    linked_request_id, request_mode = consume_invoker_request_context()
    logger.info("orchestrator_full_sync: starting")
    result = run_sync(
        SyncScope.full(),
        trigger="scheduled",
        linked_request_id=linked_request_id,
        request_mode=request_mode,
    )
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
    from app.jobs.runtime import consume_invoker_request_context
    from app.services.sync_orchestrator import (
        SyncAlreadyRunning,
        SyncScope,
        run_sync,
    )

    linked_request_id, request_mode = consume_invoker_request_context()
    try:
        result = run_sync(
            SyncScope.high_frequency(),
            trigger="scheduled",
            linked_request_id=linked_request_id,
            request_mode=request_mode,
        )
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


def raw_data_retention_sweep() -> None:
    """Daily compaction + age-based sweep across every registered
    raw-data source (#268 follow-up Plan A PR 3).

    Two phases per source:

    - **Compaction** (expensive, content-hash scan). Runs only when
      ``last_compacted_at`` is older than ``COMPACTION_STALENESS``
      (7 days) or NULL. Without this throttle the job rehashes
      225 GB daily.
    - **Sweep** (cheap, mtime glob). Runs every day per source.
      No-op when policy.max_age_days is None.

    Dry-run mode (``settings.raw_retention_dry_run=True`` by default):
    logs counts, makes zero filesystem mutations, does NOT update
    ``raw_persistence_state``. Operator flips the flag after
    observing one dry-run cycle's output.

    Observability: per-source structured log lines cover source,
    phase (compact/sweep), files_deleted, bytes_reclaimed. Job-level
    row lands in ``job_runs`` via ``_tracked_job``.

    ``catch_up_on_boot=False`` at the schedule registration so
    restarts don't trigger an unnecessary rehash.
    """
    from app.services.raw_persistence import (
        _RETENTION_POLICY,
        compact_source,
        load_state,
        needs_compaction,
        sweep_source,
        update_compaction_state,
        update_sweep_state,
    )

    dry_run = settings.raw_retention_dry_run
    with _tracked_job(JOB_RAW_DATA_RETENTION_SWEEP) as tracker:
        total_deleted = 0
        total_bytes = 0

        with psycopg.connect(settings.database_url) as conn:
            for source in _RETENTION_POLICY:
                # --- Compaction phase (throttled by staleness) ---
                # A compaction error MUST NOT skip the sweep phase
                # for the same source — they are independent
                # operations. Bug caught in pre-merge review: the
                # earlier `continue` on compaction raise silently
                # suppressed every subsequent sweep for recurring
                # compaction errors, defeating retention.
                state = load_state(conn, source)
                if needs_compaction(state):
                    try:
                        result = compact_source(source, dry_run=dry_run)
                        if not dry_run:
                            update_compaction_state(conn, source, result)
                        logger.info(
                            "raw_data_retention_sweep: source=%s phase=compact "
                            "scanned=%d deleted=%d reclaimed=%d elapsed=%.2f "
                            "dry_run=%s",
                            source,
                            result.files_scanned,
                            result.files_deleted,
                            result.bytes_reclaimed,
                            result.elapsed_seconds,
                            dry_run,
                        )
                        total_deleted += result.files_deleted
                        total_bytes += result.bytes_reclaimed
                    except Exception:
                        logger.exception(
                            "raw_data_retention_sweep: compact raised for source=%s — "
                            "sweep phase still runs for this source",
                            source,
                        )

                # --- Sweep phase (daily, cheap, independent of compaction) ---
                try:
                    sweep = sweep_source(source, dry_run=dry_run)
                except Exception:
                    logger.exception(
                        "raw_data_retention_sweep: sweep raised for source=%s",
                        source,
                    )
                    continue
                if not dry_run:
                    update_sweep_state(conn, source)
                logger.info(
                    "raw_data_retention_sweep: source=%s phase=sweep deleted=%d reclaimed=%d elapsed=%.2f dry_run=%s",
                    source,
                    sweep.files_deleted,
                    sweep.bytes_reclaimed,
                    sweep.elapsed_seconds,
                    dry_run,
                )
                total_deleted += sweep.files_deleted
                total_bytes += sweep.bytes_reclaimed

        tracker.row_count = total_deleted
        logger.info(
            "raw_data_retention_sweep complete: total_deleted=%d total_bytes_reclaimed=%d dry_run=%s",
            total_deleted,
            total_bytes,
            dry_run,
        )


def sec_manifest_worker_tick() -> None:
    """#873 — One drain pass over ``sec_filing_manifest``.

    Reads up to 100 pending / retryable rows (across all sources),
    dispatches the registered parser for each row's source, and lets
    the worker handle the manifest state transitions. Sources with
    no parser are debug-skipped and surface in
    ``GET /coverage/manifest-parsers``.

    Bounded at ``max_rows=100`` per tick so each fire is short.
    Backlog drains across successive ticks at the every-5-min cadence
    declared in ``SCHEDULED_JOBS``.
    """
    from app.jobs.sec_manifest_worker import run_manifest_worker

    with _tracked_job(JOB_SEC_MANIFEST_WORKER) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            stats = run_manifest_worker(conn, source=None, max_rows=100)
            conn.commit()

        tracker.row_count = stats.parsed + stats.tombstoned + stats.failed
        logger.info(
            "sec_manifest_worker tick: processed=%d parsed=%d tombstoned=%d failed=%d skipped_no_parser=%d",
            stats.rows_processed,
            stats.parsed,
            stats.tombstoned,
            stats.failed,
            stats.skipped_no_parser,
        )


def sec_manifest_tombstone_stale() -> None:
    """#1131 — promote stale-failed manifest rows to ``tombstoned``.

    Pre-#1131 every per-source parser treated an upsert exception as
    ``failed`` with a 1h retry. Deterministic constraint violations
    looped against SEC on every tick for the same dead XML. PR #1131
    split transient (OperationalError) from deterministic (everything
    else); this job promotes the pre-#1131 retry-stuck rows to
    ``tombstoned`` so the worker stops re-fetching them.

    Heuristic: 24h continuous failure ≥ 24 retries at the 1h cadence
    ≈ "not recovering". Skips post-#1131 transient-shape rows by
    class-name match in the ``error`` text so genuine retries are not
    masked. Self-deactivates: once the backlog drains, every run is a
    no-op (zero scanned, zero tombstoned).
    """
    from app.services.sec_manifest import tombstone_stale_failed_upserts

    with _tracked_job(JOB_SEC_MANIFEST_TOMBSTONE_STALE) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            result = tombstone_stale_failed_upserts(conn)
            conn.commit()

        tracker.row_count = result.rows_tombstoned
        logger.info(
            "sec_manifest_tombstone_stale: scanned=%d tombstoned=%d skipped_transient=%d skipped_race=%d",
            result.rows_scanned,
            result.rows_tombstoned,
            result.rows_skipped_transient,
            result.rows_skipped_race,
        )


def sec_business_summary_bootstrap() -> None:
    """One-shot drain of the 10-K Item 1 candidate set (#535).

    Calls :func:`bootstrap_business_summaries`, which loops the
    standard ingester at a higher chunk limit until the queue
    empties or the per-run deadline (1 hour) elapses. Bounded by
    the SEC fair-use rate-limit budget and the candidate query's
    backoff filter (quarantined rows stay excluded).

    Designed for first-time backfill of the SEC-CIK universe and
    operator-driven catch-up after extended outages. Manual-trigger
    only via ``POST /jobs/sec_business_summary_bootstrap/run``;
    auto-fires weekly Sunday 04:00 UTC as a safety net to catch
    anything the daily cron's bounded limit can't keep up with.
    """
    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.services.business_summary import bootstrap_business_summaries

    with _tracked_job(JOB_SEC_BUSINESS_SUMMARY_BOOTSTRAP) as tracker:
        with (
            psycopg.connect(settings.database_url) as conn,
            SecFilingsProvider(user_agent=settings.sec_user_agent) as provider,
        ):
            # #1045: prefetch cohort URLs via PipelinedSecFetcher for
            # 4-way concurrent in-flight fetches at the shared 7 req/s
            # ceiling. Bootstrap-only — steady-state per-filing path
            # now runs through the manifest worker + `sec_10k.py` parser
            # (#1152, post-#1155 retirement of `sec_business_summary_ingest`),
            # which the worker rate-limits at its own per-tick budget.
            result = bootstrap_business_summaries(
                conn,
                provider,
                prefetch_urls=True,
                prefetch_user_agent=settings.sec_user_agent,
            )

        tracker.row_count = result.rows_inserted + result.rows_updated
        logger.info(
            "sec_business_summary_bootstrap complete: "
            "scanned=%d inserted=%d updated=%d fetch_errors=%d parse_misses=%d",
            result.filings_scanned,
            result.rows_inserted,
            result.rows_updated,
            result.fetch_errors,
            result.parse_misses,
        )


def sec_insider_transactions_ingest() -> None:
    """Parse Form 4 filings into ``insider_transactions`` (#429).

    Same shape as the dividend / business-summary ingesters. Runs
    hourly because Form 4 is filed within two business days of a
    trade — stale insider data is low-value. Bounded per run and
    idempotent via the (accession, row_num) UNIQUE key so a quiet
    hour costs nothing.
    """
    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.services.insider_transactions import ingest_insider_transactions

    with _tracked_job(JOB_SEC_INSIDER_TRANSACTIONS_INGEST) as tracker:
        with (
            psycopg.connect(settings.database_url) as conn,
            SecFilingsProvider(user_agent=settings.sec_user_agent) as provider,
        ):
            result = ingest_insider_transactions(conn, provider)

        tracker.row_count = result.rows_inserted
        logger.info(
            "sec_insider_transactions_ingest: scanned=%d parsed=%d inserted=%d fetch_errors=%d parse_misses=%d",
            result.filings_scanned,
            result.filings_parsed,
            result.rows_inserted,
            result.fetch_errors,
            result.parse_misses,
        )


def sec_form3_ingest() -> None:
    """Parse SEC Form 3 filings into ``insider_initial_holdings`` (#768).

    Form 3 is the per-officer initial-snapshot filing — one per
    Section-16 appointment, no transactions. Without this ingest,
    insiders who get an RSU grant on appointment and never trade
    after are invisible to the ownership card's per-officer ring
    (no Form 4 events for them). Daily cadence is plenty: Form 3
    volume is bounded (~5-30 lifetime per issuer) and the snapshot
    isn't time-sensitive once captured.
    """
    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.services.insider_form3_ingest import ingest_form_3_filings

    with _tracked_job(JOB_SEC_FORM3_INGEST) as tracker:
        with (
            psycopg.connect(settings.database_url) as conn,
            SecFilingsProvider(user_agent=settings.sec_user_agent) as provider,
        ):
            result = ingest_form_3_filings(conn, provider)

        tracker.row_count = result.rows_inserted
        logger.info(
            "sec_form3_ingest: scanned=%d parsed=%d inserted=%d fetch_errors=%d parse_misses=%d",
            result.filings_scanned,
            result.filings_parsed,
            result.rows_inserted,
            result.fetch_errors,
            result.parse_misses,
        )


def sec_def14a_ingest() -> None:
    """Parse SEC DEF 14A proxy statements into
    ``def14a_beneficial_holdings`` (#769 / #805).

    Operator audit 2026-05-03 found this ingester was authored under
    #769 (PRs #771-#774) but never had a periodic invocation wired up,
    leaving ``def14a_beneficial_holdings`` empty in dev DB. The DEF
    14A drift detector and the ownership rollup's
    ``def14a_unmatched`` slice both depend on this data.

    Daily cadence is plenty — proxy filings are quarterly-ish per
    issuer; daily catches new ones within a cycle. Idempotent via
    the ``(accession, holder_name)`` UNIQUE key + the
    ``def14a_ingest_log`` tombstone.
    """
    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.services.def14a_ingest import ingest_def14a

    with _tracked_job(JOB_SEC_DEF14A_INGEST) as tracker:
        with (
            psycopg.connect(settings.database_url) as conn,
            SecFilingsProvider(user_agent=settings.sec_user_agent) as provider,
        ):
            result = ingest_def14a(conn, provider)

        tracker.row_count = result.rows_inserted
        logger.info(
            "sec_def14a_ingest: scanned=%d succeeded=%d partial=%d failed=%d rows_inserted=%d rows_updated=%d",
            result.accessions_seen,
            result.accessions_succeeded,
            result.accessions_partial,
            result.accessions_failed,
            result.rows_inserted,
            result.rows_updated,
        )


def sec_def14a_bootstrap() -> None:
    """One-shot drain of the DEF 14A candidate set (#839).

    Calls :func:`bootstrap_def14a`, which loops the standard ingester
    at a higher chunk limit until the queue empties or the per-run
    deadline (1 hour) elapses. Bounded by the SEC fair-use rate-limit
    budget and the discovery selector's tombstone filter (already-
    attempted rows stay excluded).

    Designed for first-time backfill of the SEC DEF 14A universe:
    operator audit 2026-05-03 found ``def14a_beneficial_holdings``
    empty across the dev DB despite 44k+ DEF 14A filings on file in
    ``filing_events``. The daily cron's ``limit=100`` is too slow to
    drain the historical backlog; this bootstrap processes the
    backlog in one bounded session.

    Manual-trigger only via ``POST /jobs/sec_def14a_bootstrap/run``;
    auto-fires weekly Sunday 02:30 UTC as a safety net to catch
    anything the daily cron's bounded limit can't keep up with. The
    cadence leaves a 2-hour buffer before ``sec_def14a_ingest`` fires
    at 04:35 UTC so the bootstrap's 1-hour deadline cannot overlap
    the daily run (Claude review for #839 caught the prior 04:30
    cadence sharing the daily ingester's window).
    Mirrors :func:`sec_business_summary_bootstrap` design (#535).
    """
    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.services.def14a_ingest import bootstrap_def14a

    with _tracked_job(JOB_SEC_DEF14A_BOOTSTRAP) as tracker:
        with (
            psycopg.connect(settings.database_url) as conn,
            SecFilingsProvider(user_agent=settings.sec_user_agent) as provider,
        ):
            # #1045: prefetch DEF 14A primary docs via PipelinedSecFetcher.
            result = bootstrap_def14a(
                conn,
                provider,
                prefetch_urls=True,
                prefetch_user_agent=settings.sec_user_agent,
            )

        tracker.row_count = result.rows_inserted + result.rows_updated
        logger.info(
            "sec_def14a_bootstrap: seen=%d succeeded=%d partial=%d failed=%d rows_inserted=%d rows_updated=%d",
            result.accessions_seen,
            result.accessions_succeeded,
            result.accessions_partial,
            result.accessions_failed,
            result.rows_inserted,
            result.rows_updated,
        )


def ownership_observations_sync() -> None:
    """Self-healing repair sweep for ``ownership_*_current`` (#892 / #873).

    Replaces the legacy nightly read-from-typed-tables sync. The live
    ingesters (#888 insiders, #889 institutions, #890 blockholders,
    #891 treasury+def14a) now write observations + refresh ``_current``
    inline at parse time, so the new role of this scheduled job is a
    self-healing safety net: scan for instruments where _current is
    staler than max(observations.ingested_at) and refresh those that
    drifted.

    On a healthy install this finds zero rows and exits in <100ms.

    The function name + ``JOB_OWNERSHIP_OBSERVATIONS_SYNC`` constant
    are preserved so existing scheduler config keeps working without
    a config migration. The job_runs audit row label is unchanged.

    Cadence kept on the daily 03:30 UTC slot (operator can flip to
    weekly via cadence config — repair sweeps don't need daily
    cadence on healthy systems but daily is cheap and catches
    drift faster).
    """
    from app.jobs.ownership_observations_repair import run_observations_repair_sweep

    with _tracked_job(JOB_OWNERSHIP_OBSERVATIONS_SYNC) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            stats = run_observations_repair_sweep(conn)
            conn.commit()

        tracker.row_count = sum(c.refreshed_rows for c in stats.per_category)
        logger.info(
            "ownership_observations_sync (repair-sweep): total_drifted=%d %s",
            stats.total_drifted,
            ", ".join(
                f"{c.category}=drifted{c.drifted_instruments}/refreshed{c.refreshed_rows}" for c in stats.per_category
            ),
        )


def ownership_observations_backfill() -> None:
    """One-shot legacy → ownership_*_observations backfill (#909).

    Calls ``ownership_observations_sync.sync_all`` with no ``since`` /
    ``limit`` so every legacy row is mirrored into the new
    ``ownership_*_observations`` tables and ``ownership_*_current`` is
    refreshed for every touched instrument. Idempotent: re-running on a
    populated install is a no-op (the underlying ``record_*_observation``
    helpers ON CONFLICT DO UPDATE on the natural keys, and the
    ``refresh_*_current`` helpers DELETE-then-INSERT under a per-instrument
    advisory lock).

    Why this is its own job rather than a one-time bootstrap script:

      - Operator can trigger from the UI / API after a fresh clone,
        before #909 ships, or after a parser-version bump.
      - Re-runnable on demand if a downstream regression empties
        ``_current`` again.
      - Auto-fires weekly as a defensive safety net so a future clone
        without an explicit operator action still self-heals.

    Cost on the dev panel today: ~427k insider_transactions +
    ~1280 insider_initial_holdings + ~5882 institutional_holdings +
    ~924 blockholder_filings + a few thousand DEF 14A rows + a few
    thousand treasury concept snapshots. Single-pass UPSERTs at
    psycopg INSERT throughput; expected wall-clock < 5 min on a warm
    dev box. No external network calls — pure DB work.

    Distinct from ``ownership_observations_sync`` (the daily repair
    sweep), which only refreshes ``_current`` and assumes
    ``_observations`` is already populated.
    """
    from app.services.ownership_observations_sync import sync_all

    with _tracked_job(JOB_OWNERSHIP_OBSERVATIONS_BACKFILL) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            result = sync_all(conn)

        tracker.row_count = result.total_observations_recorded
        logger.info(
            "ownership_observations_backfill: total_observations=%d "
            "insiders=%d institutions=%d blockholders=%d treasury=%d def14a=%d",
            result.total_observations_recorded,
            result.insiders.observations_recorded,
            result.institutions.observations_recorded,
            result.blockholders.observations_recorded,
            result.treasury.observations_recorded,
            result.def14a.observations_recorded,
        )


def cusip_universe_backfill() -> None:
    """Quarterly CUSIP coverage backfill (#914 / #841 PR3).

    Walks SEC's Official List of Section 13(f) Securities (the
    canonical free regulated source — CUSIP + issuer name + description,
    ~12k rows per quarter), fuzzy-matches each row against
    ``instruments.company_name``, INSERTs confident matches into
    ``external_identifiers``. Post-batch
    :func:`sweep_resolvable_unresolved_cusips` promotes previously-
    stranded 13F holdings into ``institutional_holdings``.
    """
    from app.services.sec_13f_securities_list import backfill_cusip_coverage

    with _tracked_job(JOB_CUSIP_UNIVERSE_BACKFILL) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            result = backfill_cusip_coverage(conn)

        tracker.row_count = result.inserted
        logger.info(
            "cusip_universe_backfill: list_rows=%d instruments_seen=%d "
            "inserted=%d already_mapped=%d unresolvable=%d ambiguous=%d "
            "conflict=%d sweep_promoted=%d sweep_rewashed=%d",
            result.list_rows,
            result.instruments_seen,
            result.inserted,
            result.skipped_already_mapped,
            result.tombstoned_unresolvable,
            result.tombstoned_ambiguous,
            result.tombstoned_conflict,
            result.sweep.promoted,
            result.sweep.rewashed,
        )


def sec_13f_quarterly_sweep(params: Mapping[str, Any]) -> None:
    """Quarterly sweep — walk every CIK in ``institutional_filers``
    (populated by ``sec_13f_filer_directory_sync`` #912) and ingest
    each filer's pending 13F-HR / 13F-HR/A accessions through the
    existing ``ingest_filer_13f`` per-filer pipeline (#913 / #841 PR2).

    Soft 6h deadline budget — already-ingested accessions are
    tombstoned in ``institutional_holdings_ingest_log`` so a
    deadline-interrupted sweep resumes the tail on the next fire.

    Honoured params (PR1c #1064):

    * ``min_period_of_report`` (date) — recency floor; accessions whose
      ``period_of_report`` is older are skipped. Default ``None`` = no
      floor (full historical sweep). Bootstrap stage 21 dispatches with
      ``today() - 380d`` so first-install completes in ~30-45 min
      instead of 11+h.
    * ``source_label`` (str, audit-only) — provenance tag on each
      ingested holding row. Default ``"sec_edgar_13f_directory"``;
      bootstrap stage 21 overrides with
      ``"sec_edgar_13f_directory_bootstrap"`` so audit history
      distinguishes bootstrap-bounded sweeps from the standalone
      weekly historical sweep. Operator-API path REJECTS this key
      via ``JOB_INTERNAL_KEYS`` allow-list (#1064 PR1a).
    """
    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.services.institutional_holdings import (
        ingest_all_active_filers,
        list_directory_filer_ciks,
    )

    deadline_seconds = settings.sec_13f_sweep_deadline_seconds
    min_period_of_report_param = params.get("min_period_of_report")
    min_period_of_report: date | None
    if min_period_of_report_param is None:
        min_period_of_report = None
    elif isinstance(min_period_of_report_param, date):
        min_period_of_report = min_period_of_report_param
    else:
        # Validator should have coerced; defensive against direct invocation.
        min_period_of_report = date.fromisoformat(str(min_period_of_report_param))
    source_label = str(params.get("source_label") or "sec_edgar_13f_directory")

    with _tracked_job(JOB_SEC_13F_QUARTERLY_SWEEP) as tracker:
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
            psycopg.connect(settings.database_url) as conn,
        ):
            ciks = list_directory_filer_ciks(conn)
            summaries = ingest_all_active_filers(
                conn,
                sec,
                ciks=ciks,
                deadline_seconds=deadline_seconds,
                source_label=source_label,
                min_period_of_report=min_period_of_report,
            )

        total_filers = len(ciks)
        processed = len(summaries)
        rows_upserted = sum(s.holdings_inserted for s in summaries)
        rows_skipped = sum(s.holdings_skipped_no_cusip for s in summaries)
        accessions_ingested = sum(s.accessions_ingested for s in summaries)
        tracker.row_count = rows_upserted
        logger.info(
            "sec_13f_quarterly_sweep: filers=%d processed=%d "
            "accessions_ingested=%d holdings_inserted=%d "
            "holdings_skipped_no_cusip=%d "
            "source_label=%s min_period_of_report=%s",
            total_filers,
            processed,
            accessions_ingested,
            rows_upserted,
            rows_skipped,
            source_label,
            min_period_of_report,
        )


# ---------------------------------------------------------------------------
# PR1c #1064 — promoted bodies (formerly bespoke wrappers in
# app/services/bootstrap_orchestrator.py).
# ---------------------------------------------------------------------------
#
# Each function below is registered in ``_INVOKERS`` directly under the
# new (PR1c) job name and dispatched by the bootstrap orchestrator via
# ``StageSpec.params``. The hardcoded values that used to live inside
# the deleted wrapper bodies now live in the bootstrap stage spec as
# data, so a single body serves both the bootstrap stage and the
# operator manual-trigger path.

# Default historical depth window for the broad filings sweep. Two
# years matches what most operators want for first-install ranking;
# the practical depth is bounded by SEC submissions.json's inline
# ``recent`` block (typically ~12 months) since
# ``SecFilingsProvider.list_filings`` does not currently walk
# secondary submissions pages.
_FILINGS_HISTORY_DAYS_DEFAULT = 730


def filings_history_seed(params: Mapping[str, Any]) -> None:
    """``_INVOKERS['filings_history_seed']`` — broad filings sweep.

    Promoted from the deleted ``bootstrap_filings_history_seed``
    bespoke wrapper (PR1c #1064). Walks each CIK-mapped tradable
    instrument's submissions.json via ``refresh_filings`` with the
    configured window and form-type allow-list, populating
    ``filing_events`` for every form type. The typed-form parsers
    later in the SEC lane (``sec_def14a_bootstrap``,
    ``sec_business_summary_bootstrap``,
    ``sec_insider_transactions_backfill``, etc.) read from
    ``filing_events`` and would otherwise no-op on a fresh DB.

    Honoured params:

    * ``days_back`` (int) — historical window. Default 730.
    * ``filing_types`` (multi_enum) — form-type allow-list. Default
      ``sorted(SEC_INGEST_KEEP_FORMS)`` (the curated three-tier set).
    * ``instrument_id`` (int) — narrow scope to a single instrument
      (operator triage path). Default ``None`` = full
      CIK-mapped-tradable universe.
    """
    from app.services.filings import SEC_INGEST_KEEP_FORMS

    days_back = int(params.get("days_back", _FILINGS_HISTORY_DAYS_DEFAULT))
    filing_types_param = params.get("filing_types") or sorted(SEC_INGEST_KEEP_FORMS)
    filing_types = list(filing_types_param)
    instrument_id_param = params.get("instrument_id")

    with _tracked_job(JOB_FILINGS_HISTORY_SEED) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            if instrument_id_param is not None:
                # Operator-triage path: single instrument, validated
                # CIK-mapped tradable. We resolve through the same
                # filter as the bulk path so an instrument without an
                # SEC primary CIK row is rejected rather than silently
                # producing zero filings.
                cik_rows = conn.execute(
                    """
                    SELECT i.symbol, i.instrument_id::text
                      FROM external_identifiers ei
                      JOIN instruments i ON i.instrument_id = ei.instrument_id
                     WHERE ei.provider = 'sec'
                       AND ei.identifier_type = 'cik'
                       AND ei.is_primary = TRUE
                       AND i.is_tradable = TRUE
                       AND i.instrument_id = %(iid)s
                    """,
                    {"iid": int(instrument_id_param)},
                ).fetchall()
            else:
                cik_rows = conn.execute(
                    """
                    SELECT i.symbol, i.instrument_id::text
                      FROM external_identifiers ei
                      JOIN instruments i ON i.instrument_id = ei.instrument_id
                     WHERE ei.provider = 'sec'
                       AND ei.identifier_type = 'cik'
                       AND ei.is_primary = TRUE
                       AND i.is_tradable = TRUE
                    """
                ).fetchall()

        if not cik_rows:
            logger.info("filings_history_seed: no CIK-mapped instruments; ensure daily_cik_refresh ran first")
            tracker.row_count = 0
            return

        instrument_ids = [row[1] for row in cik_rows]
        from_date = date.today() - timedelta(days=days_back)
        to_date = date.today()

        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
            psycopg.connect(settings.database_url) as conn,
        ):
            summary = refresh_filings(
                provider=sec,
                provider_name="sec",
                identifier_type="cik",
                conn=conn,
                instrument_ids=instrument_ids,
                start_date=from_date,
                end_date=to_date,
                filing_types=filing_types,
            )
        tracker.row_count = summary.filings_upserted
        logger.info(
            "filings_history_seed: instruments=%d filings_upserted=%d skipped=%d days_back=%d",
            summary.instruments_attempted,
            summary.filings_upserted,
            summary.instruments_skipped,
            days_back,
        )


def sec_first_install_drain(params: Mapping[str, Any]) -> None:
    """``_INVOKERS['sec_first_install_drain']`` — drain the SEC
    submissions.json manifest backlog for first-install bootstrap.

    Promoted from the deleted ``sec_first_install_drain_job`` bespoke
    wrapper (PR1c #1064). The underlying ``run_first_install_drain``
    takes an ``http_get`` callable that the wrapper adapts from the
    SecFilingsProvider's ResilientClient (``HttpGet = Callable[[str,
    dict[str, str]], tuple[int, bytes]]``).

    Honoured params:

    * ``max_subjects`` (int) — cap the number of CIKs processed.
      Default ``None`` = full universe. Operator-triage path for
      "iterate just N more CIKs from the queue head".

    Internal-only invariants (NOT operator-exposed per audit §6):
    ``follow_pagination=True`` (we want all pages), ``use_bulk_zip=False``
    (slow-connection fallback bypassed Phase A3).
    """
    from app.jobs.sec_first_install_drain import run_first_install_drain

    max_subjects_param = params.get("max_subjects")
    max_subjects = int(max_subjects_param) if max_subjects_param is not None else None

    with _tracked_job(JOB_SEC_FIRST_INSTALL_DRAIN) as tracker:
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
            psycopg.connect(settings.database_url) as conn,
        ):
            stats = run_first_install_drain(
                conn,
                http_get=_make_sec_http_get(sec),  # type: ignore[arg-type]
                follow_pagination=True,
                use_bulk_zip=False,
                max_subjects=max_subjects,
            )
        tracker.row_count = stats.manifest_rows_upserted
        logger.info(
            "sec_first_install_drain: ciks_processed=%d skipped=%d manifest_rows=%d errors=%d max_subjects=%s",
            stats.ciks_processed,
            stats.ciks_skipped,
            stats.manifest_rows_upserted,
            stats.errors,
            max_subjects,
        )


def sec_atom_fast_lane() -> None:
    """``_INVOKERS['sec_atom_fast_lane']`` — Layer 1 (5-min Atom).

    #1155 wiring. Polls SEC's getcurrent Atom feed, filters to
    (cik IN universe) + (form mapped to ManifestSource), UPSERTs
    sec_filing_manifest rows for the worker to drain. Idempotent —
    accession PK + ON CONFLICT preserves any in-flight ingest_status.
    """
    from app.jobs.sec_atom_fast_lane import run_atom_fast_lane

    with _tracked_job(JOB_SEC_ATOM_FAST_LANE) as tracker:
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
            psycopg.connect(settings.database_url) as conn,
        ):
            stats = run_atom_fast_lane(conn, http_get=_make_sec_http_get(sec))  # type: ignore[arg-type]
            conn.commit()
        tracker.row_count = stats.upserted
        logger.info(
            "sec_atom_fast_lane: feed=%d matched=%d upserted=%d unmapped_form=%d unknown_subject=%d",
            stats.feed_rows,
            stats.matched_in_universe,
            stats.upserted,
            stats.skipped_unmapped_form,
            stats.skipped_unknown_subject,
        )


def sec_daily_index_reconcile() -> None:
    """``_INVOKERS['sec_daily_index_reconcile']`` — Layer 2 (daily 04:00 UTC).

    #1155 wiring. Reads yesterday's daily-index master.idx, filters
    to (cik IN universe) + (form mapped to ManifestSource), UPSERTs
    sec_filing_manifest rows for accessions the Atom feed missed.

    NO ``_bootstrap_complete`` prereq + ``catch_up_on_boot=true``:
    JobRuntime evaluates ``catch_up_on_boot`` only at boot; a
    prereq-blocked catch-up cannot re-fire when bootstrap completes
    later. Without this exception a stack that boots mid-bootstrap
    loses yesterday's reconcile permanently. Daily-index against an
    empty universe is a natural no-op (subject_resolver filters every
    CIK).
    """
    from app.jobs.sec_daily_index_reconcile import run_daily_index_reconcile

    with _tracked_job(JOB_SEC_DAILY_INDEX_RECONCILE) as tracker:
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
            psycopg.connect(settings.database_url) as conn,
        ):
            stats = run_daily_index_reconcile(conn, http_get=_make_sec_http_get(sec))  # type: ignore[arg-type]
            conn.commit()
        tracker.row_count = stats.upserted
        logger.info(
            "sec_daily_index_reconcile: index=%d matched=%d upserted=%d unmapped_form=%d unknown_subject=%d",
            stats.index_rows,
            stats.matched_in_universe,
            stats.upserted,
            stats.skipped_unmapped_form,
            stats.skipped_unknown_subject,
        )


def sec_per_cik_poll() -> None:
    """``_INVOKERS['sec_per_cik_poll']`` — Layer 3 (hourly per-CIK).

    #1155 wiring. Reads ``data_freshness_index`` for subjects past
    their ``expected_next_at`` (poll path) AND past ``next_recheck_at``
    (recheck path for never_filed/error rows — #1155 G13). For each
    due subject calls submissions.json and UPSERTs new manifest rows.

    Bounded total budget: poll=2/3, recheck=~1/3 of ``max_subjects``
    (default 100 → 66+34).
    """
    from app.jobs.sec_per_cik_poll import run_per_cik_poll

    with _tracked_job(JOB_SEC_PER_CIK_POLL) as tracker:
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
            psycopg.connect(settings.database_url) as conn,
        ):
            stats = run_per_cik_poll(conn, http_get=_make_sec_http_get(sec))  # type: ignore[arg-type]
            conn.commit()
        tracker.row_count = stats.new_filings_recorded + stats.recheck_new_filings_recorded
        logger.info(
            "sec_per_cik_poll: poll_subjects=%d poll_new=%d errors=%d recheck_subjects=%d recheck_new=%d",
            stats.subjects_polled,
            stats.new_filings_recorded,
            stats.poll_errors,
            stats.recheck_subjects_polled,
            stats.recheck_new_filings_recorded,
        )


def sec_rebuild(params: Mapping[str, Any]) -> None:
    """``_INVOKERS['sec_rebuild']`` — operator manual triage (#1155).

    Resets manifest + scheduler rows for a scope and (default) runs a
    discovery pass via SEC submissions.json to fill missed accessions.
    The manifest worker drains the resulting pending rows.

    Honoured params (declared in ``MANUAL_TRIGGER_JOB_METADATA``):

    * ``instrument_id`` (int, optional) — issuer scope
    * ``filer_cik`` (str, optional) — institutional / blockholder filer scope
    * ``source`` (str, optional) — ManifestSource literal
    * ``discover`` (bool, default true) — run history-scan discovery pass

    At least one of instrument_id / filer_cik / source must be set;
    ``_resolve_scope`` raises ValueError otherwise (surfaces in
    ``job_runs.status='error'``).
    """
    from app.jobs.sec_rebuild import RebuildScope, run_sec_rebuild

    instrument_id_raw = params.get("instrument_id")
    filer_cik_raw = params.get("filer_cik")
    source_raw = params.get("source")
    discover = bool(params.get("discover", True))

    scope = RebuildScope(
        instrument_id=int(instrument_id_raw) if instrument_id_raw is not None else None,
        filer_cik=str(filer_cik_raw) if filer_cik_raw is not None else None,
        source=source_raw,  # type: ignore[arg-type]
    )

    with _tracked_job(JOB_SEC_REBUILD) as tracker:
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
            psycopg.connect(settings.database_url) as conn,
        ):
            stats = run_sec_rebuild(
                conn,
                scope,
                http_get=_make_sec_http_get(sec),  # type: ignore[arg-type]
                discover=discover,
            )
            conn.commit()
        tracker.row_count = stats.scope_triples
        logger.info(
            "sec_rebuild: triples=%d manifest_reset=%d scheduler_reset=%d discovery_new=%d",
            stats.scope_triples,
            stats.manifest_rows_reset,
            stats.scheduler_rows_reset,
            stats.discovery_new_manifest_rows,
        )


def _make_sec_http_get(sec_provider: object) -> Callable[[str, dict[str, str]], tuple[int, bytes]]:
    """Adapt ``SecFilingsProvider._http`` (a ``ResilientClient``) into
    the ``HttpGet`` callable shape the drain / poll / rebuild call
    sites consume (see ``app/providers/implementations/sec_submissions.py``).

    Lifted from the deleted ``sec_first_install_drain_job`` wrapper
    (PR1c #1064). The closure routes through the rate-limited shared
    client so SEC's 10 req/s bucket is honoured.
    """

    def _impl(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        response = sec_provider._http.get(url, headers=headers)  # type: ignore[attr-defined]
        return response.status_code, response.content

    return _impl


def sec_13f_filer_directory_sync() -> None:
    """Discovery sweep — populate ``institutional_filers`` from SEC's
    quarterly form.idx (#912 / #841 PR1).

    Walks the last 4 closed quarters' ``form.idx``, harvests every
    distinct 13F-HR / 13F-HR/A / 13F-NT filer CIK + canonical name,
    UPSERTs into ``institutional_filers``. ``filer_type`` resolved
    via the curated ETF list + N-CEN classifier (same priority as
    :func:`app.services.ncen_classifier.compose_filer_type`),
    defaulting to ``'INV'`` so the ≥95% non-NULL acceptance is
    structurally satisfied.

    Does NOT ingest holdings — that's PR2 (#913). This job builds
    the operand the next two PRs operate against.
    """
    from app.services.sec_13f_filer_directory import sync_filer_directory

    with _tracked_job(JOB_SEC_13F_FILER_DIRECTORY_SYNC) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            result = sync_filer_directory(conn)

        tracker.row_count = result.filers_inserted
        logger.info(
            "sec_13f_filer_directory_sync: quarters_attempted=%d "
            "quarters_failed=%d filers_seen=%d inserted=%d "
            "refreshed=%d skipped_empty_name=%d",
            result.quarters_attempted,
            result.quarters_failed,
            result.filers_seen,
            result.filers_inserted,
            result.filers_refreshed,
            result.skipped_empty_name,
        )


def sec_nport_filer_directory_sync() -> None:
    """Discovery sweep — populate ``sec_nport_filer_directory`` from
    SEC's quarterly form.idx (#963).

    Walks the last 4 closed quarters' ``form.idx``, harvests every
    distinct NPORT-P / NPORT-P/A filer CIK + canonical trust name,
    UPSERTs into ``sec_nport_filer_directory``. Sibling of
    :func:`sec_13f_filer_directory_sync` (#912) but for the disjoint
    N-PORT trust-CIK universe.

    Does NOT ingest holdings — that's ``sec_n_port_ingest`` reading
    off this directory.
    """
    from app.services.sec_nport_filer_directory import sync_nport_filer_directory

    with _tracked_job(JOB_SEC_NPORT_FILER_DIRECTORY_SYNC) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            result = sync_nport_filer_directory(conn)

        tracker.row_count = result.filers_inserted
        logger.info(
            "sec_nport_filer_directory_sync: quarters_attempted=%d "
            "quarters_failed=%d filers_seen=%d inserted=%d "
            "refreshed=%d skipped_empty_name=%d",
            result.quarters_attempted,
            result.quarters_failed,
            result.filers_seen,
            result.filers_inserted,
            result.filers_refreshed,
            result.skipped_empty_name,
        )


def sec_n_port_ingest() -> None:
    """Monthly NPORT-P fund-holdings sweep (#917 — Phase 3 PR1).

    Walks the fund-filer CIK universe and ingests each filer's pending
    NPORT-P / NPORT-P/A accessions into ``ownership_funds_observations``
    + ``ownership_funds_current``. Per-filer crashes isolated; soft
    deadline budget; resumable via ``n_port_ingest_log`` tombstones.

    Universe (post-#963): ``sec_nport_filer_directory`` — the dedicated
    RIC trust-CIK directory populated by
    :func:`sec_nport_filer_directory_sync`. Pre-#963 this read from
    ``institutional_filers`` (the 13F-manager CIK directory) which is
    the WRONG entity for N-PORT — N-PORT is filed by trust CIKs, not
    manager CIKs, leaving the standing job producing zero rows on dev
    until #919 worked around with a hardcoded panel-targeted CIK list.
    """
    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.services.n_port_ingest import ingest_all_fund_filers

    deadline_seconds = settings.sec_n_port_sweep_deadline_seconds

    with _tracked_job(JOB_SEC_N_PORT_INGEST) as tracker:
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
            psycopg.connect(settings.database_url) as conn,
        ):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cik
                    FROM sec_nport_filer_directory
                    ORDER BY last_seen_filed_at DESC NULLS LAST, cik
                    """
                )
                ciks = [str(row[0]).zfill(10) for row in cur.fetchall()]

            summaries = ingest_all_fund_filers(
                conn,
                sec,
                ciks=ciks,
                deadline_seconds=deadline_seconds,
                source_label="sec_n_port_ingest",
            )

        total_filers = len(ciks)
        processed = len(summaries)
        rows_upserted = sum(s.holdings_inserted for s in summaries)
        rows_skipped = sum(
            s.holdings_skipped_no_cusip
            + s.holdings_skipped_non_equity
            + s.holdings_skipped_short
            + s.holdings_skipped_non_share_units
            + s.holdings_skipped_zero_shares
            for s in summaries
        )
        accessions_ingested = sum(s.accessions_ingested for s in summaries)
        tracker.row_count = rows_upserted
        logger.info(
            "sec_n_port_ingest: filers=%d processed=%d accessions_ingested=%d holdings_inserted=%d holdings_skipped=%d",
            total_filers,
            processed,
            accessions_ingested,
            rows_upserted,
            rows_skipped,
        )


def cusip_extid_sweep() -> None:
    """Sweep ``unresolved_13f_cusips`` for rows whose CUSIP already
    matches an ``external_identifiers`` row, mark them
    ``resolved_via_extid``, and trigger 13F rewash so the previously
    stranded holdings land in ``institutional_holdings`` (#788 / #836).

    Closes the race-loss path between 13F-HR ingest (#730) and the
    CUSIP backfill (#740): when a 13F filing parses BEFORE the CUSIP
    backfill populates the issuer's ``external_identifiers`` row, the
    holding is tombstoned in ``unresolved_13f_cusips`` and never
    rejoins ``institutional_holdings`` even after the mapping later
    lands. Operator audit 2026-05-03 found 119 Fortune-100 names in
    that state — every blue-chip rollup is materially under-counted
    until this sweep runs.

    Cadence: daily 04:50 UTC, ~15 min after sec_def14a_ingest finishes
    so any extids that proxy ingest just published are visible. The
    sweep is cheap (one indexed JOIN; bounded to 1000 rows per pass)
    so daily is plenty — the backlog drains on the first run and
    subsequent passes only see new race-loss arrivals.
    """
    from app.services.cusip_resolver import sweep_resolvable_unresolved_cusips

    with _tracked_job(JOB_CUSIP_EXTID_SWEEP) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            report = sweep_resolvable_unresolved_cusips(conn)
            conn.commit()

        # ``promoted`` is the headline rowcount: how many backlog rows
        # we transitioned. The rewash counters are derivative — bookkept
        # in the log line so an operator can spot a regression where
        # rewash mass-defers, but tracker.row_count uses ``promoted``
        # for ops_monitor's spike detection.
        tracker.row_count = report.promoted
        logger.info(
            "cusip_extid_sweep: candidates=%d promoted=%d rewashed=%d rewash_deferred=%d rewash_failed=%d",
            report.candidates_seen,
            report.promoted,
            report.rewashed,
            report.rewash_deferred,
            report.rewash_failed,
        )


def sec_filing_documents_ingest() -> None:
    """Populate the filing_documents manifest table (#452).

    Captures the per-document list from each SEC filing's
    ``{accession}-index.json`` — primary doc + exhibits + XBRL +
    graphics + cover — as structured SQL rows so the long-tail
    ``data/raw/sec/sec_filing_*.json`` disk dump can be retired.
    """
    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.services.filing_documents import ingest_filing_documents

    with _tracked_job(JOB_SEC_FILING_DOCUMENTS_INGEST) as tracker:
        with (
            psycopg.connect(settings.database_url) as conn,
            SecFilingsProvider(user_agent=settings.sec_user_agent) as provider,
        ):
            result = ingest_filing_documents(conn, provider)

        tracker.row_count = result.documents_inserted
        logger.info(
            "sec_filing_documents_ingest: scanned=%d parsed=%d docs=%d fetch_errors=%d parse_misses=%d",
            result.filings_scanned,
            result.filings_parsed,
            result.documents_inserted,
            result.fetch_errors,
            result.parse_misses,
        )


def sec_8k_events_ingest() -> None:
    """Parse 8-K filings into structured SQL tables (#450).

    Complements the Item 8.01 dividend parser (#434) by capturing
    every 8-K's header, per-item bodies, and exhibits list. Runs
    hourly so material 8-Ks (officer departures, acquisition
    agreements, cybersecurity incidents) land in SQL within one
    cycle of hitting EDGAR.
    """
    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.services.eight_k_events import ingest_8k_events

    with _tracked_job(JOB_SEC_8K_EVENTS_INGEST) as tracker:
        with (
            psycopg.connect(settings.database_url) as conn,
            SecFilingsProvider(user_agent=settings.sec_user_agent) as provider,
        ):
            # #1045: prefetch 8-K primary docs via PipelinedSecFetcher
            # for 4-way concurrent fetches at the shared 7 req/s ceiling.
            result = ingest_8k_events(
                conn,
                provider,
                prefetch_urls=True,
                prefetch_user_agent=settings.sec_user_agent,
            )

        tracker.row_count = result.items_inserted
        logger.info(
            "sec_8k_events_ingest: scanned=%d parsed=%d items=%d fetch_errors=%d parse_misses=%d",
            result.filings_scanned,
            result.filings_parsed,
            result.items_inserted,
            result.fetch_errors,
            result.parse_misses,
        )


def sec_insider_transactions_backfill() -> None:
    """Round-robin backfill for instruments with deep Form 4 backlogs.

    The universe-wide hourly job (``sec_insider_transactions_ingest``)
    runs newest-first and is bounded at 500 filings per tick. An
    instrument with 400+ historical filings can sit starved for days
    because newer filings on other tickers saturate the budget every
    hour. This job targets the 25 instruments with the most
    un-ingested Form 4 candidates and clears up to 50 oldest filings
    per instrument per tick — the historical tail drains predictably
    without contention against the newest-first job.
    """
    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.services.insider_transactions import ingest_insider_transactions_backfill

    with _tracked_job(JOB_SEC_INSIDER_TRANSACTIONS_BACKFILL) as tracker:
        with (
            psycopg.connect(settings.database_url) as conn,
            SecFilingsProvider(user_agent=settings.sec_user_agent) as provider,
        ):
            totals = ingest_insider_transactions_backfill(conn, provider)

        tracker.row_count = totals["rows_inserted"]
        logger.info(
            "sec_insider_transactions_backfill: instruments=%d parsed=%d inserted=%d fetch_errors=%d parse_misses=%d",
            totals["instruments_processed"],
            totals["filings_parsed"],
            totals["rows_inserted"],
            totals["fetch_errors"],
            totals["parse_misses"],
        )
