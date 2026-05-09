"""Job runtime: APScheduler + manual trigger glue.

Owns one ``BackgroundScheduler`` for the lifetime of the FastAPI app.
The scheduler runs job functions on its own thread pool; the manual
trigger endpoint also routes through the same wrapper so a manual run
and a scheduled fire are indistinguishable from a tracking standpoint.

Features:

* All ``SCHEDULED_JOBS`` are registered with APScheduler cron triggers.
* Catch-up-on-boot fires overdue jobs (based on ``job_runs`` history).
* Prerequisite checks gate scheduled fires and catch-up.
* Manual triggers run on a dedicated ``ThreadPoolExecutor``.
* ``get_next_run_times()`` exposes live APScheduler fire times for the
  ``/system/jobs`` endpoint.

Why ``BackgroundScheduler`` and not ``AsyncIOScheduler``:

The job functions in ``app/workers/scheduler.py`` are synchronous
psycopg3 code that opens its own connections. ``AsyncIOScheduler``
would force every job to be wrapped in an executor anyway, and would
entangle the scheduler with the FastAPI event loop. A
``BackgroundScheduler`` runs jobs on its own thread pool, leaves the
event loop alone, and matches the synchronous shape of the jobs.

Why a separate ``ThreadPoolExecutor`` for manual triggers:

A manual trigger should return immediately (202 Accepted) and run the
job in the background. Routing manual runs through ``add_job(...,
trigger='date', run_date=now())`` would also work, but mixes the
"recurring schedule" namespace with one-shot manual runs and makes
the scheduled-jobs view harder to reason about. A small dedicated
``ThreadPoolExecutor`` (max_workers=1) is the simpler and more
honest tool: at most one manual run is in flight at a time, the
scheduler's recurring jobs run on their own pool, and the per-job
``JobLock`` still serialises both code paths against each other.
"""

from __future__ import annotations

import contextvars
import logging
import os
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Final

import psycopg
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import settings
from app.jobs.locks import JobAlreadyRunning, JobLock
from app.services.ops_monitor import fetch_latest_successful_runs, record_job_skip
from app.services.process_stop import acquire_prelude_lock
from app.workers.scheduler import (
    JOB_ATTRIBUTION_SUMMARY,
    JOB_CUSIP_EXTID_SWEEP,
    JOB_CUSIP_UNIVERSE_BACKFILL,
    JOB_DAILY_CANDLE_REFRESH,
    JOB_DAILY_CIK_REFRESH,
    JOB_DAILY_FINANCIAL_FACTS,
    JOB_DAILY_PORTFOLIO_SYNC,
    JOB_DAILY_RESEARCH_REFRESH,
    JOB_DAILY_TAX_RECONCILIATION,
    JOB_ETORO_LOOKUPS_REFRESH,
    JOB_EXCHANGES_METADATA_REFRESH,
    JOB_EXECUTE_APPROVED_ORDERS,
    JOB_FUNDAMENTALS_SYNC,
    JOB_FX_RATES_REFRESH,
    JOB_MONITOR_POSITIONS,
    JOB_MONTHLY_REPORT,
    JOB_MORNING_CANDIDATE_REVIEW,
    JOB_NIGHTLY_UNIVERSE_SYNC,
    JOB_ORCHESTRATOR_FULL_SYNC,
    JOB_ORCHESTRATOR_HIGH_FREQUENCY_SYNC,
    JOB_OWNERSHIP_OBSERVATIONS_BACKFILL,
    JOB_OWNERSHIP_OBSERVATIONS_SYNC,
    JOB_RAW_DATA_RETENTION_SWEEP,
    JOB_RETRY_DEFERRED,
    JOB_SEC_8K_EVENTS_INGEST,
    JOB_SEC_13F_FILER_DIRECTORY_SYNC,
    JOB_SEC_13F_QUARTERLY_SWEEP,
    JOB_SEC_BUSINESS_SUMMARY_BOOTSTRAP,
    JOB_SEC_BUSINESS_SUMMARY_INGEST,
    JOB_SEC_DEF14A_BOOTSTRAP,
    JOB_SEC_DEF14A_INGEST,
    JOB_SEC_DIVIDEND_CALENDAR_INGEST,
    JOB_SEC_FILING_DOCUMENTS_INGEST,
    JOB_SEC_FORM3_INGEST,
    JOB_SEC_INSIDER_TRANSACTIONS_BACKFILL,
    JOB_SEC_INSIDER_TRANSACTIONS_INGEST,
    JOB_SEC_N_PORT_INGEST,
    JOB_SEC_NPORT_FILER_DIRECTORY_SYNC,
    JOB_SEED_COST_MODELS,
    JOB_WEEKLY_REPORT,
    SCHEDULED_JOBS,
    Cadence,
    ScheduledJob,
    attribution_summary_job,
    compute_next_run,
    cusip_extid_sweep,
    cusip_universe_backfill,
    daily_candle_refresh,
    daily_cik_refresh,
    daily_financial_facts,
    daily_portfolio_sync,
    daily_research_refresh,
    daily_tax_reconciliation,
    etoro_lookups_refresh,
    exchanges_metadata_refresh,
    execute_approved_orders,
    fundamentals_sync,
    fx_rates_refresh,
    monitor_positions_job,
    monthly_report,
    morning_candidate_review,
    nightly_universe_sync,
    orchestrator_full_sync,
    orchestrator_high_frequency_sync,
    ownership_observations_backfill,
    ownership_observations_sync,
    raw_data_retention_sweep,
    retry_deferred_recommendations_job,
    sec_8k_events_ingest,
    sec_13f_filer_directory_sync,
    sec_13f_quarterly_sweep,
    sec_business_summary_bootstrap,
    sec_business_summary_ingest,
    sec_def14a_bootstrap,
    sec_def14a_ingest,
    sec_dividend_calendar_ingest,
    sec_filing_documents_ingest,
    sec_form3_ingest,
    sec_insider_transactions_backfill,
    sec_insider_transactions_ingest,
    sec_n_port_ingest,
    sec_nport_filer_directory_sync,
    seed_cost_models,
    weekly_report,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Job invoker registry
# ---------------------------------------------------------------------------
#
# Maps job names from ``SCHEDULED_JOBS`` to the actual callable that
# performs the work. A manual trigger or a scheduled fire is a single
# call into the registry.
#
# Keeping the registry as a single ``dict[str, Callable[[], None]]``
# rather than a class hierarchy is deliberate: every job has the same
# zero-argument shape, the wrapper is identical, and there is nothing
# job-specific to abstract over yet. If a future job needs arguments
# (it should not, per the design notes in #13), reach for a richer
# shape then -- not pre-emptively.
#
# Drift guard: ``JobRuntime.start()`` registers only the intersection of
# this map with ``SCHEDULED_JOBS``, and ``test_jobs_runtime.py`` asserts
# the two are equal so a job declared in the registry without an invoker
# (or vice versa) fails the test rather than silently no-opping.

_INVOKERS: Final[dict[str, Callable[[], None]]] = {
    JOB_NIGHTLY_UNIVERSE_SYNC: nightly_universe_sync,
    JOB_DAILY_CANDLE_REFRESH: daily_candle_refresh,
    JOB_ETORO_LOOKUPS_REFRESH: etoro_lookups_refresh,
    JOB_EXCHANGES_METADATA_REFRESH: exchanges_metadata_refresh,
    JOB_FX_RATES_REFRESH: fx_rates_refresh,
    JOB_DAILY_RESEARCH_REFRESH: daily_research_refresh,
    JOB_DAILY_PORTFOLIO_SYNC: daily_portfolio_sync,
    JOB_EXECUTE_APPROVED_ORDERS: execute_approved_orders,
    JOB_MORNING_CANDIDATE_REVIEW: morning_candidate_review,
    JOB_FUNDAMENTALS_SYNC: fundamentals_sync,
    JOB_DAILY_TAX_RECONCILIATION: daily_tax_reconciliation,
    JOB_RETRY_DEFERRED: retry_deferred_recommendations_job,
    JOB_MONITOR_POSITIONS: monitor_positions_job,
    JOB_ATTRIBUTION_SUMMARY: attribution_summary_job,
    JOB_SEED_COST_MODELS: seed_cost_models,
    JOB_WEEKLY_REPORT: weekly_report,
    JOB_MONTHLY_REPORT: monthly_report,
    JOB_ORCHESTRATOR_FULL_SYNC: orchestrator_full_sync,
    JOB_ORCHESTRATOR_HIGH_FREQUENCY_SYNC: orchestrator_high_frequency_sync,
    JOB_RAW_DATA_RETENTION_SWEEP: raw_data_retention_sweep,
    JOB_SEC_BUSINESS_SUMMARY_INGEST: sec_business_summary_ingest,
    JOB_SEC_BUSINESS_SUMMARY_BOOTSTRAP: sec_business_summary_bootstrap,
    JOB_SEC_DIVIDEND_CALENDAR_INGEST: sec_dividend_calendar_ingest,
    JOB_SEC_INSIDER_TRANSACTIONS_INGEST: sec_insider_transactions_ingest,
    JOB_SEC_INSIDER_TRANSACTIONS_BACKFILL: sec_insider_transactions_backfill,
    JOB_SEC_FORM3_INGEST: sec_form3_ingest,
    JOB_SEC_DEF14A_INGEST: sec_def14a_ingest,
    JOB_SEC_DEF14A_BOOTSTRAP: sec_def14a_bootstrap,
    JOB_SEC_8K_EVENTS_INGEST: sec_8k_events_ingest,
    JOB_SEC_FILING_DOCUMENTS_INGEST: sec_filing_documents_ingest,
    JOB_CUSIP_EXTID_SWEEP: cusip_extid_sweep,
    JOB_CUSIP_UNIVERSE_BACKFILL: cusip_universe_backfill,
    JOB_OWNERSHIP_OBSERVATIONS_SYNC: ownership_observations_sync,
    JOB_OWNERSHIP_OBSERVATIONS_BACKFILL: ownership_observations_backfill,
    JOB_SEC_13F_FILER_DIRECTORY_SYNC: sec_13f_filer_directory_sync,
    JOB_SEC_13F_QUARTERLY_SWEEP: sec_13f_quarterly_sweep,
    JOB_SEC_NPORT_FILER_DIRECTORY_SYNC: sec_nport_filer_directory_sync,
    JOB_SEC_N_PORT_INGEST: sec_n_port_ingest,
    # Registered for #994 (first-install bootstrap orchestrator) — these
    # were callable as scheduled-only paths before but the orchestrator
    # needs them in _INVOKERS so it can dispatch them via JobLock.
    # ``daily_cik_refresh`` seeds external_identifiers SEC CIK rows;
    # ``daily_financial_facts`` walks the SEC daily master-index.
    # Both are also wired into SCHEDULED_JOBS unchanged.
    JOB_DAILY_CIK_REFRESH: daily_cik_refresh,
    JOB_DAILY_FINANCIAL_FACTS: daily_financial_facts,
}


# ---------------------------------------------------------------------------
# Bootstrap orchestrator invokers (#994)
# ---------------------------------------------------------------------------
#
# Imported lazily and added to ``_INVOKERS`` below so the runtime
# module's own import does not pull in the bootstrap orchestrator
# transitively (the orchestrator imports from this module via
# ``from app.jobs.runtime import _INVOKERS`` at call time).
from app.services import bootstrap_orchestrator as _bootstrap_orchestrator  # noqa: E402
from app.services import sec_bulk_download as _sec_bulk_download  # noqa: E402

# #1021 — bulk-archive download stage A3 of the bulk-datasets-first
# bootstrap (#1020). Registered so the orchestrator can dispatch it.
_INVOKERS[_sec_bulk_download.JOB_SEC_BULK_DOWNLOAD] = _sec_bulk_download.sec_bulk_download_job
_INVOKERS[_bootstrap_orchestrator.JOB_BOOTSTRAP_ORCHESTRATOR] = _bootstrap_orchestrator.run_bootstrap_orchestrator
_INVOKERS[_bootstrap_orchestrator.JOB_BOOTSTRAP_FILINGS_HISTORY_SEED] = (
    _bootstrap_orchestrator.bootstrap_filings_history_seed
)
_INVOKERS[_bootstrap_orchestrator.JOB_SEC_FIRST_INSTALL_DRAIN] = _bootstrap_orchestrator.sec_first_install_drain_job
# #1008 — recency-bounded 13F sweep for first-install bootstrap.
_INVOKERS[_bootstrap_orchestrator.JOB_BOOTSTRAP_SEC_13F_RECENT_SWEEP] = (
    _bootstrap_orchestrator.bootstrap_sec_13f_recent_sweep_job
)

# ---------------------------------------------------------------------------
# Bulk-archive Phase C ingester invokers (#1027 — #1020)
# ---------------------------------------------------------------------------
# Each ingester is a zero-arg wrapper that opens its own connection
# and reads cached archives from ``resolve_data_dir() / "sec" / "bulk"``.
# Skip silently if the archive is missing (slow-connection fallback
# bypassed Phase A3).
from app.services import sec_bulk_download as _sec_bulk_download  # noqa: E402
from app.services import sec_bulk_orchestrator_jobs as _bulk_jobs  # noqa: E402
from app.services import sec_submissions_files_walk as _files_walk  # noqa: E402

# Phase A3 — bulk archive download (#1021 / #1020). Registered here
# so PR7 is self-consistent if PR #1029 has not yet landed; the
# duplicate dict-key assignment when both PRs are merged is a no-op.
_INVOKERS[_sec_bulk_download.JOB_SEC_BULK_DOWNLOAD] = _sec_bulk_download.sec_bulk_download_job
_INVOKERS[_bulk_jobs.JOB_SEC_SUBMISSIONS_INGEST] = _bulk_jobs.sec_submissions_ingest_job
_INVOKERS[_bulk_jobs.JOB_SEC_COMPANYFACTS_INGEST] = _bulk_jobs.sec_companyfacts_ingest_job
_INVOKERS[_bulk_jobs.JOB_SEC_13F_INGEST_FROM_DATASET] = _bulk_jobs.sec_13f_ingest_from_dataset_job
_INVOKERS[_bulk_jobs.JOB_SEC_INSIDER_INGEST_FROM_DATASET] = _bulk_jobs.sec_insider_ingest_from_dataset_job
_INVOKERS[_bulk_jobs.JOB_SEC_NPORT_INGEST_FROM_DATASET] = _bulk_jobs.sec_nport_ingest_from_dataset_job
_INVOKERS[_files_walk.JOB_SEC_SUBMISSIONS_FILES_WALK] = _files_walk.sec_submissions_files_walk_job


# Public registry of valid job names. The API layer (#719) imports this
# to validate ``POST /jobs/{name}/run`` before writing a queue row, so
# unknown names return 404 from the API rather than landing as a
# ``rejected`` row the operator must reconcile.
VALID_JOB_NAMES: Final[frozenset[str]] = frozenset(_INVOKERS.keys())


# ---------------------------------------------------------------------------
# Tracked-job prelude (#1071 — admin control hub PR3)
# ---------------------------------------------------------------------------
#
# Spec: docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md
#       §Full-wash execution fence + §Scheduled run interaction.
#
# Every entry path that opens a ``job_runs`` row (scheduled fire via
# ``_wrap_invoker``, manual trigger via ``_run_manual``) routes through
# ``run_with_prelude`` first. The prelude opens ONE transaction that:
#
#   1. acquires ``pg_advisory_xact_lock(hashtext(process_id)::bigint)``
#      — same key as the full-wash trigger, so the fence-check + active
#      marker publish happens atomically against any concurrent path,
#   2. checks the durable full-wash fence on
#      ``pending_job_requests`` (sql/138 partial UNIQUE),
#   3. writes the ``job_runs`` INSERT — either ``status='running'`` (no
#      fence held) or ``status='skipped'`` with
#      ``error_msg='full-wash in progress for this process'`` (fence
#      held). R5-W1: the skip row is COMMITTED, not rolled back, so the
#      audit trail survives.
#
# The pre-allocated ``run_id`` is stashed in a ``ContextVar`` so the
# inner ``_tracked_job`` (in ``app/workers/scheduler.py``) reuses it
# rather than opening a second ``job_runs`` row. This is R5-W2 — one
# writer per run, no double-write between the prelude tx and the
# tracker tx.
#
# Direct invocation of ``_tracked_job`` outside of the prelude
# wrappers (legacy / tests) still works — the tracker falls back to the
# pre-#1071 ``record_job_start`` path when the ContextVar is unset.

_FENCE_HELD_ERROR_MSG = "full-wash in progress for this process"


# Jobs whose invokers do NOT use ``_tracked_job`` and therefore must not
# go through the lock+fence prelude (Codex round 2). The prelude opens a
# ``job_runs`` row that the invoker never finalises — orphan-row source.
# These jobs own their own canonical state machine outside ``job_runs``:
#
# * ``bootstrap_orchestrator`` writes ``bootstrap_state`` + ``bootstrap_runs``
#   directly. The bootstrap fence is ``bootstrap_state.status='running'``,
#   not the ``pending_job_requests`` partial UNIQUE — the trigger handler's
#   atomic lock+precondition+INSERT is what enforces "no two bootstrap
#   triggers in flight"; the worker prelude has nothing to add.
#
# Opting out keeps the runtime honest: the queue-row fence semantics still
# enforce against scheduled-job mechanisms (where ``process_id`` ==
# ``job_name``), and bootstrap stays governed by its own state machine.

_PRELUDE_OPT_OUT_JOBS: frozenset[str] = frozenset({_bootstrap_orchestrator.JOB_BOOTSTRAP_ORCHESTRATOR})


# Carries the prelude-allocated run_id into ``_tracked_job``. Module-
# scoped ContextVar so async refactors stay safe; under the current
# synchronous BackgroundScheduler / ThreadPoolExecutor model each worker
# thread runs one invoker at a time and the var lives only for the
# duration of that invoker call.
_prelude_run_id: contextvars.ContextVar[int | None] = contextvars.ContextVar("_prelude_run_id", default=None)


def consume_prelude_run_id() -> int | None:
    """Pop the prelude-allocated ``job_runs.run_id`` for this invoker.

    ``_tracked_job`` calls this on entry. Returns the pre-allocated id
    (and clears it so a nested ``_tracked_job`` does not reuse), or
    ``None`` when the invoker was started outside the prelude wrappers
    (legacy direct call / tests). The ``None`` case falls back to the
    pre-#1071 ``record_job_start`` path inside ``_tracked_job``.
    """
    run_id = _prelude_run_id.get()
    if run_id is not None:
        _prelude_run_id.set(None)
    return run_id


def _run_prelude(
    database_url: str,
    job_name: str,
    *,
    bypass_fence_check: bool = False,
    linked_request_id: int | None = None,
) -> int | None:
    """One-tx prelude: acquire lock, check fence, write job_runs INSERT.

    Returns the ``run_id`` of the newly opened ``job_runs`` row when no
    fence is held (or ``bypass_fence_check`` is True). Returns ``None``
    when the fence rejects the run — the caller MUST NOT invoke the
    underlying job in that case (the ``status='skipped'`` row is already
    committed for audit).

    Process_id == job_name for scheduled jobs. The advisory-lock key is
    ``hashtext(process_id)::bigint`` so the same key is acquired by every
    path that mutates this process's state (full-wash trigger, Iterate
    handler, scheduled prelude).

    ``linked_request_id`` is populated on both branches (running + skipped)
    when supplied so boot-recovery's ``reset_stale_in_flight`` NOT EXISTS
    clause sees the terminal/in-flight job_runs row and suppresses
    double-replay of the queue request.
    """
    process_id = job_name
    # autocommit=False so ``BEGIN`` opens an explicit top-level tx. The
    # default ``with conn.transaction():`` block COMMITS on clean exit
    # and ROLLBACKs on exception — same shape as the bootstrap cancel
    # path and the snapshot_read read path.
    fence_held = False
    fence_holder = process_id
    with psycopg.connect(database_url) as conn:
        with conn.transaction():
            acquire_prelude_lock(conn, process_id)
            # Lazy-import to avoid a top-level cycle (api/processes
            # imports app.jobs.runtime.VALID_JOB_NAMES).
            from app.services.processes.watermarks import (
                acquire_shared_source_locks as _acquire_shared_source_locks,
            )
            from app.services.processes.watermarks import (
                freshness_source_for as _freshness_source_for,
            )
            from app.services.processes.watermarks import (
                jobs_sharing_freshness_source,
                jobs_sharing_manifest_source,
            )
            from app.services.processes.watermarks import (
                manifest_source_for as _manifest_source_for,
            )

            # Codex pre-push round 7 BLOCKING: take source-keyed
            # advisory locks BEFORE the fence-check query. A full-wash
            # trigger holds these locks while it INSERTs the durable
            # fence row; the prelude blocks here until that trigger
            # commits, then sees the committed fence row when it
            # queries below. Without these locks, the prelude can read
            # the pre-INSERT state, write ``job_runs.running``, and
            # start the body against the just-reset shared scheduler
            # source.
            _acquire_shared_source_locks(conn, process_id=process_id)

            with conn.cursor() as cur:
                if not bypass_fence_check:
                    fence_candidates: list[str] = [process_id]
                    fresh = _freshness_source_for(process_id)
                    if fresh is not None:
                        for sibling in jobs_sharing_freshness_source(fresh):
                            if sibling != process_id:
                                fence_candidates.append(sibling)
                    manifest = _manifest_source_for(process_id)
                    if manifest is not None:
                        for sibling in jobs_sharing_manifest_source(manifest):
                            if sibling != process_id and sibling not in fence_candidates:
                                fence_candidates.append(sibling)
                    # PR4 #1075 fix (Codex review): a sibling job sharing
                    # the same scheduler source can hold the fence under
                    # a different ``process_id``. Probe each candidate
                    # so an APScheduler fire of ``fundamentals_sync`` self-
                    # skips while ``daily_financial_facts`` has a queued
                    # full-wash over ``sec_xbrl_facts``.
                    cur.execute(
                        """
                        SELECT process_id
                          FROM pending_job_requests
                         WHERE process_id = ANY(%s)
                           AND mode       = 'full_wash'
                           AND status     IN ('pending', 'claimed', 'dispatched')
                         LIMIT 1
                        """,
                        (fence_candidates,),
                    )
                    holder_row = cur.fetchone()
                    if holder_row is not None:
                        fence_held = True
                        fence_holder = str(holder_row[0])
                if fence_held:
                    # When a sibling holds the fence, surface the holder
                    # in the audit row so the operator can see WHY this
                    # job skipped (e.g. ``fundamentals_sync`` skipped
                    # because ``daily_financial_facts`` is mid-wash on
                    # the shared XBRL source).
                    error_msg = (
                        _FENCE_HELD_ERROR_MSG
                        if fence_holder == process_id
                        else f"full-wash in progress on shared scheduler source (held by {fence_holder!r})"
                    )
                    cur.execute(
                        """
                        INSERT INTO job_runs (
                            job_name, started_at, finished_at, status, row_count,
                            error_msg, linked_request_id
                        ) VALUES (
                            %s, now(), now(), 'skipped', 0, %s, %s
                        )
                        RETURNING run_id
                        """,
                        (job_name, error_msg, linked_request_id),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO job_runs (
                            job_name, started_at, status, linked_request_id
                        ) VALUES (%s, now(), 'running', %s)
                        RETURNING run_id
                        """,
                        (job_name, linked_request_id),
                    )
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError("prelude: job_runs INSERT returned no row")
                run_id = int(row[0])
    if fence_held:
        logger.info(
            "prelude: skipping %r — %s (job_runs.run_id=%d committed)",
            job_name,
            _FENCE_HELD_ERROR_MSG,
            run_id,
        )
        return None
    return run_id


def run_with_prelude(
    database_url: str,
    job_name: str,
    invoker: Callable[[], None],
    *,
    bypass_fence_check: bool = False,
    linked_request_id: int | None = None,
) -> bool:
    """Run ``invoker`` after the lock+fence prelude.

    Returns True when the invoker was called, False when the prelude
    wrote a ``status='skipped'`` row and the invoker was not called
    (full-wash fence held). Caller uses the return to decide queue-row
    transitions — a skipped run must NOT be marked completed (PR #1072
    review BLOCKING — `mark_request_completed` after skip).

    No-op when the fence is held. The caller (``_wrap_invoker`` /
    ``_run_manual``) does not need to handle the skip path — this
    function returns cleanly and the invoker is never called.

    ``bypass_fence_check=True`` skips the full-wash fence query: this is
    the path used when the listener dispatches a ``mode='full_wash'``
    request — the worker IS the fence holder, so the fence row matches
    its own request_id. Without bypass the worker would self-skip and
    the full-wash work would never run. The advisory lock is still
    acquired so the prelude serialises against concurrent triggers.

    Prelude failure handling: any exception inside the prelude
    propagates to the caller — we do NOT fall through to the invoker.
    The prelude is the FENCE for full-wash safety, not an opportunistic
    telemetry write; silently running the invoker on prelude failure
    would let a scheduled / iterate run race past an active full-wash
    when the fence query happens to error. The caller (``_wrap_invoker``
    / ``_run_manual``) catches Exception around this call and logs the
    failure.

    ``linked_request_id`` (when supplied — the listener-dispatched
    manual_job path) is written into the new ``job_runs.linked_request_id``
    so boot-recovery's ``reset_stale_in_flight`` NOT EXISTS clause can
    suppress double-replay of completed runs. Scheduled fires pass
    ``None``.
    """
    run_id = _run_prelude(
        database_url,
        job_name,
        bypass_fence_check=bypass_fence_check,
        linked_request_id=linked_request_id,
    )
    if run_id is None:
        return False  # fence held; skipped row already committed
    token = _prelude_run_id.set(run_id)
    try:
        invoker()
    finally:
        _prelude_run_id.reset(token)
    return True


class UnknownJob(KeyError):
    """Raised when a manual trigger names a job not in the invoker registry."""

    def __init__(self, job_name: str) -> None:
        super().__init__(job_name)
        self.job_name = job_name


# ---------------------------------------------------------------------------
# Cadence -> APScheduler trigger
# ---------------------------------------------------------------------------


def _trigger_for(cadence: Cadence) -> CronTrigger:
    """Translate a declared :class:`Cadence` into an APScheduler trigger.

    The translation is a pure mapping -- no defaults, no fallbacks --
    so a future cadence kind that is not handled raises ``ValueError``
    rather than silently picking a wrong default.
    """
    if cadence.kind == "every_n_minutes":
        return CronTrigger(minute=f"*/{cadence.interval_minutes}", timezone="UTC")
    if cadence.kind == "hourly":
        return CronTrigger(minute=cadence.minute, timezone="UTC")
    if cadence.kind == "daily":
        return CronTrigger(hour=cadence.hour, minute=cadence.minute, timezone="UTC")
    if cadence.kind == "weekly":
        # APScheduler day_of_week: mon..sun. Cadence weekday: 0=Mon..6=Sun.
        weekday_names = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
        return CronTrigger(
            day_of_week=weekday_names[cadence.weekday],
            hour=cadence.hour,
            minute=cadence.minute,
            timezone="UTC",
        )
    if cadence.kind == "monthly":
        return CronTrigger(
            day=cadence.day,
            hour=cadence.hour,
            minute=cadence.minute,
            timezone="UTC",
        )
    raise ValueError(f"unsupported cadence kind: {cadence.kind!r}")


# ---------------------------------------------------------------------------
# JobRuntime
# ---------------------------------------------------------------------------


class JobRuntime:
    """Owns the scheduler + manual-trigger executor for one app instance.

    Built once during FastAPI lifespan startup, attached to
    ``app.state.job_runtime``, and shut down before the connection
    pool closes so any in-flight job can still write to ``job_runs``.

    The default constructor wires the production registry. Tests can
    inject a custom invoker map via the ``invokers`` argument so they
    do not need to bring up real provider clients.
    """

    def __init__(
        self,
        *,
        database_url: str | None = None,
        invokers: dict[str, Callable[[], None]] | None = None,
    ) -> None:
        self._database_url = database_url or settings.database_url
        # Copy so callers cannot mutate after construction.
        self._invokers: dict[str, Callable[[], None]] = dict(invokers if invokers is not None else _INVOKERS)
        # Per-job in-process lock for synchronous 409 detection on
        # manual triggers. The advisory ``JobLock`` (Postgres) is the
        # cross-process source of truth and is acquired on the worker
        # thread; this in-process lock is what lets ``trigger()``
        # return 409 *synchronously* to the API caller without ever
        # touching the database connection on the request thread.
        # See PR #131 round 1 review (BLOCKING 1) for the rationale --
        # we deliberately avoid handing a ``psycopg.Connection`` across
        # threads, even sequentially, because the assumption that the
        # handoff is safe is load-bearing and untested.
        self._inflight: dict[str, threading.Lock] = {name: threading.Lock() for name in self._invokers}
        self._scheduler = BackgroundScheduler(
            timezone="UTC",
            job_defaults={
                # Collapse multiple missed fires of the same recurring
                # job into a single run. Without this a scheduler that
                # restarts after a long downtime would attempt to fire
                # every missed instance.
                "coalesce": True,
                # ``misfire_grace_time=1`` -- the smallest positive
                # integer APScheduler accepts (0 raises TypeError).
                # Combined with the absence of a persistent jobstore,
                # a fire that is more than 1 second late is dropped.
                # Catch-up is driven by ``_catch_up()`` reading
                # ``job_runs``, not by APScheduler grace windows.
                "misfire_grace_time": 1,
                # One concurrent instance per job. The per-job
                # advisory lock is the source of truth for
                # serialisation; this is a defensive second layer.
                "max_instances": 1,
            },
        )
        # Manual-trigger executor sized so that distinct jobs do NOT
        # queue behind each other -- one slot per wired invoker means
        # every wired job can be in flight simultaneously without
        # head-of-line blocking. The per-job in-process lock above
        # already prevents two instances of the *same* job from
        # running, so a larger pool only ever buys "unrelated jobs
        # run concurrently", which is the correct semantics: a 202
        # response means the job is being executed now, not queued.
        # See PR #131 round 1 review (BLOCKING 2).
        self._manual_executor = ThreadPoolExecutor(
            max_workers=max(1, len(self._invokers)),
            thread_name_prefix="job-manual",
        )
        self._started = False
        # Name → ScheduledJob lookup for prerequisite checks in the
        # scheduled-fire path.
        self._job_registry: dict[str, ScheduledJob] = {
            job.name: job for job in SCHEDULED_JOBS if job.name in self._invokers
        }

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        """Register every wired invoker with the scheduler and start it.

        Only jobs whose names appear in *both* ``SCHEDULED_JOBS`` and
        ``self._invokers`` are registered. The intersection is the
        right semantics for the PR-by-PR slicing -- the registry is
        the declared truth, the invoker map is what is currently
        wired, and the runtime fires the overlap.
        """
        if self._started:
            raise RuntimeError("JobRuntime.start() called twice")
        registered = 0
        for job in SCHEDULED_JOBS:
            invoker = self._invokers.get(job.name)
            if invoker is None:
                continue
            self._scheduler.add_job(
                func=self._wrap_invoker(job.name, invoker),
                trigger=_trigger_for(job.cadence),
                id=f"recurring:{job.name}",
                name=job.name,
                replace_existing=True,
            )
            registered += 1
        self._scheduler.start()
        self._started = True
        logger.info(
            "JobRuntime started: registered=%d wired=%s",
            registered,
            sorted(self._invokers.keys()),
        )
        # Log next-fire times for operator visibility.
        for name, nrt in self.get_next_run_times().items():
            logger.info("  %s → next fire at %s", name, nrt)
        # Gate: EBULL_SKIP_CATCH_UP=1 skips the catch-up loop. Used by
        # tests/conftest.py so pytest's TestClient(app) lifespan entries
        # don't fire overdue APScheduler jobs against the dev DB (300s+
        # teardown waits otherwise).
        #
        # Gated at the call site in start() (not inside _catch_up() body)
        # so direct unit tests in TestCatchUpOnBoot that call
        # rt._catch_up() bypass this gate and continue to exercise the
        # catch-up loop in isolation.
        if os.environ.get("EBULL_SKIP_CATCH_UP") == "1":
            logger.debug("EBULL_SKIP_CATCH_UP=1; skipping catch-up on boot")
        else:
            self._catch_up()

    def _catch_up(self) -> None:
        """Fire overdue jobs after startup (fire-and-forget).

        For each registered job with ``catch_up_on_boot=True``:

        * If the job has never run successfully, it is overdue.
        * If the job's last successful run's next scheduled fire
          (per ``compute_next_run``) is at or before ``now``, it is
          overdue.

        Overdue jobs whose prerequisite check returns ``False`` are
        skipped with a ``job_runs`` row of status='skipped' and a
        single INFO log line.

        The DB query is a single round trip (one SELECT for all job
        names). The connection is opened, used, and closed within this
        method — it is not shared with any other thread.
        """
        # Build a lookup of registered jobs that opt in to catch-up.
        catch_up_jobs: dict[str, ScheduledJob] = {}
        for job in SCHEDULED_JOBS:
            if job.name in self._invokers and job.catch_up_on_boot:
                catch_up_jobs[job.name] = job
        if not catch_up_jobs:
            return

        now = datetime.now(UTC)

        try:
            with psycopg.connect(self._database_url) as conn:
                latest = fetch_latest_successful_runs(
                    conn,
                    list(catch_up_jobs.keys()),
                )
        except Exception:
            logger.exception("catch-up: failed to query job_runs; skipping catch-up")
            return

        overdue: list[str] = []
        for name, job in catch_up_jobs.items():
            last_success = latest.get(name)
            if last_success is None:
                # Never run successfully — overdue.
                overdue.append(name)
                continue
            # Ensure timezone-aware for compute_next_run.
            if last_success.tzinfo is None:
                last_success = last_success.replace(tzinfo=UTC)
            next_fire = compute_next_run(job.cadence, last_success)
            if next_fire <= now:
                overdue.append(name)

        if not overdue:
            logger.info("catch-up: all jobs are current; nothing to fire")
            return

        # Check prerequisites for overdue jobs and split into
        # fire-list vs skip-list.  ``processed`` tracks jobs that have
        # already been categorised so the fallback path does not
        # re-fire already-skipped jobs.
        #
        # ``processed.add(name)`` is called BEFORE ``record_job_skip``
        # so that if the skip recording raises after committing the
        # row, the job is still marked as processed and won't be
        # double-fired in the fallback path.
        firing: list[str] = []
        skipped: list[tuple[str, str]] = []  # (name, reason)
        processed: set[str] = set()
        try:
            with psycopg.connect(self._database_url, autocommit=True) as conn:
                for name in overdue:
                    job = catch_up_jobs[name]
                    if job.prerequisite is not None:
                        met, reason = job.prerequisite(conn)
                        if not met:
                            processed.add(name)
                            record_job_skip(conn, name, reason)
                            skipped.append((name, reason))
                            continue
                    firing.append(name)
                    processed.add(name)
        except Exception:
            logger.exception("catch-up: prerequisite check failed; firing unprocessed overdue jobs")
            # Only fire jobs that were not already processed (skipped
            # jobs already have committed job_runs rows).
            firing = [n for n in overdue if n not in processed]

        for name, reason in skipped:
            logger.info("catch-up: skipping %s — prerequisite not met: %s", name, reason)

        total = len(firing) + len(skipped)
        if firing:
            logger.info(
                "catch-up: firing %d of %d overdue job(s) (%d skipped): %s",
                len(firing),
                total,
                len(skipped),
                sorted(firing),
            )
        elif skipped:
            logger.info(
                "catch-up: all %d overdue job(s) skipped (no upstream data)",
                total,
            )

        for name in firing:
            invoker = self._invokers[name]
            wrapped = self._wrap_invoker(name, invoker)
            fut = self._manual_executor.submit(wrapped)
            fut.add_done_callback(self._log_future_exception)

    def shutdown(self, *, timeout_s: float = 5.0) -> None:
        """Stop the scheduler quickly; rely on the boot reaper for recovery.

        Called from the FastAPI lifespan teardown *before* the
        connection pool is closed.

        Recovery model (#657):

        eBull treats shutdown as best-effort and crash recovery as
        authoritative — the same pattern Postgres uses for its own
        WAL replay, Kubernetes for ReplicaSet failover, Sidekiq for
        ReliableFetch, etc. Two consequences:

        1. We do NOT block waiting for in-flight jobs. APScheduler is
           stopped with ``wait=False`` so it stops accepting new jobs
           and returns immediately; in-flight scheduled jobs get
           hard-killed by process exit and Postgres rolls back their
           open transactions when the connection drops. The manual
           ThreadPoolExecutor likewise stops with
           ``shutdown(wait=False, cancel_futures=True)`` so queued
           manual triggers do not delay teardown.

        2. The orchestrator boot reaper
           (``app.services.sync_orchestrator.reaper.reap_orphaned_syncs``,
           called with ``reap_all=True`` from the lifespan startup) is
           the authoritative cleanup. It transitions any
           ``status='running'`` ``sync_runs`` row + its leftover
           ``pending``/``running`` layer rows to terminal status with
           ``error_category='orchestrator_crash'`` (or ``'cancelled'``
           per #645 for never-started rows). This recovery runs on
           every boot, regardless of how the prior process exited
           (clean reload, SIGKILL, OOM, host reboot).

        Pre-fix history (#131 → #657): the original implementation
        used ``wait=True`` to drain jobs. A hung job blocked teardown
        for the full timeout, and a uvicorn ``--reload`` cycle that
        runs into an in-flight long-running job (SEC ingest,
        fundamentals refresh) would routinely abandon the daemon
        thread after 30s — slow enough that uvicorn's reload
        supervisor often gave up and left the dev server dead with
        no follow-up "Started server process" log. The fix lets
        shutdown return in milliseconds and shifts trust onto the
        boot reaper that already exists.

        Idempotency requirement: any long-running job MUST be safe
        under mid-flight kill. This already holds for orchestrator-
        managed layers (UPSERT semantics + the reaper). Future jobs
        added via APScheduler must not assume they'll always finish.

        ``timeout_s`` (default 5.0) is the belt-and-suspenders cap.
        With ``wait=False`` the underlying call should return in
        milliseconds; the timeout exists in case APScheduler /
        ThreadPoolExecutor itself hangs in cleanup.
        """
        if not self._started:
            return
        import threading

        def _graceful_scheduler() -> None:
            try:
                # wait=False per #657 — do not block on in-flight jobs.
                # The boot reaper handles their orphaned state on the
                # next process startup.
                self._scheduler.shutdown(wait=False)
            except Exception:
                logger.exception("JobRuntime scheduler shutdown raised")

        def _graceful_executor() -> None:
            try:
                # cancel_futures=True drops queued manual triggers
                # that haven't started yet; in-flight ones get
                # hard-killed by process exit.
                self._manual_executor.shutdown(wait=False, cancel_futures=True)
            except Exception:
                logger.exception("JobRuntime manual executor shutdown raised")

        # Daemon-thread isolation kept from the prior impl so the
        # join timeout cannot trap the lifespan teardown if either
        # underlying ``shutdown`` itself wedges (rare; should not
        # happen with wait=False but cheap insurance).
        sched_thread = threading.Thread(target=_graceful_scheduler, daemon=True)
        sched_thread.start()
        sched_thread.join(timeout=timeout_s)
        if sched_thread.is_alive():
            logger.warning(
                "JobRuntime scheduler shutdown(wait=False) wedged for %.0fs — "
                "abandoning daemon and proceeding; boot reaper will reconcile "
                "any orphaned sync_runs on next startup",
                timeout_s,
            )

        exec_thread = threading.Thread(target=_graceful_executor, daemon=True)
        exec_thread.start()
        exec_thread.join(timeout=timeout_s)
        if exec_thread.is_alive():
            logger.warning(
                "JobRuntime manual executor shutdown(wait=False) wedged for %.0fs — "
                "abandoning daemon and proceeding; queued manual triggers are dropped",
                timeout_s,
            )

        self._started = False
        logger.info("JobRuntime stopped")

    # -- introspection -----------------------------------------------------

    def get_next_run_times(self) -> dict[str, datetime | None]:
        """Return the live next-fire time for each registered job.

        Queries APScheduler's in-memory job store. Returns ``None`` for
        a job name if the scheduler has no record of it (e.g. it was
        paused or removed). On-demand-only jobs are not included.
        """
        result: dict[str, datetime | None] = {}
        for job in SCHEDULED_JOBS:
            if job.name not in self._invokers:
                continue
            aps_job = self._scheduler.get_job(f"recurring:{job.name}")
            result[job.name] = aps_job.next_run_time if aps_job is not None else None
        return result

    # -- triggers ----------------------------------------------------------

    def trigger(self, job_name: str) -> None:
        """Submit a manual run of *job_name* to the executor.

        Returns as soon as the run is queued. Does NOT wait for the
        job to finish -- the API endpoint returns 202 Accepted with
        no body and the operator polls ``/system/status`` for
        results.

        Raises:
            UnknownJob: ``job_name`` is not in the invoker registry.
            JobAlreadyRunning: another in-process manual trigger of
                this job is already in flight on this app instance.

        The synchronous 409 path uses an in-process
        ``threading.Lock`` per job name, *not* the Postgres advisory
        lock. The advisory lock is held by the worker thread for the
        duration of the run -- the request thread never touches a
        ``psycopg.Connection``. See PR #131 round 1 review
        (BLOCKING 1): the previous design acquired the advisory lock
        on the request thread and handed the connection off to the
        worker, which assumed sequential cross-thread access to a
        ``psycopg.Connection`` was safe. The assumption is technically
        defensible (executor.submit provides a happens-before barrier
        and the connection is never accessed concurrently) but
        load-bearing and untested -- one future refactor away from a
        real bug. The in-process-lock approach eliminates the
        cross-thread access entirely.

        Edge case acknowledged: if a *scheduled* fire is currently
        running this job, the in-process lock is free (scheduled
        fires never touch ``_inflight``), so ``trigger()`` returns
        202 and the worker thread will then find the advisory lock
        held by the scheduler, log a warning, and no-op. The API
        caller sees a 202 for a run that did nothing. The
        ``/system/jobs`` endpoint surfaces this honestly. This edge
        case is rare in practice (manual triggers during scheduled
        fires are unusual).
        """
        invoker = self._invokers.get(job_name)
        if invoker is None:
            raise UnknownJob(job_name)

        inflight = self._inflight[job_name]
        if not inflight.acquire(blocking=False):
            raise JobAlreadyRunning(job_name)
        try:
            fut = self._manual_executor.submit(self._run_manual, job_name, invoker, None)
            fut.add_done_callback(self._log_future_exception)
        except Exception:
            # Submission failed before the worker took ownership --
            # release the in-process lock so a retry can acquire.
            inflight.release()
            raise

    def submit_manual_with_request(
        self,
        job_name: str,
        *,
        request_id: int,
        mode: str | None = None,
    ) -> None:
        """Submit a manual run associated with a queue request_id (#719).

        Like :meth:`trigger` but routes through a wrapper that ALSO
        manages the ``pending_job_requests`` row lifecycle: opens the
        ``job_runs`` row with ``linked_request_id=request_id``,
        transitions the request to ``dispatched`` AFTER the run row
        exists, and to ``completed`` after the invoker returns.

        Used by the listener (#719). API-side callers should keep using
        :meth:`trigger` — the listener is the only path that holds a
        request_id.

        ``mode`` (#1071) is the ``pending_job_requests.mode`` value the
        listener decoded from the claimed row (``'iterate'`` or
        ``'full_wash'``; ``None`` for legacy rows). When ``mode`` is
        ``'full_wash'`` the invoker IS the fence holder, so the prelude
        bypasses the fence check (otherwise the worker would self-skip
        because its own queue row matches the fence query).

        Unlike :meth:`trigger`, this method does NOT raise
        ``JobAlreadyRunning`` when the in-process inflight lock is
        held: a contested manual run claimed from the queue marks the
        request rejected rather than failing loud. That keeps the
        cross-process semantics symmetrical with the existing
        scheduler-vs-manual race documented in the trigger() docstring.
        """
        invoker = self._invokers.get(job_name)
        if invoker is None:
            from app.services.sync_orchestrator.dispatcher import mark_request_rejected

            with psycopg.connect(self._database_url, autocommit=True) as conn:
                mark_request_rejected(
                    conn,
                    request_id,
                    error_msg=f"unknown job name: {job_name!r}",
                )
            return

        inflight = self._inflight[job_name]
        if not inflight.acquire(blocking=False):
            from app.services.sync_orchestrator.dispatcher import mark_request_rejected

            with psycopg.connect(self._database_url, autocommit=True) as conn:
                mark_request_rejected(
                    conn,
                    request_id,
                    error_msg="another manual trigger already in flight",
                )
            return

        try:
            fut = self._manual_executor.submit(self._run_manual, job_name, invoker, request_id, mode)
            fut.add_done_callback(self._log_future_exception)
        except Exception:
            inflight.release()
            raise

    # -- internals ---------------------------------------------------------

    @staticmethod
    def _log_future_exception(fut: object) -> None:
        """Done-callback for fire-and-forget executor submissions.

        ``_wrap_invoker`` and ``_run_manual`` already catch all
        exceptions internally, so this callback should never fire in
        normal operation. It exists as a defensive last-resort so that
        an unexpected executor-level failure (e.g. interpreter
        shutdown race) is logged rather than silently swallowed.
        """
        # concurrent.futures.Future, but typed as object to avoid
        # importing the type for a one-liner callback.
        try:
            exc = getattr(fut, "exception", lambda: None)()
        except Exception:
            # CancelledError (or any other unexpected state) — log and
            # move on; done-callbacks must not propagate.
            logger.warning("executor future was cancelled or in unexpected state", exc_info=True)
            return
        if exc is not None:
            logger.error("executor future raised unexpectedly: %s", exc, exc_info=exc)

    def _wrap_invoker(self, job_name: str, invoker: Callable[[], None]) -> Callable[[], None]:
        """Wrap a scheduled invoker with prerequisite check + advisory lock.

        The scheduled fire path checks the prerequisite (if any) before
        acquiring the lock.  If the prerequisite is not met, a
        ``job_runs`` row with status='skipped' is recorded and the
        invoker is not called.

        Lock contention is a normal condition -- a scheduled fire that
        overlaps a still-running manual trigger -- and is logged at INFO
        and skipped, not raised, because APScheduler would otherwise log
        a noisy traceback for an expected race.
        """
        database_url = self._database_url
        job = self._job_registry.get(job_name)

        def wrapped() -> None:
            # Prerequisite gate (scheduled fires only — manual triggers
            # bypass prerequisites so the operator can force a run).
            if job is not None and job.prerequisite is not None:
                try:
                    with psycopg.connect(database_url, autocommit=True) as conn:
                        met, reason = job.prerequisite(conn)
                        if not met:
                            record_job_skip(conn, job_name, reason)
                            logger.info(
                                "scheduled fire of %r skipped — prerequisite not met: %s",
                                job_name,
                                reason,
                            )
                            return
                except Exception:
                    # If the prerequisite check itself fails, let the
                    # job run — failing open is safer than silently
                    # skipping real work.
                    logger.warning(
                        "prerequisite check for %r failed; running job anyway",
                        job_name,
                        exc_info=True,
                    )

            try:
                with JobLock(database_url, job_name):
                    # #1071 — admin control hub PR3: route through the
                    # lock+fence prelude. Scheduled fires never bypass the
                    # fence (they are not the full-wash holder), so the
                    # default ``bypass_fence_check=False`` applies.
                    # Self-tracked invokers opt out of the prelude.
                    if job_name in _PRELUDE_OPT_OUT_JOBS:
                        invoker()
                    else:
                        run_with_prelude(database_url, job_name, invoker)
            except JobAlreadyRunning:
                logger.info(
                    "scheduled fire of %r skipped: another instance is "
                    "already running (lock held by manual trigger or "
                    "earlier overrunning fire)",
                    job_name,
                )
            except Exception:
                logger.exception(
                    "scheduled fire of %r raised; will run again at next cadence",
                    job_name,
                )

        return wrapped

    def _run_manual(
        self,
        job_name: str,
        invoker: Callable[[], None],
        request_id: int | None,
        mode: str | None = None,
    ) -> None:
        """Worker-thread entry point for manual triggers.

        Single-threaded with respect to the ``JobLock`` connection:
        we acquire, hold, and release the advisory lock entirely on
        this thread. The in-process ``_inflight`` lock that was
        acquired on the request thread is released here in
        ``finally`` so a retry can run.

        ``request_id`` is populated when the manual trigger came from
        the queue (#719): the wrapper transitions the queue row to
        ``dispatched`` after acquiring the advisory lock and
        ``completed`` / ``rejected`` on the way out. The
        ``linked_request_id`` foreign key on ``job_runs`` is populated
        by the invoker's own ``record_job_start`` call when the
        invoker reads it from a thread-local set just before
        invocation; the queue row's `dispatched` transition is the
        canonical signal that work is in flight.

        When ``request_id`` is None the wrapper preserves the original
        in-process semantics — no queue transitions, just JobLock +
        invoke.
        """
        from app.services.sync_orchestrator.dispatcher import (
            mark_request_completed,
            mark_request_rejected,
        )

        # PR #719 review BLOCKING: do NOT mark the queue row as
        # ``dispatched`` before invoker() runs. The wrapper has no
        # visibility into when the invoker writes its `job_runs` row
        # via ``record_job_start``, so any pre-invoker UPDATE creates
        # a window where ``pending_job_requests.status='dispatched'``
        # but no `job_runs` row exists with `linked_request_id` —
        # operator confusion AND a violation of the boot-recovery
        # contract. The row goes ``claimed`` → ``completed`` /
        # ``rejected`` directly. The recovery query in
        # ``reset_stale_in_flight`` already accepts both 'claimed' and
        # 'dispatched' in its WHERE, so dropping the intermediate
        # transition is forwards-compatible.
        # #1071 — admin control hub PR3: route through the lock+fence
        # prelude. ``mode='full_wash'`` is the listener-decoded marker that
        # this worker IS the fence holder, so bypass the fence check
        # (otherwise the worker would self-skip on its own queue row).
        bypass_fence_check = mode == "full_wash"
        try:
            try:
                with JobLock(self._database_url, job_name):
                    if job_name in _PRELUDE_OPT_OUT_JOBS:
                        # Self-tracked invoker; running the prelude
                        # would orphan a ``job_runs`` row the invoker
                        # never finalises. Bootstrap's fence is
                        # ``bootstrap_state.status``, not a queue row.
                        invoker()
                        invoked = True
                    else:
                        invoked = run_with_prelude(
                            self._database_url,
                            job_name,
                            invoker,
                            bypass_fence_check=bypass_fence_check,
                            linked_request_id=request_id,
                        )
                    if request_id is not None and not invoked:
                        # Prelude wrote a 'skipped' job_runs row +
                        # returned without invoking. Mark the queue row
                        # rejected so the operator's `/jobs/requests`
                        # view reflects reality (PR #1072 review BLOCKING
                        # — `mark_request_completed` after skip masks
                        # the audit trail).
                        with psycopg.connect(self._database_url, autocommit=True) as conn:
                            mark_request_rejected(
                                conn,
                                request_id,
                                error_msg=_FENCE_HELD_ERROR_MSG,
                            )
                    elif request_id is not None:
                        with psycopg.connect(self._database_url, autocommit=True) as conn:
                            mark_request_completed(conn, request_id)
            except JobAlreadyRunning:
                # Logged at INFO -- this is an expected race (manual
                # trigger landed during a scheduled fire or peer
                # process run), not an operational fault.
                logger.info(
                    "manual trigger of %r no-opped: advisory lock held by "
                    "another runner (scheduled fire or peer process); the "
                    "202 response was returned but the job did not run",
                    job_name,
                )
                if request_id is not None:
                    try:
                        with psycopg.connect(self._database_url, autocommit=True) as conn:
                            mark_request_rejected(
                                conn,
                                request_id,
                                error_msg="advisory lock held by another runner",
                            )
                    except Exception:
                        logger.exception("failed to mark manual request_id=%d rejected", request_id)
            except Exception as exc:
                logger.exception("manual trigger of %r raised", job_name)
                if request_id is not None:
                    try:
                        with psycopg.connect(self._database_url, autocommit=True) as conn:
                            mark_request_rejected(
                                conn,
                                request_id,
                                error_msg=f"{type(exc).__name__}: {exc}",
                            )
                    except Exception:
                        logger.exception("failed to mark manual request_id=%d rejected", request_id)
        finally:
            self._inflight[job_name].release()


# ---------------------------------------------------------------------------
# Lifespan helpers
# ---------------------------------------------------------------------------


def start_runtime() -> JobRuntime:
    """Build and start a production :class:`JobRuntime`.

    Called from the FastAPI lifespan after the connection pool is
    open. Returns the started runtime so the caller can store it on
    ``app.state`` and shut it down later.
    """
    runtime = JobRuntime()
    runtime.start()
    return runtime


def shutdown_runtime(runtime: JobRuntime | None) -> None:
    """Shut down a :class:`JobRuntime`, tolerating ``None``.

    The ``None`` tolerance is so the lifespan teardown can call this
    unconditionally even if startup failed before the runtime was
    built.
    """
    if runtime is None:
        return
    runtime.shutdown()
