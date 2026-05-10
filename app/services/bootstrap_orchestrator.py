"""First-install bootstrap orchestrator.

Runs the 17-stage end-to-end first-install backfill described in
``docs/superpowers/specs/2026-05-07-first-install-bootstrap.md``.

Three phases:

1. **Phase A — init** (sequential, single thread): runs the universe
   sync (A1). Every Phase B stage depends on a populated
   ``instruments`` table.
2. **Phase B — lanes** (two threads in parallel): the eToro lane
   (E1: candle refresh) runs alongside the SEC lane (S1..S15:
   filer directories, CIK refresh, filing-events seed, manifest
   drain, typed parsers, ownership rollup, fundamentals).
3. **Phase C — finalize** (sequential): inspects per-stage outcomes
   and transitions ``bootstrap_state`` to ``complete`` or
   ``partial_error``.

Per-stage execution contract (every stage):

1. Pre-check stage status; skip if ``success``.
2. Mark stage ``running``.
3. Acquire ``JobLock(database_url, job_name)`` — same primitive
   that scheduled + manual paths use.
4. Invoke ``_INVOKERS[job_name]()``.
5. Catch exceptions; record ``error`` with truncated message.
6. On success record ``success`` + ``rows_processed``.

Bootstrap dispatches stage jobs by direct invocation, bypassing the
scheduler's prerequisite gate (intentional: bootstrap is the operator
forcing first-install work). The advisory lock is acquired so a
parallel manual / scheduled trigger cannot run twice simultaneously.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Final

import psycopg

from app.config import settings
from app.jobs.locks import JobAlreadyRunning, JobLock
from app.services.bootstrap_preconditions import BootstrapPhaseSkipped
from app.services.bootstrap_state import (
    StageSpec,
    finalize_run,
    mark_run_cancelled,
    mark_stage_blocked,
    mark_stage_error,
    mark_stage_running,
    mark_stage_skipped,
    mark_stage_success,
    read_latest_run_with_stages,
)
from app.services.process_stop import is_stop_requested
from app.services.process_stop import mark_completed as mark_stop_completed
from app.services.process_stop import mark_observed as mark_stop_observed
from app.services.processes.param_metadata import (
    ParamValidationError,
    validate_job_params,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stage catalogue — single source of truth for which stages run.
# ---------------------------------------------------------------------------

# Job names registered in app/jobs/runtime.py:_INVOKERS that PR2 adds:
JOB_BOOTSTRAP_ORCHESTRATOR = "bootstrap_orchestrator"

# PR1c #1064 — three bespoke wrappers (``bootstrap_filings_history_seed``,
# ``sec_first_install_drain_job``, ``bootstrap_sec_13f_recent_sweep_job``)
# were lifted into params-aware ``JobInvoker`` bodies in
# ``app/workers/scheduler.py``. The hardcoded values that lived inside
# the wrapper bodies (``days_back=730``, ``min_period_of_report=today-380d``,
# ``source_label="sec_edgar_13f_directory_bootstrap"``, etc.) now live
# in ``StageSpec.params`` for stages 14, 15, 21 below.
#
# Constants imported from the scheduler so the bootstrap-stage entries
# below carry the canonical names and a future rename is single-site.
from app.workers.scheduler import (  # noqa: E402  (after dataclass to avoid cycle)
    JOB_FILINGS_HISTORY_SEED,
    JOB_SEC_13F_QUARTERLY_SWEEP,
    JOB_SEC_FIRST_INSTALL_DRAIN,
)

# These already exist as scheduled jobs but were not registered in
# _INVOKERS until PR2; we re-use the existing job-name constants so
# operator records / job_runs trail stays consistent.
JOB_DAILY_CIK_REFRESH = "daily_cik_refresh"
JOB_DAILY_FINANCIAL_FACTS = "daily_financial_facts"


# PR1c #1064 — bootstrap-bounded 13F sweep recency cut-off. Used to
# live as a constant inside the deleted ``bootstrap_sec_13f_recent_sweep_job``
# wrapper. 4 quarters (~380 days) = current + 3 prior periods, matches
# the rolling ownership-card window. Older 13Fs add no value to
# current-quarter ranking and pre-2013 ones don't have machine-readable
# holdings (#1008).
_BOOTSTRAP_13F_QUARTERS_BACK = 4
_BOOTSTRAP_13F_RECENCY_DAYS = _BOOTSTRAP_13F_QUARTERS_BACK * 95

# PR1c #1064 — filings_history_seed bootstrap default form-type
# allow-list. Imported once at module load so the StageSpec.params
# dict is plain data; the underlying constant lives in the canonical
# owner (``app.services.filings``) so the three-tier allow-list stays
# single-sourced. Tuple (not list) for hashable, frozen-stage-spec
# compat.
from app.services.filings import SEC_INGEST_KEEP_FORMS  # noqa: E402

_FILINGS_HISTORY_KEEP_FORMS_TUPLE: tuple[str, ...] = tuple(sorted(SEC_INGEST_KEEP_FORMS))


# Sentinel for params values that depend on dispatch-time state
# (e.g. ``date.today()``). Module-load evaluation would freeze the
# value into ``_BOOTSTRAP_STAGE_SPECS`` for the lifetime of the jobs
# process; a long-lived process would dispatch stage 21 with a stale
# cutoff. ``_resolve_dynamic_params`` materialises the absolute value
# at dispatch time. The sentinel string is namespaced so it never
# collides with a legitimate operator value.
_PARAM_DYNAMIC_BOOTSTRAP_13F_CUTOFF = "<dynamic:bootstrap_13f_cutoff>"


def _resolve_dynamic_params(params: Mapping[str, Any]) -> dict[str, Any]:
    """Materialise dispatch-time dynamic values in a stage params dict.

    Today the only dynamic value is the bootstrap-13F recency cutoff;
    the helper is structured for forward extensibility (additional
    sentinels can be added without touching call sites).

    The dispatcher calls this immediately before invoking the
    underlying ``JobInvoker`` so the absolute value is what flows
    into ``job_runs.params_snapshot`` and the invoker body.
    """
    resolved: dict[str, Any] = dict(params)
    if resolved.get("min_period_of_report") == _PARAM_DYNAMIC_BOOTSTRAP_13F_CUTOFF:
        resolved["min_period_of_report"] = date.today() - timedelta(days=_BOOTSTRAP_13F_RECENCY_DAYS)
    return resolved


def _spec(
    stage_key: str,
    stage_order: int,
    lane: str,
    job_name: str,
    *,
    params: Mapping[str, Any] | None = None,
) -> StageSpec:
    return StageSpec(
        stage_key=stage_key,
        stage_order=stage_order,
        lane=lane,  # type: ignore[arg-type]
        job_name=job_name,
        params=params if params is not None else {},
    )


# ---------------------------------------------------------------------------
# Lane concurrency model (#1020)
# ---------------------------------------------------------------------------
#
# Each lane has a max-concurrency cap. Stages in the same lane share
# a budget (rate-bound: SEC clock; or DB-bound: psycopg conn pool).
# Stages in different lanes run in parallel.

_LANE_MAX_CONCURRENCY: Final[dict[str, int]] = {
    "init": 1,
    "etoro": 1,
    "sec": 1,  # legacy catch-all; preserved for migration compat
    "sec_rate": 1,  # SEC per-IP rate clock
    "sec_bulk_download": 1,
    # PR1c #1064: every lane serialises now that JobLock is source-keyed
    # (one ``JobLock(source)`` covers all jobs in the same lane). The
    # operator-locked source-lock decision is unambiguous: same-source =
    # serialised. Setting ``db`` to 1 retires the parallel-DB-stage claim
    # from #1020 — a misleading dispatcher shape (5 db stages submitted,
    # 4 immediately blocked on the source lock). The map structure is
    # kept (rather than deleted) for one cycle so the
    # ``_phase_batched_dispatch`` shape stays stable; a follow-up PR
    # removes the map entirely.
    #
    # Tech-debt: first-install bootstrap wall-clock regresses from "5 db
    # stages parallel" → "1 db stage at a time". Measure on dev and file
    # follow-up if operator-visible — tracked in PR description.
    "db": 1,
}


# Stage-key dependency graph: which `requires` keys must be `success`
# before this stage can run. If any required stage is `error` or
# `blocked`, this stage transitions to `blocked` (orchestrator
# never invokes the underlying job).
_STAGE_REQUIRES: Final[dict[str, tuple[str, ...]]] = {
    # Phase A
    "universe_sync": (),
    "candle_refresh": ("universe_sync",),
    "cusip_universe_backfill": ("universe_sync",),
    "sec_13f_filer_directory_sync": ("universe_sync",),
    "sec_nport_filer_directory_sync": ("universe_sync",),
    "cik_refresh": ("universe_sync",),
    "sec_bulk_download": ("universe_sync",),
    # Phase C — DB-bound bulk ingesters (parallel-able in db lane)
    "sec_submissions_ingest": ("sec_bulk_download", "cik_refresh"),
    "sec_companyfacts_ingest": ("sec_bulk_download", "cik_refresh"),
    "sec_13f_ingest_from_dataset": ("sec_bulk_download", "cusip_universe_backfill"),
    "sec_insider_ingest_from_dataset": ("sec_bulk_download", "cik_refresh"),
    "sec_nport_ingest_from_dataset": ("sec_bulk_download", "cusip_universe_backfill"),
    # Phase C' — secondary-pages walker (rate-bound)
    "sec_submissions_files_walk": ("sec_submissions_ingest",),
    # Legacy chain — fallback when bulk path failed.
    # All require cik_refresh because their per-CIK fetches need
    # the CIK mapping populated first (Codex sweep BLOCKING).
    "filings_history_seed": ("cik_refresh",),
    "sec_first_install_drain": ("cik_refresh",),
    "sec_def14a_bootstrap": ("sec_submissions_ingest", "sec_submissions_files_walk"),
    "sec_business_summary_bootstrap": ("sec_submissions_ingest", "sec_submissions_files_walk"),
    "sec_insider_transactions_backfill": ("cik_refresh",),
    "sec_form3_ingest": ("cik_refresh",),
    "sec_8k_events_ingest": ("sec_submissions_ingest", "sec_submissions_files_walk"),
    "sec_13f_recent_sweep": ("cik_refresh",),
    "sec_n_port_ingest": ("cik_refresh",),
    "ownership_observations_backfill": (
        # Bulk path — direct writes to ownership_*_observations.
        "sec_13f_ingest_from_dataset",
        "sec_insider_ingest_from_dataset",
        "sec_nport_ingest_from_dataset",
        # Legacy chain — populates the legacy typed tables
        # (insider_transactions, institutional_holdings, n_port_*) that
        # the backfill mirrors into observations. In fallback mode the
        # bulk stages skip, so the legacy chain becomes the sole source;
        # without these requires the backfill could fire BEFORE the
        # legacy chain populates rows. Codex pre-push BLOCKING for #1041.
        "sec_insider_transactions_backfill",
        "sec_form3_ingest",
        "sec_13f_recent_sweep",
        "sec_n_port_ingest",
    ),
    "fundamentals_sync": ("sec_companyfacts_ingest",),
}


# Lane override map — stage_key → lane name, used by the new
# concurrency dispatcher. Stages NOT in this map default to their
# StageSpec.lane field (so existing 17-stage runs still work).
_STAGE_LANE_OVERRIDES: Final[dict[str, str]] = {
    "cusip_universe_backfill": "sec_rate",
    "sec_13f_filer_directory_sync": "sec_rate",
    "sec_nport_filer_directory_sync": "sec_rate",
    "cik_refresh": "sec_rate",
    "sec_bulk_download": "sec_bulk_download",
    "sec_submissions_ingest": "db",
    "sec_companyfacts_ingest": "db",
    "sec_13f_ingest_from_dataset": "db",
    "sec_insider_ingest_from_dataset": "db",
    "sec_nport_ingest_from_dataset": "db",
    "sec_submissions_files_walk": "sec_rate",
    "sec_def14a_bootstrap": "sec_rate",
    "sec_business_summary_bootstrap": "sec_rate",
    "sec_8k_events_ingest": "sec_rate",
    "sec_13f_recent_sweep": "sec_rate",
}


def _effective_lane(stage_key: str, default_lane: str) -> str:
    return _STAGE_LANE_OVERRIDES.get(stage_key, default_lane)


# Bulk-archive job names for the #1020 first-install bulk-datasets-first
# pipeline. Re-exported from the canonical owners so duplicate-constant
# drift is impossible (Codex review WARNING for PR #1035).
from app.services.sec_bulk_download import JOB_SEC_BULK_DOWNLOAD  # noqa: E402
from app.services.sec_bulk_orchestrator_jobs import (  # noqa: E402
    JOB_SEC_13F_INGEST_FROM_DATASET,
    JOB_SEC_COMPANYFACTS_INGEST,
    JOB_SEC_INSIDER_INGEST_FROM_DATASET,
    JOB_SEC_NPORT_INGEST_FROM_DATASET,
    JOB_SEC_SUBMISSIONS_INGEST,
)
from app.services.sec_submissions_files_walk import (  # noqa: E402
    JOB_SEC_SUBMISSIONS_FILES_WALK,
)

_BOOTSTRAP_STAGE_SPECS: tuple[StageSpec, ...] = (
    # Phase A (init, sequential)
    _spec("universe_sync", 1, "init", "nightly_universe_sync"),
    # eToro lane (separate rate budget; runs concurrent with SEC).
    _spec("candle_refresh", 2, "etoro", "daily_candle_refresh"),
    # SEC reference lane — share per-IP rate clock.
    _spec("cusip_universe_backfill", 3, "sec_rate", "cusip_universe_backfill"),
    _spec("sec_13f_filer_directory_sync", 4, "sec_rate", "sec_13f_filer_directory_sync"),
    _spec("sec_nport_filer_directory_sync", 5, "sec_rate", "sec_nport_filer_directory_sync"),
    _spec("cik_refresh", 6, "sec_rate", JOB_DAILY_CIK_REFRESH),
    # Phase A3 — bulk archive download (#1020). Ships the heavy data
    # in <10 min on a fast connection; the C-stages below ingest
    # locally with no rate-budget cost.
    _spec("sec_bulk_download", 7, "sec_bulk_download", JOB_SEC_BULK_DOWNLOAD),
    # Phase C — DB-bound bulk ingesters (#1020). Parallel within db
    # lane (max_concurrency=5).
    _spec("sec_submissions_ingest", 8, "db", JOB_SEC_SUBMISSIONS_INGEST),
    _spec("sec_companyfacts_ingest", 9, "db", JOB_SEC_COMPANYFACTS_INGEST),
    _spec("sec_13f_ingest_from_dataset", 10, "db", JOB_SEC_13F_INGEST_FROM_DATASET),
    _spec("sec_insider_ingest_from_dataset", 11, "db", JOB_SEC_INSIDER_INGEST_FROM_DATASET),
    _spec("sec_nport_ingest_from_dataset", 12, "db", JOB_SEC_NPORT_INGEST_FROM_DATASET),
    # Phase C' — per-CIK secondary-pages walk for deep-history parity.
    _spec("sec_submissions_files_walk", 13, "sec_rate", JOB_SEC_SUBMISSIONS_FILES_WALK),
    # Legacy per-filing stages — kept as a fallback path. After the
    # bulk pass these are largely idempotent DB no-ops on populated
    # observation tables; on the slow-connection bypass path they are
    # the primary write path.
    # PR1c #1064 — three bespoke wrappers in this module collapsed into
    # the SCHEDULED_JOBS-side ``filings_history_seed`` /
    # ``sec_first_install_drain`` / ``sec_13f_quarterly_sweep`` bodies.
    # Bootstrap-only knobs ride here as ``StageSpec.params``; the bodies
    # consume the same dict shape that the manual API publishes.
    _spec(
        "filings_history_seed",
        14,
        "sec_rate",
        JOB_FILINGS_HISTORY_SEED,
        params={
            "days_back": 730,
            "filing_types": tuple(_FILINGS_HISTORY_KEEP_FORMS_TUPLE),
        },
    ),
    _spec(
        "sec_first_install_drain",
        15,
        "sec_rate",
        JOB_SEC_FIRST_INSTALL_DRAIN,
        params={"max_subjects": None},
    ),
    _spec("sec_def14a_bootstrap", 16, "sec_rate", "sec_def14a_bootstrap"),
    _spec("sec_business_summary_bootstrap", 17, "sec_rate", "sec_business_summary_bootstrap"),
    _spec("sec_insider_transactions_backfill", 18, "sec_rate", "sec_insider_transactions_backfill"),
    _spec("sec_form3_ingest", 19, "sec_rate", "sec_form3_ingest"),
    _spec("sec_8k_events_ingest", 20, "sec_rate", "sec_8k_events_ingest"),
    # #1008 — first-install bootstrap uses a recency-bounded sweep
    # (last 4 quarters, ~12 months) instead of the full historical
    # sweep. Walking decades of pre-2013 filings yields zero rows
    # (no machine-readable primary_doc/infotable) and turns the
    # bootstrap into an 11+ hour wait. Standalone weekly cron keeps
    # the full historical sweep — same job, no min_period_of_report
    # bound. On the bulk path (#1020) C3 has already populated
    # ownership_institutions_observations; this stage tops up.
    #
    # PR1c #1064: bootstrap-only ``source_label`` overrides the default
    # so audit history distinguishes this bounded sweep from the
    # standalone weekly run. The validator allows ``source_label`` here
    # via ``JOB_INTERNAL_KEYS`` (PR1a) — manual API rejects it.
    _spec(
        "sec_13f_recent_sweep",
        21,
        "sec_rate",
        JOB_SEC_13F_QUARTERLY_SWEEP,
        # ``min_period_of_report`` resolves to ``today() - 380d`` at
        # dispatch time (see ``_resolve_dynamic_params``). Hardcoding
        # ``date.today()`` here would freeze the cutoff at module-load,
        # so a long-lived jobs process would dispatch stage 21 with a
        # stale floor. The sentinel keeps the StageSpec data-only.
        params={
            "min_period_of_report": _PARAM_DYNAMIC_BOOTSTRAP_13F_CUTOFF,
            "source_label": "sec_edgar_13f_directory_bootstrap",
        },
    ),
    _spec("sec_n_port_ingest", 22, "sec_rate", "sec_n_port_ingest"),
    _spec("ownership_observations_backfill", 23, "db", "ownership_observations_backfill"),
    _spec("fundamentals_sync", 24, "db", "fundamentals_sync"),
)


def get_bootstrap_stage_specs() -> tuple[StageSpec, ...]:
    """Public read-only accessor for the stage catalogue.

    The API endpoint that creates a new run imports this to seed
    ``bootstrap_stages`` rows. Lives in code (not the DB) because the
    catalogue is a deployable contract — adding / reordering stages
    is a code change with tests, not a runtime config change.
    """
    return _BOOTSTRAP_STAGE_SPECS


# ---------------------------------------------------------------------------
# Per-stage runner
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _StageOutcome:
    stage_key: str
    success: bool
    error: str | None
    skipped: bool = False
    # PR3d #1064 — operator-cancel observed mid-stage. The dispatcher
    # maps this onto stage status='cancelled' (not 'error') so the
    # Timeline tones gray instead of red. The next dispatcher
    # iteration's run-level cancel checkpoint then sweeps remaining
    # stages and terminalises the run.
    cancelled: bool = False


def _run_one_stage(
    *,
    run_id: int,
    stage_key: str,
    job_name: str,
    invoker: Callable[[Mapping[str, Any]], None],
    database_url: str,
    params: Mapping[str, Any] | None = None,
) -> _StageOutcome:
    """Execute one stage end-to-end with `JobLock` + bookkeeping.

    Exceptions inside the invoker are caught and recorded as
    ``error`` so the lane can proceed to the next stage. The only
    exceptions that escape this function are programmer errors
    (e.g. the bookkeeping query fails) — those propagate so the
    orchestrator surfaces them, but the lane runner catches a broad
    ``Exception`` to keep going.

    PR1c #1064: ``params`` carries the dispatcher-supplied stage
    params dict (from ``StageSpec.params`` after dynamic-value
    resolution + validation). The invoker consumes them through the
    widened ``JobInvoker`` contract; bootstrap-only knobs like
    ``min_period_of_report`` and ``source_label`` flow through here
    instead of living inside bespoke wrapper bodies. Default
    ``None`` = empty dict for backwards compat with any direct
    test caller that hasn't migrated.
    """
    effective_params: Mapping[str, Any] = params if params is not None else {}
    with psycopg.connect(database_url) as conn:
        mark_stage_running(conn, run_id=run_id, stage_key=stage_key)
        conn.commit()

    # PR1c #1064 (Codex pre-push WARNING): the promoted scheduler-side
    # invokers call ``_tracked_job`` which reads ``_params_snapshot_var``
    # via ``consume_params_snapshot()`` to populate
    # ``job_runs.params_snapshot``. Bootstrap dispatch bypasses
    # ``run_with_prelude``'s contextvar set, so we plumb the snapshot
    # here. Without this, stage 21's audit row would persist ``{}``
    # even though the body executed with a real ``min_period_of_report``
    # cutoff + ``source_label`` override.
    from app.jobs.runtime import _params_snapshot_var

    # PR3d #1064 — expose the cancel signal to long-running invokers.
    # Stages with multi-minute loops poll
    # ``bootstrap_cancel_requested()`` and raise
    # ``BootstrapStageCancelled`` to bail out cooperatively. The
    # context manager scopes the contextvar so scheduled / manual
    # triggers of the same job (outside bootstrap) see no signal.
    from app.services.bootstrap_state import BootstrapStageCancelled, mark_stage_cancelled
    from app.services.processes.bootstrap_cancel_signal import active_bootstrap_run

    try:
        with JobLock(database_url, job_name), active_bootstrap_run(run_id, stage_key):
            snap_token = _params_snapshot_var.set(effective_params)
            try:
                invoker(effective_params)
            finally:
                _params_snapshot_var.reset(snap_token)
    except JobAlreadyRunning:
        message = (
            f"another instance of {job_name!r} holds the advisory lock; "
            "retry from the bootstrap panel after the other run completes"
        )
        with psycopg.connect(database_url) as conn:
            mark_stage_error(conn, run_id=run_id, stage_key=stage_key, error_message=message)
            conn.commit()
        return _StageOutcome(stage_key=stage_key, success=False, error=message)
    except BootstrapStageCancelled as exc:
        # PR3d #1064 — operator clicked Cancel mid-stage; the invoker
        # observed the signal at one of its checkpoints and bailed
        # out. Mark the stage ``cancelled`` (not ``error``) so the
        # Timeline tones gray. The next dispatcher iteration's
        # run-level cancel checkpoint terminalises remaining stages.
        message = str(exc) or "stage cancelled by operator"
        logger.info(
            "bootstrap stage %s observed cancel signal; marking cancelled (%s)",
            stage_key,
            message,
        )
        with psycopg.connect(database_url) as conn:
            mark_stage_cancelled(conn, run_id=run_id, stage_key=stage_key, reason=message)
            conn.commit()
        return _StageOutcome(stage_key=stage_key, success=False, error=message, cancelled=True)
    except BootstrapPhaseSkipped as exc:
        # Operator-policy skip: A3 wrote a fallback manifest because
        # bandwidth was below threshold, and the legacy chain handles
        # ingest. Mark the stage `skipped` so finalize_run does NOT
        # count it as a failure (#1041).
        message = f"skipped: {exc}"
        logger.info("bootstrap stage %s skipped: %s", stage_key, exc)
        with psycopg.connect(database_url) as conn:
            mark_stage_skipped(conn, run_id=run_id, stage_key=stage_key, reason=message)
            conn.commit()
        return _StageOutcome(stage_key=stage_key, success=True, error=None, skipped=True)
    except Exception as exc:
        message = f"{type(exc).__name__}: {exc}"
        logger.exception("bootstrap stage %s raised; lane continues", stage_key)
        with psycopg.connect(database_url) as conn:
            mark_stage_error(conn, run_id=run_id, stage_key=stage_key, error_message=message)
            conn.commit()
        return _StageOutcome(stage_key=stage_key, success=False, error=message)

    # Auto-record the __job__ row in bootstrap_archive_results so
    # downstream stages can verify provenance via the precondition
    # checker. C-stages write their own per-archive rows; this catches
    # the B-stages and any other invoker that doesn't self-record.
    # Idempotent ON CONFLICT — no-op if the C-stage already wrote.
    from app.services.bootstrap_preconditions import record_archive_result

    with psycopg.connect(database_url) as conn:
        try:
            record_archive_result(
                conn,
                bootstrap_run_id=run_id,
                stage_key=stage_key,
                archive_name="__job__",
                rows_written=0,
            )
            conn.commit()
        except Exception as exc:  # noqa: BLE001 — auditing must not fail the stage
            logger.warning(
                "bootstrap stage %s: failed to record __job__ result: %s",
                stage_key,
                exc,
            )

    with psycopg.connect(database_url) as conn:
        mark_stage_success(conn, run_id=run_id, stage_key=stage_key)
        conn.commit()
    return _StageOutcome(stage_key=stage_key, success=True, error=None)


def _should_run(stage_status: str) -> bool:
    """Pre-check from the stage execution contract.

    On a fresh run every stage starts ``pending`` and runs. On a
    retry-failed pass, stages already in ``success`` are skipped so
    we touch only the affected stages.
    """
    return stage_status != "success"


# ---------------------------------------------------------------------------
# Lane runners
# ---------------------------------------------------------------------------


def _run_lane(
    *,
    run_id: int,
    lane_specs: Sequence[tuple[str, str, str, str, Callable[[Mapping[str, Any]], None]]],
    database_url: str,
    log_label: str,
) -> None:
    """Run a sequence of stages serially within a single lane.

    ``lane_specs`` is a sequence of
    ``(stage_key, job_name, lane, current_status, invoker)`` tuples.
    """
    logger.info("bootstrap %s lane: starting (%d stages)", log_label, len(lane_specs))
    for stage_key, job_name, _lane, status, invoker in lane_specs:
        if not _should_run(status):
            logger.info("bootstrap %s lane: skipping %s (already %s)", log_label, stage_key, status)
            continue
        outcome = _run_one_stage(
            run_id=run_id,
            stage_key=stage_key,
            job_name=job_name,
            invoker=invoker,
            database_url=database_url,
        )
        if outcome.success:
            logger.info("bootstrap %s lane: %s OK", log_label, stage_key)
        else:
            logger.warning("bootstrap %s lane: %s ERROR (%s)", log_label, stage_key, outcome.error)
    logger.info("bootstrap %s lane: done", log_label)


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------


@dataclass
class _RunnableStage:
    stage_key: str
    job_name: str
    lane: str
    invoker: Callable[[Mapping[str, Any]], None]
    requires: tuple[str, ...]
    # PR1c #1064: per-stage params dict from ``StageSpec.params``.
    # Default empty so existing test fixtures that build
    # ``_RunnableStage`` directly without params keep working.
    params: Mapping[str, Any] = field(default_factory=dict)


def _phase_batched_dispatch(
    *,
    run_id: int,
    runnable: list[_RunnableStage],
    database_url: str,
    preexisting_statuses: dict[str, str] | None = None,
) -> tuple[dict[str, str], bool]:
    """Dispatch ``runnable`` stages in phase-batched fashion with lane concurrency.

    Returns a tuple ``(statuses, cancelled)``:

    * ``statuses`` — ``{stage_key: terminal_status}`` (success / error /
      blocked / skipped) for every input stage.
    * ``cancelled`` — True if the dispatcher exited early due to an
      observed cooperative-cancel signal at a checkpoint. The caller
      uses this to skip ``finalize_run`` (the run is already in the
      terminal ``cancelled`` state).

    Algorithm:

      1. Build per-stage status map (initially ``pending``).
      2. **Cancel checkpoint** — at the top of each iteration check
         ``is_stop_requested`` against ``(target_run_kind='bootstrap_run',
         target_run_id=run_id)``. On observed cancel: mark stop
         request observed, call ``mark_run_cancelled`` (terminalises
         run + state + sweeps remaining stages), mark stop request
         completed, and return early. This is the operator-cancel
         observation point per spec §Cancel semantics — cooperative.
      3. While any stage is pending: collect every pending stage whose
         ``requires`` are all ``success`` → "ready batch". Stages
         whose any required dep is ``error``/``blocked`` →
         immediately propagate to ``blocked`` (no invocation).
      4. Group the ready batch by lane. For each lane, run up to
         ``_LANE_MAX_CONCURRENCY[lane]`` stages concurrently via a
         per-lane ``ThreadPoolExecutor``.
      5. Join all lane workers; refresh status from the DB; loop.
      6. Stop when no stage is pending.

    Stages with no `requires` start in the first batch. The dispatcher
    is fully data-driven by ``_STAGE_REQUIRES`` + ``_STAGE_LANE_OVERRIDES``.

    Cancel observation latency: at most the duration of the longest
    in-flight batch (a 13F sweep is ~30 min; CIK refresh ~30s).
    Mid-stage work runs to completion — the watermark advances on
    commit and the next Iterate resumes from there.
    """
    from concurrent.futures import ThreadPoolExecutor, wait

    by_key = {r.stage_key: r for r in runnable}
    statuses: dict[str, str] = {r.stage_key: "pending" for r in runnable}
    # Merge in upstream stages already in a terminal state so the
    # dependency check sees them.
    if preexisting_statuses:
        for key, status in preexisting_statuses.items():
            if key not in statuses:
                statuses[key] = status

    while True:
        # Cancel checkpoint — covers (W1) "before submitting Phase A's
        # first batch", "between any two ready batches", "before
        # kicking off Phase B lanes", and "between stages within a
        # lane" (the loop body re-enters here after every wait()).
        #
        # Single-tx atomicity (Codex pre-push round 1 WARNING W4):
        # observation, cancellation, and stop-completion all commit
        # together. A worker crash between two of three separate
        # commits would otherwise leave the stop row observed-but-
        # unfinished with the run still ``running``. Boot-recovery
        # would still clean up after the next jobs restart, but
        # collapsing into one tx makes the happy path clean.
        with psycopg.connect(database_url) as cancel_conn:
            stop = is_stop_requested(
                cancel_conn,
                target_run_kind="bootstrap_run",
                target_run_id=run_id,
            )
            if stop is not None:
                logger.info(
                    "bootstrap dispatcher: cancel observed at checkpoint (run_id=%d, stop_id=%d, mode=%s)",
                    run_id,
                    stop.id,
                    stop.mode,
                )
                with cancel_conn.transaction():
                    mark_stop_observed(cancel_conn, stop.id)
                    mark_run_cancelled(
                        cancel_conn,
                        run_id=run_id,
                        notes_line="cancelled by operator at dispatcher checkpoint",
                    )
                    mark_stop_completed(cancel_conn, stop.id)
                return statuses, True

        pending_keys = [k for k, s in statuses.items() if s == "pending"]
        if not pending_keys:
            break

        # Propagate blocked status from upstream failure.
        ready: list[_RunnableStage] = []
        for key in pending_keys:
            stage = by_key[key]
            # Stages whose required upstream is unknown to this run
            # (not in `statuses`) are treated as a failed dependency
            # too — without this guard, a typo in _STAGE_REQUIRES
            # would let the stage dispatch as if the dep were
            # satisfied. Codex review BLOCKING (PR #1039).
            unknown_reqs = [req for req in stage.requires if req not in statuses]
            failed_reqs = [req for req in stage.requires if req in statuses and statuses[req] in ("error", "blocked")]
            unmet_reqs = [req for req in stage.requires if req in statuses and statuses[req] == "pending"]
            if unknown_reqs or failed_reqs:
                reason_parts = list(failed_reqs) + [f"{r} (unknown to run)" for r in unknown_reqs]
                with psycopg.connect(database_url) as conn:
                    mark_stage_blocked(
                        conn,
                        run_id=run_id,
                        stage_key=key,
                        reason=f"upstream stage(s) failed/blocked: {', '.join(reason_parts)}",
                    )
                    conn.commit()
                statuses[key] = "blocked"
                logger.warning(
                    "bootstrap dispatcher: %s BLOCKED (upstream %s)",
                    key,
                    reason_parts,
                )
                continue
            if unmet_reqs:
                continue  # wait for next iteration
            ready.append(stage)

        if not ready:
            # No stage advanced this iteration. Any stage still in
            # `pending` means its requirements are stuck (e.g. all
            # in unmet_reqs) — the dispatcher cannot make progress.
            # Mark them blocked so finalize_run sees a terminal state
            # and the operator panel doesn't show "pending forever".
            # Codex review BLOCKING (PR #1039).
            stuck_keys = [k for k, s in statuses.items() if s == "pending"]
            for key in stuck_keys:
                with psycopg.connect(database_url) as conn:
                    mark_stage_blocked(
                        conn,
                        run_id=run_id,
                        stage_key=key,
                        reason="dispatcher could not resolve dependencies; stage abandoned",
                    )
                    conn.commit()
                statuses[key] = "blocked"
                logger.warning(
                    "bootstrap dispatcher: %s ABANDONED (deadlock in dependency graph)",
                    key,
                )
            break

        # Group ready by lane. Per-lane, dispatch only up to
        # ``max_concurrency`` stages in this iteration — the rest stay
        # pending and roll into the next iteration. This prevents a
        # long-running stage in one lane (e.g. sec_first_install_drain
        # in sec_rate) from blocking blocked-status propagation in
        # other lanes (e.g. db lane's C-stages waiting on a failed
        # sec_bulk_download). Without this cap, ``wait()`` blocks on
        # the entire heterogeneous batch, leaving the operator panel
        # showing C-stages as ``pending`` long after their upstream
        # has failed.
        by_lane_batch: dict[str, list[_RunnableStage]] = {}
        for stage in ready:
            by_lane_batch.setdefault(stage.lane, []).append(stage)

        # Cap each lane to its max_concurrency. Stages over the cap
        # stay in `pending` and re-enter the next outer iteration.
        for lane, stages in list(by_lane_batch.items()):
            cap = _LANE_MAX_CONCURRENCY.get(lane, 1)
            by_lane_batch[lane] = stages[:cap]

        logger.info(
            "bootstrap dispatcher: ready batch — %s",
            {lane: [s.stage_key for s in stages] for lane, stages in by_lane_batch.items()},
        )

        # One ThreadPoolExecutor per lane, sized to lane's concurrency.
        # Lanes run concurrently with each other; within a lane,
        # the cap above ensures we submit no more than max_concurrency.
        lane_executors: list[ThreadPoolExecutor] = []
        all_futures = []
        try:
            for lane, stages in by_lane_batch.items():
                max_concurrency = _LANE_MAX_CONCURRENCY.get(lane, 1)
                ex = ThreadPoolExecutor(
                    max_workers=max_concurrency,
                    thread_name_prefix=f"bootstrap-{lane}",
                )
                lane_executors.append(ex)
                for stage in stages:
                    # PR1c #1064: resolve dispatch-time dynamic values
                    # (e.g. _PARAM_DYNAMIC_BOOTSTRAP_13F_CUTOFF →
                    # ``today() - 380d``) and validate via the canonical
                    # registry validator with allow_internal_keys=True
                    # so audit-only keys (``source_label``) pass through.
                    # Validation failure is a programmer error in the
                    # stage spec — fail-fast, dispatch surfaces it as a
                    # stage error.
                    resolved = _resolve_dynamic_params(stage.params)
                    try:
                        validated_params = validate_job_params(
                            stage.job_name,
                            resolved,
                            allow_internal_keys=True,
                        )
                    except ParamValidationError as exc:
                        logger.error(
                            "bootstrap dispatcher: stage %s params invalid: %s",
                            stage.stage_key,
                            exc,
                        )
                        with psycopg.connect(database_url) as conn:
                            mark_stage_error(
                                conn,
                                run_id=run_id,
                                stage_key=stage.stage_key,
                                error_message=f"stage params invalid: {exc}",
                            )
                            conn.commit()
                        statuses[stage.stage_key] = "error"
                        continue

                    fut = ex.submit(
                        _run_one_stage,
                        run_id=run_id,
                        stage_key=stage.stage_key,
                        job_name=stage.job_name,
                        invoker=stage.invoker,
                        database_url=database_url,
                        params=validated_params,
                    )
                    all_futures.append((stage.stage_key, fut))
            wait([f for _, f in all_futures])
        finally:
            for ex in lane_executors:
                ex.shutdown(wait=True)

        for stage_key, fut in all_futures:
            outcome = fut.result()
            if outcome.skipped:
                statuses[stage_key] = "skipped"
                logger.info("bootstrap dispatcher: %s SKIPPED", stage_key)
            elif outcome.cancelled:
                # PR3d #1064 — stage observed operator cancel
                # mid-loop and exited cooperatively. Status maps to
                # 'cancelled' so the Timeline tones gray; the
                # outer cancel checkpoint at the next loop iteration
                # picks up the run-level cancel signal and sweeps
                # remaining stages.
                statuses[stage_key] = "cancelled"
                logger.info("bootstrap dispatcher: %s CANCELLED (%s)", stage_key, outcome.error)
            elif outcome.success:
                statuses[stage_key] = "success"
                logger.info("bootstrap dispatcher: %s OK", stage_key)
            else:
                statuses[stage_key] = "error"
                logger.warning("bootstrap dispatcher: %s ERROR (%s)", stage_key, outcome.error)

    return statuses, False


def run_bootstrap_orchestrator() -> None:
    """``_INVOKERS['bootstrap_orchestrator']`` — drive a queued run
    via lane-aware phase-batched dispatch (#1020).

    Replaces the prior "init thread + 2 lane threads" model with a
    data-driven dependency-graph dispatcher: stages declare
    ``requires`` in ``_STAGE_REQUIRES``; dispatcher fans out ready
    batches respecting per-lane ``max_concurrency``.
    """
    # Lazy import: app.jobs.runtime imports app.services.bootstrap_orchestrator
    # via the orchestrator job invoker registration, and importing back the
    # other way at module load would be a circular import.
    from app.jobs.runtime import _INVOKERS

    database_url = settings.database_url

    with psycopg.connect(database_url) as conn:
        snapshot = read_latest_run_with_stages(conn)
    if snapshot is None:
        logger.error("bootstrap_orchestrator: no bootstrap_runs row found; nothing to do")
        return
    run_id = snapshot.run_id
    if snapshot.run_status != "running":
        logger.info(
            "bootstrap_orchestrator: latest run %d is %r; nothing to do",
            run_id,
            snapshot.run_status,
        )
        return

    # PR1c #1064 — build a stage_key → params lookup from the static
    # ``_BOOTSTRAP_STAGE_SPECS`` so the per-stage params dict can be
    # plumbed into ``_RunnableStage`` below. ``bootstrap_stages`` in
    # DB doesn't store params (immutable across runs; lives in code),
    # so the dispatch path consults the spec table at run time.
    stage_params_by_key: dict[str, Mapping[str, Any]] = {spec.stage_key: spec.params for spec in _BOOTSTRAP_STAGE_SPECS}

    # Pre-populate statuses with stages already in a terminal state
    # so the dependency graph sees them when a downstream pending
    # stage's `requires` references them. Without this, a retry pass
    # could treat an upstream `error`/`blocked` row as satisfied
    # because that upstream was filtered out of `runnable`. Codex
    # review BLOCKING for #1020 PR2.
    preexisting_statuses: dict[str, str] = {}
    runnable: list[_RunnableStage] = []
    for stage in sorted(snapshot.stages, key=lambda s: s.stage_order):
        # Skip stages already in a terminal state (re-runs); record
        # their status so dispatch dependency checks see them.
        if stage.status in ("success", "error", "blocked", "skipped"):
            preexisting_statuses[stage.stage_key] = stage.status
            logger.info("bootstrap dispatcher: skipping %s (already %s)", stage.stage_key, stage.status)
            continue
        invoker = _INVOKERS.get(stage.job_name)
        if invoker is None:
            logger.error(
                "bootstrap dispatcher: stage %s has unknown job_name %r; marking error",
                stage.stage_key,
                stage.job_name,
            )
            with psycopg.connect(database_url) as conn:
                mark_stage_running(conn, run_id=run_id, stage_key=stage.stage_key)
                mark_stage_error(
                    conn,
                    run_id=run_id,
                    stage_key=stage.stage_key,
                    error_message=f"unknown job_name {stage.job_name!r}",
                )
                conn.commit()
            continue
        runnable.append(
            _RunnableStage(
                stage_key=stage.stage_key,
                job_name=stage.job_name,
                lane=_effective_lane(stage.stage_key, stage.lane),
                invoker=invoker,
                requires=_STAGE_REQUIRES.get(stage.stage_key, ()),
                params=stage_params_by_key.get(stage.stage_key, {}),
            )
        )

    logger.info(
        "bootstrap dispatcher: run_id=%d runnable=%d (lane breakdown: %s)",
        run_id,
        len(runnable),
        {lane: sum(1 for r in runnable if r.lane == lane) for lane in _LANE_MAX_CONCURRENCY},
    )

    _statuses, cancelled = _phase_batched_dispatch(
        run_id=run_id,
        runnable=runnable,
        database_url=database_url,
        preexisting_statuses=preexisting_statuses,
    )

    if cancelled:
        # The cancel checkpoint already terminalised the run via
        # mark_run_cancelled; finalize_run would no-op against the
        # status='running' guard, but skipping it is clearer.
        logger.info("bootstrap dispatcher: run_id=%d cancelled by operator", run_id)
        return

    with psycopg.connect(database_url) as conn:
        terminal = finalize_run(conn, run_id=run_id)
    logger.info("bootstrap dispatcher: run_id=%d finalised as %s", run_id, terminal)


# PR1c #1064 — three bespoke wrappers
# (``bootstrap_filings_history_seed``, ``sec_first_install_drain_job``,
# ``bootstrap_sec_13f_recent_sweep_job``) deleted. Their bodies were
# lifted into params-aware ``JobInvoker`` bodies in
# ``app/workers/scheduler.py`` (``filings_history_seed``,
# ``sec_first_install_drain``, extended ``sec_13f_quarterly_sweep``).
# Bootstrap stages 14, 15, 21 dispatch the promoted bodies via
# ``StageSpec.params``; the deleted JOB_* constants are gone too,
# so any straggling reference fails fast on import.


__all__ = [
    "JOB_BOOTSTRAP_ORCHESTRATOR",
    "JOB_DAILY_CIK_REFRESH",
    "JOB_DAILY_FINANCIAL_FACTS",
    "get_bootstrap_stage_specs",
    "run_bootstrap_orchestrator",
]


# Stage count assertion — pin so a future refactor that adds /
# removes a spec deliberately surfaces in code review and doesn't
# silently break the tests + frontend that hardcode "17 stages".
assert len(_BOOTSTRAP_STAGE_SPECS) == 24, (
    f"_BOOTSTRAP_STAGE_SPECS expected 24 stages, got {len(_BOOTSTRAP_STAGE_SPECS)}; "
    "update the spec, frontend, and stage_count tests in lockstep. "
    "#1027 added 7 bulk-archive stages (sec_bulk_download + C1.a/C2/C3/C4/C5 ingesters + C1.b walker)."
)
