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

import logging
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
from app.workers.scheduler import (
    JOB_ATTRIBUTION_SUMMARY,
    JOB_DAILY_CANDLE_REFRESH,
    JOB_DAILY_CIK_REFRESH,
    JOB_DAILY_FINANCIAL_FACTS,
    JOB_DAILY_NEWS_REFRESH,
    JOB_DAILY_PORTFOLIO_SYNC,
    JOB_DAILY_RESEARCH_REFRESH,
    JOB_DAILY_TAX_RECONCILIATION,
    JOB_DAILY_THESIS_REFRESH,
    JOB_EXECUTE_APPROVED_ORDERS,
    JOB_FUNDAMENTALS_SYNC,
    JOB_FX_RATES_REFRESH,
    JOB_MONITOR_POSITIONS,
    JOB_MONTHLY_REPORT,
    JOB_MORNING_CANDIDATE_REVIEW,
    JOB_NIGHTLY_UNIVERSE_SYNC,
    JOB_ORCHESTRATOR_FULL_SYNC,
    JOB_ORCHESTRATOR_HIGH_FREQUENCY_SYNC,
    JOB_RAW_DATA_RETENTION_SWEEP,
    JOB_RETRY_DEFERRED,
    JOB_SEED_COST_MODELS,
    JOB_WEEKLY_REPORT,
    SCHEDULED_JOBS,
    Cadence,
    ScheduledJob,
    attribution_summary_job,
    compute_next_run,
    daily_candle_refresh,
    daily_cik_refresh,
    daily_financial_facts,
    daily_news_refresh,
    daily_portfolio_sync,
    daily_research_refresh,
    daily_tax_reconciliation,
    daily_thesis_refresh,
    execute_approved_orders,
    fundamentals_sync,
    fx_rates_refresh,
    monitor_positions_job,
    monthly_report,
    morning_candidate_review,
    nightly_universe_sync,
    orchestrator_full_sync,
    orchestrator_high_frequency_sync,
    raw_data_retention_sweep,
    retry_deferred_recommendations_job,
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
    JOB_FX_RATES_REFRESH: fx_rates_refresh,
    JOB_DAILY_CIK_REFRESH: daily_cik_refresh,
    JOB_DAILY_RESEARCH_REFRESH: daily_research_refresh,
    JOB_DAILY_FINANCIAL_FACTS: daily_financial_facts,
    JOB_DAILY_NEWS_REFRESH: daily_news_refresh,
    JOB_DAILY_THESIS_REFRESH: daily_thesis_refresh,
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
}


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

    def shutdown(self) -> None:
        """Stop the scheduler and wait for in-flight jobs to drain.

        Called from the FastAPI lifespan teardown *before* the
        connection pool is closed so any job currently writing to
        ``job_runs`` can finish cleanly. ``wait=True`` blocks until
        the scheduler's worker threads return.

        Trade-off (PR #131 round 1 review WARNING 2): a hung job
        will block lifespan teardown for as long as it takes the
        process to be killed. The alternative -- ``wait=False`` --
        would let in-flight jobs continue running while the lifespan
        proceeds to close the connection pool, at which point the
        job's writes to ``job_runs`` would fail against a closed
        pool. That is strictly worse: a hung job under ``wait=True``
        produces a visible "lifespan teardown stuck" symptom that
        the operator notices and can investigate, whereas under
        ``wait=False`` the symptom is silent corruption of job
        tracking state. We accept the blocking behaviour for v1.
        """
        if not self._started:
            return
        try:
            self._scheduler.shutdown(wait=True)
        except Exception:
            logger.exception("JobRuntime scheduler shutdown raised")
        try:
            self._manual_executor.shutdown(wait=True)
        except Exception:
            logger.exception("JobRuntime manual executor shutdown raised")
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
            fut = self._manual_executor.submit(self._run_manual, job_name, invoker)
            fut.add_done_callback(self._log_future_exception)
        except Exception:
            # Submission failed before the worker took ownership --
            # release the in-process lock so a retry can acquire.
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
                    invoker()
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

    def _run_manual(self, job_name: str, invoker: Callable[[], None]) -> None:
        """Worker-thread entry point for manual triggers.

        Single-threaded with respect to the ``JobLock`` connection:
        we acquire, hold, and release the advisory lock entirely on
        this thread. The in-process ``_inflight`` lock that was
        acquired on the request thread is released here in
        ``finally`` so a retry can run.

        ``JobAlreadyRunning`` from the advisory lock here means a
        *different process* (or this process's APScheduler thread,
        for the scheduled-fire path) holds the advisory lock. We log
        and exit; the in-process lock is still released.

        Note on the ``finally``: ``_inflight[job_name]`` is released
        unconditionally, regardless of whether the advisory lock was
        actually obtained. The in-process lock's sole purpose is to
        gate the synchronous 202/409 response on the request thread
        -- it does not track actual execution. Releasing it on every
        worker exit (success, no-op, raise) is correct: the next
        manual trigger should be allowed to attempt acquisition
        fresh. ``threading.Lock`` permits acquire-on-thread-A /
        release-on-thread-B because it is not reentrant and carries
        no owner check.
        """
        try:
            try:
                with JobLock(self._database_url, job_name):
                    invoker()
            except JobAlreadyRunning:
                # Logged at INFO -- this is an expected race (manual
                # trigger landed during a scheduled fire or peer
                # process run), not an operational fault. WARNING
                # would alert-bait every manual trigger during the
                # 02:00 UTC window with no actionable remediation.
                logger.info(
                    "manual trigger of %r no-opped: advisory lock held by "
                    "another runner (scheduled fire or peer process); the "
                    "202 response was returned but the job did not run",
                    job_name,
                )
            except Exception:
                logger.exception("manual trigger of %r raised", job_name)
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
