"""Job runtime: APScheduler + manual trigger glue.

Owns one ``BackgroundScheduler`` for the lifetime of the FastAPI app.
The scheduler runs job functions on its own thread pool; the manual
trigger endpoint also routes through the same wrapper so a manual run
and a scheduled fire are indistinguishable from a tracking standpoint.

PR A scope (locked):

* Wires exactly one job -- ``nightly_universe_sync`` -- so the runtime
  can be exercised end-to-end without trying to bring up nine providers
  in one go. PR B will populate the rest of ``_INVOKERS`` from the
  declared registry.
* No catch-up. ``coalesce=True`` + ``misfire_grace_time=0`` means a
  freshly-restarted scheduler will *not* fire missed runs at boot.
  Catch-up belongs in PR C and will be driven by reading ``job_runs``,
  not by APScheduler's in-memory state.
* No pipeline runner.
* The manual trigger response carries no run_id -- the operator looks
  status up via the existing ``/system/status``. Avoids a post-hoc
  lookup race for run_id, and PR B's listing endpoint will provide a
  richer response shape.

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
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Final

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import settings
from app.jobs.locks import JobAlreadyRunning, JobLock
from app.workers.scheduler import (
    JOB_NIGHTLY_UNIVERSE_SYNC,
    SCHEDULED_JOBS,
    Cadence,
    nightly_universe_sync,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Job invoker registry
# ---------------------------------------------------------------------------
#
# Maps job names from ``SCHEDULED_JOBS`` to the actual callable that
# performs the work. PR A wires only ``nightly_universe_sync``. PR B will
# add the remaining 8 jobs from ``app/workers/scheduler.py``.
#
# Keeping the registry as a single ``dict[str, Callable[[], None]]``
# rather than a class hierarchy is deliberate: every job has the same
# zero-argument shape, the wrapper is identical, and there is nothing
# job-specific to abstract over yet. If a future job needs arguments
# (it should not, per the design notes in #13), reach for a richer
# shape then -- not pre-emptively.

_INVOKERS: Final[dict[str, Callable[[], None]]] = {
    JOB_NIGHTLY_UNIVERSE_SYNC: nightly_universe_sync,
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
        self._scheduler = BackgroundScheduler(
            timezone="UTC",
            job_defaults={
                # Collapse multiple missed fires of the same recurring
                # job into a single run. Without this a scheduler that
                # restarts after a long downtime would attempt to fire
                # every missed instance.
                "coalesce": True,
                # ``misfire_grace_time=0`` -- if the fire time has
                # passed by even a second, the run is considered
                # missed and skipped. Combined with the absence of a
                # persistent jobstore, this means PR A's runtime
                # explicitly does NOT catch up missed runs on
                # startup. Catch-up is PR C.
                "misfire_grace_time": 0,
                # One concurrent instance per job. The per-job
                # advisory lock is the source of truth for
                # serialisation; this is a defensive second layer.
                "max_instances": 1,
            },
        )
        # Manual triggers run on a small dedicated pool so they cannot
        # starve the scheduler's recurring threads. max_workers=1
        # because the per-job lock would serialise concurrent manual
        # triggers anyway -- a larger pool would only let unrelated
        # jobs queue, which is not a v1 requirement.
        self._manual_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="job-manual")
        self._started = False

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

    def shutdown(self) -> None:
        """Stop the scheduler and wait for in-flight jobs to drain.

        Called from the FastAPI lifespan teardown *before* the
        connection pool is closed so any job currently writing to
        ``job_runs`` can finish cleanly. ``wait=True`` blocks until
        the scheduler's worker threads return.
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

    # -- triggers ----------------------------------------------------------

    def trigger(self, job_name: str) -> None:
        """Submit a manual run of *job_name* to the executor.

        Returns as soon as the run is queued. Does NOT wait for the
        job to finish -- the API endpoint returns 202 Accepted with
        no body and the operator polls ``/system/status`` for
        results.

        Raises:
            UnknownJob: ``job_name`` is not in the invoker registry.
            JobAlreadyRunning: another instance of this job currently
                holds the per-job advisory lock.

        The lock is acquired *synchronously on the calling thread*
        before the job is submitted to the executor, not inside the
        worker. This is the only way the API endpoint can return a
        409 Conflict to the operator who clicked the button -- if
        the lock acquire happened inside the worker, the endpoint
        would have already returned 202 by then.
        """
        invoker = self._invokers.get(job_name)
        if invoker is None:
            raise UnknownJob(job_name)

        # Acquire the lock now so a contention failure surfaces
        # synchronously to the API caller. The lock is then *handed
        # off* to the worker thread which is responsible for
        # releasing it via the context-manager exit. The worker uses
        # the same JobLock instance via a closure.
        lock = JobLock(self._database_url, job_name)
        lock.__enter__()  # may raise JobAlreadyRunning
        try:
            self._manual_executor.submit(self._run_with_held_lock, job_name, invoker, lock)
        except Exception:
            # Submission failed before the worker took ownership of
            # the lock -- release it here so we do not strand it.
            lock.__exit__(None, None, None)
            raise

    # -- internals ---------------------------------------------------------

    def _wrap_invoker(self, job_name: str, invoker: Callable[[], None]) -> Callable[[], None]:
        """Wrap a scheduled invoker with the per-job advisory lock.

        The scheduled fire path takes the lock inside the worker (no
        operator is waiting for a synchronous response, so we do not
        need the surface-level 409 path the manual trigger uses).
        Lock contention here is a normal condition -- a scheduled
        fire that overlaps a still-running manual trigger -- and is
        logged at INFO and skipped, not raised, because APScheduler
        would otherwise log a noisy traceback for an expected race.
        """
        database_url = self._database_url

        def wrapped() -> None:
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

    @staticmethod
    def _run_with_held_lock(
        job_name: str,
        invoker: Callable[[], None],
        lock: JobLock,
    ) -> None:
        """Worker-thread entry point for manual triggers.

        The lock is *already held* by the time we enter -- ``trigger``
        acquired it on the request thread so a 409 could be returned
        to the operator. Our job is to invoke the function and then
        release the lock no matter what happens.
        """
        try:
            invoker()
        except Exception:
            logger.exception("manual trigger of %r raised", job_name)
        finally:
            lock.__exit__(None, None, None)


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
