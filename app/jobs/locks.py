"""Per-job advisory locks.

A job that is already running must not be allowed to start again
concurrently -- whether the second start comes from a manual trigger
double-click, an APScheduler fire that overlaps a still-running manual
run, or two operators clicking the same button at once. The
serialisation primitive is a Postgres session-scoped advisory lock
keyed on a deterministic hash of the job name.

Why session-scoped (``pg_try_advisory_lock``) rather than
transaction-scoped (``pg_try_advisory_xact_lock``):

* The job functions in ``app/workers/scheduler.py`` open and close
  *their own* connections during execution. A transaction-scoped lock
  would be tied to the wrapper's transaction and either be released
  before the job finishes, or force every job to run on the same
  connection it was launched from -- both wrong.
* A session-scoped lock is held for the lifetime of the connection
  that took it. We hold that connection in the ``JobLock`` context
  manager and release the lock explicitly on exit. The lock is
  guaranteed to be released even if the job raises (the ``finally``
  in ``__exit__``) and is also released by Postgres if the connection
  dies (e.g. process crash, network partition) -- so a crashed worker
  cannot leave a job permanently locked.

Why ``hashtext`` and not the bare name:

``pg_advisory_lock`` takes a ``bigint`` (or two ``int4``s). The job
name is a Python string. ``hashtext`` is a stable Postgres function
that returns an ``int4`` from a string and is the conventional way to
key advisory locks on a textual identifier. Two distinct job names
producing the same hash is theoretically possible but vanishingly
unlikely with the small set of names in ``SCHEDULED_JOBS``; the
collision risk is acceptable for v1 and would manifest as "two
unrelated jobs cannot run concurrently", not as a correctness bug.
"""

from __future__ import annotations

import logging
from types import TracebackType

import psycopg

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Singleton fence (#719)
# ---------------------------------------------------------------------------
#
# Constant BIGINT key for `pg_try_advisory_lock(JOBS_PROCESS_LOCK_KEY)` held
# on a dedicated long-lived connection by the jobs entrypoint. Only one
# `app.jobs` process may run at a time — boot recovery's "claimed by stale
# boot id" reset is only safe under that invariant. Imported by the
# entrypoint; never used inside the API process.
#
# Chosen as an arbitrary BIGINT well clear of any hashtext collision —
# hashtext returns int4, so any value above INT_MAX is collision-free
# against the per-job locks.
JOBS_PROCESS_LOCK_KEY: int = 0x6562_756C_6C5F_4A52  # 'ebull_JR' (ASCII)


class JobAlreadyRunning(RuntimeError):
    """Raised when ``JobLock`` cannot acquire because another holder exists.

    The API layer maps this to a 409 Conflict so the operator sees
    "already running" rather than a generic 500.
    """

    def __init__(self, job_name: str) -> None:
        super().__init__(f"job {job_name!r} is already running")
        self.job_name = job_name


class JobLock:
    """Context manager that holds a session-scoped advisory lock.

    Usage::

        with JobLock(database_url, "nightly_universe_sync"):
            run_the_job()

    On enter:
      * Opens a dedicated short-lived connection (independent of any
        connection the job itself uses).
      * Calls ``pg_try_advisory_lock(hashtext(name)::int)``. If the
        function returns false, raises :class:`JobAlreadyRunning`
        without holding the connection open.
      * If it returns true, the connection is kept open until exit.

    On exit:
      * Calls ``pg_advisory_unlock(...)`` and closes the connection.
        ``pg_advisory_unlock`` returning false at this point would
        indicate the lock was not held -- it never should be -- and
        is logged as a warning rather than raised, because the exit
        path must not mask an in-flight exception.
    """

    def __init__(self, database_url: str, job_name: str) -> None:
        self._database_url = database_url
        self._job_name = job_name
        self._conn: psycopg.Connection[object] | None = None

    def __enter__(self) -> JobLock:
        # autocommit=True so we do NOT hold an implicit transaction
        # open for the entire job duration (PR #131 round 1 review
        # WARNING 1). The advisory lock is session-scoped, not
        # transaction-scoped, so autocommit changes nothing about
        # the lock semantics -- it just stops us wasting a backend
        # transaction slot.
        conn = psycopg.connect(self._database_url, autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_try_advisory_lock(hashtext(%s)::int)",
                    (self._job_name,),
                )
                row = cur.fetchone()
            if row is None or not row[0]:
                conn.close()
                raise JobAlreadyRunning(self._job_name)
        except Exception:
            # Any failure on the entry path must release the connection.
            # Re-raise after cleanup so the caller still sees the error.
            with _suppress_close_errors():
                conn.close()
            raise
        self._conn = conn
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        conn = self._conn
        self._conn = None
        if conn is None:
            return
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_unlock(hashtext(%s)::int)",
                    (self._job_name,),
                )
                row = cur.fetchone()
            released = bool(row and row[0])  # type: ignore[index]
            if not released:
                # Lock was not held by this session. Should never happen
                # because __enter__ only stores the connection on a
                # successful acquire. Log loud rather than raise so we
                # do not clobber a real exception unwinding through us.
                logger.warning(
                    "JobLock release for %r returned false -- lock was not held by this session at exit time",
                    self._job_name,
                )
        except Exception:
            logger.exception(
                "JobLock release for %r failed; closing connection anyway",
                self._job_name,
            )
        finally:
            with _suppress_close_errors():
                conn.close()


class _suppress_close_errors:
    """Best-effort connection close that swallows secondary errors.

    Used on cleanup paths where a primary exception is already in
    flight; we want the close to happen but we must not let a close
    failure replace the original exception.
    """

    def __enter__(self) -> None:
        return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        if exc is not None:
            logger.debug("ignoring close-time exception: %s", exc)
        return True  # suppress
