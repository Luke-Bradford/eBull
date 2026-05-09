"""Source-level advisory locks.

A job that hits a shared rate-bucket must not be allowed to start
concurrently with another job in the same bucket -- whether the second
start comes from a manual trigger double-click, an APScheduler fire
overlapping a still-running manual run, two operators clicking the
same button at once, OR a different job_name that happens to share
the same source. The serialisation primitive is a Postgres
session-scoped advisory lock keyed on a deterministic hash of the
job's **source** (operator-locked decision #1064; PR1a refactor).

## Source-level vs per-job semantics

Pre-PR1a: lock keyed on ``hashtext(job_name)``. Two different SEC jobs
running concurrently would each acquire their own lock and starve the
shared SEC 10 req/s rate budget — the budget is per-IP, not per-job,
so per-job locking conflated independent rate buckets.

Post-PR1a: lock keyed on ``hashtext(f'job_source:{source}')`` where
``source`` is resolved from the canonical
``app.jobs.sources.JOB_NAME_TO_SOURCE`` registry. Same-source jobs
serialise under one lock; cross-source jobs run in parallel. This
matches the rate-bucket reality: ``sec_rate`` is one bucket;
``etoro`` is another; ``db`` is another; ``sec_bulk_download`` is
disjoint from ``sec_rate``.

Same-``job_name`` + different ``params`` semantics: the second
invocation still serialises under the source lock (Codex round-1
WARNING — per-param-set lock identity is deferred to v2).

## Why session-scoped (``pg_try_advisory_lock``)

The job functions in ``app/workers/scheduler.py`` open and close
*their own* connections during execution. A transaction-scoped lock
would be tied to the wrapper's transaction and either be released
before the job finishes, or force every job to run on the same
connection it was launched from -- both wrong. A session-scoped lock
is held for the lifetime of the connection that took it; we hold that
connection in the ``JobLock`` context manager and release the lock
explicitly on exit. Postgres also releases automatically if the
connection dies, so a crashed worker cannot leave a source
permanently locked.

## Why ``hashtext``

``pg_advisory_lock`` takes a ``bigint`` (or two ``int4``s). The
``job_source:{source}`` key is a Python string. ``hashtext`` is the
stable Postgres function that returns an ``int4`` from a string. With
only five distinct sources, hash collisions are vanishingly unlikely.

## Test fixtures

Production callers MUST resolve job_name through the registry —
unknown names raise ``KeyError`` at lock acquisition. Tests that need
a lock keyed on an arbitrary string can use
``JobLock.test_only_per_name``, which keys the lock on
``hashtext(job_name)`` directly (the pre-PR1a behaviour). Production
code never calls this constructor.
"""

from __future__ import annotations

import logging
from types import TracebackType

import psycopg

from app.jobs.sources import source_for

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
    """Context manager that holds a session-scoped source-level advisory lock.

    Usage::

        with JobLock(database_url, "nightly_universe_sync"):
            run_the_job()

    On enter:
      * Resolves ``job_name -> source`` via ``app.jobs.sources.source_for``.
        Raises ``KeyError`` if ``job_name`` is unknown (production
        callers MUST register; tests use
        :meth:`JobLock.test_only_per_name`).
      * Opens a dedicated short-lived connection.
      * Calls ``pg_try_advisory_lock(hashtext('job_source:{source}')::int)``.
        If the function returns false, raises :class:`JobAlreadyRunning`
        without holding the connection open.
      * If it returns true, the connection is kept open until exit.

    On exit:
      * Calls ``pg_advisory_unlock(...)`` and closes the connection.
        ``pg_advisory_unlock`` returning false would indicate the lock
        was not held — it never should be — and is logged as a warning
        rather than raised, because the exit path must not mask an
        in-flight exception.
    """

    def __init__(self, database_url: str, job_name: str) -> None:
        self._database_url = database_url
        self._job_name = job_name
        self._lock_key = self._lock_key_for(job_name)
        self._conn: psycopg.Connection[object] | None = None

    @staticmethod
    def _lock_key_for(job_name: str) -> str:
        """Resolve the source-level lock key for ``job_name``.

        Production path. Raises ``KeyError`` for unknown job_name —
        the registry is the single source of truth and silent fallback
        violates the source-lock decision (Codex round-1 BLOCKING).
        """
        return f"job_source:{source_for(job_name)}"

    @classmethod
    def test_only_per_name(cls, database_url: str, job_name: str) -> JobLock:
        """Test-only escape hatch: lock keyed on ``hashtext(job_name)``.

        Reproduces the pre-PR1a per-name behaviour for unit tests that
        construct synthetic job names. Production code MUST NOT call
        this — the registry is the source of truth.
        """
        instance = cls.__new__(cls)
        instance._database_url = database_url
        instance._job_name = job_name
        instance._lock_key = job_name  # raw, not source-prefixed
        instance._conn = None
        return instance

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
                    (self._lock_key,),
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
                    (self._lock_key,),
                )
                row = cur.fetchone()
            released = bool(row and row[0])  # type: ignore[index]
            if not released:
                # Lock was not held by this session. Should never happen
                # because __enter__ only stores the connection on a
                # successful acquire. Log loud rather than raise so we
                # do not clobber a real exception unwinding through us.
                logger.warning(
                    "JobLock release for %r (key=%r) returned false -- lock was not held by this session at exit time",
                    self._job_name,
                    self._lock_key,
                )
        except Exception:
            logger.exception(
                "JobLock release for %r (key=%r) failed; closing connection anyway",
                self._job_name,
                self._lock_key,
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
