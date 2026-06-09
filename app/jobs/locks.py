"""Source-level advisory locks.

A job must not start concurrently with another job in the same
**lane** (job-overlap bucket) -- whether the second start comes from a
manual trigger double-click, an APScheduler fire overlapping a
still-running manual run, two operators clicking the same button at
once, OR a different job_name that happens to share the same source.
The serialisation primitive is a Postgres session-scoped advisory lock
keyed on a deterministic hash of the job's **source** (operator-locked
decision #1064; PR1a refactor).

A lane bounds job overlap, NOT request rate (#1478): the SEC 10 req/s
per-IP budget is enforced separately at the HTTP layer (see the
"Source-level vs per-job semantics" section below). Do not conflate
the two.

## Source-level vs per-job semantics

This lock bounds **job overlap**, not request rate. (#1478 correction —
the earlier docstring wrongly implied per-job locking would "starve the
SEC rate budget"; it cannot. The SEC 10 req/s per-IP budget is enforced
at the HTTP layer by ``app/providers/implementations/sec_edgar.py``
``_PROCESS_RATE_LIMIT_CLOCK`` + ``_PROCESS_RATE_LIMIT_LOCK`` — a
process-wide atomic inter-request floor, safe under concurrent fetchers.
Two SEC jobs on different lanes still cannot exceed that floor.)

Pre-PR1a: lock keyed on ``hashtext(job_name)`` (per-job).

Post-PR1a: lock keyed on ``hashtext(f'job_source:{source}')`` where
``source`` is resolved from the canonical
``app.jobs.sources.JOB_NAME_TO_SOURCE`` registry. Same-source jobs
serialise under one lock; cross-source jobs run in parallel. The lane
is a job-overlap bucket chosen per operator policy (#1064), NOT a rate
gate: ``etoro`` serialises eToro-budget jobs; ``sec_rate`` groups the
SEC discovery/producer jobs; ``sec_manifest`` is ``sec_manifest_worker``
alone (#1478 — extracted so the heavy drainer stops starving the
producers); ``sec_bulk_download`` is disjoint from ``sec_rate``.

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

import contextlib
import logging
from collections.abc import Iterator
from contextvars import ContextVar, Token
from types import TracebackType

import psycopg

from app.jobs import sec_lane_gate
from app.jobs.sources import Lane, source_for

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-call-context held-source set (#1184)
# ---------------------------------------------------------------------------
#
# Process-local registry of source-lock buckets currently held by a
# ``JobLock`` instance in this call context. Same-source nested acquires
# in the same context are treated as re-entrant: the inner ``JobLock``
# detects the source is already held and skips the Postgres acquire that
# would otherwise self-collide (different psycopg session → Postgres
# rejects → ``JobAlreadyRunning``).
#
# The mechanism is process-wide and not orchestrator-specific. Today the
# only nesting pattern in the codebase is the orchestrator dispatch path
# (outer ``JobLock(orchestrator_*_sync, source='db')`` → inner adapter
# ``JobLock(<db-lane-job>, source='db')``), so this scope choice has no
# observable side effect today. Any future code path that nests JobLock
# acquires against the same source benefits from the correct re-entrant
# behaviour without an additional opt-in.
#
# Cross-thread / cross-process serialisation is unchanged: a new thread
# starts with an empty ``_HELD_SOURCES`` (Python ContextVar is NOT
# auto-propagated across ``threading.Thread``), so an acquire on a
# sibling thread goes to the real Postgres advisory lock and collides
# normally. See spec
# ``docs/superpowers/specs/2026-05-17-orchestrator-inner-lock-removal.md``
# §6.1 and tests/test_job_lock_reentrancy.py for the regression gate.
_HELD_SOURCES: ContextVar[frozenset[Lane]] = ContextVar("_joblock_held_sources", default=frozenset())


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


# ---------------------------------------------------------------------------
# Operator-runbook helpers (#1233 PR-D)
# ---------------------------------------------------------------------------
#
# Two helpers consumed by ``app/runbooks/*``: a side-effect-free PROBE
# (acquire-and-release on a short-lived autocommit conn) and a
# CONTEXT-MANAGER FENCE (hold across a destructive phase against the
# ``postgres`` administrative DB so the session survives
# ``DROP DATABASE ebull_dev``). Postgres advisory locks are
# CLUSTER-WIDE so either DB observes the same lock.


def probe_jobs_process_running(database_url: str) -> bool:
    """Return True iff the jobs process appears to be running.

    Tries to acquire ``JOBS_PROCESS_LOCK_KEY`` on a short-lived
    autocommit connection against ``database_url`` (the APPLICATION
    DB). If the acquire succeeds, the jobs entrypoint does NOT
    currently hold the fence on this DB — release immediately and
    return False. If the acquire fails, the fence is held by another
    session — return True.

    PG advisory locks are PER-DATABASE (NOT cluster-wide) — verified
    empirically against PG 17. Two sessions on different databases in
    the same cluster acquire the same key independently. The jobs
    entrypoint holds its fence on the application DB; ``database_url``
    here MUST be the same DB.

    Operator runbooks under ``app/runbooks/`` use this as a pre-flight
    to refuse running against a live jobs process (#1233 PR-D).
    """
    with psycopg.connect(database_url, autocommit=True) as probe:
        row = probe.execute("SELECT pg_try_advisory_lock(%s)", (JOBS_PROCESS_LOCK_KEY,)).fetchone()
        got = bool(row[0]) if row else False
        if got:
            probe.execute("SELECT pg_advisory_unlock(%s)", (JOBS_PROCESS_LOCK_KEY,))
            return False
        return True


@contextlib.contextmanager
def acquire_jobs_process_fence(database_url: str) -> Iterator[None]:
    """Hold ``JOBS_PROCESS_LOCK_KEY`` on the APPLICATION DB.

    Blocks the jobs entrypoint from acquiring its fence (they share
    the same per-database lockspace). Releases on context exit so
    the jobs process can acquire.

    LIMITATION (PR-D v3.2 fix — caught by empirical test against PG 17):
    PG advisory locks are per-database, NOT cluster-wide. The fence
    connection DIES when ``DROP DATABASE ebull_dev`` runs against the
    DB it was opened on. Callers that need to bracket a DROP must
    re-acquire on the FRESH database after CREATE + migration. The
    TOCTOU window between DROP and re-acquire is unavoidable at the
    lock layer alone — operators MUST keep the jobs process stopped
    (e.g. systemd ``stop``, not just SIGINT) for the duration of the
    destructive phase.

    Raises :class:`JobAlreadyRunning` (``job_name='jobs_process'``) if
    the fence cannot be acquired.

    Used by ``app/runbooks/stream_a_run_8_verify.py``.
    """
    with psycopg.connect(database_url, autocommit=True) as fence:
        row = fence.execute("SELECT pg_try_advisory_lock(%s)", (JOBS_PROCESS_LOCK_KEY,)).fetchone()
        got = bool(row[0]) if row else False
        if not got:
            raise JobAlreadyRunning("jobs_process")
        try:
            yield
        finally:
            # Best-effort release. If the DB was dropped under us the
            # conn is dead and this no-ops — Postgres releases the
            # advisory lock automatically when the session ends.
            try:
                fence.execute("SELECT pg_advisory_unlock(%s)", (JOBS_PROCESS_LOCK_KEY,))
            except psycopg.Error:
                pass


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
        # Lane | None — None ONLY for :meth:`test_only_per_name` acquires
        # (raw-job_name escape hatch that intentionally opts out of the
        # #1184 re-entrancy bypass). Production callers always have a
        # non-None Lane resolved via ``source_for``.
        self._source: Lane | None = source_for(job_name)
        self._lock_key = f"job_source:{self._source}"
        self._conn: psycopg.Connection[object] | None = None
        # #1184 re-entrancy state. Set in ``__enter__``; consulted in
        # ``__exit__`` to choose between no-op release (re-entrant
        # bypass — no Postgres acquire happened) and the full release
        # path (unlock + close + contextvar reset).
        self._reentrant: bool = False
        self._held_token: Token[frozenset[Lane]] | None = None
        # #1542 — set when this acquire took the in-process sec_rate gate
        # (the sec_rate lane no longer opens a Postgres advisory connection).
        self._sec_lane_held: bool = False

    @classmethod
    def test_only_per_name(cls, database_url: str, job_name: str) -> JobLock:
        """Test-only escape hatch: lock keyed on ``hashtext(job_name)``.

        Reproduces the pre-PR1a per-name behaviour for unit tests that
        construct synthetic job names. Production code MUST NOT call
        this — the registry is the source of truth.

        ``self._source`` is set to ``None`` so the #1184 re-entrancy
        bypass NEVER fires for ``test_only_per_name`` acquires; the
        per-name semantics is the whole point of the escape hatch.
        Two sibling ``test_only_per_name(url, "x")`` acquires open
        distinct psycopg sessions and collide normally at the Postgres
        layer.
        """
        instance = cls.__new__(cls)
        instance._database_url = database_url
        instance._job_name = job_name
        instance._source = None
        instance._lock_key = job_name  # raw, not source-prefixed
        instance._conn = None
        instance._reentrant = False
        instance._held_token = None
        instance._sec_lane_held = False
        return instance

    def __enter__(self) -> JobLock:
        # #1184 — re-entrant short-circuit. If our source is already
        # held by an outer ``JobLock`` in the same call context, treat
        # this acquire as a no-op. Postgres would otherwise reject the
        # second pg_try_advisory_lock from a different session (the
        # outer holds the lock on its own connection); the application-
        # layer bypass prevents the redundant self-collision. The
        # ``None``-source case (``test_only_per_name``) NEVER takes
        # this branch — its short-circuit on ``is not None`` is
        # intentional.
        held = _HELD_SOURCES.get()
        if self._source is not None and self._source in held:
            self._reentrant = True
            return self
        # #1542 — sec_rate is an in-process gate, NOT a pg-advisory lock.
        # Up to SEC_LANE_MAX_CONCURRENCY jobs run concurrently; a full gate (or a
        # same-name overlap) raises JobAlreadyRunning, which the #1538 retry
        # wrapper rides out for scheduled fires. No psycopg connection is opened.
        if self._source == "sec_rate":
            if not sec_lane_gate.SEC_LANE_GATE.try_acquire(self._job_name):
                raise JobAlreadyRunning(self._job_name)
            self._sec_lane_held = True
            new_held: frozenset[Lane] = held | frozenset[Lane]({self._source})
            self._held_token = _HELD_SOURCES.set(new_held)
            return self
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
        # Set the contextvar ONLY after the Postgres acquire succeeds
        # so an acquisition that raises does not leak a stale entry.
        # Restored in ``__exit__`` via the saved token.
        if self._source is not None:
            new_held: frozenset[Lane] = held | frozenset[Lane]({self._source})
            self._held_token = _HELD_SOURCES.set(new_held)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._reentrant:
            # Re-entrant acquire opened no connection and did not
            # mutate the contextvar. Nothing to release.
            return
        # Restore the contextvar FIRST (cheap, infallible), then run
        # the Postgres release. Ordering matters for the
        # ``test_reset_restores_prior_held_set_on_exception`` invariant:
        # even if release itself raised we'd still want the contextvar
        # restored. The try/finally in the unlock path keeps the
        # close defended; the contextvar reset is independently safe.
        if self._held_token is not None:
            try:
                _HELD_SOURCES.reset(self._held_token)
            finally:
                self._held_token = None
        # #1542 — sec_rate gate release (no connection was opened). Release
        # FIRST, then clear the ownership flag (NOT in a finally) so a
        # release-raising bug leaves the flag True — the exception propagates
        # AND the held-state stays visible for diagnostics. Clearing the flag
        # before (or in a finally around) the release would suppress that
        # signal — the exact failure mode #1543 review flagged.
        if self._sec_lane_held:
            sec_lane_gate.SEC_LANE_GATE.release(self._job_name)
            self._sec_lane_held = False
            return
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
