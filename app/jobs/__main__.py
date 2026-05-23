"""Jobs process entrypoint (#719).

Runs as ``python -m app.jobs``. Owns the JobRuntime (APScheduler +
manual ThreadPoolExecutor), the sync orchestrator's executor, the
reaper, the queue dispatcher, the listener supervisor, and the
heartbeat writer. The FastAPI process serves HTTP only and never
imports any of this.

Locked startup order (per spec; every previous in-process attempt
got bitten by a different ordering bug):

    1.  Logger + signal handlers + stop_event installed.
    2.  Hardened DB pool opened.
    3.  SINGLETON FENCE acquired on a dedicated long-lived
        psycopg.Connection. FATAL exit if held — boot recovery's
        "claimed by stale boot id" reset is only safe under that
        invariant.
    4.  Reaper transitions stale 'running' sync_runs to terminal so
        prereq checks read clean state.
    5.  JobRuntime CONSTRUCTED (no scheduler.start yet — just object
        init). Sync ThreadPoolExecutor created.
    6.  Queue stale-row recovery: reset 'claimed' / 'dispatched' rows
        from prior boots back to 'pending'.
    7.  Queue boot-drain: tight loop claiming all pending rows and
        submitting them through the executors.
    8.  scheduler.start() — registers cron triggers, kicks
        BackgroundScheduler.
    9.  JobRuntime._catch_up() (already wired into start()).
    10. Boot freshness sweep (best-effort `scope='behind'`).
    11. Listener thread started.
    12. Heartbeat threads started (one per supervised subsystem).
    13. Main loop sleeps on stop_event with periodic supervision.

Shutdown reverses, with the singleton fence connection closing LAST
so Postgres releases the advisory lock only after every other
resource has stopped.
"""

from __future__ import annotations

import contextlib
import logging
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any, Final

import psycopg

# #873: importing this package at the worker entry's module-import
# time side-effect-populates ``sec_manifest_worker._PARSERS``. The
# manifest worker dispatches parsers from that dict, so this import
# is load-bearing — without it the worker debug-skips every manifest
# row even when parser modules exist on disk.
import app.services.manifest_parsers  # noqa: F401, E402
from app.config import settings
from app.db.pool import open_pool
from app.jobs.boot_sweep import run_boot_freshness_sweep
from app.jobs.credential_health_listener import (
    listener_loop as credential_health_listener_loop,
)
from app.jobs.heartbeat import HeartbeatWriter, heartbeat_loop
from app.jobs.listener import ListenerState, listener_loop
from app.jobs.locks import JOBS_PROCESS_LOCK_KEY
from app.jobs.runtime import JobRuntime
from app.jobs.supervisor import supervise
from app.security import master_key
from app.security.secrets_crypto import set_active_key as set_broker_encryption_key
from app.services.credential_health_cache import CredentialHealthCache
from app.services.sync_orchestrator.dispatcher import (
    claim_oldest_pending,
    reset_stale_in_flight,
)
from app.services.sync_orchestrator.reaper import reap_orphaned_syncs

logger = logging.getLogger(__name__)


def _install_signal_handlers(stop_event: threading.Event) -> None:
    """Translate SIGTERM / SIGINT (and SIGBREAK on Windows) into stop_event.

    Windows note: VS Code task termination sends CTRL_BREAK_EVENT to
    process groups created with CREATE_NEW_PROCESS_GROUP; signal.SIGBREAK
    catches that. POSIX uses SIGTERM/SIGINT. We register every signal
    that is defined on the current platform.
    """

    def _handler(signum: int, _frame: Any) -> None:
        logger.info("jobs entrypoint: received signal %d, requesting stop", signum)
        stop_event.set()

    for name in ("SIGTERM", "SIGINT", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is not None:
            try:
                signal.signal(sig, _handler)
            except ValueError, OSError:
                logger.debug("jobs entrypoint: signal %s not registrable on this platform", name)


# #1290: application_name stamped on the singleton-fence connection
# so the stale-lock reaper can identify "our" idle-in-transaction
# holder PIDs in ``pg_stat_activity``. Any third-party connection
# that happened to acquire JOBS_PROCESS_LOCK_KEY (vanishingly
# unlikely given its 64-bit ASCII-bytes value) is excluded by the
# application_name filter so we never terminate a process that is
# not ours.
SINGLETON_FENCE_APPLICATION_NAME: Final[str] = "ebull-jobs-singleton-fence"

# #1290: idle-grace window. A holder that's been ``state='idle'`` for
# at least this long is assumed dead — its TCP socket from the old
# python process never sent FIN, so the backend stays around until
# kernel keepalive (default ~2h). Five minutes is short enough that
# the operator doesn't twiddle their thumbs after ``kill -9``, long
# enough that a genuinely starting concurrent boot whose first query
# happens to be slow does not get terminated.
#
# CRITICAL: this only safely identifies a stale holder because the
# live jobs process runs a fence-heartbeat thread that touches the
# fence connection every ``SINGLETON_FENCE_HEARTBEAT_PERIOD_SECONDS``.
# A live process's fence connection therefore advances ``state_change``
# every period and never crosses the grace boundary. A dead process's
# fence connection stops advancing the moment the python process
# dies, and crosses the grace boundary after the configured window.
# DO NOT REMOVE THE HEARTBEAT — Codex 2 pre-push BLOCKING on #1290:
# without it, the reaper terminates the live singleton fence.
SINGLETON_STALE_HOLDER_GRACE_SECONDS: Final[int] = 300

# #1290: fence-heartbeat period. Must be < grace_seconds so a live
# fence never stays idle long enough to qualify. 30s chosen for the
# same reasons as the other supervised heartbeat loops: short enough
# that the grace can be 5-10× without operator-perceptible delay,
# long enough that the connection is at rest >99% of the time.
SINGLETON_FENCE_HEARTBEAT_PERIOD_SECONDS: Final[float] = 30.0


def _fence_heartbeat_loop(
    fence: psycopg.Connection[Any],
    fence_lock: threading.Lock,
    stop_event: threading.Event,
    *,
    period_seconds: float = SINGLETON_FENCE_HEARTBEAT_PERIOD_SECONDS,
) -> None:
    """Periodically touch the singleton-fence connection to advance
    ``pg_stat_activity.state_change``.

    Without this thread, the fence backend goes idle immediately after
    boot's ``fence.commit()`` and STAYS idle. Five minutes later it
    looks indistinguishable from a stale post-kill -9 holder, so a
    parallel jobs-process boot would happily reap the live fence and
    acquire the lock — two jobs processes running concurrently.

    The heartbeat itself is a trivial ``SELECT 1`` issued under the
    fence lock so it does not race with the boot-time acquire or the
    shutdown close. A failure here logs + breaks the loop without
    raising — the next reaper probe will see the still-stale fence
    and decide based on its own criteria. The main process keeps
    running with a working lock until ordinary shutdown closes the
    fence.
    """
    while not stop_event.wait(timeout=period_seconds):
        try:
            with fence_lock:
                if fence.closed:
                    return
                # Fence opens with autocommit=True so this trivial
                # SELECT transitions active → idle WITHOUT a
                # transaction-bracketed phase. The reaper's
                # ``state='idle'`` filter then catches a truly dead
                # fence; a healthy fence's state_change keeps advancing.
                fence.execute("SELECT 1").fetchone()
        except Exception:
            logger.exception(
                "jobs entrypoint: fence heartbeat failed; the live-fence "
                "liveness signal is now stale. The lock is still held "
                "but a concurrent jobs boot >grace_seconds from now "
                "could reap this fence."
            )
            return


def _reap_stale_singleton_fence_holder(
    database_url: str,
    *,
    lock_key: int,
    grace_seconds: int = SINGLETON_STALE_HOLDER_GRACE_SECONDS,
) -> int | None:
    """Probe for a stale holder of the singleton advisory lock and
    terminate it. Returns the reaped PID, or ``None`` if no eligible
    holder exists.

    Eligibility criteria — ALL must hold:

      * The PID owns an advisory lock with ``locktype='advisory'``,
        ``objsubid=1`` (session-scope), matching ``classid``/``objid``
        halves of ``lock_key`` in the CURRENT database.
      * ``pg_stat_activity.application_name = SINGLETON_FENCE_APPLICATION_NAME``
        — only stale holders from a prior eBull jobs process are
        candidates. Third-party connections that happen to have
        acquired this exact 64-bit advisory key are excluded.
      * ``pg_stat_activity.state = 'idle'`` AND
        ``state_change < NOW() - INTERVAL '{grace}'`` — actively-busy
        holders (state='active') or recently-changed-state holders
        (potentially still booting) are excluded.

    The probe + terminate happen on a dedicated short-lived
    connection so the implicit autocommit semantics are explicit.

    Notes:
      * ``pg_terminate_backend`` requires superuser OR the
        ``pg_signal_backend`` role membership. In the eBull dev/prod
        setup the app user has been granted this role (sql/153);
        without it the call returns ``false`` and the reaper logs +
        bails. Either way, we never raise.
      * Idempotent: the reaper is a no-op when no eligible PID
        exists. Caller retries the lock acquire ONCE after a
        successful reap — see :func:`_acquire_singleton_fence`.
    """
    classid = (lock_key >> 32) & 0xFFFF_FFFF
    objid = lock_key & 0xFFFF_FFFF
    try:
        with psycopg.connect(database_url, autocommit=True) as probe:
            row = probe.execute(
                """
                SELECT a.pid
                  FROM pg_locks l
                  JOIN pg_stat_activity a ON a.pid = l.pid
                  JOIN pg_database d      ON d.oid = l.database
                 WHERE l.locktype = 'advisory'
                   AND l.objsubid = 1
                   AND d.datname = current_database()
                   AND l.classid = %(classid)s::oid
                   AND l.objid   = %(objid)s::oid
                   AND a.application_name = %(app_name)s
                   AND a.state    = 'idle'
                   AND a.state_change < NOW() - make_interval(secs => %(grace)s)
                 LIMIT 1
                """,
                {
                    "classid": classid,
                    "objid": objid,
                    "app_name": SINGLETON_FENCE_APPLICATION_NAME,
                    "grace": grace_seconds,
                },
            ).fetchone()
            if row is None:
                return None
            stale_pid = int(row[0])
            term_row = probe.execute(
                "SELECT pg_terminate_backend(%(pid)s)",
                {"pid": stale_pid},
            ).fetchone()
            if not (term_row and term_row[0]):
                logger.warning(
                    "jobs entrypoint: pg_terminate_backend(%d) returned false "
                    "— singleton-lock holder remains. Reap requires superuser "
                    "or pg_signal_backend membership.",
                    stale_pid,
                )
                return None
            logger.info(
                "jobs entrypoint: reaped stale singleton-fence holder (pid=%d, idle ≥ %ds, application_name=%r)",
                stale_pid,
                grace_seconds,
                SINGLETON_FENCE_APPLICATION_NAME,
            )
            return stale_pid
    except Exception:
        logger.exception("jobs entrypoint: stale-holder reap probe failed; treating as no-op")
        return None


def _acquire_singleton_fence(database_url: str) -> psycopg.Connection[Any]:
    """Acquire the session-scoped advisory lock on a dedicated connection.

    Returned connection is held for the lifetime of the process. Caller
    closes it LAST during shutdown so Postgres releases the lock only
    after every other subsystem has stopped.

    Exits the process with code 2 when the lock is already held AND
    no stale holder could be reaped (#1290). On first failure we
    consult :func:`_reap_stale_singleton_fence_holder` — if a prior
    jobs-process backend is idle-in-postgres past the grace window,
    terminate it and retry the lock acquire ONE more time. A
    genuine concurrent boot (busy backend) is preserved.

    The fence connection is opened with ``autocommit=True`` (Codex 2
    round 2 BLOCKING on #1290): without it, ``SELECT
    pg_try_advisory_lock(...)`` runs inside an implicit transaction and
    the backend reports ``state='idle in transaction'`` until
    ``.commit()`` lands. The reaper SQL filters on ``state='idle'``
    only — a dead fence stuck mid-statement would be invisible to it
    and the lock would be unreapable. Autocommit removes the
    in-transaction window entirely.

    The fence connection carries ``application_name='ebull-jobs-singleton-fence'``
    so the reaper has a precise key to identify "our" stale backends.
    """
    fence = psycopg.connect(
        database_url,
        autocommit=True,
        application_name=SINGLETON_FENCE_APPLICATION_NAME,
    )
    try:
        row = fence.execute("SELECT pg_try_advisory_lock(%s)", (JOBS_PROCESS_LOCK_KEY,)).fetchone()
        acquired = bool(row and row[0])
        if not acquired:
            # #1290: try once to reap a stale holder before refusing.
            # The reaper uses its own short-lived connection so a
            # failure there cannot corrupt the fence we still hold
            # half-open here.
            reaped = _reap_stale_singleton_fence_holder(database_url, lock_key=JOBS_PROCESS_LOCK_KEY)
            if reaped is not None:
                # Codex 2 round 3 HIGH on #1290: ``pg_terminate_backend``
                # is ASYNCHRONOUS — it queues a SIGTERM for the target
                # backend, returning ``true`` once the signal is sent
                # but BEFORE the backend has actually exited. The
                # backend's session-scope advisory lock is released
                # only on exit. An immediate retry of
                # ``pg_try_advisory_lock`` will still see the lock
                # held until the OS scheduler runs the doomed backend
                # one last time. Poll-retry for up to two seconds at
                # 100 ms intervals — well above the kernel's
                # SIGTERM-deliver-to-backend-exit latency in practice
                # but bounded so a non-cooperative backend cannot
                # delay startup indefinitely.
                for _ in range(20):
                    row = fence.execute("SELECT pg_try_advisory_lock(%s)", (JOBS_PROCESS_LOCK_KEY,)).fetchone()
                    acquired = bool(row and row[0])
                    if acquired:
                        break
                    time.sleep(0.1)
        if not acquired:
            logger.error(
                "jobs entrypoint: another app.jobs process holds the singleton "
                "advisory lock (key=%d); refusing to start a second instance",
                JOBS_PROCESS_LOCK_KEY,
            )
            fence.close()
            sys.exit(2)
    except SystemExit:
        raise
    except Exception:
        logger.exception("jobs entrypoint: singleton fence acquisition failed")
        try:
            fence.close()
        except Exception:
            pass
        sys.exit(2)
    return fence


def _bootstrap_master_key(pool: Any) -> None:
    """Mirror the API lifespan's master-key bootstrap.

    Pre-fix the jobs process opened the pool, started the scheduler,
    and let the first ``daily_portfolio_sync`` / ``daily_candle_refresh``
    tick raise ``MasterKeyNotLoadedError`` from inside
    ``_load_etoro_credentials`` because no one had ever called
    ``master_key.bootstrap(conn)`` in this process. The API process's
    lifespan at ``app/main.py`` did the bootstrap correctly; the jobs
    entrypoint silently skipped it.

    Must run AFTER ``open_pool`` (we need a connection to verify
    ciphertext) and BEFORE ``runtime.start()`` / boot-drain (so the
    key is installed before the first job fires).

    Never raises on a missing ``EBULL_SECRETS_KEY`` — a clean install
    is a valid post-boot condition and the operator clears it through
    the API setup flow. Raises only on EBULL_SECRETS_KEY mismatch with
    existing ciphertext (fail-loud per ADR-0003 §9).

    Factored out as a module-level helper so the smoke test can drive
    it without acquiring the singleton advisory lock — the live dev
    jobs daemon holds that lock so a parallel ``serve()`` call would
    abort.
    """
    with pool.connection() as conn:
        boot = master_key.bootstrap(conn)
    if boot.broker_encryption_key is not None:
        set_broker_encryption_key(boot.broker_encryption_key)
    logger.info(
        "jobs entrypoint: master-key bootstrap state=%s broker_key_loaded=%s",
        boot.state,
        boot.broker_encryption_key is not None,
    )


def _drain_pending_at_boot(
    *,
    runtime: JobRuntime,
    sync_executor: ThreadPoolExecutor,
    boot_id: str,
    state: ListenerState,
) -> int:
    """Boot-drain step 2 — claim every currently-pending row.

    Runs on the main thread before the listener starts. Same atomic
    claim path the listener uses on its 5s poll fallback, so the
    semantics match: oldest-pending wins, FOR UPDATE SKIP LOCKED keeps
    concurrent runs tidy (the singleton fence prevents the concurrency
    here, but the SQL is shared with the listener for consistency).
    """
    from app.jobs.listener import _route_claim  # local import to avoid cycle at module load

    drained = 0
    while True:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            claim = claim_oldest_pending(conn, boot_id=boot_id)
        if claim is None:
            return drained
        _route_claim(claim, runtime=runtime, sync_executor=sync_executor, state=state)
        drained += 1


def _enforce_pg_locks_with_cleanup(
    fence_conn: psycopg.Connection[Any],
    pool: Any,
) -> None:
    """Run the #1187 PG ``max_locks_per_transaction`` guard with
    fence + pool cleanup on raise.

    The guard runs BEFORE the jobs entrypoint's main try/finally
    block. A raise here must release the singleton-fence advisory
    lock + close the pool so the next jobs-process boot is not
    blocked by stale resources. Extracted from the inline guard for
    unit-testability (`tests/test_pg_settings_call_sites.py`).
    """
    from app.db.pg_settings import enforce_max_locks_floor

    try:
        with psycopg.connect(settings.database_url) as guard_conn:
            enforce_max_locks_floor(guard_conn)
    except BaseException:
        with contextlib.suppress(Exception):
            fence_conn.close()
        with contextlib.suppress(Exception):
            pool.close()
        raise


def _ensure_runtime_config_singleton_with_cleanup(
    fence_conn: psycopg.Connection[Any],
    pool: Any,
) -> None:
    """Run the #1208 runtime_config singleton-vanish guard with fence
    + pool cleanup on raise.

    Mirrors ``_enforce_pg_locks_with_cleanup``: on raise, release the
    singleton-fence advisory lock + close the pool so the next
    jobs-process boot is not blocked by stale resources.

    Pre-condition: ``run_migrations()`` has already applied
    ``sql/015_runtime_config.sql`` (API-first migration contract — jobs
    has not called ``run_migrations()`` since #719). On a totally fresh
    DB the helper's SELECT will fail with ``UndefinedTable``, which is
    the correct fail-loud signal that the operator launched jobs before
    the API ever ran migrations.
    """
    from app.services.runtime_config import ensure_runtime_config_singleton

    try:
        with psycopg.connect(settings.database_url, autocommit=True) as guard_conn:
            ensure_runtime_config_singleton(guard_conn)
    except BaseException:
        with contextlib.suppress(Exception):
            fence_conn.close()
        with contextlib.suppress(Exception):
            pool.close()
        raise


def _ensure_kill_switch_singleton_with_cleanup(
    fence_conn: psycopg.Connection[Any],
    pool: Any,
) -> None:
    """Run the #1232 kill_switch singleton-vanish guard with fence + pool
    cleanup on raise. Mirror of the runtime_config wrapper above; same
    API-first migration pre-condition (jobs reads what API migrated).

    Without this jobs-side mirror, the kill_switch boot-recovery only
    happens on API restart — a jobs-process restart after the seed row
    vanished would leave every scheduled fire rejected with
    ``kill_switch_active`` until the API process happens to restart.
    Codex 2 pre-push review caught this gap.
    """
    from app.services.ops_monitor import ensure_kill_switch_singleton

    try:
        with psycopg.connect(settings.database_url, autocommit=True) as guard_conn:
            ensure_kill_switch_singleton(guard_conn)
    except BaseException:
        with contextlib.suppress(Exception):
            fence_conn.close()
        with contextlib.suppress(Exception):
            pool.close()
        raise


def _ensure_bootstrap_state_singleton_with_cleanup(
    fence_conn: psycopg.Connection[Any],
    pool: Any,
) -> None:
    """Run the #1232 bootstrap_state singleton-vanish guard with fence +
    pool cleanup on raise. Mirror of the runtime_config wrapper above.

    Without this jobs-side mirror, every scheduled-job path that calls
    ``check_bootstrap_state_gate`` would raise ``RuntimeError`` until
    the API restarts and re-seeds the row. Codex 2 pre-push review
    caught this gap.
    """
    from app.services.bootstrap_state import ensure_bootstrap_state_singleton

    try:
        with psycopg.connect(settings.database_url, autocommit=True) as guard_conn:
            ensure_bootstrap_state_singleton(guard_conn)
    except BaseException:
        with contextlib.suppress(Exception):
            fence_conn.close()
        with contextlib.suppress(Exception):
            pool.close()
        raise


def _ensure_budget_config_singleton_with_cleanup(
    fence_conn: psycopg.Connection[Any],
    pool: Any,
) -> None:
    """Run the #1232 follow-up budget_config singleton-vanish guard.

    Without this jobs-side mirror, scheduled-job paths that consume
    ``get_budget_config`` (e.g. execution_guard, portfolio_sync)
    would raise ``BudgetConfigCorrupt`` until API restarts and
    re-seeds. Discovered post-dev-DB-wipe.
    """
    from app.services.budget import ensure_budget_config_singleton

    try:
        with psycopg.connect(settings.database_url, autocommit=True) as guard_conn:
            ensure_budget_config_singleton(guard_conn)
    except BaseException:
        with contextlib.suppress(Exception):
            fence_conn.close()
        with contextlib.suppress(Exception):
            pool.close()
        raise


def _ensure_transaction_cost_config_singleton_with_cleanup(
    fence_conn: psycopg.Connection[Any],
    pool: Any,
) -> None:
    """Run the #1232 follow-up transaction_cost_config singleton-vanish guard.

    Without this jobs-side mirror, every execution_guard cost-check
    raises ``TransactionCostConfigCorrupt`` until API restarts.
    """
    from app.services.transaction_cost import ensure_transaction_cost_config_singleton

    try:
        with psycopg.connect(settings.database_url, autocommit=True) as guard_conn:
            ensure_transaction_cost_config_singleton(guard_conn)
    except BaseException:
        with contextlib.suppress(Exception):
            fence_conn.close()
        with contextlib.suppress(Exception):
            pool.close()
        raise


def _boot_id() -> str:
    """Per-process identifier used as ``claimed_by`` on queue rows.

    pid + ISO start time is unique-enough for boot recovery: a future
    process can identify which rows it (uniquely) owns vs which were
    left behind by a prior boot.
    """
    return f"jobs-{os.getpid()}-{datetime.now(UTC).isoformat()}"


def serve(stop_event: threading.Event | None = None) -> int:
    """Run the jobs process until ``stop_event`` (or signal) fires.

    Returns the process exit code. Refactored to a callable so the
    smoke test in ``tests/smoke/test_jobs_process_boots.py`` can drive
    it under a controlled stop event without forking a subprocess.
    """
    if stop_event is None:
        stop_event = threading.Event()
        _install_signal_handlers(stop_event)

    process_started_at = datetime.now(UTC)
    boot_id = _boot_id()
    logger.info("jobs entrypoint: starting (boot_id=%s)", boot_id)

    pool = open_pool("jobs_pool", min_size=1, max_size=4)
    fence_conn = _acquire_singleton_fence(settings.database_url)
    logger.info("jobs entrypoint: singleton fence acquired")
    # #1290: fence-liveness infrastructure. The lock + stop-event are
    # constructed UP-FRONT so the cleanup paths in the startup-guard
    # wrappers can reference them — but the heartbeat THREAD itself
    # starts AFTER every guard has passed. Codex 2 round 2 HIGH on
    # #1290: if the heartbeat is already running while a guard raises
    # and calls ``fence_conn.close()`` directly, the heartbeat's next
    # ``execute()`` races against close on the same psycopg
    # connection. Delaying the thread start until all guards pass
    # avoids that race entirely. The startup window (every guard
    # combined < 1 min on a healthy DB) is well inside the reaper
    # grace, so a concurrent boot cannot incorrectly reap us during
    # this pre-thread window.
    fence_lock = threading.Lock()
    fence_heartbeat_stop = threading.Event()
    fence_heartbeat_thread: threading.Thread | None = None

    # PR1a #1064 — fail-fast at jobs entrypoint if SCHEDULED_JOBS and
    # _BOOTSTRAP_STAGE_SPECS disagree on a job_name's source. Mirrors
    # the FastAPI lifespan guard in app/main.py — surfaces conflicts
    # before any APScheduler fire or queue dispatch can mark a stage
    # 'running' against a misresolved lock.
    from app.jobs.sources import get_job_name_to_source

    get_job_name_to_source()
    logger.info("jobs entrypoint: source registry validated")

    # #1187 — fail-fast if PG ``max_locks_per_transaction`` is below the
    # floor calibrated for eBull's quarterly-partitioned ownership
    # schema. Spec
    # ``docs/superpowers/specs/2026-05-17-pg-max-locks-per-tx-guard.md``.
    # The guard runs BEFORE the main try/finally block below, so a
    # raise must release the singleton fence + pool manually — else
    # the next jobs-process boot is blocked by a stale advisory lock.
    # The cleanup-on-raise pattern is extracted to
    # ``_enforce_pg_locks_with_cleanup`` so the cleanup invariant is
    # unit-testable; without that extraction, an in-place try/except
    # could regress to leak the fence without any test catching it.
    _enforce_pg_locks_with_cleanup(fence_conn, pool)
    logger.info("jobs entrypoint: max_locks_per_transaction guard passed")

    # #1208 Sub 6 — defensive re-seed of runtime_config singleton in case
    # it vanished after the API applied migrations. API-first contract
    # (jobs does not call run_migrations since #719); helper raises
    # ``UndefinedTable`` if migrations have not run, which is the
    # correct fail-loud signal.
    _ensure_runtime_config_singleton_with_cleanup(fence_conn, pool)
    logger.info("jobs entrypoint: runtime_config singleton guard passed")

    # #1232 — same singleton-vanish posture for kill_switch +
    # bootstrap_state. Without these mirrors, the jobs process is stuck
    # in the pre-#1232 failure mode (every fire rejected with
    # kill_switch_active OR every bootstrap-state-gated path raising
    # RuntimeError) until the API restarts. Codex 2 pre-push review on
    # PR #1232 caught the gap.
    _ensure_kill_switch_singleton_with_cleanup(fence_conn, pool)
    logger.info("jobs entrypoint: kill_switch singleton guard passed")

    _ensure_bootstrap_state_singleton_with_cleanup(fence_conn, pool)
    logger.info("jobs entrypoint: bootstrap_state singleton guard passed")

    # #1232 follow-up — same singleton-vanish posture for budget_config
    # and transaction_cost_config. Without these mirrors, execution-
    # guard / portfolio-sync paths would raise (Budget|TransactionCost)
    # ConfigCorrupt until API restarts. Discovered post-dev-DB-wipe.
    _ensure_budget_config_singleton_with_cleanup(fence_conn, pool)
    logger.info("jobs entrypoint: budget_config singleton guard passed")

    _ensure_transaction_cost_config_singleton_with_cleanup(fence_conn, pool)
    logger.info("jobs entrypoint: transaction_cost_config singleton guard passed")

    _bootstrap_master_key(pool)

    sync_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="jobs-sync")

    listener_state = ListenerState()
    listener_stop = threading.Event()
    runtime = JobRuntime()
    heartbeat = HeartbeatWriter(
        settings.database_url,
        pid=os.getpid(),
        process_started_at=process_started_at,
    )
    heartbeat_threads: list[threading.Thread] = []

    # Pre-declare the credential-health listener handles so the
    # finally: clean-up block can reference them even if construction
    # raises during startup.
    credential_health_stop: threading.Event | None = None
    credential_health_thread: threading.Thread | None = None

    try:
        # #1290: start the fence heartbeat INSIDE the main try block
        # so the finally-shutdown path stops it on every exit (Codex 2
        # round 3 MEDIUM). Every startup guard above has passed, so
        # the heartbeat thread can run safely; if anything in the
        # try-body raises (queue boot-drain, runtime.start(), etc.),
        # the finally clause sets fence_heartbeat_stop + joins the
        # thread under fence_lock before closing the fence.
        fence_heartbeat_thread = threading.Thread(
            target=_fence_heartbeat_loop,
            args=(fence_conn, fence_lock, fence_heartbeat_stop),
            name="jobs-fence-heartbeat",
            daemon=True,
        )
        fence_heartbeat_thread.start()
        logger.info("jobs entrypoint: fence heartbeat started")
        # Step 4 — reaper.
        try:
            reaped = reap_orphaned_syncs(reap_all=True)
            if reaped:
                logger.info("jobs entrypoint: reaper transitioned %d sync_runs row(s)", reaped)
        except Exception:
            logger.exception("jobs entrypoint: reaper failed; continuing")

        # Step 4b — bootstrap recovery (#994 + #1296).
        #
        # If a previous jobs process crashed mid-bootstrap,
        # ``bootstrap_state.status='running'`` is now stale — no live
        # thread is executing the run.
        #
        # Pre-#1296: this step terminated the run to ``partial_error``
        # so the operator could retry-failed from the admin panel.
        # Operator-friendly for genuine bugs, painful for transient
        # crashes (OOM, kill -9, segfault) where the desired behaviour
        # is to resume where the dead process left off.
        #
        # Post-#1296: first try :func:`attempt_boot_resume` —
        #   * ``resumed`` → counter bumped + ``bootstrap_orchestrator``
        #     queue row enqueued. The orchestrator's PR-6
        #     ``reap_orphaned_running_stages`` resets stuck ``running``
        #     stages to ``pending`` so the dispatcher picks them up.
        # SKIP the terminate reaper — the run is recoverable.
        #   * ``terminated_max_attempts`` → resume cap exhausted (i.e.
        #     the prior boot already auto-resumed and the process
        #     crashed AGAIN). Fall through to the terminate reaper so
        #     the operator can intervene rather than entering an
        #     infinite resume loop.
        #   * ``no_in_flight_run`` → no-op.
        try:
            from app.services.bootstrap_state import (
                attempt_boot_resume,
                reap_orphaned_running,
            )

            with psycopg.connect(settings.database_url, autocommit=True) as conn:
                decision = attempt_boot_resume(conn, requested_by=boot_id)
            if decision.decision == "resumed":
                logger.info(
                    "jobs entrypoint: bootstrap auto-resume enqueued "
                    "(run_id=%d, attempt=%d/%d)",
                    decision.run_id or -1,
                    decision.attempts,
                    1,  # _MAX_BOOT_RESUMES default; keep in sync if widened
                )
            else:
                if decision.decision == "terminated_max_attempts":
                    logger.warning(
                        "jobs entrypoint: bootstrap auto-resume cap reached "
                        "(run_id=%d, attempts=%d) — falling through to terminate reaper",
                        decision.run_id or -1,
                        decision.attempts,
                    )
                with psycopg.connect(settings.database_url, autocommit=True) as conn:
                    if reap_orphaned_running(conn):
                        logger.info(
                            "jobs entrypoint: bootstrap reaper transitioned a stuck running run to partial_error"
                        )
        except Exception:
            logger.exception("jobs entrypoint: bootstrap recovery failed; continuing")

        # Step 5 — process_stop boot-recovery (#1065). Sweep abandoned
        # cooperative-cancel signals (>6h, never observed) and stuck
        # full-wash fence rows (dispatched >6h). Frees partial-unique
        # slots for future operator triggers.
        try:
            from app.services.process_stop import boot_recovery_sweep

            with psycopg.connect(settings.database_url) as conn:
                orphaned, observed_unfinished, stuck = boot_recovery_sweep(conn)
            if orphaned or observed_unfinished or stuck:
                logger.info(
                    "jobs entrypoint: process_stop swept %d orphaned / %d observed-unfinished / %d stuck fence row(s)",
                    orphaned,
                    observed_unfinished,
                    stuck,
                )
        except Exception:
            logger.exception("jobs entrypoint: process_stop boot-recovery failed; continuing")

        # Step 6 — queue stale-row recovery.
        try:
            with psycopg.connect(settings.database_url, autocommit=True) as conn:
                reset_count = reset_stale_in_flight(conn, current_boot_id=boot_id)
            if reset_count:
                logger.info(
                    "jobs entrypoint: reset %d stale claimed/dispatched queue row(s) from prior boot",
                    reset_count,
                )
        except Exception:
            logger.exception("jobs entrypoint: queue stale-row recovery failed; continuing")

        # Step 7 — boot-drain pending rows BEFORE scheduler.start().
        try:
            drained = _drain_pending_at_boot(
                runtime=runtime,
                sync_executor=sync_executor,
                boot_id=boot_id,
                state=listener_state,
            )
            if drained:
                logger.info("jobs entrypoint: boot-drained %d pending queue row(s)", drained)
        except Exception:
            logger.exception("jobs entrypoint: boot-drain raised; continuing")

        # Steps 8-9 — scheduler.start() (which calls _catch_up).
        try:
            runtime.start()
        except Exception:
            logger.exception("jobs entrypoint: runtime.start() raised; continuing without scheduler")

        # Step 10 — boot freshness sweep.
        run_boot_freshness_sweep()

        # Credential-health listener (#976 / #974/B). Process-local
        # cache populated by initial full-scan + LISTEN/NOTIFY +
        # 5s poll fallback. The orchestrator pre-flight gate (#977)
        # reads this cache to skip credential-using layers when
        # operator health != VALID.
        credential_health_cache = CredentialHealthCache()
        credential_health_stop = threading.Event()
        credential_health_thread = threading.Thread(
            target=credential_health_listener_loop,
            kwargs={
                "cache": credential_health_cache,
                "pool": pool,
                "stop_event": credential_health_stop,
            },
            name="jobs-credential-health-listener",
            daemon=True,
        )
        credential_health_thread.start()

        # Step 12 — heartbeat threads (one per supervised subsystem).
        for subsystem in ("scheduler", "manual_listener", "queue_drainer", "main"):
            t = threading.Thread(
                target=heartbeat_loop,
                args=(heartbeat, subsystem, stop_event),
                kwargs={"tick_seconds": 10.0},
                name=f"jobs-heartbeat-{subsystem}",
                daemon=True,
            )
            t.start()
            heartbeat_threads.append(t)

        # Step 11+13 — listener via the supervisor (so a stalled
        # listener gets restarted automatically).
        def _listener_thread_factory() -> threading.Thread:
            return threading.Thread(
                target=listener_loop,
                kwargs={
                    "runtime": runtime,
                    "sync_executor": sync_executor,
                    "stop_event": listener_stop,
                    "boot_id": boot_id,
                    "state": listener_state,
                },
                name="jobs-listener",
                daemon=True,
            )

        def _on_main_tick() -> None:
            heartbeat.beat(
                "main",
                notes={"listener_restarts": listener_state.restart_count},
            )

        last_listener = supervise(
            listener_state=listener_state,
            listener_stop=listener_stop,
            listener_thread_factory=_listener_thread_factory,
            main_stop=stop_event,
            on_main_tick=_on_main_tick,
        )
        if last_listener is not None and last_listener.is_alive():
            last_listener.join(timeout=10.0)

    finally:
        # Reverse-order shutdown.
        listener_stop.set()
        if credential_health_stop is not None:
            try:
                credential_health_stop.set()
                if credential_health_thread is not None:
                    credential_health_thread.join(timeout=5.0)
            except Exception:
                logger.exception("jobs entrypoint: credential_health listener stop raised")
        try:
            runtime.shutdown()
        except Exception:
            logger.exception("jobs entrypoint: runtime.shutdown raised")
        try:
            sync_executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            logger.exception("jobs entrypoint: sync_executor.shutdown raised")
        for t in heartbeat_threads:
            t.join(timeout=2.0)
        # #1290: stop the fence heartbeat BEFORE closing the fence,
        # under the fence_lock, so the heartbeat does not race with
        # close (psycopg's cursor is not thread-safe). The thread may
        # be None if a startup guard raised before we reached the
        # thread.start() call — in that case there is nothing to stop.
        try:
            fence_heartbeat_stop.set()
            if fence_heartbeat_thread is not None:
                fence_heartbeat_thread.join(timeout=2.0)
        except Exception:
            logger.exception("jobs entrypoint: fence heartbeat shutdown raised")
        try:
            pool.close()
        except Exception:
            logger.exception("jobs entrypoint: pool.close raised")
        try:
            with fence_lock:
                fence_conn.close()  # LAST — releases the singleton lock.
            logger.info("jobs entrypoint: singleton fence released")
        except Exception:
            logger.exception("jobs entrypoint: fence_conn.close raised")

    return 0


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    sys.exit(serve())


if __name__ == "__main__":
    main()
