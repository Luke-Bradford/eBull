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

import logging
import os
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

import psycopg

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


def _acquire_singleton_fence(database_url: str) -> psycopg.Connection[Any]:
    """Acquire the session-scoped advisory lock on a dedicated connection.

    Returned connection is held for the lifetime of the process. Caller
    closes it LAST during shutdown so Postgres releases the lock only
    after every other subsystem has stopped.

    Exits the process with code 2 when the lock is already held.
    """
    fence = psycopg.connect(database_url)
    try:
        row = fence.execute("SELECT pg_try_advisory_lock(%s)", (JOBS_PROCESS_LOCK_KEY,)).fetchone()
        acquired = bool(row and row[0])
        if not acquired:
            logger.error(
                "jobs entrypoint: another app.jobs process holds the singleton "
                "advisory lock (key=%d); refusing to start a second instance",
                JOBS_PROCESS_LOCK_KEY,
            )
            fence.close()
            sys.exit(2)
        fence.commit()  # release the implicit transaction; lock is session-scoped
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
        # Step 4 — reaper.
        try:
            reaped = reap_orphaned_syncs(reap_all=True)
            if reaped:
                logger.info("jobs entrypoint: reaper transitioned %d sync_runs row(s)", reaped)
        except Exception:
            logger.exception("jobs entrypoint: reaper failed; continuing")

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
        try:
            pool.close()
        except Exception:
            logger.exception("jobs entrypoint: pool.close raised")
        try:
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
