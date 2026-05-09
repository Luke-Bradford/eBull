"""LISTEN/NOTIFY dispatcher for the jobs process (#719).

Owns one dedicated `psycopg.Connection` running ``LISTEN ebull_job_request``
on its own thread plus a 5-second poll fallback so a NOTIFY dropped during
reconnection still surfaces within 5s.

Dispatch routing by ``request_kind``:

- ``manual_job`` → ``runtime.submit_manual(job_name)`` on the JobRuntime's
  manual ThreadPoolExecutor.
- ``sync`` → ``run_sync(scope, trigger, linked_request_id)`` submitted to
  a dedicated max-workers=1 sync ThreadPoolExecutor owned by the
  entrypoint (the orchestrator's partial unique index already serialises
  starts).

Status transitions:

- The listener atomically claims a row (UPDATE...RETURNING) and submits
  to the executor. The ``status='dispatched'`` transition happens
  INSIDE the executor task, AFTER the linked job_runs / sync_runs row
  is opened — that's the contract the boot-drain recovery clause
  relies on.
- An unknown job name, malformed payload, or executor refused-submit
  is logged at WARNING and the row marked ``rejected`` with an
  ``error_msg`` so the operator sees the failure on
  ``GET /jobs/requests``.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import psycopg
import psycopg.sql

from app.config import settings
from app.jobs.runtime import VALID_JOB_NAMES, JobRuntime
from app.services.sync_orchestrator.dispatcher import (
    NOTIFY_CHANNEL,
    claim_oldest_pending,
    claim_request_by_id,
    mark_request_completed,
    mark_request_rejected,
    scope_from_json,
)
from app.services.sync_orchestrator.executor import run_sync
from app.services.sync_orchestrator.types import SyncTrigger
from app.workers.scheduler import SCHEDULED_JOBS

logger = logging.getLogger(__name__)


# Poll interval for the safety-net oldest-pending claim. Independent of
# the NOTIFY-driven path so a dropped notify (network blip, reconnect
# window) surfaces within this interval.
POLL_INTERVAL_S: float = 5.0


# Notify-blocking timeout. The LISTEN loop polls notifies in this
# window then falls through to the poll-fallback claim. Short enough
# to keep the loop responsive to stop_event; long enough that the
# notify path stays the dominant trigger for low-latency dispatch.
NOTIFY_BLOCK_TIMEOUT_S: float = 1.0


class ListenerState:
    """Mutable state the supervisor inspects to detect a stalled listener.

    ``last_progress_at`` advances on every loop iteration where the
    listener observed progress: a notify, a successful claim, a poll
    that returned no rows, OR a clean stop check. A listener wedged on
    its psycopg socket would freeze ``last_progress_at`` until the
    supervisor restarts it.
    """

    def __init__(self) -> None:
        self.last_progress_at: float = time.monotonic()
        self.notifies_seen: int = 0
        self.claims_dispatched: int = 0
        self.claims_rejected: int = 0
        self.restart_count: int = 0


def _dispatch_manual_job(
    *,
    runtime: JobRuntime,
    request_id: int,
    job_name: str | None,
    mode: str | None = None,
) -> None:
    """Inner-loop dispatch for ``request_kind='manual_job'``.

    Validation: an unknown job_name is rejected directly (mark_request_rejected
    on its own conn) without touching the runtime — the API also pre-validates,
    but the listener must be defensive against direct INSERTs into the queue.

    ``mode`` (#1071) carries the ``pending_job_requests.mode`` column the
    claim helper returned. Passed through to the runtime so the prelude
    can bypass the fence check when this worker IS the full-wash fence
    holder (``mode='full_wash'``); otherwise the worker would self-skip
    because its own queue row matches the fence query.
    """
    if job_name is None or job_name not in VALID_JOB_NAMES:
        logger.warning(
            "listener: rejecting manual_job request_id=%d with unknown job_name=%r",
            request_id,
            job_name,
        )
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            mark_request_rejected(
                conn,
                request_id,
                error_msg=f"unknown job name: {job_name!r}",
            )
        return

    # PR1b #1064 — extend per-job prerequisite check to the manual-queue
    # path. The scheduled-fire path in app/jobs/runtime.py::_wrap_invoker
    # has always honoured ScheduledJob.prerequisite (e.g.
    # _bootstrap_complete on every SEC + fundamentals job). Pre-PR1b the
    # manual-queue path bypassed this — operators could fire any SEC
    # ingest from the admin panel during a half-installed dev DB,
    # getting confusing "instruments=0" log lines + empty results.
    #
    # Bootstrap-internal jobs (bootstrap_orchestrator + its stage jobs)
    # are NOT in SCHEDULED_JOBS, so the lookup returns no prerequisite
    # and the manual queue path proceeds unchanged. Operator-tunable
    # SEC/fundamentals jobs WITH the prerequisite get the same
    # "first-install bootstrap not complete" rejection scheduled fires
    # already produce. Manual queue uses mark_request_rejected (NOT
    # mark_request_completed — PREVENTION-grade per data-engineer skill
    # §6.5.7 step 8).
    job = next((j for j in SCHEDULED_JOBS if j.name == job_name), None)
    if job is not None and job.prerequisite is not None:
        # Two-stage try/except (review-bot PR1b BLOCKING fix):
        #   1. Inner try wraps ONLY the prereq evaluation (connect +
        #      callable). Failures fail-open (mirrors scheduled-fire
        #      posture at app/jobs/runtime.py::_wrap_invoker).
        #   2. The rejection write (mark_request_rejected + return)
        #      lives OUTSIDE the inner try — if it raises, the failure
        #      escapes to _route_claim's broad except, which is correct
        #      (rejecting a rejected job is the safe direction). It
        #      MUST NOT fall through to submit_manual_with_request.
        prereq_decision: tuple[bool, str] | None = None
        try:
            with psycopg.connect(settings.database_url, autocommit=True) as conn:
                prereq_decision = job.prerequisite(conn)
        except Exception:
            # Connection-open OR prerequisite-callable raised. Mirrors
            # scheduled-fire posture — silently dropping a real run is
            # worse than running against a partial DB where the body
            # itself can detect + skip. Logged loud so ops_monitor
            # surfaces a repeating failure pattern.
            logger.warning(
                "listener: prerequisite check for %r failed (connect or callable raised); running anyway",
                job_name,
                exc_info=True,
            )

        if prereq_decision is not None:
            met, reason = prereq_decision
            if not met:
                # Rejection write lives OUTSIDE the fail-open try so a
                # transient mark_request_rejected failure cannot fall
                # through to submit_manual_with_request — a job whose
                # prerequisite explicitly returned (False, reason) MUST
                # NOT dispatch even if the rejection write fails.
                logger.info(
                    "listener: rejecting manual_job request_id=%d for %r — prerequisite not met: %s",
                    request_id,
                    job_name,
                    reason,
                )
                with psycopg.connect(settings.database_url, autocommit=True) as conn:
                    mark_request_rejected(
                        conn,
                        request_id,
                        error_msg=reason,
                    )
                return

    # Submit to the runtime's manual executor. The runtime's own
    # wrapper handles the linked_request_id / dispatched / completed
    # transitions inside the executor task.
    runtime.submit_manual_with_request(job_name, request_id=request_id, mode=mode)


def _dispatch_sync_request(
    *,
    sync_executor: ThreadPoolExecutor,
    request_id: int,
    payload: Any,
) -> None:
    """Inner-loop dispatch for ``request_kind='sync'``.

    Submits ``run_sync(scope, trigger, linked_request_id=request_id)``
    to the sync executor. The wrapper inside the submission handles
    opening the sync_runs row, marking the request dispatched, and
    marking it completed when the run finishes.
    """
    if not isinstance(payload, dict):
        logger.warning(
            "listener: rejecting sync request_id=%d with non-dict payload",
            request_id,
        )
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            mark_request_rejected(
                conn,
                request_id,
                error_msg="payload missing or not an object",
            )
        return

    scope_json = payload.get("scope")
    trigger_value = payload.get("trigger", "manual")
    if not isinstance(scope_json, dict):
        logger.warning(
            "listener: rejecting sync request_id=%d — payload.scope not a dict",
            request_id,
        )
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            mark_request_rejected(
                conn,
                request_id,
                error_msg="payload.scope must be an object",
            )
        return

    try:
        scope = scope_from_json(scope_json)
    except ValueError as exc:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            mark_request_rejected(conn, request_id, error_msg=str(exc))
        return

    trigger: SyncTrigger
    if trigger_value in ("manual", "scheduled", "catch_up", "boot_sweep"):
        trigger = trigger_value
    else:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            mark_request_rejected(
                conn,
                request_id,
                error_msg=f"invalid trigger value: {trigger_value!r}",
            )
        return

    sync_executor.submit(_run_sync_with_request_lifecycle, scope, trigger, request_id)


def _run_sync_with_request_lifecycle(
    scope: Any,
    trigger: SyncTrigger,
    request_id: int,
) -> None:
    """Executor task wrapping run_sync with queue lifecycle transitions.

    Order is critical: the dispatched transition happens AFTER run_sync
    returns its sync_run_id (which means _start_sync_run created the
    sync_runs row with linked_request_id populated). On run_sync
    success the request transitions to ``completed``; on raise it
    transitions to ``rejected`` so the operator sees a terminal state
    rather than a row stuck at ``claimed``.
    """
    # PR #719 review BLOCKING/WARNING: don't write `dispatched` here.
    # ``run_sync`` opens the `sync_runs` row internally with
    # ``linked_request_id`` populated; by the time it returns, the
    # row is terminal. A ``dispatched`` transition between those
    # two states is unobservable (same connection, back-to-back
    # UPDATEs) and pollutes the operator-visible state machine. Go
    # straight from ``claimed`` to ``completed``.
    try:
        run_sync(scope, trigger=trigger, linked_request_id=request_id)
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            mark_request_completed(conn, request_id)
    except Exception as exc:
        logger.exception("sync request_id=%d raised", request_id)
        try:
            with psycopg.connect(settings.database_url, autocommit=True) as conn:
                mark_request_rejected(conn, request_id, error_msg=f"{type(exc).__name__}: {exc}")
        except Exception:
            logger.exception(
                "failed to mark request_id=%d rejected; row left in claimed state for boot-drain",
                request_id,
            )


def listener_loop(
    *,
    runtime: JobRuntime,
    sync_executor: ThreadPoolExecutor,
    stop_event: threading.Event,
    boot_id: str,
    state: ListenerState,
    listen_conn_factory: Callable[[], psycopg.Connection[Any]] | None = None,
) -> None:
    """Run the LISTEN + 5s poll dispatch loop until ``stop_event`` is set.

    ``listen_conn_factory`` is overridable for tests — the default
    opens a fresh psycopg.Connection against settings.database_url.
    Production reuses the supervisor's reconnect path on listener
    death; tests inject a fake connection that yields scripted
    notifies + then signals stop.
    """
    factory = listen_conn_factory or _default_listen_conn_factory
    last_poll_at = 0.0
    while not stop_event.is_set():
        try:
            conn = factory()
        except Exception:
            logger.exception("listener: failed to open LISTEN connection")
            if stop_event.wait(timeout=POLL_INTERVAL_S):
                return
            continue

        try:
            with conn.cursor() as cur:
                # ``LISTEN <channel>`` requires an SQL identifier, not
                # a parameterised value — psycopg's `Identifier` is the
                # canonical safe quoter. PR #719 review WARNING.
                cur.execute(psycopg.sql.SQL("LISTEN {}").format(psycopg.sql.Identifier(NOTIFY_CHANNEL)))
            logger.info("listener: LISTEN %s active (boot_id=%s)", NOTIFY_CHANNEL, boot_id)

            while not stop_event.is_set():
                state.last_progress_at = time.monotonic()

                # NOTIFY-driven path. ``notifies()`` blocks for at most
                # NOTIFY_BLOCK_TIMEOUT_S then returns whatever has
                # arrived. Drain everything we got before falling
                # through to the poll fallback.
                for notify in conn.notifies(timeout=NOTIFY_BLOCK_TIMEOUT_S, stop_after=64):
                    state.notifies_seen += 1
                    _dispatch_notify(notify, runtime=runtime, sync_executor=sync_executor, boot_id=boot_id, state=state)

                # Poll fallback. Runs once per POLL_INTERVAL_S so
                # dropped notifies surface within that window.
                now = time.monotonic()
                if now - last_poll_at >= POLL_INTERVAL_S:
                    last_poll_at = now
                    _drain_oldest_pending(runtime=runtime, sync_executor=sync_executor, boot_id=boot_id, state=state)
        except Exception:
            logger.exception("listener: loop crashed; supervisor will restart")
        finally:
            try:
                conn.close()
            except Exception:
                logger.debug("listener: ignoring close-time exception", exc_info=True)
        # Brief backoff before reconnect. Supervisor monitors
        # state.last_progress_at and will hard-restart this thread if
        # the inner loop spins on transient failures.
        if stop_event.wait(timeout=1.0):
            return


def _default_listen_conn_factory() -> psycopg.Connection[Any]:
    """Open a fresh autocommit connection for LISTEN.

    autocommit is required for LISTEN to take effect immediately; the
    same connection is held for the life of the inner loop and closed
    on exit so a Postgres-side reset releases all queued notifies.
    """
    return psycopg.connect(settings.database_url, autocommit=True)


def _dispatch_notify(
    notify: Any,
    *,
    runtime: JobRuntime,
    sync_executor: ThreadPoolExecutor,
    boot_id: str,
    state: ListenerState,
) -> None:
    """Parse a notify payload and try to claim + dispatch the row.

    Malformed payloads (non-numeric request_id) are dropped with a
    WARNING — the row, if it exists, will eventually surface via the
    poll fallback.
    """
    try:
        request_id = int(str(notify.payload).strip())
    except TypeError, ValueError:
        logger.warning("listener: ignoring notify with non-numeric payload: %r", notify.payload)
        return

    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        claim = claim_request_by_id(conn, request_id, boot_id=boot_id)

    if claim is None:
        # Already claimed (boot-drain or poll path beat us). Not a fault.
        logger.debug("listener: request_id=%d already claimed; skipping", request_id)
        return

    _route_claim(claim, runtime=runtime, sync_executor=sync_executor, state=state)


def _drain_oldest_pending(
    *,
    runtime: JobRuntime,
    sync_executor: ThreadPoolExecutor,
    boot_id: str,
    state: ListenerState,
) -> None:
    """Poll-fallback: claim the oldest pending row and dispatch it.

    Called once per POLL_INTERVAL_S regardless of notify activity. Loops
    until the claim returns None so a backlog of dropped notifies
    drains in one tick rather than at one-per-tick.
    """
    while True:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            claim = claim_oldest_pending(conn, boot_id=boot_id)
        if claim is None:
            return
        _route_claim(claim, runtime=runtime, sync_executor=sync_executor, state=state)


def _route_claim(
    claim: dict[str, Any],
    *,
    runtime: JobRuntime,
    sync_executor: ThreadPoolExecutor,
    state: ListenerState,
) -> None:
    """Dispatch a claimed row by request_kind."""
    request_id = int(claim["request_id"])
    kind = claim["request_kind"]
    if kind == "manual_job":
        try:
            _dispatch_manual_job(
                runtime=runtime,
                request_id=request_id,
                job_name=claim["job_name"],
                mode=claim.get("mode"),
            )
            state.claims_dispatched += 1
        except Exception as exc:
            logger.exception("listener: manual_job dispatch raised for request_id=%d", request_id)
            with psycopg.connect(settings.database_url, autocommit=True) as conn:
                mark_request_rejected(conn, request_id, error_msg=f"{type(exc).__name__}: {exc}")
            state.claims_rejected += 1
    elif kind == "sync":
        try:
            _dispatch_sync_request(sync_executor=sync_executor, request_id=request_id, payload=claim["payload"])
            state.claims_dispatched += 1
        except Exception as exc:
            logger.exception("listener: sync dispatch raised for request_id=%d", request_id)
            with psycopg.connect(settings.database_url, autocommit=True) as conn:
                mark_request_rejected(conn, request_id, error_msg=f"{type(exc).__name__}: {exc}")
            state.claims_rejected += 1
    else:
        logger.warning("listener: unknown request_kind=%r for request_id=%d", kind, request_id)
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            mark_request_rejected(conn, request_id, error_msg=f"unknown request_kind: {kind!r}")
        state.claims_rejected += 1
