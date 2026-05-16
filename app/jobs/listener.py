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
from app.services.processes.bootstrap_gate import check_bootstrap_state_gate
from app.services.processes.param_metadata import (
    ParamValidationError,
    validate_job_params,
)
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
    payload: Any = None,
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

    # PR1b-2 #1064 — extract canonical envelope from durable payload.
    # ``payload`` is the JSONB blob from ``pending_job_requests.payload``;
    # ``None`` covers legacy queue rows written before PR1b-2 (and the
    # synthetic "no body" path that publishes payload=NULL). Keys missing
    # from the envelope default to empty dicts so downstream code can
    # consume them uniformly. A structurally malformed envelope
    # (params/control present but not a JSON object) is rejected with
    # the contract message — silent coercion would mask the violation
    # for direct queue inserts.
    try:
        raw_params, control = _extract_envelope(payload)
    except _MalformedEnvelopeError as exc:
        logger.info(
            "listener: rejecting manual_job request_id=%d for %r — malformed envelope: %s",
            request_id,
            job_name,
            exc,
        )
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            mark_request_rejected(conn, request_id, error_msg=f"malformed payload: {exc}")
        return

    # Validate the durable payload's params dict against the registry.
    # The API path (run_job) already validated when the queue row was
    # written, but the listener must be defensive: a direct INSERT into
    # the queue (operator script, batch tool) bypasses the API. Reject
    # such rows BEFORE invoker dispatch so the operator sees the
    # contract violation in the queue row's error_msg.
    try:
        validated_params = validate_job_params(
            job_name,
            raw_params,
            allow_internal_keys=False,
        )
    except ParamValidationError as exc:
        logger.info(
            "listener: rejecting manual_job request_id=%d for %r — params invalid: %s",
            request_id,
            job_name,
            exc,
        )
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            mark_request_rejected(conn, request_id, error_msg=f"invalid params: {exc}")
        return

    # ``_extract_envelope`` already enforced strict-bool typing; we can
    # safely read the value as-is without a coercion that would treat
    # truthy strings as override.
    override_present = control.get("override_bootstrap_gate", False) is True

    # #1181 — single registry lookup at the top of dispatch. ``job``
    # is consumed by both the universal-gate short-circuit (below)
    # and the per-job prereq check (further down). The pre-#1181
    # listener did two scans — `job_in_registry = any(...)` here,
    # then `job = next(...)` later — which made it easy to miss the
    # exemption guard on the gate path.
    job = next((j for j in SCHEDULED_JOBS if j.name == job_name), None)

    # PR1b-2 #1064 — bootstrap_state gate at manual-queue dispatch.
    # Mirrors the scheduled-fire path in app/jobs/runtime.py::_wrap_invoker.
    # Skipped when:
    #   - ``job is None``: bootstrap-internal jobs (orchestrator + its
    #     stage jobs) are NOT in SCHEDULED_JOBS — the orchestrator
    #     MUST be able to run while bootstrap_state.status='running'
    #     or it would deadlock itself.
    #   - ``job.exempt_from_universal_bootstrap_gate``: #1181 carve-
    #     out for safety-net jobs (currently Layer 2 daily-index
    #     reconcile). See spec
    #     docs/superpowers/specs/2026-05-16-lane-b-discovery-firing.md
    #     §4.2. The override flag is meaningless for exempt jobs —
    #     the carve-out is an "unaudited design bypass", distinct
    #     from the manual-queue override which writes
    #     ``decision_audit`` for non-exempt jobs.
    if job is not None and not job.exempt_from_universal_bootstrap_gate:
        try:
            with psycopg.connect(settings.database_url, autocommit=True) as conn:
                allowed, reason = check_bootstrap_state_gate(
                    conn,
                    job_name=job_name,
                    invocation_path="manual_queue",
                    override_present=override_present,
                )
        except Exception:
            # Fail-open mirrors the prereq check below — a transient
            # bootstrap_state read failure should not silently drop a
            # real run; the body-side guards still apply.
            logger.warning(
                "listener: bootstrap_state gate for %r failed; running anyway",
                job_name,
                exc_info=True,
            )
            allowed, reason = True, ""

        if not allowed:
            logger.info(
                "listener: rejecting manual_job request_id=%d for %r — %s",
                request_id,
                job_name,
                reason,
            )
            with psycopg.connect(settings.database_url, autocommit=True) as conn:
                # PREVENTION-grade: data-engineer skill §6.5.7 step 8
                # mandates mark_request_rejected (NOT _completed) for
                # any prelude-skip path. A 'completed' row would falsely
                # claim the job ran.
                mark_request_rejected(conn, request_id, error_msg=reason)
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
    runtime.submit_manual_with_request(
        job_name,
        request_id=request_id,
        mode=mode,
        params=validated_params,
    )


class _MalformedEnvelopeError(ValueError):
    """``pending_job_requests.payload`` is structurally invalid.

    Raised by ``_extract_envelope`` when the payload has the canonical
    envelope shape but ``body.params`` or ``body.control`` is not a
    JSON object. The listener maps this to ``mark_request_rejected``
    with the message body so the operator sees the contract violation
    on ``GET /jobs/requests``. Codex pre-push round 1 WARNING.
    """


# Codex pre-push round 2 WARNING: control allow-list at the listener.
# Mirrors ``_ALLOWED_CONTROL_KEYS`` in app/api/jobs.py so a direct
# queue insert with a typo'd flag (``override_bootstrap_gates``) is
# rejected at dispatch instead of silently no-opping the operator's
# intent.
_LISTENER_ALLOWED_CONTROL_KEYS: frozenset[str] = frozenset({"override_bootstrap_gate"})


def _extract_envelope(payload: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    """Decode the durable payload into ``(params, control)``.

    PR1b-2 (#1064): ``pending_job_requests.payload`` carries the
    canonical envelope ``{"params": {...}, "control": {...}}``. This
    helper tolerates three input shapes so the listener stays robust
    against direct queue inserts and pre-PR1b-2 rows:

    1. ``None`` — legacy row or empty body. Returns ``({}, {})``.
    2. Canonical envelope dict — extract ``params`` and ``control``,
       defaulting either to ``{}`` when absent. Raises
       ``_MalformedEnvelopeError`` when the inner ``params`` /
       ``control`` exists but is not a JSON object, when an unknown
       control key is set, or when ``override_bootstrap_gate`` is not
       a boolean — silent coercion would mask a real contract violation.
    3. Flat dict (legacy ergonomic shape) — entire payload becomes
       ``params`` with empty ``control``. Mirrors the API
       ``_normalise_envelope`` helper so a directly-inserted row goes
       through the same disambiguation rule.

    Top-level non-dict payloads (lists, strings, numbers) raise
    ``_MalformedEnvelopeError`` — the API rejects them at the boundary,
    and a direct queue insert with the same shape is just as broken.
    Codex pre-push round 2 WARNING.
    """
    if payload is None:
        return ({}, {})
    if not isinstance(payload, dict):
        raise _MalformedEnvelopeError(f"payload must be a JSON object (got {type(payload).__name__})")

    has_envelope_keys = "params" in payload or "control" in payload
    if not has_envelope_keys:
        return (dict(payload), {})

    params = payload.get("params", {})
    control = payload.get("control", {})
    if params is None:
        params = {}
    if control is None:
        control = {}
    if not isinstance(params, dict):
        raise _MalformedEnvelopeError(f"payload.params must be a JSON object (got {type(params).__name__})")
    if not isinstance(control, dict):
        raise _MalformedEnvelopeError(f"payload.control must be a JSON object (got {type(control).__name__})")

    unknown = set(control) - _LISTENER_ALLOWED_CONTROL_KEYS
    if unknown:
        raise _MalformedEnvelopeError(f"unknown control key(s): {sorted(unknown)}")
    # Strict-bool check (Codex pre-push round 2 BLOCKING): truthy strings
    # like ``"false"`` would otherwise grant the override via ``bool(...)``.
    raw_override = control.get("override_bootstrap_gate", False)
    if raw_override is not False and raw_override is not True:
        raise _MalformedEnvelopeError("control.override_bootstrap_gate must be a boolean")
    return (dict(params), dict(control))


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
                payload=claim.get("payload"),
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
