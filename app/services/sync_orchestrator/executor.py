"""Sync orchestrator executor.

Implements the exact pseudocode from spec §2.2:

- _start_sync_run: synchronous planning + gate (partial unique index)
- _run_layers_loop: topological walk with composite emit handling,
  contract validation, dependency skip, PREREQ_SKIP resolution
- _safe_run_and_finalize: crash-guarded wrapper so any uncaught
  exception still finalizes the sync_runs row and releases the gate
- audit writers: _record_layer_* open fresh autocommit connections per
  write so a layer rollback can never erase its own progress row
- _finalize_sync_run: authoritative counts read from sync_layer_progress
- run_sync: synchronous public entry point (spec §2.1). The pre-#719
  in-process ``submit_sync`` / ``set_executor`` are deleted; the API
  publishes via ``dispatcher.publish_sync_request`` and the
  jobs-process listener invokes ``run_sync`` on its own executor.

Every DB-writing function uses autocommit=True + with conn.transaction()
so the block issues a real BEGIN/COMMIT (psycopg3 quirk — without
autocommit, with conn.transaction() becomes a SAVEPOINT).
"""

from __future__ import annotations

import hashlib
import logging
import re
import traceback
from collections.abc import Mapping
from typing import Any

import psycopg
import psycopg.sql

from app.config import settings
from app.services.credential_health import (
    CredentialHealth,
    get_operator_credential_health,
)
from app.services.operators import (
    AmbiguousOperatorError,
    NoOperatorError,
    sole_operator_id,
)
from app.services.process_stop import (
    acquire_prelude_lock,
    is_stop_requested,
    mark_completed,
    mark_observed,
)
from app.services.sync_orchestrator.exception_classifier import classify_exception
from app.services.sync_orchestrator.planner import build_execution_plan
from app.services.sync_orchestrator.registry import INIT_CHECKS, JOB_TO_LAYERS
from app.services.sync_orchestrator.types import (
    PREREQ_SKIP_MARKER,
    ExecutionPlan,
    LayerOutcome,
    LayerPlan,
    OrchestratorFenceHeld,
    RefreshResult,
    SyncAlreadyRunning,
    SyncCancelled,
    SyncResult,
    SyncScope,
    SyncTrigger,
)

# Process-id that anchors the orchestrator's advisory lock + fence row
# on ``pending_job_requests``. Both the full-sync and high-frequency-sync
# scheduled jobs target the same scheduler state (sync_runs single-running
# unique index), so they share the lock key.
_ORCHESTRATOR_PROCESS_ID = "orchestrator_full_sync"
_ORCHESTRATOR_FENCE_PROCESS_IDS: tuple[str, ...] = (
    "orchestrator_full_sync",
    "orchestrator_high_frequency_sync",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public entry points (spec §2.1)
# ---------------------------------------------------------------------------
#
# `set_executor` and `submit_sync` were deleted in #719. The API never
# executes the orchestrator in-process; it publishes a request via
# `dispatcher.publish_sync_request` and the jobs-process listener
# claims + invokes `run_sync` on its own dedicated executor. `run_sync`
# stays as the canonical worker entry — used by the listener, by the
# scheduled-fire wrappers in `app/workers/scheduler.py`, and by tests
# / CLI scripts.


def run_sync(
    scope: SyncScope,
    trigger: SyncTrigger,
    *,
    linked_request_id: int | None = None,
    request_mode: str | None = None,
) -> SyncResult:
    """Synchronous entry: plan + execute + finalize in caller's thread.

    ``linked_request_id`` (#719) is the queue request that triggered
    this run when the caller is the jobs-process dispatcher; the
    column is mirrored onto ``sync_runs.linked_request_id`` so
    boot-recovery's NOT EXISTS clause can suppress replay of completed
    work. Scheduled fires and tests pass ``None``.

    ``request_mode`` (#1078 — admin control hub PR6) is the listener-
    decoded ``pending_job_requests.mode`` value (``'iterate'`` /
    ``'full_wash'`` / ``None``). When ``request_mode == 'full_wash'``
    the run IS the fence holder, so ``_start_sync_run`` bypasses the
    fence check (otherwise the worker would self-skip on its own queue
    row). Defence-in-depth: ``linked_request_id`` is also excluded from
    the fence query so a future path that forgets to set
    ``request_mode`` still does the right thing.
    """
    bypass_fence_check = request_mode == "full_wash"
    sync_run_id, plan = _start_sync_run(
        scope,
        trigger,
        linked_request_id=linked_request_id,
        bypass_fence_check=bypass_fence_check,
    )
    outcomes = _safe_run_and_finalize(sync_run_id, plan)
    return SyncResult(sync_run_id=sync_run_id, outcomes=outcomes)


# ---------------------------------------------------------------------------
# _start_sync_run — gate + planning + pending rows
# ---------------------------------------------------------------------------


def _start_sync_run(
    scope: SyncScope,
    trigger: SyncTrigger,
    *,
    linked_request_id: int | None = None,
    bypass_fence_check: bool = False,
) -> tuple[int, ExecutionPlan]:
    """Plan the sync, INSERT sync_runs + pending sync_layer_progress rows.

    UniqueViolation on idx_sync_runs_single_running → SyncAlreadyRunning
    carrying the active sync_run_id for the HTTP 409 body.

    Issue #1078 (umbrella #1064) — admin control hub PR6 wires the
    advisory-lock + fence-check prelude here per spec §"sync_runs
    analogue". One transaction:

      1. ``acquire_prelude_lock(conn, 'orchestrator_full_sync')`` —
         same key as the full-wash trigger so any concurrent path
         (full-wash trigger, scheduled fire, listener-dispatched
         iterate) serialises against the fence read.
      2. Fence check — refuse if a full-wash row is held on
         ``pending_job_requests`` for any orchestrator process_id.
         ``bypass_fence_check=True`` skips the fence (the run IS the
         fence holder). ``linked_request_id`` is also excluded as
         defence-in-depth so a future path forgetting the bypass flag
         still does the right thing.
      3. ``build_execution_plan(conn, scope)`` — moved INSIDE the lock
         so the plan reads source rows under the lock. Otherwise a
         concurrent full-wash COMMITting after the planner read but
         before our INSERT could leave us planning against a stale
         scheduler state (Codex pre-impl review H1).
      4. INSERT sync_runs + pending sync_layer_progress rows.
    """
    # autocommit=False so ``with conn.transaction():`` opens an explicit
    # top-level BEGIN/COMMIT (psycopg3 quirk: nested transactions become
    # SAVEPOINTs in autocommit-off mode, but at the outer level the block
    # is a real tx). Mirrors ``_run_prelude`` in ``app/jobs/runtime.py``.
    #
    # Codex pre-push review #2 (#1078): ALWAYS run the fence query;
    # ``bypass_fence_check=True`` only excludes the run's OWN
    # ``linked_request_id`` so a sibling orchestrator full-wash queue
    # row still blocks. A naive "skip fence entirely" bypass would let
    # ``orchestrator_high_frequency_sync`` ignore an active
    # ``orchestrator_full_sync`` full-wash and vice versa.
    exclude_request_id = linked_request_id if bypass_fence_check else None
    with psycopg.connect(settings.database_url) as conn:
        try:
            with conn.transaction():
                acquire_prelude_lock(conn, _ORCHESTRATOR_PROCESS_ID)
                holder = _read_orchestrator_fence_holder(conn, exclude_request_id=exclude_request_id)
                if holder is not None:
                    raise OrchestratorFenceHeld(holder)
                plan = build_execution_plan(conn, scope)
                sync_run_id = _insert_sync_run(conn, scope, trigger, plan, linked_request_id)
                _insert_layer_progress_rows(conn, sync_run_id, plan)
            return sync_run_id, plan
        except psycopg.errors.UniqueViolation:
            active = conn.execute("SELECT sync_run_id FROM sync_runs WHERE status='running' LIMIT 1").fetchone()
            raise SyncAlreadyRunning(
                scope,
                active_sync_run_id=active[0] if active else None,
            ) from None


def _read_orchestrator_fence_holder(
    conn: psycopg.Connection[Any],
    *,
    exclude_request_id: int | None,
) -> str | None:
    """Return the holder process_id of an active orchestrator full-wash fence.

    A row in ``pending_job_requests`` for ANY orchestrator process_id with
    ``mode='full_wash' AND status IN ('pending','claimed','dispatched')``
    is the fence. ``exclude_request_id`` is the listener-dispatched
    request_id that this very run is fulfilling (defence-in-depth — the
    bypass_fence_check flag is the primary signal, but excluding the
    own request_id keeps the prelude robust against future paths that
    forget to set the bypass).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT process_id
              FROM pending_job_requests
             WHERE process_id = ANY(%s)
               AND mode       = 'full_wash'
               AND status     IN ('pending', 'claimed', 'dispatched')
               AND (%s::bigint IS NULL OR request_id <> %s::bigint)
             LIMIT 1
            """,
            (
                list(_ORCHESTRATOR_FENCE_PROCESS_IDS),
                exclude_request_id,
                exclude_request_id,
            ),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return str(row[0])


def _insert_sync_run(
    conn: psycopg.Connection[Any],
    scope: SyncScope,
    trigger: SyncTrigger,
    plan: ExecutionPlan,
    linked_request_id: int | None,
) -> int:
    row = conn.execute(
        """
        INSERT INTO sync_runs
            (scope, scope_detail, trigger, layers_planned, linked_request_id)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING sync_run_id
        """,
        (scope.kind, scope.detail, trigger, len(plan.layers_to_refresh), linked_request_id),
    ).fetchone()
    assert row is not None, "INSERT ... RETURNING sync_run_id returned no row"
    return row[0]


def _insert_layer_progress_rows(
    conn: psycopg.Connection[Any],
    sync_run_id: int,
    plan: ExecutionPlan,
) -> None:
    """One pending row per emitted layer (composite plans insert N rows)."""
    for lp in plan.layers_to_refresh:
        for emit in lp.emits:
            conn.execute(
                """
                INSERT INTO sync_layer_progress
                    (sync_run_id, layer_name, status, items_total)
                VALUES (%s, %s, 'pending', %s)
                """,
                (sync_run_id, emit, lp.estimated_items or None),
            )


# ---------------------------------------------------------------------------
# _safe_run_and_finalize — crash-guarded wrapper
# ---------------------------------------------------------------------------


def _safe_run_and_finalize(
    sync_run_id: int,
    plan: ExecutionPlan,
) -> dict[str, LayerOutcome]:
    """Crash-guarded layer loop + finalize. Used by BOTH entry points.

    Shared-outcomes contract: the outcomes dict is passed to
    _run_layers_loop by reference. A mid-loop crash leaves committed
    outcomes intact; the exception handler only fills missing entries
    with FAILED. The finally block then recomputes counts from
    sync_layer_progress (authoritative) and finalizes sync_runs.

    Cancel branch (#1078): when ``_run_layers_loop`` raises
    ``SyncCancelled`` the cancel checkpoint already wrote
    ``sync_runs.status='cancelled'`` and called ``mark_completed`` on
    the stop request. Route to ``_finalize_cancelled_sync_run`` which
    marks unfinished ``sync_layer_progress`` rows as ``cancelled``
    (skip_reason="cancelled by operator", NOT the crash text used by
    ``_fail_unfinished_layers``) and updates the layers_* counts on
    sync_runs WITHOUT touching ``status``.
    """
    outcomes: dict[str, LayerOutcome] = {}
    cancelled = False
    try:
        _run_layers_loop(sync_run_id, plan, outcomes)
    except SyncCancelled:
        logger.info("sync run %s cancelled by operator", sync_run_id)
        cancelled = True
        for lp in plan.layers_to_refresh:
            for name in lp.emits:
                outcomes.setdefault(name, LayerOutcome.DEP_SKIPPED)
    except Exception:
        logger.exception("sync run %s crashed in loop", sync_run_id)
        for lp in plan.layers_to_refresh:
            for name in lp.emits:
                outcomes.setdefault(name, LayerOutcome.FAILED)
    finally:
        if cancelled:
            try:
                _finalize_cancelled_sync_run(sync_run_id)
            except Exception:
                logger.exception(
                    "sync run %s cancel finalize failed — relying on boot reaper",
                    sync_run_id,
                )
        else:
            try:
                for name, outcome in _fail_unfinished_layers(sync_run_id).items():
                    outcomes.setdefault(name, outcome)
            except Exception:
                logger.exception(
                    "sync run %s: failed to mark unfinished layer rows as failed",
                    sync_run_id,
                )
            try:
                _finalize_sync_run(sync_run_id, outcomes)
            except SyncCancelled:
                # Late cancel observed at finalize (Codex round 2). The
                # in-tx probe set ``sync_runs.status='cancelled'`` and
                # committed; route to the cancel-branch finalizer to
                # terminalise unfinished layer rows + refresh counts.
                logger.info(
                    "sync run %s: late cancel observed at finalize",
                    sync_run_id,
                )
                try:
                    _finalize_cancelled_sync_run(sync_run_id)
                except Exception:
                    logger.exception(
                        "sync run %s late-cancel finalize failed — relying on boot reaper",
                        sync_run_id,
                    )
            except Exception:
                logger.exception(
                    "sync run %s finalize failed — relying on boot reaper",
                    sync_run_id,
                )
    return outcomes


# ---------------------------------------------------------------------------
# _run_layers_loop — topological walk
# ---------------------------------------------------------------------------


def _run_layers_loop(
    sync_run_id: int,
    plan: ExecutionPlan,
    outcomes: dict[str, LayerOutcome],
) -> None:
    """Walk layers in topological order, mutating `outcomes` in place.

    Keys are emitted layer names (not LayerPlan.name). Each layer:
    1. Build resolved upstream_outcomes (this-run + last-counting job_runs)
    2. Check blocking deps — skip all emits as DEP_SKIPPED if any failed
    3. Mark emits as 'running'
    4. Invoke adapter; validate returned emit set matches plan.emits
    5. Write per-emit sync_layer_progress result row
    """
    from app.services.sync_orchestrator.registry import LAYERS

    for layer_plan in plan.layers_to_refresh:
        # Cancel checkpoint (#1078 — admin control hub PR6).
        # Spec §"Cancel — cooperative" / §"sync_runs analogue":
        # cancel between layers in the DAG fixed-point loop. Mid-layer
        # cancel is NOT supported; the layer body runs to completion.
        # Worst-case observation latency = duration of the longest
        # in-flight layer. Acceptable v1.
        _check_cancel_signal(sync_run_id)

        upstream_outcomes = _build_upstream_outcomes(layer_plan, outcomes)

        # Pre-flight gate #1 — credential health (#977 / #974/C).
        # Layers tagged ``requires_broker_credential=True`` PREREQ_SKIP
        # when the operator's aggregate health is anything other than
        # VALID. Stops the cascade where every credential-using layer
        # 401s on each tick before the operator has even fixed their
        # keys.
        cred_skip = _credential_health_blocks(layer_plan)
        if cred_skip is not None:
            for emit in layer_plan.emits:
                _record_layer_skipped(sync_run_id, emit, cred_skip)
                outcomes[emit] = LayerOutcome.PREREQ_SKIP
            continue

        # Pre-flight gate #2 — layer initialization (#977 / #974/C).
        # Layers tagged ``requires_layer_initialized=("dep",)`` skip
        # until ``INIT_CHECKS["dep"]`` returns true. Used by
        # portfolio_sync to wait for ``instruments`` to be non-empty
        # before writing positions (FK constraint).
        init_skip = _layer_initialization_blocks(layer_plan)
        if init_skip is not None:
            for emit in layer_plan.emits:
                _record_layer_skipped(sync_run_id, emit, init_skip)
                outcomes[emit] = LayerOutcome.PREREQ_SKIP
            continue

        blocking_failure = _blocking_dependency_failed(layer_plan, upstream_outcomes)
        if blocking_failure is not None:
            for emit in layer_plan.emits:
                _record_layer_skipped(sync_run_id, emit, blocking_failure)
                outcomes[emit] = LayerOutcome.DEP_SKIPPED
            continue

        for emit in layer_plan.emits:
            _record_layer_started(sync_run_id, emit)

        # Invoke adapter via LAYERS[emits[0]].refresh (all emits of a
        # composite share a single refresh callable per spec §2.3.1).
        refresh = LAYERS[layer_plan.emits[0]].refresh
        try:
            results = refresh(
                sync_run_id=sync_run_id,
                progress=_make_progress_callback(sync_run_id, layer_plan.emits),
                upstream_outcomes=upstream_outcomes,
            )
        except Exception as exc:
            logger.exception("layer %s failed", layer_plan.name)
            for emit in layer_plan.emits:
                _record_layer_failed(sync_run_id, emit, error=exc)
                outcomes[emit] = LayerOutcome.FAILED
            continue

        # Contract guard: adapter must return exactly the planned emits.
        returned_names = [name for name, _ in results]
        if sorted(returned_names) != sorted(layer_plan.emits) or len(set(returned_names)) != len(returned_names):
            logger.error(
                "layer %s violated refresh contract: emits=%s returned=%s",
                layer_plan.name,
                layer_plan.emits,
                returned_names,
            )
            # Sort BOTH sequences so the message text is deterministic
            # across worker restarts. `set` repr ordering is
            # hash-seed-dependent (PYTHONHASHSEED varies per process),
            # which would otherwise make the #645 error_fingerprint
            # for the same contract violation hash to a different value
            # on every restart and defeat the repeat-grouping intent.
            contract_exc = RuntimeError(
                f"refresh contract violation: expected {sorted(layer_plan.emits)}, got {sorted(returned_names)}"
            )
            for emit in layer_plan.emits:
                _record_layer_failed(sync_run_id, emit, error=contract_exc)
                outcomes[emit] = LayerOutcome.FAILED
            continue

        for emit, result in results:
            _record_layer_result(sync_run_id, emit, result)
            outcomes[emit] = result.outcome

    # Codex pre-push review #1 (#1078): cancel signal arriving DURING
    # the final layer was previously dropped — the per-iteration
    # checkpoint runs only at the TOP of each layer, so a stop request
    # inserted mid-final-layer left ``process_stop_requests.completed_at``
    # NULL and ``sync_runs.status='complete'`` (or partial). Add a
    # post-loop checkpoint so a late cancel still observes + transitions
    # the run to ``cancelled`` (with all layers terminal — counts stay
    # honest via the cancel-branch finalizer).
    _check_cancel_signal(sync_run_id)


# ---------------------------------------------------------------------------
# Dependency gate + upstream resolution
# ---------------------------------------------------------------------------


def _credential_health_blocks(layer: LayerPlan) -> str | None:
    """Return skip_reason if any emit needs broker creds and aggregate != VALID.

    Reads operator credential health on a fresh autocommit connection
    per gate check. Per-tick DB hit is acceptable because the
    orchestrator runs as discrete sync ticks, not per-request. The
    cache at ``app.services.credential_health_cache`` exists for the
    request-path consumers (admin UI, WS subscriber) where DB latency
    matters; the orchestrator goes direct.

    Environment scoping (review #983 BLOCKING): gates on EVERY
    environment for which the operator has an active credential row.
    v1 only uses 'demo' but the runtime may add 'live' at any time;
    hardcoding 'demo' here would let a layer with invalid 'live'
    credentials pass the gate and trade live with bad keys.

    Returns None when:
      * No emit requires broker credentials.
      * Operator has no active environments (treated as MISSING by
        the underlying aggregate, so the gate blocks — explicit
        return below).
      * All environments aggregate VALID.
      * DB lookup itself failed (don't block on infra error — let
        the layer's adapter surface the real failure if it tries
        to run).
    """
    from app.services.sync_orchestrator.registry import LAYERS

    requires_creds = any(LAYERS[emit].requires_broker_credential for emit in layer.emits)
    if not requires_creds:
        return None

    try:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            try:
                op_id = sole_operator_id(conn)
            except (NoOperatorError, AmbiguousOperatorError) as exc:
                return f"operator not configured ({type(exc).__name__})"

            # Discover the operator's active environments. Any
            # environment with active rows is gated — if any of them
            # is not VALID, block the layer.
            environments = _operator_active_environments(conn, op_id)
            if not environments:
                # Operator has no active credential rows at all.
                return "broker credentials not configured for any environment"

            for env in environments:
                health = get_operator_credential_health(conn, operator_id=op_id, environment=env)
                if health != CredentialHealth.VALID:
                    return f"broker credentials not validated (env={env}, health={health.value})"
    except Exception:
        logger.exception(
            "credential_health gate: DB lookup failed for layer %s; not blocking",
            layer.name,
        )
        return None

    return None


def _operator_active_environments(
    conn: psycopg.Connection[Any],
    operator_id: Any,
) -> list[str]:
    """Return distinct environments the operator has non-revoked rows for.

    Sorted for deterministic gate-skip-reason output. Empty list when
    the operator has no active rows.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT environment
              FROM broker_credentials
             WHERE operator_id = %s
               AND revoked_at IS NULL
             ORDER BY environment
            """,
            (operator_id,),
        )
        return [row[0] for row in cur.fetchall()]


def _layer_initialization_blocks(layer: LayerPlan) -> str | None:
    """Return skip_reason if any required init-dep is not content-initialized.

    INIT_CHECKS is a registry mapping layer name -> SQL EXISTS query.
    Each requires_layer_initialized entry must have an INIT_CHECKS
    mapping or the gate fails closed (logged + skipped).
    """
    from app.services.sync_orchestrator.registry import LAYERS

    init_deps: set[str] = set()
    for emit in layer.emits:
        init_deps.update(LAYERS[emit].requires_layer_initialized)
    if not init_deps:
        return None

    try:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                for dep_name in sorted(init_deps):  # deterministic order
                    init_sql = INIT_CHECKS.get(dep_name)
                    if init_sql is None:
                        # A layer registered as requires_layer_initialized
                        # for a name with no INIT_CHECKS entry is a
                        # registry config error. Fail closed.
                        logger.error(
                            "layer %s requires_layer_initialized=%r but no INIT_CHECKS entry exists",
                            layer.name,
                            dep_name,
                        )
                        return f"init-check missing for dep {dep_name}"
                    cur.execute(psycopg.sql.SQL(init_sql))  # type: ignore[arg-type]
                    row = cur.fetchone()
                    if row is None or not row[0]:
                        return f"layer {dep_name} not yet initialized"
    except Exception:
        logger.exception(
            "init-check gate: DB lookup failed for layer %s; not blocking",
            layer.name,
        )
        return None

    return None


def _blocking_dependency_failed(
    layer: LayerPlan,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> str | None:
    """Return skip_reason if any blocking dep did not complete successfully.

    Resolved `upstream_outcomes` from _build_upstream_outcomes contains
    every dep — planned ones use this-run outcome, unplanned ones use
    last-counting job_runs row. This lets the gate correctly block on an
    unplanned blocking dep whose latest row is PREREQ_SKIP.
    """
    from app.services.sync_orchestrator.registry import LAYERS

    blocking_bad = {LayerOutcome.FAILED, LayerOutcome.DEP_SKIPPED}
    for dep in layer.dependencies:
        dep_outcome = upstream_outcomes[dep]  # always present
        if dep_outcome in blocking_bad and LAYERS[dep].is_blocking:
            return f"blocking dependency {dep} did not complete ({dep_outcome.value})"
        if dep_outcome is LayerOutcome.PREREQ_SKIP and LAYERS[dep].is_blocking:
            return f"blocking dependency {dep} skipped (prerequisite missing)"
    return None


def _build_upstream_outcomes(
    layer_plan: LayerPlan,
    outcomes: dict[str, LayerOutcome],
) -> Mapping[str, LayerOutcome]:
    """Resolve dependency outcomes: in-run map first, else last-counting
    job_runs row. Missing deps (never-run) default to FAILED so they
    cannot pass the blocking gate silently."""
    resolved: dict[str, LayerOutcome] = {}
    for dep in layer_plan.dependencies:
        if dep in outcomes:
            resolved[dep] = outcomes[dep]
        else:
            resolved[dep] = _last_counting_outcome_from_job_runs(dep)
    return resolved


def _last_counting_outcome_from_job_runs(layer_name: str) -> LayerOutcome:
    """Read last counting job_runs row for the job that emits layer_name,
    convert to LayerOutcome. Used when the dep was already fresh and not
    planned in this sync run."""
    # Reverse-lookup: layer_name → legacy job_name.
    job_name = None
    for job, emits in JOB_TO_LAYERS.items():
        if layer_name in emits:
            job_name = job
            break
    if job_name is None:
        return LayerOutcome.FAILED

    # autocommit=True per orchestrator convention — SELECT must not leave
    # an idle implicit transaction open across dependency-resolution
    # calls during the _run_layers_loop walk.
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        row = conn.execute(
            """
            SELECT status, row_count, error_msg
            FROM job_runs
            WHERE job_name = %s
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (job_name,),
        ).fetchone()

    if row is None:
        return LayerOutcome.FAILED
    status, row_count, error_msg = row
    if status == "success":
        return LayerOutcome.SUCCESS if (row_count or 0) else LayerOutcome.NO_WORK
    if status == "skipped" and error_msg is not None and error_msg.startswith(PREREQ_SKIP_MARKER):
        return LayerOutcome.PREREQ_SKIP
    return LayerOutcome.FAILED


# ---------------------------------------------------------------------------
# Audit writers — each opens its own autocommit connection
# ---------------------------------------------------------------------------


def _check_cancel_signal(sync_run_id: int) -> None:
    """Poll ``process_stop_requests`` between layers; raise on observation.

    Issue #1078 (umbrella #1064) — admin control hub PR6.
    Spec §"Cancel — cooperative" / §"sync_runs analogue".

    Operator clicks Cancel on the orchestrator_full_sync row → API writes
    a stop row with ``target_run_kind='sync_run'`` + UPDATEs
    ``sync_runs.cancel_requested_at``. Each layer iteration polls here.

    On observation:
      1. ``mark_observed`` — UI flips chip from ``cancelling`` to
         ``cancelling (observed)``.
      2. UPDATE ``sync_runs.status='cancelled'`` with the
         ``WHERE status='running'`` guard — assert rowcount=1 because
         ``_finalize_sync_run`` runs in ``_safe_run_and_finalize``'s
         finally block AFTER the loop exits, so it cannot race against
         an in-flight layer iteration. rowcount=0 indicates a producer
         bug (Codex pre-impl review M-r2-1).
      3. ``mark_completed`` — release the partial-unique active-stop
         slot for future cancels.
      4. raise ``SyncCancelled``.

    Each call opens its own short-lived autocommit conn — the orchestrator
    convention (see other audit writers) where every poll is its own
    transaction so a long-running adapter doesn't carry a stale read
    snapshot.
    """
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        stop = is_stop_requested(
            conn,
            target_run_kind="sync_run",
            target_run_id=sync_run_id,
        )
        if stop is None:
            return
        # Bot review WARNING (PR #1079): all stop-request writes must
        # COMMIT regardless of the rowcount guard outcome. A bare
        # ``raise RuntimeError`` inside the tx would ROLLBACK
        # ``mark_observed`` and the stop_request row would be stranded
        # ``observed_at IS NULL`` forever (the boot reaper cannot
        # reconcile sync_run-kind stop rows that never observed). Do
        # ``mark_observed`` + ``UPDATE`` + ``mark_completed`` inside
        # the tx, capture the rowcount, exit the tx so writes durably
        # commit, then decide which exception to raise.
        with conn.transaction():
            mark_observed(conn, stop.id)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE sync_runs
                       SET status      = 'cancelled',
                           finished_at = COALESCE(finished_at, now())
                     WHERE sync_run_id = %s
                       AND status      = 'running'
                    """,
                    (sync_run_id,),
                )
                update_rowcount = cur.rowcount
            mark_completed(conn, stop.id)
        logger.info(
            "sync run %s: cancel signal observed (stop_request_id=%d, mode=%s)",
            sync_run_id,
            stop.id,
            stop.mode,
        )
    if update_rowcount != 1:
        # rowcount=0 indicates the sync_runs row was already terminal
        # when the checkpoint ran. By design this is impossible — the
        # cancel checkpoint runs strictly inside the loop and
        # ``_finalize_sync_run`` only fires after the loop exits, AND
        # the partial-unique index on ``process_stop_requests`` prevents
        # two active stop rows for the same target_run_id from racing.
        # Raise so the bug surfaces; the stop_request row was already
        # marked observed+completed above so the active-stop slot is
        # released and a future cancel for a re-run can succeed.
        raise RuntimeError(
            f"cancel checkpoint: expected 1 sync_runs row for sync_run_id={sync_run_id}, got rowcount={update_rowcount}"
        )
    raise SyncCancelled(sync_run_id)


def _finalize_cancelled_sync_run(sync_run_id: int) -> None:
    """Cancel-branch finalizer.

    Issue #1078 (umbrella #1064) — admin control hub PR6.

    The cancel checkpoint already wrote ``sync_runs.status='cancelled'``
    + finished_at, and ``mark_completed`` on the stop request. This
    finalizer:

      1. Marks any unfinished ``sync_layer_progress`` rows
         (``status IN ('pending','running')``) as ``status='cancelled'``
         with ``skip_reason='cancelled by operator'``. This is DIFFERENT
         from ``_fail_unfinished_layers`` which uses crash text — the
         operator-cancel skip_reason makes DAG triage honest.
      2. Recomputes ``layers_done/failed/skipped`` counts from the
         authoritative ``sync_layer_progress`` rollup and UPDATEs
         ``sync_runs`` — but does NOT touch ``status`` (already set).

    The crash-branch ``_finalize_sync_run`` is NEVER called on the
    cancel path; if it were, its ``WHERE status='running'`` guard
    would no-op the count refresh and leave stale layers_* values
    (Codex pre-impl review B1).
    """
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE sync_layer_progress
                       SET status      = 'cancelled',
                           finished_at = now(),
                           skip_reason = 'cancelled by operator'
                     WHERE sync_run_id = %s
                       AND status      IN ('pending', 'running')
                    """,
                    (sync_run_id,),
                )
            counts_row = conn.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status IN ('complete', 'partial'))   AS done,
                    COUNT(*) FILTER (WHERE status = 'failed')                   AS failed,
                    COUNT(*) FILTER (WHERE status IN ('skipped', 'cancelled'))  AS skipped
                  FROM sync_layer_progress
                 WHERE sync_run_id = %s
                """,
                (sync_run_id,),
            ).fetchone()
            assert counts_row is not None, "COUNT(*) aggregate returned no row"
            done, failed, skipped = counts_row
            conn.execute(
                """
                UPDATE sync_runs
                   SET layers_done    = %s,
                       layers_failed  = %s,
                       layers_skipped = %s,
                       finished_at    = COALESCE(finished_at, now())
                 WHERE sync_run_id = %s
                """,
                (done, failed, skipped, sync_run_id),
            )


def _record_layer_started(sync_run_id: int, layer_name: str) -> None:
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.transaction():
            conn.execute(
                """
                UPDATE sync_layer_progress
                SET status = 'running',
                    started_at = now()
                WHERE sync_run_id = %s AND layer_name = %s
                """,
                (sync_run_id, layer_name),
            )


def _record_layer_result(
    sync_run_id: int,
    layer_name: str,
    result: RefreshResult,
) -> None:
    status_map: dict[LayerOutcome, str] = {
        LayerOutcome.SUCCESS: "complete",
        LayerOutcome.NO_WORK: "complete",
        LayerOutcome.PARTIAL: "partial",
        LayerOutcome.PREREQ_SKIP: "skipped",
        LayerOutcome.FAILED: "failed",
        LayerOutcome.DEP_SKIPPED: "skipped",
    }
    status = status_map[result.outcome]
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.transaction():
            conn.execute(
                """
                UPDATE sync_layer_progress
                SET status = %s,
                    finished_at = now(),
                    items_total = %s,
                    items_done = %s,
                    row_count = %s,
                    error_category = %s,
                    skip_reason = %s
                WHERE sync_run_id = %s AND layer_name = %s
                """,
                (
                    status,
                    result.items_total,
                    result.items_processed,
                    result.row_count,
                    result.error_category,
                    result.detail if result.outcome is LayerOutcome.PREREQ_SKIP else None,
                    sync_run_id,
                    layer_name,
                ),
            )


_FORENSICS_MESSAGE_LIMIT = 1000
_FORENSICS_TRACEBACK_LIMIT = 8000

# Strip absolute paths + line numbers from a traceback so the
# fingerprint groups repeats of the same exception class + frame
# regardless of refactor noise. e.g. `File "D:\\Repos\\eBull\\app\\
# services\\foo.py", line 142, in bar` collapses to `File foo.py, in
# bar`. Conservative — keeps file basename + function name (the bits
# that actually identify the failure site).
_FRAME_LINE_PATTERN = re.compile(r'File "(?:.*[\\/])?([^"\\/]+)", line \d+, in (\S+)')


def _build_forensics(error: BaseException) -> tuple[str, str, str]:
    """Return (error_message, error_traceback, error_fingerprint).

    Caller passes the exception. Uses
    ``traceback.format_exception(error)`` (NOT ``format_exc()``) so the
    function works for exceptions constructed but not raised — e.g.
    the contract-guard path in ``_run_layers_loop`` builds a
    ``RuntimeError`` to record without raising it. ``format_exc()``
    only reads the active exception via ``sys.exc_info()``; outside
    an active ``except`` block it returns the literal stub
    ``'NoneType: None\\n'`` which would poison both the traceback
    column and the fingerprint hash.

    Strings are length-capped so a pathological adapter (e.g. an LLM
    client raising a multi-MB error) cannot blow up the row.
    Fingerprint groups repeats of the same exception class + call
    stack so the operator can tell "this is the same failure as the
    prior run" without diffing tracebacks by hand.
    """
    message = repr(error)[:_FORENSICS_MESSAGE_LIMIT]
    tb_text = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    tb = tb_text[:_FORENSICS_TRACEBACK_LIMIT]
    normalised = _FRAME_LINE_PATTERN.sub(r"File \1, in \2", tb)
    fingerprint = hashlib.sha1(normalised.encode("utf-8")).hexdigest()
    return message, tb, fingerprint


def _record_layer_failed(
    sync_run_id: int,
    layer_name: str,
    error: BaseException,
) -> None:
    error_message, error_traceback, error_fingerprint = _build_forensics(error)
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.transaction():
            conn.execute(
                """
                UPDATE sync_layer_progress
                SET status            = 'failed',
                    finished_at       = now(),
                    error_category    = %s,
                    error_message     = %s,
                    error_traceback   = %s,
                    error_fingerprint = %s
                WHERE sync_run_id = %s AND layer_name = %s
                """,
                (
                    classify_exception(error).value,
                    error_message,
                    error_traceback,
                    error_fingerprint,
                    sync_run_id,
                    layer_name,
                ),
            )


def _record_layer_skipped(
    sync_run_id: int,
    layer_name: str,
    reason: str,
) -> None:
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.transaction():
            conn.execute(
                """
                UPDATE sync_layer_progress
                SET status = 'skipped',
                    finished_at = now(),
                    skip_reason = %s
                WHERE sync_run_id = %s AND layer_name = %s
                """,
                (reason, sync_run_id, layer_name),
            )


def _fail_unfinished_layers(sync_run_id: int) -> dict[str, LayerOutcome]:
    """Finalize any unfinished sync_layer_progress rows after a crash.

    Two cases — distinguished by `started_at` to keep the consecutive-
    failure streak in the admin banner truthful (#645):

    - `pending` rows (started_at IS NULL) — the adapter never ran. The
      worker died between row insert and adapter dispatch. Marked
      `'cancelled'` (NOT `'failed'`) so the streak counter does not
      treat reaper noise from dev `--reload` cycles as real adapter
      failures. The legacy behavior here was the dominant source of
      the 140/328 inflated streak counts the operator reported.
    - `running` rows (started_at IS NOT NULL) — the adapter started
      and the worker died mid-flight. Marked `'failed'` with
      `'orchestrator_crash'` because real work was in progress.

    Returns {layer_name: outcome} for rows actually updated.
    """
    cancelled: dict[str, LayerOutcome] = {}
    failed: dict[str, LayerOutcome] = {}
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.transaction():
            cancelled_rows = conn.execute(
                """
                UPDATE sync_layer_progress
                SET status      = 'cancelled',
                    finished_at = now(),
                    skip_reason = 'worker died before adapter dispatched'
                WHERE sync_run_id = %s
                  AND status      = 'pending'
                  AND started_at IS NULL
                RETURNING layer_name
                """,
                (sync_run_id,),
            ).fetchall()
            for r in cancelled_rows:
                cancelled[r[0]] = LayerOutcome.DEP_SKIPPED

            failed_rows = conn.execute(
                """
                UPDATE sync_layer_progress
                SET status         = 'failed',
                    finished_at    = now(),
                    error_category = 'orchestrator_crash'
                WHERE sync_run_id = %s
                  AND status IN ('pending', 'running')
                RETURNING layer_name
                """,
                (sync_run_id,),
            ).fetchall()
            for r in failed_rows:
                failed[r[0]] = LayerOutcome.FAILED
    return {**cancelled, **failed}


def _finalize_sync_run(
    sync_run_id: int,
    outcomes: dict[str, LayerOutcome],
) -> None:
    """Compute terminal sync_runs status from authoritative
    sync_layer_progress rows; log drift vs in-memory outcomes.

    Codex pre-push round 2 (#1078): probe ``process_stop_requests`` for
    an active stop signal under the SAME tx + sync_runs FOR UPDATE
    lock that the cancel API uses. A cancel arriving in the narrow
    window between the post-loop checkpoint and finalize would
    otherwise be overwritten by ``status='complete/partial'`` and
    leave ``process_stop_requests.completed_at IS NULL``. With the
    in-tx probe + lock, the cancel is observed atomically and the
    cancel-branch terminal status is preserved.
    """
    late_cancel_observed = False
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.transaction():
            # Lock the sync_runs row to serialise against the cancel
            # API's ``SELECT ... FOR UPDATE`` — whichever path commits
            # first wins, the second observes the committed state.
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status FROM sync_runs WHERE sync_run_id = %s FOR UPDATE",
                    (sync_run_id,),
                )
                lock_row = cur.fetchone()
            if lock_row is None:
                logger.error("sync run %s: row missing at finalize", sync_run_id)
                return
            current_status = lock_row[0]

            # Active-stop probe under the same tx. If a stop signal
            # arrived after the post-loop checkpoint, mark it
            # observed/completed atomically with the status flip.
            stop = is_stop_requested(
                conn,
                target_run_kind="sync_run",
                target_run_id=sync_run_id,
            )
            if stop is not None and current_status == "running":
                mark_observed(conn, stop.id)
                conn.execute(
                    """
                    UPDATE sync_runs
                       SET status      = 'cancelled',
                           finished_at = COALESCE(finished_at, now())
                     WHERE sync_run_id = %s
                    """,
                    (sync_run_id,),
                )
                mark_completed(conn, stop.id)
                logger.info(
                    "sync run %s: cancel signal observed at finalize (stop_request_id=%d)",
                    sync_run_id,
                    stop.id,
                )
                late_cancel_observed = True
                # Fall through and let the conn.transaction() block
                # exit cleanly so the writes COMMIT. Raise
                # ``SyncCancelled`` AFTER the tx context so
                # ``_safe_run_and_finalize`` can route through
                # ``_finalize_cancelled_sync_run`` for layer
                # terminalisation + count refresh. Raising inside the
                # tx context would trigger ROLLBACK and discard the
                # cancel writes (Codex round 2 follow-up).

            if late_cancel_observed:
                # Skip the normal counts-and-status UPDATE below;
                # the cancel branch already terminalised status.
                pass
            else:
                # COUNT(*) with FILTER always returns exactly one row.
                counts_row = _compute_terminal_counts_and_update(conn, sync_run_id, outcomes)
                _drift_check(sync_run_id, counts_row, outcomes)
    if late_cancel_observed:
        raise SyncCancelled(sync_run_id)
    return


def _compute_terminal_counts_and_update(
    conn: psycopg.Connection[Any],
    sync_run_id: int,
    outcomes: dict[str, LayerOutcome],
) -> tuple[int, int, int, int]:
    """Inner helper: count + status UPDATE for the normal finalize path.

    Split out of ``_finalize_sync_run`` so the cancel-at-finalize race
    short-circuits cleanly. Caller MUST be inside the ``conn.transaction()``
    block. Returns ``(done, failed, skipped, total)`` for the drift-check.
    """
    counts_row = conn.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE status IN ('complete', 'partial'))   AS done,
            COUNT(*) FILTER (WHERE status = 'failed')                   AS failed,
            -- `cancelled` is rolled into the skipped bucket for
            -- parent-status accounting (#645). Layer-row distinction
            -- (skipped = blocked by dep, cancelled = worker died
            -- before adapter dispatched) is preserved on
            -- sync_layer_progress; sync_runs.layers_skipped just
            -- tracks "didn't complete and didn't fail".
            COUNT(*) FILTER (WHERE status IN ('skipped', 'cancelled'))  AS skipped,
            COUNT(*)                                                    AS total
          FROM sync_layer_progress
         WHERE sync_run_id = %s
        """,
        (sync_run_id,),
    ).fetchone()
    assert counts_row is not None, "COUNT(*) aggregate returned no row"
    done, failed, skipped, total = counts_row

    # `failed=0 && done=total` → complete (every layer ran and won).
    # Anything that didn't run AND didn't fail still leaves the parent in an
    # "incomplete success" state — `partial` rather than `complete` so the
    # operator can spot crash-early finalizations from the /sync/runs feed
    # (a sync that died before any adapter dispatched would otherwise
    # report `complete` with zero layers done).
    if failed == 0 and done == total:
        status = "complete"
    elif done == 0 and failed > 0:
        status = "failed"
    else:
        status = "partial"

    error_category = "all_layers_failed" if status == "failed" else None

    # Spec §"Finalizer-preserves-cancelled invariant" (Codex round 5
    # R5-W4). The ``AND status='running'`` guard is belt-and-suspenders;
    # the in-tx FOR UPDATE in the caller already routes cancel-at-finalize
    # through the cancel branch.
    conn.execute(
        """
        UPDATE sync_runs
        SET finished_at    = now(),
            status         = %s,
            layers_done    = %s,
            layers_failed  = %s,
            layers_skipped = %s,
            error_category = %s
        WHERE sync_run_id = %s
          AND status      = 'running'
        """,
        (status, done, failed, skipped, error_category, sync_run_id),
    )
    return int(done), int(failed), int(skipped), int(total)


def _drift_check(
    sync_run_id: int,
    counts: tuple[int, int, int, int],
    outcomes: dict[str, LayerOutcome],
) -> None:
    """Log if in-memory outcomes disagree with the authoritative DB counts."""
    _done, failed, _skipped, _total = counts
    memory_failed = sum(1 for o in outcomes.values() if o is LayerOutcome.FAILED)
    if abs(memory_failed - failed) > 0:
        logger.warning(
            "sync run %s: memory/DB failure-count drift: memory=%d db=%d",
            sync_run_id,
            memory_failed,
            failed,
        )


# ---------------------------------------------------------------------------
# Progress callback
# ---------------------------------------------------------------------------


def _make_progress_callback(sync_run_id: int, emits: tuple[str, ...]):
    """Return a callback that updates items_done for each emit of this
    layer plan. Opens a short-lived autocommit connection per call."""

    def _callback(items_done: int, items_total: int | None = None) -> None:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            with conn.transaction():
                for emit in emits:
                    conn.execute(
                        """
                        UPDATE sync_layer_progress
                        SET items_done  = %s,
                            items_total = COALESCE(%s, items_total)
                        WHERE sync_run_id = %s AND layer_name = %s
                        """,
                        (items_done, items_total, sync_run_id, emit),
                    )

    return _callback
