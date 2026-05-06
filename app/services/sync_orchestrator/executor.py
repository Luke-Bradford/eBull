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
from app.services.sync_orchestrator.exception_classifier import classify_exception
from app.services.sync_orchestrator.planner import build_execution_plan
from app.services.sync_orchestrator.registry import INIT_CHECKS, JOB_TO_LAYERS
from app.services.sync_orchestrator.types import (
    PREREQ_SKIP_MARKER,
    ExecutionPlan,
    LayerOutcome,
    LayerPlan,
    RefreshResult,
    SyncAlreadyRunning,
    SyncResult,
    SyncScope,
    SyncTrigger,
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
) -> SyncResult:
    """Synchronous entry: plan + execute + finalize in caller's thread.

    ``linked_request_id`` (#719) is the queue request that triggered
    this run when the caller is the jobs-process dispatcher; the
    column is mirrored onto ``sync_runs.linked_request_id`` so
    boot-recovery's NOT EXISTS clause can suppress replay of completed
    work. Scheduled fires and tests pass ``None``.
    """
    sync_run_id, plan = _start_sync_run(scope, trigger, linked_request_id=linked_request_id)
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
) -> tuple[int, ExecutionPlan]:
    """Plan the sync, INSERT sync_runs + pending sync_layer_progress rows.

    UniqueViolation on idx_sync_runs_single_running → SyncAlreadyRunning
    carrying the active sync_run_id for the HTTP 409 body.

    Planning runs OUTSIDE the try/except so a UniqueViolation raised from
    anywhere in build_execution_plan (e.g. a future freshness predicate
    using ON CONFLICT DO NOTHING RETURNING) cannot be misidentified as a
    concurrency conflict. Only the two INSERTs are wrapped.
    """
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        plan = build_execution_plan(conn, scope)
        try:
            with conn.transaction():
                sync_run_id = _insert_sync_run(conn, scope, trigger, plan, linked_request_id)
                _insert_layer_progress_rows(conn, sync_run_id, plan)
            return sync_run_id, plan
        except psycopg.errors.UniqueViolation:
            active = conn.execute("SELECT sync_run_id FROM sync_runs WHERE status='running' LIMIT 1").fetchone()
            raise SyncAlreadyRunning(
                scope,
                active_sync_run_id=active[0] if active else None,
            ) from None


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
    """
    outcomes: dict[str, LayerOutcome] = {}
    try:
        _run_layers_loop(sync_run_id, plan, outcomes)
    except Exception:
        logger.exception("sync run %s crashed in loop", sync_run_id)
        for lp in plan.layers_to_refresh:
            for name in lp.emits:
                outcomes.setdefault(name, LayerOutcome.FAILED)
    finally:
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

    Returns None when:
      * No emit requires broker credentials.
      * Aggregate is VALID.
      * DB lookup itself failed (don't block on infra error — let
        the layer's adapter surface the real failure if it tries to
        run).
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

            health = get_operator_credential_health(conn, operator_id=op_id, environment="demo")
    except Exception:
        logger.exception(
            "credential_health gate: DB lookup failed for layer %s; not blocking",
            layer.name,
        )
        return None

    if health == CredentialHealth.VALID:
        return None
    return f"broker credentials not validated (operator health={health.value})"


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
    sync_layer_progress rows; log drift vs in-memory outcomes."""
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.transaction():
            # COUNT(*) with FILTER always returns exactly one row — no None fallback needed.
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
            # Anything that didn't run AND didn't fail still leaves the
            # parent in an "incomplete success" state — `partial` rather
            # than `complete` so the operator can spot crash-early
            # finalizations from the /sync/runs feed (a sync that died
            # before any adapter dispatched would otherwise report
            # `complete` with zero layers done).
            if failed == 0 and done == total:
                status = "complete"
            elif done == 0 and failed > 0:
                status = "failed"
            else:
                status = "partial"

            error_category = "all_layers_failed" if status == "failed" else None

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
                """,
                (status, done, failed, skipped, error_category, sync_run_id),
            )

    # Drift check: in-memory outcomes should roughly agree with DB counts.
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
