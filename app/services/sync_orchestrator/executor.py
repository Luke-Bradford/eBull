"""Sync orchestrator executor.

Implements the exact pseudocode from spec §2.2:

- _start_sync_run: synchronous planning + gate (partial unique index)
- _run_layers_loop: topological walk with composite emit handling,
  contract validation, dependency skip, PREREQ_SKIP resolution
- _safe_run_and_finalize: crash-guarded wrapper used by BOTH entry
  points (run_sync sync; submit_sync async) so any uncaught exception
  still finalizes the sync_runs row and releases the gate
- audit writers: _record_layer_* open fresh autocommit connections per
  write so a layer rollback can never erase its own progress row
- _finalize_sync_run: authoritative counts read from sync_layer_progress
- set_executor / run_sync / submit_sync: public entry points (spec §2.1)

Every DB-writing function uses autocommit=True + with conn.transaction()
so the block issues a real BEGIN/COMMIT (psycopg3 quirk — without
autocommit, with conn.transaction() becomes a SAVEPOINT).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Protocol

import psycopg

from app.config import settings
from app.services.sync_orchestrator.planner import build_execution_plan
from app.services.sync_orchestrator.registry import JOB_TO_LAYERS
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
# Executor registration (set at app startup in main.py)
# ---------------------------------------------------------------------------


class _ExecutorLike(Protocol):
    def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any: ...


_executor_ref: _ExecutorLike | None = None


def set_executor(executor: _ExecutorLike) -> None:
    """Register the worker pool used by submit_sync().

    Called once at app startup with job_runtime._manual_executor. In
    tests, pass a ThreadPoolExecutor (or a synchronous-inline stub).
    """
    global _executor_ref
    _executor_ref = executor


# ---------------------------------------------------------------------------
# Public entry points (spec §2.1)
# ---------------------------------------------------------------------------


def run_sync(scope: SyncScope, trigger: SyncTrigger) -> SyncResult:
    """Synchronous entry: plan + execute + finalize in caller's thread."""
    sync_run_id, plan = _start_sync_run(scope, trigger)
    outcomes = _safe_run_and_finalize(sync_run_id, plan)
    return SyncResult(sync_run_id=sync_run_id, outcomes=outcomes)


def submit_sync(scope: SyncScope, trigger: SyncTrigger) -> tuple[int, ExecutionPlan]:
    """Async entry: plan + submit to worker; return before layers run."""
    if _executor_ref is None:
        raise RuntimeError(
            "sync orchestrator executor not set — app startup must call set_executor(job_runtime._manual_executor)"
        )
    sync_run_id, plan = _start_sync_run(scope, trigger)
    _executor_ref.submit(_safe_run_and_finalize, sync_run_id, plan)
    return sync_run_id, plan


# ---------------------------------------------------------------------------
# _start_sync_run — gate + planning + pending rows
# ---------------------------------------------------------------------------


def _start_sync_run(
    scope: SyncScope,
    trigger: SyncTrigger,
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
                sync_run_id = _insert_sync_run(conn, scope, trigger, plan)
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
) -> int:
    row = conn.execute(
        """
        INSERT INTO sync_runs (scope, scope_detail, trigger, layers_planned)
        VALUES (%s, %s, %s, %s)
        RETURNING sync_run_id
        """,
        (scope.kind, scope.detail, trigger, len(plan.layers_to_refresh)),
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
            contract_exc = RuntimeError(
                f"refresh contract violation: expected {set(layer_plan.emits)}, got {returned_names}"
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


def _record_layer_failed(
    sync_run_id: int,
    layer_name: str,
    error: BaseException,
) -> None:
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.transaction():
            conn.execute(
                """
                UPDATE sync_layer_progress
                SET status = 'failed',
                    finished_at = now(),
                    error_category = %s
                WHERE sync_run_id = %s AND layer_name = %s
                """,
                (_categorize_error(error), sync_run_id, layer_name),
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
    """Mark any 'pending' or 'running' sync_layer_progress rows for the
    given sync as 'failed' with error_category='orchestrator_crash'.
    Returns {layer_name: FAILED} for rows actually updated."""
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.transaction():
            rows = conn.execute(
                """
                UPDATE sync_layer_progress
                SET status = 'failed',
                    finished_at = now(),
                    error_category = 'orchestrator_crash'
                WHERE sync_run_id = %s
                  AND status IN ('pending', 'running')
                RETURNING layer_name
                """,
                (sync_run_id,),
            ).fetchall()
    return {r[0]: LayerOutcome.FAILED for r in rows}


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
                    COUNT(*) FILTER (WHERE status IN ('complete', 'partial')) AS done,
                    COUNT(*) FILTER (WHERE status = 'failed')                 AS failed,
                    COUNT(*) FILTER (WHERE status = 'skipped')                AS skipped
                FROM sync_layer_progress
                WHERE sync_run_id = %s
                """,
                (sync_run_id,),
            ).fetchone()
            assert counts_row is not None, "COUNT(*) aggregate returned no row"
            done, failed, skipped = counts_row

            if failed == 0:
                status = "complete"
            elif done == 0:
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


# ---------------------------------------------------------------------------
# Error categorization (sanitized, stable set per spec §3.4)
# ---------------------------------------------------------------------------


def _categorize_error(exc: BaseException) -> str:
    """Map an exception to a stable sanitized category.

    Full detail stays in logs (exc_info). The DB column holds only a
    coarse category string — never SQL fragments or driver internals.
    """
    name = type(exc).__name__.lower()
    if isinstance(exc, psycopg.errors.IntegrityError):
        return "db_constraint"
    if isinstance(exc, psycopg.OperationalError):
        return "db_connection"
    if "auth" in name or "credential" in name or "token" in name:
        return "provider_auth"
    if "rate" in name or "throttle" in name:
        return "provider_rate_limit"
    if "timeout" in name or "unavailable" in name:
        return "provider_unavailable"
    if "validation" in name or "value" in name:
        return "validation"
    return "unknown"
