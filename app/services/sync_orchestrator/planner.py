"""Sync orchestrator planner.

Builds ExecutionPlan from SyncScope. Applies freshness filtering
(unless force=True on the scope's target job), derives
LayerPlan.dependencies per the external-only rule (spec §2.6), and
topologically sorts layers_to_refresh from roots outward.
"""

from __future__ import annotations

from typing import Any

import psycopg

from app.services.sync_orchestrator.layer_state import compute_layer_states_from_db
from app.services.sync_orchestrator.layer_types import LayerState
from app.services.sync_orchestrator.registry import JOB_TO_LAYERS, LAYERS
from app.services.sync_orchestrator.types import (
    ExecutionPlan,
    LayerPlan,
    LayerSkip,
    SyncScope,
)


def build_execution_plan(
    conn: psycopg.Connection[Any],
    scope: SyncScope,
) -> ExecutionPlan:
    """Build the plan for a sync run per spec §2.6."""
    candidate_jobs = _scope_to_candidate_jobs(scope, conn=conn)
    target_job = scope.detail if scope.kind == "job" else None

    layers_to_refresh: list[LayerPlan] = []
    layers_skipped: list[LayerSkip] = []

    # `behind` scope: candidates were already state-selected
    # (DEGRADED / ACTION_NEEDED + non-HEALTHY upstreams). Skip the
    # legacy is_fresh re-filter so the state machine's selection is
    # authoritative — a DEGRADED layer must fire even if is_fresh
    # says otherwise. Keys on `kind` alone (not compound with
    # `scope.force`) so this cannot accidentally bypass freshness for
    # an unrelated `job` scope whose `force=True` target happens to
    # coincide with a behind candidate.
    bypass_freshness_for_all = scope.kind == "behind"

    for job_name in candidate_jobs:
        emits = JOB_TO_LAYERS[job_name]
        if not emits:  # outside-DAG job — should not be in candidates
            continue

        is_target = job_name == target_job
        if (is_target and scope.force) or bypass_freshness_for_all:
            include = True
            reason = f"forced by scope={scope.kind}"
        else:
            fresh, reason = _all_emits_fresh(conn, emits)
            include = not fresh

        if include:
            layers_to_refresh.append(_build_layer_plan(job_name, emits, reason))
        else:
            for emit in emits:
                layers_skipped.append(LayerSkip(name=emit, reason=f"fresh: {reason}"))

    layers_to_refresh = _topo_sort(layers_to_refresh)

    return ExecutionPlan(
        layers_to_refresh=tuple(layers_to_refresh),
        layers_skipped=tuple(layers_skipped),
        estimated_duration=None,  # Phase 2 enhancement
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _scope_to_candidate_jobs(
    scope: SyncScope,
    conn: psycopg.Connection[Any] | None = None,
) -> list[str]:
    """Return the ordered list of legacy job names to consider for the
    given scope. Returns all in-DAG jobs (empty-tuple outside-DAG entries
    excluded). Per-scope filtering:

    - full: every in-DAG job
    - high_frequency: only portfolio_sync + fx_rates emitters
    - layer(name): only jobs whose emits include the named layer + jobs
      emitting any of its transitive dependencies
    - job(legacy_name): that job + jobs emitting any of its deps
    - behind: DEGRADED + ACTION_NEEDED layers plus transitive non-HEALTHY
      upstreams; requires a live conn
    """
    in_dag = [name for name, emits in JOB_TO_LAYERS.items() if emits]

    if scope.kind == "full":
        return in_dag

    if scope.kind == "high_frequency":
        hf_layers = {"portfolio_sync", "fx_rates"}
        return [job for job in in_dag if any(e in hf_layers for e in JOB_TO_LAYERS[job])]

    if scope.kind == "layer":
        assert scope.detail is not None
        target_layer = scope.detail
        if target_layer not in LAYERS:
            raise ValueError(f"unknown layer: {target_layer}")
        needed_layers = _transitive_layer_closure({target_layer})
        return [job for job in in_dag if any(e in needed_layers for e in JOB_TO_LAYERS[job])]

    if scope.kind == "job":
        assert scope.detail is not None
        target_job = scope.detail
        if target_job not in JOB_TO_LAYERS or not JOB_TO_LAYERS[target_job]:
            raise ValueError(f"unknown in-DAG job: {target_job}")
        target_emits = set(JOB_TO_LAYERS[target_job])
        needed_layers = _transitive_layer_closure(target_emits)
        return [job for job in in_dag if any(e in needed_layers for e in JOB_TO_LAYERS[job])]

    if scope.kind == "behind":
        if conn is None:
            raise ValueError("scope='behind' requires a live connection")
        states = compute_layer_states_from_db(conn)
        # RETRYING is a DIRECT target alongside DEGRADED / ACTION_NEEDED (#2274).
        #
        # ⚠ It was excluded on the strength of a spec promise that was never
        # implemented: freshness-unification.md:78 says "orchestrator will
        # re-fire with backoff", but no production code re-fires a layer —
        # ``RetryPolicy.backoff_seconds`` is read only by its own __post_init__
        # validation and by tests, and ``layer_state`` reads ``max_attempts``
        # alone. So RETRYING named a catch-up that never happened, and the
        # exclusion made the gate NON-MONOTONE in failure count: 0 failures
        # (DEGRADED) selected, 1..max_attempts-1 (RETRYING) NOT selected,
        # >= max_attempts (ACTION_NEEDED) selected again. A layer that had
        # failed LESS was treated as less recoverable.
        #
        # Measured cost on the dev corpus: ``candles`` sat RETRYING from
        # 2026-09-15 03:00Z while two boot sweeps (13:22Z, 14:22Z) each planned
        # ZERO layers, leaving 4,292 instruments without their 09-14 bar. All 5
        # of the 64 ``behind`` sweeps in the preceding 14 days that planned
        # nothing had ``candles`` failed with an as-of streak of 1 or 2.
        #
        # ⚠ This is not a new behaviour class: ``_transitive_upstreams_not_healthy``
        # below already pulls RETRYING layers into a plan whenever some OTHER
        # layer is unhealthy. What was broken is that recovery of a failed layer
        # depended on an unrelated layer also being unhealthy. It IS a wider
        # trigger, though — see the composite-job note on the closure line.
        target_layers = {
            n for n, s in states.items() if s in {LayerState.DEGRADED, LayerState.RETRYING, LayerState.ACTION_NEEDED}
        }
        if not target_layers:
            return []
        # Include any non-HEALTHY upstreams transitively, so a waiting
        # layer's prerequisites get refreshed first. ⚠ The closure predicate is
        # NOT the same as the target predicate above — it excludes only HEALTHY
        # and DISABLED, so it can pull in RUNNING, SECRET_MISSING and
        # CASCADE_WAITING upstreams, and it stops traversal at a healthy
        # intermediate. Do not restate one as the other.
        target_layers |= _transitive_upstreams_not_healthy(target_layers, states)
        # ⚠ PRE-EXISTING GAP, now reachable from one more trigger: a job that
        # emits several layers runs ALL of them, and the executor does not
        # recheck the operator's enabled flag. ``morning_candidate_review`` is
        # the only multi-emit job in the registry (scoring + recommendations),
        # so a DISABLED scoring can be written by a RETRYING recommendations —
        # exactly as it already could by a DEGRADED one. Pinned by
        # ``test_behind_composite_job_runs_disabled_sibling`` so the behaviour
        # is explicit rather than silent; the fix belongs in the executor.
        return [job for job in in_dag if any(e in target_layers for e in JOB_TO_LAYERS[job])]

    raise ValueError(f"unknown scope kind: {scope.kind}")


def _transitive_layer_closure(seed: set[str]) -> set[str]:
    """Return seed plus all transitive dependencies as a set of layer names."""
    result: set[str] = set(seed)
    stack = list(seed)
    while stack:
        name = stack.pop()
        for dep in LAYERS[name].dependencies:
            if dep not in result:
                result.add(dep)
                stack.append(dep)
    return result


def _transitive_upstreams_not_healthy(seed: set[str], states: dict[str, LayerState]) -> set[str]:
    """Return all transitive upstream dependencies of seed layers that are
    not HEALTHY. Used by scope='behind' to include prerequisites that need
    refreshing before the target layers can be satisfied."""
    result: set[str] = set()
    stack = list(seed)
    while stack:
        name = stack.pop()
        if name not in LAYERS:
            continue
        for dep in LAYERS[name].dependencies:
            if dep in result:
                continue
            dep_state = states.get(dep)
            # Skip HEALTHY (no work needed) and DISABLED (operator
            # toggle wins — spec §3.2 rule 1). A disabled upstream
            # leaves the target in CASCADE_WAITING by design; firing
            # it anyway would bypass the operator's explicit off.
            if dep_state in {LayerState.HEALTHY, LayerState.DISABLED}:
                continue
            result.add(dep)
            stack.append(dep)
    return result


def _all_emits_fresh(
    conn: psycopg.Connection[Any],
    emits: tuple[str, ...],
) -> tuple[bool, str]:
    """True iff every emit's is_fresh() returns True. Returns the first
    stale layer's detail string as the reason when false."""
    for emit in emits:
        fresh, detail = LAYERS[emit].is_fresh(conn)
        if not fresh:
            return False, f"{emit}: {detail}"
    return True, "all emits fresh"


def _build_layer_plan(
    job_name: str,
    emits: tuple[str, ...],
    reason: str,
) -> LayerPlan:
    """Derive LayerPlan.dependencies per spec §2.6 external-only rule.

    external = (union of LAYERS[emit].dependencies for emit in emits)
               - set(emits)

    Intra-composite edges are dropped; the underlying legacy job body
    runs them atomically. Transitive ancestors are NOT included — the
    orchestrator walks the DAG in topological order, so transitive skip
    propagation happens via DEP_SKIPPED bubbling through direct deps.
    """
    emit_set = set(emits)
    emit_deps: set[str] = set()
    for emit in emits:
        emit_deps.update(LAYERS[emit].dependencies)
    external_deps = emit_deps - emit_set

    # is_blocking = any emit blocks. Current composites share
    # is_blocking=True for both emits, so any()/all() agree in practice.
    is_blocking = any(LAYERS[emit].is_blocking for emit in emits)

    return LayerPlan(
        name=job_name,
        emits=emits,
        reason=reason,
        dependencies=tuple(sorted(external_deps)),  # deterministic; same-depth order irrelevant
        is_blocking=is_blocking,
        estimated_items=0,  # Phase 2: query historical items_total
    )


def _topo_sort(plans: list[LayerPlan]) -> list[LayerPlan]:
    """Stable topological sort by emit depth in the LAYERS DAG.

    Plans whose direct dependencies (in the pre-derivation sense, using
    LAYERS[emit].dependencies) are satisfied by earlier plans appear
    first. Deterministic: ties broken by job name.
    """
    if not plans:
        return []

    by_name = {p.name: p for p in plans}

    # Depth per layer name, memoized.
    depth_cache: dict[str, int] = {}

    def depth(layer: str) -> int:
        if layer in depth_cache:
            return depth_cache[layer]
        deps = LAYERS[layer].dependencies
        d = 0 if not deps else 1 + max(depth(dep) for dep in deps)
        depth_cache[layer] = d
        return d

    # Plan depth = max depth across its emits.
    def plan_depth(p: LayerPlan) -> int:
        return max(depth(e) for e in p.emits)

    return sorted(by_name.values(), key=lambda p: (plan_depth(p), p.name))
