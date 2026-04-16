"""Sync orchestrator planner.

Builds ExecutionPlan from SyncScope. Applies freshness filtering
(unless force=True on the scope's target job), derives
LayerPlan.dependencies per the external-only rule (spec §2.6), and
topologically sorts layers_to_refresh from roots outward.
"""

from __future__ import annotations

from typing import Any

import psycopg

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
    candidate_jobs = _scope_to_candidate_jobs(scope)
    target_job = scope.detail if scope.kind == "job" else None

    layers_to_refresh: list[LayerPlan] = []
    layers_skipped: list[LayerSkip] = []

    for job_name in candidate_jobs:
        emits = JOB_TO_LAYERS[job_name]
        if not emits:  # outside-DAG job — should not be in candidates
            continue

        is_target = job_name == target_job
        if is_target and scope.force:
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


def _scope_to_candidate_jobs(scope: SyncScope) -> list[str]:
    """Return the ordered list of legacy job names to consider for the
    given scope. Returns all in-DAG jobs (empty-tuple outside-DAG entries
    excluded). Per-scope filtering:

    - full: every in-DAG job
    - high_frequency: only portfolio_sync + fx_rates emitters
    - layer(name): only jobs whose emits include the named layer + jobs
      emitting any of its transitive dependencies
    - job(legacy_name): that job + jobs emitting any of its deps
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
