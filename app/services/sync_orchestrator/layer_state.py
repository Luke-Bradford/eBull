"""Compute LayerState for every registered layer (spec §3.2).

`compute_layer_state(ctx) -> LayerState` is pure. Input is a
LayerContext, output is a LayerState. The DB-facing builder
`compute_layer_states_from_db(conn)` lives in the same module and
applies fixed-point cascade propagation.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.services.sync_orchestrator.layer_types import (
    FailureCategory,
    LayerState,
    REMEDIES,
)


@dataclass(frozen=True)
class LayerContext:
    is_enabled: bool
    is_running: bool
    # sync_layer_progress.status vocabulary: 'pending' | 'running' |
    # 'complete' | 'failed' | 'skipped' | 'partial'. A caller reading
    # from job_runs (success/failure/skipped) must translate before
    # building a context.
    latest_status: str
    latest_category: str | None
    attempts: int
    upstream_states: dict[str, LayerState]
    secret_present: bool
    content_ok: bool
    age_seconds: float
    cadence_seconds: float
    grace_multiplier: float
    max_attempts: int


def compute_layer_state(ctx: LayerContext) -> LayerState:
    # Rule 1: operator toggle wins.
    if not ctx.is_enabled:
        return LayerState.DISABLED
    # Rule 2: run in flight.
    if ctx.is_running:
        return LayerState.RUNNING
    # Rule 3: missing secrets beat stale failure rows.
    if not ctx.secret_present:
        return LayerState.SECRET_MISSING
    # Rules 4-6: local-failure branches.
    if ctx.latest_status == "failed":
        category_values = {c.value for c in FailureCategory}
        category = (
            FailureCategory(ctx.latest_category)
            if ctx.latest_category in category_values
            else FailureCategory.INTERNAL_ERROR
        )
        remedy = REMEDIES[category]
        if not remedy.self_heal:
            return LayerState.ACTION_NEEDED
        if ctx.attempts >= ctx.max_attempts:
            return LayerState.ACTION_NEEDED
        return LayerState.RETRYING
    # Rule 7: cascade — only terminal upstream states propagate.
    if any(
        s in {LayerState.ACTION_NEEDED, LayerState.SECRET_MISSING}
        for s in ctx.upstream_states.values()
    ):
        return LayerState.CASCADE_WAITING
    # Rule 8: content predicate.
    if not ctx.content_ok:
        return LayerState.DEGRADED
    # Rule 9: age vs grace window.
    if ctx.age_seconds > ctx.cadence_seconds * ctx.grace_multiplier:
        return LayerState.DEGRADED
    return LayerState.HEALTHY
