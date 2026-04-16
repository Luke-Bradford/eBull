"""Sync orchestrator package.

Public surface: types + registries + planner + public entry points
(run_sync, submit_sync, set_executor). Adapters and reaper wired in
subsequent tasks.

Spec: docs/superpowers/specs/2026-04-16-data-orchestrator-and-observability-design.md
Plan: docs/superpowers/plans/2026-04-16-data-orchestrator-p1.md
"""

from app.services.sync_orchestrator.executor import (
    run_sync,
    set_executor,
    submit_sync,
)
from app.services.sync_orchestrator.planner import build_execution_plan
from app.services.sync_orchestrator.registry import (
    JOB_TO_LAYERS,
    LAYERS,
    DataLayer,
)
from app.services.sync_orchestrator.types import (
    PREREQ_SKIP_MARKER,
    ExecutionPlan,
    LayerOutcome,
    LayerPlan,
    LayerRefresh,
    LayerSkip,
    ProgressCallback,
    RefreshResult,
    SyncAlreadyRunning,
    SyncResult,
    SyncScope,
    SyncTrigger,
    prereq_skip_reason,
)

__all__ = [
    "DataLayer",
    "ExecutionPlan",
    "JOB_TO_LAYERS",
    "LAYERS",
    "LayerOutcome",
    "LayerPlan",
    "LayerRefresh",
    "LayerSkip",
    "PREREQ_SKIP_MARKER",
    "ProgressCallback",
    "RefreshResult",
    "SyncAlreadyRunning",
    "SyncResult",
    "SyncScope",
    "SyncTrigger",
    "build_execution_plan",
    "prereq_skip_reason",
    "run_sync",
    "set_executor",
    "submit_sync",
]
