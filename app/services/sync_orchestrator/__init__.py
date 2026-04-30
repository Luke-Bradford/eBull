"""Sync orchestrator package.

Public surface: types + registries + planner + the synchronous
``run_sync`` entry. The pre-#719 ``submit_sync`` / ``set_executor``
in-process executor wiring is gone; the API publishes via
``app.services.sync_orchestrator.dispatcher.publish_sync_request`` and
the jobs-process listener invokes ``run_sync`` on its own executor.

Spec: docs/superpowers/specs/2026-04-30-jobs-out-of-process-design.md
"""

from app.services.sync_orchestrator.executor import run_sync
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
]
