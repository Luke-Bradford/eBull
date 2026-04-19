"""Typed vocabulary for the freshness state machine (spec sub-project A).

Consumed by the registry (chunk 2), adapters (chunk 3), state
computation (chunk 4), and the v2 API (chunk 5). Must not import from
any orchestrator runtime module — this sits at the bottom of the
orchestrator import graph.
"""

from __future__ import annotations

from enum import StrEnum


class LayerState(StrEnum):
    HEALTHY = "healthy"
    RUNNING = "running"
    RETRYING = "retrying"
    DEGRADED = "degraded"
    ACTION_NEEDED = "action_needed"
    SECRET_MISSING = "secret_missing"
    CASCADE_WAITING = "cascade_waiting"
    DISABLED = "disabled"
