"""Typed vocabulary for the freshness state machine (spec sub-project A).

Consumed by the registry (chunk 2), adapters (chunk 3), state
computation (chunk 4), and the v2 API (chunk 5). Must not import from
any orchestrator runtime module — this sits at the bottom of the
orchestrator import graph.
"""

from __future__ import annotations

from dataclasses import dataclass
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


class FailureCategory(StrEnum):
    AUTH_EXPIRED = "auth_expired"
    RATE_LIMITED = "rate_limited"
    SOURCE_DOWN = "source_down"
    SCHEMA_DRIFT = "schema_drift"
    DB_CONSTRAINT = "db_constraint"
    DATA_GAP = "data_gap"
    UPSTREAM_WAITING = "upstream_waiting"
    INTERNAL_ERROR = "internal_error"


@dataclass(frozen=True)
class Remedy:
    message: str
    operator_fix: str | None
    self_heal: bool


REMEDIES: dict[FailureCategory, Remedy] = {
    FailureCategory.AUTH_EXPIRED: Remedy(
        message="Credential rejected by provider",
        operator_fix="Update the API key in Settings → Providers",
        self_heal=False,
    ),
    FailureCategory.RATE_LIMITED: Remedy(
        message="Rate limit hit — retrying with backoff",
        operator_fix=None,
        self_heal=True,
    ),
    FailureCategory.SOURCE_DOWN: Remedy(
        message="Data source unreachable — retrying with backoff",
        operator_fix=None,
        self_heal=True,
    ),
    FailureCategory.SCHEMA_DRIFT: Remedy(
        message="Provider payload shape changed — needs code update",
        operator_fix="File a bug; the adapter needs a parser update",
        self_heal=False,
    ),
    FailureCategory.DB_CONSTRAINT: Remedy(
        message="Database constraint violated — likely data-model bug",
        operator_fix="Open orchestrator details and inspect the offending row",
        self_heal=False,
    ),
    FailureCategory.DATA_GAP: Remedy(
        message="Source returned no data — will retry next cycle",
        operator_fix=None,
        self_heal=True,
    ),
    FailureCategory.UPSTREAM_WAITING: Remedy(
        message="Waiting on upstream layer",
        operator_fix=None,
        self_heal=True,
    ),
    FailureCategory.INTERNAL_ERROR: Remedy(
        message="Unclassified error — retrying with backoff",
        operator_fix=None,
        self_heal=True,
    ),
}
