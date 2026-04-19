"""Typed vocabulary for the freshness state machine (spec sub-project A).

Consumed by the registry (chunk 2), adapters (chunk 3), state
computation (chunk 4), and the v2 API (chunk 5). Must not import from
any orchestrator runtime module — this sits at the bottom of the
orchestrator import graph.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    # psycopg is only used in ContentPredicate's type annotation. Guarding
    # under TYPE_CHECKING keeps this bottom-of-graph module importable in
    # contexts that need only the enums + dataclasses.
    import psycopg


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


@dataclass(frozen=True)
class Cadence:
    interval: timedelta

    def __post_init__(self) -> None:
        if self.interval <= timedelta(0):
            raise ValueError("interval must be positive")

    def grace_window(self, grace_multiplier: float) -> timedelta:
        if grace_multiplier <= 0:
            raise ValueError(f"grace_multiplier must be positive (got {grace_multiplier})")
        return self.interval * grace_multiplier


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff_seconds: tuple[int, ...] = (60, 600, 3600)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if len(self.backoff_seconds) != self.max_attempts:
            raise ValueError(
                "backoff_seconds must have exactly max_attempts entries "
                f"(got {len(self.backoff_seconds)} for max_attempts={self.max_attempts})"
            )
        if any(b <= 0 for b in self.backoff_seconds):
            raise ValueError(f"backoff_seconds entries must all be positive (got {self.backoff_seconds})")


DEFAULT_RETRY_POLICY = RetryPolicy()


@dataclass(frozen=True)
class SecretRef:
    env_var: str
    display_name: str


class ContentPredicate(Protocol):
    """Structural signature for a per-layer content check.

    Returns (ok, detail). `ok=True` means the layer's data is
    considered content-current — for example, every Tier 1 ticker has a
    candle for today. `detail` is an operator-visible sentence surfaced
    when the predicate fails. Pure SELECT; must not write.
    """

    def __call__(self, conn: psycopg.Connection[Any]) -> tuple[bool, str]: ...


class LayerRefreshFailed(Exception):
    """Adapter-level failure carrying a categorisation.

    Adapters raise this so the executor can persist the category
    alongside the error message. Use this rather than `RuntimeError`
    when failing from inside a refresh adapter so downstream logging
    and the Admin UI can surface the taxonomy.
    """

    def __init__(self, category: FailureCategory, detail: str) -> None:
        super().__init__(f"{category.value}: {detail}")
        self.category = category
        self.detail = detail


def cadence_display_string(cadence: Cadence) -> str:
    """Short human label used by dashboards where a one-liner is enough."""
    total = int(cadence.interval.total_seconds())
    if total % 86400 == 0:
        d = total // 86400
        # d == 1 → "daily" (spoken word); d > 1 → "Nd" (compact label).
        return f"{d}d" if d > 1 else "daily"
    if total % 3600 == 0:
        return f"{total // 3600}h"
    if total % 60 == 0:
        return f"{total // 60}m"
    return f"{total}s"
