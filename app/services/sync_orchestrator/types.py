"""Type and constant definitions for the sync orchestrator.

Pure data — no I/O, no DB, no dependencies on other orchestrator modules.
Importable from planner, executor, adapters without cycles.

See spec §2.1, §2.3, §2.6.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Literal, Protocol

# ---------------------------------------------------------------------------
# Outcome enumeration (spec §2.3)
# ---------------------------------------------------------------------------


class LayerOutcome(StrEnum):
    """What happened to a layer during this sync run."""

    SUCCESS = "success"
    NO_WORK = "no_work"
    PARTIAL = "partial"
    FAILED = "failed"
    DEP_SKIPPED = "dep_skipped"
    PREREQ_SKIP = "prereq_skip"


# ---------------------------------------------------------------------------
# PREREQ_SKIP marker (spec §1.3)
# ---------------------------------------------------------------------------


PREREQ_SKIP_MARKER = "prereq_missing:"


def prereq_skip_reason(detail: str) -> str:
    """Return the canonical ``record_job_skip(reason=...)`` string.

    fresh_by_audit counts a job_runs 'skipped' row ONLY when its
    error_msg starts with PREREQ_SKIP_MARKER. Legacy skip reasons
    without the marker do not count — they fall through to stale.
    """
    return f"{PREREQ_SKIP_MARKER} {detail}"


# ---------------------------------------------------------------------------
# RefreshResult + LayerRefresh protocol (spec §2.3)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RefreshResult:
    """Outcome of one layer's refresh call."""

    outcome: LayerOutcome
    row_count: int
    items_processed: int
    items_total: int | None
    detail: str
    error_category: str | None = None


class ProgressCallback(Protocol):
    def __call__(self, items_done: int, items_total: int | None = None) -> None: ...


class LayerRefresh(Protocol):
    """Canonical adapter signature — one declaration, used by both
    single-layer and composite adapters.

    Single-layer adapters return ``[(layer_name, result)]`` — one element.
    Composite adapters return one element per emitted layer, in declared
    emit order. See spec §2.3 + §2.3.1.
    """

    def __call__(
        self,
        *,
        sync_run_id: int,
        progress: ProgressCallback,
        upstream_outcomes: Mapping[str, LayerOutcome],
    ) -> Sequence[tuple[str, RefreshResult]]: ...


# ---------------------------------------------------------------------------
# LayerPlan + ExecutionPlan (spec §2.6)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LayerPlan:
    name: str
    emits: tuple[str, ...]
    reason: str
    dependencies: tuple[str, ...]
    is_blocking: bool
    estimated_items: int


@dataclass(frozen=True)
class LayerSkip:
    name: str
    reason: str


@dataclass(frozen=True)
class ExecutionPlan:
    layers_to_refresh: tuple[LayerPlan, ...]
    layers_skipped: tuple[LayerSkip, ...]
    estimated_duration: timedelta | None


# ---------------------------------------------------------------------------
# SyncScope, SyncTrigger, SyncResult (spec §2.1)
# ---------------------------------------------------------------------------


SyncTrigger = Literal["manual", "scheduled", "catch_up"]


@dataclass(frozen=True)
class SyncScope:
    kind: Literal["full", "layer", "high_frequency", "job", "behind"]
    detail: str | None = None
    force: bool = False

    @classmethod
    def full(cls) -> SyncScope:
        return cls(kind="full")

    @classmethod
    def layer(cls, name: str) -> SyncScope:
        return cls(kind="layer", detail=name)

    @classmethod
    def job(cls, legacy_job_name: str, force: bool = True) -> SyncScope:
        return cls(kind="job", detail=legacy_job_name, force=force)

    @classmethod
    def high_frequency(cls) -> SyncScope:
        return cls(kind="high_frequency")

    @classmethod
    def behind(cls) -> SyncScope:
        # `force=True`: target layers were already state-selected as
        # DEGRADED / ACTION_NEEDED (and their non-HEALTHY upstreams).
        # Legacy `is_fresh` re-filtering would drop jobs the state
        # machine explicitly wants refreshed, so bypass it.
        return cls(kind="behind", force=True)


@dataclass(frozen=True)
class SyncResult:
    sync_run_id: int
    outcomes: Mapping[str, LayerOutcome]


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class SyncAlreadyRunning(RuntimeError):
    """Raised when the partial unique index gate denies a new sync.

    The HTTP layer maps this to 409 Conflict with the active
    sync_run_id in the body so the client can poll it.
    """

    def __init__(
        self,
        scope: SyncScope,
        active_sync_run_id: int | None = None,
    ) -> None:
        super().__init__(f"sync already running (scope={scope.kind}, active_id={active_sync_run_id})")
        self.scope = scope
        self.active_sync_run_id = active_sync_run_id
