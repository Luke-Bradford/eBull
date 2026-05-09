"""Process envelope dataclasses for the admin control hub.

Issue #1071 (umbrella #1064).
Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §Process envelope.

Every row in the admin Processes table conforms to ``ProcessRow``,
regardless of whether the underlying mechanism is the bootstrap
orchestrator, an APScheduler job, or an ingest sweep. Adapters in this
package translate the per-mechanism source tables into ProcessRows so
the API layer renders one consistent shape.

The envelope is intentionally pure data — no DB handles, no sessions,
no async — because adapter callers serialise straight to JSON for the
``/system/processes`` endpoint. Watermark resolution is deferred to PR4
(#1064 spec §PR4); PR3 sets ``watermark=None`` on every row.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

ProcessLane = Literal[
    "setup",
    "universe",
    "candles",
    "sec",
    "ownership",
    "fundamentals",
    "ops",
    "ai",
]

ProcessMechanism = Literal["bootstrap", "scheduled_job", "ingest_sweep"]

ProcessStatus = Literal[
    "idle",
    "pending_first_run",
    "running",
    "ok",
    "failed",
    "stale",
    "pending_retry",
    "cancelled",
    "disabled",
]

RunStatus = Literal["success", "failure", "partial", "cancelled", "skipped"]

WatermarkCursorKind = Literal[
    "filed_at",
    "accession",
    "instrument_offset",
    "stage_index",
    "epoch",
    "atom_etag",
]


@dataclass(frozen=True, slots=True)
class ErrorClassSummary:
    """One grouped error class on a process row.

    Mirrors ``job_runs.error_classes`` JSONB shape (sql/137 header):
    grouping key + count + sample message + last subject + last seen
    timestamp. Adapters coalesce subject to ``None`` when the producer
    did not provide one.
    """

    error_class: str
    count: int
    last_seen_at: datetime
    sample_message: str
    sample_subject: str | None


@dataclass(frozen=True, slots=True)
class ProcessRunSummary:
    """One terminal run from the per-process History tab.

    ``rows_skipped_by_reason`` mirrors ``job_runs.rows_skipped_by_reason``
    (sql/137). Adapters that lack per-reason granularity emit
    ``{"unknown": <count>}`` rather than ``{}`` so the FE chart always
    has a key.
    """

    run_id: int
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    rows_processed: int | None
    rows_skipped_by_reason: dict[str, int]
    rows_errored: int
    status: RunStatus
    cancelled_by_operator_id: UUID | None


@dataclass(frozen=True, slots=True)
class ActiveRunSummary:
    """In-flight run telemetry rendered above the per-row progress bar.

    ``progress_units_done`` / ``progress_units_total`` populate when the
    producer cooperates via ``JobTelemetryAggregator.set_target`` +
    ``record_processed``. ``rows_processed_so_far`` is the same scalar
    surfaced as the "Processed: N" ticker even when no target is known.

    ``is_stale`` defaults False in PR3 — the rolling-p95 stale rule is
    PR8. ``is_cancelling`` reflects ``cancel_requested_at`` non-NULL on
    the underlying run row.
    """

    run_id: int
    started_at: datetime
    rows_processed_so_far: int | None
    progress_units_done: int | None
    progress_units_total: int | None
    expected_p95_seconds: float | None
    is_cancelling: bool
    is_stale: bool


@dataclass(frozen=True, slots=True)
class ProcessWatermark:
    """Operator-visible resume cursor surfaced on the Iterate tooltip.

    Wired in PR4. PR3 sets ``ProcessRow.watermark = None`` on every row.
    """

    cursor_kind: WatermarkCursorKind
    cursor_value: str
    human: str
    last_advanced_at: datetime


@dataclass(frozen=True, slots=True)
class ProcessRow:
    """One row in the admin Processes table.

    ``last_n_errors`` is computed by the adapter using the
    auto-hide-on-retry rule (spec §Auto-hide-on-retry rule): empty when
    the latest terminal run is a failure AND a retry is currently in
    flight; full grouped errors otherwise. The FE just renders.

    PR3 leaves ``watermark`` at ``None``; PR4 wires the resolver and
    the per-mechanism `human` strings that surface on Iterate tooltips.
    """

    process_id: str
    display_name: str
    lane: ProcessLane
    mechanism: ProcessMechanism
    status: ProcessStatus
    last_run: ProcessRunSummary | None
    active_run: ActiveRunSummary | None
    cadence_human: str
    cadence_cron: str | None
    next_fire_at: datetime | None
    watermark: ProcessWatermark | None
    can_iterate: bool
    can_full_wash: bool
    can_cancel: bool
    last_n_errors: tuple[ErrorClassSummary, ...]


@dataclass(frozen=True, slots=True)
class ProcessSnapshot:
    """Cross-adapter snapshot returned by ``GET /system/processes``.

    ``partial`` flips True when at least one adapter raised — surfaced
    so the FE can render a banner ("ingest sweep telemetry unavailable")
    while still rendering the lanes that succeeded. Spec
    §Failure-mode invariants.
    """

    rows: tuple[ProcessRow, ...]
    partial: bool


__all__ = [
    "ActiveRunSummary",
    "ErrorClassSummary",
    "ProcessLane",
    "ProcessMechanism",
    "ProcessRow",
    "ProcessRunSummary",
    "ProcessSnapshot",
    "ProcessStatus",
    "ProcessWatermark",
    "RunStatus",
    "WatermarkCursorKind",
]
