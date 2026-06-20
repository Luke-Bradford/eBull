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

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal
from uuid import UUID

from app.services.processes.param_metadata import ParamMetadata

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

# C7 (#1530) — page-scope classification of a process row. Mirrors the
# ``ScheduledJob.role`` literal in ``app/workers/scheduler.py`` (kept
# inline there to avoid a scheduler→processes-service import). ``Literal``-
# typed on both the source dataclass (``ProcessRow.role``) and the wire
# carrier (``ProcessRowResponse.role``) so pyright type-guards the role at
# the API boundary, consistent with ``ProcessLane`` / ``ProcessMechanism`` /
# ``ProcessStatus``.
ProcessRole = Literal["steady_state", "bootstrap", "backfill"]

ProcessStatus = Literal[
    "idle",
    "pending_first_run",
    "running",
    "ok",
    "failed",
    "pending_retry",
    "cancelled",
    "disabled",
]

# #1512 — single computed health verdict that collapses the two
# orthogonal axes (``status`` + ``stale_reasons``) the operator used to
# see rendered side-by-side as contradictory chips. Derived (not stored)
# at the API layer (``app/api/processes.py::_convert_row`` →
# ``health_verdict.compute_verdict``). The FE renders ONE verdict pill.
HealthVerdict = Literal["current", "working", "self_healing", "attention", "stale_manual"]

RunStatus = Literal["success", "failure", "partial", "cancelled", "skipped"]

WatermarkCursorKind = Literal[
    "filed_at",
    "accession",
    "instrument_offset",
    "stage_index",
    "epoch",
    "atom_etag",
]

# Operator-amendment §A1 (PR8 / #1083): four-case stale model. Supersedes
# the original PR8 spec text ("rolling p95 + last log timestamp" — line
# 953-958) which was a v0 sketch. Multiple reasons can fire on one row.
StaleReason = Literal[
    "schedule_missed",
    "watermark_gap",
    "queue_stuck",
    "mid_flight_stuck",
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

    ``last_progress_at`` is the heartbeat (sql/140 §A3): the producer's
    ``record_processed`` bumps it. The FE renders the elapsed-since-
    heartbeat on the mid_flight_stuck chip. ``None`` when the producer
    has not yet recorded its first tick — adapters fall back to
    ``started_at`` when computing stale_reasons.

    ``is_cancelling`` reflects ``cancel_requested_at`` non-NULL on the
    underlying run row.
    """

    run_id: int
    started_at: datetime
    rows_processed_so_far: int | None
    progress_units_done: int | None
    progress_units_total: int | None
    last_progress_at: datetime | None
    is_cancelling: bool


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

    ``stale_reasons`` is computed by the adapter under the four-case
    stale model (operator-amendment §A1, PR8 / #1083). Empty tuple
    means not stale; multiple reasons can fire simultaneously. See
    ``app/services/processes/stale_detection.py`` for the rule shapes.

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
    stale_reasons: tuple[StaleReason, ...]
    # PR4 #1082 — operator-facing description. Renders as the ⓘ
    # tooltip on the admin ProcessesTable. Empty for processes that
    # don't have one declared (e.g. legacy fallback paths) — the FE
    # hides the tooltip when this is empty rather than showing a
    # blank popover.
    description: str = ""
    # PR2 #1064 — operator-exposable params for the Advanced disclosure
    # tab on the drill-in. Bootstrap + ingest_sweep adapters keep the
    # default empty tuple; scheduled_adapter populates from the
    # underlying ``ScheduledJob.params_metadata`` so the FE knows
    # which form fields to render.
    params_metadata: tuple[ParamMetadata, ...] = field(default_factory=tuple)
    # #1511 / T5 — set by scheduled_adapter when the job's data_freshness
    # source is bootstrap-covered AND its newest filing is within cadence.
    # Fed to ``compute_verdict(watermark_is_fresh=...)`` so a never-run
    # (``pending_first_run``) poll whose source bootstrap already seeded
    # reads Current instead of "first run pending". Bootstrap + ingest_sweep
    # adapters keep the default — the look-through is scheduled-job-only.
    source_watermark_fresh: bool = False
    # #1509 / T3 — ``job_runs.next_retry_at`` of the latest terminal run,
    # set by scheduled_adapter when the last failure scheduled a backoff
    # retry. ``_convert_row`` derives ``retry_in_flight`` + an "HH:MM" label
    # from it for ``compute_verdict`` so a transiently-failed row reads
    # Self-healing "will retry HH:MM" instead of red. None = no retry pending.
    next_retry_at: datetime | None = None
    # #1510 / T4 — True when a FRESH ``manual_job`` re-enqueue placed by the
    # liveness watchdog (``requested_by='system:liveness_kick'``) is in flight
    # for this job. Set by scheduled_adapter via a dedicated EXISTS probe
    # bounded to ``requested_at >= now - 30m`` (a kick aged past that window is
    # itself wedged and must not keep painting the row green). Fed to
    # ``compute_verdict(liveness_kick_in_flight=...)`` so a watchdog-re-enqueued
    # stalled job reads Self-healing "re-enqueued, recovering" instead of red.
    liveness_kick_in_flight: bool = False
    # #1508 / C6 — True when this scheduled job has ZERO lifetime runs AND is
    # now overdue past its first expected fire (persisted ``job_first_seen`` +
    # one cadence + grace), computed by scheduled_adapter. Fed to
    # ``compute_verdict(never_started=...)`` so a broken-from-day-one job reads
    # attention "never started" instead of forever-green "first run pending".
    # Bootstrap + ingest_sweep adapters keep the default — scheduled-job-only.
    never_started: bool = False
    # #1508 / Task 5 — True when the latest terminal run was ``cancelled`` AND
    # that cancel is traceable to a deliberate operator stop request (a
    # ``process_stop_requests`` row pinning the terminal run's kind + id),
    # computed by scheduled_adapter ONLY when status is ``cancelled``. Fed to
    # ``compute_verdict(cancel_was_operator_initiated=...)`` so an operator-
    # cancelled job reads benign Current (green) until its next fire, while a
    # system/crash cancel stays attention "last run cancelled". Bootstrap +
    # ingest_sweep adapters keep the default.
    cancel_was_operator_initiated: bool = False
    # C7 (#1530) — page-scope classification: ``steady_state`` (recurring
    # keeper that holds a source / data current), ``bootstrap`` (one-time
    # install drain), or ``backfill`` (historical catch-up). Unlike the
    # verdict-input fields above, this is a WIRE field: ``_convert_row``
    # maps it onto ``ProcessRowResponse`` so the FE can move bootstrap /
    # backfill rows into a collapsed section and default the page to
    # steady-state keepers only. scheduled_adapter copies it from the
    # underlying ``ScheduledJob.role``; bootstrap_adapter sets
    # ``"bootstrap"``; ingest_sweep_adapter keeps the default
    # ``"steady_state"`` (sweeps keep their source current).
    role: ProcessRole = "steady_state"
    # #1689 — ``job_runs.attempt`` of the latest terminal run (the
    # consecutive-failure streak position, 1 = first natural fire). Set by
    # scheduled_adapter from the terminal row so the FE can render "attempt N"
    # on a retrying row. None for adapters that don't track it (bootstrap /
    # ingest_sweep) or jobs that have never failed.
    attempt: int | None = None


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
    "HealthVerdict",
    "ProcessLane",
    "ProcessMechanism",
    "ProcessRole",
    "ProcessRow",
    "ProcessRunSummary",
    "ProcessSnapshot",
    "ProcessStatus",
    "ProcessWatermark",
    "RunStatus",
    "WatermarkCursorKind",
]
