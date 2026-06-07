"""Pure-logic four-case stale model.

Issue #1083 (umbrella #1064) — admin control hub PR8.
Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §A1 (operator-amendment round 1, line 11-22) — supersedes the
      legacy §"Stale-detection rule" (line 597-606) v0 sketch.

Four reasons can fire on one row simultaneously:

1. ``schedule_missed`` — ``mechanism="scheduled_job"`` only. Cron
   should have fired by now and didn't. Negative when the job is
   actively running (overlap-suppression is intentional, not a miss).
2. ``watermark_gap`` — ``mechanism="scheduled_job"`` and ``ingest_sweep``
   whose watermark lives in ``data_freshness_index``. Source has fresh
   data; we're behind. Negative when the row is currently running.
   Bootstrap NEVER watermark-gaps (no ``data_freshness_index`` row).
3. ``queue_stuck`` — applies to ALL mechanisms. A
   ``pending_job_requests`` row with ``status='dispatched'`` and worker
   pickup older than ``QUEUE_STUCK_THRESHOLD_S``. The dispatcher hasn't
   observed terminal status from the worker; the worker may have
   crashed. Boot-recovery sweep (sql/137 §R2-W2) handles >6h; this is
   the in-window display.
4. ``mid_flight_stuck`` — ``status="running"`` AND
   ``COALESCE(active_run.last_progress_at, active_run.started_at) <
   now() - threshold``. Falling back to ``started_at`` covers the
   "stuck before first tick" case (Codex pre-impl review BLOCKING) —
   without it, a worker that crashes before its first
   ``record_processed`` would never surface as stale.

Adapters do the per-rule DB probes (one query each); this module
composes the boolean results into the ordered ``stale_reasons`` tuple.
Keeping the logic pure makes it cheap to unit-test without DB.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Final

from app.services.processes import (
    ProcessMechanism,
    ProcessStatus,
    StaleReason,
)
from app.services.processes.stale_thresholds import get_threshold

# Cron miss tolerance — APScheduler fires within a few seconds of the
# nominal time; 60s absorbs jitter without masking a genuine miss.
# Retained for back-compat / external reference; the C1 rule below now
# keys off a full cadence cycle, not this single-tick tolerance.
SCHEDULE_MISS_TOLERANCE_S: Final[int] = 60

# C1 (#1508 two-state): schedule_missed fires only when overdue by a WHOLE
# cadence cycle, not a single late tick. FLOOR protects sub-cycle jobs
# (every-5-min) from flapping when their cadence is shorter than the floor.
SCHEDULE_MISS_FLOOR_S: Final[int] = 300

# Watermark-gap tolerance — same shape: source's ``expected_next_at``
# is a prediction, so allow 60s slack before declaring a gap.
WATERMARK_GAP_TOLERANCE_S: Final[int] = 60

# Queue-stuck threshold — 30 min in the operator-amendment §A1.3.
# Boot-recovery sweep handles >6h.
QUEUE_STUCK_THRESHOLD_S: Final[int] = 30 * 60


def compute(
    *,
    mechanism: ProcessMechanism,
    status: ProcessStatus,
    expected_fire_at: datetime | None,
    has_data_freshness_gap: bool,
    has_dispatched_queue_age: bool,
    last_progress_at: datetime | None,
    active_run_started_at: datetime | None,
    process_id: str,
    now: datetime,
    cadence_period_s: int = 0,
) -> tuple[StaleReason, ...]:
    """Compose the per-row ``stale_reasons`` tuple.

    Args:
        mechanism: Row's mechanism (``bootstrap`` / ``scheduled_job`` /
            ``ingest_sweep``).
        status: Row's ``ProcessStatus``.
        expected_fire_at: The FIRST cadence-occurrence strictly after
            the latest terminal run's ``started_at``. ``None`` when the
            job has never run (``pending_first_run`` already covers
            that surface) or when the mechanism has no schedule. The
            rule fires when this timestamp is more than
            ``max(cadence_period_s, SCHEDULE_MISS_FLOOR_S)`` in the past
            (C1, #1508) — i.e., a whole cadence cycle has elapsed past
            the slot we should have fired in and we still didn't.
            Computed from
            ``compute_next_run(cadence, latest_terminal.started_at)``
            in the scheduled adapter; pure-future ``next_fire_at``
            values would never be reachable so the rule could never
            fire (Codex pre-push BLOCKING).
        has_data_freshness_gap: True when at least one
            ``data_freshness_index`` row for this process's freshness
            source has ``expected_next_at IS NOT NULL`` AND
            ``expected_next_at < now() - WATERMARK_GAP_TOLERANCE_S``.
            Adapter probes once via a per-source ``LIMIT 1`` query.
        has_dispatched_queue_age: True when at least one
            ``pending_job_requests`` row for this process_id has
            ``status='dispatched'`` AND worker pickup older than
            ``QUEUE_STUCK_THRESHOLD_S``. Adapter probes once.
        last_progress_at: Active run's heartbeat (``None`` when the
            producer has not yet recorded its first tick OR there is
            no active run).
        active_run_started_at: Active run's ``started_at`` — the
            fallback heartbeat when ``last_progress_at`` is ``None``.
            ``None`` when the row has no active run.
        process_id: Used for the per-process mid_flight_stuck threshold
            override.
        now: Reference time (UTC). Caller passes ``datetime.now(UTC)``;
            tests pin a specific instant.
        cadence_period_s: The job's cadence period in seconds (C1,
            #1508). ``schedule_missed`` fires only when
            ``expected_fire_at`` is overdue by more than
            ``max(cadence_period_s, SCHEDULE_MISS_FLOOR_S)`` — a whole
            skipped cycle, not a single late tick. Defaults to ``0`` so
            the non-scheduled callers (bootstrap / ingest_sweep), which
            are skipped by the ``mechanism`` gate anyway, need no change.

    Returns:
        Ordered tuple of ``StaleReason`` literals. Order is fixed
        (schedule_missed → watermark_gap → queue_stuck →
        mid_flight_stuck) so the FE renders chips in a stable
        sequence.
    """
    reasons: list[StaleReason] = []

    # Rule 1: schedule_missed — overdue by more than a full cadence cycle
    # (C1, #1508). The adapter anchors ``expected_fire_at`` on the
    # terminal run's ``max(started_at, finished_at)``, so a run that just
    # finished resets the clock. A single late tick no longer fires; an
    # entire skipped cycle does. The FLOOR keeps sub-cycle jobs
    # (every-5-min) from flapping. Negative when the job is currently
    # running (overlap-suppression is intentional, not a miss).
    overdue_threshold = max(cadence_period_s, SCHEDULE_MISS_FLOOR_S)
    if (
        mechanism == "scheduled_job"
        and status != "running"
        and expected_fire_at is not None
        and expected_fire_at < now - _seconds(overdue_threshold)
    ):
        reasons.append("schedule_missed")

    # Rule 2: watermark_gap — scheduled_job + ingest_sweep with a
    # freshness source. Negative when actively running. Bootstrap has
    # no freshness source so the adapter passes
    # has_data_freshness_gap=False (defensive: even if the caller
    # forgot, mechanism gate skips it).
    if mechanism in ("scheduled_job", "ingest_sweep") and status != "running" and has_data_freshness_gap:
        reasons.append("watermark_gap")

    # Rule 3: queue_stuck — all mechanisms. The probe is keyed on
    # ``process_id`` so sweeps (which never insert pending_job_requests
    # rows in v1) trivially return False. Keeping the call uniform
    # future-proofs against v2 sweep-trigger plumbing.
    if has_dispatched_queue_age:
        reasons.append("queue_stuck")

    # Rule 4: mid_flight_stuck — only when running. Heartbeat is
    # ``COALESCE(last_progress_at, started_at)``: producers that have
    # not yet emitted their first tick fall back to the run start.
    # Without the fallback, a worker that crashes before its first
    # record_processed would silently never surface as stale.
    if status == "running":
        threshold_s = get_threshold(process_id)
        heartbeat = last_progress_at or active_run_started_at
        if heartbeat is not None and heartbeat < now - _seconds(threshold_s):
            reasons.append("mid_flight_stuck")

    return tuple(reasons)


def _seconds(n: int) -> timedelta:
    """Return ``timedelta(seconds=n)`` — wrapped so the rule shape
    reads ``now - _seconds(60)`` left-to-right.
    """
    return timedelta(seconds=n)


__all__ = [
    "QUEUE_STUCK_THRESHOLD_S",
    "SCHEDULE_MISS_FLOOR_S",
    "SCHEDULE_MISS_TOLERANCE_S",
    "WATERMARK_GAP_TOLERANCE_S",
    "compute",
]
