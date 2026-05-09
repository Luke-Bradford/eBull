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
SCHEDULE_MISS_TOLERANCE_S: Final[int] = 60

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
            ``SCHEDULE_MISS_TOLERANCE_S`` in the past — i.e., we should
            have fired again by now and didn't. Computed from
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

    Returns:
        Ordered tuple of ``StaleReason`` literals. Order is fixed
        (schedule_missed → watermark_gap → queue_stuck →
        mid_flight_stuck) so the FE renders chips in a stable
        sequence.
    """
    reasons: list[StaleReason] = []

    # Rule 1: schedule_missed — scheduled_job only. ``expected_fire_at``
    # is the first cadence-occurrence after the latest terminal run; if
    # it's now > tolerance seconds in the past we should have fired
    # again by now and didn't. Negative when the job is currently
    # running (overlap-suppression is intentional, not a miss).
    if (
        mechanism == "scheduled_job"
        and status != "running"
        and expected_fire_at is not None
        and expected_fire_at < now - _seconds(SCHEDULE_MISS_TOLERANCE_S)
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
    "SCHEDULE_MISS_TOLERANCE_S",
    "WATERMARK_GAP_TOLERANCE_S",
    "compute",
]
