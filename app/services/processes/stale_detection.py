"""Pure-logic five-case stale model.

Issue #1083 (umbrella #1064) — admin control hub PR8.
Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §A1 (operator-amendment round 1, line 11-22) — supersedes the
      legacy §"Stale-detection rule" (line 597-606) v0 sketch.

Five reasons can fire on one row simultaneously:

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
4. ``mid_flight_stuck`` — an ACTIVE RUN exists AND
   ``COALESCE(active_run.last_progress_at, active_run.started_at) <
   now() - threshold``. Falling back to ``started_at`` covers the
   "stuck before first tick" case (Codex pre-impl review BLOCKING) —
   without it, a worker that crashes before its first
   ``record_processed`` would never surface as stale, and bootstrap
   would have no hung-run signal at all (rule 5 excludes it).
   ⚠⚠ Gated on the active run, NOT on ``status``: a ``status ==
   "running"`` gate is unsatisfiable while the kill switch is on,
   because ``_status_for`` returns ``disabled`` first. See the inline
   comment on the rule — that masked #1689's whole hung-job answer for
   79 days.
5. ``runtime_ceiling`` (#2274) — ``mechanism="scheduled_job"`` only.
   An ACTIVE RUN exists AND ``active_run.started_at < now() -
   RUNTIME_CEILING_S``. Rule 4's sibling, and the difference is the
   whole point: **this one never consults the heartbeat**. Rule 4 is
   muted the moment a producer ticks, so once #2274's heartbeat lands a
   job that ticks forever would read *Working* forever; rule 5 measures
   the run's AGE and nothing else, so it cannot be muted. That is what
   makes the heartbeat safe to have — see
   ``docs/proposals/ops/2026-09-15-2274-job-runtime-ceiling.md``.

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
from app.services.processes.stale_thresholds import RUNTIME_CEILING_S, get_threshold

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

# ``RUNTIME_CEILING_S`` (rule 5's whole-run wall-clock ceiling) is DEFINED in
# ``stale_thresholds`` and re-exported here. It moved there so
# ``_OVERRIDES["orchestrator_full_sync"]`` can be expressed as that same ceiling
# instead of a second hand-picked number — this module already imports
# ``get_threshold`` from there, so the dependency can only run one way. Existing
# importers (``run_liveness``) keep reading it from this module; the value and
# its measured-evidence comment are unchanged, just relocated.


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
        mid_flight_stuck → runtime_ceiling) so the FE renders chips in
        a stable sequence.
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

    # Rule 4: mid_flight_stuck. Heartbeat is
    # ``COALESCE(last_progress_at, started_at)``: producers that have
    # not yet emitted their first tick fall back to the run start.
    # Without the fallback, a worker that crashes before its first
    # record_processed would silently never surface as stale — and for
    # ``mechanism="bootstrap"`` that fallback is the ONLY hung-run signal
    # there is, because rule 5 excludes bootstrap by design.
    #
    # ⚠⚠ Gated on the ACTIVE RUN, not on ``status`` — the same predicate
    # rule 5 uses, for the same reason, and it took a second occurrence to
    # propagate. ``scheduled_adapter._status_for`` returns ``disabled``
    # FIRST when the kill switch is on, BEFORE it looks at
    # ``has_running_row``, so a ``status == "running"`` gate is
    # unsatisfiable for a halted system. That is not a dark display chip:
    # #1689 §Decision 4 chose THIS rule as its entire hung-job answer
    # ("running_too_long: no new code") over a periodic reaper, so between
    # 2026-06-28 (kill switch on, unattended loop) and the fix there was no
    # hung-job coverage at all, and ``_WEDGE_STALE``'s ``mid_flight_stuck``
    # membership — which exists precisely to keep this reason red under the
    # switch — was dead code. The regression test #1689 asked for was
    # written with ``status="running"`` and so passed throughout.
    # ⚠ ``active_run_started_at is not None`` already means "a run is in
    # flight": the adapters build ``active_run`` from the running row
    # regardless of status, and every row without one reaches here with
    # ``None``.
    if active_run_started_at is not None:
        threshold_s = get_threshold(process_id)
        heartbeat = last_progress_at or active_run_started_at
        if heartbeat < now - _seconds(threshold_s):
            reasons.append("mid_flight_stuck")

    # Rule 5: runtime_ceiling (#2274) — the run's AGE, and nothing else.
    #
    # ⚠⚠ Deliberately does NOT consult ``last_progress_at``. Rule 4 above is
    # muted the moment a producer ticks, so once #2274's heartbeat writer
    # lands, a job that ticks forever reads *Working* forever and rule 4's
    # current stand-in coverage INVERTS. Rule 5 cannot be muted by a
    # heartbeat, which is the property that makes the heartbeat safe to
    # switch on (predecessor doc §3).
    #
    # ⚠ Scheduled jobs only. ``ingest_sweep`` passes
    # ``active_run_started_at=None`` by construction (sweeps own no active
    # run). ``bootstrap`` is excluded on purpose: a one-time install drives
    # 17 stages including multi-GB archive seeds, has no cadence to bound a
    # ceiling against and no measured duration distribution, and already
    # carries a mid_flight_stuck signal via its 1,800s threshold override.
    #
    # ⚠ This does NOT terminalise the run. ``job_runs.status='running'`` is a
    # mutual-exclusion signal, not a display state — ``_has_active_job_run``
    # (app/api/processes.py) refuses a manual trigger while it is set, so a
    # full wash cannot "reset watermarks under the running worker's feet". A
    # ceiling that rewrote the status would unlock that guard while the
    # worker thread is still alive, and Python cannot kill that thread.
    # ⚠⚠ Gated on the ACTIVE RUN, not on ``status``, and that is deliberate.
    # ``scheduled_adapter._status_for`` returns ``disabled`` FIRST when the
    # kill switch is on — before it looks at ``has_running_row`` — so a
    # ``status == "running"`` gate would silently never fire for a halted
    # system, and the ``_WEDGE_STALE`` membership that exists to keep this
    # reason red under the kill switch would be dead code (Codex ckpt-2).
    # Halting the SCHEDULE does not end a run that started before the halt,
    # which is exactly when a ceiling breach matters most. ``active_run_
    # started_at is not None`` already means "a run is in flight": the
    # adapter builds ``active_run`` from the running row regardless of
    # status, and every non-running status reaches here with ``None``.
    if mechanism == "scheduled_job" and active_run_started_at is not None:
        if active_run_started_at < now - _seconds(RUNTIME_CEILING_S):
            reasons.append("runtime_ceiling")

    return tuple(reasons)


def _seconds(n: int) -> timedelta:
    """Return ``timedelta(seconds=n)`` — wrapped so the rule shape
    reads ``now - _seconds(60)`` left-to-right.
    """
    return timedelta(seconds=n)


__all__ = [
    "QUEUE_STUCK_THRESHOLD_S",
    "RUNTIME_CEILING_S",
    "SCHEDULE_MISS_FLOOR_S",
    "SCHEDULE_MISS_TOLERANCE_S",
    "WATERMARK_GAP_TOLERANCE_S",
    "compute",
]
