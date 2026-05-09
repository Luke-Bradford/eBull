"""Pure-logic tests for the four-case stale model (PR8 / #1083).

Issue #1083 (umbrella #1064) — admin control hub PR8.
Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §A1 (operator-amendment round 1, line 11-22).

These tests pin the ordering, mechanism gates, and boundary semantics
of ``stale_detection.compute()``. The function is pure (no DB) so the
suite is fast and exercises every reason path the adapters can route
through.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.services.processes.stale_detection import (
    QUEUE_STUCK_THRESHOLD_S,
    SCHEDULE_MISS_TOLERANCE_S,
    WATERMARK_GAP_TOLERANCE_S,
    compute,
)
from app.services.processes.stale_thresholds import (
    DEFAULT_THRESHOLD_S,
    get_threshold,
)

NOW = datetime(2026, 5, 9, 12, 0, 0, tzinfo=UTC)


def _seconds_ago(n: int) -> datetime:
    return NOW - timedelta(seconds=n)


# ---------------------------------------------------------------------------
# Empty / not-stale baselines
# ---------------------------------------------------------------------------


def test_idle_row_with_no_signals_is_not_stale() -> None:
    assert (
        compute(
            mechanism="scheduled_job",
            status="idle",
            next_fire_at=_seconds_ago(0) + timedelta(minutes=5),  # future fire
            has_data_freshness_gap=False,
            has_dispatched_queue_age=False,
            last_progress_at=None,
            active_run_started_at=None,
            process_id="some_job",
            now=NOW,
        )
        == ()
    )


def test_running_row_with_fresh_heartbeat_is_not_stale() -> None:
    assert (
        compute(
            mechanism="scheduled_job",
            status="running",
            next_fire_at=_seconds_ago(120),  # past fire — but row is running
            has_data_freshness_gap=False,
            has_dispatched_queue_age=False,
            last_progress_at=_seconds_ago(30),
            active_run_started_at=_seconds_ago(120),
            process_id="some_job",
            now=NOW,
        )
        == ()
    )


# ---------------------------------------------------------------------------
# schedule_missed
# ---------------------------------------------------------------------------


def test_schedule_missed_fires_when_next_fire_is_past_tolerance() -> None:
    reasons = compute(
        mechanism="scheduled_job",
        status="ok",
        next_fire_at=_seconds_ago(SCHEDULE_MISS_TOLERANCE_S + 10),
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=None,
        process_id="some_job",
        now=NOW,
    )
    assert reasons == ("schedule_missed",)


def test_schedule_missed_does_not_fire_within_tolerance() -> None:
    """Exact-tolerance boundary: APScheduler jitter should not surface."""
    reasons = compute(
        mechanism="scheduled_job",
        status="ok",
        next_fire_at=_seconds_ago(SCHEDULE_MISS_TOLERANCE_S - 1),
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=None,
        process_id="some_job",
        now=NOW,
    )
    assert "schedule_missed" not in reasons


def test_schedule_missed_suppressed_while_running() -> None:
    """A scheduled job that's still running has not "missed" its fire —
    overlap-suppression is intentional."""
    reasons = compute(
        mechanism="scheduled_job",
        status="running",
        next_fire_at=_seconds_ago(SCHEDULE_MISS_TOLERANCE_S + 600),
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=_seconds_ago(30),
        active_run_started_at=_seconds_ago(SCHEDULE_MISS_TOLERANCE_S + 600),
        process_id="some_job",
        now=NOW,
    )
    assert "schedule_missed" not in reasons


def test_bootstrap_never_schedule_misses() -> None:
    """Bootstrap is on-demand — no cron fire to miss."""
    reasons = compute(
        mechanism="bootstrap",
        status="ok",
        next_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=None,
        process_id="bootstrap",
        now=NOW,
    )
    assert "schedule_missed" not in reasons


def test_ingest_sweep_never_schedule_misses() -> None:
    """Sweeps have no own cron — the underlying scheduled_job carries
    the schedule, so the sweep row should never schedule_miss."""
    reasons = compute(
        mechanism="ingest_sweep",
        status="ok",
        next_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=None,
        process_id="sec_form4_sweep",
        now=NOW,
    )
    assert "schedule_missed" not in reasons


# ---------------------------------------------------------------------------
# watermark_gap
# ---------------------------------------------------------------------------


def test_watermark_gap_fires_for_scheduled_job() -> None:
    reasons = compute(
        mechanism="scheduled_job",
        status="ok",
        next_fire_at=NOW + timedelta(minutes=5),
        has_data_freshness_gap=True,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=None,
        process_id="some_job",
        now=NOW,
    )
    assert reasons == ("watermark_gap",)


def test_watermark_gap_fires_for_ingest_sweep() -> None:
    reasons = compute(
        mechanism="ingest_sweep",
        status="ok",
        next_fire_at=None,
        has_data_freshness_gap=True,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=None,
        process_id="sec_form4_sweep",
        now=NOW,
    )
    assert reasons == ("watermark_gap",)


def test_watermark_gap_suppressed_while_running() -> None:
    """Source has fresh data; we are actively processing it — not stale."""
    reasons = compute(
        mechanism="scheduled_job",
        status="running",
        next_fire_at=None,
        has_data_freshness_gap=True,
        has_dispatched_queue_age=False,
        last_progress_at=_seconds_ago(10),
        active_run_started_at=_seconds_ago(60),
        process_id="some_job",
        now=NOW,
    )
    assert "watermark_gap" not in reasons


def test_bootstrap_never_watermark_gaps() -> None:
    """Bootstrap has no data_freshness_index row; the rule is gated on
    mechanism so even if the caller passed True, it must not fire."""
    reasons = compute(
        mechanism="bootstrap",
        status="ok",
        next_fire_at=None,
        has_data_freshness_gap=True,  # defensively set; gate must skip
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=None,
        process_id="bootstrap",
        now=NOW,
    )
    assert "watermark_gap" not in reasons


# Tolerance constant is exported for adapter use; test importable + numeric.
def test_watermark_gap_tolerance_constant_exposed() -> None:
    assert WATERMARK_GAP_TOLERANCE_S == 60


# ---------------------------------------------------------------------------
# queue_stuck
# ---------------------------------------------------------------------------


def test_queue_stuck_fires_for_all_mechanisms_when_signal_set() -> None:
    for mechanism in ("bootstrap", "scheduled_job", "ingest_sweep"):
        reasons = compute(
            mechanism=mechanism,  # type: ignore[arg-type]
            status="idle",
            next_fire_at=None,
            has_data_freshness_gap=False,
            has_dispatched_queue_age=True,
            last_progress_at=None,
            active_run_started_at=None,
            process_id=f"pid_for_{mechanism}",
            now=NOW,
        )
        assert reasons == ("queue_stuck",)


def test_queue_stuck_threshold_constant_is_30_minutes() -> None:
    assert QUEUE_STUCK_THRESHOLD_S == 30 * 60


# ---------------------------------------------------------------------------
# mid_flight_stuck
# ---------------------------------------------------------------------------


def test_mid_flight_stuck_fires_when_heartbeat_past_threshold() -> None:
    reasons = compute(
        mechanism="scheduled_job",
        status="running",
        next_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=_seconds_ago(DEFAULT_THRESHOLD_S + 30),
        active_run_started_at=_seconds_ago(DEFAULT_THRESHOLD_S + 60),
        process_id="some_job",
        now=NOW,
    )
    assert reasons == ("mid_flight_stuck",)


def test_mid_flight_stuck_fallback_to_started_at_when_no_heartbeat() -> None:
    """Codex pre-impl review BLOCKING: a worker that crashes before its
    first ``record_processed`` would otherwise never surface as stale.
    Fall back to ``started_at`` so that case is still caught."""
    reasons = compute(
        mechanism="scheduled_job",
        status="running",
        next_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=_seconds_ago(DEFAULT_THRESHOLD_S + 30),
        process_id="some_job",
        now=NOW,
    )
    assert reasons == ("mid_flight_stuck",)


def test_mid_flight_stuck_no_false_positive_on_first_tick_lag() -> None:
    """A run that just started (started_at within threshold) and has
    not yet recorded its first tick is NOT stale — first-tick lag is
    benign on unbounded jobs (PR8 spec gotcha §A1 / first-tick)."""
    reasons = compute(
        mechanism="scheduled_job",
        status="running",
        next_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=_seconds_ago(5),
        process_id="some_job",
        now=NOW,
    )
    assert "mid_flight_stuck" not in reasons


def test_mid_flight_stuck_uses_per_process_threshold_override() -> None:
    """Bootstrap's 30-min override means a 6-min stale heartbeat does
    NOT fire mid_flight_stuck — but a 31-min one does."""
    threshold_s = get_threshold("bootstrap")
    assert threshold_s == 1800

    # 6 min — under bootstrap's 30-min override; not stale.
    reasons_under = compute(
        mechanism="bootstrap",
        status="running",
        next_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=_seconds_ago(360),
        active_run_started_at=_seconds_ago(600),
        process_id="bootstrap",
        now=NOW,
    )
    assert "mid_flight_stuck" not in reasons_under

    # 31 min — past bootstrap's 30-min override; stale.
    reasons_over = compute(
        mechanism="bootstrap",
        status="running",
        next_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=_seconds_ago(threshold_s + 60),
        active_run_started_at=_seconds_ago(threshold_s + 120),
        process_id="bootstrap",
        now=NOW,
    )
    assert "mid_flight_stuck" in reasons_over


def test_mid_flight_stuck_does_not_fire_on_terminal_status() -> None:
    """A failed/cancelled row with an old heartbeat is not stale —
    the run terminated. mid_flight_stuck is gated on status='running'."""
    for status in ("failed", "cancelled", "ok"):
        reasons = compute(
            mechanism="scheduled_job",
            status=status,  # type: ignore[arg-type]
            next_fire_at=None,
            has_data_freshness_gap=False,
            has_dispatched_queue_age=False,
            last_progress_at=_seconds_ago(DEFAULT_THRESHOLD_S * 10),
            active_run_started_at=_seconds_ago(DEFAULT_THRESHOLD_S * 11),
            process_id="some_job",
            now=NOW,
        )
        assert "mid_flight_stuck" not in reasons


def test_mid_flight_stuck_at_exact_threshold_does_not_fire() -> None:
    """Strictly less-than. ``last_progress_at == now - threshold`` is
    on-the-edge fresh."""
    reasons = compute(
        mechanism="scheduled_job",
        status="running",
        next_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=_seconds_ago(DEFAULT_THRESHOLD_S),
        active_run_started_at=_seconds_ago(DEFAULT_THRESHOLD_S + 60),
        process_id="some_job",
        now=NOW,
    )
    assert "mid_flight_stuck" not in reasons


# ---------------------------------------------------------------------------
# Ordering / multiplicity
# ---------------------------------------------------------------------------


def test_multiple_reasons_fire_in_canonical_order() -> None:
    """The FE renders chips in the order returned, so adapters must
    surface a deterministic order: schedule_missed → watermark_gap →
    queue_stuck → mid_flight_stuck."""
    reasons = compute(
        mechanism="scheduled_job",
        status="ok",  # not running — schedule_missed + watermark_gap can fire
        next_fire_at=_seconds_ago(SCHEDULE_MISS_TOLERANCE_S + 30),
        has_data_freshness_gap=True,
        has_dispatched_queue_age=True,
        last_progress_at=None,
        active_run_started_at=None,
        process_id="some_job",
        now=NOW,
    )
    assert reasons == ("schedule_missed", "watermark_gap", "queue_stuck")


def test_running_row_can_fire_queue_and_midflight_simultaneously() -> None:
    """Running can overlap with queue_stuck (a stale dispatched row
    pre-dates the live run) and mid_flight_stuck — but never with
    schedule_missed or watermark_gap."""
    reasons = compute(
        mechanism="scheduled_job",
        status="running",
        next_fire_at=_seconds_ago(SCHEDULE_MISS_TOLERANCE_S + 30),
        has_data_freshness_gap=True,
        has_dispatched_queue_age=True,
        last_progress_at=_seconds_ago(DEFAULT_THRESHOLD_S + 60),
        active_run_started_at=_seconds_ago(DEFAULT_THRESHOLD_S + 120),
        process_id="some_job",
        now=NOW,
    )
    assert reasons == ("queue_stuck", "mid_flight_stuck")
