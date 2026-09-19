"""Pure-logic tests for the five-case stale model (PR8 / #1083; rule 5 #2274).

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
    RUNTIME_CEILING_S,
    SCHEDULE_MISS_FLOOR_S,
    SCHEDULE_MISS_TOLERANCE_S,
    WATERMARK_GAP_TOLERANCE_S,
    compute,
)
from app.services.processes.stale_thresholds import (
    DEFAULT_THRESHOLD_S,
    get_threshold,
)

NOW = datetime(2026, 5, 9, 12, 0, 0, tzinfo=UTC)

_DAILY_S = 86_400


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
            expected_fire_at=_seconds_ago(0) + timedelta(minutes=5),  # future fire
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
            expected_fire_at=_seconds_ago(120),  # past fire — but row is running
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


def test_schedule_missed_fires_when_overdue_by_a_full_cycle() -> None:
    """C1 (#1508): fires only when overdue by more than a whole cadence
    cycle — here a daily job overdue by >1 day."""
    reasons = compute(
        mechanism="scheduled_job",
        status="ok",
        expected_fire_at=_seconds_ago(_DAILY_S + 60),
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=None,
        process_id="some_job",
        now=NOW,
        cadence_period_s=_DAILY_S,
    )
    assert reasons == ("schedule_missed",)


def test_schedule_missed_does_not_fire_within_one_cycle() -> None:
    """C1: a single late tick (overdue by less than a cadence cycle)
    is no longer a miss — only a whole skipped cycle fires."""
    reasons = compute(
        mechanism="scheduled_job",
        status="ok",
        expected_fire_at=_seconds_ago(_DAILY_S - 1),
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=None,
        process_id="some_job",
        now=NOW,
        cadence_period_s=_DAILY_S,
    )
    assert "schedule_missed" not in reasons


def test_schedule_missed_suppressed_while_running() -> None:
    """A scheduled job that's still running has not "missed" its fire —
    overlap-suppression is intentional."""
    reasons = compute(
        mechanism="scheduled_job",
        status="running",
        expected_fire_at=_seconds_ago(SCHEDULE_MISS_TOLERANCE_S + 600),
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
        expected_fire_at=None,
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
        expected_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=None,
        process_id="sec_form4_sweep",
        now=NOW,
    )
    assert "schedule_missed" not in reasons


# C1 (#1508) — full-cycle threshold + sub-cycle floor. These three pin
# the behavioural contract: within one cycle is benign, a whole skipped
# cycle fires, and the 300s floor protects fast (every-5-min) jobs.


def test_within_one_cycle_is_not_schedule_missed() -> None:
    # expected fire was 2h ago — well under a daily cycle → NOT missed.
    assert "schedule_missed" not in compute(
        mechanism="scheduled_job",
        status="ok",
        expected_fire_at=NOW - timedelta(hours=2),
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=None,
        process_id="x",
        now=NOW,
        cadence_period_s=_DAILY_S,
    )


def test_overdue_by_more_than_a_cycle_is_schedule_missed() -> None:
    # expected fire was 25h ago — past a full daily cycle → missed.
    assert "schedule_missed" in compute(
        mechanism="scheduled_job",
        status="ok",
        expected_fire_at=NOW - timedelta(hours=25),
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=None,
        process_id="x",
        now=NOW,
        cadence_period_s=_DAILY_S,
    )


def test_floor_protects_every_5min_jobs() -> None:
    # 5-min cadence, expected 3 min ago — under the 300s FLOOR → not missed.
    assert "schedule_missed" not in compute(
        mechanism="scheduled_job",
        status="ok",
        expected_fire_at=NOW - timedelta(minutes=3),
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=None,
        process_id="x",
        now=NOW,
        cadence_period_s=300,
    )


def test_schedule_miss_floor_constant_is_300() -> None:
    assert SCHEDULE_MISS_FLOOR_S == 300


# ---------------------------------------------------------------------------
# watermark_gap
# ---------------------------------------------------------------------------


def test_watermark_gap_fires_for_scheduled_job() -> None:
    reasons = compute(
        mechanism="scheduled_job",
        status="ok",
        expected_fire_at=NOW + timedelta(minutes=5),
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
        expected_fire_at=None,
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
        expected_fire_at=None,
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
        expected_fire_at=None,
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
            expected_fire_at=None,
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
        expected_fire_at=None,
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
        expected_fire_at=None,
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
        expected_fire_at=None,
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
        expected_fire_at=None,
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
        expected_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=_seconds_ago(threshold_s + 60),
        active_run_started_at=_seconds_ago(threshold_s + 120),
        process_id="bootstrap",
        now=NOW,
    )
    assert "mid_flight_stuck" in reasons_over


def test_mid_flight_stuck_does_not_fire_without_an_active_run() -> None:
    """A terminated row is not stale — but the thing that makes it
    terminated is the ABSENCE OF AN ACTIVE RUN, not its ``status``.

    ⚠ #2274: this test used to assert the same intent via
    ``status in ("failed", "cancelled", "ok")`` while passing a non-None
    ``active_run_started_at``. That is a combination ``_status_for`` cannot
    construct (with ``active_row`` present it returns only ``running`` or
    ``disabled``), so the test froze a defensive contract on an unreachable
    input — and it was the same ``status``-coupling that let the kill switch
    mask rule 4 entirely. Rule 5 has always fired on that input. The
    reachable contract is the one asserted here.
    """
    for status in ("failed", "cancelled", "ok"):
        reasons = compute(
            mechanism="scheduled_job",
            status=status,  # type: ignore[arg-type]
            expected_fire_at=None,
            has_data_freshness_gap=False,
            has_dispatched_queue_age=False,
            last_progress_at=None,
            active_run_started_at=None,
            process_id="some_job",
            now=NOW,
        )
        assert "mid_flight_stuck" not in reasons


def test_mid_flight_stuck_fires_while_halted_with_a_heartbeat() -> None:
    """#2274 REGRESSION — the kill switch must not mask a wedged run.

    ``scheduled_adapter._status_for`` returns ``disabled`` BEFORE it looks at
    ``has_running_row``, so every row on a halted system carries
    ``status="disabled"`` even with a run in flight. Rule 4 was gated on
    ``status == "running"`` and therefore could not fire from 2026-06-28
    (kill switch on) onward — which silently removed #1689 §Decision 4's
    entire hung-job answer, and made ``health_verdict._WEDGE_STALE``'s
    ``mid_flight_stuck`` membership dead code.
    """
    reasons = compute(
        mechanism="scheduled_job",
        status="disabled",
        expected_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=_seconds_ago(DEFAULT_THRESHOLD_S + 60),
        active_run_started_at=_seconds_ago(DEFAULT_THRESHOLD_S * 3),
        process_id="some_job",
        now=NOW,
    )
    assert "mid_flight_stuck" in reasons


def test_mid_flight_stuck_fires_while_halted_on_the_started_at_fallback() -> None:
    """#1689's own case (no heartbeat at all), under the halt.

    Of the 63 jobs rule 4 evaluates exactly one (``thesis_refresh``) reaches
    ``report_progress``, so the fallback — not the heartbeat — is the path
    almost every real wedge would take.
    """
    reasons = compute(
        mechanism="scheduled_job",
        status="disabled",
        expected_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=_seconds_ago(DEFAULT_THRESHOLD_S + 30),
        process_id="some_job",
        now=NOW,
    )
    assert "mid_flight_stuck" in reasons


def test_mid_flight_stuck_fires_for_halted_bootstrap_without_a_heartbeat() -> None:
    """Bootstrap has NO other hung-run signal — rule 5 excludes it by design
    ("no cadence to bound a ceiling against"), and ``bootstrap_adapter``'s own
    comment says only ``queue_stuck`` + ``mid_flight_stuck`` are reachable.
    So the halt mask left a wedged install with nothing at all.
    """
    reasons = compute(
        mechanism="bootstrap",
        status="disabled",
        expected_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=_seconds_ago(get_threshold("bootstrap") + 60),
        process_id="bootstrap",
        now=NOW,
    )
    assert reasons == ("mid_flight_stuck",)


def test_mid_flight_stuck_fresh_heartbeat_still_mutes_while_halted() -> None:
    """The fix must not repaint a HEALTHY halted board — a ticking producer
    mutes rule 4 exactly as it does when running.
    """
    fresh = compute(
        mechanism="scheduled_job",
        status="disabled",
        expected_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=_seconds_ago(10),
        active_run_started_at=_seconds_ago(DEFAULT_THRESHOLD_S * 20),
        process_id="some_job",
        now=NOW,
    )
    assert "mid_flight_stuck" not in fresh


def test_mid_flight_stuck_honours_per_process_override_while_halted() -> None:
    """The 1800s override is consulted on the halted path too — a slow-tick
    ingester quiet for 1000s is fine, 2000s is not.
    """
    pid = "sec_filing_documents_ingest"
    assert get_threshold(pid) == 1800, "fixture assumes the shipped override"
    for quiet_s, expected in ((1000, False), (2000, True)):
        reasons = compute(
            mechanism="scheduled_job",
            status="disabled",
            expected_fire_at=None,
            has_data_freshness_gap=False,
            has_dispatched_queue_age=False,
            last_progress_at=_seconds_ago(quiet_s),
            active_run_started_at=_seconds_ago(quiet_s + 60),
            process_id=pid,
            now=NOW,
        )
        assert ("mid_flight_stuck" in reasons) is expected, f"quiet_s={quiet_s}"


def test_mid_flight_stuck_at_exact_threshold_does_not_fire() -> None:
    """Strictly less-than. ``last_progress_at == now - threshold`` is
    on-the-edge fresh."""
    reasons = compute(
        mechanism="scheduled_job",
        status="running",
        expected_fire_at=None,
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
        expected_fire_at=_seconds_ago(_DAILY_S + 30),
        has_data_freshness_gap=True,
        has_dispatched_queue_age=True,
        last_progress_at=None,
        active_run_started_at=None,
        process_id="some_job",
        now=NOW,
        cadence_period_s=_DAILY_S,
    )
    assert reasons == ("schedule_missed", "watermark_gap", "queue_stuck")


def test_running_row_can_fire_queue_and_midflight_simultaneously() -> None:
    """Running can overlap with queue_stuck (a stale dispatched row
    pre-dates the live run) and mid_flight_stuck — but never with
    schedule_missed or watermark_gap."""
    reasons = compute(
        mechanism="scheduled_job",
        status="running",
        expected_fire_at=_seconds_ago(SCHEDULE_MISS_TOLERANCE_S + 30),
        has_data_freshness_gap=True,
        has_dispatched_queue_age=True,
        last_progress_at=_seconds_ago(DEFAULT_THRESHOLD_S + 60),
        active_run_started_at=_seconds_ago(DEFAULT_THRESHOLD_S + 120),
        process_id="some_job",
        now=NOW,
    )
    assert reasons == ("queue_stuck", "mid_flight_stuck")


# ---------------------------------------------------------------------------
# Rule 5: runtime_ceiling (#2274)
# ---------------------------------------------------------------------------


def test_runtime_ceiling_fires_when_run_age_exceeds_ceiling() -> None:
    reasons = compute(
        mechanism="scheduled_job",
        status="running",
        expected_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=_seconds_ago(RUNTIME_CEILING_S + 60),
        process_id="some_job",
        now=NOW,
    )
    assert "runtime_ceiling" in reasons


def test_runtime_ceiling_does_not_fire_under_the_ceiling() -> None:
    """Revert-probe: drop the age comparison in rule 5 and this fails."""
    reasons = compute(
        mechanism="scheduled_job",
        status="running",
        expected_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=_seconds_ago(RUNTIME_CEILING_S - 60),
        process_id="some_job",
        now=NOW,
    )
    assert "runtime_ceiling" not in reasons


def test_runtime_ceiling_at_exact_ceiling_does_not_fire() -> None:
    """Strictly-older-than, consistent with rules 1/2/4's boundaries."""
    reasons = compute(
        mechanism="scheduled_job",
        status="running",
        expected_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=_seconds_ago(RUNTIME_CEILING_S),
        process_id="some_job",
        now=NOW,
    )
    assert "runtime_ceiling" not in reasons


def test_runtime_ceiling_is_not_muted_by_a_fresh_heartbeat() -> None:
    """⚠⚠ The property the whole rule exists for (#2274).

    ``mid_flight_stuck`` is muted the instant a producer ticks. Once
    #2274's heartbeat writer lands, a job that ticks forever would read
    *Working* forever and rule 4's stand-in coverage would INVERT. Rule 5
    measures the run's AGE and nothing else, so a heartbeat one second old
    cannot suppress it — that is what makes the heartbeat safe to switch on.
    """
    reasons = compute(
        mechanism="scheduled_job",
        status="running",
        expected_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=_seconds_ago(1),  # ticking healthily, right now
        active_run_started_at=_seconds_ago(RUNTIME_CEILING_S + 60),
        process_id="some_job",
        now=NOW,
    )
    assert reasons == ("runtime_ceiling",)
    assert "mid_flight_stuck" not in reasons


def test_runtime_ceiling_does_not_fire_without_an_active_run() -> None:
    """The gate is the presence of an active run, not the display status:
    every non-running status reaches ``compute`` with
    ``active_run_started_at=None`` (the adapter builds ``active_run`` from the
    running row, and there is none)."""
    for status in ("ok", "idle", "failed", "disabled", "pending_retry"):
        reasons = compute(
            mechanism="scheduled_job",
            status=status,  # type: ignore[arg-type]
            expected_fire_at=None,
            has_data_freshness_gap=False,
            has_dispatched_queue_age=False,
            last_progress_at=None,
            active_run_started_at=None,
            process_id="some_job",
            now=NOW,
        )
        assert "runtime_ceiling" not in reasons, status


def test_runtime_ceiling_still_fires_when_the_kill_switch_reads_disabled() -> None:
    """⚠⚠ Codex ckpt-2. ``scheduled_adapter._status_for`` returns ``disabled``
    FIRST when the kill switch is on — before it looks at ``has_running_row``
    — so a ``status == 'running'`` gate would make this reason unreachable on
    a halted system, and its ``_WEDGE_STALE`` membership dead code. Halting
    the SCHEDULE does not end a run that started before the halt.
    """
    reasons = compute(
        mechanism="scheduled_job",
        status="disabled",
        expected_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=_seconds_ago(RUNTIME_CEILING_S + 60),
        process_id="some_job",
        now=NOW,
    )
    assert "runtime_ceiling" in reasons


def test_runtime_ceiling_is_scheduled_job_only() -> None:
    """Bootstrap is excluded deliberately: a one-time install drives 17
    stages including multi-GB archive seeds, has no cadence to bound a
    ceiling against, and already carries a 1,800s mid_flight_stuck
    override. ``ingest_sweep`` passes ``active_run_started_at=None`` in
    production, but the mechanism gate is asserted directly so the
    exclusion survives an adapter change."""
    for mechanism in ("bootstrap", "ingest_sweep"):
        reasons = compute(
            mechanism=mechanism,  # type: ignore[arg-type]
            status="running",
            expected_fire_at=None,
            has_data_freshness_gap=False,
            has_dispatched_queue_age=False,
            last_progress_at=None,
            active_run_started_at=_seconds_ago(RUNTIME_CEILING_S + 60),
            process_id="bootstrap",
            now=NOW,
        )
        assert "runtime_ceiling" not in reasons, mechanism


def test_runtime_ceiling_sorts_last_in_canonical_order() -> None:
    """The FE renders chips in the order returned."""
    reasons = compute(
        mechanism="scheduled_job",
        status="running",
        expected_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=True,
        last_progress_at=None,
        active_run_started_at=_seconds_ago(RUNTIME_CEILING_S + 60),
        process_id="some_job",
        now=NOW,
    )
    assert reasons == ("queue_stuck", "mid_flight_stuck", "runtime_ceiling")


# ---------------------------------------------------------------------------
# #2274 — the orchestrator wrappers, whose active run comes from sync_runs
# ---------------------------------------------------------------------------


def _wrapper_reasons(process_id: str, *, age_s: int) -> tuple[str, ...]:
    """Rules as they fire for an orchestrator wrapper's in-flight sync run.

    ``last_progress_at=None`` is the common case by a wide margin on
    ``sync_runs`` (most rows never record one), which is exactly why rule 4
    degenerates into a duration rule for these processes.
    """
    return compute(
        mechanism="scheduled_job",
        status="running",
        expected_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=_seconds_ago(age_s),
        process_id=process_id,
        now=NOW,
        cadence_period_s=300,
    )


def test_full_sync_mid_run_is_not_mid_flight_stuck() -> None:
    """A full sync well past the 300 s default is WORKING, not stuck.

    The measured shape this defends: on ``sync_runs`` scope='full' the large
    majority of runs that COMPLETED SUCCESSFULLY would have tripped rule 4 at
    the default. Surfacing the active run without this would have turned most
    healthy full syncs red — i.e. routed a sync run into the threshold model
    ``run_liveness`` deliberately refuses.
    """
    assert _wrapper_reasons("orchestrator_full_sync", age_s=DEFAULT_THRESHOLD_S * 10) == ()


def test_full_sync_past_the_ceiling_still_chips() -> None:
    """The exemption is a RAISED THRESHOLD, not a removed rule. Past the
    ceiling both rule 4 and rule 5 fire — deliberately, since for a run with no
    heartbeat the two are measuring the same instant and silence about a
    day-old sync would be the worse error."""
    reasons = _wrapper_reasons("orchestrator_full_sync", age_s=RUNTIME_CEILING_S + 1)
    assert "runtime_ceiling" in reasons
    assert "mid_flight_stuck" in reasons


def test_high_frequency_sync_keeps_the_default_threshold() -> None:
    """⚠ The asymmetry, asserted rather than assumed.

    The HF wrapper reads the same table through the same code, so an exemption
    keyed on the RUN KIND would have covered it too. It must not: its whole
    active window is a fraction of the 300 s default, so a run silent past the
    default is genuinely stranded — and with the row now reading ``running``,
    rule 1 no longer covers that case.
    """
    assert _wrapper_reasons("orchestrator_high_frequency_sync", age_s=DEFAULT_THRESHOLD_S + 1) == ("mid_flight_stuck",)


def test_bootstrap_still_fires_rule_4_at_its_own_override() -> None:
    """Regression guard for the design this ticket did NOT ship (Codex ckpt-1).

    Rev 1 proposed exempting rule 4 via a new ``active_run_kind`` parameter on
    ``compute``. Bootstrap passes a real ``active_run_started_at`` and rule 5
    excludes bootstrap by design, so rule 4 is bootstrap's ONLY hung-run
    detector — a signature change there is the kind of thing that silently
    disarms it. Shipping the fix as a threshold override instead means
    ``compute`` is untouched, and this test pins that.
    """
    reasons = compute(
        mechanism="bootstrap",
        status="running",
        expected_fire_at=None,
        has_data_freshness_gap=False,
        has_dispatched_queue_age=False,
        last_progress_at=None,
        active_run_started_at=_seconds_ago(get_threshold("bootstrap") + 1),
        process_id="bootstrap",
        now=NOW,
    )
    assert reasons == ("mid_flight_stuck",)
