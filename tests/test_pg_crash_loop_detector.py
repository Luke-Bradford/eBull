"""Pure-policy tests for the dev-PG crash-loop detector (D1, #1449).

Only ``evaluate`` is exercised — it is IO-free, so the whole alert/no-alert
matrix runs in microseconds without Docker or a database.
"""

from __future__ import annotations

from scripts.pg_crash_loop_detector import Sample, evaluate

_NOW = 10_000.0
_HEALTHY = "in production"  # any non-recovery pg_controldata state


def _restart_series(counts: list[tuple[float, int]]) -> list[Sample]:
    """Build samples with given (ts_offset_from_now, restart_count)."""
    return [Sample(ts=_NOW - off, restart_count=c, cluster_state=_HEALTHY, redo_lsn=None) for off, c in counts]


def _recovery_series(entries: list[tuple[float, str]]) -> list[Sample]:
    """Build in-recovery samples with given (ts_offset_from_now, redo_lsn)."""
    return [
        Sample(ts=_NOW - off, restart_count=5, cluster_state="in crash recovery", redo_lsn=redo)
        for off, redo in entries
    ]


def test_empty_history_is_healthy() -> None:
    assert evaluate([], now=_NOW) is None


def test_crash_loop_threshold_breached() -> None:
    # +3 restarts over ~12min (inside the 15min window) → alert.
    samples = _restart_series([(720, 4), (480, 5), (240, 6), (0, 7)])
    reason = evaluate(samples, now=_NOW)
    assert reason is not None
    assert "crash-loop" in reason


def test_crash_loop_below_threshold_is_healthy() -> None:
    # Only +2 inside the window → no alert.
    samples = _restart_series([(480, 5), (240, 6), (0, 7)])
    assert evaluate(samples, now=_NOW) is None


def test_old_restarts_outside_window_dont_alert() -> None:
    # The +3 happened >15min ago; current window shows a flat counter.
    samples = _restart_series([(2000, 1), (1900, 2), (1850, 3), (240, 4), (0, 4)])
    assert evaluate(samples, now=_NOW) is None


def test_container_recreation_resets_counter_no_false_positive() -> None:
    # Counter reset to 0 after a recreate, then one restart → min-baseline
    # keeps the delta at +1, not a spurious negative/large value.
    samples = _restart_series([(800, 9), (300, 0), (0, 1)])
    assert evaluate(samples, now=_NOW) is None


def test_recovery_frozen_redo_alerts() -> None:
    # Same redo LSN across 12min of in-recovery samples (> 10min stall) → alert.
    samples = _recovery_series([(720, "1A/D3"), (480, "1A/D3"), (240, "1A/D3"), (0, "1A/D3")])
    reason = evaluate(samples, now=_NOW)
    assert reason is not None
    assert "recovery stalled" in reason
    assert "1A/D3" in reason


def test_recovery_advancing_redo_is_healthy() -> None:
    # Redo LSN advances each sample → recovery is progressing, not wedged.
    samples = _recovery_series([(720, "1A/10"), (480, "1B/20"), (240, "1C/30"), (0, "1D/40")])
    assert evaluate(samples, now=_NOW) is None


def test_recovery_just_started_under_stall_budget_is_healthy() -> None:
    # Frozen, but only for ~4min (< 10min budget) → not yet an alert.
    samples = _recovery_series([(240, "1A/D3"), (120, "1A/D3"), (0, "1A/D3")])
    assert evaluate(samples, now=_NOW) is None


def test_redo_change_resets_the_frozen_span() -> None:
    # Old frozen span ended when redo advanced; the current frozen span is
    # short → no alert (guards against counting a stale frozen prefix).
    # Old 1A/D3 froze for 300s long ago, then redo advanced to 1B/E4 which has
    # only been frozen 300s (< 600s budget) → no alert. Guards against counting
    # a stale frozen prefix from before the redo advanced.
    samples = _recovery_series([(1200, "1A/D3"), (900, "1A/D3"), (300, "1B/E4"), (150, "1B/E4"), (0, "1B/E4")])
    assert evaluate(samples, now=_NOW) is None


def test_thresholds_are_configurable() -> None:
    samples = _recovery_series([(400, "1A/D3"), (0, "1A/D3")])
    # Default 600s stall budget → 400s span is healthy.
    assert evaluate(samples, now=_NOW) is None
    # Tighten to 300s → the same 400s span now alerts.
    assert evaluate(samples, now=_NOW, recovery_stall_s=300.0) is not None
