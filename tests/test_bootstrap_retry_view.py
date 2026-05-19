"""Pure-function tests for ``compute_retryable_view`` (#1136 Phase A.3).

Mirrors the SQL semantics of ``reset_failed_stages_for_retry`` at
``app/services/bootstrap_state.py`` exactly: lane-MIN over failed
stage_orders + ``stage.stage_order >= min_failed_order``, with the
stage's own current status ignored. See
``docs/superpowers/specs/2026-05-19-1136-bootstrap-state-audit.md``
§4.2 for the predicate and the run_id=3 worked example.
"""

from __future__ import annotations

from datetime import UTC, datetime

from app.services.bootstrap_state import (
    BootstrapState,
    RunSnapshot,
    StageRow,
    compute_retryable_view,
)


def _stage(
    stage_key: str,
    stage_order: int,
    lane: str,
    status: str,
    *,
    rows_processed: int | None = None,
    attempt_count: int = 1,
) -> StageRow:
    return StageRow(
        id=stage_order,
        bootstrap_run_id=1,
        stage_key=stage_key,
        stage_order=stage_order,
        lane=lane,  # type: ignore[arg-type]
        job_name="x",
        status=status,  # type: ignore[arg-type]
        started_at=None,
        completed_at=None,
        rows_processed=rows_processed,
        expected_units=None,
        units_done=None,
        last_error=None,
        attempt_count=attempt_count,
    )


def _snap(*stages: StageRow, run_status: str = "partial_error") -> RunSnapshot:
    return RunSnapshot(
        run_id=1,
        run_status=run_status,  # type: ignore[arg-type]
        triggered_at=datetime(2026, 5, 17, tzinfo=UTC),
        completed_at=datetime(2026, 5, 17, 5, 30, tzinfo=UTC),
        stages=tuple(stages),
    )


# ---------------------------------------------------------------------------
# Precedence: blocked-reason short circuits
# ---------------------------------------------------------------------------


def test_no_prior_run() -> None:
    state = BootstrapState(status="pending", last_run_id=None, last_completed_at=None)
    view = compute_retryable_view(state, None)
    assert view.retry_available is False
    assert view.retry_blocked_reason == "no_prior_run"
    assert view.stage_retryable == {}


def test_bootstrap_running() -> None:
    state = BootstrapState(status="running", last_run_id=7, last_completed_at=None)
    snap = _snap(
        _stage("s1", 1, "init", "success"),
        _stage("s2", 2, "sec_rate", "running"),
        run_status="running",
    )
    view = compute_retryable_view(state, snap)
    assert view.retry_available is False
    assert view.retry_blocked_reason == "bootstrap_running"
    # Every stage retryable=False — never advertise retry while running.
    assert all(value is False for value in view.stage_retryable.values())
    assert set(view.stage_retryable) == {"s1", "s2"}


def test_state_not_resettable_complete() -> None:
    state = BootstrapState(
        status="complete",
        last_run_id=7,
        last_completed_at=datetime(2026, 5, 17, tzinfo=UTC),
    )
    snap = _snap(_stage("s1", 1, "init", "success"), run_status="complete")
    view = compute_retryable_view(state, snap)
    assert view.retry_blocked_reason == "state_not_resettable"
    assert view.retry_available is False


def test_state_not_resettable_pending_with_run_id() -> None:
    # Edge case: pending state but last_run_id set (e.g. wipe-then-mark
    # path). No prior failures to reset; should report state_not_resettable
    # because pending is not in the resettable set.
    state = BootstrapState(status="pending", last_run_id=7, last_completed_at=None)
    snap = _snap(_stage("s1", 1, "init", "success"))
    view = compute_retryable_view(state, snap)
    assert view.retry_blocked_reason == "state_not_resettable"


def test_no_failed_stages_with_resettable_state() -> None:
    state = BootstrapState(
        status="partial_error",
        last_run_id=7,
        last_completed_at=datetime(2026, 5, 17, tzinfo=UTC),
    )
    snap = _snap(_stage("s1", 1, "init", "success"))
    view = compute_retryable_view(state, snap)
    assert view.retry_available is False
    assert view.retry_blocked_reason == "no_failed_stages"
    assert view.stage_retryable == {"s1": False}


def test_no_failed_stages_with_no_snapshot() -> None:
    state = BootstrapState(status="partial_error", last_run_id=7, last_completed_at=None)
    view = compute_retryable_view(state, None)
    assert view.retry_blocked_reason == "no_failed_stages"


# ---------------------------------------------------------------------------
# Happy-path retryability
# ---------------------------------------------------------------------------


def test_single_error_one_lane_retryable() -> None:
    state = BootstrapState(
        status="partial_error",
        last_run_id=7,
        last_completed_at=datetime(2026, 5, 17, tzinfo=UTC),
    )
    snap = _snap(
        _stage("init", 1, "init", "success"),
        _stage("sec1", 2, "sec_rate", "error"),
        _stage("db1", 3, "db", "success"),
    )
    view = compute_retryable_view(state, snap)
    assert view.retry_available is True
    assert view.retry_blocked_reason is None
    assert view.stage_retryable == {"init": False, "sec1": True, "db1": False}


def test_lane_downstream_success_is_retryable() -> None:
    """Same-lane success row with stage_order >= MIN(failed) gets reset."""
    state = BootstrapState(status="partial_error", last_run_id=7, last_completed_at=None)
    snap = _snap(
        _stage("s17", 17, "sec_rate", "error"),
        _stage("s18", 18, "sec_rate", "error"),
        _stage("s19", 19, "sec_rate", "success"),  # success but >= min(17)
    )
    view = compute_retryable_view(state, snap)
    assert view.stage_retryable["s17"] is True
    assert view.stage_retryable["s18"] is True
    assert view.stage_retryable["s19"] is True


def test_own_status_irrelevance_pending_in_failed_lane() -> None:
    """Pending downstream in same failed lane → retryable=True.

    Codex 1b §3 — the predicate is lane+order, not the row's own
    status. A pending stage with stage_order >= MIN(failed) WILL be
    reset alongside the failures.
    """
    state = BootstrapState(status="partial_error", last_run_id=7, last_completed_at=None)
    snap = _snap(
        _stage("s17", 17, "sec_rate", "error"),
        _stage("s20", 20, "sec_rate", "pending"),  # pending but >= min(17)
    )
    view = compute_retryable_view(state, snap)
    assert view.stage_retryable["s17"] is True
    assert view.stage_retryable["s20"] is True


def test_pre_min_order_row_not_retryable() -> None:
    """Pending stage upstream of first failed row → retryable=False.

    Exact shape of run_id=3 S16: stage_order=16, sec_rate lane,
    pending; first sec_rate failure is at order 17. Reset SQL only
    walks stage_order >= 17, so S16 stays put.
    """
    state = BootstrapState(status="partial_error", last_run_id=3, last_completed_at=None)
    snap = _snap(
        _stage("s16", 16, "sec_rate", "pending"),
        _stage("s17", 17, "sec_rate", "error"),
    )
    view = compute_retryable_view(state, snap)
    assert view.stage_retryable["s16"] is False
    assert view.stage_retryable["s17"] is True


def test_pending_in_unfailed_lane_not_retryable() -> None:
    """Helper only resets failed lanes. Pending in another lane stays."""
    state = BootstrapState(status="partial_error", last_run_id=7, last_completed_at=None)
    snap = _snap(
        _stage("init", 1, "init", "pending"),  # no init-lane failures
        _stage("sec1", 2, "sec_rate", "error"),
    )
    view = compute_retryable_view(state, snap)
    assert view.stage_retryable["init"] is False
    assert view.stage_retryable["sec1"] is True


def test_cancelled_state_with_cancelled_stages() -> None:
    state = BootstrapState(status="cancelled", last_run_id=7, last_completed_at=None)
    snap = _snap(
        _stage("a", 1, "sec_rate", "cancelled"),
        _stage("b", 2, "sec_rate", "cancelled"),
        run_status="cancelled",
    )
    view = compute_retryable_view(state, snap)
    assert view.retry_available is True
    assert view.retry_blocked_reason is None
    assert view.stage_retryable == {"a": True, "b": True}


def test_blocked_status_is_failed() -> None:
    """`blocked` counts as failed for reset purposes (matches SQL)."""
    state = BootstrapState(status="partial_error", last_run_id=7, last_completed_at=None)
    snap = _snap(
        _stage("a", 1, "db", "success"),
        _stage("b", 2, "db", "blocked"),
    )
    view = compute_retryable_view(state, snap)
    assert view.retry_available is True
    assert view.stage_retryable == {"a": False, "b": True}


def test_run_id_3_worked_example() -> None:
    """Mirror the run_id=3 audit § 4.2 worked example exactly.

    Validates the predicate against the live dev-DB state recorded in
    the spec. S1-S16 retryable=False; S17-S24 retryable=True.
    """
    state = BootstrapState(
        status="partial_error",
        last_run_id=3,
        last_completed_at=datetime(2026, 5, 17, 5, 30, 37, tzinfo=UTC),
    )
    snap = _snap(
        _stage("universe_sync", 1, "init", "success"),
        _stage("candle_refresh", 2, "etoro", "success"),
        _stage("cusip_universe_backfill", 3, "sec_rate", "success"),
        _stage("sec_13f_filer_directory_sync", 4, "sec_rate", "success"),
        _stage("sec_nport_filer_directory_sync", 5, "sec_rate", "success"),
        _stage("cik_refresh", 6, "sec_rate", "success"),
        _stage("sec_bulk_download", 7, "sec_bulk_download", "success"),
        _stage("sec_submissions_ingest", 8, "db", "success"),
        _stage("sec_companyfacts_ingest", 9, "db", "success"),
        _stage("sec_13f_ingest_from_dataset", 10, "db", "success"),
        _stage("sec_insider_ingest_from_dataset", 11, "db", "success"),
        _stage("sec_nport_ingest_from_dataset", 12, "db", "success"),
        _stage("sec_submissions_files_walk", 13, "sec_rate", "success"),
        _stage("filings_history_seed", 14, "sec_rate", "success"),
        _stage("sec_first_install_drain", 15, "sec_rate", "success"),
        _stage("sec_def14a_bootstrap", 16, "sec_rate", "pending"),
        _stage("sec_business_summary_bootstrap", 17, "sec_rate", "error"),
        _stage("sec_insider_transactions_backfill", 18, "sec_rate", "error"),
        _stage("sec_form3_ingest", 19, "sec_rate", "error"),
        _stage("sec_8k_events_ingest", 20, "sec_rate", "error"),
        _stage("sec_13f_recent_sweep", 21, "sec_rate", "error"),
        _stage("sec_n_port_ingest", 22, "sec_rate", "error"),
        _stage("ownership_observations_backfill", 23, "db", "blocked"),
        _stage("fundamentals_sync", 24, "db", "blocked"),
    )
    view = compute_retryable_view(state, snap)
    assert view.retry_available is True
    expected = {
        # init / etoro / sec_bulk_download lanes have no failures.
        "universe_sync": False,
        "candle_refresh": False,
        "sec_bulk_download": False,
        # sec_rate lane: min failed order = 17. Anything < 17 stays put.
        "cusip_universe_backfill": False,
        "sec_13f_filer_directory_sync": False,
        "sec_nport_filer_directory_sync": False,
        "cik_refresh": False,
        "sec_submissions_files_walk": False,
        "filings_history_seed": False,
        "sec_first_install_drain": False,
        "sec_def14a_bootstrap": False,  # S16 — pre-min-order
        # sec_rate lane >= 17.
        "sec_business_summary_bootstrap": True,
        "sec_insider_transactions_backfill": True,
        "sec_form3_ingest": True,
        "sec_8k_events_ingest": True,
        "sec_13f_recent_sweep": True,
        "sec_n_port_ingest": True,
        # db lane: success rows 8-12 stay put; min failed order = 23.
        "sec_submissions_ingest": False,
        "sec_companyfacts_ingest": False,
        "sec_13f_ingest_from_dataset": False,
        "sec_insider_ingest_from_dataset": False,
        "sec_nport_ingest_from_dataset": False,
        "ownership_observations_backfill": True,
        "fundamentals_sync": True,
    }
    assert view.stage_retryable == expected
