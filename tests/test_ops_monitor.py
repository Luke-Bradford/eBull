"""
Tests for app.services.ops_monitor.

Structure:
  - TestCheckLayerStaleness — per-layer staleness detection
  - TestCheckAllLayers      — aggregated layer check
  - TestRecordJobStart      — job start recording
  - TestRecordJobFinish     — job finish recording
  - TestCheckJobHealth      — job health status
  - TestCheckRowCountSpike  — row-count spike detection
  - TestKillSwitch          — activate / deactivate / status
  - TestGetSystemHealth     — unified health report

Mock DB approach: same cursor/connection pattern as other test files.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.services.ops_monitor import (
    _STALENESS_THRESHOLDS,
    activate_kill_switch,
    check_all_layers,
    check_job_health,
    check_layer_staleness,
    check_row_count_spike,
    deactivate_kill_switch,
    get_kill_switch_status,
    get_system_health,
    record_job_finish,
    record_job_start,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 6, 9, 0, 0, tzinfo=UTC)


def _make_cursor(rows: list[dict[str, Any]]) -> MagicMock:
    """Build a mock cursor that returns dict rows from fetchone/fetchall."""
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = rows[0] if rows else None
    cur.fetchall.return_value = rows
    return cur


def _make_conn(cursors: list[MagicMock]) -> MagicMock:
    """Build a mock connection whose cursor() calls consume cursors in order.

    conn.cursor() is consumed from the cursors list (for functions using
    `with conn.cursor() as cur: cur.execute(...)`).
    conn.execute() is a separate MagicMock (for functions calling
    `conn.execute(...)` directly, e.g. record_job_finish).
    """
    conn = MagicMock()
    cursor_iter = iter(cursors)
    conn.cursor.side_effect = lambda **kwargs: next(cursor_iter)
    conn.execute.return_value = MagicMock()
    conn.commit.return_value = None
    tx = MagicMock()
    tx.__enter__ = MagicMock(return_value=tx)
    tx.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value = tx
    return conn


# ---------------------------------------------------------------------------
# TestCheckLayerStaleness
# ---------------------------------------------------------------------------


class TestCheckLayerStaleness:
    """Test staleness detection for individual data layers."""

    def test_empty_layer_returns_empty_status(self) -> None:
        conn = _make_conn([_make_cursor([{"latest": None}])])
        result = check_layer_staleness(conn, "universe", now=_NOW)
        assert result.status == "empty"
        assert result.layer == "universe"
        assert "no data rows" in result.detail

    def test_fresh_layer_returns_ok(self) -> None:
        # Universe threshold is 2 days; set latest to 1 hour ago.
        latest = _NOW - timedelta(hours=1)
        conn = _make_conn([_make_cursor([{"latest": latest}])])
        result = check_layer_staleness(conn, "universe", now=_NOW)
        assert result.status == "ok"
        assert result.latest == latest
        assert result.age is not None
        assert result.age < _STALENESS_THRESHOLDS["universe"]

    def test_stale_layer_returns_stale(self) -> None:
        # Universe threshold is 2 days; set latest to 3 days ago.
        latest = _NOW - timedelta(days=3)
        conn = _make_conn([_make_cursor([{"latest": latest}])])
        result = check_layer_staleness(conn, "universe", now=_NOW)
        assert result.status == "stale"
        assert "exceeds threshold" in result.detail

    def test_exactly_at_threshold_is_ok(self) -> None:
        # Exactly at threshold boundary (not strictly greater).
        threshold = _STALENESS_THRESHOLDS["scores"]
        latest = _NOW - threshold
        conn = _make_conn([_make_cursor([{"latest": latest}])])
        result = check_layer_staleness(conn, "scores", now=_NOW)
        assert result.status == "ok"

    def test_one_second_past_threshold_is_stale(self) -> None:
        threshold = _STALENESS_THRESHOLDS["scores"]
        latest = _NOW - threshold - timedelta(seconds=1)
        conn = _make_conn([_make_cursor([{"latest": latest}])])
        result = check_layer_staleness(conn, "scores", now=_NOW)
        assert result.status == "stale"

    def test_naive_datetime_gets_utc(self) -> None:
        # A naive datetime (no tzinfo) should still compare correctly.
        latest_naive = (_NOW - timedelta(hours=1)).replace(tzinfo=None)
        conn = _make_conn([_make_cursor([{"latest": latest_naive}])])
        result = check_layer_staleness(conn, "universe", now=_NOW)
        assert result.status == "ok"

    @pytest.mark.parametrize(
        "layer",
        [
            "universe",
            "prices",
            "quotes",
            "fundamentals",
            "filings",
            "news",
            "theses",
            "scores",
        ],
    )
    def test_every_layer_has_threshold(self, layer: str) -> None:
        assert layer in _STALENESS_THRESHOLDS


# ---------------------------------------------------------------------------
# TestCheckAllLayers
# ---------------------------------------------------------------------------


class TestCheckAllLayers:
    """Test the aggregated layer check."""

    def test_returns_one_result_per_layer(self) -> None:
        # All layers empty.
        cursors = [_make_cursor([{"latest": None}]) for _ in range(8)]
        conn = _make_conn(cursors)
        results = check_all_layers(conn, now=_NOW)
        assert len(results) == 8
        layers = [r.layer for r in results]
        assert "universe" in layers
        assert "scores" in layers

    def test_mixed_status_layers(self) -> None:
        # universe: fresh, prices: stale, rest: empty
        fresh = _NOW - timedelta(hours=1)
        stale = _NOW - timedelta(days=10)
        cursors = [
            _make_cursor([{"latest": fresh}]),  # universe
            _make_cursor([{"latest": stale}]),  # prices
        ] + [_make_cursor([{"latest": None}]) for _ in range(6)]
        conn = _make_conn(cursors)
        results = check_all_layers(conn, now=_NOW)
        status_map = {r.layer: r.status for r in results}
        assert status_map["universe"] == "ok"
        assert status_map["prices"] == "stale"
        assert status_map["fundamentals"] == "empty"


# ---------------------------------------------------------------------------
# TestRecordJobStart
# ---------------------------------------------------------------------------


class TestRecordJobStart:
    """Test job start recording."""

    def test_returns_run_id(self) -> None:
        conn = _make_conn([_make_cursor([{"run_id": 42}])])
        run_id = record_job_start(conn, "test_job", now=_NOW)
        assert run_id == 42
        conn.commit.assert_called_once()

    def test_raises_if_no_row_returned(self) -> None:
        conn = _make_conn([_make_cursor([])])
        with pytest.raises(RuntimeError, match="no row"):
            record_job_start(conn, "test_job", now=_NOW)


# ---------------------------------------------------------------------------
# TestRecordJobFinish
# ---------------------------------------------------------------------------


class TestRecordJobFinish:
    """Test job finish recording."""

    def test_success_updates_row(self) -> None:
        conn = MagicMock()
        record_job_finish(conn, 42, status="success", row_count=100, now=_NOW)
        conn.execute.assert_called_once()
        conn.commit.assert_called_once()
        call_args = conn.execute.call_args
        params = call_args[0][1]
        assert params["status"] == "success"
        assert params["row_count"] == 100
        assert params["run_id"] == 42

    def test_failure_records_error_msg(self) -> None:
        conn = MagicMock()
        record_job_finish(conn, 7, status="failure", error_msg="boom", now=_NOW)
        call_args = conn.execute.call_args
        params = call_args[0][1]
        assert params["status"] == "failure"
        assert params["error_msg"] == "boom"


# ---------------------------------------------------------------------------
# TestCheckJobHealth
# ---------------------------------------------------------------------------


class TestCheckJobHealth:
    """Test job health status queries."""

    def test_no_runs_returns_empty_detail(self) -> None:
        conn = _make_conn([_make_cursor([])])
        result = check_job_health(conn, "test_job")
        assert result.last_status is None
        assert "no runs recorded" in result.detail

    def test_successful_run_returns_status(self) -> None:
        started = _NOW - timedelta(minutes=5)
        finished = _NOW - timedelta(minutes=3)
        conn = _make_conn(
            [
                _make_cursor(
                    [
                        {
                            "status": "success",
                            "started_at": started,
                            "finished_at": finished,
                            "error_msg": None,
                        }
                    ]
                )
            ]
        )
        result = check_job_health(conn, "test_job")
        assert result.last_status == "success"
        assert result.last_started_at == started
        assert result.detail == ""

    def test_failed_run_includes_error(self) -> None:
        conn = _make_conn(
            [
                _make_cursor(
                    [
                        {
                            "status": "failure",
                            "started_at": _NOW - timedelta(minutes=5),
                            "finished_at": _NOW - timedelta(minutes=4),
                            "error_msg": "connection refused",
                        }
                    ]
                )
            ]
        )
        result = check_job_health(conn, "test_job")
        assert result.last_status == "failure"
        assert "last run failed" in result.detail
        assert "connection refused" in result.detail

    @patch("app.services.ops_monitor._utcnow", return_value=_NOW)
    def test_running_job_shows_in_progress(self, mock_now: MagicMock) -> None:
        conn = _make_conn(
            [
                _make_cursor(
                    [
                        {
                            "status": "running",
                            "started_at": _NOW - timedelta(minutes=10),
                            "finished_at": None,
                            "error_msg": None,
                        }
                    ]
                )
            ]
        )
        result = check_job_health(conn, "test_job")
        assert result.last_status == "running"
        assert "still in progress" in result.detail

    @patch("app.services.ops_monitor._utcnow", return_value=_NOW)
    def test_stuck_running_treated_as_failure(self, mock_now: MagicMock) -> None:
        # Started > 2 hours ago and still 'running' → treated as stuck/failed.
        conn = _make_conn(
            [
                _make_cursor(
                    [
                        {
                            "status": "running",
                            "started_at": _NOW - timedelta(hours=3),
                            "finished_at": None,
                            "error_msg": None,
                        }
                    ]
                )
            ]
        )
        result = check_job_health(conn, "test_job")
        assert result.last_status == "failure"
        assert "stuck" in result.detail


# ---------------------------------------------------------------------------
# TestCheckRowCountSpike
# ---------------------------------------------------------------------------


class TestCheckRowCountSpike:
    """Test row-count spike detection."""

    def test_no_prior_run_not_flagged(self) -> None:
        conn = _make_conn([_make_cursor([])])
        result = check_row_count_spike(conn, "test_job", 100)
        assert not result.flagged
        assert "no prior" in result.detail

    def test_count_above_threshold_not_flagged(self) -> None:
        # Previous: 100, current: 80 → ratio 0.8 > 0.5 threshold
        conn = _make_conn([_make_cursor([{"row_count": 100}])])
        result = check_row_count_spike(conn, "test_job", 80)
        assert not result.flagged
        assert result.previous_count == 100

    def test_count_below_threshold_flagged(self) -> None:
        # Previous: 100, current: 40 → ratio 0.4 < 0.5 threshold
        conn = _make_conn([_make_cursor([{"row_count": 100}])])
        result = check_row_count_spike(conn, "test_job", 40)
        assert result.flagged
        assert "dropped" in result.detail

    def test_zero_current_flagged(self) -> None:
        # Previous: 50, current: 0 → ratio 0.0 < 0.5 threshold
        conn = _make_conn([_make_cursor([{"row_count": 50}])])
        result = check_row_count_spike(conn, "test_job", 0)
        assert result.flagged

    def test_zero_previous_not_flagged(self) -> None:
        # Previous: 0 → skip comparison (avoid divide by zero).
        conn = _make_conn([_make_cursor([{"row_count": 0}])])
        result = check_row_count_spike(conn, "test_job", 0)
        assert not result.flagged

    def test_exactly_at_threshold_not_flagged(self) -> None:
        # Previous: 100, current: 50 → ratio 0.5 == threshold (not strictly less)
        conn = _make_conn([_make_cursor([{"row_count": 100}])])
        result = check_row_count_spike(conn, "test_job", 50)
        assert not result.flagged

    def test_just_below_threshold_flagged(self) -> None:
        # Previous: 100, current: 49 → ratio 0.49 < 0.5 threshold
        conn = _make_conn([_make_cursor([{"row_count": 100}])])
        result = check_row_count_spike(conn, "test_job", 49)
        assert result.flagged


# ---------------------------------------------------------------------------
# TestKillSwitch
# ---------------------------------------------------------------------------


class TestKillSwitch:
    """Test kill switch management functions."""

    def test_activate_sets_fields(self) -> None:
        conn = MagicMock()
        conn.execute.return_value = MagicMock(rowcount=1)
        activate_kill_switch(conn, reason="data corruption", activated_by="ops", now=_NOW)
        conn.execute.assert_called_once()
        params = conn.execute.call_args[0][1]
        assert params["reason"] == "data corruption"
        assert params["by"] == "ops"
        assert params["at"] == _NOW
        conn.commit.assert_called_once()

    def test_activate_raises_on_missing_row(self) -> None:
        conn = MagicMock()
        conn.execute.return_value = MagicMock(rowcount=0)
        with pytest.raises(RuntimeError, match="kill_switch row missing"):
            activate_kill_switch(conn, reason="test", activated_by="ops", now=_NOW)

    def test_deactivate_clears_fields(self) -> None:
        conn = MagicMock()
        conn.execute.return_value = MagicMock(rowcount=1)
        deactivate_kill_switch(conn)
        conn.execute.assert_called_once()
        conn.commit.assert_called_once()

    def test_deactivate_raises_on_missing_row(self) -> None:
        conn = MagicMock()
        conn.execute.return_value = MagicMock(rowcount=0)
        with pytest.raises(RuntimeError, match="kill_switch row missing"):
            deactivate_kill_switch(conn)

    def test_status_returns_active_state(self) -> None:
        conn = _make_conn(
            [
                _make_cursor(
                    [
                        {
                            "is_active": True,
                            "activated_at": _NOW,
                            "activated_by": "ops",
                            "reason": "test",
                        }
                    ]
                )
            ]
        )
        status = get_kill_switch_status(conn)
        assert status["is_active"] is True
        assert status["reason"] == "test"

    def test_status_returns_inactive_state(self) -> None:
        conn = _make_conn(
            [
                _make_cursor(
                    [
                        {
                            "is_active": False,
                            "activated_at": None,
                            "activated_by": None,
                            "reason": None,
                        }
                    ]
                )
            ]
        )
        status = get_kill_switch_status(conn)
        assert status["is_active"] is False

    def test_missing_row_returns_active_fail_closed(self) -> None:
        conn = _make_conn([_make_cursor([])])
        status = get_kill_switch_status(conn)
        assert status["is_active"] is True
        assert "corrupt" in status["reason"]


# ---------------------------------------------------------------------------
# TestGetSystemHealth
# ---------------------------------------------------------------------------


class TestGetSystemHealth:
    """Test unified health report."""

    @patch("app.services.ops_monitor._utcnow", return_value=_NOW)
    def test_report_includes_all_layers(self, mock_now: MagicMock) -> None:
        # 8 layer cursors (all empty) + 1 kill switch cursor
        cursors = [_make_cursor([{"latest": None}]) for _ in range(8)]
        cursors.append(
            _make_cursor(
                [
                    {
                        "is_active": False,
                        "activated_at": None,
                        "activated_by": None,
                        "reason": None,
                    }
                ]
            )
        )
        conn = _make_conn(cursors)
        report = get_system_health(conn)
        assert len(report.layers) == 8
        assert report.kill_switch_active is False

    @patch("app.services.ops_monitor._utcnow", return_value=_NOW)
    def test_report_includes_job_health(self, mock_now: MagicMock) -> None:
        # 8 layer cursors + 1 job health cursor + 1 kill switch cursor
        cursors = [_make_cursor([{"latest": None}]) for _ in range(8)]
        cursors.append(
            _make_cursor(
                [
                    {
                        "status": "success",
                        "started_at": _NOW,
                        "finished_at": _NOW,
                        "error_msg": None,
                    }
                ]
            )
        )
        cursors.append(
            _make_cursor(
                [
                    {
                        "is_active": False,
                        "activated_at": None,
                        "activated_by": None,
                        "reason": None,
                    }
                ]
            )
        )
        conn = _make_conn(cursors)
        report = get_system_health(conn, job_names=["test_job"])
        assert len(report.jobs) == 1
        assert report.jobs[0].last_status == "success"

    @patch("app.services.ops_monitor._utcnow", return_value=_NOW)
    def test_kill_switch_active_in_report(self, mock_now: MagicMock) -> None:
        cursors = [_make_cursor([{"latest": None}]) for _ in range(8)]
        cursors.append(
            _make_cursor(
                [
                    {
                        "is_active": True,
                        "activated_at": _NOW,
                        "activated_by": "ops",
                        "reason": "emergency halt",
                    }
                ]
            )
        )
        conn = _make_conn(cursors)
        report = get_system_health(conn)
        assert report.kill_switch_active is True
        assert "emergency halt" in report.kill_switch_detail
