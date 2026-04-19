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

Mock DB approach: same cursor/connection pattern as other test files.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.services.ops_monitor import (
    _STALENESS_THRESHOLDS,
    LAYER_QUERY_FAILED_DETAIL_TEMPLATE,
    activate_kill_switch,
    check_all_layers,
    check_job_health,
    check_layer_staleness,
    check_row_count_spike,
    deactivate_kill_switch,
    get_kill_switch_status,
    record_job_finish,
    record_job_skip,
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

    def test_single_layer_failure_does_not_abort_others(self) -> None:
        # First cursor (universe) raises mid-execute; the other 7 succeed.
        # The aggregate must still return 8 LayerHealth entries with the
        # broken one marked status="error". Prevention-log #70: never let
        # one infra fault degrade the whole operator-visibility surface.
        broken = MagicMock()
        broken.__enter__ = MagicMock(return_value=broken)
        broken.__exit__ = MagicMock(return_value=False)
        broken.execute.side_effect = RuntimeError("relation 'instruments' does not exist")

        cursors: list[MagicMock] = [broken]
        cursors.extend(_make_cursor([{"latest": _NOW - timedelta(hours=1)}]) for _ in range(7))
        conn = _make_conn(cursors)

        results = check_all_layers(conn, now=_NOW)

        assert len(results) == 8
        status_map = {r.layer: r.status for r in results}
        assert status_map["universe"] == "error"
        # Detail must NOT carry the raw exception text — that lands in the
        # API response and would leak schema/table names to bearer-token
        # holders. The fixed marker plus the layer name is enough for
        # operator triage; the full traceback is on the server-side logs.
        universe = next(r for r in results if r.layer == "universe")
        # Reference the production constant directly so test/prod drift is
        # impossible — if the template ever changes, this assertion moves
        # with it (#86 round 3 review).
        assert universe.detail == LAYER_QUERY_FAILED_DETAIL_TEMPLATE.format(layer="universe")
        assert "relation 'instruments' does not exist" not in universe.detail
        # Every other layer rendered normally.
        for layer, status in status_map.items():
            if layer != "universe":
                assert status == "ok", f"layer {layer} should be ok, got {status}"

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
# TestRecordJobSkip
# ---------------------------------------------------------------------------


class TestRecordJobSkip:
    """Test job skip recording.

    ``record_job_skip`` uses ``conn.transaction()`` + ``conn.execute()``
    (no separate cursor block, no explicit ``conn.commit()``).
    """

    @staticmethod
    def _conn_returning(row: tuple[Any, ...] | None) -> MagicMock:
        conn = _make_conn([])
        conn.autocommit = True
        result = MagicMock()
        result.fetchone.return_value = row
        conn.execute.return_value = result
        return conn

    def test_returns_run_id(self) -> None:
        conn = self._conn_returning((99,))
        run_id = record_job_skip(conn, "test_job", "no coverage rows", now=_NOW)
        assert run_id == 99
        conn.transaction.assert_called_once()

    def test_inserts_skipped_status_with_reason(self) -> None:
        conn = self._conn_returning((1,))
        record_job_skip(conn, "my_job", "no Tier 1/2 coverage rows", now=_NOW)
        call_args = conn.execute.call_args
        params = call_args[0][1]
        assert params["name"] == "my_job"
        assert params["reason"] == "no Tier 1/2 coverage rows"
        assert "skipped" in call_args[0][0]

    def test_raises_if_no_row_returned(self) -> None:
        conn = self._conn_returning(None)
        with pytest.raises(RuntimeError, match="no row"):
            record_job_skip(conn, "test_job", "reason", now=_NOW)

    def test_does_not_call_conn_commit_directly(self) -> None:
        """Commit is handled by conn.transaction(), not conn.commit()."""
        conn = self._conn_returning((1,))
        record_job_skip(conn, "test_job", "reason", now=_NOW)
        conn.commit.assert_not_called()


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

    def test_skipped_run_shows_reason(self) -> None:
        conn = _make_conn(
            [
                _make_cursor(
                    [
                        {
                            "status": "skipped",
                            "started_at": _NOW,
                            "finished_at": _NOW,
                            "error_msg": "no Tier 1/2 coverage rows",
                        }
                    ]
                )
            ]
        )
        result = check_job_health(conn, "test_job")
        assert result.last_status == "skipped"
        assert "skipped" in result.detail
        assert "no Tier 1/2 coverage rows" in result.detail

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
# TestFetchLatestSuccessfulRuns
# ---------------------------------------------------------------------------


class TestFetchLatestSuccessfulRuns:
    """Tests for ``fetch_latest_successful_runs``."""

    def test_returns_latest_success_per_job(self) -> None:
        from app.services.ops_monitor import fetch_latest_successful_runs

        ts1 = datetime(2026, 4, 9, 2, 0, 0, tzinfo=UTC)
        ts2 = datetime(2026, 4, 9, 4, 0, 0, tzinfo=UTC)
        rows = [
            {"job_name": "job_a", "started_at": ts1},
            {"job_name": "job_b", "started_at": ts2},
        ]
        conn = _make_conn([_make_cursor(rows)])
        result = fetch_latest_successful_runs(conn, ["job_a", "job_b"])
        assert result == {"job_a": ts1, "job_b": ts2}

    def test_missing_job_is_absent_from_result(self) -> None:
        from app.services.ops_monitor import fetch_latest_successful_runs

        ts = datetime(2026, 4, 9, 2, 0, 0, tzinfo=UTC)
        rows = [{"job_name": "job_a", "started_at": ts}]
        conn = _make_conn([_make_cursor(rows)])
        result = fetch_latest_successful_runs(conn, ["job_a", "job_b"])
        assert "job_a" in result
        assert "job_b" not in result

    def test_empty_names_returns_empty_dict(self) -> None:
        from app.services.ops_monitor import fetch_latest_successful_runs

        result = fetch_latest_successful_runs(MagicMock(), [])
        assert result == {}

    def test_no_successful_runs_returns_empty_dict(self) -> None:
        from app.services.ops_monitor import fetch_latest_successful_runs

        conn = _make_conn([_make_cursor([])])
        result = fetch_latest_successful_runs(conn, ["job_a"])
        assert result == {}


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
    """Test kill switch management functions.

    The transactional shape is:
        with conn.transaction():
            with conn.cursor() as cur: cur.execute(SELECT ... FOR UPDATE)
            conn.execute(UPDATE)
            conn.execute(INSERT INTO runtime_config_audit ...)
    """

    def test_activate_sets_fields_and_writes_audit(self) -> None:
        # Two cursors are opened: SELECT FOR UPDATE, then UPDATE ... RETURNING.
        select_cur = _make_cursor([{"is_active": False}])
        update_cur = _make_cursor([{"activated_at": _NOW}])
        conn = _make_conn([select_cur, update_cur])

        result = activate_kill_switch(conn, reason="data corruption", activated_by="ops", now=_NOW)

        # The UPDATE happens through the second cursor; the audit INSERT is the
        # only conn.execute() call.
        assert conn.execute.call_count == 1
        audit_call = conn.execute.call_args_list[0]
        assert "INSERT INTO runtime_config_audit" in audit_call[0][0]
        assert audit_call[0][1]["field"] == "kill_switch"
        assert audit_call[0][1]["new"] == "true"
        assert audit_call[0][1]["old"] == "false"
        assert audit_call[0][1]["by"] == "ops"

        # The UPDATE was issued on the second cursor with RETURNING.
        update_sql, update_params = update_cur.execute.call_args[0]
        assert "UPDATE kill_switch" in update_sql
        assert "RETURNING activated_at" in update_sql
        assert update_params["reason"] == "data corruption"
        assert update_params["by"] == "ops"
        assert update_params["at"] == _NOW

        # Returned dict carries DB-committed activated_at, not the app `now`.
        assert result["activated_at"] == _NOW
        assert result["activated_by"] == "ops"
        assert result["is_active"] is True

        conn.transaction.assert_called_once()

    def test_activate_raises_on_missing_row(self) -> None:
        conn = _make_conn([_make_cursor([])])  # SELECT FOR UPDATE returns no row
        with pytest.raises(RuntimeError, match="kill_switch row missing"):
            activate_kill_switch(conn, reason="test", activated_by="ops", now=_NOW)

    def test_deactivate_clears_fields_and_writes_audit(self) -> None:
        conn = _make_conn([_make_cursor([{"is_active": True}])])
        conn.execute.return_value = MagicMock(rowcount=1)

        deactivate_kill_switch(conn, deactivated_by="ops", reason="resolved", now=_NOW)

        assert conn.execute.call_count == 2
        update_call, audit_call = conn.execute.call_args_list
        # Order matters: UPDATE must precede the audit INSERT so the audit row
        # records committed state, not a speculative future state.
        assert "UPDATE kill_switch" in update_call[0][0]
        assert "INSERT INTO runtime_config_audit" in audit_call[0][0]
        assert audit_call[0][1]["field"] == "kill_switch"
        assert audit_call[0][1]["new"] == "false"
        assert audit_call[0][1]["old"] == "true"

    def test_deactivate_raises_on_missing_row(self) -> None:
        conn = _make_conn([_make_cursor([])])
        with pytest.raises(RuntimeError, match="kill_switch row missing"):
            deactivate_kill_switch(conn, deactivated_by="ops", reason="resolved")

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
