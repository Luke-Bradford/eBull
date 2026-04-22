"""Tests for the autonomous scheduler additions (Tasks 4-7).

Covers:
- retry_deferred_recommendations_job: kill switch gate, normal execution path
- monitor_positions_job: calls check_position_health and sets row_count
- execute_approved_orders: stamps timing_deferred_at in Phase 0 UPDATE
- morning_candidate_review: triggers execute_approved_orders when actionable recs present
"""

from __future__ import annotations

import inspect
from typing import Any
from unittest.mock import MagicMock, patch

import app.workers.scheduler as scheduler_module
from app.workers.scheduler import (
    monitor_positions_job,
    retry_deferred_recommendations_job,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DB_URL_PATCH = "app.workers.scheduler.settings"
_PSYCOPG_CONNECT_PATCH = "app.workers.scheduler.psycopg.connect"
_RECORD_START_PATCH = "app.workers.scheduler.record_job_start"
_RECORD_FINISH_PATCH = "app.workers.scheduler.record_job_finish"
_SPIKE_PATCH = "app.workers.scheduler.check_row_count_spike"


def _make_spike_mock() -> MagicMock:
    m = MagicMock()
    m.flagged = False
    return m


# ---------------------------------------------------------------------------
# Task 4: retry_deferred_recommendations_job
# ---------------------------------------------------------------------------


_GET_KS_PATCH = "app.services.ops_monitor.get_kill_switch_status"
_GET_CONFIG_PATCH = "app.services.runtime_config.get_runtime_config"


class TestRetryDeferredJob:
    """Tests for retry_deferred_recommendations_job."""

    @patch(_SPIKE_PATCH, return_value=MagicMock(flagged=False))
    @patch(_RECORD_FINISH_PATCH)
    @patch(_RECORD_START_PATCH, return_value=1)
    @patch("app.workers.scheduler.retry_deferred_recommendations")
    @patch(_PSYCOPG_CONNECT_PATCH)
    def test_kill_switch_active_skips_service(
        self,
        mock_connect: MagicMock,
        mock_retry_svc: MagicMock,
        mock_start: MagicMock,
        mock_finish: MagicMock,
        mock_spike: MagicMock,
    ) -> None:
        """When kill switch is active, retry service must not be called."""
        from app.services.runtime_config import RuntimeConfig

        mock_ks = {"is_active": True, "reason": "test"}
        mock_config = MagicMock(spec=RuntimeConfig)
        mock_config.enable_auto_trading = True

        conn_ctx = MagicMock()
        conn_ctx.__enter__ = MagicMock(return_value=conn_ctx)
        conn_ctx.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = conn_ctx

        with (
            patch(_GET_KS_PATCH, return_value=mock_ks),
            patch(_GET_CONFIG_PATCH, return_value=mock_config),
        ):
            retry_deferred_recommendations_job()

        mock_retry_svc.assert_not_called()

    @patch(_SPIKE_PATCH, return_value=MagicMock(flagged=False))
    @patch(_RECORD_FINISH_PATCH)
    @patch(_RECORD_START_PATCH, return_value=1)
    @patch("app.workers.scheduler.retry_deferred_recommendations")
    @patch(_PSYCOPG_CONNECT_PATCH)
    def test_auto_trading_disabled_skips_service(
        self,
        mock_connect: MagicMock,
        mock_retry_svc: MagicMock,
        mock_start: MagicMock,
        mock_finish: MagicMock,
        mock_spike: MagicMock,
    ) -> None:
        """When enable_auto_trading=False, retry service must not be called."""
        from app.services.runtime_config import RuntimeConfig

        mock_ks = {"is_active": False}
        mock_config = MagicMock(spec=RuntimeConfig)
        mock_config.enable_auto_trading = False

        conn_ctx = MagicMock()
        conn_ctx.__enter__ = MagicMock(return_value=conn_ctx)
        conn_ctx.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = conn_ctx

        with (
            patch(_GET_KS_PATCH, return_value=mock_ks),
            patch(_GET_CONFIG_PATCH, return_value=mock_config),
        ):
            retry_deferred_recommendations_job()

        mock_retry_svc.assert_not_called()

    @patch(_SPIKE_PATCH, return_value=MagicMock(flagged=False))
    @patch(_RECORD_FINISH_PATCH)
    @patch(_RECORD_START_PATCH, return_value=1)
    @patch("app.workers.scheduler.retry_deferred_recommendations")
    @patch(_PSYCOPG_CONNECT_PATCH)
    def test_calls_retry_service_when_config_allows(
        self,
        mock_connect: MagicMock,
        mock_retry_svc: MagicMock,
        mock_start: MagicMock,
        mock_finish: MagicMock,
        mock_spike: MagicMock,
    ) -> None:
        """When kill switch off and auto_trading on, retry service is called once."""
        from app.services.deferred_retry import RetryResult
        from app.services.runtime_config import RuntimeConfig

        mock_ks = {"is_active": False}
        mock_config = MagicMock(spec=RuntimeConfig)
        mock_config.enable_auto_trading = True

        fake_result = RetryResult(retried=2, re_proposed=1, re_deferred=1, expired=0, errors=0)
        mock_retry_svc.return_value = fake_result

        conn_ctx = MagicMock()
        conn_ctx.__enter__ = MagicMock(return_value=conn_ctx)
        conn_ctx.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = conn_ctx

        with (
            patch(_GET_KS_PATCH, return_value=mock_ks),
            patch(_GET_CONFIG_PATCH, return_value=mock_config),
        ):
            retry_deferred_recommendations_job()

        mock_retry_svc.assert_called_once()


# ---------------------------------------------------------------------------
# Task 5: monitor_positions_job
# ---------------------------------------------------------------------------


class TestMonitorPositionsJob:
    """Tests for monitor_positions_job."""

    @patch(_SPIKE_PATCH, return_value=MagicMock(flagged=False))
    @patch(_RECORD_FINISH_PATCH)
    @patch(_RECORD_START_PATCH, return_value=1)
    @patch("app.workers.scheduler.persist_position_alerts")
    @patch("app.workers.scheduler.check_position_health")
    @patch(_PSYCOPG_CONNECT_PATCH)
    def test_calls_check_position_health(
        self,
        mock_connect: MagicMock,
        mock_health: MagicMock,
        mock_persist: MagicMock,
        mock_start: MagicMock,
        mock_finish: MagicMock,
        mock_spike: MagicMock,
    ) -> None:
        """monitor_positions_job must call check_position_health and set row_count."""
        from app.services.position_monitor import MonitorResult, PersistStats

        fake_result = MonitorResult(positions_checked=3, alerts=())
        mock_health.return_value = fake_result
        mock_persist.return_value = PersistStats(opened=0, resolved=0, unchanged=0)

        conn_ctx = MagicMock()
        conn_ctx.__enter__ = MagicMock(return_value=conn_ctx)
        conn_ctx.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = conn_ctx

        monitor_positions_job()

        mock_health.assert_called_once()

    @patch(_SPIKE_PATCH, return_value=MagicMock(flagged=False))
    @patch(_RECORD_FINISH_PATCH)
    @patch(_RECORD_START_PATCH, return_value=1)
    @patch("app.workers.scheduler.persist_position_alerts")
    @patch("app.workers.scheduler.check_position_health")
    @patch(_PSYCOPG_CONNECT_PATCH)
    def test_row_count_equals_positions_checked(
        self,
        mock_connect: MagicMock,
        mock_health: MagicMock,
        mock_persist: MagicMock,
        mock_start: MagicMock,
        mock_finish: MagicMock,
        mock_spike: MagicMock,
    ) -> None:
        """tracker.row_count must equal result.positions_checked."""
        from app.services.position_monitor import MonitorAlert, MonitorResult, PersistStats

        alert = MonitorAlert(
            instrument_id=1,
            symbol="AAPL",
            alert_type="sl_breach",
            detail="bid=100 < stop_loss=110",
        )
        fake_result = MonitorResult(positions_checked=5, alerts=(alert,))
        mock_health.return_value = fake_result
        mock_persist.return_value = PersistStats(opened=0, resolved=0, unchanged=0)

        conn_ctx = MagicMock()
        conn_ctx.__enter__ = MagicMock(return_value=conn_ctx)
        conn_ctx.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = conn_ctx

        # Capture the tracker row_count by inspecting record_job_finish call args.
        captured_row_count: list[Any] = []

        def capture_finish(conn: Any, run_id: Any, **kwargs: Any) -> None:
            captured_row_count.append(kwargs.get("row_count"))

        with patch(_RECORD_FINISH_PATCH, side_effect=capture_finish):
            monitor_positions_job()

        assert captured_row_count and captured_row_count[0] == 5


# ---------------------------------------------------------------------------
# Task 6: timing_deferred_at stamp — source-level assertion
# ---------------------------------------------------------------------------


class TestTimingDeferredAtStamp:
    """Verify that execute_approved_orders and _timing_error_defer stamp timing_deferred_at."""

    def test_timing_deferred_at_in_execute_approved_orders_source(self) -> None:
        """Phase 0 defer UPDATE must include timing_deferred_at."""
        source = inspect.getsource(scheduler_module.execute_approved_orders)
        assert "timing_deferred_at" in source, (
            "execute_approved_orders must stamp timing_deferred_at = COALESCE(timing_deferred_at, NOW()) "
            "when deferring a rec in Phase 0"
        )

    def test_timing_deferred_at_in_timing_error_defer_source(self) -> None:
        """_timing_error_defer UPDATE must include timing_deferred_at."""
        source = inspect.getsource(scheduler_module._timing_error_defer)
        assert "timing_deferred_at" in source, (
            "_timing_error_defer must stamp timing_deferred_at = COALESCE(timing_deferred_at, NOW()) "
            "so error-deferred recs are eligible for expiry"
        )


# ---------------------------------------------------------------------------
# Task 7: Pipeline trigger — source-level assertion
# ---------------------------------------------------------------------------


class TestPipelineTrigger:
    """Verify that morning_candidate_review calls execute_approved_orders."""

    def test_execute_approved_orders_called_in_morning_review_source(self) -> None:
        """morning_candidate_review source must contain execute_approved_orders call."""
        source = inspect.getsource(scheduler_module.morning_candidate_review)
        assert "execute_approved_orders" in source, (
            "morning_candidate_review must trigger execute_approved_orders when actionable recs are generated"
        )
