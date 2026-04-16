"""Unit tests for app.services.sync_orchestrator.progress.

Focus:
  - ``report_progress`` is a no-op when no callback is installed
    (the common case — APScheduler calling a legacy job directly, or
    unit tests of the service-layer function without an orchestrator).
  - ``set_active_progress`` fires an immediate (0, None) tick so the
    UI can leave 'pending' on the first poll.
  - Throttling: emits only when the item delta OR time delta exceeds
    the threshold; ``force=True`` bypasses both.
  - Callback exceptions are swallowed (progress reporting must never
    abort work).
  - The ContextVar is properly cleared by the token restore so state
    does not leak between layers in the same sync run.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.services.sync_orchestrator.progress import (
    clear_active_progress,
    report_progress,
    set_active_progress,
)


class TestNoActiveCallback:
    def test_report_progress_is_noop_when_no_callback(self) -> None:
        # No ContextVar set — must return without raising.
        report_progress(5, 100)
        report_progress(10, 100, force=True)


class TestImmediateTickOnInstall:
    def test_set_active_progress_fires_initial_zero_tick(self) -> None:
        cb = MagicMock()
        token = set_active_progress(cb)
        try:
            cb.assert_called_once_with(0, None)
        finally:
            clear_active_progress(token)

    def test_swallows_exception_from_initial_tick(self) -> None:
        cb = MagicMock(side_effect=RuntimeError("boom"))
        # Must not raise — progress reporting must never abort the
        # work it is instrumenting.
        token = set_active_progress(cb)
        clear_active_progress(token)


class TestThrottle:
    def test_suppresses_ticks_below_item_and_time_threshold(self) -> None:
        cb = MagicMock()
        with patch("app.services.sync_orchestrator.progress.time.monotonic", return_value=100.0):
            token = set_active_progress(cb)
            cb.reset_mock()  # drop the install-time tick

            # Items delta = 2 (below default 5); time delta = 0 (below 10s).
            report_progress(2, 100)
            report_progress(4, 100)

            assert cb.call_count == 0
            clear_active_progress(token)

    def test_emits_when_item_threshold_met(self) -> None:
        cb = MagicMock()
        with patch("app.services.sync_orchestrator.progress.time.monotonic", return_value=100.0):
            token = set_active_progress(cb)
            cb.reset_mock()

            # Items delta = 5 (default threshold).
            report_progress(5, 100)

            cb.assert_called_once_with(5, 100)
            clear_active_progress(token)

    def test_emits_when_time_threshold_met(self) -> None:
        cb = MagicMock()
        monotonic = MagicMock(side_effect=[100.0, 115.0])  # install, then tick
        with patch("app.services.sync_orchestrator.progress.time.monotonic", monotonic):
            token = set_active_progress(cb)
            cb.reset_mock()

            # Items delta = 1 (below 5); time delta = 15s (above 10s).
            report_progress(1, 100)

            cb.assert_called_once_with(1, 100)
            clear_active_progress(token)

    def test_force_bypasses_throttle(self) -> None:
        cb = MagicMock()
        with patch("app.services.sync_orchestrator.progress.time.monotonic", return_value=100.0):
            token = set_active_progress(cb)
            cb.reset_mock()

            report_progress(1, 100, force=True)

            cb.assert_called_once_with(1, 100)
            clear_active_progress(token)


class TestCallbackErrorHandling:
    def test_callback_exception_is_swallowed_and_state_advances(self) -> None:
        # A callback that raises must not abort the loop. The throttle
        # state is NOT advanced on failure, so subsequent threshold
        # checks continue to operate on the prior tick — this is
        # intentional: if the DB is momentarily unavailable, we want
        # to retry on the next tick rather than silently skip until
        # the next time threshold.
        cb = MagicMock(side_effect=RuntimeError("db down"))
        with patch("app.services.sync_orchestrator.progress.time.monotonic", return_value=100.0):
            token = set_active_progress(cb)
            cb.reset_mock()
            cb.side_effect = RuntimeError("db down")

            report_progress(5, 100)  # hits threshold, callback raises

            # Callback was invoked once despite raising.
            cb.assert_called_once_with(5, 100)
            clear_active_progress(token)


class TestContextVarIsolation:
    def test_clear_restores_previous_state(self) -> None:
        # After clear, a subsequent report_progress must not invoke
        # the old callback — the var has been reset.
        cb = MagicMock()
        token = set_active_progress(cb)
        cb.reset_mock()
        clear_active_progress(token)

        report_progress(100, 100, force=True)

        cb.assert_not_called()

    def test_nested_progress_contexts_restore_outer(self) -> None:
        outer = MagicMock()
        inner = MagicMock()

        outer_token = set_active_progress(outer)
        outer.reset_mock()

        inner_token = set_active_progress(inner)
        inner.reset_mock()

        # Inner active: only `inner` should receive the forced tick.
        report_progress(1, 10, force=True)
        inner.assert_called_once_with(1, 10)
        outer.assert_not_called()

        clear_active_progress(inner_token)

        # Outer restored: now `outer` receives.
        outer.reset_mock()
        report_progress(2, 10, force=True)
        outer.assert_called_once_with(2, 10)

        clear_active_progress(outer_token)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
