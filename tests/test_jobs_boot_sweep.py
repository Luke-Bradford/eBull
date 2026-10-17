"""Boot freshness sweep relocation (#719).

The pre-#719 ``app.main._boot_freshness_sweep`` moved to
``app.jobs.boot_sweep.run_boot_freshness_sweep``. Same best-effort
contract: every exception is logged + swallowed, ``EBULL_SKIP_BOOT_SWEEP``
opts out.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from app.jobs.boot_sweep import RETRY_DEADLINE_S, RETRY_INTERVAL_S, run_boot_freshness_sweep
from app.services.sync_orchestrator.types import (
    SyncAlreadyRunning,
    SyncResult,
    SyncScope,
)


def test_skip_env_var_short_circuits() -> None:
    """``EBULL_SKIP_BOOT_SWEEP=1`` must avoid calling run_sync at all."""
    with (
        patch.dict("os.environ", {"EBULL_SKIP_BOOT_SWEEP": "1"}),
        patch("app.jobs.boot_sweep.run_sync") as run,
    ):
        run_boot_freshness_sweep()
    run.assert_not_called()


def test_dispatches_with_behind_scope_and_boot_sweep_trigger() -> None:
    with (
        patch.dict("os.environ", {}, clear=False),
        patch("app.jobs.boot_sweep.run_sync") as run,
    ):
        # Drop the env var if the parent shell set it.
        import os

        os.environ.pop("EBULL_SKIP_BOOT_SWEEP", None)
        run.return_value = SyncResult(sync_run_id=1, outcomes={})
        run_boot_freshness_sweep()
    run.assert_called_once()
    args, kwargs = run.call_args
    assert isinstance(args[0], SyncScope)
    assert args[0].kind == "behind"
    assert kwargs["trigger"] == "boot_sweep"
    assert kwargs["linked_request_id"] is None


class _Clock:
    """A monotonic clock that only ``sleep`` advances."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds

    def monotonic(self) -> float:
        return self.now


def _busy(active_id: int | None = 99) -> SyncAlreadyRunning:
    return SyncAlreadyRunning(SyncScope.behind(), active_sync_run_id=active_id)


def test_a_high_frequency_holder_is_waited_out_then_the_sweep_dispatches() -> None:
    """#3784: catch-up's high-frequency sync held the slot for 7.7 s on 2026-10-13 and the sweep was dropped."""
    import os

    os.environ.pop("EBULL_SKIP_BOOT_SWEEP", None)
    clock = _Clock()
    with patch("app.jobs.boot_sweep.run_sync") as run:
        run.side_effect = [_busy(), _busy(), SyncResult(sync_run_id=2, outcomes={})]
        run_boot_freshness_sweep(
            sleep=clock.sleep, monotonic=clock.monotonic, active_scope=lambda _id: "high_frequency"
        )
    assert run.call_count == 3
    assert clock.sleeps == [RETRY_INTERVAL_S, RETRY_INTERVAL_S]


def test_a_holder_that_finished_between_the_conflict_and_the_lookup_is_retried() -> None:
    import os

    os.environ.pop("EBULL_SKIP_BOOT_SWEEP", None)
    clock = _Clock()
    with patch("app.jobs.boot_sweep.run_sync") as run:
        run.side_effect = [_busy(None), SyncResult(sync_run_id=2, outcomes={})]
        run_boot_freshness_sweep(sleep=clock.sleep, monotonic=clock.monotonic, active_scope=lambda _id: None)
    assert run.call_count == 2


@pytest.mark.parametrize("holder", ["full", "behind"])
def test_a_full_or_behind_holder_covers_the_sweep(holder: str) -> None:
    import os

    os.environ.pop("EBULL_SKIP_BOOT_SWEEP", None)
    clock = _Clock()
    with patch("app.jobs.boot_sweep.run_sync") as run:
        run.side_effect = _busy()
        run_boot_freshness_sweep(sleep=clock.sleep, monotonic=clock.monotonic, active_scope=lambda _id: holder)
    run.assert_called_once()
    assert clock.sleeps == []


def test_a_holder_past_the_deadline_gives_up_with_a_warning() -> None:
    """Another sync racing the sweep must not propagate, and the wait is bounded."""
    import os

    os.environ.pop("EBULL_SKIP_BOOT_SWEEP", None)
    clock = _Clock()
    with patch("app.jobs.boot_sweep.run_sync") as run, patch("app.jobs.boot_sweep.logger") as log:
        run.side_effect = _busy()
        run_boot_freshness_sweep(
            sleep=clock.sleep, monotonic=clock.monotonic, active_scope=lambda _id: "high_frequency"
        )  # must not raise
    assert clock.now <= RETRY_DEADLINE_S
    assert run.call_count == int(RETRY_DEADLINE_S // RETRY_INTERVAL_S) + 1
    log.warning.assert_called_once()


def test_swallows_arbitrary_exceptions() -> None:
    import os

    os.environ.pop("EBULL_SKIP_BOOT_SWEEP", None)
    with patch("app.jobs.boot_sweep.run_sync") as run:
        run.side_effect = RuntimeError("boom")
        run_boot_freshness_sweep()  # must not raise


def test_boot_sweep_thread_target_runs_sweep() -> None:
    """#1479: the daemon thread target invokes ``run_boot_freshness_sweep``."""
    from app.jobs.__main__ import _run_boot_freshness_sweep_thread

    with patch("app.jobs.__main__.run_boot_freshness_sweep") as sweep:
        _run_boot_freshness_sweep_thread()
    sweep.assert_called_once_with()


def test_boot_sweep_thread_target_swallows_unexpected_escape() -> None:
    """An unexpected escape from the sweep must be logged, not raised out of
    the daemon thread (which would only surface as an unraisable-thread
    traceback). ``run_boot_freshness_sweep`` already swallows its own
    ``SyncAlreadyRunning`` / arbitrary exceptions; this guards the residual
    unexpected-``Exception`` escape path (the wrapper's ``except Exception``;
    ``BaseException`` subclasses like ``KeyboardInterrupt`` are deliberately
    not caught)."""
    from app.jobs.__main__ import _run_boot_freshness_sweep_thread

    with (
        patch("app.jobs.__main__.run_boot_freshness_sweep", side_effect=RuntimeError("boom")),
        patch("app.jobs.__main__.logger") as log,
    ):
        _run_boot_freshness_sweep_thread()  # must not raise
    log.exception.assert_called_once()
