"""Boot freshness sweep relocation (#719).

The pre-#719 ``app.main._boot_freshness_sweep`` moved to
``app.jobs.boot_sweep.run_boot_freshness_sweep``. Same best-effort
contract: every exception is logged + swallowed, ``EBULL_SKIP_BOOT_SWEEP``
opts out.
"""

from __future__ import annotations

from unittest.mock import patch

from app.jobs.boot_sweep import run_boot_freshness_sweep
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


def test_swallows_sync_already_running() -> None:
    """Another sync racing the sweep must not propagate."""
    import os

    os.environ.pop("EBULL_SKIP_BOOT_SWEEP", None)
    with patch("app.jobs.boot_sweep.run_sync") as run:
        run.side_effect = SyncAlreadyRunning(SyncScope.behind(), active_sync_run_id=99)
        run_boot_freshness_sweep()  # must not raise


def test_swallows_arbitrary_exceptions() -> None:
    import os

    os.environ.pop("EBULL_SKIP_BOOT_SWEEP", None)
    with patch("app.jobs.boot_sweep.run_sync") as run:
        run.side_effect = RuntimeError("boom")
        run_boot_freshness_sweep()  # must not raise
