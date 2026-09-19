from unittest import mock

from app.services.processes import scheduled_adapter
from app.services.processes.scheduled_adapter import ORCHESTRATOR_SYNC_SCOPE


def test_full_sync_resolves_from_sync_runs():
    assert ORCHESTRATOR_SYNC_SCOPE.get("orchestrator_full_sync") == "full"


def test_high_frequency_sync_still_mapped():
    assert ORCHESTRATOR_SYNC_SCOPE.get("orchestrator_high_frequency_sync") == "high_frequency"


def test_active_row_resolver_dispatches_on_the_same_registry() -> None:
    """#2274 — the ACTIVE half re-homes for exactly the jobs the TERMINAL half
    does, and for no others.

    #1474 Part 2 re-homed the terminal row and left the active read on
    ``job_runs``, where these two jobs have never written a single ``running``
    row — so the status pill, the active-run block and the Cancel button were
    all unreachable for them by construction. Pinning the dispatch against the
    registry (rather than against two name literals) is what stops the two
    halves of one row disagreeing about which table the job records in.
    """
    calls: list[tuple[str, str]] = []

    def fake_sync(_conn: object, *, scope: str) -> None:
        calls.append(("sync_runs", scope))

    def fake_job(_conn: object, *, job_name: str) -> None:
        calls.append(("job_runs", job_name))

    with (
        mock.patch.object(scheduled_adapter, "_read_running_sync_run", fake_sync),
        mock.patch.object(scheduled_adapter, "_read_running_run", fake_job),
    ):
        for name in ORCHESTRATOR_SYNC_SCOPE:
            scheduled_adapter._resolve_active_row(None, job_name=name)  # type: ignore[arg-type]
        scheduled_adapter._resolve_active_row(None, job_name="daily_candle_refresh")  # type: ignore[arg-type]

    assert calls == [
        ("sync_runs", ORCHESTRATOR_SYNC_SCOPE["orchestrator_high_frequency_sync"]),
        ("sync_runs", ORCHESTRATOR_SYNC_SCOPE["orchestrator_full_sync"]),
        ("job_runs", "daily_candle_refresh"),
    ]
