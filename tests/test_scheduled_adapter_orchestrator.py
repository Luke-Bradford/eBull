from app.services.processes.scheduled_adapter import _ORCHESTRATOR_SYNC_SCOPE


def test_full_sync_resolves_from_sync_runs():
    assert _ORCHESTRATOR_SYNC_SCOPE.get("orchestrator_full_sync") == "full"


def test_high_frequency_sync_still_mapped():
    assert _ORCHESTRATOR_SYNC_SCOPE.get("orchestrator_high_frequency_sync") == "high_frequency"
