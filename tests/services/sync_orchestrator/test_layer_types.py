from app.services.sync_orchestrator.layer_types import LayerState


def test_layer_state_has_eight_members() -> None:
    assert {s.value for s in LayerState} == {
        "healthy",
        "running",
        "retrying",
        "degraded",
        "action_needed",
        "secret_missing",
        "cascade_waiting",
        "disabled",
    }


def test_layer_state_is_str_enum() -> None:
    assert LayerState.HEALTHY == "healthy"
    assert LayerState("healthy") is LayerState.HEALTHY
