from app.services.sync_orchestrator.layer_types import LayerState
from app.services.sync_orchestrator.layer_types import (
    FailureCategory,
    REMEDIES,
    Remedy,
)


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


def test_failure_category_members() -> None:
    assert {c.value for c in FailureCategory} == {
        "auth_expired",
        "rate_limited",
        "source_down",
        "schema_drift",
        "db_constraint",
        "data_gap",
        "upstream_waiting",
        "internal_error",
    }


def test_every_category_has_a_remedy() -> None:
    for category in FailureCategory:
        assert category in REMEDIES
        remedy = REMEDIES[category]
        assert isinstance(remedy, Remedy)
        assert remedy.message
        if not remedy.self_heal:
            assert remedy.operator_fix is not None


def test_non_self_heal_categories_match_spec() -> None:
    non_self_heal = {
        FailureCategory.AUTH_EXPIRED,
        FailureCategory.SCHEMA_DRIFT,
        FailureCategory.DB_CONSTRAINT,
    }
    for category in FailureCategory:
        assert REMEDIES[category].self_heal == (category not in non_self_heal)
