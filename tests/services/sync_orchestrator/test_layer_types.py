from datetime import timedelta
from typing import Any

import psycopg
import pytest

from app.services.sync_orchestrator.layer_types import LayerState
from app.services.sync_orchestrator.layer_types import (
    FailureCategory,
    REMEDIES,
    Remedy,
)
from app.services.sync_orchestrator.layer_types import (
    Cadence,
    ContentPredicate,
    DEFAULT_RETRY_POLICY,
    FailureCategory as _FC,  # avoid redef shadow; OK to remove alias if not needed
    LayerRefreshFailed,
    RetryPolicy,
    SecretRef,
    cadence_display_string,
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


def test_cadence_grace_window_uses_multiplier() -> None:
    c = Cadence(interval=timedelta(hours=24))
    assert c.grace_window(grace_multiplier=1.25) == timedelta(hours=30)


def test_cadence_rejects_non_positive_interval() -> None:
    with pytest.raises(ValueError, match="interval must be positive"):
        Cadence(interval=timedelta(0))


def test_default_retry_policy() -> None:
    assert DEFAULT_RETRY_POLICY.max_attempts == 3
    assert DEFAULT_RETRY_POLICY.backoff_seconds == (60, 600, 3600)


def test_retry_policy_backoff_matches_max_attempts() -> None:
    with pytest.raises(ValueError, match="backoff_seconds"):
        RetryPolicy(max_attempts=3, backoff_seconds=(60, 600))


def test_secret_ref_fields() -> None:
    ref = SecretRef(env_var="ANTHROPIC_API_KEY", display_name="Anthropic API key")
    assert ref.env_var == "ANTHROPIC_API_KEY"
    assert ref.display_name == "Anthropic API key"


def test_layer_refresh_failed_carries_category() -> None:
    err = LayerRefreshFailed(category=_FC.SOURCE_DOWN, detail="finnhub 503")
    assert err.category is _FC.SOURCE_DOWN
    assert err.detail == "finnhub 503"
    assert "source_down" in str(err)
    assert "finnhub" in str(err)


def test_content_predicate_is_a_callable_protocol() -> None:
    def my_pred(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
        return True, "ok"
    _: ContentPredicate = my_pred
    assert my_pred is not None


def test_cadence_display_string() -> None:
    assert cadence_display_string(Cadence(interval=timedelta(hours=24))) == "daily"
    assert cadence_display_string(Cadence(interval=timedelta(days=7))) == "7d"
    assert cadence_display_string(Cadence(interval=timedelta(hours=4))) == "4h"
    assert cadence_display_string(Cadence(interval=timedelta(minutes=5))) == "5m"
