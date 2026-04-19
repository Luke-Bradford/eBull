from datetime import timedelta
from typing import Any

import psycopg
import pytest

from app.services.sync_orchestrator.layer_types import (
    DEFAULT_RETRY_POLICY,
    REMEDIES,
    Cadence,
    ContentPredicate,
    FailureCategory,
    LayerRefreshFailed,
    LayerState,
    Remedy,
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
    err = LayerRefreshFailed(category=FailureCategory.SOURCE_DOWN, detail="finnhub 503")
    assert err.category is FailureCategory.SOURCE_DOWN
    assert err.detail == "finnhub 503"
    assert str(err) == "source_down: finnhub 503"


def test_content_predicate_callable_returns_tuple() -> None:
    from unittest.mock import MagicMock

    def my_pred(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
        return True, "all clear"

    _: ContentPredicate = my_pred  # pyright-time assignability check
    ok, detail = my_pred(MagicMock())
    assert ok is True
    assert detail == "all clear"


def test_retry_policy_rejects_non_positive_backoff() -> None:
    with pytest.raises(ValueError, match="backoff_seconds"):
        RetryPolicy(max_attempts=3, backoff_seconds=(0, 10, 20))
    with pytest.raises(ValueError, match="backoff_seconds"):
        RetryPolicy(max_attempts=3, backoff_seconds=(10, -1, 20))


def test_retry_policy_rejects_zero_max_attempts() -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        RetryPolicy(max_attempts=0, backoff_seconds=())


def test_cadence_grace_window_rejects_non_positive_multiplier() -> None:
    c = Cadence(interval=timedelta(hours=24))
    with pytest.raises(ValueError, match="grace_multiplier"):
        c.grace_window(grace_multiplier=0)
    with pytest.raises(ValueError, match="grace_multiplier"):
        c.grace_window(grace_multiplier=-0.5)


def test_cadence_display_string() -> None:
    assert cadence_display_string(Cadence(interval=timedelta(hours=24))) == "daily"
    assert cadence_display_string(Cadence(interval=timedelta(days=7))) == "7d"
    assert cadence_display_string(Cadence(interval=timedelta(hours=4))) == "4h"
    assert cadence_display_string(Cadence(interval=timedelta(minutes=5))) == "5m"
    assert cadence_display_string(Cadence(interval=timedelta(seconds=30))) == "30s"
