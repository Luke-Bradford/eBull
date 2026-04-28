from datetime import UTC, datetime, timedelta
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
    assert cadence_display_string(Cadence(calendar_months=1)) == "monthly"
    assert cadence_display_string(Cadence(calendar_months=3)) == "3mo"


def test_cadence_calendar_months_effective_interval_is_31_day_upper_bound() -> None:
    """Calendar-anchored cadence reports a 31-day-per-month upper bound
    so the layer-state age-vs-grace check tolerates the longest-month
    case without flapping. The authoritative freshness check is the
    layer's ``is_fresh`` predicate; ``effective_interval`` is only used
    for the orchestrator's degraded-by-age fallback rule.
    """
    assert Cadence(calendar_months=1).effective_interval == timedelta(days=31)
    assert Cadence(calendar_months=3).effective_interval == timedelta(days=93)


def test_cadence_requires_exactly_one_mode() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        Cadence()
    with pytest.raises(ValueError, match="exactly one"):
        Cadence(interval=timedelta(hours=1), calendar_months=1)


def test_cadence_rejects_non_positive_calendar_months() -> None:
    with pytest.raises(ValueError, match="calendar_months must be positive"):
        Cadence(calendar_months=0)
    with pytest.raises(ValueError, match="calendar_months must be positive"):
        Cadence(calendar_months=-2)


def test_cadence_window_start_calendar_months_one() -> None:
    """Monthly cadence window starts at day-1 of the current month UTC."""
    now = datetime(2026, 4, 15, 12, 30, tzinfo=UTC)
    assert Cadence(calendar_months=1).window_start(now) == datetime(2026, 4, 1, tzinfo=UTC)


def test_cadence_window_start_calendar_months_three_handles_year_wrap() -> None:
    """3-month window in February 2026 anchors to December 2025."""
    now = datetime(2026, 2, 10, tzinfo=UTC)
    assert Cadence(calendar_months=3).window_start(now) == datetime(2025, 12, 1, tzinfo=UTC)


def test_cadence_window_start_interval_is_now_minus_interval() -> None:
    now = datetime(2026, 4, 15, 12, 30, tzinfo=UTC)
    cadence = Cadence(interval=timedelta(hours=24))
    assert cadence.window_start(now) == now - timedelta(hours=24)


def test_cadence_window_start_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Cadence(calendar_months=1).window_start(datetime(2026, 4, 15, 12, 30))


def test_cadence_seconds_for_state_machine_interval_is_unchanged() -> None:
    """Pre-#335 callers (interval cadences) see the literal interval
    seconds — backward-compatible with the prior shape."""
    cadence = Cadence(interval=timedelta(hours=24))
    assert cadence.cadence_seconds_for_state_machine(datetime.now(UTC), grace_multiplier=1.25) == 86400.0


def test_cadence_seconds_for_state_machine_calendar_collapses_grace() -> None:
    """Calendar mode: cadence_seconds * grace_multiplier MUST equal the
    seconds between ``now`` and ``window_start``, so the existing rule 9
    age check fires exactly at the calendar boundary regardless of the
    layer's grace_multiplier value (no over- or under-shoot)."""
    now = datetime(2026, 4, 15, 12, 30, tzinfo=UTC)
    cadence = Cadence(calendar_months=1)
    boundary_age = (now - datetime(2026, 4, 1, tzinfo=UTC)).total_seconds()
    grace = 1.25
    cadence_seconds = cadence.cadence_seconds_for_state_machine(now, grace_multiplier=grace)
    assert cadence_seconds * grace == pytest.approx(boundary_age)


def test_cadence_seconds_for_state_machine_at_exact_boundary_is_floored() -> None:
    """At ``now == window_start`` (exact day-1 UTC tick) the literal
    boundary_age is 0. Floor to 1 second so callers that log or divide
    by ``cadence_seconds`` never see zero. Rule 9's correctness is
    unchanged because any prior run still has ``age_seconds > 0``."""
    now = datetime(2026, 4, 1, 0, 0, 0, tzinfo=UTC)
    cadence = Cadence(calendar_months=1)
    cadence_seconds = cadence.cadence_seconds_for_state_machine(now, grace_multiplier=1.25)
    # 1.0 / 1.25 == 0.8; the floor is on boundary_age, not on the final
    # quotient — what matters is that cadence_seconds is strictly
    # positive so downstream math is never divide-by-zero.
    assert cadence_seconds > 0
