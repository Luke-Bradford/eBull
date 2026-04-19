from app.services.sync_orchestrator.layer_state import (
    LayerContext,
    compute_layer_state,
)
from app.services.sync_orchestrator.layer_types import LayerState


def _ctx(**overrides):
    defaults = dict(
        is_enabled=True,
        is_running=False,
        latest_status="complete",
        latest_category=None,
        attempts=0,
        upstream_states={},
        secret_present=True,
        content_ok=True,
        age_seconds=60.0,
        cadence_seconds=86400.0,
        grace_multiplier=1.25,
        max_attempts=3,
    )
    defaults.update(overrides)
    return LayerContext(**defaults)


def test_disabled_overrides_everything() -> None:
    assert compute_layer_state(_ctx(is_enabled=False, latest_status="failed")) is LayerState.DISABLED


def test_healthy_when_all_clean() -> None:
    assert compute_layer_state(_ctx()) is LayerState.HEALTHY


def test_running_wins_over_failure() -> None:
    assert compute_layer_state(_ctx(is_running=True, latest_status="failed", attempts=99)) is LayerState.RUNNING


def test_secret_missing_beats_prior_failure() -> None:
    assert (
        compute_layer_state(_ctx(secret_present=False, latest_status="failed", latest_category="source_down"))
        is LayerState.SECRET_MISSING
    )


def test_auth_expired_escalates_on_first_failure() -> None:
    assert (
        compute_layer_state(_ctx(latest_status="failed", latest_category="auth_expired", attempts=1))
        is LayerState.ACTION_NEEDED
    )


def test_schema_drift_escalates_on_first_failure() -> None:
    assert (
        compute_layer_state(_ctx(latest_status="failed", latest_category="schema_drift", attempts=1))
        is LayerState.ACTION_NEEDED
    )


def test_db_constraint_escalates_on_first_failure() -> None:
    assert (
        compute_layer_state(_ctx(latest_status="failed", latest_category="db_constraint", attempts=1))
        is LayerState.ACTION_NEEDED
    )


def test_rate_limited_under_budget_retries() -> None:
    assert (
        compute_layer_state(_ctx(latest_status="failed", latest_category="rate_limited", attempts=1))
        is LayerState.RETRYING
    )


def test_rate_limited_exhausted_escalates() -> None:
    assert (
        compute_layer_state(_ctx(latest_status="failed", latest_category="rate_limited", attempts=3))
        is LayerState.ACTION_NEEDED
    )


def test_unknown_category_treated_as_internal_error() -> None:
    # INTERNAL_ERROR is self_heal=True — retries while under budget.
    assert (
        compute_layer_state(_ctx(latest_status="failed", latest_category="totally-made-up", attempts=1))
        is LayerState.RETRYING
    )


def test_cascade_waiting_on_action_needed_upstream() -> None:
    assert (
        compute_layer_state(_ctx(upstream_states={"cik_mapping": LayerState.ACTION_NEEDED}))
        is LayerState.CASCADE_WAITING
    )


def test_cascade_waiting_on_secret_missing_upstream() -> None:
    assert compute_layer_state(_ctx(upstream_states={"news": LayerState.SECRET_MISSING})) is LayerState.CASCADE_WAITING


def test_upstream_degraded_does_not_cascade() -> None:
    assert compute_layer_state(_ctx(upstream_states={"financial_facts": LayerState.DEGRADED})) is LayerState.HEALTHY


def test_upstream_retrying_does_not_cascade() -> None:
    assert compute_layer_state(_ctx(upstream_states={"financial_facts": LayerState.RETRYING})) is LayerState.HEALTHY


def test_content_predicate_failure_marks_degraded() -> None:
    assert compute_layer_state(_ctx(content_ok=False)) is LayerState.DEGRADED


def test_age_past_grace_marks_degraded() -> None:
    assert compute_layer_state(_ctx(age_seconds=80, cadence_seconds=60, grace_multiplier=1.25)) is LayerState.DEGRADED


def test_age_inside_grace_is_healthy() -> None:
    assert compute_layer_state(_ctx(age_seconds=70, cadence_seconds=60, grace_multiplier=1.25)) is LayerState.HEALTHY


def test_local_failure_beats_cascade() -> None:
    # Spec §3.2 rule 4 precedes rule 7 — downstream with own failure
    # surfaces as ACTION_NEEDED, not CASCADE_WAITING, so the operator
    # sees the real failure, not a waiter.
    assert (
        compute_layer_state(
            _ctx(
                latest_status="failed",
                latest_category="schema_drift",
                upstream_states={"cik_mapping": LayerState.ACTION_NEEDED},
            )
        )
        is LayerState.ACTION_NEEDED
    )


def test_cascade_propagates_transitively() -> None:
    # A -> B -> C. A=ACTION_NEEDED, B=CASCADE_WAITING => C must also
    # CASCADE_WAITING so a multi-hop chain does not leave mid-depth
    # layers looking healthy.
    assert compute_layer_state(_ctx(upstream_states={"B": LayerState.CASCADE_WAITING})) is LayerState.CASCADE_WAITING


def test_degraded_upstream_still_does_not_cascade_after_transitive_fix() -> None:
    # Regression guard: the transitive-cascade fix must not start
    # propagating DEGRADED (self-healing) — only CASCADE_WAITING.
    assert compute_layer_state(_ctx(upstream_states={"B": LayerState.DEGRADED})) is LayerState.HEALTHY
