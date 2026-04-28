from typing import Any

from app.services.sync_orchestrator.layer_state import (
    LayerContext,
    compute_layer_state,
)
from app.services.sync_orchestrator.layer_types import LayerState


def _ctx(**overrides: Any) -> LayerContext:
    """Build a LayerContext from defaults + overrides. Keyword-only.

    Typed-Any overrides because pyright (strict) otherwise narrows the
    unpacked defaults-dict to the widest field union and flags every
    kwarg. The LayerContext constructor still validates the types at
    runtime via its dataclass annotations.
    """
    params: dict[str, Any] = {
        "is_enabled": True,
        "is_running": False,
        "latest_status": "complete",
        "latest_category": None,
        "attempts": 0,
        "upstream_states": {},
        "secret_present": True,
        "content_ok": True,
        "age_seconds": 60.0,
        "cadence_seconds": 86400.0,
        "grace_multiplier": 1.25,
        "max_attempts": 3,
    }
    params.update(overrides)
    return LayerContext(
        is_enabled=params["is_enabled"],
        is_running=params["is_running"],
        latest_status=params["latest_status"],
        latest_category=params["latest_category"],
        attempts=params["attempts"],
        upstream_states=params["upstream_states"],
        secret_present=params["secret_present"],
        content_ok=params["content_ok"],
        age_seconds=params["age_seconds"],
        cadence_seconds=params["cadence_seconds"],
        grace_multiplier=params["grace_multiplier"],
        max_attempts=params["max_attempts"],
    )


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


def test_calendar_month_cadence_degrades_at_day_one_tick() -> None:
    """#335 — when the DB-facing builder feeds rule 9 with the calendar
    cadence_seconds (computed by ``Cadence.cadence_seconds_for_state_machine``),
    a monthly_reports run from the previous calendar month flips the
    state to DEGRADED at the day-1 UTC tick instead of after a 31-day
    rolling window.

    Construct the context with the same cadence_seconds the production
    builder would derive at ``now = Feb 1, 2026 00:00 UTC + 1 second``
    for a run that started on Jan 31, 2026 23:59 UTC. The literal age
    is ~1 minute (well under any 31-day window), but rule 9 still
    fires because the calendar boundary collapses the window to the
    last-month / this-month edge.
    """
    from datetime import UTC, datetime

    from app.services.sync_orchestrator.layer_types import Cadence

    cadence = Cadence(calendar_months=1)
    grace = 1.25
    now = datetime(2026, 2, 1, 0, 0, 1, tzinfo=UTC)
    cadence_seconds = cadence.cadence_seconds_for_state_machine(now, grace)
    # Run started Jan 31 23:59 UTC, never finished — anchor is started_at.
    started_at = datetime(2026, 1, 31, 23, 59, tzinfo=UTC)
    age_seconds = (now - started_at).total_seconds()
    assert (
        compute_layer_state(_ctx(age_seconds=age_seconds, cadence_seconds=cadence_seconds, grace_multiplier=grace))
        is LayerState.DEGRADED
    )


def test_calendar_month_cadence_remains_healthy_inside_current_month() -> None:
    """Mirror of the day-1-tick test, the other side of the boundary:
    a run started early in the current calendar month must keep the
    layer HEALTHY for the rest of that month, no matter the literal
    age in days."""
    from datetime import UTC, datetime

    from app.services.sync_orchestrator.layer_types import Cadence

    cadence = Cadence(calendar_months=1)
    grace = 1.25
    # Late in March 2026; ``now`` is Mar 28.
    now = datetime(2026, 3, 28, 18, 0, tzinfo=UTC)
    cadence_seconds = cadence.cadence_seconds_for_state_machine(now, grace)
    # Run started Mar 1 (27 days ago) — a flat ``timedelta(days=31)``
    # check would still call this fresh, but the test pins that the
    # calendar-aware shape doesn't accidentally over-tighten the
    # boundary either.
    started_at = datetime(2026, 3, 1, 6, 0, tzinfo=UTC)
    age_seconds = (now - started_at).total_seconds()
    assert (
        compute_layer_state(_ctx(age_seconds=age_seconds, cadence_seconds=cadence_seconds, grace_multiplier=grace))
        is LayerState.HEALTHY
    )


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
