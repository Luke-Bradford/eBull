from datetime import timedelta

from app.services.sync_orchestrator.content_predicates import (
    candles_content_ok,
    fundamentals_content_ok,
)
from app.services.sync_orchestrator.layer_types import (
    DEFAULT_RETRY_POLICY,
    Cadence,
)
from app.services.sync_orchestrator.registry import LAYERS

EXPECTED_CADENCES: dict[str, timedelta] = {
    "universe": timedelta(days=7),
    "candles": timedelta(hours=24),
    "fundamentals": timedelta(days=90),
    "scoring": timedelta(hours=24),
    "recommendations": timedelta(hours=24),
    "portfolio_sync": timedelta(minutes=5),
    "fx_rates": timedelta(minutes=5),
    "cost_models": timedelta(hours=24),
    "weekly_reports": timedelta(days=7),
    # monthly_reports uses a calendar-anchored cadence (#335); it is
    # asserted separately rather than mapped to a fixed timedelta.
}


def test_every_layer_has_typed_cadence() -> None:
    for name, layer in LAYERS.items():
        assert isinstance(layer.cadence, Cadence), f"{name} cadence is not Cadence"


def test_cadence_intervals_match_expected() -> None:
    for name, expected in EXPECTED_CADENCES.items():
        assert LAYERS[name].cadence.interval == expected, (
            f"{name} cadence interval {LAYERS[name].cadence.interval} != {expected}"
        )


def test_monthly_reports_uses_calendar_cadence() -> None:
    """#335 — monthly_reports must be calendar-anchored, not a flat 31d."""
    cadence = LAYERS["monthly_reports"].cadence
    assert cadence.calendar_months == 1, (
        f"monthly_reports cadence must be calendar-anchored (calendar_months=1); got {cadence}"
    )
    assert cadence.interval is None


def test_minute_cadence_layers_have_tighter_retry_policy() -> None:
    for name in ("fx_rates", "portfolio_sync"):
        policy = LAYERS[name].retry_policy
        assert policy.max_attempts == 5
        assert policy.backoff_seconds == (30, 60, 120, 300, 600)


def test_daily_layers_use_default_retry_policy() -> None:
    for name in ("candles", "fundamentals", "scoring"):
        assert LAYERS[name].retry_policy == DEFAULT_RETRY_POLICY


def test_every_layer_has_non_empty_plain_language_sla() -> None:
    for name, layer in LAYERS.items():
        assert layer.plain_language_sla, f"{name} missing plain_language_sla"


def test_grace_multiplier_default() -> None:
    for name, layer in LAYERS.items():
        assert layer.grace_multiplier == 1.25


# News + thesis retired from orchestrator layers in Phase 1.2 — they
# are now on-demand endpoints (POST /instruments/{symbol}/thesis) that
# check ANTHROPIC_API_KEY at request time via a FastAPI dependency
# (app.api.theses.get_anthropic_client). No remaining scheduled layer
# declares the secret; the former test is no longer applicable.


def test_market_data_layers_declare_no_env_secrets() -> None:
    assert LAYERS["candles"].secret_refs == ()
    assert LAYERS["fundamentals"].secret_refs == ()


def test_candles_has_content_predicate() -> None:
    assert LAYERS["candles"].content_predicate is candles_content_ok


def test_fundamentals_has_content_predicate() -> None:
    assert LAYERS["fundamentals"].content_predicate is fundamentals_content_ok


def test_layers_without_content_predicate_have_none() -> None:
    for name in ("universe", "scoring", "portfolio_sync", "recommendations"):
        assert LAYERS[name].content_predicate is None
