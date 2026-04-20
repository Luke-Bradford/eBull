"""LAYERS and JOB_TO_LAYERS registries.

Adapter functions are wired in Task 10 (adapters.py); the registry
here declares the DAG structure. is_blocking, dependencies, and
display_name all come from spec §1.1 + §2.4.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import psycopg

from app.services.sync_orchestrator.adapters import (
    refresh_candles,
    refresh_cost_models,
    refresh_fundamentals,
    refresh_fx_rates,
    refresh_monthly_reports,
    refresh_portfolio_sync,
    refresh_scoring_and_recommendations,
    refresh_universe,
    refresh_weekly_reports,
)
from app.services.sync_orchestrator.content_predicates import (
    candles_content_ok,
    fundamentals_content_ok,
)
from app.services.sync_orchestrator.freshness import (
    candles_is_fresh,
    cost_models_is_fresh,
    fundamentals_is_fresh,
    fx_rates_is_fresh,
    monthly_reports_is_fresh,
    portfolio_sync_is_fresh,
    recommendations_is_fresh,
    scoring_is_fresh,
    universe_is_fresh,
    weekly_reports_is_fresh,
)
from app.services.sync_orchestrator.layer_types import (
    DEFAULT_RETRY_POLICY,
    Cadence,
    ContentPredicate,
    RetryPolicy,
    SecretRef,
)
from app.services.sync_orchestrator.types import LayerRefresh


@dataclass(frozen=True)
class DataLayer:
    name: str
    display_name: str
    tier: int
    cadence: Cadence
    # Legacy combined audit-age + content predicate. Retained until chunk 7
    # retires freshness.py; the new state machine (chunk 4) calls
    # `content_predicate` + its own age check separately.
    is_fresh: Callable[[psycopg.Connection[Any]], tuple[bool, str]]
    refresh: LayerRefresh
    dependencies: tuple[str, ...] = ()
    is_blocking: bool = True
    grace_multiplier: float = 1.25
    retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY
    secret_refs: tuple[SecretRef, ...] = ()
    content_predicate: ContentPredicate | None = None
    plain_language_sla: str = ""


MINUTE_LAYER_RETRY = RetryPolicy(max_attempts=5, backoff_seconds=(30, 60, 120, 300, 600))

LAYERS: dict[str, DataLayer] = {
    "universe": DataLayer(
        name="universe",
        display_name="Tradable Universe",
        tier=0,
        cadence=Cadence(interval=timedelta(days=7)),
        is_fresh=universe_is_fresh,
        refresh=refresh_universe,
        dependencies=(),
        plain_language_sla="Refreshed weekly — eToro instrument list.",
    ),
    "candles": DataLayer(
        name="candles",
        display_name="Daily Price Candles",
        tier=1,
        cadence=Cadence(interval=timedelta(hours=24)),
        is_fresh=candles_is_fresh,
        refresh=refresh_candles,
        dependencies=("universe",),
        content_predicate=candles_content_ok,
        plain_language_sla="Refreshed every trading day after market close.",
    ),
    "fundamentals": DataLayer(
        name="fundamentals",
        display_name="Fundamentals Snapshot",
        tier=1,
        cadence=Cadence(interval=timedelta(days=90)),
        is_fresh=fundamentals_is_fresh,
        refresh=refresh_fundamentals,
        dependencies=("universe",),
        content_predicate=fundamentals_content_ok,
        plain_language_sla="Refreshed quarterly alongside earnings.",
    ),
    "scoring": DataLayer(
        name="scoring",
        display_name="Ranking Scores",
        tier=3,
        cadence=Cadence(interval=timedelta(hours=24)),
        is_fresh=scoring_is_fresh,
        refresh=refresh_scoring_and_recommendations,
        dependencies=("candles", "fundamentals"),
        plain_language_sla="Refreshed every morning pre-market.",
    ),
    "recommendations": DataLayer(
        name="recommendations",
        display_name="Trade Recommendations",
        tier=3,
        cadence=Cadence(interval=timedelta(hours=24)),
        is_fresh=recommendations_is_fresh,
        refresh=refresh_scoring_and_recommendations,
        dependencies=("scoring",),
        plain_language_sla="Refreshed every morning after scoring.",
    ),
    "portfolio_sync": DataLayer(
        name="portfolio_sync",
        display_name="Portfolio Sync",
        tier=0,
        cadence=Cadence(interval=timedelta(minutes=5)),
        is_fresh=portfolio_sync_is_fresh,
        refresh=refresh_portfolio_sync,
        dependencies=(),
        is_blocking=False,
        retry_policy=MINUTE_LAYER_RETRY,
        plain_language_sla="Synced every 5 minutes against eToro.",
    ),
    "fx_rates": DataLayer(
        name="fx_rates",
        display_name="FX Rates",
        tier=0,
        cadence=Cadence(interval=timedelta(minutes=5)),
        is_fresh=fx_rates_is_fresh,
        refresh=refresh_fx_rates,
        dependencies=(),
        is_blocking=False,
        retry_policy=MINUTE_LAYER_RETRY,
        plain_language_sla="Refreshed every 5 minutes for live valuation.",
    ),
    "cost_models": DataLayer(
        name="cost_models",
        display_name="Transaction Cost Models",
        tier=2,
        cadence=Cadence(interval=timedelta(hours=24)),
        is_fresh=cost_models_is_fresh,
        refresh=refresh_cost_models,
        dependencies=("universe",),
        plain_language_sla="Re-seeded nightly.",
    ),
    "weekly_reports": DataLayer(
        name="weekly_reports",
        display_name="Weekly Performance Report",
        tier=3,
        cadence=Cadence(interval=timedelta(days=7)),
        is_fresh=weekly_reports_is_fresh,
        refresh=refresh_weekly_reports,
        dependencies=(),
        is_blocking=False,
        plain_language_sla="Published every Monday morning.",
    ),
    "monthly_reports": DataLayer(
        name="monthly_reports",
        display_name="Monthly Performance Report",
        tier=3,
        cadence=Cadence(interval=timedelta(days=31)),
        is_fresh=monthly_reports_is_fresh,
        refresh=refresh_monthly_reports,
        dependencies=(),
        is_blocking=False,
        plain_language_sla="Published on the 1st of every month.",
    ),
}


# Mapping: legacy job name (matches _INVOKERS key in app/jobs/runtime.py)
# to tuple of emitted layer names. Empty tuple = outside-DAG job (stays
# as-is in Phase 1–3, dashboard shows in "Background tasks" panel).
JOB_TO_LAYERS: dict[str, tuple[str, ...]] = {
    # In-DAG (9 entries, non-empty tuples):
    "nightly_universe_sync": ("universe",),
    "daily_candle_refresh": ("candles",),
    "daily_research_refresh": ("fundamentals",),
    "daily_portfolio_sync": ("portfolio_sync",),
    "morning_candidate_review": ("scoring", "recommendations"),
    "seed_cost_models": ("cost_models",),
    "weekly_report": ("weekly_reports",),
    "monthly_report": ("monthly_reports",),
    "fx_rates_refresh": ("fx_rates",),
    # Outside-DAG (6 entries, empty tuples):
    "execute_approved_orders": (),
    "fundamentals_sync": (),
    "retry_deferred_recommendations": (),
    "monitor_positions": (),
    "attribution_summary": (),
    "daily_tax_reconciliation": (),
}
