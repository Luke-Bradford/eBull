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
    refresh_cik_mapping,
    refresh_cost_models,
    refresh_financial_facts_and_normalization,
    refresh_fundamentals,
    refresh_fx_rates,
    refresh_monthly_reports,
    refresh_news,
    refresh_portfolio_sync,
    refresh_scoring_and_recommendations,
    refresh_thesis,
    refresh_universe,
    refresh_weekly_reports,
)
from app.services.sync_orchestrator.content_predicates import (
    candles_content_ok,
    fundamentals_content_ok,
)
from app.services.sync_orchestrator.freshness import (
    candles_is_fresh,
    cik_mapping_is_fresh,
    cost_models_is_fresh,
    financial_facts_is_fresh,
    financial_normalization_is_fresh,
    fundamentals_is_fresh,
    fx_rates_is_fresh,
    monthly_reports_is_fresh,
    news_is_fresh,
    portfolio_sync_is_fresh,
    recommendations_is_fresh,
    scoring_is_fresh,
    thesis_is_fresh,
    universe_is_fresh,
    weekly_reports_is_fresh,
)
from app.services.sync_orchestrator.layer_types import (
    Cadence,
    ContentPredicate,
    DEFAULT_RETRY_POLICY,
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
    is_fresh: Callable[[psycopg.Connection[Any]], tuple[bool, str]]
    refresh: LayerRefresh
    dependencies: tuple[str, ...] = ()
    is_blocking: bool = True
    grace_multiplier: float = 1.25
    retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY
    secret_refs: tuple[SecretRef, ...] = ()
    content_predicate: ContentPredicate | None = None
    plain_language_sla: str = ""


_TIGHT_RETRY = RetryPolicy(max_attempts=5, backoff_seconds=(30, 60, 120, 300, 600))

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
    "cik_mapping": DataLayer(
        name="cik_mapping",
        display_name="SEC CIK Mapping",
        tier=0,
        cadence=Cadence(interval=timedelta(hours=24)),
        is_fresh=cik_mapping_is_fresh,
        refresh=refresh_cik_mapping,
        dependencies=("universe",),
        plain_language_sla="Refreshed nightly from SEC company_tickers.json.",
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
    "financial_facts": DataLayer(
        name="financial_facts",
        display_name="SEC EDGAR XBRL Facts",
        tier=1,
        cadence=Cadence(interval=timedelta(hours=24)),
        is_fresh=financial_facts_is_fresh,
        refresh=refresh_financial_facts_and_normalization,
        dependencies=("cik_mapping",),
        plain_language_sla="Refreshed nightly from SEC XBRL filings.",
    ),
    "financial_normalization": DataLayer(
        name="financial_normalization",
        display_name="Financial Period Normalization",
        tier=2,
        cadence=Cadence(interval=timedelta(hours=24)),
        is_fresh=financial_normalization_is_fresh,
        refresh=refresh_financial_facts_and_normalization,
        dependencies=("financial_facts",),
        plain_language_sla="Derived nightly from SEC XBRL facts.",
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
    "news": DataLayer(
        name="news",
        display_name="News & Sentiment",
        tier=1,
        cadence=Cadence(interval=timedelta(hours=4)),
        is_fresh=news_is_fresh,
        refresh=refresh_news,
        dependencies=("universe",),
        is_blocking=False,
        secret_refs=(SecretRef(env_var="ANTHROPIC_API_KEY", display_name="Anthropic API key"),),
        plain_language_sla="Refreshed every 4h — news + sentiment scoring.",
    ),
    "thesis": DataLayer(
        name="thesis",
        display_name="Investment Thesis",
        tier=2,
        cadence=Cadence(interval=timedelta(hours=24)),
        is_fresh=thesis_is_fresh,
        refresh=refresh_thesis,
        dependencies=("fundamentals", "financial_normalization", "news"),
        secret_refs=(SecretRef(env_var="ANTHROPIC_API_KEY", display_name="Anthropic API key"),),
        plain_language_sla="Refreshed nightly for stale Tier 1 tickers.",
    ),
    "scoring": DataLayer(
        name="scoring",
        display_name="Ranking Scores",
        tier=3,
        cadence=Cadence(interval=timedelta(hours=24)),
        is_fresh=scoring_is_fresh,
        refresh=refresh_scoring_and_recommendations,
        dependencies=("thesis", "candles"),
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
        retry_policy=_TIGHT_RETRY,
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
        retry_policy=_TIGHT_RETRY,
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
    # In-DAG (13 entries, non-empty tuples):
    "nightly_universe_sync": ("universe",),
    "daily_cik_refresh": ("cik_mapping",),
    "daily_candle_refresh": ("candles",),
    "daily_financial_facts": ("financial_facts", "financial_normalization"),
    "daily_research_refresh": ("fundamentals",),
    "daily_news_refresh": ("news",),
    "daily_thesis_refresh": ("thesis",),
    "daily_portfolio_sync": ("portfolio_sync",),
    "morning_candidate_review": ("scoring", "recommendations"),
    "seed_cost_models": ("cost_models",),
    "weekly_report": ("weekly_reports",),
    "monthly_report": ("monthly_reports",),
    "fx_rates_refresh": ("fx_rates",),
    # Outside-DAG (7 entries, empty tuples):
    "execute_approved_orders": (),
    "weekly_coverage_review": (),
    "weekly_coverage_audit": (),
    "retry_deferred_recommendations": (),
    "monitor_positions": (),
    "attribution_summary": (),
    "daily_tax_reconciliation": (),
}
