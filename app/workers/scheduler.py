"""
Scheduled job stubs.

Each function represents one scheduled job. Wire these into APScheduler
(or equivalent) in a later ticket when the scheduler infrastructure is set up.
"""

import logging

import psycopg

from app.config import settings
from app.providers.implementations.etoro import EtoroMarketDataProvider
from app.services.universe import sync_universe

logger = logging.getLogger(__name__)


def nightly_universe_sync() -> None:
    """
    Sync the eToro tradable instrument universe to the local DB.

    Runs nightly. Idempotent — safe to re-run.
    """
    if not settings.etoro_read_api_key:
        logger.error("nightly_universe_sync: ETORO_READ_API_KEY not set, skipping")
        return

    provider = EtoroMarketDataProvider(
        api_key=settings.etoro_read_api_key,
        env=settings.etoro_env,
    )

    with psycopg.connect(settings.database_url) as conn:
        summary = sync_universe(provider, conn)

    logger.info(
        "Universe sync complete: inserted=%d updated=%d deactivated=%d",
        summary.inserted,
        summary.updated,
        summary.deactivated,
    )


def hourly_market_refresh() -> None:
    """Refresh quotes and candles for all active Tier 1/2 instruments."""
    raise NotImplementedError("Implemented in issue #3")


def daily_research_refresh() -> None:
    """Refresh filings, fundamentals, news, and theses for covered instruments."""
    raise NotImplementedError("Implemented in issues #4, #5, #6")


def morning_candidate_review() -> None:
    """Re-score and rank Tier 1 candidates, produce trade recommendations."""
    raise NotImplementedError("Implemented in issues #7, #8")


def weekly_coverage_review() -> None:
    """Review coverage tier assignments; promote/demote instruments."""
    raise NotImplementedError("Implemented in issue #12")
