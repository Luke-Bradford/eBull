"""
Scheduled job stubs.

Each function represents one scheduled job. Wire these into APScheduler
(or equivalent) in a later ticket when the scheduler infrastructure is set up.
"""

import logging
from datetime import date, timedelta

import psycopg

from app.config import settings
from app.providers.implementations.etoro import EtoroMarketDataProvider
from app.services.market_data import refresh_market_data
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

    with (
        EtoroMarketDataProvider(api_key=settings.etoro_read_api_key, env=settings.etoro_env) as provider,
        psycopg.connect(settings.database_url) as conn,
    ):
        summary = sync_universe(provider, conn)

    logger.info(
        "Universe sync complete: inserted=%d updated=%d deactivated=%d",
        summary.inserted,
        summary.updated,
        summary.deactivated,
    )


def hourly_market_refresh() -> None:
    """
    Refresh quotes and candles for all active Tier 1/2 instruments.

    Fetches candles from the last 400 days (enough for 1y return + buffer)
    and the current quote for each covered instrument.
    """
    if not settings.etoro_read_api_key:
        logger.error("hourly_market_refresh: ETORO_READ_API_KEY not set, skipping")
        return

    to_date = date.today()
    from_date = to_date - timedelta(days=400)

    with (
        EtoroMarketDataProvider(api_key=settings.etoro_read_api_key, env=settings.etoro_env) as provider,
        psycopg.connect(settings.database_url) as conn,
    ):
        rows = conn.execute(
            """
            SELECT i.symbol, i.instrument_id::text
            FROM instruments i
            JOIN coverage c ON c.instrument_id = i.instrument_id
            WHERE i.is_tradable = TRUE
              AND c.coverage_tier IN (1, 2)
            ORDER BY i.symbol
            """
        ).fetchall()

        if not rows:
            logger.info("hourly_market_refresh: no covered instruments found, skipping")
            return

        symbols = [(row[0], row[1]) for row in rows]
        summary = refresh_market_data(provider, conn, symbols, from_date, to_date)

    logger.info(
        "Market refresh complete: symbols=%d candles=%d features=%d quotes=%d spread_flags=%d",
        summary.symbols_refreshed,
        summary.candle_rows_upserted,
        summary.features_computed,
        summary.quotes_updated,
        summary.spread_flags_set,
    )


def daily_research_refresh() -> None:
    """Refresh filings, fundamentals, news, and theses for covered instruments."""
    raise NotImplementedError("Implemented in issues #4, #5, #6")


def morning_candidate_review() -> None:
    """Re-score and rank Tier 1 candidates, produce trade recommendations."""
    raise NotImplementedError("Implemented in issues #7, #8")


def weekly_coverage_review() -> None:
    """Review coverage tier assignments; promote/demote instruments."""
    raise NotImplementedError("Implemented in issue #12")
