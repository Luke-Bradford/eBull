"""
Scheduled job stubs.

Each function represents one scheduled job. Wire these into APScheduler
(or equivalent) in a later ticket when the scheduler infrastructure is set up.
"""

import logging
from datetime import date, timedelta

import psycopg

from app.config import settings
from app.providers.implementations.companies_house import CompaniesHouseFilingsProvider
from app.providers.implementations.etoro import EtoroMarketDataProvider
from app.providers.implementations.fmp import FmpFundamentalsProvider
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.filings import FilingsRefreshSummary, refresh_filings, upsert_cik_mapping
from app.services.fundamentals import refresh_fundamentals
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


def daily_cik_refresh() -> None:
    """
    Refresh SEC ticker → CIK mapping and upsert into external_identifiers.

    Runs daily. Idempotent — safe to re-run.
    """
    with (
        SecFilingsProvider(user_agent=settings.sec_user_agent) as provider,
        psycopg.connect(settings.database_url) as conn,
    ):
        mapping = provider.build_cik_mapping()

        rows = conn.execute("SELECT symbol, instrument_id::text FROM instruments WHERE is_tradable = TRUE").fetchall()
        instrument_symbols = [(row[0], row[1]) for row in rows]

        upserted = upsert_cik_mapping(conn, mapping, instrument_symbols)

    logger.info("CIK refresh complete: mapping_size=%d upserted=%d", len(mapping), upserted)


def daily_research_refresh() -> None:
    """
    Refresh fundamentals and filings for all active Tier 1/2 instruments.

    Runs daily. Fetches:
      - FMP fundamentals snapshot (latest) for each covered symbol
      - SEC EDGAR filing metadata for US instruments with a known CIK
      - Companies House filing metadata for UK instruments with a company_number
    """
    if not settings.fmp_api_key:
        logger.error("daily_research_refresh: FMP_API_KEY not set, skipping fundamentals")
    if not settings.companies_house_api_key:
        logger.warning("daily_research_refresh: COMPANIES_HOUSE_API_KEY not set, skipping CH filings")

    with psycopg.connect(settings.database_url) as conn:
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
        logger.info("daily_research_refresh: no covered instruments found, skipping")
        return

    symbols = [(row[0], row[1]) for row in rows]
    instrument_ids = [row[1] for row in rows]
    from_date = date.today() - timedelta(days=30)
    to_date = date.today()

    # Fundamentals (FMP)
    if settings.fmp_api_key:
        with (
            FmpFundamentalsProvider(api_key=settings.fmp_api_key) as fmp,
            psycopg.connect(settings.database_url) as conn,
        ):
            summary = refresh_fundamentals(fmp, conn, symbols)
        logger.info(
            "Fundamentals refresh: attempted=%d upserted=%d skipped=%d",
            summary.symbols_attempted,
            summary.snapshots_upserted,
            summary.symbols_skipped,
        )

    # Filings — SEC EDGAR
    with (
        SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
        psycopg.connect(settings.database_url) as conn,
    ):
        sec_summary = refresh_filings(
            provider=sec,
            provider_name="sec",
            identifier_type="cik",
            conn=conn,
            instrument_ids=instrument_ids,
            start_date=from_date,
            end_date=to_date,
            filing_types=["10-K", "10-Q", "8-K"],
        )
    logger.info(
        "SEC filings refresh: attempted=%d upserted=%d skipped=%d",
        sec_summary.instruments_attempted,
        sec_summary.filings_upserted,
        sec_summary.instruments_skipped,
    )

    # Filings — Companies House
    if settings.companies_house_api_key:
        with (
            CompaniesHouseFilingsProvider(api_key=settings.companies_house_api_key) as ch,
            psycopg.connect(settings.database_url) as conn,
        ):
            ch_summary: FilingsRefreshSummary = refresh_filings(
                provider=ch,
                provider_name="companies_house",
                identifier_type="company_number",
                conn=conn,
                instrument_ids=instrument_ids,
                start_date=from_date,
                end_date=to_date,
            )
        logger.info(
            "CH filings refresh: attempted=%d upserted=%d skipped=%d",
            ch_summary.instruments_attempted,
            ch_summary.filings_upserted,
            ch_summary.instruments_skipped,
        )


def morning_candidate_review() -> None:
    """Re-score and rank Tier 1 candidates, produce trade recommendations."""
    raise NotImplementedError("Implemented in issues #7, #8")


def weekly_coverage_review() -> None:
    """Review coverage tier assignments; promote/demote instruments."""
    raise NotImplementedError("Implemented in issue #12")
