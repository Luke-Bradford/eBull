"""
Scheduled job stubs.

Each function represents one scheduled job. Wire these into APScheduler
(or equivalent) in a later ticket when the scheduler infrastructure is set up.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Generator
from datetime import UTC, date, datetime, timedelta

import anthropic
import psycopg

from app.config import settings
from app.providers.implementations.companies_house import CompaniesHouseFilingsProvider
from app.providers.implementations.etoro import EtoroMarketDataProvider
from app.providers.implementations.fmp import FmpFundamentalsProvider
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.coverage import review_coverage
from app.services.filings import FilingsRefreshSummary, refresh_filings, upsert_cik_mapping
from app.services.fundamentals import refresh_fundamentals
from app.services.market_data import refresh_market_data
from app.services.ops_monitor import check_row_count_spike, record_job_finish, record_job_start
from app.services.portfolio import run_portfolio_review
from app.services.scoring import compute_rankings
from app.services.sentiment import ClaudeSentimentScorer
from app.services.tax_ledger import ingest_tax_events, run_disposal_matching
from app.services.thesis import find_stale_instruments, generate_thesis
from app.services.universe import sync_universe

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Job tracking helper
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _tracked_job(job_name: str) -> Generator[_JobTracker, None, None]:
    """
    Context manager that records a job_runs row on entry (status=running)
    and updates it on exit (status=success or failure).

    Usage::

        with _tracked_job("nightly_universe_sync") as tracker:
            # ... do work ...
            tracker.row_count = summary.inserted + summary.updated

    If the body raises, the job is recorded as failure with the error message.
    """
    tracker = _JobTracker(job_name)
    try:
        with psycopg.connect(settings.database_url) as conn:
            tracker.run_id = record_job_start(conn, job_name)
    except Exception:
        logger.error("Failed to record job start for %s", job_name, exc_info=True)
        # Still run the job even if tracking fails.
        yield tracker
        return

    try:
        yield tracker
    except Exception as exc:
        try:
            with psycopg.connect(settings.database_url) as conn:
                record_job_finish(conn, tracker.run_id, status="failure", error_msg=str(exc))
        except Exception:
            logger.error("Failed to record job failure for %s", job_name, exc_info=True)
        raise
    else:
        try:
            with psycopg.connect(settings.database_url) as conn:
                record_job_finish(
                    conn,
                    tracker.run_id,
                    status="success",
                    row_count=tracker.row_count,
                )
                # Check for row-count spikes after recording the successful run.
                if tracker.row_count is not None:
                    spike = check_row_count_spike(conn, job_name, tracker.row_count)
                    if spike.flagged:
                        logger.warning("Row-count spike detected: %s", spike.detail)
        except Exception:
            logger.error("Failed to record job success for %s", job_name, exc_info=True)


class _JobTracker:
    """Mutable bag passed into the tracked_job context so the caller can set row_count."""

    def __init__(self, job_name: str) -> None:
        self.job_name = job_name
        self.run_id: int = 0
        self.row_count: int | None = None


def nightly_universe_sync() -> None:
    """
    Sync the eToro tradable instrument universe to the local DB.

    Runs nightly. Idempotent — safe to re-run.
    """
    if not settings.etoro_read_api_key:
        logger.error("nightly_universe_sync: ETORO_READ_API_KEY not set, skipping")
        return

    with _tracked_job("nightly_universe_sync") as tracker:
        with (
            EtoroMarketDataProvider(api_key=settings.etoro_read_api_key, env=settings.etoro_env) as provider,
            psycopg.connect(settings.database_url) as conn,
        ):
            summary = sync_universe(provider, conn)
        tracker.row_count = summary.inserted + summary.updated

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

    with _tracked_job("hourly_market_refresh") as tracker:
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
        tracker.row_count = summary.candle_rows_upserted + summary.quotes_updated

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
    with _tracked_job("daily_cik_refresh") as tracker:
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as provider,
            psycopg.connect(settings.database_url) as conn,
        ):
            mapping = provider.build_cik_mapping()

            rows = conn.execute(
                "SELECT symbol, instrument_id::text FROM instruments WHERE is_tradable = TRUE"
            ).fetchall()
            instrument_symbols = [(row[0], row[1]) for row in rows]

            upserted = upsert_cik_mapping(conn, mapping, instrument_symbols)
        tracker.row_count = upserted

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

    with _tracked_job("daily_research_refresh") as tracker:
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

        total_rows = 0

        # Fundamentals (FMP)
        if settings.fmp_api_key:
            with (
                FmpFundamentalsProvider(api_key=settings.fmp_api_key) as fmp,
                psycopg.connect(settings.database_url) as conn,
            ):
                summary = refresh_fundamentals(fmp, conn, symbols)
            total_rows += summary.snapshots_upserted
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
        total_rows += sec_summary.filings_upserted
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
            total_rows += ch_summary.filings_upserted
            logger.info(
                "CH filings refresh: attempted=%d upserted=%d skipped=%d",
                ch_summary.instruments_attempted,
                ch_summary.filings_upserted,
                ch_summary.instruments_skipped,
            )

        tracker.row_count = total_rows


def daily_news_refresh() -> None:
    """
    Fetch, deduplicate, and score news events for all active Tier 1/2 instruments.

    Runs daily (or on-demand). Idempotent — safe to re-run.
    Requires ANTHROPIC_API_KEY to be set; skips sentiment scoring otherwise.
    """
    if not settings.anthropic_api_key:
        logger.error("daily_news_refresh: ANTHROPIC_API_KEY not set, skipping")
        return

    to_dt = datetime.now(tz=UTC)
    from_dt = to_dt - timedelta(hours=72)

    with _tracked_job("daily_news_refresh") as tracker:
        # The DB connection is opened once and kept open for the full pipeline.
        # refresh_news() performs DB reads (dedup checks) and writes (upserts)
        # throughout its execution — the connection must not be closed early.
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
                logger.info("daily_news_refresh: no covered instruments found, skipping")
                return

            instrument_symbols = [(row[0], row[1]) for row in rows]
            scorer = ClaudeSentimentScorer(api_key=settings.anthropic_api_key)

            # NewsProvider: no concrete implementation wired in v1.
            # Wire a real provider here once one is available (e.g. Benzinga, NewsAPI).
            # When wired, replace the warning + return with:
            #   summary = refresh_news(provider, scorer, conn, instrument_symbols, from_dt, to_dt)
            #   logger.info("News refresh: %s", summary)
            logger.warning("daily_news_refresh: no NewsProvider implementation wired in v1 — skipping fetch")
            _ = instrument_symbols
            _ = scorer
            _ = from_dt
            _ = to_dt
        tracker.row_count = 0


def daily_thesis_refresh() -> None:
    """
    Regenerate theses for stale Tier 1 instruments.

    An instrument is stale when:
      - it has no thesis row, or
      - its most recent thesis is older than coverage.review_frequency allows.

    Requires ANTHROPIC_API_KEY. Skips silently if not set.
    Each instrument is processed independently — a failure on one does not
    abort the rest of the batch.
    """
    if not settings.anthropic_api_key:
        logger.error("daily_thesis_refresh: ANTHROPIC_API_KEY not set, skipping")
        return

    with _tracked_job("daily_thesis_refresh") as tracker:
        logger.info("daily_thesis_refresh: checking for stale Tier 1 instruments")
        try:
            with psycopg.connect(settings.database_url) as conn:
                stale = find_stale_instruments(conn, tier=1)
        except Exception:
            logger.error("daily_thesis_refresh: failed to query stale instruments", exc_info=True)
            return

        if not stale:
            logger.info("daily_thesis_refresh: no stale Tier 1 instruments found")
            tracker.row_count = 0
            return

        logger.info("daily_thesis_refresh: %d stale instrument(s) to refresh", len(stale))

        claude_client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

        generated = 0
        skipped = 0
        for item in stale:
            try:
                with psycopg.connect(settings.database_url) as conn:
                    generate_thesis(
                        instrument_id=item.instrument_id,
                        conn=conn,
                        client=claude_client,
                    )
                generated += 1
            except Exception:
                logger.warning(
                    "daily_thesis_refresh: failed for symbol=%s instrument_id=%d, skipping",
                    item.symbol,
                    item.instrument_id,
                    exc_info=True,
                )
                skipped += 1

        tracker.row_count = generated

    logger.info(
        "daily_thesis_refresh complete: generated=%d skipped=%d",
        generated,
        skipped,
    )


def morning_candidate_review() -> None:
    """
    Re-score, rank, and generate trade recommendations for Tier 1 candidates.

    Steps (run sequentially on the same connection for each phase):
      1. Score all eligible Tier 1 instruments (v1-balanced).
      2. Run portfolio review to produce BUY/ADD/HOLD/EXIT recommendations.

    Each phase opens its own connection so a failure in recommendations
    does not roll back the completed scoring run.
    """
    with _tracked_job("morning_candidate_review") as tracker:
        logger.info("morning_candidate_review: starting scoring run")
        try:
            with psycopg.connect(settings.database_url) as conn:
                score_result = compute_rankings(conn)
        except Exception:
            logger.error("morning_candidate_review: scoring run failed", exc_info=True)
            return

        if not score_result.scored:
            logger.info("morning_candidate_review: no eligible instruments to score")
            tracker.row_count = 0
            return

        top5 = score_result.scored[:5]
        top5_summary = ", ".join(
            f"instrument_id={r.instrument_id} score={r.total_score:.3f} rank={r.rank}" for r in top5
        )
        logger.info(
            "morning_candidate_review: scored %d instruments [model=%s] top5=[%s]",
            len(score_result.scored),
            score_result.model_version,
            top5_summary,
        )

        logger.info("morning_candidate_review: starting portfolio review")
        try:
            with psycopg.connect(settings.database_url) as conn:
                rec_result = run_portfolio_review(conn, model_version=score_result.model_version)
        except Exception:
            logger.error("morning_candidate_review: portfolio review failed", exc_info=True)
            return

        tracker.row_count = len(score_result.scored) + len(rec_result.recommendations)

    logger.info(
        "morning_candidate_review: recommendations=%d (BUY=%d ADD=%d HOLD=%d EXIT=%d) aum=%.2f",
        len(rec_result.recommendations),
        sum(1 for r in rec_result.recommendations if r.action == "BUY"),
        sum(1 for r in rec_result.recommendations if r.action == "ADD"),
        sum(1 for r in rec_result.recommendations if r.action == "HOLD"),
        sum(1 for r in rec_result.recommendations if r.action == "EXIT"),
        rec_result.total_aum,
    )


def weekly_coverage_review() -> None:
    """
    Review coverage tier assignments; promote/demote instruments.

    Runs weekly. Evaluates all instruments with coverage rows against
    deterministic promotion/demotion rules. Enforces Tier 1 cap.
    All changes are recorded in coverage_audit.
    """
    with _tracked_job("weekly_coverage_review") as tracker:
        logger.info("weekly_coverage_review: starting coverage tier review")
        try:
            with psycopg.connect(settings.database_url) as conn:
                result = review_coverage(conn)
        except Exception:
            logger.error("weekly_coverage_review: failed", exc_info=True)
            return

        tracker.row_count = len(result.promotions) + len(result.demotions)

    logger.info(
        "weekly_coverage_review complete: promotions=%d demotions=%d blocked=%d unchanged=%d",
        len(result.promotions),
        len(result.demotions),
        len(result.blocked),
        result.unchanged,
    )


def daily_tax_reconciliation() -> None:
    """
    Ingest new fills into tax_lots and re-run disposal matching.

    Runs daily. Idempotent — safe to re-run. Requires fx_rates to be
    populated for any non-GBP instruments before ingestion.

    Two separate connections are used intentionally so that a matching
    failure does not roll back committed tax_lot ingestion. If the
    process crashes between ingestion and matching, disposal_matches
    will be stale until the next run — acceptable because matching is
    a full delete-and-recompute and will self-correct on re-run.
    """
    with _tracked_job("daily_tax_reconciliation") as tracker:
        logger.info("daily_tax_reconciliation: starting")

        try:
            with psycopg.connect(settings.database_url) as conn:
                ingestion = ingest_tax_events(conn)
        except Exception:
            logger.error("daily_tax_reconciliation: ingestion failed", exc_info=True)
            return

        logger.info(
            "daily_tax_reconciliation: ingested fills=%d cash_events=%d already_present=%d",
            ingestion.fills_ingested,
            ingestion.cash_events_ingested,
            ingestion.already_present,
        )

        try:
            with psycopg.connect(settings.database_url) as conn:
                matching = run_disposal_matching(conn)
        except Exception:
            logger.error("daily_tax_reconciliation: matching failed", exc_info=True)
            return

        tracker.row_count = ingestion.fills_ingested + matching.matches_created

    logger.info(
        "daily_tax_reconciliation complete: instruments=%d matches=%d gain=%.2f loss=%.2f",
        matching.instruments_processed,
        matching.matches_created,
        matching.total_gain_gbp,
        matching.total_loss_gbp,
    )
