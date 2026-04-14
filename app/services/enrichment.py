"""
Enrichment service.

Fetches supplemental instrument data (company profile, earnings calendar,
analyst consensus estimates) from an EnrichmentProvider and upserts it into
the DB.

The service layer owns identifier resolution and DB writes.
The provider is a pure HTTP client and owns no DB access.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import psycopg

from app.providers.enrichment import (
    AnalystEstimates,
    EarningsEvent,
    EnrichmentProvider,
    InstrumentProfileData,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EnrichmentRefreshSummary:
    symbols_attempted: int
    profiles_upserted: int
    earnings_upserted: int
    estimates_upserted: int
    symbols_skipped: int


def refresh_enrichment(
    provider: EnrichmentProvider,
    conn: psycopg.Connection,  # type: ignore[type-arg]
    symbols: Sequence[tuple[str, str]],  # [(symbol, instrument_id), ...]
) -> EnrichmentRefreshSummary:
    """
    For each symbol, fetch and upsert profile, earnings events, and analyst
    estimates.

    symbols is a sequence of (symbol, instrument_id) tuples.  If the provider
    raises for a symbol, that symbol is counted as skipped and the batch
    continues.
    """
    profiles_upserted = 0
    earnings_upserted = 0
    estimates_upserted = 0
    skipped = 0
    now = datetime.now(tz=UTC)

    for symbol, instrument_id in symbols:
        try:
            profile = provider.get_profile_enrichment(symbol)
            events = provider.get_earnings_calendar(symbol)
            est = provider.get_analyst_estimates(symbol)

            # Wrap all DB writes for this symbol in a savepoint so a
            # constraint violation or transient error doesn't abort the
            # outer transaction and silently drop prior symbols' writes.
            with conn.transaction():
                if profile is not None:
                    _upsert_profile(conn, instrument_id, profile, now)
                    profiles_upserted += 1

                if events:
                    _upsert_earnings_events(conn, instrument_id, events)
                    earnings_upserted += len(events)

                if est is not None:
                    _upsert_analyst_estimates(conn, instrument_id, est)
                    estimates_upserted += 1

        except Exception:
            logger.warning(
                "Enrichment: failed to refresh %s, skipping",
                symbol,
                exc_info=True,
            )
            skipped += 1

    return EnrichmentRefreshSummary(
        symbols_attempted=len(symbols),
        profiles_upserted=profiles_upserted,
        earnings_upserted=earnings_upserted,
        estimates_upserted=estimates_upserted,
        symbols_skipped=skipped,
    )


def _upsert_profile(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: str,
    profile: InstrumentProfileData,
    now: datetime,
) -> None:
    """
    Upsert a single instrument profile row into instrument_profile.
    Idempotent — keyed on instrument_id.
    Skips write if all tracked fields are unchanged (IS DISTINCT FROM).
    """
    conn.execute(
        """
        INSERT INTO instrument_profile (
            instrument_id,
            beta, public_float, avg_volume_30d, market_cap,
            employees, ipo_date, is_actively_trading, fetched_at
        )
        VALUES (
            %(instrument_id)s,
            %(beta)s, %(public_float)s, %(avg_volume_30d)s, %(market_cap)s,
            %(employees)s, %(ipo_date)s, %(is_actively_trading)s, %(fetched_at)s
        )
        ON CONFLICT (instrument_id) DO UPDATE SET
            beta                = EXCLUDED.beta,
            public_float        = EXCLUDED.public_float,
            avg_volume_30d      = EXCLUDED.avg_volume_30d,
            market_cap          = EXCLUDED.market_cap,
            employees           = EXCLUDED.employees,
            ipo_date            = EXCLUDED.ipo_date,
            is_actively_trading = EXCLUDED.is_actively_trading,
            fetched_at          = EXCLUDED.fetched_at
        WHERE (
            instrument_profile.beta                IS DISTINCT FROM EXCLUDED.beta OR
            instrument_profile.public_float        IS DISTINCT FROM EXCLUDED.public_float OR
            instrument_profile.avg_volume_30d      IS DISTINCT FROM EXCLUDED.avg_volume_30d OR
            instrument_profile.market_cap          IS DISTINCT FROM EXCLUDED.market_cap OR
            instrument_profile.employees           IS DISTINCT FROM EXCLUDED.employees OR
            instrument_profile.ipo_date            IS DISTINCT FROM EXCLUDED.ipo_date OR
            instrument_profile.is_actively_trading IS DISTINCT FROM EXCLUDED.is_actively_trading
        )
        """,
        {
            "instrument_id": instrument_id,
            "beta": profile.beta,
            "public_float": profile.public_float,
            "avg_volume_30d": profile.avg_volume_30d,
            "market_cap": profile.market_cap,
            "employees": profile.employees,
            "ipo_date": profile.ipo_date,
            "is_actively_trading": profile.is_actively_trading,
            "fetched_at": now,
        },
    )


def _upsert_earnings_events(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: str,
    events: Sequence[EarningsEvent],
) -> None:
    """
    Upsert each earnings event into earnings_events.
    Idempotent — keyed on (instrument_id, fiscal_date_ending).
    Only updates a row when eps_actual, revenue_actual, or surprise_pct changed.
    """
    for event in events:
        conn.execute(
            """
            INSERT INTO earnings_events (
                instrument_id,
                fiscal_date_ending, reporting_date,
                eps_estimate, eps_actual,
                revenue_estimate, revenue_actual,
                surprise_pct
            )
            VALUES (
                %(instrument_id)s,
                %(fiscal_date_ending)s, %(reporting_date)s,
                %(eps_estimate)s, %(eps_actual)s,
                %(revenue_estimate)s, %(revenue_actual)s,
                %(surprise_pct)s
            )
            ON CONFLICT (instrument_id, fiscal_date_ending) DO UPDATE SET
                reporting_date   = EXCLUDED.reporting_date,
                eps_estimate     = EXCLUDED.eps_estimate,
                eps_actual       = EXCLUDED.eps_actual,
                revenue_estimate = EXCLUDED.revenue_estimate,
                revenue_actual   = EXCLUDED.revenue_actual,
                surprise_pct     = EXCLUDED.surprise_pct,
                fetched_at       = NOW()
            WHERE (
                earnings_events.reporting_date   IS DISTINCT FROM EXCLUDED.reporting_date OR
                earnings_events.eps_estimate     IS DISTINCT FROM EXCLUDED.eps_estimate OR
                earnings_events.eps_actual       IS DISTINCT FROM EXCLUDED.eps_actual OR
                earnings_events.revenue_estimate IS DISTINCT FROM EXCLUDED.revenue_estimate OR
                earnings_events.revenue_actual   IS DISTINCT FROM EXCLUDED.revenue_actual OR
                earnings_events.surprise_pct     IS DISTINCT FROM EXCLUDED.surprise_pct
            )
            """,
            {
                "instrument_id": instrument_id,
                "fiscal_date_ending": event.fiscal_date_ending,
                "reporting_date": event.reporting_date,
                "eps_estimate": event.eps_estimate,
                "eps_actual": event.eps_actual,
                "revenue_estimate": event.revenue_estimate,
                "revenue_actual": event.revenue_actual,
                "surprise_pct": event.surprise_pct,
            },
        )


def _upsert_analyst_estimates(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: str,
    est: AnalystEstimates,
) -> None:
    """
    Upsert analyst consensus estimates into analyst_estimates.
    Idempotent — keyed on (instrument_id, as_of_date).
    """
    conn.execute(
        """
        INSERT INTO analyst_estimates (
            instrument_id, as_of_date,
            consensus_eps_fq, consensus_eps_fy,
            consensus_rev_fq, consensus_rev_fy,
            analyst_count, buy_count, hold_count, sell_count,
            price_target_mean, price_target_high, price_target_low
        )
        VALUES (
            %(instrument_id)s, %(as_of_date)s,
            %(consensus_eps_fq)s, %(consensus_eps_fy)s,
            %(consensus_rev_fq)s, %(consensus_rev_fy)s,
            %(analyst_count)s, %(buy_count)s, %(hold_count)s, %(sell_count)s,
            %(price_target_mean)s, %(price_target_high)s, %(price_target_low)s
        )
        ON CONFLICT (instrument_id, as_of_date) DO UPDATE SET
            consensus_eps_fq  = EXCLUDED.consensus_eps_fq,
            consensus_eps_fy  = EXCLUDED.consensus_eps_fy,
            consensus_rev_fq  = EXCLUDED.consensus_rev_fq,
            consensus_rev_fy  = EXCLUDED.consensus_rev_fy,
            analyst_count     = EXCLUDED.analyst_count,
            buy_count         = EXCLUDED.buy_count,
            hold_count        = EXCLUDED.hold_count,
            sell_count        = EXCLUDED.sell_count,
            price_target_mean = EXCLUDED.price_target_mean,
            price_target_high = EXCLUDED.price_target_high,
            price_target_low  = EXCLUDED.price_target_low,
            fetched_at        = NOW()
        WHERE (
            analyst_estimates.consensus_eps_fq  IS DISTINCT FROM EXCLUDED.consensus_eps_fq OR
            analyst_estimates.consensus_eps_fy  IS DISTINCT FROM EXCLUDED.consensus_eps_fy OR
            analyst_estimates.consensus_rev_fq  IS DISTINCT FROM EXCLUDED.consensus_rev_fq OR
            analyst_estimates.consensus_rev_fy  IS DISTINCT FROM EXCLUDED.consensus_rev_fy OR
            analyst_estimates.analyst_count     IS DISTINCT FROM EXCLUDED.analyst_count OR
            analyst_estimates.buy_count         IS DISTINCT FROM EXCLUDED.buy_count OR
            analyst_estimates.hold_count        IS DISTINCT FROM EXCLUDED.hold_count OR
            analyst_estimates.sell_count        IS DISTINCT FROM EXCLUDED.sell_count OR
            analyst_estimates.price_target_mean IS DISTINCT FROM EXCLUDED.price_target_mean OR
            analyst_estimates.price_target_high IS DISTINCT FROM EXCLUDED.price_target_high OR
            analyst_estimates.price_target_low  IS DISTINCT FROM EXCLUDED.price_target_low
        )
        """,
        {
            "instrument_id": instrument_id,
            "as_of_date": est.as_of_date,
            "consensus_eps_fq": est.consensus_eps_fq,
            "consensus_eps_fy": est.consensus_eps_fy,
            "consensus_rev_fq": est.consensus_rev_fq,
            "consensus_rev_fy": est.consensus_rev_fy,
            "analyst_count": est.analyst_count,
            "buy_count": est.buy_count,
            "hold_count": est.hold_count,
            "sell_count": est.sell_count,
            "price_target_mean": est.price_target_mean,
            "price_target_high": est.price_target_high,
            "price_target_low": est.price_target_low,
        },
    )
