"""
Universe service.

Syncs the eToro tradable instrument list to the local `instruments` table.
Detects new instruments, removed instruments, and changed metadata.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import psycopg

from app.providers.implementations.fmp import FmpFundamentalsProvider
from app.providers.market_data import InstrumentRecord, MarketDataProvider

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SyncSummary:
    inserted: int
    updated: int
    deactivated: int  # marked is_tradable=False (no longer on eToro)


def sync_universe(
    provider: MarketDataProvider,
    conn: psycopg.Connection,  # type: ignore[type-arg]
) -> SyncSummary:
    """
    Pull the full tradable instrument list from the provider and upsert
    into the instruments table.

    - New instruments are inserted with is_tradable=True.
    - Changed metadata (name, sector, etc.) is updated in place.
    - Instruments no longer returned by the provider are marked
      is_tradable=False; they are never deleted.

    If the provider returns zero instruments, deactivation is skipped to
    avoid silently wiping the entire universe on a transient API error.

    Raw provider response is persisted via the provider implementation
    before this function is called (responsibility of the provider).
    """
    records = provider.get_tradable_instruments()

    if not records:
        logger.warning(
            "Provider returned zero instruments — skipping sync to avoid wiping universe. "
            "Check API credentials and endpoint health."
        )
        return SyncSummary(inserted=0, updated=0, deactivated=0)

    provider_ids = {r.provider_id for r in records}
    deactivated = 0

    with conn.transaction():
        # Upsert each record from the provider
        for rec in records:
            conn.execute(
                """
                INSERT INTO instruments (
                    instrument_id, symbol, company_name, exchange, currency,
                    sector, industry, country, is_tradable, instrument_type,
                    first_seen_at, last_seen_at
                )
                VALUES (
                    %(provider_id)s, %(symbol)s, %(company_name)s, %(exchange)s,
                    %(currency)s, %(sector)s, %(industry)s, %(country)s, %(is_tradable)s,
                    %(instrument_type)s, NOW(), NOW()
                )
                ON CONFLICT (instrument_id) DO UPDATE SET
                    symbol          = EXCLUDED.symbol,
                    company_name    = EXCLUDED.company_name,
                    exchange        = EXCLUDED.exchange,
                    currency        = COALESCE(EXCLUDED.currency, instruments.currency),
                    sector          = EXCLUDED.sector,
                    industry        = EXCLUDED.industry,
                    country         = EXCLUDED.country,
                    is_tradable     = EXCLUDED.is_tradable,
                    -- COALESCE preserves a previously-known type when a
                    -- transient eToro response omits ``instrumentTypeName``.
                    -- Otherwise a single empty-field response would erase
                    -- the cross-validation signal we need against
                    -- ``exchanges.asset_class`` (#503 PR 4).
                    instrument_type = COALESCE(EXCLUDED.instrument_type, instruments.instrument_type),
                    last_seen_at    = NOW()
                WHERE (
                    instruments.symbol          IS DISTINCT FROM EXCLUDED.symbol          OR
                    instruments.company_name    IS DISTINCT FROM EXCLUDED.company_name    OR
                    instruments.exchange        IS DISTINCT FROM EXCLUDED.exchange        OR
                    (EXCLUDED.currency IS NOT NULL AND
                     instruments.currency IS DISTINCT FROM EXCLUDED.currency)             OR
                    instruments.sector          IS DISTINCT FROM EXCLUDED.sector          OR
                    instruments.industry        IS DISTINCT FROM EXCLUDED.industry        OR
                    instruments.country         IS DISTINCT FROM EXCLUDED.country         OR
                    instruments.is_tradable     IS DISTINCT FROM EXCLUDED.is_tradable     OR
                    (EXCLUDED.instrument_type IS NOT NULL AND
                     instruments.instrument_type IS DISTINCT FROM EXCLUDED.instrument_type)
                )
                """,
                {
                    "provider_id": rec.provider_id,
                    "symbol": rec.symbol,
                    "company_name": rec.company_name,
                    "exchange": rec.exchange,
                    "currency": rec.currency,
                    "sector": rec.sector,
                    "industry": rec.industry,
                    "country": rec.country,
                    "is_tradable": rec.is_tradable,
                    "instrument_type": rec.instrument_type,
                },
            )

        # Deactivate instruments no longer in the provider feed
        rows = conn.execute(
            """
            UPDATE instruments
            SET is_tradable = FALSE, last_seen_at = NOW()
            WHERE is_tradable = TRUE
              AND instrument_id != ALL(%(provider_ids)s)
            RETURNING instrument_id
            """,
            {"provider_ids": list(provider_ids)},
        )
        deactivated = rows.rowcount

    # Re-query to get accurate inserted/updated counts.
    # ON CONFLICT DO UPDATE doesn't distinguish insert vs update via rowcount;
    # use timestamp comparison as a best-effort summary (not used in decision logic).
    inserted, updated = _count_changes(conn, records)

    return SyncSummary(inserted=inserted, updated=updated, deactivated=deactivated)


def _count_changes(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    records: list[InstrumentRecord],
) -> tuple[int, int]:
    """
    Return (inserted, updated) counts after a sync by comparing last_seen_at
    and first_seen_at timestamps — inserted rows have them equal (both set to
    NOW() in the same transaction), updated rows have first_seen_at < last_seen_at.

    Best-effort count for the summary log only; not used in any decision logic.
    """
    if not records:
        return 0, 0

    provider_ids = [r.provider_id for r in records]
    row = conn.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE first_seen_at = last_seen_at) AS inserted,
            COUNT(*) FILTER (WHERE first_seen_at < last_seen_at) AS updated
        FROM instruments
        WHERE instrument_id = ANY(%(ids)s)
        """,
        {"ids": provider_ids},
    ).fetchone()

    if row is None:
        return 0, 0
    return int(row[0]), int(row[1])


def enrich_instrument_currencies(
    fmp_provider: FmpFundamentalsProvider,
    conn: psycopg.Connection[Any],
) -> int:
    """Enrich instrument currencies from FMP profile endpoint.

    Fetches currency information for tradable instruments that either have
    no currency set, have never been enriched, or were last enriched more
    than 90 days ago.

    Returns the number of instruments enriched.
    """
    rows = conn.execute(
        """
        SELECT instrument_id, symbol
        FROM instruments
        WHERE is_tradable = TRUE
          AND (currency IS NULL
               OR currency_enriched_at IS NULL
               OR currency_enriched_at < NOW() - INTERVAL '90 days')
        ORDER BY instrument_id
        """,
    ).fetchall()

    enriched = 0
    for row in rows:
        instrument_id, symbol = row[0], row[1]
        profile = fmp_provider.get_instrument_profile(symbol)
        if profile is None:
            logger.warning("FMP profile not found for %s (id=%s)", symbol, instrument_id)
            continue
        conn.execute(
            """
            UPDATE instruments
            SET currency = %(currency)s,
                currency_enriched_at = NOW()
            WHERE instrument_id = %(instrument_id)s
            """,
            {"currency": profile.currency, "instrument_id": instrument_id},
        )
        enriched += 1
        logger.info("Enriched currency for %s: %s", symbol, profile.currency)
    return enriched
