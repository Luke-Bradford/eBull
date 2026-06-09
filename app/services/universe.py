"""
Universe service.

Syncs the eToro tradable instrument list to the local `instruments` table.
Detects new instruments, removed instruments, and changed metadata.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import psycopg

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
        # Upsert each record from the provider.
        #
        # ``country`` and ``currency`` are NOT taken from the provider
        # (the eToro instruments endpoint exposes neither — see
        # ``app/providers/implementations/etoro.py``). Both are derived
        # from the operator-curated ``exchanges`` table (``country`` ISO
        # 3166-1 alpha-2, ``currency`` ISO 4217) via the
        # ``instruments.exchange = exchanges.exchange_id`` join. Currency
        # is a property of where a listing trades (NYSE→USD, SIX→CHF,
        # Oslo→NOK), not 1:1 with country — see sql/159 (#1431).
        #
        # Semantic: the source of truth is the exchanges curator. A
        # curated change ("exchange 5 is now uk_equity / GB") flows
        # through to every instrument on that exchange on the next
        # sync. There is NO instrument-level operator override (#1233
        # §6.1).
        #
        # Edge case: if the exchange row is missing from ``exchanges``
        # at upsert time (transient bootstrap order, brand-new
        # exchange not yet seeded by sql/067), the CASE branch
        # preserves the existing ``instruments.country`` rather than
        # wiping it to NULL. The next sync, after sql/067 backfills
        # the missing exchange, picks up the curated value. Codex 2
        # pre-push catch (PR1 #1233).
        #
        # One-shot backfill for existing rows lives in
        # ``sql/158_instruments_country_backfill.sql`` (country) and
        # ``sql/159_instruments_currency_backfill.sql`` (currency).
        for rec in records:
            conn.execute(
                """
                INSERT INTO instruments (
                    instrument_id, symbol, company_name, exchange, currency,
                    sector, industry, country, is_tradable,
                    instrument_type_id,
                    first_seen_at, last_seen_at
                )
                VALUES (
                    %(provider_id)s, %(symbol)s, %(company_name)s, %(exchange)s,
                    (SELECT currency FROM exchanges WHERE exchange_id = %(exchange)s),
                    %(sector)s, %(industry)s,
                    (SELECT country FROM exchanges WHERE exchange_id = %(exchange)s),
                    %(is_tradable)s,
                    %(instrument_type_id)s, NOW(), NOW()
                )
                ON CONFLICT (instrument_id) DO UPDATE SET
                    symbol             = EXCLUDED.symbol,
                    company_name       = EXCLUDED.company_name,
                    exchange           = EXCLUDED.exchange,
                    -- Same shape as ``country`` below: mirror the
                    -- curator's exchanges.currency, but preserve the prior
                    -- value if the exchange row is missing (transient
                    -- bootstrap order) rather than wiping it to NULL.
                    currency           = CASE
                        WHEN EXISTS (
                            SELECT 1 FROM exchanges
                            WHERE exchange_id = EXCLUDED.exchange
                        )
                        THEN (
                            SELECT currency FROM exchanges
                            WHERE exchange_id = EXCLUDED.exchange
                        )
                        ELSE instruments.currency
                    END,
                    sector             = EXCLUDED.sector,
                    industry           = EXCLUDED.industry,
                    -- Preserve prior country if the exchange row is
                    -- missing (transient bootstrap order); otherwise
                    -- mirror the curator's value (which may legitimately
                    -- be NULL for crypto / FX / index exchanges).
                    country            = CASE
                        WHEN EXISTS (
                            SELECT 1 FROM exchanges
                            WHERE exchange_id = EXCLUDED.exchange
                        )
                        THEN (
                            SELECT country FROM exchanges
                            WHERE exchange_id = EXCLUDED.exchange
                        )
                        ELSE instruments.country
                    END,
                    is_tradable        = EXCLUDED.is_tradable,
                    -- COALESCE preserves a previously-known id when a
                    -- transient eToro response omits ``instrumentTypeID``,
                    -- rather than wiping it to NULL. (#1464 dropped the
                    -- companion ``instrument_type`` TEXT column — it was
                    -- always NULL because the instruments endpoint never
                    -- returns ``instrumentTypeName``; the label is derivable
                    -- via ``instrument_type_id -> etoro_instrument_types``.)
                    instrument_type_id = COALESCE(EXCLUDED.instrument_type_id, instruments.instrument_type_id),
                    last_seen_at       = NOW()
                WHERE (
                    instruments.symbol             IS DISTINCT FROM EXCLUDED.symbol             OR
                    instruments.company_name       IS DISTINCT FROM EXCLUDED.company_name       OR
                    instruments.exchange           IS DISTINCT FROM EXCLUDED.exchange           OR
                    -- Guarded by EXISTS so a missing exchange row (where
                    -- the SET CASE preserves the prior currency) does NOT
                    -- score as a change and force a redundant rewrite +
                    -- last_seen_at bump. ``country`` below predates this
                    -- and carries the same latent no-op in that rare state
                    -- (exchange deleted / not-yet-seeded); not touched here
                    -- to keep its shipped behaviour stable (#1431, Codex).
                    (EXISTS (SELECT 1 FROM exchanges WHERE exchange_id = EXCLUDED.exchange)
                     AND instruments.currency IS DISTINCT FROM EXCLUDED.currency)              OR
                    instruments.sector             IS DISTINCT FROM EXCLUDED.sector             OR
                    instruments.industry           IS DISTINCT FROM EXCLUDED.industry           OR
                    instruments.country            IS DISTINCT FROM EXCLUDED.country            OR
                    instruments.is_tradable        IS DISTINCT FROM EXCLUDED.is_tradable        OR
                    (EXCLUDED.instrument_type_id IS NOT NULL AND
                     instruments.instrument_type_id IS DISTINCT FROM EXCLUDED.instrument_type_id)
                )
                """,
                {
                    "provider_id": rec.provider_id,
                    "symbol": rec.symbol,
                    "company_name": rec.company_name,
                    "exchange": rec.exchange,
                    "sector": rec.sector,
                    "industry": rec.industry,
                    "is_tradable": rec.is_tradable,
                    "instrument_type_id": rec.instrument_type_id,
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
