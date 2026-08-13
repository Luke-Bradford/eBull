"""eToro lookup-catalogue refresh.

Pulls the provider's ``get_instrument_types()`` and
``get_stocks_industries()`` endpoints and upserts their contents
into reference tables. The frontend joins on those tables so an
instrument page renders "Stocks" / "Healthcare" instead of
numeric ids — see #515 spec workstream 1.

Both catalogues rarely churn (a few rows added per year at most),
so the refresh job runs weekly. ``description`` / ``name`` updates
in place when eToro changes a label; rows are never deleted, so
historical references to a retired id still resolve to its label
on operator audit pages.

Typed against the abstract ``MarketDataProvider`` interface — not
the concrete eToro class — so the boundary the providers package
advertises stays clean.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import psycopg

from app.providers.market_data import MarketDataProvider

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LookupRefreshSummary:
    """Result of one ``refresh_etoro_lookups`` call."""

    instrument_types_fetched: int
    instrument_types_inserted: int
    instrument_types_updated: int
    industries_fetched: int
    industries_inserted: int
    industries_updated: int


def refresh_etoro_lookups(
    provider: MarketDataProvider,
    conn: psycopg.Connection,  # type: ignore[type-arg]
) -> LookupRefreshSummary:
    """Refresh ``etoro_instrument_types`` + ``etoro_stocks_industries``.

    Same shape as ``refresh_exchanges_metadata``: only ``description``
    / ``name`` is upserted; the seeded_at / updated_at columns
    track first-seen-vs-changed-since. Empty provider responses
    are no-ops (guards against an eToro blip wiping label data).
    """
    type_records = provider.get_instrument_types()
    industry_records = provider.get_stocks_industries()

    if not type_records and not industry_records:
        logger.warning(
            "etoro_lookups_refresh: provider returned zero rows on both endpoints — "
            "skipping upsert to avoid clobbering label data."
        )
        return LookupRefreshSummary(0, 0, 0, 0, 0, 0)

    types_inserted = 0
    types_updated = 0
    industries_inserted = 0
    industries_updated = 0

    with conn.transaction():
        for rec in type_records:
            row = conn.execute(
                """
                INSERT INTO etoro_instrument_types (instrument_type_id, description)
                VALUES (%(id)s, %(description)s)
                ON CONFLICT (instrument_type_id) DO UPDATE SET
                    description = COALESCE(EXCLUDED.description, etoro_instrument_types.description),
                    updated_at  = NOW()
                WHERE EXCLUDED.description IS NOT NULL
                  AND etoro_instrument_types.description IS DISTINCT FROM EXCLUDED.description
                RETURNING (xmax = 0) AS was_inserted
                """,
                {"id": rec.type_id, "description": rec.description},
            ).fetchone()
            if row is None:
                continue
            if bool(row[0]):
                types_inserted += 1
            else:
                types_updated += 1

        for rec in industry_records:
            row = conn.execute(
                """
                INSERT INTO etoro_stocks_industries (industry_id, name)
                VALUES (%(id)s, %(name)s)
                ON CONFLICT (industry_id) DO UPDATE SET
                    name       = COALESCE(EXCLUDED.name, etoro_stocks_industries.name),
                    updated_at = NOW()
                WHERE EXCLUDED.name IS NOT NULL
                  AND etoro_stocks_industries.name IS DISTINCT FROM EXCLUDED.name
                RETURNING (xmax = 0) AS was_inserted
                """,
                {"id": rec.industry_id, "name": rec.name},
            ).fetchone()
            if row is None:
                continue
            if bool(row[0]):
                industries_inserted += 1
            else:
                industries_updated += 1

    # No legacy-row backfill: the eToro instruments endpoint
    # returns only ``instrumentTypeID`` (int), NOT
    # ``instrumentTypeName`` (text). The text column added in
    # migration 068 stays NULL across the universe; the int
    # column added in migration 070 is the canonical persisted
    # field. Pre-migration-070 rows pick up
    # ``instrument_type_id`` on the next ``nightly_universe_sync``
    # — note: that job is on-demand (operator triggers via Admin
    # "Run now" or it gets scheduled by orchestrator_full_sync),
    # NOT catch-up-on-boot. So the rollout gap is: existing rows
    # stay NULL until the operator's next sync. Acceptable
    # because the column is purely additive (frontend treats
    # NULL as "label unknown, fall back to numeric id render").

    summary = LookupRefreshSummary(
        instrument_types_fetched=len(type_records),
        instrument_types_inserted=types_inserted,
        instrument_types_updated=types_updated,
        industries_fetched=len(industry_records),
        industries_inserted=industries_inserted,
        industries_updated=industries_updated,
    )
    logger.info(
        "etoro_lookups_refresh: types fetched=%d inserted=%d updated=%d; industries fetched=%d inserted=%d updated=%d",
        summary.instrument_types_fetched,
        summary.instrument_types_inserted,
        summary.instrument_types_updated,
        summary.industries_fetched,
        summary.industries_inserted,
        summary.industries_updated,
    )
    return summary
