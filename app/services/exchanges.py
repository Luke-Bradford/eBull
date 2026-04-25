"""Exchanges metadata refresh.

Pulls eToro's ``/api/v1/market-data/exchanges`` catalogue and upserts
the ``description`` column on the ``exchanges`` table. Operator-curated
columns (``country``, ``asset_class``) are NEVER touched here — the
operator's classification is the source of truth and a description
update must not silently demote a curated row to ``unknown`` or wipe a
country code.

New ``exchange_id`` values eToro adds land as ``asset_class='unknown'``
so they show up in the operator audit query (#503 PR 3 invariant: an
unknown exchange is excluded from the SEC mapper until manually
classified — the test suite pins this).

Cadence: weekly. Exchange catalogue rarely churns and the endpoint is
small (~tens of rows), so daily polling is wasted budget.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import psycopg

from app.providers.implementations.etoro import EtoroMarketDataProvider

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExchangesRefreshSummary:
    """Result of one ``refresh_exchanges_metadata`` call."""

    fetched: int  # rows returned by eToro
    inserted: int  # new exchanges seen for the first time (asset_class='unknown')
    description_updated: int  # existing rows whose description changed


def refresh_exchanges_metadata(
    provider: EtoroMarketDataProvider,
    conn: psycopg.Connection,  # type: ignore[type-arg]
) -> ExchangesRefreshSummary:
    """Refresh ``exchanges.description`` from eToro's exchanges endpoint.

    Behaviour:

    - Inserts rows for ``exchange_id`` values not yet in the table.
      New rows land as ``asset_class='unknown'`` so the operator audit
      flow sees them; ``country`` is NULL.
    - Updates ``description`` on existing rows when eToro's value
      differs. ``country`` and ``asset_class`` are NOT touched.
    - If the provider returns zero rows, this is a no-op — we never
      wipe operator data on a transient API blip.
    """
    records = provider.get_exchanges()

    if not records:
        logger.warning(
            "exchanges_metadata_refresh: provider returned zero rows — "
            "skipping upsert to avoid clobbering operator-curated data."
        )
        return ExchangesRefreshSummary(fetched=0, inserted=0, description_updated=0)

    inserted = 0
    description_updated = 0

    with conn.transaction():
        for rec in records:
            # ON CONFLICT DO UPDATE only when description actually
            # changed AND the new value is non-NULL — a partial eToro
            # response that returns a row without ``exchangeDescription``
            # must not blank a previously-known description. The
            # COALESCE preserves the existing value when EXCLUDED is
            # NULL; the WHERE clause prevents an idempotent no-op
            # update from advancing ``description_updated``.
            row = conn.execute(
                """
                INSERT INTO exchanges (exchange_id, description, asset_class)
                VALUES (%(provider_id)s, %(description)s, 'unknown')
                ON CONFLICT (exchange_id) DO UPDATE SET
                    description = COALESCE(EXCLUDED.description, exchanges.description),
                    updated_at  = NOW()
                WHERE EXCLUDED.description IS NOT NULL
                  AND exchanges.description IS DISTINCT FROM EXCLUDED.description
                RETURNING (xmax = 0) AS was_inserted
                """,
                {
                    "provider_id": rec.provider_id,
                    "description": rec.description,
                },
            ).fetchone()
            if row is None:
                # ON CONFLICT WHERE description-unchanged returns no
                # row — neither insert nor update. Counts stay flat.
                continue
            if bool(row[0]):
                inserted += 1
            else:
                description_updated += 1

    logger.info(
        "exchanges_metadata_refresh: fetched=%d inserted=%d description_updated=%d",
        len(records),
        inserted,
        description_updated,
    )
    return ExchangesRefreshSummary(
        fetched=len(records),
        inserted=inserted,
        description_updated=description_updated,
    )
