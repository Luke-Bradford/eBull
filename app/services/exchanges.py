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


@dataclass(frozen=True)
class ExchangesReclassifySummary:
    """Result of one ``reclassify_unknown_exchanges`` call."""

    classified: int  # asset_class='unknown' rows promoted to a concrete class


def _count_unknown_exchanges(conn: psycopg.Connection) -> int:  # type: ignore[type-arg]
    """Return the count of exchanges with asset_class='unknown'."""
    row = conn.execute("SELECT COUNT(*) FROM exchanges WHERE asset_class = 'unknown'").fetchone()
    return int(row[0]) if row else 0


def reclassify_unknown_exchanges(
    conn: psycopg.Connection,  # type: ignore[type-arg]
) -> ExchangesReclassifySummary:
    """Auto-classify ``exchanges.asset_class='unknown'`` rows from
    instrument-suffix patterns (#1055).

    Why this exists: sql/068 ran the same classification CTE at
    migration time when the ``instruments`` table was empty. The
    dominance computation produced nothing and every exchange stayed
    ``unknown``. After ``nightly_universe_sync`` populates instruments
    the CTE has data to work with — but it never re-fires. Result:
    fresh installs have AAPL/MSFT etc on exchanges classified as
    'unknown', so every job that filters on
    ``e.asset_class = 'us_equity'`` (cik_refresh, cusip backfill,
    bootstrap_preconditions cohort queries) returns 0 rows.

    Operator-curated rows are PRESERVED — the WHERE filter only
    touches rows where ``asset_class='unknown'``. A row promoted to
    a concrete class by a prior run STAYS at that class even if the
    suffix pattern would now imply a different one.

    The classification is the same as sql/068 — kept in lock-step;
    extending the suffix → asset_class map requires updating both
    sites (the migration runs once at install, this service runs
    every nightly_universe_sync).

    ``classified`` in the returned summary is the count of rows
    PROMOTED THIS CALL (before-after delta on asset_class='unknown').
    """
    unknown_before = _count_unknown_exchanges(conn)
    with conn.cursor() as cur:
        # Hard-coded overrides FIRST so the suffix-CTE doesn't
        # mis-classify them as us_equity (e.g. exchange '1' = FX has
        # no suffix and would match the 'no suffix + total_n>30 →
        # us_equity' rule). Each override only fires when
        # asset_class='unknown', preserving operator-curated values.
        # Mirrors sql/068 lines 193-203 plus exchange_id='8' crypto.
        cur.execute(
            "UPDATE exchanges SET asset_class='fx', country=NULL, updated_at=NOW() "
            "WHERE exchange_id='1' AND asset_class='unknown'"
        )
        cur.execute(
            "UPDATE exchanges SET asset_class='commodity', country=NULL, updated_at=NOW() "
            "WHERE exchange_id='2' AND asset_class='unknown'"
        )
        cur.execute(
            "UPDATE exchanges SET asset_class='index', country=NULL, updated_at=NOW() "
            "WHERE exchange_id='3' AND asset_class='unknown'"
        )
        cur.execute(
            "UPDATE exchanges SET asset_class='crypto', country=NULL, updated_at=NOW() "
            "WHERE exchange_id='8' AND asset_class='unknown'"
        )
        cur.execute(
            "UPDATE exchanges SET asset_class='us_equity', country='US', updated_at=NOW() "
            "WHERE exchange_id IN ('19','20') AND asset_class='unknown'"
        )
        cur.execute(
            """
            WITH suffix_counts AS (
                SELECT
                    i.exchange AS exchange_id,
                    CASE
                        WHEN POSITION('.' IN i.symbol) > 0
                            THEN UPPER(SPLIT_PART(REVERSE(i.symbol), '.', 1))
                        ELSE NULL
                    END AS suffix,
                    COUNT(*) AS n
                FROM instruments i
                WHERE i.exchange IS NOT NULL
                GROUP BY 1, 2
            ),
            ranked AS (
                SELECT exchange_id, suffix, n,
                       ROW_NUMBER() OVER (PARTITION BY exchange_id ORDER BY n DESC) AS rn,
                       SUM(n) OVER (PARTITION BY exchange_id) AS total_n
                FROM suffix_counts
            ),
            dominant AS (
                SELECT
                    r.exchange_id,
                    REVERSE(r.suffix) AS suffix,
                    r.n,
                    r.total_n
                FROM ranked r
                WHERE r.rn = 1
                  AND r.n::numeric / NULLIF(r.total_n, 0) > 0.80
                  AND NOT EXISTS (
                      SELECT 1 FROM ranked r2
                      WHERE r2.exchange_id = r.exchange_id
                        AND r2.rn = 2
                        AND r2.n = r.n
                  )
            )
            UPDATE exchanges e
               SET asset_class = m.asset_class,
                   country     = m.country,
                   updated_at  = NOW()
              FROM (
                  SELECT d.exchange_id,
                         CASE
                             WHEN d.suffix IS NULL AND d.total_n > 30 THEN 'us_equity'
                             WHEN d.suffix = 'L'    THEN 'uk_equity'
                             WHEN d.suffix = 'DE'   THEN 'eu_equity'
                             WHEN d.suffix = 'PA'   THEN 'eu_equity'
                             WHEN d.suffix = 'ST'   THEN 'eu_equity'
                             WHEN d.suffix = 'OL'   THEN 'eu_equity'
                             WHEN d.suffix = 'IM'   THEN 'eu_equity'
                             WHEN d.suffix = 'MI'   THEN 'eu_equity'
                             WHEN d.suffix = 'HE'   THEN 'eu_equity'
                             WHEN d.suffix = 'NV'   THEN 'eu_equity'
                             WHEN d.suffix = 'AS'   THEN 'eu_equity'
                             WHEN d.suffix = 'CO'   THEN 'eu_equity'
                             WHEN d.suffix = 'BR'   THEN 'eu_equity'
                             WHEN d.suffix = 'MC'   THEN 'eu_equity'
                             WHEN d.suffix = 'ZU'   THEN 'eu_equity'
                             WHEN d.suffix = 'LS'   THEN 'eu_equity'
                             WHEN d.suffix = 'LSB'  THEN 'eu_equity'
                             WHEN d.suffix = 'HK'   THEN 'asia_equity'
                             WHEN d.suffix = 'T'    THEN 'asia_equity'
                             WHEN d.suffix = 'ASX'  THEN 'asia_equity'
                             WHEN d.suffix = 'DH'   THEN 'mena_equity'
                             WHEN d.suffix = 'AE'   THEN 'mena_equity'
                             WHEN d.suffix = 'RTH'  THEN 'us_equity'
                             WHEN d.suffix = 'FUT'  THEN 'commodity'
                             ELSE NULL
                         END AS asset_class,
                         CASE
                             WHEN d.suffix = 'L'    THEN 'GB'
                             WHEN d.suffix = 'DE'   THEN 'DE'
                             WHEN d.suffix = 'PA'   THEN 'FR'
                             WHEN d.suffix = 'ST'   THEN 'SE'
                             WHEN d.suffix = 'OL'   THEN 'NO'
                             WHEN d.suffix IN ('IM', 'MI') THEN 'IT'
                             WHEN d.suffix = 'HE'   THEN 'FI'
                             WHEN d.suffix IN ('NV', 'AS') THEN 'NL'
                             WHEN d.suffix = 'CO'   THEN 'DK'
                             WHEN d.suffix = 'BR'   THEN 'BE'
                             WHEN d.suffix = 'MC'   THEN 'ES'
                             WHEN d.suffix = 'ZU'   THEN 'CH'
                             WHEN d.suffix IN ('LS', 'LSB') THEN 'PT'
                             WHEN d.suffix = 'HK'   THEN 'HK'
                             WHEN d.suffix = 'T'    THEN 'JP'
                             WHEN d.suffix = 'ASX'  THEN 'AU'
                             WHEN d.suffix = 'DH'   THEN 'AE'
                             WHEN d.suffix = 'AE'   THEN 'AE'
                             WHEN d.suffix IS NULL AND d.total_n > 30 THEN 'US'
                             WHEN d.suffix = 'RTH'  THEN 'US'
                             ELSE NULL
                         END AS country
                  FROM dominant d
              ) AS m
             WHERE e.exchange_id = m.exchange_id
               AND e.asset_class = 'unknown'
               AND m.asset_class IS NOT NULL
            """
        )
        cur.execute("SELECT COUNT(*) FROM exchanges")
        row = cur.fetchone()
        total = int(row[0]) if row else 0
    unknown_after = _count_unknown_exchanges(conn)
    classified = max(0, unknown_before - unknown_after)
    logger.info(
        "reclassify_unknown_exchanges: classified=%d this call (unknown %d -> %d, total exchanges=%d)",
        classified,
        unknown_before,
        unknown_after,
        total,
    )
    return ExchangesReclassifySummary(classified=classified)
