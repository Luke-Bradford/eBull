"""Prospective point-in-time type, listing and provider industry (#2508/#2523)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg
import psycopg.rows


@dataclass(frozen=True)
class ClassificationReconcileStats:
    confirmed: int
    opened: int
    changed: int
    corrected_same_day: int
    closed: int


def reconcile_instrument_market_classification(
    conn: psycopg.Connection[Any],
) -> ClassificationReconcileStats:
    """Record current provider classifications without rewriting history.

    Must run after the universe upsert/deactivation in the same transaction.
    The record begins prospectively; no value is backfilled before the day on
    which it was observed. A same-day correction updates the day's row because
    daily provenance cannot establish an ordering within that date.
    """
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute(
            """
            WITH observed AS (
                SELECT i.instrument_id, i.exchange AS provider_exchange_id,
                       CASE
                           WHEN i.sector ~ '^[0-9]+$'
                            AND i.sector::BIGINT BETWEEN 1 AND 2147483647
                               THEN i.sector::INTEGER
                           ELSE NULL
                       END AS provider_industry_id,
                       CASE
                           WHEN i.exchange = '5' OR lower(coalesce(e.description, '')) = 'nyse' THEN 'nyse'
                           WHEN i.exchange = '4' OR lower(coalesce(e.description, '')) = 'nasdaq' THEN 'nasdaq'
                           ELSE CASE WHEN i.exchange IS NULL THEN 'unknown' ELSE 'other' END
                       END AS primary_listing_market,
                       i.instrument_type_id,
                       CASE
                           WHEN i.instrument_type_id = 5 OR lower(coalesce(t.description, '')) = 'stocks'
                               THEN 'common_stock'
                           WHEN i.instrument_type_id = 6 OR lower(coalesce(t.description, '')) = 'etf' THEN 'etf'
                           ELSE CASE WHEN i.instrument_type_id IS NULL THEN 'unknown' ELSE 'other' END
                       END AS security_type
                FROM instruments i
                LEFT JOIN exchanges e ON e.exchange_id = i.exchange
                LEFT JOIN etoro_instrument_types t
                  ON t.instrument_type_id = i.instrument_type_id
                WHERE i.is_tradable
            )
            UPDATE instrument_market_classification_history h
               SET last_confirmed_on = (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date
              FROM observed o
             WHERE h.instrument_id = o.instrument_id
               AND h.effective_to IS NULL
               AND h.last_confirmed_on < (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date
               AND (h.provider_exchange_id, h.provider_industry_id,
                    h.primary_listing_market, h.instrument_type_id, h.security_type)
                   IS NOT DISTINCT FROM
                   (o.provider_exchange_id, o.provider_industry_id,
                    o.primary_listing_market, o.instrument_type_id, o.security_type)
            """
        )
        confirmed = cur.rowcount

        cur.execute(
            """
            WITH observed AS (
                SELECT i.instrument_id, i.exchange AS provider_exchange_id,
                       CASE
                           WHEN i.sector ~ '^[0-9]+$'
                            AND i.sector::BIGINT BETWEEN 1 AND 2147483647
                               THEN i.sector::INTEGER
                           ELSE NULL
                       END AS provider_industry_id,
                       CASE
                           WHEN i.exchange = '5' OR lower(coalesce(e.description, '')) = 'nyse' THEN 'nyse'
                           WHEN i.exchange = '4' OR lower(coalesce(e.description, '')) = 'nasdaq' THEN 'nasdaq'
                           ELSE CASE WHEN i.exchange IS NULL THEN 'unknown' ELSE 'other' END
                       END AS primary_listing_market,
                       i.instrument_type_id,
                       CASE
                           WHEN i.instrument_type_id = 5 OR lower(coalesce(t.description, '')) = 'stocks'
                               THEN 'common_stock'
                           WHEN i.instrument_type_id = 6 OR lower(coalesce(t.description, '')) = 'etf' THEN 'etf'
                           ELSE CASE WHEN i.instrument_type_id IS NULL THEN 'unknown' ELSE 'other' END
                       END AS security_type
                FROM instruments i
                LEFT JOIN exchanges e ON e.exchange_id = i.exchange
                LEFT JOIN etoro_instrument_types t
                  ON t.instrument_type_id = i.instrument_type_id
                WHERE i.is_tradable
            )
            UPDATE instrument_market_classification_history h
               SET provider_exchange_id = o.provider_exchange_id,
                   provider_industry_id = o.provider_industry_id,
                   primary_listing_market = o.primary_listing_market,
                   instrument_type_id = o.instrument_type_id,
                   security_type = o.security_type,
                   last_confirmed_on = (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date
              FROM observed o
             WHERE h.instrument_id = o.instrument_id
               AND h.effective_to IS NULL
               AND h.effective_from = (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date
               AND (h.provider_exchange_id, h.provider_industry_id,
                    h.primary_listing_market, h.instrument_type_id, h.security_type)
                   IS DISTINCT FROM
                   (o.provider_exchange_id, o.provider_industry_id,
                    o.primary_listing_market, o.instrument_type_id, o.security_type)
            """
        )
        corrected_same_day = cur.rowcount

        # Close a changed classification on yesterday, then open today's row.
        # Rows first observed today were handled above and cannot reach this arm.
        cur.execute(
            """
            WITH observed AS (
                SELECT i.instrument_id, i.exchange AS provider_exchange_id,
                       CASE
                           WHEN i.sector ~ '^[0-9]+$'
                            AND i.sector::BIGINT BETWEEN 1 AND 2147483647
                               THEN i.sector::INTEGER
                           ELSE NULL
                       END AS provider_industry_id,
                       CASE
                           WHEN i.exchange = '5' OR lower(coalesce(e.description, '')) = 'nyse' THEN 'nyse'
                           WHEN i.exchange = '4' OR lower(coalesce(e.description, '')) = 'nasdaq' THEN 'nasdaq'
                           ELSE CASE WHEN i.exchange IS NULL THEN 'unknown' ELSE 'other' END
                       END AS primary_listing_market,
                       i.instrument_type_id,
                       CASE
                           WHEN i.instrument_type_id = 5 OR lower(coalesce(t.description, '')) = 'stocks'
                               THEN 'common_stock'
                           WHEN i.instrument_type_id = 6 OR lower(coalesce(t.description, '')) = 'etf' THEN 'etf'
                           ELSE CASE WHEN i.instrument_type_id IS NULL THEN 'unknown' ELSE 'other' END
                       END AS security_type
                FROM instruments i
                LEFT JOIN exchanges e ON e.exchange_id = i.exchange
                LEFT JOIN etoro_instrument_types t
                  ON t.instrument_type_id = i.instrument_type_id
                WHERE i.is_tradable
            )
            UPDATE instrument_market_classification_history h
               SET effective_to = (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date - 1,
                   last_confirmed_on = (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date - 1
              FROM observed o
             WHERE h.instrument_id = o.instrument_id
               AND h.effective_to IS NULL
               AND h.effective_from < (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date
               AND (h.provider_exchange_id, h.provider_industry_id,
                    h.primary_listing_market, h.instrument_type_id, h.security_type)
                   IS DISTINCT FROM
                   (o.provider_exchange_id, o.provider_industry_id,
                    o.primary_listing_market, o.instrument_type_id, o.security_type)
            """
        )
        changed = cur.rowcount

        cur.execute(
            """
            INSERT INTO instrument_market_classification_history (
                instrument_id, effective_from, effective_to, last_confirmed_on,
                provider_exchange_id, primary_listing_market,
                instrument_type_id, security_type, source_event,
                provider_industry_id
            )
            SELECT i.instrument_id,
                   (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date,
                   NULL,
                   (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date,
                   i.exchange,
                   CASE
                       WHEN i.exchange = '5' OR lower(coalesce(e.description, '')) = 'nyse' THEN 'nyse'
                       WHEN i.exchange = '4' OR lower(coalesce(e.description, '')) = 'nasdaq' THEN 'nasdaq'
                       ELSE CASE WHEN i.exchange IS NULL THEN 'unknown' ELSE 'other' END
                   END,
                   i.instrument_type_id,
                   CASE
                       WHEN i.instrument_type_id = 5 OR lower(coalesce(t.description, '')) = 'stocks'
                           THEN 'common_stock'
                       WHEN i.instrument_type_id = 6 OR lower(coalesce(t.description, '')) = 'etf' THEN 'etf'
                       ELSE CASE WHEN i.instrument_type_id IS NULL THEN 'unknown' ELSE 'other' END
                   END,
                   CASE WHEN EXISTS (
                       SELECT 1 FROM instrument_market_classification_history p
                       WHERE p.instrument_id = i.instrument_id
                   ) THEN 'classification_change' ELSE 'imported' END,
                   CASE
                       WHEN i.sector ~ '^[0-9]+$'
                        AND i.sector::BIGINT BETWEEN 1 AND 2147483647
                           THEN i.sector::INTEGER
                       ELSE NULL
                   END
              FROM instruments i
              LEFT JOIN exchanges e ON e.exchange_id = i.exchange
              LEFT JOIN etoro_instrument_types t
                ON t.instrument_type_id = i.instrument_type_id
             WHERE i.is_tradable
               AND NOT EXISTS (
                   SELECT 1 FROM instrument_market_classification_history h
                   WHERE h.instrument_id = i.instrument_id AND h.effective_to IS NULL
               )
            """
        )
        opened = cur.rowcount

        cur.execute(
            """
            UPDATE instrument_market_classification_history h
               SET effective_to = h.last_confirmed_on
              FROM instruments i
             WHERE i.instrument_id = h.instrument_id
               AND NOT i.is_tradable
               AND h.effective_to IS NULL
            """
        )
        closed = cur.rowcount

    return ClassificationReconcileStats(
        confirmed=confirmed,
        opened=opened,
        changed=changed,
        corrected_same_day=corrected_same_day,
        closed=closed,
    )


__all__ = ["ClassificationReconcileStats", "reconcile_instrument_market_classification"]
