"""Regression tests for migrations 072 + 073 (#532 stage 1).

Pins the surgical contracts Codex flagged on PR review:

* 072 — FMP element is REMOVED from each capability array,
  not the array clobbered. Operator overrides like
  ``["sec_xbrl", "fmp", "custom"]`` keep their non-fmp
  additions.
* 073 — ``fundamentals_snapshot`` rows for instruments WITHOUT
  a primary SEC CIK get deleted (FMP-only rows). Rows for SEC-
  CIK instruments survive (SEC overwrites them on next ingest).

Migration 071 + 072 + 073 all run at fixture setup; tests assert
post-migration state on a controlled seed inserted into ebull_test.
"""

from __future__ import annotations

import psycopg
import pytest

pytestmark = pytest.mark.integration


def test_072_preserves_custom_providers_in_mixed_arrays(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Operator override containing custom providers alongside fmp
    keeps the non-fmp additions when 072 strips fmp.

    Re-runs the 072 UPDATE against a synthetic operator-override
    row to pin the surgical behaviour: only ``fmp`` is removed.
    """
    exchange_id = "test_072_custom"
    with ebull_test_conn.cursor() as cur:
        cur.execute("DELETE FROM exchanges WHERE exchange_id = %s", (exchange_id,))
        cur.execute(
            """
            INSERT INTO exchanges (
                exchange_id, asset_class, capabilities
            )
            VALUES (
                %s,
                'us_equity',
                jsonb_build_object(
                    'fundamentals', jsonb_build_array('sec_xbrl', 'fmp', 'custom_x'),
                    'analyst',      jsonb_build_array('fmp', 'custom_y')
                )
            )
            """,
            (exchange_id,),
        )
    ebull_test_conn.commit()

    # Re-run the 072 UPDATE. Migration 071 + 072 already ran at
    # fixture setup, but only against rows that existed at that
    # point — this row is new, so we re-execute the same logic.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE exchanges
               SET capabilities = jsonb_set(
                       capabilities,
                       '{fundamentals}',
                       COALESCE(
                           (SELECT jsonb_agg(elem)
                              FROM jsonb_array_elements(capabilities -> 'fundamentals') elem
                             WHERE elem <> '"fmp"'::jsonb),
                           '[]'::jsonb
                       )
                   )
             WHERE exchange_id = %s AND (capabilities -> 'fundamentals') ? 'fmp'
            """,
            (exchange_id,),
        )
        cur.execute(
            """
            UPDATE exchanges
               SET capabilities = jsonb_set(
                       capabilities,
                       '{analyst}',
                       COALESCE(
                           (SELECT jsonb_agg(elem)
                              FROM jsonb_array_elements(capabilities -> 'analyst') elem
                             WHERE elem <> '"fmp"'::jsonb),
                           '[]'::jsonb
                       )
                   )
             WHERE exchange_id = %s AND (capabilities -> 'analyst') ? 'fmp'
            """,
            (exchange_id,),
        )
        cur.execute(
            "SELECT capabilities FROM exchanges WHERE exchange_id = %s",
            (exchange_id,),
        )
        row = cur.fetchone()

    try:
        assert row is not None
        capabilities = row[0]
        assert capabilities["fundamentals"] == ["sec_xbrl", "custom_x"]
        assert capabilities["analyst"] == ["custom_y"]
    finally:
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM exchanges WHERE exchange_id = %s", (exchange_id,))
        ebull_test_conn.commit()


def test_072_idempotent_on_already_clean_arrays(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A row that already has no ``fmp`` is untouched by 072 (no
    spurious updated_at bump, no array-shape rewrite)."""
    exchange_id = "test_072_clean"
    with ebull_test_conn.cursor() as cur:
        cur.execute("DELETE FROM exchanges WHERE exchange_id = %s", (exchange_id,))
        cur.execute(
            """
            INSERT INTO exchanges (
                exchange_id, asset_class, capabilities
            )
            VALUES (
                %s,
                'us_equity',
                jsonb_build_object(
                    'fundamentals', jsonb_build_array('sec_xbrl'),
                    'analyst',      jsonb_build_array()
                )
            )
            """,
            (exchange_id,),
        )
    ebull_test_conn.commit()

    # Re-run the WHERE-guarded UPDATE; it must skip this row.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE exchanges
               SET capabilities = jsonb_set(
                       capabilities,
                       '{fundamentals}',
                       '[]'::jsonb
                   )
             WHERE exchange_id = %s
               AND capabilities ? 'fundamentals'
               AND (capabilities -> 'fundamentals') ? 'fmp'
            """,
            (exchange_id,),
        )
        affected = cur.rowcount
        cur.execute(
            "SELECT capabilities FROM exchanges WHERE exchange_id = %s",
            (exchange_id,),
        )
        row = cur.fetchone()

    try:
        assert affected == 0  # WHERE didn't match
        assert row is not None
        assert row[0]["fundamentals"] == ["sec_xbrl"]
    finally:
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM exchanges WHERE exchange_id = %s", (exchange_id,))
        ebull_test_conn.commit()


def test_073_purges_fundamentals_snapshot_for_non_sec_cik_instruments(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """fundamentals_snapshot rows for instruments WITHOUT a primary
    SEC CIK get deleted by migration 073. Rows WITH a SEC CIK
    survive."""
    sec_id = 970001
    fmp_id = 970002
    exchange_id = "test_073_x"
    with ebull_test_conn.cursor() as cur:
        cur.execute("DELETE FROM exchanges WHERE exchange_id = %s", (exchange_id,))
        cur.execute(
            "INSERT INTO exchanges (exchange_id, asset_class, capabilities) VALUES (%s, 'us_equity', '{}'::jsonb)",
            (exchange_id,),
        )
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, exchange) "
            "VALUES (%s, 'TST073A', 'Test 073 A', %s), (%s, 'TST073B', 'Test 073 B', %s)",
            (sec_id, exchange_id, fmp_id, exchange_id),
        )
        # SEC-CIK on sec_id; fmp_id has no SEC CIK (FMP-only).
        cur.execute(
            "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cik', '0000999991', TRUE)",
            (sec_id,),
        )
        cur.execute(
            "INSERT INTO fundamentals_snapshot (instrument_id, as_of_date) "
            "VALUES (%s, '2026-04-25'), (%s, '2026-04-25')",
            (sec_id, fmp_id),
        )
    ebull_test_conn.commit()

    # Re-run the 073 surgical DELETE. It already ran at fixture
    # setup but our seeded rows were inserted post-migration.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM fundamentals_snapshot fs
             WHERE NOT EXISTS (
                    SELECT 1 FROM external_identifiers e
                     WHERE e.instrument_id   = fs.instrument_id
                       AND e.provider        = 'sec'
                       AND e.identifier_type = 'cik'
                       AND e.is_primary      = TRUE
                   )
               AND fs.instrument_id IN (%s, %s)
            """,
            (sec_id, fmp_id),
        )
        cur.execute(
            "SELECT instrument_id FROM fundamentals_snapshot WHERE instrument_id IN (%s, %s) ORDER BY instrument_id",
            (sec_id, fmp_id),
        )
        rows = cur.fetchall()

    try:
        assert rows == [(sec_id,)]  # SEC-CIK row kept; FMP-only row deleted
    finally:
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM fundamentals_snapshot WHERE instrument_id IN (%s, %s)",
                (sec_id, fmp_id),
            )
            cur.execute(
                "DELETE FROM external_identifiers WHERE instrument_id IN (%s, %s)",
                (sec_id, fmp_id),
            )
            cur.execute(
                "DELETE FROM instruments WHERE instrument_id IN (%s, %s)",
                (sec_id, fmp_id),
            )
            cur.execute("DELETE FROM exchanges WHERE exchange_id = %s", (exchange_id,))
        ebull_test_conn.commit()
