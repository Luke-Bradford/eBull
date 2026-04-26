"""Regression tests for migration 071 (#515 PR 3).

Pins three contracts:

1. ``exchanges.capabilities`` JSONB column exists with the
   ``jsonb_typeof = 'object'`` CHECK constraint.
2. Every ``us_equity`` row gets the canonical SEC + FMP seed (the
   ``filings`` cell uses ``sec_edgar``, NOT ``sec_xbrl`` — Codex
   round-1 finding caught the conflation).
3. Every non-``us_equity`` row gets the empty-but-correctly-shaped
   default object so the resolver doesn't have to special-case
   missing keys.

Migration 071 runs at fixture setup; tests assert post-migration
state directly.
"""

from __future__ import annotations

import psycopg
import pytest

pytestmark = pytest.mark.integration


def test_capabilities_column_exists_with_object_check(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Schema-level pin: column exists, JSONB type, CHECK
    constraint enforces an object (not array / scalar)."""
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT data_type
              FROM information_schema.columns
             WHERE table_name = 'exchanges'
               AND column_name = 'capabilities'
            """
        )
        row = cur.fetchone()
    assert row is not None, "exchanges.capabilities column missing"
    assert row[0] == "jsonb"

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM pg_constraint
             WHERE conname = 'exchanges_capabilities_is_object'
            """
        )
        assert cur.fetchone() is not None, "object-shape CHECK missing"


def test_us_equity_seed_includes_sec_edgar_for_filings(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Every us_equity row's ``filings`` cell is ``["sec_edgar"]``,
    NOT ``["sec_xbrl"]``. PR 3a Codex round-1 finding: the seed
    originally used sec_xbrl which mis-represents filings as
    XBRL fundamentals data."""
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT exchange_id, capabilities -> 'filings' AS filings
              FROM exchanges
             WHERE asset_class = 'us_equity'
            """
        )
        rows = cur.fetchall()

    assert rows, "no us_equity rows seeded — migration 067 didn't run?"
    for exchange_id, filings in rows:
        assert filings == ["sec_edgar"], f"id={exchange_id}: filings drift, expected ['sec_edgar'], got {filings}"


def test_us_equity_seed_full_shape(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Pin the full us_equity capability seed so a future drift
    in the migration's UPDATE values is caught (e.g. someone
    swaps fmp into ratings by accident)."""
    expected = {
        "filings": ["sec_edgar"],
        "fundamentals": ["sec_xbrl", "fmp"],
        "dividends": ["sec_dividend_summary"],
        "insider": ["sec_form4"],
        "analyst": ["fmp"],
        "ratings": [],
        "esg": [],
        "ownership": ["sec_13f", "sec_13d_13g"],
        "corporate_events": ["sec_8k_events"],
        "business_summary": ["sec_10k_item1"],
        "officers": [],
    }
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT capabilities FROM exchanges WHERE asset_class = 'us_equity' LIMIT 1")
        row = cur.fetchone()
    assert row is not None
    assert row[0] == expected


def test_non_us_equity_seed_is_empty_shape(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Non-us_equity rows have all 11 keys present with empty
    lists — resolver doesn't have to special-case missing keys."""
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT exchange_id, capabilities
              FROM exchanges
             WHERE asset_class IN ('crypto', 'unknown')
             LIMIT 1
            """
        )
        row = cur.fetchone()
    if row is None:
        pytest.skip("no non-us_equity rows in test DB")
    capabilities = row[1]
    expected_keys = {
        "filings",
        "fundamentals",
        "dividends",
        "insider",
        "analyst",
        "ratings",
        "esg",
        "ownership",
        "corporate_events",
        "business_summary",
        "officers",
    }
    assert set(capabilities.keys()) == expected_keys
    for k, v in capabilities.items():
        assert v == [], f"key={k} not empty: {v}"


def test_object_check_rejects_non_object(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The CHECK constraint refuses a JSONB array / scalar / null
    in the capabilities column — operator can't accidentally
    set capabilities = '[]'::jsonb and break the resolver."""
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO exchanges (exchange_id, asset_class, capabilities)
            VALUES ('test_071_seed', 'unknown', '{}'::jsonb)
            ON CONFLICT (exchange_id) DO NOTHING
            """
        )
    ebull_test_conn.commit()

    try:
        with ebull_test_conn.cursor() as cur:
            with pytest.raises(psycopg.errors.CheckViolation):
                cur.execute(
                    """
                    UPDATE exchanges
                       SET capabilities = '[]'::jsonb
                     WHERE exchange_id = 'test_071_seed'
                    """
                )
        ebull_test_conn.rollback()
    finally:
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM exchanges WHERE exchange_id = 'test_071_seed'")
        ebull_test_conn.commit()
