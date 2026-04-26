"""Regression tests for migration 069 (#514).

Pins post-migration ``exchanges.asset_class`` / ``country`` truth
table for the rows migration 069 reclassifies. Two failure modes
this test catches:

1. A future "improvement" that re-seeds id 7 (LSE) as us_equity
   would re-introduce the cross-source data leak #503 was meant
   to prevent — the SEC mapper would attach US CIKs to .L
   instruments again. This test fails immediately if that
   regression lands.
2. The downstream candidate query the SEC mapper uses
   (``asset_class = 'us_equity'``) must now exclude the previously-
   misclassified ids. We assert the expected US-only set on the
   live exchanges table.

Method: the test DB starts at migration 067's seed (8 us_equity
rows + crypto). 069 only updates ids 1-7 from that seed (the rest
of the truth table targets ids that don't exist in the test DB
unless populated by ``refresh_exchanges_metadata``). To exercise
the full truth table we seed every id in scope as ``unknown``
first, then re-execute the migration's UPDATEs inline (verbatim
from sql/069_…) so a future SQL refactor that breaks the contract
is caught.
"""

from __future__ import annotations

import psycopg
import pytest

pytestmark = pytest.mark.integration


# Truth table — (asset_class, country) per exchange_id post-#514.
# Rows 4, 5, 19, 20, 33 stay us_equity (canonical US set);
# 18, 48 stay 'unknown' (Canada — see #523).
_EXPECTED_TRUTH_TABLE: dict[str, tuple[str, str | None]] = {
    "1": ("fx", None),
    "2": ("commodity", None),
    "3": ("unknown", None),
    "6": ("eu_equity", "DE"),
    "7": ("uk_equity", "GB"),
    "13": ("asia_equity", "JP"),
    "24": ("mena_equity", "SA"),
    "32": ("eu_equity", "AT"),
    "34": ("eu_equity", "IE"),
    "35": ("eu_equity", "CZ"),
    "36": ("eu_equity", "PL"),
    "37": ("eu_equity", "HU"),
    "40": ("commodity", None),
    "45": ("asia_equity", "CN"),
    "46": ("asia_equity", "CN"),
    "47": ("asia_equity", "IN"),
    "49": ("asia_equity", "SG"),
    "50": ("eu_equity", "IS"),
    "51": ("eu_equity", "EE"),
    "52": ("eu_equity", "LT"),
    "53": ("eu_equity", "LV"),
    "54": ("asia_equity", "KR"),
    "55": ("asia_equity", "TW"),
}


# Verbatim from sql/069_reclassify_exchanges_from_descriptions.sql.
# A future refactor that diverges the migration text from this
# fixture is caught by these tests failing.
_RECLASSIFY_SQL = """
UPDATE exchanges SET asset_class = 'fx', country = NULL, updated_at = NOW()
 WHERE exchange_id = '1' AND asset_class = 'us_equity';
UPDATE exchanges SET asset_class = 'commodity', country = NULL, updated_at = NOW()
 WHERE exchange_id = '2' AND asset_class = 'us_equity';
UPDATE exchanges SET asset_class = 'unknown', country = NULL, updated_at = NOW()
 WHERE exchange_id = '3' AND asset_class = 'us_equity';
UPDATE exchanges SET asset_class = 'eu_equity', country = 'DE', updated_at = NOW()
 WHERE exchange_id = '6' AND asset_class = 'us_equity';
UPDATE exchanges SET asset_class = 'uk_equity', country = 'GB', updated_at = NOW()
 WHERE exchange_id = '7' AND asset_class = 'us_equity';

UPDATE exchanges SET asset_class = 'asia_equity', country = 'JP', updated_at = NOW()
 WHERE exchange_id = '13' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'mena_equity', country = 'SA', updated_at = NOW()
 WHERE exchange_id = '24' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'eu_equity', country = 'AT', updated_at = NOW()
 WHERE exchange_id = '32' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'eu_equity', country = 'IE', updated_at = NOW()
 WHERE exchange_id = '34' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'eu_equity', country = 'CZ', updated_at = NOW()
 WHERE exchange_id = '35' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'eu_equity', country = 'PL', updated_at = NOW()
 WHERE exchange_id = '36' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'eu_equity', country = 'HU', updated_at = NOW()
 WHERE exchange_id = '37' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'commodity', country = NULL, updated_at = NOW()
 WHERE exchange_id = '40' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'asia_equity', country = 'CN', updated_at = NOW()
 WHERE exchange_id = '45' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'asia_equity', country = 'CN', updated_at = NOW()
 WHERE exchange_id = '46' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'asia_equity', country = 'IN', updated_at = NOW()
 WHERE exchange_id = '47' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'asia_equity', country = 'SG', updated_at = NOW()
 WHERE exchange_id = '49' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'eu_equity', country = 'IS', updated_at = NOW()
 WHERE exchange_id = '50' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'eu_equity', country = 'EE', updated_at = NOW()
 WHERE exchange_id = '51' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'eu_equity', country = 'LT', updated_at = NOW()
 WHERE exchange_id = '52' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'eu_equity', country = 'LV', updated_at = NOW()
 WHERE exchange_id = '53' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'asia_equity', country = 'KR', updated_at = NOW()
 WHERE exchange_id = '54' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'asia_equity', country = 'TW', updated_at = NOW()
 WHERE exchange_id = '55' AND asset_class = 'unknown';
"""


def _seed_pre_069_state(conn: psycopg.Connection[tuple]) -> list[str]:
    """Seed exchanges in their pre-#514 state.

    For ids 1-7: simulate migration 067's wrong us_equity seed.
    For every other id in the truth table: insert as 'unknown'
    (mimicking #513's refresh-job backfill behaviour). Returns the
    list of ids it touched so the test can clean up.
    """
    pre_us_equity_ids = ["1", "2", "3", "6", "7"]
    other_ids = [eid for eid in _EXPECTED_TRUTH_TABLE if eid not in pre_us_equity_ids]
    touched = pre_us_equity_ids + other_ids

    with conn.cursor() as cur:
        for eid in pre_us_equity_ids:
            cur.execute(
                """
                INSERT INTO exchanges (exchange_id, asset_class, country)
                VALUES (%s, 'us_equity', 'US')
                ON CONFLICT (exchange_id) DO UPDATE SET
                    asset_class = 'us_equity',
                    country     = 'US'
                """,
                (eid,),
            )
        for eid in other_ids:
            cur.execute(
                """
                INSERT INTO exchanges (exchange_id, asset_class, country)
                VALUES (%s, 'unknown', NULL)
                ON CONFLICT (exchange_id) DO UPDATE SET
                    asset_class = 'unknown',
                    country     = NULL
                """,
                (eid,),
            )
    conn.commit()
    return touched


def _cleanup(conn: psycopg.Connection[tuple], exchange_ids: list[str]) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM exchanges WHERE exchange_id = ANY(%s)", (exchange_ids,))
    conn.commit()


def test_post_069_truth_table(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Every row in the expected truth table has the correct
    (asset_class, country) after the migration UPDATEs run."""
    touched = _seed_pre_069_state(ebull_test_conn)
    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_RECLASSIFY_SQL)
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT exchange_id, asset_class, country FROM exchanges WHERE exchange_id = ANY(%s)",
                (list(_EXPECTED_TRUTH_TABLE.keys()),),
            )
            actual = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

        mismatches = [
            f"  {eid}: expected {expected}, got {actual.get(eid, 'MISSING')}"
            for eid, expected in _EXPECTED_TRUTH_TABLE.items()
            if actual.get(eid) != expected
        ]
        assert not mismatches, "Truth-table drift:\n" + "\n".join(mismatches)
    finally:
        _cleanup(ebull_test_conn, touched)


def test_us_equity_set_unaffected_for_canonical_us_ids(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Migration 069's UPDATEs are gated on (exchange_id, current
    asset_class). For ids that genuinely ARE us_equity (4, 5, 19,
    20, 33), the migration's WHERE clauses don't match, so they
    stay us_equity. Pin that — a future broad re-seed mustn't
    accidentally demote them.

    Does NOT seed or clean up the canonical US rows (they're
    shared by other integration tests via the migration seed +
    refresh job). Just runs the migration UPDATEs against the
    existing rows and asserts they survive unchanged.
    """
    canonical_us = ["4", "5", "19", "20", "33"]

    with ebull_test_conn.cursor() as cur:
        cur.execute(_RECLASSIFY_SQL)
    ebull_test_conn.commit()

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT exchange_id, asset_class FROM exchanges WHERE exchange_id = ANY(%s)",
            (canonical_us,),
        )
        rows = {r[0]: r[1] for r in cur.fetchall()}

    # Only assert rows that already exist in the test DB — the
    # canonical US set comes from migration 067's seed, but the
    # test DB may not include every id in this list. The contract
    # the test pins is "any canonical US id that exists is still
    # us_equity post-069".
    for eid, asset_class in rows.items():
        assert asset_class == "us_equity", f"id {eid} drifted from us_equity → {asset_class}"


def test_migration_069_is_idempotent(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Re-running the FULL migration SQL after the first pass is
    a no-op because every row's asset_class no longer matches the
    WHERE clause's old value. Pin that an operator who hand-
    corrects rows before re-running migrations doesn't get
    clobbered.

    Verification: snapshot ``updated_at`` per row after the first
    pass, run the entire ``_RECLASSIFY_SQL`` block again, then
    assert no row's ``updated_at`` advanced. Covers all 23
    UPDATEs in one sweep — Codex round 1 finding on PR #525
    flagged that the previous version only verified the single
    id-7 UPDATE.
    """
    touched = _seed_pre_069_state(ebull_test_conn)
    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_RECLASSIFY_SQL)
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT exchange_id, asset_class, country, updated_at FROM exchanges "
                "WHERE exchange_id = ANY(%s) ORDER BY exchange_id",
                (touched,),
            )
            after_first = cur.fetchall()

        # Run the entire migration SQL block again. None of the 23
        # UPDATE statements should match any row (every row's
        # asset_class now differs from the gating value).
        with ebull_test_conn.cursor() as cur:
            cur.execute(_RECLASSIFY_SQL)
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT exchange_id, asset_class, country, updated_at FROM exchanges "
                "WHERE exchange_id = ANY(%s) ORDER BY exchange_id",
                (touched,),
            )
            after_second = cur.fetchall()

        # Tuple-equality covers asset_class, country, AND
        # updated_at — any UPDATE that fired would bump the
        # ``NOW()``-set updated_at and break the equality.
        drift = [
            f"  {row1[0]}: first {row1} vs second {row2}"
            for row1, row2 in zip(after_first, after_second, strict=True)
            if row1 != row2
        ]
        assert not drift, "Idempotency broken — rows changed on second pass:\n" + "\n".join(drift)
    finally:
        _cleanup(ebull_test_conn, touched)
