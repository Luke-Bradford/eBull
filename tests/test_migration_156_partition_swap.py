"""Integration test for migration 156 swap-rename (#1208 Phase 3).

Exercises the full swap shape against a fresh DB:

1. Build an empty DB.
2. Apply migrations 1..155 (stops BEFORE 156).
3. Seed `financial_facts_raw` (un-partitioned shape) with rows spanning
   in-window quarters + pre-1900 junk + far-future junk.
4. Apply migration 156.
5. Assert:
   * row count preserved
   * in-window rows route to their expected quarterly partitions
   * pre-1900 + far-future rows route to DEFAULT (NOT pre2010)
   * `financial_facts_raw_old` does NOT exist (cleanup OK)
   * all 5 canonical indexes present on the partitioned parent
   * dependent views re-created
   * sequence `financial_facts_raw_fact_id_seq` is `OWNED BY` the new
     `financial_facts_raw.fact_id` (verified via `pg_depend`) AND a
     subsequent `nextval()` returns a value greater than the seeded
     `max(fact_id)` (Codex 1b WARNING #5 — explicit invocation test).

Slow because it replays the 1..155 migration stack on a fresh DB
(~10-20 seconds). Acceptable for a once-per-release migration sanity
check; do not run on every save loop.
"""

from __future__ import annotations

import contextlib
import secrets
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

from tests.fixtures.ebull_test_db import (
    _admin_database_url,
    _apply_migrations,
    test_db_available,
)

pytestmark = pytest.mark.integration

_SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
_MIGRATION_156 = _SQL_DIR / "156_financial_facts_raw_partition.sql"
_STOP_AT_155 = "155_postgres_runtime_tuning.sql"


@contextlib.contextmanager
def _temp_database_at_migration_155() -> Iterator[str]:
    """Build an ephemeral DB with migrations 1..155 applied. Yields the
    DB URL; drops the DB at exit."""
    if not test_db_available():
        pytest.skip("test DB unavailable")

    db_name = f"ebull_mig156_{secrets.token_hex(4)}"
    admin_url = _admin_database_url()
    target_url = admin_url.rsplit("/", 1)[0] + f"/{db_name}"

    with psycopg.connect(admin_url, autocommit=True) as admin:
        with admin.cursor() as cur:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    try:
        _apply_migrations(target_url, stop_after=_STOP_AT_155)
        yield target_url
    finally:
        with psycopg.connect(admin_url, autocommit=True) as admin:
            with admin.cursor() as cur:
                cur.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(db_name)))


_FAR_FACT_ID = 999_999_999  # Codex 2 WARNING — exercise the setval drift case


def _seed_pre_migration(conn: psycopg.Connection[tuple]) -> int:
    """Seed `financial_facts_raw` in its pre-156 un-partitioned shape.

    Includes one row with an explicit `fact_id` far above the
    sequence's `last_value` to exercise the `setval()` drift case
    (Codex 2 WARNING #1: without setval the post-migration sequence
    can hand out a `fact_id` that collides with this row).

    Returns the row count seeded.
    """
    with conn.cursor() as cur:
        # One instrument
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
            "VALUES (40001, 'TST', 'Test', TRUE)"
        )
        # 50 in-window facts spanning 5 quarters × 10 facts each.
        in_window_quarters = [
            date(2020, 3, 15),
            date(2021, 6, 15),
            date(2022, 9, 15),
            date(2023, 12, 15),
            date(2024, 6, 15),
        ]
        for q_idx, q_end in enumerate(in_window_quarters):
            for i in range(10):
                cur.execute(
                    """
                    INSERT INTO financial_facts_raw (
                        instrument_id, taxonomy, concept, unit, period_end,
                        val, accession_number, form_type, filed_date
                    ) VALUES (
                        40001, 'us-gaap', %s, 'USD', %s, %s, %s, '10-K', %s
                    )
                    """,
                    (
                        f"Concept_{q_idx}_{i}",
                        q_end,
                        100 + i,
                        f"acc-{q_end.isoformat()}",
                        q_end,
                    ),
                )
        # 1 pre-1900 parser junk row
        cur.execute(
            """
            INSERT INTO financial_facts_raw (
                instrument_id, taxonomy, concept, unit, period_end,
                val, accession_number, form_type, filed_date
            ) VALUES (
                40001, 'us-gaap', 'JunkPre1900', 'USD', '1850-01-01', 0,
                'acc-pre1900', '10-K', '1850-01-01'
            )
            """
        )
        # 1 far-future parser junk row (XBRL year-overflow shape)
        cur.execute(
            """
            INSERT INTO financial_facts_raw (
                instrument_id, taxonomy, concept, unit, period_end,
                val, accession_number, form_type, filed_date
            ) VALUES (
                40001, 'us-gaap', 'JunkFar', 'USD', '6016-06-30', 0,
                'acc-far', '10-K', '6016-06-30'
            )
            """
        )
        # 1 row with explicit fact_id far above the sequence's current
        # last_value (simulates restore-from-dump / manual backfill).
        # Post-migration setval() must bump the sequence past this
        # value or the next regular insert would PK-collide.
        cur.execute(
            """
            INSERT INTO financial_facts_raw (
                fact_id, instrument_id, taxonomy, concept, unit,
                period_end, val, accession_number, form_type, filed_date
            ) VALUES (
                %s, 40001, 'us-gaap', 'SetvalDrift', 'USD',
                '2024-06-15', 0, 'acc-setval-drift', '10-K', '2024-06-15'
            )
            """,
            (_FAR_FACT_ID,),
        )
    conn.commit()
    return 53


def test_migration_156_swap_preserves_rows_and_indexes() -> None:
    with _temp_database_at_migration_155() as target_url:
        with psycopg.connect(target_url) as conn:
            seeded = _seed_pre_migration(conn)
            with conn.cursor() as cur:
                cur.execute("SELECT max(fact_id) FROM financial_facts_raw")
                row = cur.fetchone()
            assert row is not None
            max_fact_id_pre = int(row[0])

        # Apply migration 156 only (next pending).
        _apply_migrations(target_url, stop_after="156_financial_facts_raw_partition.sql")

        with psycopg.connect(target_url) as verify:
            # ── Row count preserved ──────────────────────────────────
            with verify.cursor() as cur:
                cur.execute("SELECT count(*) FROM financial_facts_raw")
                row = cur.fetchone()
            assert row is not None
            assert row[0] == seeded, f"expected {seeded} rows post-migration, got {row[0]}"

            # ── In-window rows landed in expected partitions ─────────
            with verify.cursor() as cur:
                cur.execute(
                    "SELECT tableoid::regclass::text, count(*)   FROM financial_facts_raw  GROUP BY 1 ORDER BY 1"
                )
                hist = dict(cur.fetchall())
            assert hist.get("financial_facts_raw_2020q1") == 10
            assert hist.get("financial_facts_raw_2021q2") == 10
            assert hist.get("financial_facts_raw_2022q3") == 10
            assert hist.get("financial_facts_raw_2023q4") == 10
            # 2024q2 holds 10 in-window seed facts + the explicit-fact_id drift seed
            assert hist.get("financial_facts_raw_2024q2") == 11
            # Both junk rows in DEFAULT (NOT pre2010 — pre-1900 too)
            assert hist.get("financial_facts_raw_default") == 2
            assert "financial_facts_raw_pre2010" not in hist, "pre-1900 junk should land in DEFAULT, not pre2010"

            # ── Old table cleanup ────────────────────────────────────
            with verify.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM pg_class "
                    " WHERE relname IN ("
                    "   'financial_facts_raw_old', "
                    "   'financial_facts_raw_new')"
                )
                row = cur.fetchone()
            assert row is not None and row[0] == 0, "stale `_old` / `_new` relations left behind"

            # ── All 5 canonical indexes on parent ───────────────────
            with verify.cursor() as cur:
                cur.execute(
                    "SELECT indexname FROM pg_indexes "
                    " WHERE schemaname = 'public' "
                    "   AND tablename = 'financial_facts_raw' "
                    " ORDER BY indexname"
                )
                names = {r[0] for r in cur.fetchall()}
            assert names == {
                "financial_facts_raw_pkey",
                "uq_facts_raw_identity",
                "idx_facts_raw_instrument_concept",
                "idx_facts_raw_retention_ranking",
                "idx_facts_raw_retention_evict",
            }, f"unexpected index set post-migration: {names}"

            # ── Dependent views re-created ───────────────────────────
            with verify.cursor() as cur:
                cur.execute(
                    "SELECT viewname FROM pg_views "
                    " WHERE schemaname = 'public' AND viewname IN ("
                    "   'share_count_history', "
                    "   'instrument_dilution_summary', "
                    "   'instrument_share_count_latest')"
                )
                views = {r[0] for r in cur.fetchall()}
            assert views == {
                "share_count_history",
                "instrument_dilution_summary",
                "instrument_share_count_latest",
            }

            # ── Sequence reattached + nextval works ─────────────────
            with verify.cursor() as cur:
                cur.execute(
                    """
                    SELECT count(*) FROM pg_depend d
                    JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S'
                    JOIN pg_class t ON t.oid = d.refobjid
                    WHERE s.relname = 'financial_facts_raw_fact_id_seq'
                      AND t.relname = 'financial_facts_raw'
                      AND d.deptype = 'a'
                    """
                )
                row = cur.fetchone()
            assert row is not None and row[0] == 1, "sequence not OWNED BY the new financial_facts_raw.fact_id"

            with verify.cursor() as cur:
                cur.execute("SELECT nextval('financial_facts_raw_fact_id_seq')")
                row = cur.fetchone()
            assert row is not None
            next_val = int(row[0])
            assert next_val > max_fact_id_pre, (
                f"nextval()={next_val} should exceed max(fact_id)={max_fact_id_pre} from the seeded pre-migration data"
            )
            # Codex 2 WARNING #1 regression: max(fact_id) in the seed is
            # the explicit _FAR_FACT_ID. Post-migration setval() must
            # have bumped the sequence past it. Without setval(), the
            # next regular insert would collide with the drift row's PK.
            assert next_val > _FAR_FACT_ID, (
                f"nextval()={next_val} should exceed explicit fact_id {_FAR_FACT_ID} — setval() drift guard not applied"
            )

            # A regular INSERT (no explicit fact_id) MUST succeed and
            # NOT collide with the existing _FAR_FACT_ID row.
            with verify.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO financial_facts_raw (
                        instrument_id, taxonomy, concept, unit, period_end,
                        val, accession_number, form_type, filed_date
                    ) VALUES (
                        40001, 'us-gaap', 'PostSwapInsert', 'USD',
                        '2024-08-15', 1, 'acc-post-swap', '10-K', '2024-08-15'
                    ) RETURNING fact_id
                    """
                )
                row = cur.fetchone()
            verify.commit()
            assert row is not None
            assert int(row[0]) > _FAR_FACT_ID, "post-swap regular insert produced fact_id ≤ drift seed"
