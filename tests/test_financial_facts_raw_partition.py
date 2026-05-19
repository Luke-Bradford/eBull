"""Partition-shape regression tests for `financial_facts_raw` (#1208 Phase 3 Sub 3).

After migration 156 the table is `PARTITION BY RANGE (period_end)` with:

- `financial_facts_raw_pre2010`: ['1900-01-01', '2010-01-01')
- 84 quarterly partitions `financial_facts_raw_<year>q<1..4>` covering
  2010-Q1 → 2030-Q4
- `financial_facts_raw_default` DEFAULT (pre-1900 + far-future + post-2030)

These tests assert structural invariants that the migration claims:
partition tree shape, routing correctness for in-window / pre-1900 /
far-future period_end values, ON CONFLICT identity preservation across
partitions, and the canonical names of every supporting index.
"""

from __future__ import annotations

from datetime import date

import psycopg


def _insert_one(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    concept: str,
    period_end: date,
    val: int,
    accession_number: str,
    form_type: str = "10-K",
) -> str:
    """Insert one fact row and return the partition `tableoid` resolved
    to a partition name. The partition routing is what we want to
    assert — tableoid is the cheapest way to read it back.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO financial_facts_raw (
                instrument_id, taxonomy, concept, unit, period_end, val,
                accession_number, form_type, filed_date
            ) VALUES (
                %(iid)s, 'us-gaap', %(c)s, 'USD', %(pe)s, %(v)s,
                %(acc)s, %(ft)s, %(fd)s
            ) RETURNING tableoid::regclass::text
            """,
            {
                "iid": instrument_id,
                "c": concept,
                "pe": period_end,
                "v": val,
                "acc": accession_number,
                "ft": form_type,
                "fd": period_end,
            },
        )
        row = cur.fetchone()
        assert row is not None
        return row[0]


def _seed_instrument(conn: psycopg.Connection[tuple], *, instrument_id: int) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (instrument_id, f"T{instrument_id}", f"Test {instrument_id}"),
    )
    conn.commit()


def test_partitioned_table_metadata(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """`financial_facts_raw` is a RANGE-partitioned table on period_end."""
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT partstrat, "
            "       (SELECT array_agg(attname ORDER BY attnum) "
            "          FROM pg_attribute "
            "         WHERE attrelid = pt.partrelid "
            "           AND attnum = ANY(pt.partattrs::int[])) "
            "  FROM pg_partitioned_table pt "
            " WHERE partrelid = 'financial_facts_raw'::regclass"
        )
        row = cur.fetchone()
    assert row is not None, "financial_facts_raw is not partitioned"
    assert row[0] == "r", f"expected RANGE partition strategy, got {row[0]!r}"
    assert row[1] == ["period_end"], f"expected partition key=[period_end], got {row[1]}"


def test_partition_inventory(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Exactly 86 leaf partitions: 1 pre2010 + 84 quarterly + 1 default."""
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) "
            "  FROM pg_inherits i "
            "  JOIN pg_class c ON c.oid = i.inhrelid "
            " WHERE i.inhparent = 'financial_facts_raw'::regclass"
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == 86, f"expected 86 leaf partitions, got {row[0]}"

    # Spot-check three named partitions
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT relname FROM pg_class "
            " WHERE relname IN ("
            "   'financial_facts_raw_pre2010', "
            "   'financial_facts_raw_2025q3', "
            "   'financial_facts_raw_default') "
            " ORDER BY relname"
        )
        names = [r[0] for r in cur.fetchall()]
    assert names == [
        "financial_facts_raw_2025q3",
        "financial_facts_raw_default",
        "financial_facts_raw_pre2010",
    ]


def test_inwindow_period_end_routes_to_quarterly_partition(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """An INSERT with period_end='2025-08-15' routes to *_2025q3."""
    _seed_instrument(ebull_test_conn, instrument_id=10001)
    partition = _insert_one(
        ebull_test_conn,
        instrument_id=10001,
        concept="Revenues",
        period_end=date(2025, 8, 15),
        val=100,
        accession_number="acc-2025q3",
    )
    assert partition == "financial_facts_raw_2025q3"


def test_far_future_routes_to_default(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """An INSERT with period_end='6016-06-30' (parser-junk overflow)
    routes to the DEFAULT partition."""
    _seed_instrument(ebull_test_conn, instrument_id=10002)
    partition = _insert_one(
        ebull_test_conn,
        instrument_id=10002,
        concept="Revenues",
        period_end=date(6016, 6, 30),
        val=1,
        accession_number="acc-junk-future",
    )
    assert partition == "financial_facts_raw_default"


def test_pre1900_routes_to_default_not_pre2010(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Codex 1a WARNING #1 regression: pre-1900 junk goes to DEFAULT,
    not pre2010 (which starts at '1900-01-01')."""
    _seed_instrument(ebull_test_conn, instrument_id=10003)
    partition = _insert_one(
        ebull_test_conn,
        instrument_id=10003,
        concept="Revenues",
        period_end=date(1850, 1, 1),
        val=1,
        accession_number="acc-junk-pre1900",
    )
    assert partition == "financial_facts_raw_default"


def test_2008_routes_to_pre2010(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Pre-2010 in-bounds (1900..2010) lands in the pre2010 catch."""
    _seed_instrument(ebull_test_conn, instrument_id=10004)
    partition = _insert_one(
        ebull_test_conn,
        instrument_id=10004,
        concept="Revenues",
        period_end=date(2008, 3, 15),
        val=1,
        accession_number="acc-2008",
    )
    assert partition == "financial_facts_raw_pre2010"


def test_on_conflict_upsert_across_partitions(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The identity constraint is declared on the partitioned parent,
    so ON CONFLICT (identity) DO UPDATE fires correctly regardless of
    which leaf partition holds the row."""
    _seed_instrument(ebull_test_conn, instrument_id=10005)
    # First insert
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO financial_facts_raw (
                instrument_id, taxonomy, concept, unit, period_end, val,
                accession_number, form_type, filed_date
            ) VALUES (
                10005, 'us-gaap', 'Revenues', 'USD', '2025-08-15', 100,
                'acc-conflict', '10-K', '2025-08-15'
            )
            """
        )
    ebull_test_conn.commit()

    # Conflicting upsert with NEW val — should UPDATE in place
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO financial_facts_raw (
                instrument_id, taxonomy, concept, unit, period_end, val,
                accession_number, form_type, filed_date
            ) VALUES (
                10005, 'us-gaap', 'Revenues', 'USD', '2025-08-15', 200,
                'acc-conflict', '10-K', '2025-08-15'
            )
            ON CONFLICT (
                instrument_id, concept, unit,
                COALESCE(period_start, '0001-01-01'::date),
                period_end, accession_number
            )
            DO UPDATE SET val = EXCLUDED.val
            """
        )
    ebull_test_conn.commit()

    # Exactly one row, val=200
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT val FROM financial_facts_raw WHERE instrument_id = 10005 AND accession_number = 'acc-conflict'"
        )
        rows = cur.fetchall()
    assert rows == [(200,)]


def test_canonical_indexes_on_parent(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """All 5 canonical indexes exist on the partitioned parent under
    their canonical names — no `_new` residue."""
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT indexname FROM pg_indexes "
            " WHERE schemaname = 'public' AND tablename = 'financial_facts_raw' "
            " ORDER BY indexname"
        )
        names = [r[0] for r in cur.fetchall()]
    assert set(names) == {
        "financial_facts_raw_pkey",
        "uq_facts_raw_identity",
        "idx_facts_raw_instrument_concept",
        "idx_facts_raw_retention_ranking",
        "idx_facts_raw_retention_evict",
    }, f"unexpected index set: {names}"


def test_each_leaf_partition_has_attached_child_indexes(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Codex 1c WARNING #5 regression: leaf indexes are PG-auto-named,
    so the canonical name lives on the partitioned parent. Verify each
    leaf has 5 child indexes attached to the 5 parent partitioned
    indexes (`pg_inherits` join on `pg_index.indexrelid`)."""
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            WITH leaves AS (
                SELECT inhrelid AS leaf_oid
                FROM pg_inherits
                WHERE inhparent = 'financial_facts_raw'::regclass
            ),
            parent_indexes AS (
                SELECT indexrelid
                FROM pg_index
                WHERE indrelid = 'financial_facts_raw'::regclass
            ),
            child_index_attach AS (
                SELECT inh.inhparent AS parent_idx_oid,
                       inh.inhrelid  AS child_idx_oid,
                       pi.indrelid   AS child_table_oid
                FROM pg_inherits inh
                JOIN pg_index pi ON pi.indexrelid = inh.inhrelid
                WHERE inh.inhparent IN (SELECT indexrelid FROM parent_indexes)
            )
            SELECT l.leaf_oid::regclass::text,
                   count(DISTINCT cia.parent_idx_oid) AS attached_index_count
            FROM leaves l
            LEFT JOIN child_index_attach cia ON cia.child_table_oid = l.leaf_oid
            GROUP BY l.leaf_oid
            ORDER BY l.leaf_oid::regclass::text
            """
        )
        results = cur.fetchall()
    # 86 leaves, each with 5 child indexes attached (one per parent
    # partitioned index).
    assert len(results) == 86, f"expected 86 leaves, got {len(results)}"
    for leaf_name, attached in results:
        assert attached == 5, f"leaf {leaf_name!r} has {attached} attached child indexes, expected 5"


def test_default_partition_growth_alarm(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """DEFAULT partition is a parking lot for parser bugs, not normal
    storage. Test template is empty, so this asserts the floor. The
    same assertion run on dev should hold <5000 — operator alarm if it
    exceeds."""
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM financial_facts_raw_default")
        row = cur.fetchone()
    assert row is not None
    assert row[0] < 5000, (
        f"financial_facts_raw_default has {row[0]} rows — investigate "
        "the XBRL parser for out-of-window period_end values or extend "
        "the quarterly partitions past 2030-Q4"
    )
