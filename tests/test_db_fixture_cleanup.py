"""Integration tests for the per-test DB cleanup lifecycle (#1568).

``_reset_planner_tables`` replaced a pair of ~845 ms ``TRUNCATE`` sweeps with a
dirty-set probe plus FK-topological ``DELETE``. If the catalog-derived plan is
wrong, every DB test starts seeing its predecessor's rows — so the plan is
exercised here against the real schema rather than a hand-built graph (the
ordering logic itself is unit-tested in ``test_db_fixture_delete_order.py``).
"""

from __future__ import annotations

import psycopg
import pytest

from tests.fixtures.ebull_test_db import (
    _CLEANUP_PLANS,
    _PLANNER_TABLES,
    _cleanup_plan,
    _reset_planner_tables,
    test_db_name,
)


def test_plan_covers_cascade_only_tables_and_orders_them_first(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``price_daily`` is absent from ``_PLANNER_TABLES``.

    The old ``TRUNCATE ... CASCADE`` reached it through its FK to
    ``instruments``. ``DELETE`` has no CASCADE, so the derived plan must both
    include it and empty it before its parent — the exact case that made a
    throwaway benchmark raise ``ForeignKeyViolation`` while researching #1568.
    """
    plan = _cleanup_plan(ebull_test_conn)
    assert "price_daily" not in _PLANNER_TABLES
    assert "price_daily" in plan.delete_order
    assert plan.delete_order.index("price_daily") < plan.delete_order.index("instruments")
    assert set(_PLANNER_TABLES) <= set(plan.delete_order)


def test_reset_empties_listed_and_cascade_only_tables(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (1, 'CLN1', 'Cleanup Test Co', TRUE)"
    )
    conn.execute("INSERT INTO price_daily (instrument_id, price_date, close) VALUES (1, DATE '2024-01-02', 10)")
    conn.execute("INSERT INTO institutional_filers (cik, name) VALUES ('0000000001', 'F1')")
    conn.commit()

    _reset_planner_tables(conn)

    for table in ("instruments", "price_daily", "institutional_filers"):
        with conn.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM "{table}"')  # noqa: S608 — from a fixed literal tuple
            row = cur.fetchone()
        assert row is not None and row[0] == 0, f"{table} still holds rows after cleanup"


def test_reset_restarts_owned_sequences(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    conn.execute("INSERT INTO institutional_filers (cik, name) VALUES ('0000000001', 'F1')")
    conn.commit()

    _reset_planner_tables(conn)

    with conn.cursor() as cur:
        cur.execute("INSERT INTO institutional_filers (cik, name) VALUES ('0000000002', 'F2') RETURNING filer_id")
        row = cur.fetchone()
    assert row is not None and row[0] == 1, "owned sequence was not restarted"
    conn.rollback()


def test_reset_restarts_a_sequence_advanced_by_a_rolled_back_insert(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``nextval`` is not transactional.

    A rolled-back INSERT leaves the sequence advanced while the table stays
    empty, so a row-only dirty probe would skip the table and the next test
    would see ids starting at 2. Asserted against the real cluster because the
    behaviour is Postgres', not ours.
    """
    conn = ebull_test_conn
    with conn.cursor() as cur:
        cur.execute("INSERT INTO institutional_filers (cik, name) VALUES ('0000000009', 'Rolled') RETURNING filer_id")
        first = cur.fetchone()
    assert first is not None and first[0] == 1
    conn.rollback()

    _reset_planner_tables(conn)

    with conn.cursor() as cur:
        cur.execute("INSERT INTO institutional_filers (cik, name) VALUES ('0000000010', 'After') RETURNING filer_id")
        row = cur.fetchone()
    assert row is not None and row[0] == 1, "sequence advanced by a rolled-back insert was not reset"
    conn.rollback()


def test_reset_falls_back_to_truncate_when_the_plan_is_stale(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A test that creates its own FK-bearing table outgrows the cached plan.

    The cached delete order cannot know about ``zz_1568_fallback``, so emptying
    ``instruments`` raises ``ForeignKeyViolation``. The fixture must degrade to
    ``TRUNCATE ... CASCADE`` (which needs no precomputed order) rather than
    erroring, and must drop the stale plan so the next test rebuilds it.
    """
    conn = ebull_test_conn
    _cleanup_plan(conn)  # cache a plan that predates the new table
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (1, 'CLN2', 'Fallback Test Co', TRUE)"
    )
    conn.execute(
        "CREATE TABLE zz_1568_fallback ("
        "  id INT PRIMARY KEY,"
        "  instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id)"
        ")"
    )
    conn.execute("INSERT INTO zz_1568_fallback (id, instrument_id) VALUES (1, 1)")
    conn.commit()
    try:
        with pytest.warns(UserWarning, match="falling back to TRUNCATE"):
            _reset_planner_tables(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM instruments")
            row = cur.fetchone()
        assert row is not None and row[0] == 0
        assert test_db_name() not in _CLEANUP_PLANS, "stale plan must be invalidated"
    finally:
        # Relations leak into the session-reused worker DB and count against
        # the #1401 tripwire, so the drop must not be conditional on success.
        conn.rollback()
        conn.execute("DROP TABLE IF EXISTS zz_1568_fallback")
        conn.commit()
