"""Integration tests for the per-test DB cleanup lifecycle (#1568).

``_reset_planner_tables`` replaced a pair of ~845 ms ``TRUNCATE`` sweeps with a
dirty-set probe plus FK-topological ``DELETE``. If the catalog-derived plan is
wrong, every DB test starts seeing its predecessor's rows — so the plan is
exercised here against the real schema rather than a hand-built graph (the
ordering logic itself is unit-tested in ``test_db_fixture_delete_order.py``).
"""

from __future__ import annotations

import psycopg
import psycopg.errors
import pytest
from psycopg import sql

from tests.fixtures.ebull_test_db import (
    _CLEANUP_PLANS,
    _JANITOR_CONNS,
    _PLANNER_TABLES,
    _admin_database_url,
    _cleanup_plan,
    _janitor_conn,
    _reset_planner_tables,
    _snapshot_name,
    _truncate_planner_tables,
    test_database_url,
    test_db_available,
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


def test_reset_refuses_a_connection_to_the_wrong_database(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The C1/#1447 guard must still escape, and must not degrade to a TRUNCATE.

    ``assert_test_db`` runs inside ``_reset_planner_tables``' try block so that a
    DEAD connection reaches the fallback. Its wrong-database refusal is a
    ``RuntimeError``, not a ``psycopg.Error``, so it escapes uncaught — if that
    ever inverted, cleanup would quietly TRUNCATE whatever database it was
    handed, including the operator's dev DB.
    """
    del ebull_test_conn  # only needed so the worker DB exists to compare against
    with psycopg.connect(_admin_database_url()) as admin:
        with pytest.raises(RuntimeError, match="Refusing to TRUNCATE"):
            _reset_planner_tables(admin)  # type: ignore[arg-type]


def test_reset_recovers_when_the_connection_it_was_given_is_dead(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A dead backend must not take the fallback down with it.

    ``_truncate_planner_tables`` cannot run on a broken connection either, so the
    fallback reconnects. Without that, a backend dying mid-cleanup would raise
    out of teardown and leave the worker DB dirty for every following test
    (review bot WARNING on PR #2211).
    """
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (1, 'CLN3', 'Dead Conn Test Co', TRUE)"
    )
    conn.commit()

    doomed = psycopg.connect(test_database_url())
    try:
        with doomed.cursor() as cur:
            cur.execute("SELECT pg_backend_pid()")
            row = cur.fetchone()
        assert row is not None
        doomed.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_terminate_backend(%s)", (row[0],))
        conn.commit()

        with pytest.warns(UserWarning, match="falling back to TRUNCATE"):
            _reset_planner_tables(doomed)
        assert doomed.broken, "precondition: the connection under test must be broken"
    finally:
        doomed.close()

    # The janitor cache must not be left holding a corpse.
    cached = _JANITOR_CONNS.get(test_db_name())
    assert cached is None or not cached.closed

    conn.rollback()
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM instruments")
        row = cur.fetchone()
    assert row is not None and row[0] == 0, "fallback did not clean the database"
    assert not _janitor_conn().closed


@pytest.mark.skipif(not test_db_available(), reason="ebull_test DB unavailable")
def test_no_standalone_strategy_table_is_invisible_to_the_cleanup_planner(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#2224 cause 2 — the guard that catches the NEXT one.

    The planner derives its dirty set from ``pg_constraint``, so a table with no
    foreign key in EITHER direction can only be cleaned if it is named in
    ``_PLANNER_TABLES`` explicitly. Two have now been missed this way —
    ``strategy_results_store`` (collision) and ``strategy_signal_daily_counts``
    (a whole-database census refusing on a predecessor's aggregate rows) — and
    both presented as flake rather than leakage, because the symptom lands in a
    different module from the writer.

    ⚠ Scoped to ``strategy_*`` deliberately, and the scope is measured rather
    than assumed: 1,034 standalone base tables corpus-wide are unlisted, so a
    blanket assertion would be a 1,034-entry allowlist that nobody maintains.
    Within the strategy ledger the set is small enough to be EMPTY, which is the
    only form of this check worth having — an allowlist would absorb the next
    miss silently.

    ⚠ Partitions are excluded: a partition carries no constraint of its own and
    is emptied with its parent, so counting one would be a false positive that
    grows every month.
    """
    orphans = ebull_test_conn.execute(
        """
        SELECT c.relname
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'public'
           AND c.relkind = 'r'
           AND NOT c.relispartition
           AND c.relname LIKE 'strategy\\_%'
           AND NOT EXISTS (SELECT 1 FROM pg_constraint k
                            WHERE k.conrelid = c.oid AND k.contype = 'f')
           AND NOT EXISTS (SELECT 1 FROM pg_constraint k
                            WHERE k.confrelid = c.oid AND k.contype = 'f')
         ORDER BY 1
        """
    ).fetchall()
    unlisted = sorted(row[0] for row in orphans if row[0] not in set(_PLANNER_TABLES))
    assert unlisted == [], (
        "these strategy tables have no FK in either direction, so the cleanup planner "
        f"cannot discover them and their committed rows outlive the test that wrote them: {unlisted}. "
        "Add each to _PLANNER_TABLES with the reason it is standalone."
    )


# ---------------------------------------------------------------------------
# #2224 cause 3 — seeded tables are RESTORED to their template content, not
# emptied and not left alone. The defect this replaces was a committed synthetic
# `exchanges` row surviving into a later module that asserts over every row.
# ---------------------------------------------------------------------------


def _differs_from_snapshot(conn: psycopg.Connection[tuple], table: str) -> bool:
    """True when ``table`` is not multiset-equal to its pristine snapshot."""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT EXISTS ((TABLE {t} EXCEPT ALL TABLE {s}) UNION ALL (TABLE {s} EXCEPT ALL TABLE {t}))"
            ).format(t=sql.Identifier(table), s=sql.Identifier(_snapshot_name(table)))
        )
        row = cur.fetchone()
    assert row is not None
    return bool(row[0])


def test_seed_set_is_derived_and_disjoint_from_the_wipe_set(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The restore set comes from the cloned manifest, not a hand-list.

    ``exchanges`` must be in it (the reproduced defect) and
    ``institutional_filers`` must NOT be: it is seeded too, but it sits inside
    the FK closure, so it is deliberately still emptied. A table cannot be both.
    """
    plan = _cleanup_plan(ebull_test_conn)
    assert "exchanges" in plan.seed_tables
    assert "institutional_filers" not in plan.seed_tables
    assert not set(plan.seed_tables) & set(plan.delete_order)


def test_reset_removes_a_foreign_row_and_undoes_a_mutated_seed_row(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The exact shape of the #2224 cause-3 reproduction.

    ``test_migration_159_currency_backfill`` both INSERTs a synthetic
    ``us_equity`` row and UPDATEs a currency. A count-only dirty probe would see
    the insert and miss the update, so both halves are asserted.
    """
    conn = ebull_test_conn
    conn.execute("INSERT INTO exchanges (exchange_id, asset_class) VALUES ('zz9593', 'us_equity')")
    conn.execute("UPDATE exchanges SET currency = 'CHF' WHERE asset_class = 'us_equity' AND exchange_id <> 'zz9593'")
    conn.commit()
    assert _differs_from_snapshot(conn, "exchanges"), "precondition: the table must be dirty"

    _reset_planner_tables(conn)

    assert not _differs_from_snapshot(conn, "exchanges")
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM exchanges WHERE exchange_id = 'zz9593'")
        row = cur.fetchone()
    assert row is not None and row[0] == 0, "foreign seed row survived cleanup"


def test_reset_restores_seed_rows_a_test_deleted(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Emptying a seed table must be undone, not accepted as the new baseline."""
    conn = ebull_test_conn
    conn.execute("DELETE FROM sec_8k_item_codes")
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM sec_8k_item_codes")
        row = cur.fetchone()
    assert row is not None and row[0] == 0, "precondition: the seed must be gone"

    _reset_planner_tables(conn)

    assert not _differs_from_snapshot(conn, "sec_8k_item_codes")


def test_reset_restores_a_seed_table_guarded_by_an_immutability_trigger(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``sql/299`` refuses to delete an active/retired universe version.

    Restoring this table therefore requires the ``session_replication_role =
    replica`` bypass — a plain ``DELETE`` raises "active/retired intraday
    universe versions are immutable". The control assertion pins that, so a
    future refactor that drops the bypass fails here rather than silently
    dropping the table out of the restore set.
    """
    conn = ebull_test_conn
    with conn.cursor() as cur:
        with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
            cur.execute("DELETE FROM strategy_intraday_universe_versions")
    conn.rollback()

    conn.execute(
        "INSERT INTO strategy_intraday_universe_versions "
        "(universe_version, provider, session_rule, rationale, status) "
        "VALUES ('ZZ-2224-V1', 'etoro', 'nyse_rth', 'cause 3 fixture', 'draft')"
    )
    conn.commit()
    assert _differs_from_snapshot(conn, "strategy_intraday_universe_versions")

    _reset_planner_tables(conn)

    assert not _differs_from_snapshot(conn, "strategy_intraday_universe_versions")


def test_truncate_fallback_also_restores_seed_tables(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The slow path must reach the same end state as the fast one.

    ``TRUNCATE`` restores nothing, so a fallback that skipped the seed restore
    would leave the leak in place exactly when the probe had already failed.
    """
    conn = ebull_test_conn
    conn.execute("INSERT INTO exchanges (exchange_id, asset_class) VALUES ('zz9594', 'us_equity')")
    conn.commit()

    _truncate_planner_tables(conn)

    assert not _differs_from_snapshot(conn, "exchanges")
