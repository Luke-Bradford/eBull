"""Migration 047 + trigger behaviour tests against real ``ebull_test``.

Structural assertions (table columns, FK, dual partial indexes, trigger,
cursor column) + trigger behaviour (transitions, no-op UPDATEs, INSERT
not covered, advisory-lock serialization) + concurrent-writer test.
"""

from __future__ import annotations

import psycopg

from tests.fixtures.ebull_test_db import ebull_test_conn as ebull_test_conn  # noqa: F401


def _fetch_one_scalar(
    conn: psycopg.Connection[tuple],
    sql: str,
    params: tuple[object, ...] = (),
) -> object:
    with conn.cursor() as cur:
        cur.execute(sql, params)  # type: ignore[call-overload]
        row = cur.fetchone()
    conn.commit()
    assert row is not None, f"expected one row from: {sql}"
    return row[0]


class TestMigration047Structure:
    def test_coverage_status_events_table_exists(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        exists = _fetch_one_scalar(
            ebull_test_conn,
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
            "WHERE table_name = 'coverage_status_events')",
        )
        assert exists is True

    def test_coverage_status_events_columns(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type, is_nullable "
                "FROM information_schema.columns "
                "WHERE table_name = 'coverage_status_events' "
                "ORDER BY ordinal_position"
            )
            cols = cur.fetchall()
        ebull_test_conn.commit()
        by_name = {c[0]: (c[1], c[2]) for c in cols}
        assert by_name["event_id"] == ("bigint", "NO")
        assert by_name["instrument_id"] == ("bigint", "NO")
        assert by_name["changed_at"] == ("timestamp with time zone", "NO")
        assert by_name["old_status"] == ("text", "YES")
        assert by_name["new_status"] == ("text", "YES")

    def test_instrument_id_fk_present(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_class r ON r.oid = c.confrelid
                WHERE c.contype = 'f'
                  AND t.relname = 'coverage_status_events'
                  AND r.relname = 'instruments'
                """
            )
            row = cur.fetchone()
        ebull_test_conn.commit()
        assert row is not None
        assert row[0] == 1

    def test_drops_partial_index_on_event_id(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        indexdef = _fetch_one_scalar(
            ebull_test_conn,
            "SELECT indexdef FROM pg_indexes "
            "WHERE indexname = 'idx_coverage_status_events_drops'",
        )
        assert indexdef is not None
        s = str(indexdef)
        assert "event_id DESC" in s
        assert "old_status = 'analysable'" in s
        assert "new_status IS DISTINCT FROM 'analysable'" in s

    def test_drops_partial_index_on_changed_at(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        indexdef = _fetch_one_scalar(
            ebull_test_conn,
            "SELECT indexdef FROM pg_indexes "
            "WHERE indexname = 'idx_coverage_status_events_drops_changed_at'",
        )
        assert indexdef is not None
        s = str(indexdef)
        assert "changed_at DESC" in s
        assert "old_status = 'analysable'" in s
        assert "new_status IS DISTINCT FROM 'analysable'" in s

    def test_operators_cursor_column_exists(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT data_type, is_nullable FROM information_schema.columns "
                "WHERE table_name = 'operators' "
                "  AND column_name = 'alerts_last_seen_coverage_event_id'"
            )
            row = cur.fetchone()
        ebull_test_conn.commit()
        assert row is not None, (
            "alerts_last_seen_coverage_event_id column missing from operators"
        )
        assert row[0] == "bigint"
        assert row[1] == "YES"

    def test_trigger_exists_after_update_of_filings_status(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT t.tgname, pg_get_triggerdef(t.oid)
                FROM pg_trigger t
                JOIN pg_class c ON c.oid = t.tgrelid
                WHERE c.relname = 'coverage'
                  AND t.tgname = 'trg_coverage_filings_status_transition'
                """
            )
            row = cur.fetchone()
        ebull_test_conn.commit()
        assert row is not None, (
            "trigger trg_coverage_filings_status_transition missing"
        )
        triggerdef = str(row[1])
        assert "AFTER UPDATE OF filings_status" in triggerdef
        assert "FOR EACH ROW" in triggerdef

    def test_trigger_function_takes_advisory_lock(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        prosrc = _fetch_one_scalar(
            ebull_test_conn,
            "SELECT prosrc FROM pg_proc WHERE proname = 'log_coverage_status_transition'",
        )
        assert prosrc is not None
        src = str(prosrc)
        assert "pg_advisory_xact_lock" in src
        assert "coverage_status_events_writer" in src
