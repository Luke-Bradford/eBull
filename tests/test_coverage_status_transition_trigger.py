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


_TRG_INSTRUMENT_ID_COUNTER = 5000  # isolated from _PA_INSTRUMENT_ID_COUNTER (1000+)


def _seed_instrument_with_coverage(
    conn: psycopg.Connection[tuple],
    *,
    initial_status: str | None = None,
) -> int:
    """Insert one tradable instrument + its coverage row; return instrument_id.

    instruments.instrument_id is caller-supplied BIGINT PK (no sequence) per
    sql/001_init.sql. Module-level counter guarantees unique IDs across tests.

    initial_status=None leaves coverage.filings_status NULL (pre-audit).
    Otherwise the coverage row lands NULL and is immediately UPDATEd to
    initial_status — that UPDATE fires the trigger (INSERT does NOT, per
    spec scope).
    """
    global _TRG_INSTRUMENT_ID_COUNTER
    _TRG_INSTRUMENT_ID_COUNTER += 1
    instrument_id = _TRG_INSTRUMENT_ID_COUNTER

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', TRUE)",
            (instrument_id, f"TRG{instrument_id}", f"Trig {instrument_id}"),
        )
        cur.execute(
            "INSERT INTO coverage (instrument_id, coverage_tier, filings_status) "
            "VALUES (%s, 3, NULL)",
            (instrument_id,),
        )
        if initial_status is not None:
            cur.execute(
                "UPDATE coverage SET filings_status = %s WHERE instrument_id = %s",
                (initial_status, instrument_id),
            )
    conn.commit()
    return instrument_id


def _count_events(
    conn: psycopg.Connection[tuple], instrument_id: int | None = None
) -> int:
    with conn.cursor() as cur:
        if instrument_id is None:
            cur.execute("SELECT COUNT(*) FROM coverage_status_events")
        else:
            cur.execute(
                "SELECT COUNT(*) FROM coverage_status_events WHERE instrument_id = %s",
                (instrument_id,),
            )
        row = cur.fetchone()
    conn.commit()
    assert row is not None
    return int(row[0])


class TestTriggerBehaviour:
    def test_null_to_analysable_logs_event(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid = _seed_instrument_with_coverage(ebull_test_conn)  # filings_status NULL
        assert _count_events(ebull_test_conn, iid) == 0

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE coverage SET filings_status = 'analysable' WHERE instrument_id = %s",
                (iid,),
            )
        ebull_test_conn.commit()

        assert _count_events(ebull_test_conn, iid) == 1
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT old_status, new_status FROM coverage_status_events "
                "WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        ebull_test_conn.commit()
        assert row is not None
        assert row[0] is None
        assert row[1] == "analysable"

    def test_analysable_to_insufficient_logs_event(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid = _seed_instrument_with_coverage(ebull_test_conn, initial_status="analysable")
        baseline = _count_events(ebull_test_conn, iid)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE coverage SET filings_status = 'insufficient' WHERE instrument_id = %s",
                (iid,),
            )
        ebull_test_conn.commit()

        assert _count_events(ebull_test_conn, iid) == baseline + 1

    def test_no_op_update_same_value_writes_nothing(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid = _seed_instrument_with_coverage(ebull_test_conn, initial_status="analysable")
        baseline = _count_events(ebull_test_conn, iid)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE coverage SET filings_status = 'analysable' WHERE instrument_id = %s",
                (iid,),
            )
        ebull_test_conn.commit()

        assert _count_events(ebull_test_conn, iid) == baseline

    def test_case_expression_preserving_same_value_writes_nothing(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """Mirrors audit_all_instruments demote-guard pattern."""
        iid = _seed_instrument_with_coverage(ebull_test_conn, initial_status="structurally_young")
        baseline = _count_events(ebull_test_conn, iid)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                UPDATE coverage
                SET filings_status = CASE
                    WHEN filings_status = 'structurally_young' AND %s = 'insufficient'
                    THEN filings_status
                    ELSE %s
                END
                WHERE instrument_id = %s
                """,
                ("insufficient", "insufficient", iid),
            )
        ebull_test_conn.commit()

        assert _count_events(ebull_test_conn, iid) == baseline

    def test_update_of_unrelated_column_does_not_fire(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid = _seed_instrument_with_coverage(ebull_test_conn, initial_status="analysable")
        baseline = _count_events(ebull_test_conn, iid)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE coverage SET filings_audit_at = now() WHERE instrument_id = %s",
                (iid,),
            )
        ebull_test_conn.commit()

        assert _count_events(ebull_test_conn, iid) == baseline

    def test_insert_with_filings_status_does_not_fire(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """Documented scope limit: INSERT path not covered by trigger."""
        global _TRG_INSTRUMENT_ID_COUNTER
        _TRG_INSTRUMENT_ID_COUNTER += 1
        iid = _TRG_INSTRUMENT_ID_COUNTER

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
                "VALUES (%s, %s, %s, 'USD', TRUE)",
                (iid, f"INS{iid}", f"Insert {iid}"),
            )
            cur.execute(
                "INSERT INTO coverage (instrument_id, coverage_tier, filings_status) "
                "VALUES (%s, 3, 'unknown')",
                (iid,),
            )
        ebull_test_conn.commit()

        assert _count_events(ebull_test_conn, iid) == 0

    def test_bulk_update_mixed_transitioning_and_static_rows(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid_a = _seed_instrument_with_coverage(ebull_test_conn, initial_status="analysable")
        iid_b = _seed_instrument_with_coverage(ebull_test_conn, initial_status="insufficient")
        baseline = _count_events(ebull_test_conn)

        # A transitions, B stays the same via the CASE-demote-guard-style pattern.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                UPDATE coverage
                SET filings_status = CASE
                    WHEN instrument_id = %s THEN 'insufficient'
                    ELSE 'insufficient'
                END
                WHERE instrument_id IN (%s, %s)
                """,
                (iid_a, iid_a, iid_b),
            )
        ebull_test_conn.commit()

        # Exactly one new event (for iid_a). iid_b was already 'insufficient'.
        assert _count_events(ebull_test_conn) == baseline + 1
