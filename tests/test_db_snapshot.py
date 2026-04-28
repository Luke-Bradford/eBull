"""Integration tests for ``app.db.snapshot.snapshot_read`` (#395).

The helper opens a REPEATABLE READ transaction so multi-query read
handlers see one consistent snapshot. Verified by:

  1. Setting up a known row count.
  2. Opening ``snapshot_read`` and executing the first read.
  3. From a second connection, INSERTing a row that would otherwise
     change the count.
  4. Executing a second read inside the same ``snapshot_read`` block
     and asserting the count is unchanged from step 2.

Without REPEATABLE READ the second read would see the new row.
"""

from __future__ import annotations

import psycopg
import pytest

from app.db.snapshot import snapshot_read
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401
from tests.fixtures.ebull_test_db import test_database_url as _test_database_url

pytestmark = pytest.mark.integration


def _seed_operator(conn: psycopg.Connection[tuple]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO operators (operator_id, username, password_hash)
            VALUES ('22222222-2222-2222-2222-222222222222', 'snapshot_test_op', 'x')
            ON CONFLICT DO NOTHING
            """
        )
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (501, 'SNP', 'Snapshot test', 'USD', TRUE) "
            "ON CONFLICT DO NOTHING"
        )
    conn.commit()


def _count_guard_fails(conn: psycopg.Connection[tuple]) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM decision_audit "
        "WHERE pass_fail = 'FAIL' AND stage = 'execution_guard' "
        "AND decision_time >= now() - INTERVAL '7 days'"
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_snapshot_read_isolates_from_concurrent_insert(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A concurrent INSERT on a second connection must not be visible
    to the second read inside ``snapshot_read``."""
    _seed_operator(ebull_test_conn)

    with snapshot_read(ebull_test_conn):
        before = _count_guard_fails(ebull_test_conn)

        # Concurrent writer on a second connection. The COMMIT here
        # publishes the new row globally — but the snapshot-bound
        # reader connection must not see it because REPEATABLE READ
        # pins its visible snapshot to transaction start.
        with psycopg.connect(_test_database_url()) as writer:
            with writer.cursor() as cur:
                cur.execute(
                    "INSERT INTO decision_audit "
                    "(decision_time, instrument_id, stage, pass_fail, explanation) "
                    "VALUES (now(), 501, 'execution_guard', 'FAIL', 'concurrent')"
                )
            writer.commit()

        after = _count_guard_fails(ebull_test_conn)

    assert after == before, f"snapshot_read must hide concurrent inserts; saw before={before} after={after}"

    # Outside the block, the next read sees the now-committed row.
    eventual = _count_guard_fails(ebull_test_conn)
    assert eventual == before + 1


def test_snapshot_read_isolates_after_prior_statement(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Codex regression for #395: pool connections are autocommit-off,
    so any statement issued BEFORE ``snapshot_read`` opens an implicit
    READ COMMITTED transaction. ``conn.transaction()`` inside the
    helper would then nest as a SAVEPOINT and the isolation_level
    change never takes effect — the handler would silently inherit
    READ COMMITTED.

    The helper commits the implicit transaction first; this test
    seeds a prior SELECT (mirrors what a FastAPI dependency does
    when it resolves an operator id) and asserts the snapshot still
    hides concurrent inserts.
    """
    _seed_operator(ebull_test_conn)

    # Prior statement opens the implicit transaction. Mirrors
    # _resolve_operator(conn) in app/api/alerts.py.
    ebull_test_conn.execute("SELECT 1").fetchone()

    with snapshot_read(ebull_test_conn):
        before = _count_guard_fails(ebull_test_conn)

        with psycopg.connect(_test_database_url()) as writer:
            with writer.cursor() as cur:
                cur.execute(
                    "INSERT INTO decision_audit "
                    "(decision_time, instrument_id, stage, pass_fail, explanation) "
                    "VALUES (now(), 501, 'execution_guard', 'FAIL', 'codex-regression')"
                )
            writer.commit()

        after = _count_guard_fails(ebull_test_conn)

    assert after == before, (
        "snapshot_read must commit any prior implicit transaction so "
        "the isolation change opens a real REPEATABLE READ snapshot, "
        f"not a nested savepoint; saw before={before} after={after}"
    )


def test_snapshot_read_restores_isolation_level(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The connection's isolation level must be restored on exit so
    pool reuse does not leak REPEATABLE READ into the next request."""
    prior = ebull_test_conn.isolation_level
    with snapshot_read(ebull_test_conn):
        pass
    assert ebull_test_conn.isolation_level == prior


def test_snapshot_read_restores_isolation_level_on_exception(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Restoration must run even when the block raises — otherwise a
    test that errors mid-handler would leak REPEATABLE READ."""
    prior = ebull_test_conn.isolation_level
    with pytest.raises(RuntimeError, match="boom"):
        with snapshot_read(ebull_test_conn):
            raise RuntimeError("boom")
    assert ebull_test_conn.isolation_level == prior
