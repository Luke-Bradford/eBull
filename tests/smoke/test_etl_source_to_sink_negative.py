"""#1322 — empirical negative test for source-to-sink smoke.

Isolated in its own file (per code-simplifier review F1 safety concern):
DDL-in-test + ROLLBACK pattern carries pollution risk if the rollback ever
fails on an exception path. Separate file confines the failure mode and
caps the blast radius. The fixture (`ebull_test_conn`) owns the
exception-path rollback + connection close in its own `finally`; this
test does explicit SAVEPOINT/ROLLBACK TO SAVEPOINT for the inner DROP.

Pattern (multi-agent IMPORTANT-I1 + I5 fold; prevention-log "psycopg v3
conn.transaction() is savepoint not commit" applies):

* BEGIN → SAVEPOINT s1 → DROP TABLE ownership_funds_current
* Invoke positive-test assertion (must fire — table is gone)
* ROLLBACK TO SAVEPOINT s1 → ROLLBACK
* Post-rollback: assert table exists again (proves rollback worked, leaves
  worker DB clean for subsequent tests in same xdist session)

Target table choice (multi-agent IMPORTANT-I1 fold): `ownership_funds_current`
has no inbound FKs per `sql/123_ownership_funds.sql`; in `_PLANNER_TABLES`;
safe to drop transiently. NOT a partitioned table (would CASCADE across
quarter partitions and break unrelated tests via savepoint scope).

xdist_group serializes within worker group — no cross-test pollution
even if pytest dispatched two tests in this file concurrently (it doesn't,
but defence-in-depth).
"""

from __future__ import annotations

import psycopg
import pytest

from tests.smoke.test_etl_source_to_sink import _table_exists

pytestmark = pytest.mark.xdist_group(name="etl_source_to_sink_negative")

_TARGET_TABLE = "ownership_funds_current"


def test_negative_drop_target_fires_smoke_assertion_then_rolls_back(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#1322 — empirical proof the positive smoke catches a dropped table.

    DROP target → assert _table_exists returns False (positive smoke would
    FAIL here) → ROLLBACK → assert _table_exists returns True (rollback
    worked).

    Uses ``ebull_test_conn`` (worker-private DB per CLAUDE.md test isolation
    + Codex iter-on-diff cross-impact note) — never the dev DB. SAVEPOINT
    + ROLLBACK confines the DROP to the test's own transaction.
    """
    # Pre-condition: table exists before DROP
    assert _table_exists(ebull_test_conn, _TARGET_TABLE), (
        f"Pre-condition failed: {_TARGET_TABLE!r} missing from ebull_test DB "
        f"before negative test ran. Check migration state."
    )

    with ebull_test_conn.cursor() as cur:
        cur.execute("SAVEPOINT s1")
        cur.execute(f"DROP TABLE {_TARGET_TABLE}")

        # Inside the savepoint: positive-smoke assertion MUST fire
        assert not _table_exists(ebull_test_conn, _TARGET_TABLE), (
            f"Negative test bug: after DROP {_TARGET_TABLE}, _table_exists still returns True — savepoint scope wrong"
        )

        cur.execute("ROLLBACK TO SAVEPOINT s1")
        cur.execute("RELEASE SAVEPOINT s1")

    # Post-rollback: table exists again (proves rollback worked)
    assert _table_exists(ebull_test_conn, _TARGET_TABLE), (
        f"ROLLBACK TO SAVEPOINT failed to restore {_TARGET_TABLE!r}. "
        f"Worker DB is now poisoned for subsequent tests. Check savepoint "
        f"scope + psycopg v3 conn.transaction() semantics."
    )
