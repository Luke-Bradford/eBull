"""#1187 — Empirical pg_locks count for partitioned-parent statements.

Pins the measured 431-lock claim (an unpruned SELECT on a
partitioned parent reserves ~431 distinct relation locks under PG17
with eBull's quarterly-partition layout). Without this test, a
Postgres upgrade or schema change that alters lock semantics could
silently invalidate the spec's floor justification.

Decisive integration probe for Codex 1a v1 WARNING — proves the
root-cause analysis against the real DB rather than mocking
``SHOW`` output.

Spec: ``docs/superpowers/specs/2026-05-17-pg-max-locks-per-tx-guard.md``
§2 (root cause) + §5.1 (floor justification) + §10 (audit pruning
hot paths).
"""

from __future__ import annotations

import psycopg

LOCK_COUNT_SQL = """
    SELECT COUNT(DISTINCT relation)
      FROM pg_locks
     WHERE pid = pg_backend_pid()
       AND locktype = 'relation'
"""


def test_unpruned_parent_select_locks_exceed_default_floor(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Unpruned SELECT on partitioned parent locks > 64 relations.

    Demonstrates the OOM root cause: with PG default ``max_locks=64``,
    a single such query overruns its allotted slice and adds pressure
    to the cluster-wide shared lock table.
    """
    ebull_test_conn.execute("BEGIN")
    try:
        ebull_test_conn.execute("SELECT 1 FROM ownership_insiders_observations LIMIT 1")
        row = ebull_test_conn.execute(LOCK_COUNT_SQL).fetchone()
        assert row is not None
        lock_count = int(row[0])
        assert lock_count > 64, (
            f"unpruned parent SELECT acquired only {lock_count} locks; "
            f"expected >64 to validate #1187 root cause analysis. Either "
            f"schema changed (partitions reduced) or PG semantics shifted "
            f"— re-evaluate spec §2 + §5.1 floor."
        )
    finally:
        ebull_test_conn.execute("ROLLBACK")


def test_pruned_parent_select_locks_within_default_floor(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Pruned SELECT (period_end predicate) prunes to one partition →
    far fewer locks. Pins the spec §10 audit recommendation that
    partition-key WHERE clauses fix the problem at code level.
    """
    ebull_test_conn.execute("BEGIN")
    try:
        ebull_test_conn.execute("SELECT 1 FROM ownership_insiders_observations WHERE period_end = '2024-03-31' LIMIT 1")
        row = ebull_test_conn.execute(LOCK_COUNT_SQL).fetchone()
        assert row is not None
        lock_count = int(row[0])
        assert lock_count < 64, (
            f"pruned parent SELECT acquired {lock_count} locks; expected "
            f"<64. Partition pruning may be broken or planner behaviour "
            f"changed — re-evaluate spec §10 audit recommendation."
        )
    finally:
        ebull_test_conn.execute("ROLLBACK")
