"""The bounded strategy ledgers keep one useful index per lookup shape."""

from __future__ import annotations

from pathlib import Path

import psycopg


def test_duplicate_outcome_index_is_removed_but_unique_lookup_remains(
    ebull_test_conn: psycopg.Connection,
) -> None:
    indexes = {
        str(row[0]): str(row[1])
        for row in ebull_test_conn.execute(
            """
            SELECT indexname,indexdef
            FROM pg_indexes
            WHERE schemaname='public' AND tablename='strategy_outcomes'
            """
        ).fetchall()
    }

    assert "idx_strategy_outcomes_signal_versions" not in indexes
    assert "strategy_outcomes_unique" in indexes
    assert "UNIQUE INDEX" in indexes["strategy_outcomes_unique"]
    assert "(signal_id, rule_set_version, input_rule_set_version)" in indexes["strategy_outcomes_unique"]


def test_cleanup_is_bounded_concurrent_and_replay_safe() -> None:
    migration = Path(__file__).resolve().parents[1] / "sql" / "325_strategy_ledger_index_cleanup.sql"
    sql = migration.read_text()

    assert sql.lstrip().startswith("-- runner: autocommit")
    assert "DROP INDEX CONCURRENTLY IF EXISTS idx_strategy_outcomes_signal_versions" in sql
    assert "REINDEX INDEX CONCURRENTLY strategy_outcomes_unique" in sql
    assert "REINDEX INDEX CONCURRENTLY strategy_signals_unique" in sql
    assert "lock_timeout = '5s'" in sql
    assert "statement_timeout = '5min'" in sql
