"""#2603 durable account provenance for core trades, against migrated Postgres."""

from __future__ import annotations

from typing import Any

import psycopg


def test_core_trade_proof_column_references_the_immutable_evidence_table(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    row = ebull_test_conn.execute(
        """
        SELECT pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        WHERE c.conrelid='strategy_trades'::regclass
          AND c.contype='f'
          AND pg_get_constraintdef(c.oid) LIKE '%core_eligibility_proof_id%'
        ORDER BY c.conname
        LIMIT 1
        """
    ).fetchone()
    assert row is not None
    definition = str(row[0])
    assert "strategy_core_eligibility_proofs" in definition
    assert "ON DELETE RESTRICT" in definition
