"""The 13F per-accession advisory lock helper issues the canonical key (#1542 Task A)."""

from unittest.mock import MagicMock

from app.services.institutional_holdings import acquire_13f_accession_write_lock


def test_helper_issues_xact_lock_with_accession_keyed_hash():
    conn = MagicMock()
    acquire_13f_accession_write_lock(conn, "0001234567-25-000001")
    assert conn.execute.call_count == 1
    sql, params = conn.execute.call_args.args
    # transaction-scoped advisory lock (auto-released on commit)
    assert "pg_advisory_xact_lock" in sql
    # namespaced + accession-keyed via hashtextextended XOR — same key in both call sites
    assert "hashtextextended('ingest_13f_accession', 0)" in sql
    assert "hashtextextended(%s, 0)" in sql
    assert params == ("0001234567-25-000001",)
