"""Behaviour test for migration 182 — #1320 PRE 14A purge + freshness repair.

Seeds the exact pre-migration pollution (PRE 14A manifest rows that advanced
`data_freshness_index.last_known_*`), runs the migration SQL inline, asserts:
  - PRE 14A manifest rows are deleted.
  - A subject with a real DEF 14A has its freshness pointer recomputed to the
    DEF accession (dangling PRE pointer repaired).
  - A PRE-only subject has its freshness pointer cleared to NULL / 'unknown'.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest

from app.services.sec_manifest import record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration

_MIGRATION_SQL = (Path(__file__).resolve().parents[1] / "sql" / "182_purge_pre14a_manifest_rows.sql").read_text()

_IID_BOTH = 8_820_001  # subject with both DEF 14A and a (newer) PRE 14A
_IID_PRE_ONLY = 8_820_002  # subject with only a PRE 14A

_DEF_ACC = "0000820001-26-000001"
_PRE_ACC_BOTH = "0000820001-26-000002"
_PRE_ACC_ONLY = "0000820002-26-000001"

_DEF_FILED = datetime(2026, 1, 15, tzinfo=UTC)
_PRE_FILED = datetime(2026, 5, 1, tzinfo=UTC)  # newer than DEF → advances freshness


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_manifest(
    conn: psycopg.Connection[tuple], *, accession: str, iid: int, cik: str, form: str, filed_at: datetime
) -> None:
    # record_manifest_entry seeds data_freshness_index inline (latest-row
    # semantics), so a newer PRE 14A advances last_known_* to the draft —
    # exactly the production pollution #1320 fixes.
    record_manifest_entry(
        conn,
        accession,
        cik=cik,
        form=form,
        source="sec_def14a",
        subject_type="issuer",
        subject_id=cik,
        instrument_id=iid,
        filed_at=filed_at,
        primary_document_url=f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/d.htm",
    )


def _seed_pre_migration_state(conn: psycopg.Connection[tuple]) -> None:
    _seed_instrument(conn, _IID_BOTH, "BOTH")
    _seed_instrument(conn, _IID_PRE_ONLY, "PREO")
    _seed_manifest(conn, accession=_DEF_ACC, iid=_IID_BOTH, cik="0000820001", form="DEF 14A", filed_at=_DEF_FILED)
    _seed_manifest(conn, accession=_PRE_ACC_BOTH, iid=_IID_BOTH, cik="0000820001", form="PRE 14A", filed_at=_PRE_FILED)
    _seed_manifest(
        conn, accession=_PRE_ACC_ONLY, iid=_IID_PRE_ONLY, cik="0000820002", form="PRE 14A", filed_at=_PRE_FILED
    )
    conn.commit()


# Strip the migration's own BEGIN;/COMMIT; wrapper before inline execution:
# the test drives transaction control via the ebull_test_conn fixture, so an
# embedded COMMIT inside the executed string would commit out from under it and
# leave the trailing conn.commit() flushing an empty implicit transaction
# (behaviour then depends on the fixture's autocommit mode). Run the migration
# body inside the fixture's transaction and let the test commit once.
_MIGRATION_BODY = "\n".join(line for line in _MIGRATION_SQL.splitlines() if line.strip() not in ("BEGIN;", "COMMIT;"))


def _run_migration(conn: psycopg.Connection[tuple]) -> None:
    with psycopg.ClientCursor(conn) as cur:
        cur.execute(_MIGRATION_BODY)  # type: ignore[call-overload]
    conn.commit()


def test_pre_migration_state_has_pre_polluted_freshness(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Sanity: before the migration, both subjects' freshness pointer aims at
    the PRE 14A draft (newest filed_at)."""
    conn = ebull_test_conn
    _seed_pre_migration_state(conn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT subject_id, last_known_filing_id FROM data_freshness_index "
            "WHERE source='sec_def14a' ORDER BY subject_id"
        )
        rows = dict(cur.fetchall())
    assert rows["0000820001"] == _PRE_ACC_BOTH
    assert rows["0000820002"] == _PRE_ACC_ONLY


def test_migration_182_deletes_pre_manifest_rows(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_pre_migration_state(conn)
    _run_migration(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM sec_filing_manifest WHERE source='sec_def14a' AND TRIM(form)='PRE 14A'")
        result = cur.fetchone()
    assert result is not None and result[0] == 0


def test_migration_182_recomputes_freshness_for_subject_with_def(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Subject with a real DEF 14A: dangling PRE pointer is recomputed back to
    the DEF accession + its filed_at + expected_next_at = filed_at + 365d."""
    conn = ebull_test_conn
    _seed_pre_migration_state(conn)
    _run_migration(conn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_known_filing_id, last_known_filed_at, expected_next_at, state "
            "FROM data_freshness_index WHERE source='sec_def14a' AND subject_id='0000820001'"
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == _DEF_ACC
    assert row[1] == _DEF_FILED
    assert row[2] == _DEF_FILED.replace(year=2027)  # +365d (non-leap 2026)
    assert row[3] == "current"


def test_migration_182_clears_freshness_for_pre_only_subject(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """PRE-only subject: pointer aimed solely at a draft → cleared to NULL /
    'unknown' so it drops out of the MAX() watermark, row kept (still watched)."""
    conn = ebull_test_conn
    _seed_pre_migration_state(conn)
    _run_migration(conn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_known_filing_id, last_known_filed_at, expected_next_at, state, state_reason "
            "FROM data_freshness_index WHERE source='sec_def14a' AND subject_id='0000820002'"
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] is None
    assert row[1] is None
    assert row[2] is None
    assert row[3] == "unknown"
    assert row[4] == "pre14a_pointer_cleared_1320"
