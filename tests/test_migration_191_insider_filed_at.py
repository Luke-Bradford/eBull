"""Behaviour test for migration 191 — #899 insider observations filed_at backfill.

Seeds observation rows carrying the legacy event-date semantics, runs the
migration SQL inline, asserts:
  - a row whose accession has a sec_filing_manifest row is restamped to the
    manifest filed_at (covers the bulk-path source_document_id shape too —
    the join keys on source_accession);
  - a row whose accession exists only in filing_events is restamped to the
    filing_date (legacy cohort fallback arm);
  - a row whose accession resolves in neither source is left untouched;
  - a non-insider source row is out of scope.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from app.services.ownership_observations import record_insider_observation
from app.services.sec_manifest import record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration

_MIGRATION_SQL = (
    Path(__file__).resolve().parents[1] / "sql" / "191_backfill_insider_observations_filed_at.sql"
).read_text()

_IID = 8_910_001
_ACC_MANIFEST = "0000891001-26-000001"  # has manifest row
_ACC_EVENTS = "0000891001-26-000002"  # filing_events only
_ACC_ORPHAN = "0000891001-26-000003"  # neither source

_WRONG_FILED = datetime(2026, 1, 5, tzinfo=UTC)  # legacy txn-date semantics
_MANIFEST_FILED = datetime(2026, 1, 9, 14, 30, tzinfo=UTC)
_EVENTS_FILED = date(2026, 1, 12)


def _seed_obs(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    source_document_id: str | None = None,
) -> None:
    record_insider_observation(
        conn,
        instrument_id=_IID,
        holder_cik="0001000001",
        holder_name="Test Insider",
        ownership_nature="direct",
        source="form4",
        source_document_id=source_document_id or accession,
        source_accession=accession,
        source_field=None,
        source_url=None,
        filed_at=_WRONG_FILED,
        period_start=None,
        period_end=date(2026, 1, 5),
        ingest_run_id=uuid4(),
        shares=Decimal("100"),
    )


def _obs_filed_at(conn: psycopg.Connection[tuple], source_document_id: str) -> datetime:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT filed_at FROM ownership_insiders_observations "
            "WHERE source = 'form4' AND source_document_id = %s "
            "ORDER BY filed_at DESC LIMIT 1",
            (source_document_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return row[0]


def test_migration_191_backfills_filed_at_from_manifest_then_filing_events(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, 'M191', 'M191 Inc', '4', 'USD', TRUE) ON CONFLICT (instrument_id) DO NOTHING
        """,
        (_IID,),
    )
    record_manifest_entry(
        conn,
        _ACC_MANIFEST,
        cik="0000891001",
        form="4",
        source="sec_form4",
        subject_type="issuer",
        subject_id=str(_IID),
        instrument_id=_IID,
        filed_at=_MANIFEST_FILED,
        primary_document_url="https://www.sec.gov/Archives/m191.xml",
    )
    conn.execute(
        """
        INSERT INTO filing_events (instrument_id, filing_date, filing_type, provider, provider_filing_id)
        VALUES (%s, %s, '4', 'sec', %s)
        """,
        (_IID, _EVENTS_FILED, _ACC_EVENTS),
    )
    # Manifest-resolved row, seeded under the BULK dataset's
    # source_document_id shape (accn:NDT:*) — the backfill must key on
    # source_accession, not source_document_id.
    _seed_obs(conn, accession=_ACC_MANIFEST, source_document_id=f"{_ACC_MANIFEST}:NDT:1")
    _seed_obs(conn, accession=_ACC_EVENTS)
    _seed_obs(conn, accession=_ACC_ORPHAN)
    conn.commit()

    conn.execute(_MIGRATION_SQL)  # type: ignore[arg-type]  # no embedded BEGIN/COMMIT (runner wraps)
    conn.commit()

    assert _obs_filed_at(conn, f"{_ACC_MANIFEST}:NDT:1") == _MANIFEST_FILED
    # filing_events arm: DATE pinned to UTC midnight by the migration's
    # `::timestamp AT TIME ZONE 'UTC'` (session-TZ-independent).
    assert _obs_filed_at(conn, _ACC_EVENTS) == datetime(
        _EVENTS_FILED.year, _EVENTS_FILED.month, _EVENTS_FILED.day, tzinfo=UTC
    )
    assert _obs_filed_at(conn, _ACC_ORPHAN) == _WRONG_FILED  # untouched
