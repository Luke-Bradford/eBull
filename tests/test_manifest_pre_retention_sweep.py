"""Tests for the #1686 bulk pre-retention tombstone sweep.

Covers the inclusion/exclusion rule (only the pre-fetch-``filed_at``-gated
sources are swept), the cutoff-boundary semantics, and that excluded
sources are never touched (the silent-data-loss guard).
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg
import psycopg.rows
import pytest

from app.services.blockholders import blockholders_retention_cutoff
from app.services.insider_transactions import form4_retention_cutoff, form5_retention_cutoff
from app.services.manifest_pre_retention_sweep import gated_cutoffs, sweep_pre_retention
from app.services.sec_manifest import record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn, test_database_url  # noqa: F401 — fixture re-export

# Clearly inside / outside both cutoffs (form4 today-3y; 13d/g 2024-12-18).
_OLD = datetime(2018, 1, 15, tzinfo=UTC)  # pre every cutoff
_RECENT = datetime(2026, 6, 1, tzinfo=UTC)  # post every cutoff


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, cik: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, f"SYM{iid}", f"SYM{iid} Inc"),
    )
    conn.execute(
        """
        INSERT INTO instrument_sec_profile (instrument_id, cik)
        VALUES (%s, %s)
        ON CONFLICT (instrument_id) DO UPDATE SET cik = EXCLUDED.cik
        """,
        (iid, cik),
    )


def _seed(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    source: str,
    filed_at: datetime,
    issuer_iid: int | None,
    cik: str,
    subject_type: str,
) -> None:
    record_manifest_entry(
        conn,
        accession,
        cik=cik,
        form="4" if source == "sec_form4" else "X",
        source=source,  # type: ignore[arg-type]
        subject_type=subject_type,  # type: ignore[arg-type]
        subject_id=cik if subject_type != "issuer" else str(issuer_iid),
        instrument_id=issuer_iid,
        filed_at=filed_at,
    )


def test_gated_cutoffs_resolve_to_source_functions() -> None:
    """SINGLE-source-of-truth: the resolver returns the sources' OWN cutoff
    functions (identity), never a copied literal — and exactly the three
    pre-fetch-gated sources."""
    gc = gated_cutoffs()
    assert set(gc) == {"sec_form4", "sec_form5", "sec_13d", "sec_13g"}
    assert gc["sec_form4"] is form4_retention_cutoff
    assert gc["sec_form5"] is form5_retention_cutoff
    assert gc["sec_13d"] is blockholders_retention_cutoff
    assert gc["sec_13g"] is blockholders_retention_cutoff


def _status(conn: psycopg.Connection[tuple], accession: str) -> str:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT ingest_status FROM sec_filing_manifest WHERE accession_number = %s", (accession,))
        row = cur.fetchone()
    assert row is not None
    return row["ingest_status"]


@pytest.mark.db
def test_sweep_tombstones_only_gated_pre_cutoff_rows(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, iid=1, cik="0000000001")

    # Gated sources — OLD (pre-cutoff, must tombstone) + RECENT (in-retention, stays).
    _seed(
        conn,
        accession="0000000001-18-000001",
        source="sec_form4",
        filed_at=_OLD,
        issuer_iid=1,
        cik="0000000001",
        subject_type="issuer",
    )  # noqa: E501
    _seed(
        conn,
        accession="0000000001-26-000001",
        source="sec_form4",
        filed_at=_RECENT,
        issuer_iid=1,
        cik="0000000001",
        subject_type="issuer",
    )  # noqa: E501
    _seed(
        conn,
        accession="0000000002-18-000001",
        source="sec_13d",
        filed_at=_OLD,
        issuer_iid=None,
        cik="0000000002",
        subject_type="blockholder_filer",
    )  # noqa: E501
    _seed(
        conn,
        accession="0000000002-26-000001",
        source="sec_13d",
        filed_at=_RECENT,
        issuer_iid=None,
        cik="0000000002",
        subject_type="blockholder_filer",
    )  # noqa: E501
    _seed(
        conn,
        accession="0000000003-18-000001",
        source="sec_13g",
        filed_at=_OLD,
        issuer_iid=None,
        cik="0000000003",
        subject_type="blockholder_filer",
    )  # noqa: E501

    # EXCLUDED sources — OLD rows that MUST stay pending (no pre-fetch gate;
    # a filed_at sweep here would be silent data loss).
    _seed(
        conn,
        accession="0000000001-18-000002",
        source="sec_10q",
        filed_at=_OLD,
        issuer_iid=1,
        cik="0000000001",
        subject_type="issuer",
    )  # noqa: E501
    _seed(
        conn,
        accession="0000000001-18-000003",
        source="sec_def14a",
        filed_at=_OLD,
        issuer_iid=1,
        cik="0000000001",
        subject_type="issuer",
    )  # noqa: E501
    _seed(
        conn,
        accession="0000000004-18-000001",
        source="sec_13f_hr",
        filed_at=_OLD,
        issuer_iid=None,
        cik="0000000004",
        subject_type="institutional_filer",
    )  # noqa: E501
    conn.commit()

    summary = sweep_pre_retention(database_url=test_database_url())

    # Per-source counts: 1 form4 + 1 13d + 1 13g tombstoned; form5 seeded none.
    assert summary.by_source == {"sec_form4": 1, "sec_form5": 0, "sec_13d": 1, "sec_13g": 1}
    assert summary.total == 3

    # Re-read on a fresh transaction so the autocommit sweep's writes are visible.
    conn.rollback()
    assert _status(conn, "0000000001-18-000001") == "tombstoned"  # form4 OLD
    assert _status(conn, "0000000002-18-000001") == "tombstoned"  # 13d OLD
    assert _status(conn, "0000000003-18-000001") == "tombstoned"  # 13g OLD
    # in-retention gated rows untouched
    assert _status(conn, "0000000001-26-000001") == "pending"  # form4 RECENT
    assert _status(conn, "0000000002-26-000001") == "pending"  # 13d RECENT
    # excluded sources untouched even though OLD
    assert _status(conn, "0000000001-18-000002") == "pending"  # 10q OLD
    assert _status(conn, "0000000001-18-000003") == "pending"  # def14a OLD
    assert _status(conn, "0000000004-18-000001") == "pending"  # 13f OLD


@pytest.mark.db
def test_sweep_is_idempotent(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    """A second run over an already-swept boundary tombstones 0 rows."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=1, cik="0000000001")
    _seed(
        conn,
        accession="0000000001-18-000001",
        source="sec_form4",
        filed_at=_OLD,
        issuer_iid=1,
        cik="0000000001",
        subject_type="issuer",
    )  # noqa: E501
    conn.commit()

    first = sweep_pre_retention(database_url=test_database_url())
    assert first.total == 1
    second = sweep_pre_retention(database_url=test_database_url())
    assert second.total == 0
