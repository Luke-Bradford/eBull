"""Integration tests for coverage_audit against real ebull_test DB.

Verifies the SQL aggregate's window filtering + bulk UPDATE landing
correctly per instrument.
"""

from __future__ import annotations

from datetime import date, timedelta

import psycopg
import pytest

from app.services.coverage_audit import (
    audit_all_instruments,
    audit_instrument,
)
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    symbol: str,
    cik: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (instrument_id, symbol, symbol),
    )
    conn.execute(
        "INSERT INTO coverage (instrument_id, coverage_tier) VALUES (%s, 3)",
        (instrument_id,),
    )
    if cik is not None:
        conn.execute(
            "INSERT INTO external_identifiers "
            "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cik', %s, TRUE)",
            (instrument_id, cik),
        )
    conn.commit()


def _seed_filing(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    filing_date: date,
    filing_type: str,
    accession: str,
    provider: str = "sec",
) -> None:
    conn.execute(
        """
        INSERT INTO filing_events (
            instrument_id, filing_date, filing_type,
            provider, provider_filing_id
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        (instrument_id, filing_date, filing_type, provider, accession),
    )
    conn.commit()


def test_analysable_us_issuer_with_full_history(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """US issuer with 2 × 10-K in 3y + 4 × 10-Q in 18mo → analysable."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="AAPL", cik="0000320193")
    today = date.today()
    # 2 × 10-K in past 3 years
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=today - timedelta(days=400),
        filing_type="10-K",
        accession="0000320193-25-000001",
    )
    # Second 10-K at days=700: well inside the 3-year window
    # (1095 days) with ~12 months of safety margin so the test
    # doesn't become a calendar-drift time-bomb a year from now.
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=today - timedelta(days=700),
        filing_type="10-K",
        accession="0000320193-24-000001",
    )
    # 4 × 10-Q in past 18 months
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=today - timedelta(days=30),
        filing_type="10-Q",
        accession="0000320193-26-000001",
    )
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=today - timedelta(days=120),
        filing_type="10-Q",
        accession="0000320193-26-000002",
    )
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=today - timedelta(days=210),
        filing_type="10-Q",
        accession="0000320193-26-000003",
    )
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=today - timedelta(days=300),
        filing_type="10-Q",
        accession="0000320193-25-000042",
    )

    summary = audit_all_instruments(ebull_test_conn)

    assert summary.analysable == 1
    assert summary.total_updated == 1

    row = ebull_test_conn.execute(
        "SELECT filings_status, filings_audit_at FROM coverage WHERE instrument_id = 1"
    ).fetchone()
    assert row is not None
    assert row[0] == "analysable"
    assert row[1] is not None


def test_insufficient_below_bar(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """US issuer below the bar (1 × 10-K, 3 × 10-Q) → insufficient."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000000001")
    today = date.today()
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=today - timedelta(days=100),
        filing_type="10-K",
        accession="0000000001-26-000001",
    )
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=today - timedelta(days=30),
        filing_type="10-Q",
        accession="0000000001-26-000002",
    )
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=today - timedelta(days=120),
        filing_type="10-Q",
        accession="0000000001-26-000003",
    )
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=today - timedelta(days=210),
        filing_type="10-Q",
        accession="0000000001-26-000004",
    )

    summary = audit_all_instruments(ebull_test_conn)

    assert summary.insufficient == 1
    row = ebull_test_conn.execute("SELECT filings_status FROM coverage WHERE instrument_id = 1").fetchone()
    assert row is not None and row[0] == "insufficient"


def test_fpi_with_20f_only(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """SEC CIK + 1 × 20-F + zero 10-K/10-Q → fpi."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="FPI1", cik="0001234567")
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=date.today() - timedelta(days=60),
        filing_type="20-F",
        accession="0001234567-26-000001",
    )

    summary = audit_all_instruments(ebull_test_conn)

    assert summary.fpi == 1
    row = ebull_test_conn.execute("SELECT filings_status FROM coverage WHERE instrument_id = 1").fetchone()
    assert row is not None and row[0] == "fpi"


def test_no_primary_sec_cik(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Tradable instrument with no primary SEC CIK → no_primary_sec_cik."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="UK01")  # no cik

    summary = audit_all_instruments(ebull_test_conn)

    assert summary.no_primary_sec_cik == 1
    row = ebull_test_conn.execute("SELECT filings_status FROM coverage WHERE instrument_id = 1").fetchone()
    assert row is not None and row[0] == "no_primary_sec_cik"


def test_amendments_do_not_count_toward_bar(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """1 × 10-K + 1 × 10-K/A + 4 × 10-Q → insufficient (amendment
    restates same year, not a distinct second annual period)."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000000002")
    today = date.today()
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=today - timedelta(days=400),
        filing_type="10-K",
        accession="0000000002-26-000001",
    )
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=today - timedelta(days=350),
        filing_type="10-K/A",
        accession="0000000002-26-000002",
    )
    for i, offset in enumerate([30, 120, 210, 300], start=3):
        _seed_filing(
            ebull_test_conn,
            instrument_id=1,
            filing_date=today - timedelta(days=offset),
            filing_type="10-Q",
            accession=f"0000000002-26-00000{i}",
        )

    summary = audit_all_instruments(ebull_test_conn)

    assert summary.insufficient == 1
    row = ebull_test_conn.execute("SELECT filings_status FROM coverage WHERE instrument_id = 1").fetchone()
    assert row is not None and row[0] == "insufficient"


def test_filings_provider_filter_excludes_companies_house(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Companies House filings for a SEC-CIK instrument must NOT count
    toward the SEC bar."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="DUAL", cik="0000000003")
    today = date.today()
    # CH filings would match on filing_type label but MUST NOT count.
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=today - timedelta(days=30),
        filing_type="10-K",
        accession="CH-26-000001",
        provider="companies_house",
    )
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=today - timedelta(days=120),
        filing_type="10-K",
        accession="CH-26-000002",
        provider="companies_house",
    )
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=today - timedelta(days=210),
        filing_type="10-Q",
        accession="CH-26-000003",
        provider="companies_house",
    )

    summary = audit_all_instruments(ebull_test_conn)

    # Zero SEC filings despite 3 CH rows → insufficient, not analysable.
    assert summary.insufficient == 1
    assert summary.analysable == 0


def test_idempotent_rerun(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Running the audit twice produces the same counts and same final status."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="UK02")

    first = audit_all_instruments(ebull_test_conn)
    second = audit_all_instruments(ebull_test_conn)

    assert first.no_primary_sec_cik == 1
    assert second.no_primary_sec_cik == 1
    assert first.total_updated == second.total_updated


def test_single_instrument_audit(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """audit_instrument classifies one instrument and updates its row."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="FPI", cik="0009999999")
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=date.today() - timedelta(days=90),
        filing_type="40-F",
        accession="0009999999-26-000001",
    )

    status = audit_instrument(ebull_test_conn, instrument_id=1)

    assert status == "fpi"
    row = ebull_test_conn.execute("SELECT filings_status FROM coverage WHERE instrument_id = 1").fetchone()
    assert row is not None and row[0] == "fpi"


def test_null_anomaly_detected(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A tradable instrument without a coverage row triggers null_anomalies."""
    # Instrument exists + is tradable but NO coverage row.
    ebull_test_conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (1, 'ORPHAN', 'ORPHAN', TRUE)"
    )
    ebull_test_conn.commit()

    summary = audit_all_instruments(ebull_test_conn)

    assert summary.null_anomalies >= 1


def test_audit_instrument_raises_on_missing_coverage_row(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Single-instrument audit must NOT silently succeed when the
    coverage row is missing (a Chunk B regression). Raising loudly
    beats returning a status string that was never persisted."""
    # Instrument exists + tradable but NO coverage row.
    ebull_test_conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (42, 'NOCOV', 'NOCOV', TRUE)"
    )
    ebull_test_conn.commit()

    with pytest.raises(RuntimeError, match="no coverage row"):
        audit_instrument(ebull_test_conn, instrument_id=42)
