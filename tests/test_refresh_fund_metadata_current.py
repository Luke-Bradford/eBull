"""Tests for ``app.services.fund_metadata.refresh_fund_metadata_current`` (#1171).

Covers the source-priority chain + write-through semantics per spec §8.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest

from app.services.fund_metadata import refresh_fund_metadata_current
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    symbol: str = "VFIAX",
) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, 'Vanguard 500 Index', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol),
    )


def _insert_observation(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    source_accession: str,
    filed_at: datetime,
    period_end: date,
    document_type: str = "N-CSR",
    parser_version: str = "n-csr-fund-metadata-v1",
    expense_ratio_pct: Decimal | None = Decimal("0.0004"),
    known_to: datetime | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO fund_metadata_observations (
            instrument_id, source_accession, filed_at, period_end,
            document_type, amendment_flag, parser_version,
            trust_cik, class_id, expense_ratio_pct,
            known_from, known_to
        ) VALUES (
            %s, %s, %s, %s, %s, FALSE, %s,
            '0000036405', 'C000000001', %s,
            NOW(), %s
        )
        """,
        (
            instrument_id,
            source_accession,
            filed_at,
            period_end,
            document_type,
            parser_version,
            expense_ratio_pct,
            known_to,
        ),
    )


def test_inserted_first_observation(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    _seed_instrument(ebull_test_conn, iid=2001)
    _insert_observation(
        ebull_test_conn,
        instrument_id=2001,
        source_accession="0001-25-000001",
        filed_at=datetime(2026, 2, 27, tzinfo=UTC),
        period_end=date(2025, 12, 31),
    )
    ebull_test_conn.commit()

    outcome = refresh_fund_metadata_current(ebull_test_conn, instrument_id=2001)
    assert outcome == "inserted"

    cur = ebull_test_conn.execute(
        "SELECT source_accession, expense_ratio_pct FROM fund_metadata_current WHERE instrument_id = %s",
        (2001,),
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] == "0001-25-000001"
    assert row[1] == Decimal("0.00040000")


def test_updated_newer_period_end(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    _seed_instrument(ebull_test_conn, iid=2002)
    _insert_observation(
        ebull_test_conn,
        instrument_id=2002,
        source_accession="0001-25-000010",
        filed_at=datetime(2025, 8, 27, tzinfo=UTC),
        period_end=date(2025, 6, 30),
        document_type="N-CSRS",
    )
    refresh_fund_metadata_current(ebull_test_conn, instrument_id=2002)

    _insert_observation(
        ebull_test_conn,
        instrument_id=2002,
        source_accession="0001-26-000020",
        filed_at=datetime(2026, 2, 27, tzinfo=UTC),
        period_end=date(2025, 12, 31),  # newer period_end
    )
    ebull_test_conn.commit()

    outcome = refresh_fund_metadata_current(ebull_test_conn, instrument_id=2002)
    assert outcome == "updated"

    cur = ebull_test_conn.execute(
        "SELECT source_accession FROM fund_metadata_current WHERE instrument_id = %s",
        (2002,),
    )
    assert cur.fetchone()[0] == "0001-26-000020"


def test_filed_at_tie_break_amendment_wins(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """N-CSR/A (amendment) with later filed_at + same period_end wins over plain N-CSR."""
    _seed_instrument(ebull_test_conn, iid=2003)
    _insert_observation(
        ebull_test_conn,
        instrument_id=2003,
        source_accession="0001-26-AAA",
        filed_at=datetime(2026, 2, 27, tzinfo=UTC),
        period_end=date(2025, 12, 31),
        document_type="N-CSR",
    )
    _insert_observation(
        ebull_test_conn,
        instrument_id=2003,
        source_accession="0001-26-AMD",
        filed_at=datetime(2026, 3, 15, tzinfo=UTC),  # later filed_at
        period_end=date(2025, 12, 31),
        document_type="N-CSR/A",
    )
    ebull_test_conn.commit()

    outcome = refresh_fund_metadata_current(ebull_test_conn, instrument_id=2003)
    assert outcome == "inserted"
    cur = ebull_test_conn.execute(
        "SELECT source_accession, document_type FROM fund_metadata_current WHERE instrument_id = %s",
        (2003,),
    )
    row = cur.fetchone()
    assert row[0] == "0001-26-AMD"
    assert row[1] == "N-CSR/A"


def test_source_accession_tie_break_degenerate(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Same period_end + same filed_at (degenerate) → source_accession DESC wins."""
    _seed_instrument(ebull_test_conn, iid=2004)
    same_filed_at = datetime(2026, 2, 27, tzinfo=UTC)
    _insert_observation(
        ebull_test_conn,
        instrument_id=2004,
        source_accession="0001-26-AAA",
        filed_at=same_filed_at,
        period_end=date(2025, 12, 31),
    )
    _insert_observation(
        ebull_test_conn,
        instrument_id=2004,
        source_accession="0001-26-ZZZ",  # sorts after AAA
        filed_at=same_filed_at,
        period_end=date(2025, 12, 31),
    )
    ebull_test_conn.commit()

    refresh_fund_metadata_current(ebull_test_conn, instrument_id=2004)
    cur = ebull_test_conn.execute(
        "SELECT source_accession FROM fund_metadata_current WHERE instrument_id = %s",
        (2004,),
    )
    assert cur.fetchone()[0] == "0001-26-ZZZ"


def test_idempotent_second_refresh_returns_updated(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Second refresh with unchanged observation state returns 'updated' (not
    'suppressed' — we always UPSERT because rewash content can change without
    changing the source_accession/filed_at provenance triple)."""
    _seed_instrument(ebull_test_conn, iid=2005)
    _insert_observation(
        ebull_test_conn,
        instrument_id=2005,
        source_accession="0001-26-IDEM",
        filed_at=datetime(2026, 2, 27, tzinfo=UTC),
        period_end=date(2025, 12, 31),
    )
    ebull_test_conn.commit()

    assert refresh_fund_metadata_current(ebull_test_conn, instrument_id=2005) == "inserted"
    assert refresh_fund_metadata_current(ebull_test_conn, instrument_id=2005) == "updated"


def test_known_to_filter_excludes_superseded(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Superseded observation (known_to NOT NULL) must not be picked as winner."""
    _seed_instrument(ebull_test_conn, iid=2006)
    # Old superseded row (would win on period_end DESC if known_to were NULL).
    _insert_observation(
        ebull_test_conn,
        instrument_id=2006,
        source_accession="0001-26-OLD",
        filed_at=datetime(2026, 5, 1, tzinfo=UTC),
        period_end=date(2026, 3, 31),
        known_to=datetime(2026, 5, 14, tzinfo=UTC),
    )
    # Current valid row.
    _insert_observation(
        ebull_test_conn,
        instrument_id=2006,
        source_accession="0001-26-NEW",
        filed_at=datetime(2026, 2, 27, tzinfo=UTC),
        period_end=date(2025, 12, 31),
    )
    ebull_test_conn.commit()

    refresh_fund_metadata_current(ebull_test_conn, instrument_id=2006)
    cur = ebull_test_conn.execute(
        "SELECT source_accession FROM fund_metadata_current WHERE instrument_id = %s",
        (2006,),
    )
    assert cur.fetchone()[0] == "0001-26-NEW"


def test_deleted_when_no_currently_valid_observations(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    _seed_instrument(ebull_test_conn, iid=2007)
    _insert_observation(
        ebull_test_conn,
        instrument_id=2007,
        source_accession="0001-26-X",
        filed_at=datetime(2026, 2, 27, tzinfo=UTC),
        period_end=date(2025, 12, 31),
    )
    ebull_test_conn.commit()
    refresh_fund_metadata_current(ebull_test_conn, instrument_id=2007)

    # Supersede the only observation.
    ebull_test_conn.execute(
        "UPDATE fund_metadata_observations SET known_to = NOW() WHERE instrument_id = %s",
        (2007,),
    )
    ebull_test_conn.commit()

    outcome = refresh_fund_metadata_current(ebull_test_conn, instrument_id=2007)
    assert outcome == "deleted"
    cur = ebull_test_conn.execute(
        "SELECT 1 FROM fund_metadata_current WHERE instrument_id = %s",
        (2007,),
    )
    assert cur.fetchone() is None
