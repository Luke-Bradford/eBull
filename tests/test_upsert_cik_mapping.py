"""Regression tests for upsert_cik_mapping — pins #257 / #267 fix.

external_identifiers has two uniqueness constraints:

- uq_external_identifiers_provider_value — UNIQUE(provider, identifier_type,
  identifier_value)
- uq_external_identifiers_primary — partial UNIQUE(instrument_id, provider,
  identifier_type) WHERE is_primary=TRUE

ON CONFLICT in upsert_cik_mapping targets the first. The partial UNIQUE is
handled by demoting any mismatching primary row first. These tests lock that
behaviour so a future refactor cannot reintroduce the UniqueViolation that
crashed daily_cik_refresh on every repeat run (#257).
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.filings import upsert_cik_mapping
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
) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (%s, %s, %s, TRUE)",
        (instrument_id, symbol, symbol),
    )
    conn.commit()


def _primary_cik(
    conn: psycopg.Connection[tuple],
    instrument_id: int,
) -> str | None:
    row = conn.execute(
        "SELECT identifier_value FROM external_identifiers "
        "WHERE instrument_id = %s AND provider = 'sec' "
        "AND identifier_type = 'cik' AND is_primary = TRUE",
        (instrument_id,),
    ).fetchone()
    return row[0] if row else None


def _all_rows(
    conn: psycopg.Connection[tuple],
    instrument_id: int,
) -> list[tuple[str, bool]]:
    rows = conn.execute(
        "SELECT identifier_value, is_primary FROM external_identifiers "
        "WHERE instrument_id = %s AND provider = 'sec' "
        "AND identifier_type = 'cik' ORDER BY identifier_value",
        (instrument_id,),
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def test_first_insert_creates_primary_row(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, instrument_id=1, symbol="AAPL")

    upserted = upsert_cik_mapping(
        conn,
        {"AAPL": "0000320193"},
        [("AAPL", "1")],
    )

    assert upserted == 1
    assert _primary_cik(conn, 1) == "0000320193"


def test_idempotent_rerun_same_mapping(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, instrument_id=1, symbol="AAPL")

    upsert_cik_mapping(conn, {"AAPL": "0000320193"}, [("AAPL", "1")])
    upsert_cik_mapping(conn, {"AAPL": "0000320193"}, [("AAPL", "1")])

    assert _all_rows(conn, 1) == [("0000320193", True)]


def test_cik_change_demotes_prior_primary(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """SEC ticker map hands a different CIK for the same instrument.

    Before #267: partial UNIQUE fired because the old primary row lived on.
    After: the old row is demoted to is_primary=FALSE and the new row takes
    the primary slot.
    """
    conn = ebull_test_conn
    _seed_instrument(conn, instrument_id=1, symbol="AAPL")

    upsert_cik_mapping(conn, {"AAPL": "0000320193"}, [("AAPL", "1")])
    upsert_cik_mapping(conn, {"AAPL": "0000999999"}, [("AAPL", "1")])

    assert _primary_cik(conn, 1) == "0000999999"
    assert _all_rows(conn, 1) == [
        ("0000320193", False),
        ("0000999999", True),
    ]


def test_cik_reassigned_to_different_instrument(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Same CIK moves from instrument A to instrument B.

    Exercises the (provider, identifier_type, identifier_value) conflict path:
    ON CONFLICT updates instrument_id to the new owner.
    """
    conn = ebull_test_conn
    _seed_instrument(conn, instrument_id=1, symbol="OLD")
    _seed_instrument(conn, instrument_id=2, symbol="NEW")

    upsert_cik_mapping(conn, {"OLD": "0000555555"}, [("OLD", "1")])
    upsert_cik_mapping(conn, {"NEW": "0000555555"}, [("NEW", "2")])

    assert _primary_cik(conn, 1) is None
    assert _primary_cik(conn, 2) == "0000555555"


def test_symbol_missing_from_mapping_is_skipped(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, instrument_id=1, symbol="AAPL")

    upserted = upsert_cik_mapping(conn, {}, [("AAPL", "1")])

    assert upserted == 0
    assert _primary_cik(conn, 1) is None
