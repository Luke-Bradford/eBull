"""Regression tests for upsert_cik_mapping.

Pins #257 / #267 (original constraint design) and #1102 (share-class
CIK relaxation). Post-#1102 ``external_identifiers`` enforces:

- uq_external_identifiers_provider_value_non_cik — partial UNIQUE
  (provider, identifier_type, identifier_value) WHERE NOT (sec/cik).
  CUSIP / symbol / accession_no remain globally unique.
- uq_external_identifiers_cik_per_instrument — partial UNIQUE
  (provider, identifier_type, identifier_value, instrument_id) WHERE
  (sec/cik). Multiple instruments may share a CIK (share-class
  siblings: GOOG/GOOGL, BRK.A/BRK.B); each (CIK, instrument) pair is
  unique.
- uq_external_identifiers_primary — partial UNIQUE
  (instrument_id, provider, identifier_type) WHERE is_primary=TRUE.

upsert_cik_mapping's ON CONFLICT targets the per-instrument CIK index;
the partial primary UNIQUE is handled by demoting any mismatching
primary row first. Tests lock the new behaviour so a future refactor
cannot reintroduce the flap (#1102) that left one share-class sibling
without 10-K / fundamentals / filings.
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
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
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


def test_same_cik_co_binds_to_two_instruments(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """#1102: share-class siblings legitimately share a CIK.

    Pre-#1102 the global UNIQUE on (provider, type, value) flapped the
    binding between siblings on every ``daily_cik_refresh`` — one
    sibling always lost its 10-K / fundamentals.

    Post-#1102 the per-instrument partial unique index allows two
    rows for the same CIK as long as ``instrument_id`` differs. Both
    instruments hold the CIK as primary; neither flaps.
    """
    conn = ebull_test_conn
    _seed_instrument(conn, instrument_id=1, symbol="GOOG")
    _seed_instrument(conn, instrument_id=2, symbol="GOOGL")

    upsert_cik_mapping(conn, {"GOOG": "0001652044"}, [("GOOG", "1")])
    upsert_cik_mapping(conn, {"GOOGL": "0001652044"}, [("GOOGL", "2")])

    # BOTH instruments hold the CIK as primary. Pre-#1102 the second
    # call would have rewritten instrument 1's row to instrument 2.
    assert _all_rows(conn, 1) == [("0001652044", True)]
    assert _all_rows(conn, 2) == [("0001652044", True)]
    assert _primary_cik(conn, 1) == "0001652044"
    assert _primary_cik(conn, 2) == "0001652044"


def test_share_class_repeat_run_is_idempotent(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """A second pass over the same panel adds zero rows; both
    instruments still hold the CIK as primary and ``last_verified_at``
    advances on the in-place UPDATE (asserted explicitly).
    """
    conn = ebull_test_conn
    _seed_instrument(conn, instrument_id=1, symbol="GOOG")
    _seed_instrument(conn, instrument_id=2, symbol="GOOGL")

    upsert_cik_mapping(conn, {"GOOG": "0001652044"}, [("GOOG", "1")])
    upsert_cik_mapping(conn, {"GOOGL": "0001652044"}, [("GOOGL", "2")])

    last_verified_before = conn.execute(
        "SELECT instrument_id, last_verified_at FROM external_identifiers "
        "WHERE provider='sec' AND identifier_type='cik' "
        "AND identifier_value='0001652044' ORDER BY instrument_id"
    ).fetchall()

    # Second pass — no new rows, just refreshes.
    upsert_cik_mapping(conn, {"GOOG": "0001652044"}, [("GOOG", "1")])
    upsert_cik_mapping(conn, {"GOOGL": "0001652044"}, [("GOOGL", "2")])

    assert _all_rows(conn, 1) == [("0001652044", True)]
    assert _all_rows(conn, 2) == [("0001652044", True)]

    last_verified_after = conn.execute(
        "SELECT instrument_id, last_verified_at FROM external_identifiers "
        "WHERE provider='sec' AND identifier_type='cik' "
        "AND identifier_value='0001652044' ORDER BY instrument_id"
    ).fetchall()
    assert len(last_verified_before) == 2
    assert len(last_verified_after) == 2
    for (iid_b, ts_b), (iid_a, ts_a) in zip(last_verified_before, last_verified_after, strict=True):
        assert iid_b == iid_a
        assert ts_a >= ts_b, f"last_verified_at did not advance for iid={iid_a}: {ts_b} → {ts_a}"


def test_cik_reassignment_does_not_remove_prior_instrument_binding(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Post-#1102 the rename / reassignment scenario no longer rewrites
    the prior instrument's row. Instrument 1 keeps its CIK; instrument
    2 also acquires the CIK as primary. If the rename intent is
    genuine the prior row must be cleaned up by an explicit operator
    action (not a side effect of upsert_cik_mapping).
    """
    conn = ebull_test_conn
    _seed_instrument(conn, instrument_id=1, symbol="OLD")
    _seed_instrument(conn, instrument_id=2, symbol="NEW")

    upsert_cik_mapping(conn, {"OLD": "0000555555"}, [("OLD", "1")])
    upsert_cik_mapping(conn, {"NEW": "0000555555"}, [("NEW", "2")])

    assert _all_rows(conn, 1) == [("0000555555", True)]
    assert _all_rows(conn, 2) == [("0000555555", True)]


def test_cik_change_demotes_prior_primary_when_target_already_holds_different_cik(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Combined: instrument 2 already holds CIK 0000999999 as primary;
    incoming map says it should hold CIK 0000555555 instead. The
    demote UPDATE fires (0000999999 → is_primary=FALSE) and the
    incoming CIK is inserted as primary. Instrument 1's row is
    untouched (it still holds 0000555555 from a prior pass —
    share-class co-binding).
    """
    conn = ebull_test_conn
    _seed_instrument(conn, instrument_id=1, symbol="OLD")
    _seed_instrument(conn, instrument_id=2, symbol="NEW")

    upsert_cik_mapping(conn, {"OLD": "0000555555"}, [("OLD", "1")])
    upsert_cik_mapping(conn, {"NEW": "0000999999"}, [("NEW", "2")])
    upsert_cik_mapping(conn, {"NEW": "0000555555"}, [("NEW", "2")])

    assert _all_rows(conn, 1) == [("0000555555", True)]
    assert _primary_cik(conn, 2) == "0000555555"
    assert _all_rows(conn, 2) == [
        ("0000555555", True),
        ("0000999999", False),
    ]


def test_symbol_missing_from_mapping_is_skipped(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, instrument_id=1, symbol="AAPL")

    upserted = upsert_cik_mapping(conn, {}, [("AAPL", "1")])

    assert upserted == 0
    assert _primary_cik(conn, 1) is None
