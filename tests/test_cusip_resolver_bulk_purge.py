"""Tests for the #1349 bulk-partition hygiene paths.

* `purge_unresolved_bulk_rows_outside_retention` — deletes per-(cusip,
  source) rows whose latest sighting (`last_period_end`) fell behind the
  per-source retention floor. In-window rows (pending OR tombstoned) and
  the legacy partition are kept.
* `sweep_bulk_cusips_resolved_via_extid` — tombstones pending bulk rows
  whose CUSIP is now mapped in `external_identifiers` (any route).
  Replaces the #1399 inline delete.
"""

from __future__ import annotations

from datetime import date

import psycopg

from app.services.cusip_resolver import (
    purge_unresolved_bulk_rows_outside_retention,
    sweep_bulk_cusips_resolved_via_extid,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

_CUTOFF = date(2024, 1, 1)
_OLD = date(2022, 6, 30)  # < cutoff → purgeable
_RECENT = date(2024, 6, 30)  # >= cutoff → kept


def _seed_bulk(
    conn: psycopg.Connection[tuple],
    *,
    cusip: str,
    source: str,
    last_period_end: date,
    resolution_status: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO unresolved_13f_cusips
            (cusip, source, observation_count, first_period_end, last_period_end, resolution_status)
        VALUES (%s, %s, 1, %s, %s, %s)
        """,
        (cusip, source, last_period_end, last_period_end, resolution_status),
    )


def _seed_legacy(conn: psycopg.Connection[tuple], *, cusip: str) -> None:
    # Legacy partition: source IS NULL; no period dimension.
    conn.execute(
        "INSERT INTO unresolved_13f_cusips (cusip, name_of_issuer, last_accession_number) VALUES (%s, 'Co', '0-0')",
        (cusip,),
    )


def _surviving(conn: psycopg.Connection[tuple]) -> set[tuple[str, str | None]]:
    rows = conn.execute("SELECT cusip, source FROM unresolved_13f_cusips").fetchall()
    return {(r[0], r[1]) for r in rows}


def test_purge_deletes_only_out_of_retention_bulk_rows(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_bulk(conn, cusip="111111111", source="bulk_13f_dataset", last_period_end=_OLD)  # → deleted
    _seed_bulk(conn, cusip="222222222", source="bulk_13f_dataset", last_period_end=_RECENT)  # → kept
    _seed_bulk(
        conn,
        cusip="333333333",
        source="bulk_13f_dataset",
        last_period_end=_OLD,
        resolution_status="unresolvable",
    )  # < cutoff → deleted (only the period drives the purge, not status)
    _seed_bulk(
        conn,
        cusip="555555555",
        source="bulk_13f_dataset",
        last_period_end=_RECENT,
        resolution_status="resolved_via_extid",
    )  # tombstoned-but-in-window → kept (ages out later)
    _seed_legacy(conn, cusip="444444444")  # legacy → kept (source-scoped)
    conn.commit()

    deleted = purge_unresolved_bulk_rows_outside_retention(conn, source="bulk_13f_dataset", cutoff=_CUTOFF)
    conn.commit()

    assert deleted == 2
    assert _surviving(conn) == {
        ("222222222", "bulk_13f_dataset"),  # in-window pending kept
        ("555555555", "bulk_13f_dataset"),  # in-window tombstone kept
        ("444444444", None),  # legacy kept
    }


def test_purge_is_source_scoped(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A 13F-source purge must not touch N-PORT-source rows (distinct floors)."""
    conn = ebull_test_conn
    _seed_bulk(conn, cusip="555555551", source="bulk_13f_dataset", last_period_end=_OLD)
    _seed_bulk(conn, cusip="666666666", source="bulk_nport_dataset", last_period_end=_OLD)
    conn.commit()

    deleted = purge_unresolved_bulk_rows_outside_retention(conn, source="bulk_13f_dataset", cutoff=_CUTOFF)
    conn.commit()

    assert deleted == 1
    assert _surviving(conn) == {("666666666", "bulk_nport_dataset")}  # N-PORT untouched

    deleted_nport = purge_unresolved_bulk_rows_outside_retention(conn, source="bulk_nport_dataset", cutoff=_CUTOFF)
    conn.commit()
    assert deleted_nport == 1
    assert _surviving(conn) == set()


def _seed_mapping(conn: psycopg.Connection[tuple], *, instrument_id: int, symbol: str, cusip: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', TRUE)",
            (instrument_id, symbol, f"{symbol} Inc"),
        )
        cur.execute(
            "INSERT INTO external_identifiers "
            "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cusip', %s, TRUE)",
            (instrument_id, cusip),
        )


def test_extid_sweep_tombstones_mapped_bulk_rows_only(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Mapped bulk cusip → `resolved_via_extid`; unmapped stays pending;
    legacy partition untouched (it has its own extid sweep with rewash
    semantics)."""
    conn = ebull_test_conn
    _seed_mapping(conn, instrument_id=91001, symbol="MAPD", cusip="MAPPED0001")
    _seed_bulk(conn, cusip="MAPPED0001", source="bulk_13f_dataset", last_period_end=_RECENT)
    _seed_bulk(conn, cusip="MAPPED0001", source="bulk_nport_dataset", last_period_end=_RECENT)
    _seed_bulk(conn, cusip="PENDING001", source="bulk_13f_dataset", last_period_end=_RECENT)
    _seed_legacy(conn, cusip="MAPPED0001")  # same cusip, legacy partition
    conn.commit()

    tombstoned = sweep_bulk_cusips_resolved_via_extid(conn)
    conn.commit()

    assert tombstoned == 2  # both sources for the mapped cusip
    rows = conn.execute(
        "SELECT source, resolution_status FROM unresolved_13f_cusips "
        "WHERE cusip = 'MAPPED0001' ORDER BY source NULLS FIRST"
    ).fetchall()
    assert rows == [
        (None, None),  # legacy untouched
        ("bulk_13f_dataset", "resolved_via_extid"),
        ("bulk_nport_dataset", "resolved_via_extid"),
    ]
    pending = conn.execute("SELECT resolution_status FROM unresolved_13f_cusips WHERE cusip = 'PENDING001'").fetchall()
    assert pending == [(None,)]


def test_extid_sweep_is_idempotent_and_skips_already_tombstoned(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_mapping(conn, instrument_id=91002, symbol="IDM2", cusip="IDEMP00001")
    _seed_bulk(
        conn,
        cusip="IDEMP00001",
        source="bulk_13f_dataset",
        last_period_end=_RECENT,
        resolution_status="resolved_via_openfigi",  # already tombstoned by the OpenFIGI sweep
    )
    conn.commit()

    first = sweep_bulk_cusips_resolved_via_extid(conn)
    conn.commit()
    assert first == 0  # resolution_status IS NULL filter — never overwrites

    row = conn.execute("SELECT resolution_status FROM unresolved_13f_cusips WHERE cusip = 'IDEMP00001'").fetchone()
    assert row == ("resolved_via_openfigi",)
