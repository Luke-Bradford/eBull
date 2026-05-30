"""Tests for `purge_unresolved_bulk_rows_outside_retention` (#1349 PR1).

The bulk partition of `unresolved_13f_cusips` grew unbounded because nothing
deleted aged rows. The purge drains the provably-dead subset — bulk rows
whose `period_end` is outside the per-source retention floor (periods no
pipeline will re-materialise). In-retention rows and the legacy partition are
kept. Period-based predicate is the only grain-safe cleanup (spec §2a).
"""

from __future__ import annotations

from datetime import date

import psycopg

from app.services.cusip_resolver import purge_unresolved_bulk_rows_outside_retention
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

_CUTOFF = date(2024, 1, 1)
_OLD = date(2022, 6, 30)  # < cutoff → purgeable
_RECENT = date(2024, 6, 30)  # >= cutoff → kept


def _seed_bulk(
    conn: psycopg.Connection[tuple],
    *,
    cusip: str,
    source: str,
    period_end: date,
    filer_cik: str = "0000111111",
    resolution_status: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO unresolved_13f_cusips (cusip, source, period_end, filer_cik, resolution_status)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (cusip, source, period_end, filer_cik, resolution_status),
    )


def _seed_legacy(conn: psycopg.Connection[tuple], *, cusip: str, period_end: date) -> None:
    # Legacy partition: source IS NULL.
    conn.execute(
        "INSERT INTO unresolved_13f_cusips (cusip, source, period_end) VALUES (%s, NULL, %s)",
        (cusip, period_end),
    )


def _surviving(conn: psycopg.Connection[tuple]) -> set[tuple[str, str | None]]:
    rows = conn.execute("SELECT cusip, source FROM unresolved_13f_cusips").fetchall()
    return {(r[0], r[1]) for r in rows}


def test_purge_deletes_only_out_of_retention_bulk_rows(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_bulk(conn, cusip="111111111", source="bulk_13f_dataset", period_end=_OLD)  # → deleted
    _seed_bulk(conn, cusip="222222222", source="bulk_13f_dataset", period_end=_RECENT)  # → kept
    _seed_bulk(
        conn,
        cusip="333333333",
        source="bulk_13f_dataset",
        period_end=_OLD,
        resolution_status="unresolvable",
    )  # < cutoff → deleted (only period drives the purge, not status)
    _seed_legacy(conn, cusip="444444444", period_end=_OLD)  # legacy → kept (source-scoped)
    conn.commit()

    deleted = purge_unresolved_bulk_rows_outside_retention(conn, source="bulk_13f_dataset", cutoff=_CUTOFF)
    conn.commit()

    assert deleted == 2
    assert _surviving(conn) == {
        ("222222222", "bulk_13f_dataset"),  # in-window kept
        ("444444444", None),  # legacy kept
    }


def test_purge_is_source_scoped(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A 13F-source purge must not touch N-PORT-source rows (distinct floors)."""
    conn = ebull_test_conn
    _seed_bulk(conn, cusip="555555555", source="bulk_13f_dataset", period_end=_OLD)
    _seed_bulk(conn, cusip="666666666", source="bulk_nport_dataset", period_end=_OLD)
    conn.commit()

    deleted = purge_unresolved_bulk_rows_outside_retention(conn, source="bulk_13f_dataset", cutoff=_CUTOFF)
    conn.commit()

    assert deleted == 1
    assert _surviving(conn) == {("666666666", "bulk_nport_dataset")}  # N-PORT untouched

    deleted_nport = purge_unresolved_bulk_rows_outside_retention(conn, source="bulk_nport_dataset", cutoff=_CUTOFF)
    conn.commit()
    assert deleted_nport == 1
    assert _surviving(conn) == set()


def test_ctid_cap_bounds_physical_rows_and_loop_drains(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The per-pass cap bounds *physical rows* (one high-fanout CUSIP cannot
    blow it); looping drains the rest."""
    conn = ebull_test_conn
    # 5 out-of-window rows for ONE high-fanout CUSIP (distinct filer_cik so
    # the bulk unique index admits them).
    for i in range(5):
        _seed_bulk(
            conn,
            cusip="777777777",
            source="bulk_13f_dataset",
            period_end=_OLD,
            filer_cik=f"00000{i:05d}",
        )
    conn.commit()

    first = purge_unresolved_bulk_rows_outside_retention(conn, source="bulk_13f_dataset", cutoff=_CUTOFF, limit=2)
    conn.commit()
    assert first == 2  # capped at physical-row limit, not the 1 distinct CUSIP

    total = first
    for _ in range(10):
        n = purge_unresolved_bulk_rows_outside_retention(conn, source="bulk_13f_dataset", cutoff=_CUTOFF, limit=2)
        conn.commit()
        total += n
        if n == 0:
            break
    assert total == 5
    assert _surviving(conn) == set()
