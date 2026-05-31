"""Tests for `delete_resolved_bulk_markers` + `in_window_bulk_markers_exist`
(#1399, PR2 of #1349).

When a bulk ingest materialises an observation for a `(cusip, filer_cik,
period_end)` an earlier run recorded as unresolved, the marker row is
redundant. The inline delete removes the EXACT matching bulk row at the
precise grain `(source, cusip, filer_cik, period_end)` — the only safe
shape, because the coarse bulk-marker key cannot be matched against the
fine-grained observation tables without false positives (spec §2a).

Synthetic-seeded; no live archive. Live DoD smoke (real archives,
before/after marker counts) is operator-run post-bootstrap.
"""

from __future__ import annotations

from datetime import date

import psycopg
import pytest

import app.services.sec_13f_dataset_ingest as f13
from app.services.cusip_resolver import (
    delete_resolved_bulk_markers,
    in_window_bulk_markers_exist,
    reconcile_survived_markers,
)
from app.services.sec_13f_dataset_ingest import Form13FIngestResult
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

_FILER = "0000111111"
_P1 = date(2024, 6, 30)
_P2 = date(2023, 3, 31)


_IID = 4242


# ── reconcile_survived_markers — pure, no DB (Codex ckpt-2 HIGH fix) ──


def test_reconcile_keeps_only_survived_obs() -> None:
    """A marker whose obs grain (instrument_id, filer, period) survived
    the COPY is deletable; one whose row was wire-skipped is NOT."""
    markers = {
        ("AAA", _FILER, _P1, _IID),  # obs survived → deletable
        ("BBB", _FILER, _P1, 9999),  # obs skipped at COPY → keep marker
    }
    survived = {(_IID, _FILER, _P1)}
    assert reconcile_survived_markers(markers, survived) == {("AAA", _FILER, _P1)}


def test_reconcile_empty_survived_deletes_nothing() -> None:
    """If the whole archive's holdings were wire-skipped, no marker is
    deleted (the false-positive class Codex flagged)."""
    markers = {("AAA", _FILER, _P1, _IID)}
    assert reconcile_survived_markers(markers, set()) == set()


def test_reconcile_matches_on_obs_grain_not_period_mismatch() -> None:
    """Survival is matched on (instrument_id, filer, period); a survived
    row for a different period does not license deleting this marker."""
    markers = {("AAA", _FILER, _P1, _IID)}
    survived = {(_IID, _FILER, _P2)}  # same instrument+filer, different period
    assert reconcile_survived_markers(markers, survived) == set()


def _seed_bulk(
    conn: psycopg.Connection[tuple],
    *,
    cusip: str,
    source: str | None,
    period_end: date,
    filer_cik: str | None = _FILER,
) -> None:
    conn.execute(
        "INSERT INTO unresolved_13f_cusips (cusip, source, period_end, filer_cik) VALUES (%s, %s, %s, %s)",
        (cusip, source, period_end, filer_cik),
    )


def _surviving(conn: psycopg.Connection[tuple]) -> set[tuple[str, str | None, date | None]]:
    rows = conn.execute("SELECT cusip, source, period_end FROM unresolved_13f_cusips").fetchall()
    return {(r[0], r[1], r[2]) for r in rows}


def test_deletes_exact_bulk_marker(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_bulk(conn, cusip="111111111", source="bulk_13f_dataset", period_end=_P1)
    conn.commit()

    deleted = delete_resolved_bulk_markers(conn, [("111111111", _FILER, _P1)], source="bulk_13f_dataset")
    conn.commit()

    assert deleted == 1
    assert _surviving(conn) == set()


def test_keeps_marker_for_different_period(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A marker whose period differs from the materialised obs is the
    stranded-period invariant (spec §2): it must survive."""
    conn = ebull_test_conn
    _seed_bulk(conn, cusip="222222222", source="bulk_13f_dataset", period_end=_P2)
    conn.commit()

    deleted = delete_resolved_bulk_markers(conn, [("222222222", _FILER, _P1)], source="bulk_13f_dataset")
    conn.commit()

    assert deleted == 0
    assert _surviving(conn) == {("222222222", "bulk_13f_dataset", _P2)}


def test_keeps_marker_for_different_source(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Same (cusip, filer, period) under a different source must survive —
    `source` equality is exact (bulk markers are always non-null source)."""
    conn = ebull_test_conn
    _seed_bulk(conn, cusip="333333333", source="bulk_nport_dataset", period_end=_P1)
    conn.commit()

    deleted = delete_resolved_bulk_markers(conn, [("333333333", _FILER, _P1)], source="bulk_13f_dataset")
    conn.commit()

    assert deleted == 0
    assert _surviving(conn) == {("333333333", "bulk_nport_dataset", _P1)}


def test_keeps_legacy_null_source_marker(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Legacy partition (source IS NULL) is owned by the legacy DELETE
    path — `u.source = 'bulk_13f_dataset'` excludes NULL, so it survives."""
    conn = ebull_test_conn
    _seed_bulk(conn, cusip="444444444", source=None, period_end=_P1)
    conn.commit()

    deleted = delete_resolved_bulk_markers(conn, [("444444444", _FILER, _P1)], source="bulk_13f_dataset")
    conn.commit()

    assert deleted == 0
    assert _surviving(conn) == {("444444444", None, _P1)}


def test_keeps_marker_for_different_filer(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_bulk(conn, cusip="555555555", source="bulk_13f_dataset", period_end=_P1, filer_cik="0000999999")
    conn.commit()

    deleted = delete_resolved_bulk_markers(conn, [("555555555", _FILER, _P1)], source="bulk_13f_dataset")
    conn.commit()

    assert deleted == 0
    assert _surviving(conn) == {("555555555", "bulk_13f_dataset", _P1)}


def test_duplicate_tuples_delete_one_row(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Many obs rows share one marker tuple (multiple exposure_kind / fund
    series). A buffer carrying duplicates deletes the single marker row
    exactly once — no double-count, no error (the caller's SET also
    dedups, but the helper must be correct on a raw duplicate iterable)."""
    conn = ebull_test_conn
    _seed_bulk(conn, cusip="666666666", source="bulk_13f_dataset", period_end=_P1)
    conn.commit()

    deleted = delete_resolved_bulk_markers(
        conn,
        [("666666666", _FILER, _P1), ("666666666", _FILER, _P1), ("666666666", _FILER, _P1)],
        source="bulk_13f_dataset",
    )
    conn.commit()

    assert deleted == 1
    assert _surviving(conn) == set()


def test_empty_buffer_returns_zero(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_bulk(conn, cusip="777777777", source="bulk_13f_dataset", period_end=_P1)
    conn.commit()

    deleted = delete_resolved_bulk_markers(conn, [], source="bulk_13f_dataset")
    conn.commit()

    assert deleted == 0
    assert _surviving(conn) == {("777777777", "bulk_13f_dataset", _P1)}


def test_filters_malformed_triples(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Empty cusip/filer or NULL period are skipped in the COPY pass; the
    one valid triple still deletes its marker."""
    conn = ebull_test_conn
    _seed_bulk(conn, cusip="888888888", source="bulk_13f_dataset", period_end=_P1)
    conn.commit()

    deleted = delete_resolved_bulk_markers(
        conn,
        [
            ("", _FILER, _P1),  # empty cusip → skipped
            ("888888888", "", _P1),  # empty filer → skipped
            ("888888888", _FILER, None),  # NULL period → skipped  # type: ignore[list-item]
            ("888888888", _FILER, _P1),  # valid → deletes
        ],
        source="bulk_13f_dataset",
    )
    conn.commit()

    assert deleted == 1
    assert _surviving(conn) == set()


def test_cusip_normalised_before_match(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The helper upper-cases + strips cusip/filer, mirroring the bulk
    writer, so a lowercase/padded buffer value still matches the stored
    upper-cased marker."""
    conn = ebull_test_conn
    _seed_bulk(conn, cusip="ABCDE1234", source="bulk_13f_dataset", period_end=_P1)
    conn.commit()

    deleted = delete_resolved_bulk_markers(conn, [("abcde1234", f"  {_FILER}  ", _P1)], source="bulk_13f_dataset")
    conn.commit()

    assert deleted == 1
    assert _surviving(conn) == set()


def test_in_window_exists_gate(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Preflight: True only when an in-window (`period_end >= cutoff`)
    bulk marker exists for the source."""
    conn = ebull_test_conn
    cutoff = date(2024, 1, 1)

    # No rows at all → False.
    conn.commit()
    assert in_window_bulk_markers_exist(conn, "bulk_13f_dataset", cutoff) is False

    # Out-of-window only → False.
    _seed_bulk(conn, cusip="999999999", source="bulk_13f_dataset", period_end=date(2022, 6, 30))
    conn.commit()
    assert in_window_bulk_markers_exist(conn, "bulk_13f_dataset", cutoff) is False

    # In-window for a DIFFERENT source → still False for 13F.
    _seed_bulk(conn, cusip="101010101", source="bulk_nport_dataset", period_end=date(2024, 6, 30))
    conn.commit()
    assert in_window_bulk_markers_exist(conn, "bulk_13f_dataset", cutoff) is False

    # In-window 13F marker → True.
    _seed_bulk(conn, cusip="121212121", source="bulk_13f_dataset", period_end=date(2024, 6, 30))
    conn.commit()
    assert in_window_bulk_markers_exist(conn, "bulk_13f_dataset", cutoff) is True


def test_delete_failure_is_savepoint_isolated(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ingester wrapper isolates a delete failure to a savepoint:
    `parse_errors` increments, the count stays 0, and the outer archive
    tx survives (the marker write is NOT lost). Mirrors the proven
    `_flush_unresolved_buffer` failure contract."""
    conn = ebull_test_conn
    _seed_bulk(conn, cusip="131313131", source="bulk_13f_dataset", period_end=_P1)
    conn.commit()

    def _boom(*_args: object, **_kwargs: object) -> int:
        raise RuntimeError("simulated delete failure")

    monkeypatch.setattr(f13, "delete_resolved_bulk_markers", _boom)

    result = Form13FIngestResult()
    f13._delete_resolved_markers(conn, {("131313131", _FILER, _P1)}, result=result)

    assert result.parse_errors == 1
    assert result.resolved_markers_deleted == 0
    # Outer tx survived — the connection is usable and the marker remains.
    assert _surviving(conn) == {("131313131", "bulk_13f_dataset", _P1)}
