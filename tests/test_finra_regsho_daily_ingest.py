"""Tests for ``finra_regsho_daily_ingest`` service — G6/#916.

Service-layer integration against ``ebull_test_conn``. Pins:

* Happy path against panel fixture — 5 panel rows resolved + upserted +
  manifest written + freshness seeded.
* Empty-file (FNRA shape) → success with zero rows + manifest still
  written.
* Header / footer / body-date corruption → ``HeaderCorruptionError``.
* Per-row defects (truncated, malformed-decimal, blank-symbol,
  no-match) → row skipped + per-defect counter incremented.
* Multi-prefix coexistence — CNMS aggregate + FNQC facility for same
  ``(instrument, trade_date)`` both land (PK includes
  ``market`` + ``source_document_id``).
* Revision UPSERT — re-ingest mutates row in-place.
* Service-no-commit invariant.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from app.services.finra_regsho_ingest import (
    PARSER_VERSION,
    ingest_regsho_daily_file,
)
from app.services.finra_short_interest_ingest import (
    HeaderCorruptionError,
    build_preloaded_symbol_resolver,
)

_PANEL = Path("tests/fixtures/finra/regsho/CNMS_panel_20260515.txt")
_EMPTY_FNRA = Path("tests/fixtures/finra/regsho/FNRA_empty_20260515.txt")
_ROW_DEFECTS = Path("tests/fixtures/finra/regsho/CNMS_row_defects_20260515.txt")
_HEADER_CORRUPT = Path("tests/fixtures/finra/regsho/CNMS_header_corrupt_20260515.txt")
_FOOTER_MISMATCH = Path("tests/fixtures/finra/regsho/CNMS_footer_mismatch_20260515.txt")
_BODY_DATE_MISMATCH = Path("tests/fixtures/finra/regsho/CNMS_body_date_mismatch_20260515.txt")

_TRADE_DATE = date(2026, 5, 15)


# ----------------------------------------------------------------------
# Seed helpers
# ----------------------------------------------------------------------


def _seed_instrument(conn: psycopg.Connection[tuple], *, instrument_id: int, symbol: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
            "VALUES (%s, %s, %s, TRUE) ON CONFLICT (instrument_id) DO NOTHING",
            (instrument_id, symbol, symbol),
        )


def _seed_panel(conn: psycopg.Connection[tuple]) -> None:
    _seed_instrument(conn, instrument_id=1001, symbol="AAPL")
    _seed_instrument(conn, instrument_id=1002, symbol="GME")
    _seed_instrument(conn, instrument_id=1003, symbol="MSFT")
    _seed_instrument(conn, instrument_id=1004, symbol="JPM")
    _seed_instrument(conn, instrument_id=1005, symbol="HD")


# ----------------------------------------------------------------------
# 1 — Happy path (CNMS panel fixture)
# ----------------------------------------------------------------------


def test_happy_path_cnms_panel(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _seed_panel(ebull_test_conn)
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    raw = _PANEL.read_bytes()
    ingest_run_id = uuid4()

    with ebull_test_conn.transaction():
        stats = ingest_regsho_daily_file(
            ebull_test_conn,
            _TRADE_DATE,
            "CNMS",
            raw,
            resolver,
            ingest_run_id,
        )

    assert stats.rows_parsed == 5
    assert stats.rows_resolved == 5
    assert stats.rows_upserted == 5
    assert stats.skipped_invalid_row == 0
    assert stats.skipped_no_instrument_match == 0
    assert stats.skipped_ambiguous_symbol == 0

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id, market, source_document_id,
                   short_volume, short_exempt_volume, total_volume
            FROM finra_regsho_daily_observations
            WHERE trade_date = %s
            ORDER BY instrument_id
            """,
            (_TRADE_DATE,),
        )
        rows = cur.fetchall()
        assert len(rows) == 5
        assert [r[0] for r in rows] == [1001, 1002, 1003, 1004, 1005]
        for _, market, sdid, *_volumes in rows:
            assert market == "B,Q,N"
            assert sdid == "CNMS_20260515"

        cur.execute(
            "SELECT ingest_status, parser_version, raw_status, source, subject_id "
            "FROM sec_filing_manifest WHERE accession_number = %s",
            ("FINRA_REGSHO_CNMS_20260515",),
        )
        manifest = cur.fetchone()
        assert manifest is not None
        ingest_status, parser_version, raw_status, source, subject_id = manifest
        assert ingest_status == "parsed"
        assert parser_version == PARSER_VERSION == "finra-regsho-daily-v1"
        assert raw_status == "stored"
        assert source == "finra_regsho_daily"
        assert subject_id == "FINRA_REGSHO"

        cur.execute(
            "SELECT subject_type, subject_id, source FROM data_freshness_index WHERE subject_id = %s AND source = %s",
            ("FINRA_REGSHO", "finra_regsho_daily"),
        )
        freshness = cur.fetchone()
        assert freshness == ("finra_universe", "FINRA_REGSHO", "finra_regsho_daily")


# ----------------------------------------------------------------------
# 2 — Empty file (FNRA legitimate-empty shape)
# ----------------------------------------------------------------------


def test_empty_file_fnra(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _seed_panel(ebull_test_conn)
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    raw = _EMPTY_FNRA.read_bytes()

    with ebull_test_conn.transaction():
        stats = ingest_regsho_daily_file(
            ebull_test_conn,
            _TRADE_DATE,
            "FNRA",
            raw,
            resolver,
            uuid4(),
        )

    assert stats.rows_parsed == 0
    assert stats.rows_upserted == 0
    assert stats.failed is False

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT ingest_status FROM sec_filing_manifest WHERE accession_number = %s",
            ("FINRA_REGSHO_FNRA_20260515",),
        )
        row = cur.fetchone()
        assert row is not None and row[0] == "parsed"


# ----------------------------------------------------------------------
# 3 — Header / footer / body-date fatal raises
# ----------------------------------------------------------------------


def test_header_corruption_raises() -> None:
    """Header column re-ordered → HeaderCorruptionError; no cursor()."""
    raw = _HEADER_CORRUPT.read_bytes()

    class _NeverUsed:
        def cursor(self) -> object:
            raise AssertionError("service must raise before cursor() on header corruption")

    with pytest.raises(HeaderCorruptionError, match="header mismatch"):
        ingest_regsho_daily_file(
            _NeverUsed(),  # type: ignore[arg-type]
            _TRADE_DATE,
            "CNMS",
            raw,
            lambda _s: None,
            uuid4(),
        )


def test_footer_mismatch_raises(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Footer says 5, body has 2 rows → HeaderCorruptionError raised
    AFTER the body loop (so any successful row upserts inside the same
    txn are rolled back atomically by the caller).
    """
    _seed_panel(ebull_test_conn)
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    raw = _FOOTER_MISMATCH.read_bytes()

    with pytest.raises(HeaderCorruptionError, match="footer-count mismatch"):
        with ebull_test_conn.transaction():
            ingest_regsho_daily_file(
                ebull_test_conn,
                _TRADE_DATE,
                "CNMS",
                raw,
                resolver,
                uuid4(),
            )

    # Rollback semantics: no observations land for the failed file.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM finra_regsho_daily_observations WHERE trade_date = %s",
            (_TRADE_DATE,),
        )
        assert cur.fetchone() == (0,)


def test_body_date_mismatch_raises(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Body row Date=20260516, caller passes 2026-05-15 → fatal raise."""
    _seed_panel(ebull_test_conn)
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    raw = _BODY_DATE_MISMATCH.read_bytes()

    with pytest.raises(HeaderCorruptionError, match="body-date mismatch"):
        with ebull_test_conn.transaction():
            ingest_regsho_daily_file(
                ebull_test_conn,
                _TRADE_DATE,
                "CNMS",
                raw,
                resolver,
                uuid4(),
            )


# ----------------------------------------------------------------------
# 4 — Per-row defect counters
# ----------------------------------------------------------------------


def test_row_defects_counters(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """row_defects fixture: 1 happy AAPL + 1 truncated GME +
    1 malformed-decimal MSFT + 1 blank-Symbol + 1 unknown ZZZZUNK.
    Footer matches body count (5) so the file is structurally valid.
    """
    _seed_panel(ebull_test_conn)
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    raw = _ROW_DEFECTS.read_bytes()

    with ebull_test_conn.transaction():
        stats = ingest_regsho_daily_file(
            ebull_test_conn,
            _TRADE_DATE,
            "CNMS",
            raw,
            resolver,
            uuid4(),
        )

    assert stats.rows_parsed == 5
    assert stats.rows_upserted == 1  # AAPL only
    assert stats.rows_resolved == 1
    # truncated row + malformed-decimal row + blank-Symbol row = 3 invalid.
    assert stats.skipped_invalid_row == 3
    # ZZZZUNK = no instrument match.
    assert stats.skipped_no_instrument_match == 1


def test_ambiguous_symbol_counter(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Two instruments collapse to the same normalised key →
    skipped_ambiguous_symbol incremented for any FINRA row with that key.
    """
    # Seed colliding instruments — both normalise to 'AAPL'.
    _seed_instrument(ebull_test_conn, instrument_id=9001, symbol="AAPL")
    _seed_instrument(ebull_test_conn, instrument_id=9002, symbol="A.APL")
    # No other panel — every other panel symbol will go skipped_no_match.
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    raw = _PANEL.read_bytes()

    with ebull_test_conn.transaction():
        stats = ingest_regsho_daily_file(
            ebull_test_conn,
            _TRADE_DATE,
            "CNMS",
            raw,
            resolver,
            uuid4(),
        )

    assert stats.rows_parsed == 5
    assert stats.skipped_ambiguous_symbol == 1  # AAPL row hits the collision.
    assert stats.skipped_no_instrument_match == 4  # GME/MSFT/JPM/HD un-seeded.
    assert stats.rows_upserted == 0


# ----------------------------------------------------------------------
# 5 — Multi-prefix coexistence (CNMS + FNQC for same instrument/date)
# ----------------------------------------------------------------------


def test_multi_prefix_coexistence(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """CNMS aggregate row + FNQC facility row for same (instrument, date)
    both land. PK includes ``market`` + ``source_document_id`` so the
    two facts coexist.
    """
    _seed_panel(ebull_test_conn)
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    cnms = _PANEL.read_bytes()
    # Synthesise an FNQC fixture by replacing the Market column.
    fnqc = cnms.replace(b"B,Q,N", b"B")
    ingest_run_id = uuid4()

    with ebull_test_conn.transaction():
        ingest_regsho_daily_file(ebull_test_conn, _TRADE_DATE, "CNMS", cnms, resolver, ingest_run_id)
    with ebull_test_conn.transaction():
        ingest_regsho_daily_file(ebull_test_conn, _TRADE_DATE, "FNQC", fnqc, resolver, ingest_run_id)

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT market, source_document_id FROM finra_regsho_daily_observations "
            "WHERE instrument_id = %s AND trade_date = %s ORDER BY source_document_id",
            (1001, _TRADE_DATE),
        )
        rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0] == ("B,Q,N", "CNMS_20260515")
        assert rows[1] == ("B", "FNQC_20260515")


# ----------------------------------------------------------------------
# 6 — Revision UPSERT (re-ingest mutates row)
# ----------------------------------------------------------------------


def test_revision_upsert(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _seed_panel(ebull_test_conn)
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    cnms = _PANEL.read_bytes()
    revised = cnms.replace(b"8714049.111124", b"9999999.999999")

    with ebull_test_conn.transaction():
        ingest_regsho_daily_file(ebull_test_conn, _TRADE_DATE, "CNMS", cnms, resolver, uuid4())
    with ebull_test_conn.transaction():
        ingest_regsho_daily_file(ebull_test_conn, _TRADE_DATE, "CNMS", revised, resolver, uuid4())

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT short_volume FROM finra_regsho_daily_observations "
            "WHERE instrument_id = %s AND trade_date = %s AND market = %s AND source_document_id = %s",
            (1001, _TRADE_DATE, "B,Q,N", "CNMS_20260515"),
        )
        row = cur.fetchone()
        assert row is not None
        # Compare as string to avoid Decimal precision shenanigans.
        assert str(row[0]) == "9999999.999999"


# ----------------------------------------------------------------------
# 7 — Service-no-commit invariant
# ----------------------------------------------------------------------


def test_ingest_does_not_commit_or_rollback(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Service body MUST NOT call ``conn.commit`` / ``conn.rollback``."""
    _seed_panel(ebull_test_conn)
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    raw = _PANEL.read_bytes()

    commit_calls = 0
    rollback_calls = 0
    orig_commit = ebull_test_conn.commit
    orig_rollback = ebull_test_conn.rollback

    def _spy_commit() -> None:
        nonlocal commit_calls
        commit_calls += 1
        orig_commit()

    def _spy_rollback() -> None:
        nonlocal rollback_calls
        rollback_calls += 1
        orig_rollback()

    ebull_test_conn.commit = _spy_commit  # type: ignore[method-assign]
    ebull_test_conn.rollback = _spy_rollback  # type: ignore[method-assign]

    try:
        with ebull_test_conn.transaction():
            ingest_regsho_daily_file(
                ebull_test_conn,
                _TRADE_DATE,
                "CNMS",
                raw,
                resolver,
                uuid4(),
            )
    finally:
        ebull_test_conn.commit = orig_commit  # type: ignore[method-assign]
        ebull_test_conn.rollback = orig_rollback  # type: ignore[method-assign]

    assert commit_calls == 0
    assert rollback_calls == 0
