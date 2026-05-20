"""Tests for ``finra_short_interest_ingest`` service — G6/#915.

Service-layer integration against ``ebull_test_conn``. Pins:

* ``normalise_symbol`` shape (BRK.A → BRKA, lowercase → upper, no-sep
  idempotent).
* ``build_preloaded_symbol_resolver`` happy path + None on no-match
  + ambiguous-key tracking.
* ``ingest_settlement_file`` SQL-only contract (NEVER commits the
  caller-supplied conn).
* Header corruption → ``HeaderCorruptionError`` raised.
* Per-row defects (truncated, malformed-int, blank-symbol, no-match,
  ambiguous) → row skipped + per-defect counter incremented.
* _current settlement-date-wins-most-recent semantics + same-date
  revision UPSERT semantics.
* Manifest UPSERT shape (synth FINRA tuple, parser_version unified).

Spec: docs/superpowers/specs/2026-05-18-finra-bimonthly-short-interest.md.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from app.services.finra_short_interest_ingest import (
    PARSER_VERSION,
    HeaderCorruptionError,
    build_preloaded_symbol_resolver,
    ingest_settlement_file,
    normalise_symbol,
)

_PRISTINE = Path("tests/fixtures/finra/shrt20260430_sample.csv")
_DEFECTS = Path("tests/fixtures/finra/shrt20260430_defects.csv")


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
    """Smoke panel: AAPL=1001, GME=1002, MSFT=1003, JPM=1004, HD=1005."""
    _seed_instrument(conn, instrument_id=1001, symbol="AAPL")
    _seed_instrument(conn, instrument_id=1002, symbol="GME")
    _seed_instrument(conn, instrument_id=1003, symbol="MSFT")
    _seed_instrument(conn, instrument_id=1004, symbol="JPM")
    _seed_instrument(conn, instrument_id=1005, symbol="HD")


# ----------------------------------------------------------------------
# 1 — normalise_symbol
# ----------------------------------------------------------------------


def test_normalise_symbol_brk_dot() -> None:
    assert normalise_symbol("BRK.A") == "BRKA"


def test_normalise_symbol_lowercase_uppered() -> None:
    assert normalise_symbol("goog") == "GOOG"


def test_normalise_symbol_idempotent_no_separator() -> None:
    assert normalise_symbol("ABRPRD") == "ABRPRD"


def test_normalise_symbol_strips_hyphen_underscore() -> None:
    assert normalise_symbol("BRK-A") == "BRKA"
    assert normalise_symbol("BRK_A") == "BRKA"


def test_normalise_symbol_all_separators() -> None:
    assert normalise_symbol(" b.r.k - a _ ") == "BRKA"


# ----------------------------------------------------------------------
# 2 — build_preloaded_symbol_resolver
# ----------------------------------------------------------------------


def test_resolver_resolves_panel(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _seed_panel(ebull_test_conn)
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    assert resolver("AAPL") == 1001
    assert resolver("GME") == 1002
    assert resolver("MSFT") == 1003
    assert resolver("JPM") == 1004
    assert resolver("HD") == 1005


def test_resolver_normalises_input(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """`BRK.A` in DB resolves a `BRKA`-shaped FINRA symbol."""
    _seed_instrument(ebull_test_conn, instrument_id=2001, symbol="BRK.A")
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    assert resolver("BRKA") == 2001
    assert resolver("brk.a") == 2001  # lowercase normalised too


def test_resolver_returns_none_on_no_match(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_panel(ebull_test_conn)
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    assert resolver("NOTREAL") is None


def test_resolver_marks_ambiguous_via_attr(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Two instruments whose symbols collapse to the same normalised
    key should resolve to None + appear in ``ambiguous_keys``.
    """
    _seed_instrument(ebull_test_conn, instrument_id=3001, symbol="ABR.PRD")
    _seed_instrument(ebull_test_conn, instrument_id=3002, symbol="ABRPRD")
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    assert resolver("ABRPRD") is None
    assert "ABRPRD" in resolver.ambiguous_keys  # type: ignore[attr-defined]


# ----------------------------------------------------------------------
# 3 — ingest_settlement_file happy path (pristine fixture)
# ----------------------------------------------------------------------


def _read_pristine() -> bytes:
    return _PRISTINE.read_bytes()


def test_ingest_happy_path_panel_writes_observations(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_panel(ebull_test_conn)
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    raw = _read_pristine()
    ingest_run_id = uuid4()

    with ebull_test_conn.transaction():
        stats = ingest_settlement_file(
            ebull_test_conn,
            date(2026, 4, 30),
            raw,
            resolver,
            ingest_run_id,
        )

    # 9 pristine rows: 5 in-universe (AAPL/GME/MSFT/JPM/HD)
    # + 4 not-in-universe (ABRPRD/ABRPRE/ALLPRB/ANCTF).
    assert stats.rows_parsed == 9
    assert stats.rows_resolved == 5
    assert stats.rows_upserted == 5
    assert stats.skipped_no_instrument_match == 4
    assert stats.skipped_ambiguous_symbol == 0
    assert stats.skipped_invalid_row == 0

    # Verify observations rows landed for each panel symbol.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id, current_short_interest, days_to_cover
            FROM finra_short_interest_observations
            WHERE settlement_date = %s
            ORDER BY instrument_id
            """,
            (date(2026, 4, 30),),
        )
        rows = cur.fetchall()
        assert len(rows) == 5
        instrument_ids = [r[0] for r in rows]
        assert instrument_ids == [1001, 1002, 1003, 1004, 1005]

        # Verify _current snapshot matches.
        cur.execute(
            "SELECT instrument_id, settlement_date FROM finra_short_interest_current "
            "WHERE instrument_id = ANY(%s) ORDER BY instrument_id",
            ([1001, 1002, 1003, 1004, 1005],),
        )
        current_rows = cur.fetchall()
        assert len(current_rows) == 5
        for _, settlement in current_rows:
            assert settlement == date(2026, 4, 30)

        # Verify manifest row UPSERTed with unified parser_version.
        cur.execute(
            "SELECT ingest_status, parser_version, raw_status FROM sec_filing_manifest WHERE accession_number = %s",
            ("FINRA_SI_20260430",),
        )
        manifest = cur.fetchone()
        assert manifest is not None
        ingest_status, parser_version, raw_status = manifest
        assert ingest_status == "parsed"
        assert parser_version == PARSER_VERSION == "finra-si-bimonthly-v1"
        assert raw_status == "stored"


# ----------------------------------------------------------------------
# 4 — Per-row defect skips (defects fixture)
# ----------------------------------------------------------------------


def test_ingest_skips_defect_rows(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """defects fixture: 1 ambiguous-collapse + 1 truncated + 1 no-match
    + 1 blank-symbol + 1 malformed-int = 5 rows; all skipped.
    """
    # Seed two instruments that collide on the COLLISION normalised key
    # so the ambiguous-collapse row hits skipped_ambiguous_symbol.
    _seed_instrument(ebull_test_conn, instrument_id=4001, symbol="COLLISION")
    _seed_instrument(ebull_test_conn, instrument_id=4002, symbol="COLLI.SION")
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    raw = _DEFECTS.read_bytes()
    ingest_run_id = uuid4()

    with ebull_test_conn.transaction():
        stats = ingest_settlement_file(
            ebull_test_conn,
            date(2026, 4, 30),
            raw,
            resolver,
            ingest_run_id,
        )

    assert stats.rows_parsed == 5
    assert stats.rows_resolved == 0
    assert stats.rows_upserted == 0
    assert stats.skipped_ambiguous_symbol == 1
    assert stats.skipped_no_instrument_match == 1
    # blank-symbol + truncated + malformed-int all bucket as invalid.
    assert stats.skipped_invalid_row == 3


# ----------------------------------------------------------------------
# 5 — Header corruption raises HeaderCorruptionError
# ----------------------------------------------------------------------


def test_ingest_header_corruption_raises() -> None:
    """Mangled header → HeaderCorruptionError; no observations land.

    Service is SQL-only — even though we DON'T wrap in transaction here,
    the early-raise path never executes any cursor.execute. Using a
    fresh in-memory test (no DB writes attempted) is sufficient.
    """
    raw = b"WRONG|HEADER|SHAPE\nx|y|z\n"
    # No real conn needed because header validation raises BEFORE any
    # cursor() call; pass a sentinel object that would fail loudly if
    # touched.

    class _NeverUsed:
        def cursor(self) -> object:
            raise AssertionError("service must raise before cursor() on header corruption")

    with pytest.raises(HeaderCorruptionError, match="header mismatch"):
        ingest_settlement_file(
            _NeverUsed(),  # type: ignore[arg-type]
            date(2026, 4, 30),
            raw,
            lambda _s: None,  # resolver — never called
            uuid4(),
        )


def test_ingest_blank_file_raises_header_corruption() -> None:
    """Empty/blank file: csv.DictReader returns fieldnames=None →
    HeaderCorruptionError.
    """

    class _NeverUsed:
        def cursor(self) -> object:
            raise AssertionError("service must raise before cursor() on blank")

    with pytest.raises(HeaderCorruptionError):
        ingest_settlement_file(
            _NeverUsed(),  # type: ignore[arg-type]
            date(2026, 4, 30),
            b"",
            lambda _s: None,
            uuid4(),
        )


# ----------------------------------------------------------------------
# 6 — Service-no-commit invariant
# ----------------------------------------------------------------------


def test_ingest_does_not_commit_or_rollback(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Service body MUST NOT call ``conn.commit`` / ``conn.rollback``.

    Spy on the conn methods; assert NEITHER fires across a happy-path
    ingest. JOB body (NOT the service) is the only commit/rollback
    caller per the prevention-log "service-no-commit" invariant.
    """
    _seed_panel(ebull_test_conn)
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    raw = _read_pristine()
    ingest_run_id = uuid4()

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
            ingest_settlement_file(
                ebull_test_conn,
                date(2026, 4, 30),
                raw,
                resolver,
                ingest_run_id,
            )
    finally:
        ebull_test_conn.commit = orig_commit  # type: ignore[method-assign]
        ebull_test_conn.rollback = orig_rollback  # type: ignore[method-assign]

    assert commit_calls == 0, (
        f"Service called conn.commit() {commit_calls} time(s); must be 0 "
        "per prevention-log service-no-commit invariant."
    )
    assert rollback_calls == 0, f"Service called conn.rollback() {rollback_calls} time(s); must be 0."


# ----------------------------------------------------------------------
# 7 — _current settlement-date-wins-most-recent
# ----------------------------------------------------------------------


def test_current_settlement_wins_more_recent(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Ingest 2026-04-15 first then 2026-04-30 — _current reflects 04-30.

    Reverse order: 2026-04-30 first then 2026-04-15 — _current STILL
    reflects 04-30 (newer settlement_date wins, regardless of write order).
    """
    _seed_panel(ebull_test_conn)
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    raw_pristine = _read_pristine()

    # Build a synthetic 2026-04-15 variant by replacing every settlement_date
    # field. Pristine fixture is small; in-memory string replace is fine.
    raw_apr_15 = raw_pristine.replace(b"2026-04-30", b"2026-04-15").replace(b"20260430", b"20260415")

    ingest_run_id = uuid4()
    # Write the OLDER snapshot first.
    with ebull_test_conn.transaction():
        ingest_settlement_file(
            ebull_test_conn,
            date(2026, 4, 15),
            raw_apr_15,
            resolver,
            ingest_run_id,
        )
    # Then the NEWER snapshot.
    with ebull_test_conn.transaction():
        ingest_settlement_file(
            ebull_test_conn,
            date(2026, 4, 30),
            raw_pristine,
            resolver,
            ingest_run_id,
        )

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT settlement_date FROM finra_short_interest_current WHERE instrument_id = %s",
            (1001,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == date(2026, 4, 30), "newer settlement_date should win"

    # Now reverse — ingest an even-older 2026-03-31; _current must stay
    # at 04-30 (older snapshots never displace newer).
    raw_mar = raw_pristine.replace(b"2026-04-30", b"2026-03-31").replace(b"20260430", b"20260331")
    with ebull_test_conn.transaction():
        ingest_settlement_file(
            ebull_test_conn,
            date(2026, 3, 31),
            raw_mar,
            resolver,
            ingest_run_id,
        )
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT settlement_date FROM finra_short_interest_current WHERE instrument_id = %s",
            (1001,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == date(2026, 4, 30), "older settlement_date must NOT displace newer one in _current"


# ----------------------------------------------------------------------
# 8 — Manifest UPSERT re-ingest stays parsed + last_attempted_at advances
# ----------------------------------------------------------------------


def test_manifest_upsert_keeps_parsed_on_re_ingest(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_panel(ebull_test_conn)
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    raw = _read_pristine()
    ingest_run_id = uuid4()

    with ebull_test_conn.transaction():
        ingest_settlement_file(ebull_test_conn, date(2026, 4, 30), raw, resolver, ingest_run_id)

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT ingest_status, last_attempted_at FROM sec_filing_manifest WHERE accession_number = %s",
            ("FINRA_SI_20260430",),
        )
        row_a = cur.fetchone()
        assert row_a is not None
        assert row_a[0] == "parsed"
        first_attempted_at: datetime = row_a[1]

    # Re-ingest the same file (revision-window simulation).
    with ebull_test_conn.transaction():
        ingest_settlement_file(ebull_test_conn, date(2026, 4, 30), raw, resolver, ingest_run_id)

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT ingest_status, last_attempted_at FROM sec_filing_manifest WHERE accession_number = %s",
            ("FINRA_SI_20260430",),
        )
        row_b = cur.fetchone()
        assert row_b is not None
        assert row_b[0] == "parsed"
        second_attempted_at: datetime = row_b[1]
        # last_attempted_at advances on re-ingest (UTC NOW() shift).
        assert second_attempted_at >= first_attempted_at


# ----------------------------------------------------------------------
# 9 — Filed-at midnight UTC anchor
# ----------------------------------------------------------------------


def test_filed_at_anchored_at_settlement_midnight_utc(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_panel(ebull_test_conn)
    resolver = build_preloaded_symbol_resolver(ebull_test_conn)
    raw = _read_pristine()
    ingest_run_id = uuid4()

    with ebull_test_conn.transaction():
        ingest_settlement_file(ebull_test_conn, date(2026, 4, 30), raw, resolver, ingest_run_id)

    expected = datetime(2026, 4, 30, 0, 0, 0, tzinfo=UTC)
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT filed_at FROM finra_short_interest_observations WHERE instrument_id = %s",
            (1001,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == expected
