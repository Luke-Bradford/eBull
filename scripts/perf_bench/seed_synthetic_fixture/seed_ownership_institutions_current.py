"""Seed ``ownership_institutions_current`` to its floor on a bench DB.

Phase 0 NEW-C reference implementation per
docs/proposals/etl/phase-0-instrumentation.md §2.8.

Usage::

    EBULL_BENCH_DB_URL=postgresql://.../ebull_bench \\
        uv run python -m scripts.perf_bench.seed_synthetic_fixture.seed_ownership_institutions_current

Refusals (all exit 2):

* ``EBULL_BENCH_DB_URL`` unset / missing 'bench' / contains 'dev' or 'prod'.
* ``instruments.MAX(instrument_id) >= SENTINEL_INSTRUMENT_ID_BASE``.
* ``ownership_institutions_observations`` contains a sentinel row.
* ``ownership_refresh_state`` contains a sentinel row.

Behaviour:

The seeder writes rows directly into ``ownership_institutions_current``
via psycopg's binary COPY. It does NOT write to ``_observations`` or
``ownership_refresh_state`` — see
:mod:`scripts.perf_bench.seed_synthetic_fixture` for the writer-safety
rationale. Every emitted row uses sentinel ``instrument_id`` so the
refresh-sweep helper's drifted-set query never picks up a synthetic id,
which protects synthetic rows from the ``WHEN NOT MATCHED BY SOURCE
... THEN DELETE`` clause at
``app/services/ownership_observations.py:505``.

Shape: ``num_instruments`` × ``num_filers`` × |ownership_nature| rows.
At the defaults (1000 × 200 × 5) this yields 1,000,000 rows, matching
the floor at ``scripts/perf_bench/floors.yaml``.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from collections.abc import Iterator
from typing import Final

import psycopg
from psycopg import sql

from scripts.perf_bench._floors import load_floors
from scripts.perf_bench.seed_synthetic_fixture import (
    SENTINEL_INSTRUMENT_ID_BASE,
    assert_sentinel_range_clear,
    require_bench_db_url,
    sentinel_instrument_id,
    validate_floor,
    validate_no_refresh_leak,
)

TARGET_TABLE: Final[str] = "ownership_institutions_current"

# All literal values match CHECK constraints at sql/114
# (filer_type / ownership_nature / source / exposure_kind enums) AND
# sql/134_ownership_identifier_check_constraints.sql:54-58
# (filer_cik ~ '^[0-9]{10}$'). Codex 2 pre-push catch (2026-05-27):
# the initial implementation generated ``SYN`` + zero-padded CIKs and
# failed the sql/134 CHECK on the first COPY row. Grep-before-cite must
# include ALTER TABLE ... ADD CONSTRAINT migrations, not just the
# CREATE TABLE statement. Captured in docs/review-prevention-log.md.
_OWNERSHIP_NATURES: Final[tuple[str, ...]] = (
    "direct",
    "indirect",
    "beneficial",
    "voting",
    "economic",
)
_FILER_TYPES: Final[tuple[str, ...]] = ("ETF", "INV", "INS", "BD", "OTHER")
_SOURCE_LITERAL: Final[str] = "derived"
_EXPOSURE_KIND: Final[str] = "EQUITY"
_PERIOD_END: Final[dt.date] = dt.date(2026, 3, 31)
_FILED_AT: Final[dt.datetime] = dt.datetime(2026, 4, 15, tzinfo=dt.UTC)

# Synthetic 10-digit CIK base. Real SEC CIKs allocate ascending from
# 0000000001; the highest in use as of 2026 is ~0002100000 (filer
# ``EDGAR Filer ID`` issuance). The 9_000_000_000+ range keeps synthetic
# CIKs distinct from any real one for the foreseeable future and still
# satisfies the 10-digit zero-padded format CHECK.
_SYNTHETIC_FILER_CIK_BASE: Final[int] = 9_000_000_000


def _synthetic_filer_cik(offset: int) -> str:
    """Return a 10-digit synthetic CIK satisfying ``^[0-9]{10}$``."""
    if offset < 0:
        raise ValueError(f"filer offset must be non-negative, got {offset}")
    value = _SYNTHETIC_FILER_CIK_BASE + offset
    if value > 9_999_999_999:
        raise ValueError(
            f"filer offset {offset} overflows synthetic 10-digit CIK range "
            f"(base {_SYNTHETIC_FILER_CIK_BASE}, max value 9999999999)"
        )
    return f"{value:010d}"


_RowTuple = tuple[int, str, str, str, str, str, str, dt.date, dt.datetime, str]


def _generate_rows(num_instruments: int, num_filers: int) -> Iterator[_RowTuple]:
    """Stream synthetic rows so the seeder does not buffer 1M tuples in memory."""
    for i in range(num_instruments):
        iid = sentinel_instrument_id(i)
        for f in range(num_filers):
            filer_cik = _synthetic_filer_cik(f)
            filer_name = f"Synthetic Filer {f}"
            filer_type = _FILER_TYPES[f % len(_FILER_TYPES)]
            for nature in _OWNERSHIP_NATURES:
                source_document_id = f"SYN-{iid}-{filer_cik}-{nature}"
                yield (
                    iid,
                    filer_cik,
                    filer_name,
                    filer_type,
                    nature,
                    _SOURCE_LITERAL,
                    source_document_id,
                    _PERIOD_END,
                    _FILED_AT,
                    _EXPOSURE_KIND,
                )


def _assert_observations_sentinel_free(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ownership_institutions_observations WHERE instrument_id >= %s",
            (SENTINEL_INSTRUMENT_ID_BASE,),
        )
        row = cur.fetchone()
    count = int(row[0]) if row else 0
    if count != 0:
        raise AssertionError(
            f"ownership_institutions_observations has {count} sentinel rows; "
            "the direct-current-seed protocol was violated. Investigate any "
            "prior seeder run that wrote to _observations."
        )


def _assert_refresh_state_sentinel_free(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ownership_refresh_state WHERE instrument_id >= %s AND category = 'institutions'",
            (SENTINEL_INSTRUMENT_ID_BASE,),
        )
        row = cur.fetchone()
    count = int(row[0]) if row else 0
    if count != 0:
        raise AssertionError(
            f"ownership_refresh_state has {count} sentinel rows for "
            "category='institutions'; refresh-sweep would attempt to drain "
            "synthetic ids. Re-applying sql/163 backfill against a seeded "
            "DB is the known cause; delete sentinel rows and re-seed."
        )


def _assert_pk_unique(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ("
            " SELECT instrument_id, filer_cik, ownership_nature, exposure_kind"
            " FROM ownership_institutions_current"
            " GROUP BY 1, 2, 3, 4 HAVING COUNT(*) > 1"
            ") x"
        )
        row = cur.fetchone()
    dupes = int(row[0]) if row else 0
    if dupes != 0:
        raise AssertionError(
            f"ownership_institutions_current PK violated: {dupes} duplicate "
            "(instrument_id, filer_cik, ownership_nature, exposure_kind) groups"
        )


def _assert_no_real_overlap(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ownership_institutions_current oic "
            "JOIN instruments i ON i.instrument_id = oic.instrument_id "
            "WHERE oic.instrument_id < %s",
            (SENTINEL_INSTRUMENT_ID_BASE,),
        )
        row = cur.fetchone()
    overlap = int(row[0]) if row else 0
    if overlap != 0:
        raise AssertionError(
            f"ownership_institutions_current has {overlap} rows whose "
            "instrument_id matches a real instruments row; seed accidentally "
            "wrote into real-id space"
        )


def _assert_sentinel_range_only(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ownership_institutions_current WHERE instrument_id < %s",
            (SENTINEL_INSTRUMENT_ID_BASE,),
        )
        row = cur.fetchone()
    real_rows = int(row[0]) if row else 0
    if real_rows != 0:
        raise AssertionError(
            f"ownership_institutions_current has {real_rows} non-sentinel "
            f"rows (instrument_id < {SENTINEL_INSTRUMENT_ID_BASE}); the "
            "bench DB contains real data — refusing to validate"
        )


def _seed(conn: psycopg.Connection, num_instruments: int, num_filers: int) -> int:
    columns = (
        "instrument_id",
        "filer_cik",
        "filer_name",
        "filer_type",
        "ownership_nature",
        "source",
        "source_document_id",
        "period_end",
        "filed_at",
        "exposure_kind",
    )
    # ``TARGET_TABLE`` is a hardcoded ``Final`` and column names are a
    # literal tuple, but compose via ``sql.Identifier`` / ``sql.SQL`` so
    # the defence-in-depth pattern matches ``validate_floor`` and the
    # prevention-log entry "Unquoted SQL identifier in shell-out
    # harness" (Claude review NITPICK fold, PR #1359).
    copy_sql = sql.SQL("COPY {tbl} ({cols}) FROM STDIN").format(
        tbl=sql.Identifier(TARGET_TABLE),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
    )
    written = 0
    with conn.cursor() as cur, cur.copy(copy_sql) as copy:
        for row in _generate_rows(num_instruments, num_filers):
            copy.write_row(row)
            written += 1
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--num-instruments",
        type=int,
        default=1000,
        help="Number of distinct sentinel instrument_ids (default 1000)",
    )
    parser.add_argument(
        "--num-filers",
        type=int,
        default=200,
        help="Number of distinct filer_ciks per instrument (default 200)",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Skip seeding; only run the 7 sentinel-invariant assertions",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned row count and exit without writing",
    )
    args = parser.parse_args(argv)

    if args.num_instruments <= 0 or args.num_filers <= 0:
        print("error: --num-instruments and --num-filers must be positive", file=sys.stderr)
        return 2

    planned = args.num_instruments * args.num_filers * len(_OWNERSHIP_NATURES)
    floor = load_floors()[TARGET_TABLE]
    if not args.verify_only and planned < floor:
        print(
            f"error: planned row count {planned} is below floor {floor}; raise --num-instruments or --num-filers",
            file=sys.stderr,
        )
        return 2

    if args.dry_run:
        print(
            f"dry-run: would seed {planned} rows into {TARGET_TABLE} "
            f"(floor {floor}); sentinel base {SENTINEL_INSTRUMENT_ID_BASE}"
        )
        return 0

    url = require_bench_db_url()
    with psycopg.connect(url) as conn:
        assert_sentinel_range_clear(conn)
        if not args.verify_only:
            written = _seed(conn, args.num_instruments, args.num_filers)
            conn.commit()
            print(f"seeded {written} rows into {TARGET_TABLE}")

        # Real-numbers verification (7 assertions per spec §2.8).
        # 1: floor met
        count = validate_floor(conn, TARGET_TABLE, floor)
        # 2: PK uniqueness
        _assert_pk_unique(conn)
        # 3: sentinel-range only
        _assert_sentinel_range_only(conn)
        # 4: no real-instrument overlap
        _assert_no_real_overlap(conn)
        # 5: observations sentinel-free
        _assert_observations_sentinel_free(conn)
        # 6: refresh_state sentinel-free
        _assert_refresh_state_sentinel_free(conn)
        # 7: drifted set excludes sentinels
        drifted = validate_no_refresh_leak(conn)

    print(f"verification PASS: count={count} floor={floor} drifted_non_sentinel={len(drifted)}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
