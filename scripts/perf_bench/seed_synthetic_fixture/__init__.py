"""Synthetic-fixture seeders for the perf-bench harness.

Phase 0 NEW-C per docs/proposals/etl/bootstrap-sub-1h-plan.md §7 (Phase 0
acceptance) + docs/proposals/etl/phase-0-instrumentation.md §2.8.

The seeders populate **bench-only** Postgres databases with row counts
meeting the floors at scripts/perf_bench/floors.yaml so the perf-bench
harness can run EXPLAIN ANALYZE on fixtures with prod-scale row counts
without exposing real instrument data.

Writer-safety strategy (full rationale in §2.8 of the phase-0 spec):

1.  All synthetic rows use sentinel ``instrument_id >= 1_000_000_000``
    (well above any real ``instruments.instrument_id``).
2.  The seeder writes ONLY to ``_current`` tables, never to
    ``_observations`` or to ``ownership_refresh_state``. This keeps the
    refresh-sweep helper's drifted-set query (which is anchored on
    ``ownership_refresh_state``) from ever returning a sentinel id, so
    the writer's ``WHEN NOT MATCHED BY SOURCE ... DELETE`` clause cannot
    wipe synthetic rows.
3.  A sentinel preflight assertion fails closed if real
    ``instrument_id`` values ever cross ``1_000_000_000``.

Refusals:

* ``EBULL_BENCH_DB_URL`` unset.
* ``EBULL_BENCH_DB_URL`` does not contain the substring ``bench`` OR
  contains the substring ``dev`` or ``prod`` (case-insensitive). The
  allowlist + denylist guards against accidental seeding of a non-bench
  database.

The implemented seeder is ``seed_ownership_institutions_current``. The
six other floor tables are stubbed with the per-table plan as a
docstring + ``NotImplementedError`` on call; each implementation lands
when the first downstream perf claim needs it.
"""

from __future__ import annotations

import os
import re
import sys
from typing import Any, Final, NoReturn

import psycopg
from psycopg import sql

SENTINEL_INSTRUMENT_ID_BASE: Final[int] = 1_000_000_000

_BENCH_URL_ENV: Final[str] = "EBULL_BENCH_DB_URL"
_BENCH_URL_REQUIRED_SUBSTRING: Final[str] = "bench"
_BENCH_URL_DENYLIST: Final[tuple[str, ...]] = ("dev", "prod")

# Same identifier shape as scripts/perf_bench/_run_explain.py
# TABLE_IDENT_RE. The seeder never interpolates operator input but the
# floors-lookup table name is forwarded into a ``SELECT COUNT(*) FROM
# <ident>`` so we validate before substitution. Prevention-log entry
# "perf_bench harness: unquoted SQL identifier from YAML" (2026-05-26).
_TABLE_IDENT_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z_][a-z0-9_]*$")


def _err(msg: str) -> NoReturn:
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(2)


def sentinel_instrument_id(offset: int) -> int:
    """Return the sentinel ``instrument_id`` for synthetic row ``offset``.

    Sentinels start at ``SENTINEL_INSTRUMENT_ID_BASE`` and increase
    monotonically. Callers MUST run :func:`assert_sentinel_range_clear`
    against the target DB before emitting rows, so a future change to
    ``instruments.instrument_id`` that bumps real ids above the base
    fails closed.
    """
    if offset < 0:
        raise ValueError(f"sentinel offset must be non-negative, got {offset}")
    return SENTINEL_INSTRUMENT_ID_BASE + offset


def assert_sentinel_range_clear(conn: psycopg.Connection[Any]) -> None:
    """Refuse to run if real ``instruments.instrument_id`` crosses the sentinel base.

    Per phase-0 spec §2.8 Codex iter-4 BLOCKING-1: the sentinel range is
    only safe while every real instrument_id stays below the base. Run
    BEFORE emitting any seed rows.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(instrument_id), 0) FROM instruments")
        row = cur.fetchone()
    real_max = int(row[0]) if row else 0
    if real_max >= SENTINEL_INSTRUMENT_ID_BASE:
        _err(
            f"refusing to seed: instruments.MAX(instrument_id)={real_max} has "
            f"crossed sentinel base {SENTINEL_INSTRUMENT_ID_BASE}; raise the "
            "base in scripts/perf_bench/seed_synthetic_fixture/__init__.py "
            "and audit existing synthetic fixtures."
        )


def require_bench_db_url() -> str:
    """Return ``EBULL_BENCH_DB_URL`` or exit 2 with a clear message.

    The URL must contain ``bench`` (case-insensitive) AND must not
    contain ``dev`` or ``prod``. Operator-authored env var so the check
    is bounded but matches the same shape used by the perf-bench
    harness at scripts/perf_bench/_run_explain.py.
    """
    raw = os.environ.get(_BENCH_URL_ENV)
    if not raw:
        _err(f"{_BENCH_URL_ENV} unset; set to a bench-only Postgres URL")
    lowered = raw.lower()
    if _BENCH_URL_REQUIRED_SUBSTRING not in lowered:
        _err(
            f"{_BENCH_URL_ENV} must contain the substring "
            f"'{_BENCH_URL_REQUIRED_SUBSTRING}' (case-insensitive); refusing "
            "to seed a non-bench database"
        )
    for needle in _BENCH_URL_DENYLIST:
        if needle in lowered:
            _err(
                f"{_BENCH_URL_ENV} contains denylisted substring '{needle}'; "
                "refusing to seed; bench databases must not share a name with "
                "dev or prod"
            )
    return raw


def validate_floor(conn: psycopg.Connection[Any], table: str, floor: int) -> int:
    """Return current ``COUNT(*)`` for ``table``; raise if below ``floor``.

    Validates the identifier first (same shape as the perf-bench
    harness) so an arbitrary YAML value cannot reach the cursor as a
    raw identifier. Operator-authored input only, but the shape is the
    same SQL-injection class noted in the prevention log.
    """
    if not _TABLE_IDENT_RE.fullmatch(table):
        raise ValueError(
            f"validate_floor: {table!r} is not a valid lowercase Postgres "
            f"identifier (matches {_TABLE_IDENT_RE.pattern})"
        )
    # Identity-validated above; psycopg.sql.Identifier provides the
    # canonical safe-composition path (defence in depth) so a future
    # caller bypassing the regex still cannot inject SQL.
    query = sql.SQL("SELECT COUNT(*) FROM {tbl}").format(tbl=sql.Identifier(table))
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
    count = int(row[0]) if row else 0
    if count < floor:
        raise AssertionError(f"validate_floor: {table} has {count} rows; below floor {floor}")
    return count


def validate_no_refresh_leak(conn: psycopg.Connection[Any]) -> list[int]:
    """Confirm no sentinel ``instrument_id`` appears in the drifted set.

    Per phase-0 spec §2.8 Codex iter-4 HALLUCINATED-API-1: calls the
    repair-sweep's drifted-set predicate directly (proven importable
    via tests/test_ownership_refresh_writer_merge.py:627). Returns the
    drifted list (typically empty on a freshly-seeded bench DB) so the
    caller can log it. Raises if any returned id is in the sentinel
    range — which would prove the seeder's "never touch _observations
    or ownership_refresh_state" invariant was violated.
    """
    from app.jobs.ownership_observations_repair import _drifted_instruments

    drifted = _drifted_instruments(
        conn,
        "ownership_institutions_current",
        "ownership_institutions_observations",
        "institutions",
    )
    leaked = [iid for iid in drifted if iid >= SENTINEL_INSTRUMENT_ID_BASE]
    if leaked:
        raise AssertionError(
            "validate_no_refresh_leak: sentinel ids surfaced in the "
            f"drifted set: {leaked[:10]} (showing first 10); refresh-sweep "
            "would attempt to delete synthetic rows on next run. Investigate "
            "writes to ownership_institutions_observations or "
            "ownership_refresh_state during the seed."
        )
    return drifted


__all__ = [
    "SENTINEL_INSTRUMENT_ID_BASE",
    "assert_sentinel_range_clear",
    "require_bench_db_url",
    "sentinel_instrument_id",
    "validate_floor",
    "validate_no_refresh_leak",
]
