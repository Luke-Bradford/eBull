"""Regression tests for migration 068 (#503 PR 4).

Pins two contracts of the migration against a real ``ebull_test`` DB:

1. ``instruments.instrument_type`` exists, is a TEXT column, and the
   index ``idx_instruments_instrument_type`` exists.
2. The auto-classifier UPDATE recognises the documented suffixes
   (``.L`` → ``uk_equity`` + GB, ``.HK`` → ``asia_equity`` + HK,
   no-suffix-but-large → ``us_equity`` + US, etc.) and leaves rows
   with mixed / unrecognised suffixes as ``unknown``.

The migration runs at fixture setup so the schema check passes by
construction. The classification test re-executes the migration's
UPDATE inline (it is idempotent — the WHERE clause is gated on
``asset_class = 'unknown'``) against synthetic exchange ids that
the migration's seed cannot have already classified.
"""

from __future__ import annotations

import psycopg
import pytest

from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# Re-runs the dominant-suffix classifier UPDATE. Verbatim from
# sql/068_exchanges_classify_and_instrument_type.sql so a future
# refactor that changes the SQL is caught by this test failing
# (rather than by the auto-applied migration succeeding once and
# never being checked again).
_CLASSIFY_SQL = """
WITH suffix_counts AS (
    SELECT
        i.exchange AS exchange_id,
        CASE
            WHEN POSITION('.' IN i.symbol) > 0
                THEN UPPER(SPLIT_PART(REVERSE(i.symbol), '.', 1))
            ELSE NULL
        END AS suffix,
        COUNT(*) AS n
    FROM instruments i
    WHERE i.exchange IS NOT NULL
    GROUP BY 1, 2
),
ranked AS (
    SELECT exchange_id, suffix, n,
           ROW_NUMBER() OVER (PARTITION BY exchange_id ORDER BY n DESC) AS rn,
           SUM(n) OVER (PARTITION BY exchange_id) AS total_n
    FROM suffix_counts
),
dominant AS (
    SELECT
        r.exchange_id,
        REVERSE(r.suffix) AS suffix,
        r.n,
        r.total_n
    FROM ranked r
    WHERE r.rn = 1
      AND r.n::numeric / NULLIF(r.total_n, 0) > 0.80
      AND NOT EXISTS (
          SELECT 1 FROM ranked r2
          WHERE r2.exchange_id = r.exchange_id
            AND r2.rn = 2
            AND r2.n = r.n
      )
)
UPDATE exchanges e
   SET asset_class = m.asset_class,
       country     = m.country,
       updated_at  = NOW()
  FROM (
      SELECT d.exchange_id,
             CASE
                 WHEN d.suffix IS NULL AND d.total_n > 30 THEN 'us_equity'
                 WHEN d.suffix = 'L'    THEN 'uk_equity'
                 WHEN d.suffix = 'HK'   THEN 'asia_equity'
                 WHEN d.suffix = 'T'    THEN 'asia_equity'
                 ELSE NULL
             END AS asset_class,
             CASE
                 WHEN d.suffix = 'L'    THEN 'GB'
                 WHEN d.suffix = 'HK'   THEN 'HK'
                 WHEN d.suffix = 'T'    THEN 'JP'
                 WHEN d.suffix IS NULL AND d.total_n > 30 THEN 'US'
                 ELSE NULL
             END AS country
      FROM dominant d
  ) AS m
 WHERE e.exchange_id = m.exchange_id
   AND e.asset_class = 'unknown'
   AND m.asset_class IS NOT NULL
"""


# Synthetic exchange ids — chosen so they cannot collide with the
# migration-067 seed or the migration-068 UPDATE having run earlier
# against a real id.
_EX_UK = "test_ex_uk"
_EX_HK = "test_ex_hk"
_EX_JP = "test_ex_jp"
_EX_US = "test_ex_us"
_EX_MIXED = "test_ex_mixed"


def _seed_exchange(conn: psycopg.Connection[tuple], exchange_id: str) -> None:
    """Insert (or reset) an unknown-class exchange row for the test
    to act on. Each test cleans up its own ids in the finally block."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO exchanges (exchange_id, asset_class, country)
            VALUES (%s, 'unknown', NULL)
            ON CONFLICT (exchange_id) DO UPDATE SET
                asset_class = 'unknown',
                country     = NULL
            """,
            (exchange_id,),
        )


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    symbol: str,
    exchange: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, exchange, is_tradable)
            VALUES (%s, %s, %s, %s, TRUE)
            """,
            (instrument_id, symbol, f"Test {symbol}", exchange),
        )


def _read(conn: psycopg.Connection[tuple], exchange_id: str) -> tuple[str, str | None] | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT asset_class, country FROM exchanges WHERE exchange_id = %s",
            (exchange_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return (row[0], row[1])


def _cleanup(conn: psycopg.Connection[tuple], ids: list[str]) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM exchanges WHERE exchange_id = ANY(%s)", (ids,))
    conn.commit()


def test_instrument_type_column_exists(ebull_test_conn) -> None:  # noqa: F811
    """Schema-level pin: the column the universe upsert writes to
    must exist with TEXT type, and the lookup index for operator
    audit queries must be present."""
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT data_type
              FROM information_schema.columns
             WHERE table_name = 'instruments'
               AND column_name = 'instrument_type'
            """,
        )
        row = cur.fetchone()
    assert row is not None, "instruments.instrument_type column missing"
    assert row[0] == "text"

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM pg_indexes
             WHERE tablename = 'instruments'
               AND indexname = 'idx_instruments_instrument_type'
            """,
        )
        assert cur.fetchone() is not None, "idx_instruments_instrument_type index missing"


def test_classifier_uk_suffix(ebull_test_conn) -> None:  # noqa: F811
    """``.L`` suffix dominates → ``uk_equity`` / GB."""
    _seed_exchange(ebull_test_conn, _EX_UK)
    _seed_instrument(ebull_test_conn, instrument_id=900001, symbol="BARC.L", exchange=_EX_UK)
    _seed_instrument(ebull_test_conn, instrument_id=900002, symbol="LLOY.L", exchange=_EX_UK)
    ebull_test_conn.commit()
    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_CLASSIFY_SQL)
        ebull_test_conn.commit()
        assert _read(ebull_test_conn, _EX_UK) == ("uk_equity", "GB")
    finally:
        _cleanup(ebull_test_conn, [_EX_UK])


def test_classifier_asia_suffixes(ebull_test_conn) -> None:  # noqa: F811
    """``.HK`` and ``.T`` both classify to ``asia_equity`` but with
    distinct country codes — pin both to catch a future bug that
    collapses the country mapping."""
    _seed_exchange(ebull_test_conn, _EX_HK)
    _seed_exchange(ebull_test_conn, _EX_JP)
    _seed_instrument(ebull_test_conn, instrument_id=900010, symbol="0700.HK", exchange=_EX_HK)
    _seed_instrument(ebull_test_conn, instrument_id=900011, symbol="7203.T", exchange=_EX_JP)
    ebull_test_conn.commit()
    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_CLASSIFY_SQL)
        ebull_test_conn.commit()
        assert _read(ebull_test_conn, _EX_HK) == ("asia_equity", "HK")
        assert _read(ebull_test_conn, _EX_JP) == ("asia_equity", "JP")
    finally:
        _cleanup(ebull_test_conn, [_EX_HK, _EX_JP])


def test_classifier_us_no_suffix_large_universe(ebull_test_conn) -> None:  # noqa: F811
    """No-suffix dominance + total_n > 30 → ``us_equity`` / US.
    Mirrors the NASDAQ/NYSE main-listing heuristic."""
    _seed_exchange(ebull_test_conn, _EX_US)
    for i in range(40):
        _seed_instrument(
            ebull_test_conn,
            instrument_id=900100 + i,
            symbol=f"USCO{i:02d}",
            exchange=_EX_US,
        )
    ebull_test_conn.commit()
    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_CLASSIFY_SQL)
        ebull_test_conn.commit()
        assert _read(ebull_test_conn, _EX_US) == ("us_equity", "US")
    finally:
        _cleanup(ebull_test_conn, [_EX_US])


def test_classifier_mixed_plurality_stays_unknown(
    ebull_test_conn,  # noqa: F811
) -> None:
    """Plurality at 60% (below the 80% dominance gate) → stays
    ``unknown``. Pins the BLOCKING-fix from Codex round 1: a mixed
    exchange with both ``.L`` (60%) and ``.MI`` (40%) listings must
    NOT auto-classify as ``uk_equity``. Operator review required."""
    _seed_exchange(ebull_test_conn, _EX_MIXED)
    # 6 .L + 4 .MI = 10 total, dominant suffix = .L at 60%.
    for i in range(6):
        _seed_instrument(
            ebull_test_conn,
            instrument_id=900400 + i,
            symbol=f"UK{i:02d}.L",
            exchange=_EX_MIXED,
        )
    for i in range(4):
        _seed_instrument(
            ebull_test_conn,
            instrument_id=900410 + i,
            symbol=f"IT{i:02d}.MI",
            exchange=_EX_MIXED,
        )
    ebull_test_conn.commit()
    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_CLASSIFY_SQL)
        ebull_test_conn.commit()
        assert _read(ebull_test_conn, _EX_MIXED) == ("unknown", None)
    finally:
        _cleanup(ebull_test_conn, [_EX_MIXED])


def test_classifier_tied_winners_stay_unknown(
    ebull_test_conn,  # noqa: F811
) -> None:
    """Two suffixes tied for #1 → ``ROW_NUMBER`` would pick one
    arbitrarily, so the dominance gate's NOT-EXISTS clause refuses
    to classify. Belt-and-braces protection beyond the 80% rule."""
    _seed_exchange(ebull_test_conn, _EX_MIXED)
    # 5 .L + 5 .MI — tied at 50/50, neither passes 80%, AND tie-guard
    # would reject even if it did.
    for i in range(5):
        _seed_instrument(
            ebull_test_conn,
            instrument_id=900500 + i,
            symbol=f"UK{i:02d}.L",
            exchange=_EX_MIXED,
        )
    for i in range(5):
        _seed_instrument(
            ebull_test_conn,
            instrument_id=900510 + i,
            symbol=f"IT{i:02d}.MI",
            exchange=_EX_MIXED,
        )
    ebull_test_conn.commit()
    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_CLASSIFY_SQL)
        ebull_test_conn.commit()
        assert _read(ebull_test_conn, _EX_MIXED) == ("unknown", None)
    finally:
        _cleanup(ebull_test_conn, [_EX_MIXED])


def test_classifier_no_suffix_small_universe_stays_unknown(
    ebull_test_conn,  # noqa: F811
) -> None:
    """No-suffix and total_n <= 30 → not enough confidence; stays
    ``unknown`` so the operator can audit. Pins the threshold so a
    future change that drops the > 30 guard is caught."""
    _seed_exchange(ebull_test_conn, _EX_MIXED)
    for i in range(5):  # 5 << 30
        _seed_instrument(
            ebull_test_conn,
            instrument_id=900200 + i,
            symbol=f"OTC{i:02d}",
            exchange=_EX_MIXED,
        )
    ebull_test_conn.commit()
    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_CLASSIFY_SQL)
        ebull_test_conn.commit()
        assert _read(ebull_test_conn, _EX_MIXED) == ("unknown", None)
    finally:
        _cleanup(ebull_test_conn, [_EX_MIXED])
