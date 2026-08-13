"""Regression tests for migration 158 (#1233 §6.1).

Pins two contracts of the migration against a real ``ebull_test`` DB:

1. ``instruments.country`` index exists (``idx_instruments_country``)
   so the cross-cutting ``WHERE country='US'`` SEC filter has an
   index-backed lookup path.
2. The backfill UPDATE derives ``instruments.country`` from
   ``exchanges.country`` via the ``instruments.exchange =
   exchanges.exchange_id`` join, only when ``exchanges.country IS NOT
   NULL`` (crypto / FX / index exchanges leave the instrument's
   country NULL — they have no country).

The migration runs at fixture setup so the index check passes by
construction. The backfill SQL is re-executed inline against synthetic
rows so the test pins the behaviour even if a later migration or a
universe-sync upsert mutates the canonical instrument rows between
test runs.
"""

from __future__ import annotations

import psycopg
import pytest

from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# Verbatim from sql/158_instruments_country_backfill.sql so a refactor
# that changes the SQL is caught by this test failing rather than by
# the auto-applied migration succeeding once and never being checked.
_BACKFILL_SQL = """
UPDATE instruments i
SET country = e.country
FROM exchanges e
WHERE i.exchange = e.exchange_id
  AND e.country IS NOT NULL
  AND i.country IS DISTINCT FROM e.country
"""


def test_index_idx_instruments_country_exists(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_indexes WHERE indexname = 'idx_instruments_country'",
        )
        row = cur.fetchone()
    assert row is not None, "migration 158 should create idx_instruments_country"


def _seed(
    conn: psycopg.Connection[tuple],
    *,
    exchange_id: str,
    exchange_country: str | None,
    instrument_id: int,
    symbol: str,
    instrument_country: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO exchanges (exchange_id, country, asset_class)
            VALUES (%s, %s, 'us_equity')
            ON CONFLICT (exchange_id) DO UPDATE SET country = EXCLUDED.country
            """,
            (exchange_id, exchange_country),
        )
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, exchange, country, is_tradable)
            VALUES (%s, %s, %s, %s, %s, TRUE)
            ON CONFLICT (instrument_id) DO UPDATE SET
                country = EXCLUDED.country,
                exchange = EXCLUDED.exchange
            """,
            (instrument_id, symbol, f"Test {symbol}", exchange_id, instrument_country),
        )


def _read_country(conn: psycopg.Connection[tuple], instrument_id: int) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT country FROM instruments WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return row[0]


class TestBackfillCountryFromExchanges:
    def test_us_exchange_populates_country(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
        _seed(
            ebull_test_conn,
            exchange_id="9580",
            exchange_country="US",
            instrument_id=958001,
            symbol="MIG158A",
            instrument_country=None,
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute(_BACKFILL_SQL)
        ebull_test_conn.commit()
        assert _read_country(ebull_test_conn, 958001) == "US"

    def test_null_exchange_country_leaves_instrument_null(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed(
            ebull_test_conn,
            exchange_id="9581",
            exchange_country=None,
            instrument_id=958002,
            symbol="MIG158B",
            instrument_country=None,
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute(_BACKFILL_SQL)
        ebull_test_conn.commit()
        assert _read_country(ebull_test_conn, 958002) is None

    def test_idempotent_on_already_correct_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """``IS DISTINCT FROM`` guard means a re-run of the backfill
        against a row already matching exchange country does not write
        anything — the test pins this by asserting ``rowcount=0``."""
        _seed(
            ebull_test_conn,
            exchange_id="9582",
            exchange_country="GB",
            instrument_id=958003,
            symbol="MIG158C",
            instrument_country="GB",
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute(_BACKFILL_SQL)
            # Re-run on the same row scope.
            cur.execute(
                _BACKFILL_SQL + " AND i.instrument_id = %s",
                (958003,),
            )
            assert cur.rowcount == 0
        ebull_test_conn.commit()
        assert _read_country(ebull_test_conn, 958003) == "GB"

    def test_overwrites_when_exchange_curator_changes_country(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """If the operator re-curates an exchange from US→GB (and a
        prior backfill landed US on the instrument), the next backfill
        propagates the change."""
        _seed(
            ebull_test_conn,
            exchange_id="9583",
            exchange_country="US",
            instrument_id=958004,
            symbol="MIG158D",
            instrument_country=None,
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute(_BACKFILL_SQL)
        ebull_test_conn.commit()
        assert _read_country(ebull_test_conn, 958004) == "US"

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE exchanges SET country = 'GB' WHERE exchange_id = %s",
                ("9583",),
            )
            cur.execute(_BACKFILL_SQL)
        ebull_test_conn.commit()
        assert _read_country(ebull_test_conn, 958004) == "GB"
