"""Regression tests for migration 159 (#1431).

Pins the contracts of the migration against a real ``ebull_test`` DB:

1. ``exchanges.currency`` carries the ISO-4217 CHECK constraint
   (``exchanges_currency_iso4217_chk``): NULL allowed, 3 uppercase
   letters allowed, anything else rejected.
2. The backfill UPDATE derives ``instruments.currency`` from
   ``exchanges.currency`` via the ``instruments.exchange =
   exchanges.exchange_id`` join, only when ``exchanges.currency IS NOT
   NULL`` (crypto / FX / index / uncurated exchanges leave the
   instrument's currency NULL — they have no single trading currency).

Mirrors ``test_migration_158_country_backfill.py`` — currency is the
same operator-curated exchange-join derivation as country, just not 1:1
with country (CH→CHF, Nordics diverge from EUR).
"""

from __future__ import annotations

import psycopg
import pytest

from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# Verbatim from sql/159_instruments_currency_backfill.sql so a refactor
# that changes the SQL is caught by this test failing rather than by the
# auto-applied migration succeeding once and never being checked.
_BACKFILL_SQL = """
UPDATE instruments i
SET currency = e.currency
FROM exchanges e
WHERE i.exchange = e.exchange_id
  AND e.currency IS NOT NULL
  AND i.currency IS DISTINCT FROM e.currency
"""


def test_check_constraint_exists(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_constraint "
            "WHERE conname = 'exchanges_currency_iso4217_chk' "
            "AND conrelid = 'exchanges'::regclass",
        )
        assert cur.fetchone() is not None, "migration 159 should add the ISO-4217 CHECK on exchanges"


@pytest.mark.parametrize("bad", ["usd", "US", "DOLLAR", "12", "us$"])
def test_check_constraint_rejects_non_iso4217(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    bad: str,
) -> None:
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO exchanges (exchange_id, asset_class) VALUES ('959bad', 'unknown') "
            "ON CONFLICT (exchange_id) DO NOTHING",
        )
        with pytest.raises(psycopg.errors.CheckViolation):
            cur.execute(
                "UPDATE exchanges SET currency = %s WHERE exchange_id = '959bad'",
                (bad,),
            )
    ebull_test_conn.rollback()


def _seed(
    conn: psycopg.Connection[tuple],
    *,
    exchange_id: str,
    exchange_currency: str | None,
    instrument_id: int,
    symbol: str,
    instrument_currency: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO exchanges (exchange_id, currency, asset_class)
            VALUES (%s, %s, 'us_equity')
            ON CONFLICT (exchange_id) DO UPDATE SET currency = EXCLUDED.currency
            """,
            (exchange_id, exchange_currency),
        )
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
            VALUES (%s, %s, %s, %s, %s, TRUE)
            ON CONFLICT (instrument_id) DO UPDATE SET
                currency = EXCLUDED.currency,
                exchange = EXCLUDED.exchange
            """,
            (instrument_id, symbol, f"Test {symbol}", exchange_id, instrument_currency),
        )


def _read_currency(conn: psycopg.Connection[tuple], instrument_id: int) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT currency FROM instruments WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return row[0]


class TestBackfillCurrencyFromExchanges:
    def test_curated_exchange_populates_currency(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
        _seed(
            ebull_test_conn,
            exchange_id="9590",
            exchange_currency="USD",
            instrument_id=959001,
            symbol="MIG159A",
            instrument_currency=None,
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute(_BACKFILL_SQL)
        ebull_test_conn.commit()
        assert _read_currency(ebull_test_conn, 959001) == "USD"

    def test_null_exchange_currency_leaves_instrument_null(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed(
            ebull_test_conn,
            exchange_id="9591",
            exchange_currency=None,
            instrument_id=959002,
            symbol="MIG159B",
            instrument_currency=None,
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute(_BACKFILL_SQL)
        ebull_test_conn.commit()
        assert _read_currency(ebull_test_conn, 959002) is None

    def test_idempotent_on_already_correct_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """``IS DISTINCT FROM`` guard: a re-run against a row already
        matching the exchange currency writes nothing (rowcount=0)."""
        _seed(
            ebull_test_conn,
            exchange_id="9592",
            exchange_currency="GBP",
            instrument_id=959003,
            symbol="MIG159C",
            instrument_currency="GBP",
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute(_BACKFILL_SQL)
            cur.execute(_BACKFILL_SQL + " AND i.instrument_id = %s", (959003,))
            assert cur.rowcount == 0
        ebull_test_conn.commit()
        assert _read_currency(ebull_test_conn, 959003) == "GBP"

    def test_overwrites_when_exchange_curator_changes_currency(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Operator re-curating an exchange's currency propagates on the
        next backfill (USD→CHF, e.g. a re-classified listing venue)."""
        _seed(
            ebull_test_conn,
            exchange_id="9593",
            exchange_currency="USD",
            instrument_id=959004,
            symbol="MIG159D",
            instrument_currency=None,
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute(_BACKFILL_SQL)
        ebull_test_conn.commit()
        assert _read_currency(ebull_test_conn, 959004) == "USD"

        with ebull_test_conn.cursor() as cur:
            cur.execute("UPDATE exchanges SET currency = 'CHF' WHERE exchange_id = '9593'")
            cur.execute(_BACKFILL_SQL)
        ebull_test_conn.commit()
        assert _read_currency(ebull_test_conn, 959004) == "CHF"
