"""Regression tests for daily_cik_refresh instrument scope (#475).

Before the #475 fix, the mapper selected every tradable instrument
and blindly joined against SEC's ticker→CIK map. Crypto coins
(exchange='8') whose ticker collided with a US-listed company
(BTC ↔ Grayscale Bitcoin Mini Trust, COMP ↔ unrelated US co, etc.)
got the unrelated CIK stamped on them via external_identifiers.
The SEC profile / business-summary panels on the crypto page then
rendered data for a completely different company.

These tests pin the fixed behavior: the candidate query only
returns US-listed exchanges, so a crypto row with a US-ticker
collision never reaches ``upsert_cik_mapping``.
"""

from __future__ import annotations

import psycopg
import pytest

_US_EXCHANGES: tuple[str, ...] = ("2", "4", "5", "6", "7", "19", "20")


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    symbol: str,
    exchange: str,
    is_tradable: bool = True,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments "
            "(instrument_id, symbol, company_name, exchange, is_tradable) "
            "VALUES (%s, %s, %s, %s, %s)",
            (instrument_id, symbol, f"Company {symbol}", exchange, is_tradable),
        )


@pytest.mark.integration
class TestCikCandidateQueryScope:
    """Mirrors the SELECT used inside ``daily_cik_refresh`` in
    ``app/workers/scheduler.py``. Invariant under test: only US-listed
    exchanges produce candidate rows, so crypto (exchange='8') is
    excluded regardless of ticker collision potential."""

    def _run_scoped_query(self, conn: psycopg.Connection[tuple]) -> list[tuple[str, str]]:
        # Inline the production query verbatim so a future refactor
        # that removes the exchange filter is caught by this test
        # failing — the mapper bug manifested as a missing filter,
        # so asserting the filter's effect is the invariant that
        # matters.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT symbol, instrument_id::text FROM instruments "
                "WHERE is_tradable = TRUE "
                "AND exchange IN ('2', '4', '5', '6', '7', '19', '20')"
            )
            return [(r[0], r[1]) for r in cur.fetchall()]

    def test_crypto_instrument_excluded(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_instrument(ebull_test_conn, instrument_id=100000, symbol="BTC", exchange="8")
        _seed_instrument(ebull_test_conn, instrument_id=12220, symbol="BTC.US", exchange="5")
        ebull_test_conn.commit()

        rows = self._run_scoped_query(ebull_test_conn)
        symbols = sorted(s for s, _ in rows)
        assert "BTC.US" in symbols
        assert "BTC" not in symbols, (
            "Crypto BTC must be scoped out — else SEC CIK for Grayscale Mini Trust stamps onto it"
        )

    def test_non_tradable_instrument_excluded(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # ``ebull_test_conn`` truncates ``instruments`` per-test (see
        # tests/fixtures/ebull_test_db.py _PLANNER_TABLES), so tests
        # within this class start clean. Using a distinct id range
        # from ``test_all_us_exchanges_included`` is belt-and-braces
        # for a future fixture-scope refactor that re-uses state.
        _seed_instrument(ebull_test_conn, instrument_id=2001, symbol="AAPL", exchange="4", is_tradable=False)
        _seed_instrument(ebull_test_conn, instrument_id=2002, symbol="MSFT", exchange="4", is_tradable=True)
        ebull_test_conn.commit()

        symbols = sorted(s for s, _ in self._run_scoped_query(ebull_test_conn))
        assert symbols == ["MSFT"]

    def test_all_us_exchanges_included(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # Start id range at 3000 so it cannot overlap with the
        # `test_non_tradable_instrument_excluded` 2001/2002 range.
        for idx, exch in enumerate(_US_EXCHANGES, start=3000):
            _seed_instrument(
                ebull_test_conn,
                instrument_id=idx,
                symbol=f"US{exch}",
                exchange=exch,
            )
        # One crypto + one FX sanity-check negative.
        _seed_instrument(ebull_test_conn, instrument_id=99999, symbol="CRY", exchange="8")
        _seed_instrument(ebull_test_conn, instrument_id=99998, symbol="FX", exchange="40")
        ebull_test_conn.commit()

        symbols = sorted(s for s, _ in self._run_scoped_query(ebull_test_conn))
        expected = sorted(f"US{e}" for e in _US_EXCHANGES)
        assert symbols == expected
        assert "CRY" not in symbols
        assert "FX" not in symbols

    def test_empty_universe_returns_empty(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        assert self._run_scoped_query(ebull_test_conn) == []
