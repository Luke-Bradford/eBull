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

# Canonical US exchange_ids that exist as us_equity in the test DB
# post-migrations 067 + 069. Was ("2", "4", "5", "6", "7", "19",
# "20") on the pre-#514 seed, but ids 2 (Commodity), 6 (FRA),
# 7 (LSE) were misclassified by migration 067 and got
# reclassified to commodity / eu_equity / uk_equity by migration
# 069. Production has an additional id `33` (Regular Trading
# Hours) that #513's exchanges_metadata_refresh adds to the live
# DB; it isn't in the test DB because the refresh job only runs
# against eToro at runtime, not in the migration seed.
_US_EXCHANGES: tuple[str, ...] = ("4", "5", "19", "20")


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
        # failing. #503 PR 3 swapped the hardcoded id list for a
        # JOIN against the ``exchanges`` table — same invariant
        # ("only us_equity exchanges produce candidates"), expressed
        # via the curated mapping.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT i.symbol, i.instrument_id::text FROM instruments i "
                "JOIN exchanges e ON e.exchange_id = i.exchange "
                "WHERE i.is_tradable = TRUE "
                "AND e.asset_class = 'us_equity'"
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

    def test_unknown_exchange_classification_excluded(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """An instrument on an exchange the operator hasn't yet
        classified (``asset_class = 'unknown'``) is excluded from
        the SEC mapper. New eToro exchange ids land as ``unknown``
        per the migration backfill so they don't silently pick up
        SEC CIKs (Codex round 1 acceptance for #503 PR 3)."""
        # Seed an exchange row classified as ``unknown``.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO exchanges (exchange_id, asset_class) "
                "VALUES ('99', 'unknown') "
                "ON CONFLICT (exchange_id) DO UPDATE SET asset_class = 'unknown'"
            )
        _seed_instrument(ebull_test_conn, instrument_id=4001, symbol="UNK", exchange="99")
        ebull_test_conn.commit()

        symbols = sorted(s for s, _ in self._run_scoped_query(ebull_test_conn))
        assert "UNK" not in symbols
