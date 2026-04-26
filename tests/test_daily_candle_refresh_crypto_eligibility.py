"""Integration test for the T3 candle-bootstrap eligibility query
fix in #515 PR 0. Crypto / FX / commodity / index instruments are
tier-3 + no candles + no fundamentals (by design — those classes
have no fundamentals_snapshot rows). Before this PR the bootstrap
required EXISTS fundamentals_snapshot, locking every crypto coin
out of the candle pipeline forever — operator-visible symptom: BTC
and LRC instrument pages rendered "no price data".

These tests pin the fixed contract: a non-fundamentals-bearing
asset_class qualifies via the OR branch even without fundamentals,
while a us_equity instrument without fundamentals stays gated
(preserves the original heuristic — "only bother if we'll score
it" — for fundamentals-bearing classes).

Imports the production SELECT directly so a future refactor that
changes the SQL is caught by these tests failing — no inline copy
that could drift (Codex round 1 finding).
"""

from __future__ import annotations

import psycopg
import pytest

from app.workers.scheduler import _T3_BOOTSTRAP_BATCH_SIZE, _T3_BOOTSTRAP_SELECT

pytestmark = pytest.mark.integration


_QUERY_PARAMS = {"limit": _T3_BOOTSTRAP_BATCH_SIZE}


def _seed_exchange(
    conn: psycopg.Connection[tuple],
    *,
    exchange_id: str,
    asset_class: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO exchanges (exchange_id, asset_class)
            VALUES (%s, %s)
            ON CONFLICT (exchange_id) DO UPDATE SET
                asset_class = EXCLUDED.asset_class
            """,
            (exchange_id, asset_class),
        )


def _seed_instrument_t3_no_candles(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    symbol: str,
    exchange: str,
) -> None:
    """Tier 3, tradable, no candles, no fundamentals — the exact
    shape a fresh non-fundamentals-bearing instrument lands in."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, exchange, is_tradable)
            VALUES (%s, %s, %s, %s, TRUE)
            """,
            (instrument_id, symbol, f"Test {symbol}", exchange),
        )
        cur.execute(
            """
            INSERT INTO coverage (instrument_id, coverage_tier, filings_status)
            VALUES (%s, 3, 'analysable')
            """,
            (instrument_id,),
        )


def _cleanup_exchange(conn: psycopg.Connection[tuple], exchange_ids: list[str]) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM exchanges WHERE exchange_id = ANY(%s)", (exchange_ids,))
    conn.commit()


def test_crypto_instrument_qualifies_without_fundamentals(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The PR 0 contract: a crypto instrument at tier 3 with no
    fundamentals row appears in the T3 bootstrap. Before this fix
    BTC / LRC / ETH stayed permanently outside the candle ingest."""
    _seed_exchange(ebull_test_conn, exchange_id="test_crypto", asset_class="crypto")
    _seed_instrument_t3_no_candles(
        ebull_test_conn,
        instrument_id=950100,
        symbol="TESTBTC",
        exchange="test_crypto",
    )
    ebull_test_conn.commit()

    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_T3_BOOTSTRAP_SELECT, _QUERY_PARAMS)
            symbols = sorted(r[1] for r in cur.fetchall())
        assert "TESTBTC" in symbols
    finally:
        _cleanup_exchange(ebull_test_conn, ["test_crypto"])


@pytest.mark.parametrize("asset_class", ["fx", "commodity", "index"])
def test_other_non_fundamentals_classes_qualify(
    ebull_test_conn: psycopg.Connection[tuple],
    asset_class: str,
) -> None:
    """Same OR branch covers fx / commodity / index — none of these
    asset classes carry fundamentals rows by design."""
    exchange_id = f"test_{asset_class}"
    _seed_exchange(ebull_test_conn, exchange_id=exchange_id, asset_class=asset_class)
    symbol = f"T{asset_class.upper()[:3]}"
    _seed_instrument_t3_no_candles(
        ebull_test_conn,
        instrument_id=950200,
        symbol=symbol,
        exchange=exchange_id,
    )
    ebull_test_conn.commit()

    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_T3_BOOTSTRAP_SELECT, _QUERY_PARAMS)
            symbols = sorted(r[1] for r in cur.fetchall())
        assert symbol in symbols
    finally:
        _cleanup_exchange(ebull_test_conn, [exchange_id])


def test_us_equity_without_fundamentals_still_gated(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Preserves original behaviour for fundamentals-bearing classes:
    a us_equity instrument at tier 3 with NO fundamentals_snapshot
    is still excluded (the heuristic 'only bother if we'll score
    it'). The OR branch only widens for non-fundamentals-bearing
    classes; us_equity stays on the original gate."""
    _seed_exchange(ebull_test_conn, exchange_id="test_us_pr0", asset_class="us_equity")
    _seed_instrument_t3_no_candles(
        ebull_test_conn,
        instrument_id=950300,
        symbol="TESTUSEQ",
        exchange="test_us_pr0",
    )
    ebull_test_conn.commit()

    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_T3_BOOTSTRAP_SELECT, _QUERY_PARAMS)
            symbols = sorted(r[1] for r in cur.fetchall())
        assert "TESTUSEQ" not in symbols
    finally:
        _cleanup_exchange(ebull_test_conn, ["test_us_pr0"])


def test_crypto_with_existing_candles_excluded(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Crypto with candle history already drops out of the
    bootstrap (the NOT EXISTS price_daily clause is unaffected by
    the new OR branch). Pins that the bootstrap doesn't re-fetch
    instruments that already have data."""
    _seed_exchange(ebull_test_conn, exchange_id="test_crypto_done", asset_class="crypto")
    _seed_instrument_t3_no_candles(
        ebull_test_conn,
        instrument_id=950400,
        symbol="TESTDONE",
        exchange="test_crypto_done",
    )
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO price_daily (instrument_id, price_date, open, high, low, close, volume)
            VALUES (%s, '2026-04-25', 1, 2, 0.5, 1.5, 100)
            """,
            (950400,),
        )
    ebull_test_conn.commit()

    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_T3_BOOTSTRAP_SELECT, _QUERY_PARAMS)
            symbols = sorted(r[1] for r in cur.fetchall())
        assert "TESTDONE" not in symbols
    finally:
        _cleanup_exchange(ebull_test_conn, ["test_crypto_done"])
