"""Integration test for the T3 candle eligibility query — both arms.

SEED arm (#515 PR 0). Crypto / FX / commodity / index instruments are
tier-3 + no candles + no fundamentals (by design — those classes
have no fundamentals_snapshot rows). Before that PR the bootstrap
required EXISTS fundamentals_snapshot, locking every crypto coin
out of the candle pipeline forever — operator-visible symptom: BTC
and LRC instrument pages rendered "no price data". A non-fundamentals-
bearing asset_class qualifies even without fundamentals.

⚠ #2262 REPLACED the seed arm's fundamentals-shaped predicate with design
decision 9's price-eligibility one (tradable + asset_class known and not
'unknown'). An UNPRICED us_equity without fundamentals is now ADMITTED, not
gated — see test_us_equity_without_fundamentals_is_now_admitted for why the
old assertion was backwards. The one remaining rejection is
asset_class='unknown'.

MAINTENANCE arm (#2254). The seed arm's `NOT EXISTS (price_daily)`
used to be the WHOLE query, so a T3 left candle-refresh scope
permanently on its first bar — 3,523 of 3,838 priced T3 had no bar in
30 days. A priced T3 behind the most recent trading day is now
re-admitted, and the seeding gate is deliberately NOT applied to it:
274 priced us_equity T3 carry no fundamentals_snapshot row and would
otherwise stay frozen forever.

Imports the production SELECT directly so a future refactor that
changes the SQL is caught by these tests failing — no inline copy
that could drift (Codex round 1 finding).
"""

from __future__ import annotations

from datetime import date, timedelta

import psycopg
import pytest

from app.services.market_data import most_recent_trading_day
from app.workers.scheduler import (
    _T3_CANDLE_BATCH_SIZE,
    _T3_CANDLE_SELECT,
    _T3_SUPPLY_LESS_MISSES,
    _T3_SUPPLY_LESS_RECHECK,
    BENCHMARK_SYMBOLS,
)

pytestmark = pytest.mark.integration


# Pinned once per module so a case that seeds a bar AT the freshness
# boundary and the query that reads it can never straddle midnight —
# the wall-clock-window flake class from #2224.
_FRESH_THROUGH = most_recent_trading_day(date.today())

_QUERY_PARAMS = {
    "limit": _T3_CANDLE_BATCH_SIZE,
    "benchmark_symbols": [],
    "fresh_through": _FRESH_THROUGH,
    # #2262 — supply-less de-prioritisation params.
    "supply_misses": _T3_SUPPLY_LESS_MISSES,
    "supply_recheck": _T3_SUPPLY_LESS_RECHECK,
}


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


def _cleanup(
    conn: psycopg.Connection[tuple],
    *,
    exchange_ids: list[str],
    instrument_ids: list[int],
) -> None:
    """Drop the seeded fixture rows. Coverage cascades from
    instruments; price_daily does not always cascade (depends on
    constraint shape) so it's deleted explicitly. Both arguments
    are required so a parametrized test that reuses an instrument
    id across cases (Codex review on PR #524) cannot leave a stale
    row that PK-collides on the next run."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM price_daily WHERE instrument_id = ANY(%s)", (instrument_ids,))
        cur.execute("DELETE FROM coverage WHERE instrument_id = ANY(%s)", (instrument_ids,))
        cur.execute("DELETE FROM instruments WHERE instrument_id = ANY(%s)", (instrument_ids,))
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
            cur.execute(_T3_CANDLE_SELECT, _QUERY_PARAMS)
            symbols = sorted(r[1] for r in cur.fetchall())
        assert "TESTBTC" in symbols
    finally:
        _cleanup(
            ebull_test_conn,
            exchange_ids=["test_crypto"],
            instrument_ids=[950100],
        )


# Distinct instrument_id per parametrized case — function-scoped
# `ebull_test_conn` truncates between cases, but if a future
# refactor moves the fixture to class scope the parametrized cases
# would collide on a single id. Belt-and-braces (Codex round 1
# review on PR #524).
_PARAMETRIZED_INSTRUMENT_IDS = {"fx": 950201, "commodity": 950202, "index": 950203}


@pytest.mark.parametrize("asset_class", ["fx", "commodity", "index"])
def test_other_non_fundamentals_classes_qualify(
    ebull_test_conn: psycopg.Connection[tuple],
    asset_class: str,
) -> None:
    """Same OR branch covers fx / commodity / index — none of these
    asset classes carry fundamentals rows by design."""
    exchange_id = f"test_{asset_class}"
    instrument_id = _PARAMETRIZED_INSTRUMENT_IDS[asset_class]
    symbol = f"T{asset_class.upper()[:3]}"

    _seed_exchange(ebull_test_conn, exchange_id=exchange_id, asset_class=asset_class)
    _seed_instrument_t3_no_candles(
        ebull_test_conn,
        instrument_id=instrument_id,
        symbol=symbol,
        exchange=exchange_id,
    )
    ebull_test_conn.commit()

    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_T3_CANDLE_SELECT, _QUERY_PARAMS)
            symbols = sorted(r[1] for r in cur.fetchall())
        assert symbol in symbols
    finally:
        _cleanup(
            ebull_test_conn,
            exchange_ids=[exchange_id],
            instrument_ids=[instrument_id],
        )


def test_us_equity_without_fundamentals_is_now_admitted(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#2262 INVERTS this case, and the inversion is the whole ticket.

    It previously asserted the opposite: an UNPRICED tier-3 us_equity with no
    fundamentals_snapshot row was excluded, on the heuristic "only bother if
    we'll score it". But coverage_tier and fundamentals_snapshot are SEC-fed, so
    that gate made the PRICE universe US-filer-only while presenting as "the
    market" — 2,493 us_equity in exactly this state, plus 4,749 non-US equities,
    7,242 in total, were unpriced for that reason alone.

    Design decision 9 (settled by S6 #2246): price-data eligibility is defined
    on the PRICE path, never on scoring eligibility, and is orthogonal to
    fundamentals coverage.
    """
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
            cur.execute(_T3_CANDLE_SELECT, _QUERY_PARAMS)
            symbols = sorted(r[1] for r in cur.fetchall())
        assert "TESTUSEQ" in symbols
    finally:
        _cleanup(
            ebull_test_conn,
            exchange_ids=["test_us_pr0"],
            instrument_ids=[950300],
        )


def test_unknown_asset_class_stays_gated(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A narrowing gate is measured by what it REJECTS — this is the one thing
    the #2262 predicate still rejects, so it needs its own test.

    194 instruments (CME 192 + 2) sit on exchanges with asset_class='unknown'.
    The operator curates the exchange row first via the #503 PR 4 admin path;
    until then the instrument renders "no data", it is not absent. Without this
    branch the predicate would be purely widening and nobody would notice it had
    stopped excluding anything.
    """
    _seed_exchange(ebull_test_conn, exchange_id="test_unk_pr0", asset_class="unknown")
    _seed_instrument_t3_no_candles(
        ebull_test_conn,
        instrument_id=950301,
        symbol="TESTUNK",
        exchange="test_unk_pr0",
    )
    ebull_test_conn.commit()

    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_T3_CANDLE_SELECT, _QUERY_PARAMS)
            symbols = sorted(r[1] for r in cur.fetchall())
        assert "TESTUNK" not in symbols
    finally:
        _cleanup(
            ebull_test_conn,
            exchange_ids=["test_unk_pr0"],
            instrument_ids=[950301],
        )


def _seed_bar(conn: psycopg.Connection[tuple], instrument_id: int, price_date: date) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO price_daily (instrument_id, price_date, open, high, low, close, volume)
            VALUES (%s, %s, 1, 2, 0.5, 1.5, 100)
            """,
            (instrument_id, price_date),
        )


def test_crypto_with_stale_candles_is_readmitted(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#2254 — the inverted case. This test previously asserted the
    OPPOSITE (`assert "TESTDONE" not in symbols`), pinning the seed-only
    behaviour that froze 3,523 series: crypto with ANY bar dropped out
    forever. A crypto series behind the most recent trading day must now
    come back through the maintenance arm."""
    _seed_exchange(ebull_test_conn, exchange_id="test_crypto_done", asset_class="crypto")
    _seed_instrument_t3_no_candles(
        ebull_test_conn,
        instrument_id=950400,
        symbol="TESTDONE",
        exchange="test_crypto_done",
    )
    _seed_bar(ebull_test_conn, 950400, _FRESH_THROUGH - timedelta(days=30))
    ebull_test_conn.commit()

    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_T3_CANDLE_SELECT, _QUERY_PARAMS)
            symbols = sorted(r[1] for r in cur.fetchall())
        assert "TESTDONE" in symbols
    finally:
        _cleanup(
            ebull_test_conn,
            exchange_ids=["test_crypto_done"],
            instrument_ids=[950400],
        )


def test_current_series_is_excluded(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The maintenance arm is bounded by FRESHNESS, not by existence:
    an instrument already carrying the most recent trading day's bar
    must not consume a request. Without this the branch would re-fetch
    the whole T3 population every run."""
    _seed_exchange(ebull_test_conn, exchange_id="test_crypto_cur", asset_class="crypto")
    _seed_instrument_t3_no_candles(
        ebull_test_conn,
        instrument_id=950410,
        symbol="TESTCUR",
        exchange="test_crypto_cur",
    )
    _seed_bar(ebull_test_conn, 950410, _FRESH_THROUGH)
    ebull_test_conn.commit()

    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_T3_CANDLE_SELECT, _QUERY_PARAMS)
            symbols = sorted(r[1] for r in cur.fetchall())
        assert "TESTCUR" not in symbols
    finally:
        _cleanup(
            ebull_test_conn,
            exchange_ids=["test_crypto_cur"],
            instrument_ids=[950410],
        )


def test_priced_us_equity_without_fundamentals_is_maintained(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The seeding gate must NOT be carried onto the maintenance arm.

    274 priced us_equity T3 in the dev corpus have no
    fundamentals_snapshot row (259 already >30d stale, measured
    2026-08-04). Gating maintenance on fundamentals would leave every
    one of them frozen while still reporting a successful run — the
    same class of silent hole #2254 fixes."""
    _seed_exchange(ebull_test_conn, exchange_id="test_us_stale", asset_class="us_equity")
    _seed_instrument_t3_no_candles(
        ebull_test_conn,
        instrument_id=950420,
        symbol="TESTUSSTALE",
        exchange="test_us_stale",
    )
    _seed_bar(ebull_test_conn, 950420, _FRESH_THROUGH - timedelta(days=60))
    ebull_test_conn.commit()

    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_T3_CANDLE_SELECT, _QUERY_PARAMS)
            symbols = sorted(r[1] for r in cur.fetchall())
        assert "TESTUSSTALE" in symbols
    finally:
        _cleanup(
            ebull_test_conn,
            exchange_ids=["test_us_stale"],
            instrument_ids=[950420],
        )


def test_stalest_series_sort_before_less_stale(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """When the cap binds, the worst series must be fixed first. The
    ORDER BY is `last_bar ASC NULLS FIRST` so unseeded rows lead and
    the stalest maintenance rows follow — a symbol-ordered cap would
    starve the same tail every run."""
    _seed_exchange(ebull_test_conn, exchange_id="test_order", asset_class="crypto")
    # Symbol order is deliberately the INVERSE of staleness order, so a
    # residual `ORDER BY symbol` would fail this assertion.
    _seed_instrument_t3_no_candles(ebull_test_conn, instrument_id=950430, symbol="AAA_RECENT", exchange="test_order")
    _seed_instrument_t3_no_candles(ebull_test_conn, instrument_id=950431, symbol="MMM_ANCIENT", exchange="test_order")
    _seed_instrument_t3_no_candles(ebull_test_conn, instrument_id=950432, symbol="ZZZ_UNSEEDED", exchange="test_order")
    _seed_bar(ebull_test_conn, 950430, _FRESH_THROUGH - timedelta(days=5))
    _seed_bar(ebull_test_conn, 950431, _FRESH_THROUGH - timedelta(days=400))
    ebull_test_conn.commit()

    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(_T3_CANDLE_SELECT, _QUERY_PARAMS)
            ordered = [r[1] for r in cur.fetchall() if r[1].endswith(("_RECENT", "_ANCIENT", "_UNSEEDED"))]
        assert ordered == ["ZZZ_UNSEEDED", "MMM_ANCIENT", "AAA_RECENT"]
    finally:
        _cleanup(
            ebull_test_conn,
            exchange_ids=["test_order"],
            instrument_ids=[950430, 950431, 950432],
        )


def test_benchmark_excluded_before_limit(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Benchmarks are excluded from the T3 bootstrap BEFORE the LIMIT
    is applied. A benchmark symbol that would otherwise sort first
    (alphabetically) must NOT consume a T3 bootstrap slot, leaving
    room for legitimate non-benchmark candidates.

    Concretely: insert SPY (a BENCHMARK_SYMBOLS member) and a
    non-benchmark crypto instrument, run the query with limit=1 and
    benchmark_symbols=["SPY"], and assert the returned row is the
    non-benchmark candidate, not SPY.
    """
    # SPY is a real BENCHMARK_SYMBOLS member; confirm the assumption
    # holds even if BENCHMARK_SYMBOLS changes — skip rather than fail.
    if "SPY" not in BENCHMARK_SYMBOLS:
        pytest.skip("SPY not in BENCHMARK_SYMBOLS — update this test")

    _seed_exchange(ebull_test_conn, exchange_id="test_bench_excl_eq", asset_class="us_equity")
    _seed_exchange(ebull_test_conn, exchange_id="test_bench_excl_cr", asset_class="crypto")

    # SPY-like row at tier 3, no candles. Symbol "SPY" sorts BEFORE the
    # non-benchmark candidate alphabetically, so without the exclusion
    # it would consume the single LIMIT=1 slot.
    _seed_instrument_t3_no_candles(
        ebull_test_conn,
        instrument_id=950501,
        symbol="SPY",
        exchange="test_bench_excl_eq",
    )
    # Non-benchmark tier-3 crypto candidate.
    _seed_instrument_t3_no_candles(
        ebull_test_conn,
        instrument_id=950502,
        symbol="ZTESTCOIN",
        exchange="test_bench_excl_cr",
    )
    ebull_test_conn.commit()

    try:
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                _T3_CANDLE_SELECT,
                {**_QUERY_PARAMS, "limit": 1, "benchmark_symbols": ["SPY"]},
            )
            rows = cur.fetchall()
        returned_symbols = [r[1] for r in rows]
        assert "SPY" not in returned_symbols, "benchmark must be excluded before LIMIT"
        assert "ZTESTCOIN" in returned_symbols, "non-benchmark candidate must fill the slot"
    finally:
        _cleanup(
            ebull_test_conn,
            exchange_ids=["test_bench_excl_eq", "test_bench_excl_cr"],
            instrument_ids=[950501, 950502],
        )
