"""Integration test for the #1988 staleness-v2 SQL (one DB-tier file for
the genuinely-new SQL mechanism, house rule).

Pins the new find_stale_instruments LATERALs end-to-end: latest-thesis
row (bear/bull + mint date), at-or-before mint close, latest close +
freshness date, and the 7d/30d importance-mass aggregate — one
instrument per new reason, all cadence-fresh so only v2 rules can fire.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from app.services.thesis import find_stale_instruments
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)

_NOW = datetime.now(tz=UTC)


def _seed_name(
    conn: psycopg.Connection[tuple],
    iid: int,
    symbol: str,
    *,
    bear: float | None = None,
    bull: float | None = None,
) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (%s, %s, %s, TRUE) ON CONFLICT (instrument_id) DO NOTHING",
        (iid, symbol, f"{symbol} Test Co"),
    )
    conn.execute(
        "INSERT INTO coverage (instrument_id, coverage_tier, review_frequency, filings_status) "
        "VALUES (%s, 1, 'monthly', 'analysable') ON CONFLICT (instrument_id) DO NOTHING",
        (iid,),
    )
    conn.execute(
        """
        INSERT INTO theses (instrument_id, thesis_version, thesis_type, stance, memo_markdown,
                            bear_value, bull_value, created_at)
        VALUES (%s, 1, 'value', 'watch', 'memo', %s, %s, %s)
        """,
        (iid, bear, bull, _NOW - timedelta(days=2)),
    )


def _seed_close(conn: psycopg.Connection[tuple], iid: int, days_ago: int, close: float) -> None:
    conn.execute(
        "INSERT INTO price_daily (instrument_id, price_date, close) VALUES (%s, %s, %s) "
        "ON CONFLICT (instrument_id, price_date) DO UPDATE SET close = EXCLUDED.close",
        (iid, (_NOW - timedelta(days=days_ago)).date(), close),
    )


def test_v2_rules_fire_through_the_sql(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    move_iid, band_iid, news_iid = 930_001, 930_002, 930_003

    # price_move: -35% since mint, fresh latest close, no band.
    _seed_name(conn, move_iid, "MOVE")
    _seed_close(conn, move_iid, 2, 100.0)
    _seed_close(conn, move_iid, 0, 65.0)

    # band_exit: +25% (below price_move threshold), minted inside [90, 120],
    # now outside.
    _seed_name(conn, band_iid, "BAND", bear=90.0, bull=120.0)
    _seed_close(conn, band_iid, 2, 100.0)
    _seed_close(conn, band_iid, 0, 125.0)

    # news_spike: no prices at all (price rules must not mask the news
    # rule); 3 stories @1.0 in the 7d window vs 1 @1.0 in the baseline.
    _seed_name(conn, news_iid, "NEWS")
    for days_ago, score in ((1, 1.0), (2, 1.0), (3, 1.0), (20, 1.0)):
        conn.execute(
            "INSERT INTO news_events (instrument_id, event_time, headline, importance_score, url_hash) "
            "VALUES (%s, %s, %s, %s, %s)",
            (news_iid, _NOW - timedelta(days=days_ago), f"story {days_ago}d", score, f"v2hash{days_ago}"),
        )
    conn.commit()

    result = find_stale_instruments(conn, tier=None, instrument_ids=[move_iid, band_iid, news_iid])

    reasons = {r.instrument_id: r.reason for r in result}
    assert reasons == {
        move_iid: "price_move",
        band_iid: "band_exit",
        news_iid: "news_spike",
    }
