from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import psycopg
import pytest

from app.providers.market_data import IntradayBar as ProviderBar
from app.providers.market_data import MarketDataProvider
from app.services.strategy_intraday_harvest import (
    _completed_rth_bars,
    _fetch_count,
    _gap_ranges,
    run_intraday_harvest,
)


def _bar(stamp: str, close: str = "100") -> ProviderBar:
    value = Decimal(close)
    return ProviderBar(
        timestamp=datetime.fromisoformat(stamp),
        open=value,
        high=value + 1,
        low=value - 1,
        close=value,
        volume=100,
    )


def test_completed_rth_filters_extended_and_forming_bars() -> None:
    observed = datetime.fromisoformat("2026-08-07T14:00:00+00:00")
    bars = [
        _bar("2026-08-07T12:00:00+00:00"),  # pre-market
        _bar("2026-08-07T13:30:00+00:00"),
        _bar("2026-08-07T13:55:00+00:00"),  # closes exactly at observed_at
        _bar("2026-08-07T14:00:00+00:00"),  # still forming
    ]

    kept = _completed_rth_bars(bars, timeframe="5m", observed_at=observed)

    assert [bar.timestamp for bar in kept] == [bars[1].timestamp, bars[2].timestamp]


def test_gap_detection_never_calls_an_overnight_a_gap() -> None:
    stamps = [
        datetime.fromisoformat("2026-08-07T19:55:00+00:00"),
        datetime.fromisoformat("2026-08-10T13:30:00+00:00"),
        datetime.fromisoformat("2026-08-10T13:40:00+00:00"),
    ]

    assert _gap_ranges(stamps, timeframe="5m", watermark=None) == (
        (
            datetime.fromisoformat("2026-08-10T13:35:00+00:00"),
            datetime.fromisoformat("2026-08-10T13:40:00+00:00"),
        ),
    )


def test_fetch_count_is_small_for_incremental_and_bounded_for_backfill() -> None:
    observed = datetime.fromisoformat("2026-08-07T14:00:00+00:00")
    assert _fetch_count(timeframe="5m", watermark=None, observed_at=observed) == 1_000
    assert (
        _fetch_count(
            timeframe="5m",
            watermark=datetime.fromisoformat("2026-08-07T13:50:00+00:00"),
            observed_at=observed,
        )
        == 5
    )
    assert (
        _fetch_count(
            timeframe="1m",
            watermark=datetime.fromisoformat("2026-01-01T00:00:00+00:00"),
            observed_at=observed,
        )
        == 1_000
    )


def test_seeded_active_panel_is_cross_market_and_bounded(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    version = ebull_test_conn.execute(
        "SELECT universe_version FROM strategy_intraday_universe_versions WHERE status = 'active'"
    ).fetchone()
    counts = ebull_test_conn.execute(
        """
        SELECT timeframe, count(*)
        FROM strategy_intraday_universe_members
        WHERE universe_version = 'ETORO-RTH-V2'
        GROUP BY timeframe ORDER BY timeframe
        """
    ).fetchall()
    symbols = {
        row[0]
        for row in ebull_test_conn.execute(
            """
            SELECT DISTINCT symbol
            FROM strategy_intraday_universe_members
            WHERE universe_version = 'ETORO-RTH-V2'
            """
        ).fetchall()
    }

    assert version == ("ETORO-RTH-V2",)
    assert counts == [("1m", 2), ("30m", 8), ("5m", 8)]
    assert symbols == {"SPY", "QQQ", "IWM", "AAPL", "CENN", "F", "KO", "JPM"}


def _activate_test_universe(
    conn: psycopg.Connection[tuple], *, instrument_id: int, include_missing: bool = False
) -> None:
    conn.execute(
        """
        UPDATE strategy_intraday_universe_versions
        SET status = 'retired', retired_at = now()
        WHERE status = 'active'
        """
    )
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, 'HARVEST', 'Harvest Fixture', 'test', 'USD', true)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id,),
    )
    conn.execute(
        """
        INSERT INTO strategy_intraday_universe_versions (
            universe_version, provider, session_rule, status, rationale, activated_at
        ) VALUES ('TEST-RTH-V1', 'etoro', 'nyse_rth', 'draft', 'fixture', NULL)
        """
    )
    if include_missing:
        conn.execute(
            """
            INSERT INTO strategy_intraday_universe_members (
                universe_version, ordinal, timeframe, symbol, purpose
            ) VALUES ('TEST-RTH-V1', 2, '5m', 'MISSING-HARVEST', 'resolution refusal fixture')
            """
        )
    conn.execute(
        """
        INSERT INTO strategy_intraday_universe_members (
            universe_version, ordinal, timeframe, symbol, purpose
        ) VALUES ('TEST-RTH-V1', 1, '5m', 'HARVEST', 'fixture')
        """
    )
    conn.execute(
        """
        INSERT INTO strategy_intraday_harvest_cursors (universe_version, last_ordinal)
        VALUES ('TEST-RTH-V1', 0)
        """
    )
    conn.execute(
        """
        UPDATE strategy_intraday_universe_versions
        SET status = 'active', activated_at = now()
        WHERE universe_version = 'TEST-RTH-V1'
        """
    )


def test_harvest_writes_completed_rth_once_and_records_gap(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    instrument_id = 2_477_001
    _activate_test_universe(ebull_test_conn, instrument_id=instrument_id)
    provider = MagicMock(spec=MarketDataProvider)
    provider.get_intraday_candles.return_value = [
        _bar("2026-08-07T12:00:00+00:00"),
        _bar("2026-08-07T13:30:00+00:00"),
        _bar("2026-08-07T13:35:00+00:00"),
        _bar("2026-08-07T13:45:00+00:00"),
        _bar("2026-08-07T14:00:00+00:00"),
    ]
    observed = datetime.fromisoformat("2026-08-07T14:00:00+00:00")

    first = run_intraday_harvest(ebull_test_conn, provider, observed_at=observed, max_requests=1)
    second = run_intraday_harvest(ebull_test_conn, provider, observed_at=observed, max_requests=1)

    assert first.written == 3
    assert first.gaps_recorded == 1
    assert first.failures == ()
    assert second.written == 0
    assert second.gaps_recorded == 0
    assert second.failures == ()
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_intraday_bars WHERE timeframe = '5m' AND instrument_id = %s",
        (instrument_id,),
    ).fetchone() == (3,)
    assert ebull_test_conn.execute(
        "SELECT gap_start, gap_end FROM strategy_intraday_gaps WHERE instrument_id = %s",
        (instrument_id,),
    ).fetchone() == (
        datetime.fromisoformat("2026-08-07T13:40:00+00:00"),
        datetime.fromisoformat("2026-08-07T13:45:00+00:00"),
    )


def test_harvest_reports_member_failure_without_writing(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    instrument_id = 2_477_002
    _activate_test_universe(ebull_test_conn, instrument_id=instrument_id)
    provider = MagicMock(spec=MarketDataProvider)
    provider.get_intraday_candles.side_effect = RuntimeError("provider down")

    report = run_intraday_harvest(
        ebull_test_conn,
        provider,
        observed_at=datetime.now(tz=UTC),
        max_requests=1,
    )

    assert report.written == 0
    assert len(report.failures) == 1
    assert report.failures[0].symbol == "HARVEST"
    assert report.failures[0].reason == "RuntimeError"


def test_unresolved_member_does_not_erase_healthy_peer(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    instrument_id = 2_477_003
    _activate_test_universe(ebull_test_conn, instrument_id=instrument_id, include_missing=True)
    provider = MagicMock(spec=MarketDataProvider)
    provider.get_intraday_candles.return_value = [_bar("2026-08-07T13:30:00+00:00")]

    report = run_intraday_harvest(
        ebull_test_conn,
        provider,
        observed_at=datetime.fromisoformat("2026-08-07T13:40:00+00:00"),
        max_requests=2,
    )

    assert report.written == 1
    assert len(report.failures) == 1
    assert report.failures[0].symbol == "MISSING-HARVEST"
    assert report.failures[0].reason == "universe_resolution: expected one tradable instrument, found 0"


def test_active_universe_membership_is_immutable(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _activate_test_universe(ebull_test_conn, instrument_id=2_477_004)

    with pytest.raises(psycopg.errors.RaiseException, match="mutable only while.*draft"):
        with ebull_test_conn.transaction():
            ebull_test_conn.execute(
                """
                INSERT INTO strategy_intraday_universe_members (
                    universe_version, ordinal, timeframe, symbol, purpose
                ) VALUES ('TEST-RTH-V1', 2, '5m', 'LATE', 'must be rejected')
                """
            )
