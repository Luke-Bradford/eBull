"""Nasdaq Trader halt feed parsing and bounded persistence tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest

from app.services.strategy_halts import HaltFeedError, parse_halt_rss, store_halt_snapshot

_XML = b"""<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">
  <channel>
    <title>NASDAQTrader.com</title>
    <pubDate>Fri, 07 Aug 2026 15:00:00 GMT</pubDate>
    <ndaq:numItems>2</ndaq:numItems>
    <item>
      <ndaq:HaltDate>08/07/2026</ndaq:HaltDate>
      <ndaq:HaltTime>10:01:02.000</ndaq:HaltTime>
      <ndaq:IssueSymbol>HALT</ndaq:IssueSymbol>
      <ndaq:Market>NASDAQ</ndaq:Market>
      <ndaq:ReasonCode>T1</ndaq:ReasonCode>
      <ndaq:ResumptionDate />
      <ndaq:ResumptionTradeTime />
    </item>
    <item>
      <ndaq:HaltDate>08/07/2026</ndaq:HaltDate>
      <ndaq:HaltTime>09:45:00.000</ndaq:HaltTime>
      <ndaq:IssueSymbol>BACK</ndaq:IssueSymbol>
      <ndaq:Market>NYSE</ndaq:Market>
      <ndaq:ReasonCode>T5</ndaq:ReasonCode>
      <ndaq:ResumptionDate>08/07/2026</ndaq:ResumptionDate>
      <ndaq:ResumptionTradeTime>10:00:00.000</ndaq:ResumptionTradeTime>
    </item>
  </channel>
</rss>"""


def test_parse_uses_provider_identity_and_eastern_market_time() -> None:
    snapshot = parse_halt_rss(_XML)
    assert len(snapshot.halts) == 2
    assert snapshot.halts[0].symbol == "HALT"
    assert snapshot.halts[0].halt_at == datetime(2026, 8, 7, 14, 1, 2, tzinfo=UTC)
    assert snapshot.halts[0].resumed_at is None
    assert snapshot.halts[1].resumed_at == datetime(2026, 8, 7, 14, 0, tzinfo=UTC)


def test_partial_resumption_and_count_drift_fail_closed() -> None:
    for malformed in (
        _XML.replace(
            b"<ndaq:ResumptionTradeTime />",
            b"<ndaq:ResumptionTradeTime>10:00:00.000</ndaq:ResumptionTradeTime>",
        ),
        _XML.replace(b"<ndaq:numItems>2</ndaq:numItems>", b"<ndaq:numItems>3</ndaq:numItems>"),
    ):
        with pytest.raises(HaltFeedError):
            parse_halt_rss(malformed)


def test_provider_date_without_trade_time_remains_an_active_halt() -> None:
    live_shape = _XML.replace(
        b"<ndaq:ResumptionDate />",
        b"<ndaq:ResumptionDate>08/07/2026</ndaq:ResumptionDate>",
    )
    snapshot = parse_halt_rss(live_shape)
    assert snapshot.halts[0].resumed_at is None


def test_provider_timestamp_without_fractional_seconds_is_supported() -> None:
    live_shape = _XML.replace(b"10:00:00.000", b"10:00:00")
    snapshot = parse_halt_rss(live_shape)
    assert snapshot.halts[1].resumed_at == datetime(2026, 8, 7, 14, 0, tzinfo=UTC)


def test_stale_source_publication_is_not_made_fresh_by_fetch_time(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    snapshot = parse_halt_rss(_XML)
    with pytest.raises(HaltFeedError, match="pubDate is stale"):
        store_halt_snapshot(
            ebull_test_conn,
            snapshot=snapshot,
            fetched_at=datetime(2026, 8, 7, 15, 6, tzinfo=UTC),
        )


def test_feed_publication_time_cannot_regress(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    snapshot = parse_halt_rss(_XML)
    fetched = datetime(2026, 8, 7, 15, 0, tzinfo=UTC)
    store_halt_snapshot(ebull_test_conn, snapshot=snapshot, fetched_at=fetched)
    with pytest.raises(HaltFeedError, match="publication time regressed"):
        store_halt_snapshot(
            ebull_test_conn,
            snapshot=replace(snapshot, source_pub_at=fetched - timedelta(minutes=1)),
            fetched_at=fetched + timedelta(minutes=1),
        )
    assert ebull_test_conn.execute(
        "SELECT source_pub_at FROM strategy_halt_feed_state WHERE source = 'nasdaq_trader_rss'"
    ).fetchone() == (fetched,)


@pytest.mark.integration
def test_store_upserts_current_state_and_removes_old_rows(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    fetched = datetime(2026, 8, 7, 15, 0, tzinfo=UTC)
    conn.execute(
        """
        INSERT INTO strategy_market_halts (
            source, symbol, halt_at, market, reason_code, resumed_at, observed_at
        ) VALUES ('nasdaq_trader_rss', 'OLD', %s, 'NASDAQ', 'T1', %s, %s),
                 ('nasdaq_trader_rss', 'STILL', %s, 'NASDAQ', 'T1', NULL, %s)
        """,
        (
            fetched - timedelta(days=91),
            fetched - timedelta(days=90),
            fetched - timedelta(days=91),
            fetched - timedelta(days=120),
            fetched - timedelta(days=120),
        ),
    )
    snapshot = parse_halt_rss(_XML)

    assert store_halt_snapshot(conn, snapshot=snapshot, fetched_at=fetched) == 1
    assert store_halt_snapshot(conn, snapshot=snapshot, fetched_at=fetched) == 0
    assert conn.execute("SELECT count(*) FROM strategy_halt_feed_state").fetchone() == (1,)
    assert conn.execute("SELECT count(*) FROM strategy_market_halts").fetchone() == (3,)
    assert conn.execute(
        "SELECT symbol FROM strategy_market_halts WHERE resumed_at IS NULL ORDER BY symbol"
    ).fetchall() == [("HALT",), ("STILL",)]
