"""Nasdaq Trader halt feed parsing and bounded persistence tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest

from app.services.strategy_halt_identity import INSTRUMENT_HALT_SYMBOL_SQL
from app.services.strategy_halts import (
    HaltFeedError,
    content_sha256,
    parse_halt_rss,
    store_halt_snapshot,
)

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


def test_feed_publication_time_cannot_regress_when_the_content_changed(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """A regressed stamp whose halt content DIFFERS is still refused, with numbers.

    ⚠ This test previously regressed the stamp on an otherwise identical snapshot
    (``replace(snapshot, source_pub_at=...)``), which is precisely the CDN re-serve
    #3049 measured and the case the fix now accepts — so it PINNED the defect. Its
    inverse, not its deletion, is the replacement (#2795's recorded lesson).
    """
    snapshot = parse_halt_rss(_XML)
    fetched = datetime(2026, 8, 7, 15, 0, tzinfo=UTC)
    store_halt_snapshot(ebull_test_conn, snapshot=snapshot, fetched_at=fetched)
    # Drop a halt: an older stamp that has LOST a published halt must never be
    # allowed to overwrite the newer view that still carries it.
    diverged = replace(
        snapshot,
        source_pub_at=fetched - timedelta(minutes=1),
        halts=snapshot.halts[:1],
        content_sha256=content_sha256(snapshot.halts[:1]),
    )
    with pytest.raises(HaltFeedError, match="regressed with changed content") as excinfo:
        store_halt_snapshot(
            ebull_test_conn,
            snapshot=diverged,
            fetched_at=fetched + timedelta(minutes=1),
        )
    # The magnitude must be IN the message: #3049's other half is that 54 lifetime
    # failures were untriageable because a 35-second skew and a 6-hour stale cache
    # produced byte-identical text.
    message = str(excinfo.value)
    assert "stored=2026-08-07T15:00:00+00:00" in message
    assert "fetched=2026-08-07T14:59:00+00:00" in message
    assert "delta=60s" in message
    assert ebull_test_conn.execute(
        "SELECT source_pub_at FROM strategy_halt_feed_state WHERE source = 'nasdaq_trader_rss'"
    ).fetchone() == (fetched,)


def test_a_regressed_stamp_with_identical_content_is_accepted_as_a_reserve(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The #3049 fix: an unchanged payload under an older stamp overwrites nothing.

    The stored ``source_pub_at`` is held at the MAXIMUM rather than taking the
    re-serve's older value — otherwise the next poll from the newer CDN lineage
    would compare against a stamp it has already passed and the refusals would
    simply alternate.
    """
    snapshot = parse_halt_rss(_XML)
    fetched = datetime(2026, 8, 7, 15, 0, tzinfo=UTC)
    store_halt_snapshot(ebull_test_conn, snapshot=snapshot, fetched_at=fetched)
    reserve_fetched = fetched + timedelta(minutes=1)
    store_halt_snapshot(
        ebull_test_conn,
        snapshot=replace(snapshot, source_pub_at=fetched - timedelta(seconds=21)),
        fetched_at=reserve_fetched,
    )
    assert ebull_test_conn.execute(
        "SELECT source_pub_at, fetched_at FROM strategy_halt_feed_state WHERE source = 'nasdaq_trader_rss'"
    ).fetchone() == (fetched, reserve_fetched)


def test_payload_sha256_cannot_stand_in_for_the_content_fingerprint() -> None:
    """The stored ``payload_sha256`` hashes ``<pubDate>`` too, so it always differs.

    #3049's scope proposed reusing ``payload_sha256`` to tell a re-serve from a real
    rollback. This is why that does not work, asserted rather than asserted-in-prose.
    """
    republished = _XML.replace(
        b"<pubDate>Fri, 07 Aug 2026 15:00:00 GMT</pubDate>",
        b"<pubDate>Fri, 07 Aug 2026 14:59:39 GMT</pubDate>",
    )
    first = parse_halt_rss(_XML)
    second = parse_halt_rss(republished)
    assert first.source_pub_at != second.source_pub_at
    assert first.payload_sha256 != second.payload_sha256
    assert first.content_sha256 == second.content_sha256


def test_content_fingerprint_moves_on_a_resumption() -> None:
    """A resumption is a content change, so the fingerprint must not be stamp-blind."""
    resumed = _XML.replace(
        b"""<ndaq:ResumptionDate />
      <ndaq:ResumptionTradeTime />""",
        b"""<ndaq:ResumptionDate>08/07/2026</ndaq:ResumptionDate>
      <ndaq:ResumptionTradeTime>10:30:00.000</ndaq:ResumptionTradeTime>""",
    )
    assert parse_halt_rss(resumed).content_sha256 != parse_halt_rss(_XML).content_sha256


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


@pytest.mark.integration
@pytest.mark.parametrize(
    ("instrument_symbol", "expected_feed_symbol"),
    [
        ("AAPL.RTH", "AAPL"),
        ("AAPL.24-7", "AAPL"),
        ("ABT.US", "ABT"),
        ("TCOM.CH", "TCOM"),
        ("brk.b", "BRK.B"),
        ("BF.A", "BF.A"),
        ("ACLX.CVR", "ACLX.CVR"),
        ("BBBY.WS", "BBBY.WS"),
        ("ATH.old", "ATH.OLD"),
    ],
)
def test_halt_identity_strips_only_measured_etoro_venue_suffixes(
    ebull_test_conn: psycopg.Connection[Any],
    instrument_symbol: str,
    expected_feed_symbol: str,
) -> None:
    row = ebull_test_conn.execute(
        f"SELECT {INSTRUMENT_HALT_SYMBOL_SQL} FROM (VALUES (%s::text)) AS i(symbol)",
        (instrument_symbol,),
    ).fetchone()
    assert row == (expected_feed_symbol,)
