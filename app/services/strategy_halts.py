"""Bounded ingestion of Nasdaq Trader's free, primary-source halt RSS feed.

The feed is a safety observation, never trading authority. A missing, stale or
malformed snapshot makes the paper executor refuse the signal. Successful
polls update one feed-state row and upsert provider-native halt identities;
rows older than 90 days are removed by the same transaction.
"""

from __future__ import annotations

import hashlib
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from types import MappingProxyType
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import psycopg
from psycopg.pq import TransactionStatus

SOURCE = "nasdaq_trader_rss"
NASDAQ_HALT_RSS_URL = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
_NDAQ = "{http://www.nasdaqtrader.com/}"
_NY = ZoneInfo("America/New_York")
_MAX_BYTES = 2_000_000
_MAX_ITEMS = 5_000
_RETENTION_DAYS = 90
_MAX_SOURCE_LAG = timedelta(minutes=5)


class HaltFeedError(ValueError):
    """The source payload cannot safely establish current halt state."""


@dataclass(frozen=True)
class MarketHalt:
    symbol: str
    halt_at: datetime
    market: str
    reason_code: str
    resumed_at: datetime | None


@dataclass(frozen=True)
class HaltSnapshot:
    source_pub_at: datetime
    payload_sha256: str
    halts: tuple[MarketHalt, ...]


def _text(item: ET.Element, name: str, *, required: bool = False) -> str | None:
    node = item.find(f"{_NDAQ}{name}")
    value = node.text.strip() if node is not None and node.text else ""
    if required and not value:
        raise HaltFeedError(f"halt item is missing {name}")
    return value or None


def _market_time(date_value: str, time_value: str) -> datetime:
    raw = f"{date_value} {time_value}"
    local: datetime | None = None
    for format_string in ("%m/%d/%Y %H:%M:%S.%f", "%m/%d/%Y %H:%M:%S"):
        try:
            local = datetime.strptime(raw, format_string).replace(tzinfo=_NY)
            break
        except ValueError:
            continue
    if local is None:
        raise HaltFeedError("halt feed contains an invalid market timestamp")
    return local.astimezone(UTC)


def parse_halt_rss(payload: bytes) -> HaltSnapshot:
    """Parse the documented RSS fields without evaluating embedded HTML."""
    if not payload or len(payload) > _MAX_BYTES:
        raise HaltFeedError("halt feed payload is empty or exceeds the byte limit")
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as exc:
        raise HaltFeedError("halt feed is not valid XML") from exc
    channel = root.find("channel")
    if root.tag != "rss" or channel is None:
        raise HaltFeedError("halt feed is not an RSS channel")
    pub_text = channel.findtext("pubDate")
    if not pub_text:
        raise HaltFeedError("halt feed is missing pubDate")
    try:
        source_pub_at = parsedate_to_datetime(pub_text).astimezone(UTC)
    except (TypeError, ValueError) as exc:
        raise HaltFeedError("halt feed pubDate is invalid") from exc
    items = channel.findall("item")
    if len(items) > _MAX_ITEMS:
        raise HaltFeedError("halt feed exceeds the item limit")
    declared = channel.findtext(f"{_NDAQ}numItems")
    if declared is None or not declared.isdigit() or int(declared) != len(items):
        raise HaltFeedError("halt feed item count does not match numItems")

    rows: list[MarketHalt] = []
    identities: set[tuple[str, datetime]] = set()
    for item in items:
        symbol = _text(item, "IssueSymbol", required=True)
        halt_date = _text(item, "HaltDate", required=True)
        halt_time = _text(item, "HaltTime", required=True)
        market = _text(item, "Market", required=True)
        reason = _text(item, "ReasonCode", required=True)
        assert symbol and halt_date and halt_time and market and reason
        halt_at = _market_time(halt_date, halt_time)
        resume_date = _text(item, "ResumptionDate")
        resume_time = _text(item, "ResumptionTradeTime")
        # The live feed populates ResumptionDate on some active halts before a
        # trade-resumption time exists.  A time without a date is malformed;
        # a date without a time is still an active halt.
        if resume_time is not None and resume_date is None:
            raise HaltFeedError("halt feed has an incomplete resumption timestamp")
        resumed_at = _market_time(resume_date, resume_time) if resume_date and resume_time else None
        if resumed_at is not None and resumed_at < halt_at:
            raise HaltFeedError("halt resumption precedes its halt")
        identity = (symbol.upper(), halt_at)
        if identity in identities:
            raise HaltFeedError("halt feed contains duplicate provider identity")
        identities.add(identity)
        rows.append(MarketHalt(symbol.upper(), halt_at, market, reason, resumed_at))
    return HaltSnapshot(
        source_pub_at=source_pub_at,
        payload_sha256=hashlib.sha256(payload).hexdigest(),
        halts=tuple(rows),
    )


def store_halt_snapshot(
    conn: psycopg.Connection[Any],
    *,
    snapshot: HaltSnapshot,
    fetched_at: datetime,
) -> int:
    """Upsert one snapshot and enforce the 90-day storage boundary."""
    if fetched_at.tzinfo is None:
        raise HaltFeedError("fetched_at must be timezone-aware")
    if snapshot.source_pub_at > fetched_at + timedelta(minutes=5):
        raise HaltFeedError("halt feed pubDate is implausibly in the future")
    if snapshot.source_pub_at < fetched_at - _MAX_SOURCE_LAG:
        raise HaltFeedError("halt feed pubDate is stale")
    current = conn.execute(
        "SELECT source_pub_at FROM strategy_halt_feed_state WHERE source = %s FOR UPDATE",
        (SOURCE,),
    ).fetchone()
    if current is not None and snapshot.source_pub_at < current[0]:
        raise HaltFeedError("halt feed publication time regressed")
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO strategy_market_halts (
                source, symbol, halt_at, market, reason_code, resumed_at, observed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source, symbol, halt_at) DO UPDATE SET
                market = EXCLUDED.market,
                reason_code = EXCLUDED.reason_code,
                resumed_at = EXCLUDED.resumed_at,
                observed_at = EXCLUDED.observed_at
            """,
            [
                (SOURCE, row.symbol, row.halt_at, row.market, row.reason_code, row.resumed_at, fetched_at)
                for row in snapshot.halts
            ],
        )
    conn.execute(
        """
        INSERT INTO strategy_halt_feed_state (
            source, fetched_at, source_pub_at, item_count, payload_sha256
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (source) DO UPDATE SET
            fetched_at = EXCLUDED.fetched_at,
            source_pub_at = EXCLUDED.source_pub_at,
            item_count = EXCLUDED.item_count,
            payload_sha256 = EXCLUDED.payload_sha256
        """,
        (SOURCE, fetched_at, snapshot.source_pub_at, len(snapshot.halts), snapshot.payload_sha256),
    )
    deleted = conn.execute(
        "DELETE FROM strategy_market_halts WHERE resumed_at IS NOT NULL AND halt_at < %s RETURNING 1",
        (fetched_at - timedelta(days=_RETENTION_DAYS),),
    ).fetchall()
    return len(deleted)


def active_halt_symbols(snapshot: HaltSnapshot) -> MappingProxyType[str, MarketHalt]:
    """Expose the current active rows for pure tests/callers."""
    return MappingProxyType({row.symbol: row for row in snapshot.halts if row.resumed_at is None})


def refresh_halt_feed(
    conn: psycopg.Connection[Any],
    *,
    client: httpx.Client,
    fetched_at: datetime | None = None,
) -> HaltSnapshot:
    """Fetch then atomically store one feed without holding a DB transaction."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise HaltFeedError("halt refresh requires an idle database connection")
    observed = (fetched_at or datetime.now(UTC)).astimezone(UTC)
    snapshot = fetch_halt_snapshot(client)
    with conn.transaction():
        store_halt_snapshot(conn, snapshot=snapshot, fetched_at=observed)
    return snapshot


def fetch_halt_snapshot(client: httpx.Client) -> HaltSnapshot:
    """Fetch and parse without requiring or holding a database connection."""
    try:
        response = client.get(NASDAQ_HALT_RSS_URL, timeout=20.0)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise HaltFeedError("Nasdaq halt feed request failed") from exc
    return parse_halt_rss(response.content)


__all__ = [
    "HaltFeedError",
    "HaltSnapshot",
    "MarketHalt",
    "NASDAQ_HALT_RSS_URL",
    "SOURCE",
    "active_halt_symbols",
    "fetch_halt_snapshot",
    "parse_halt_rss",
    "refresh_halt_feed",
    "store_halt_snapshot",
]
