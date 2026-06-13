"""DB integration test for the trade_events partial-unique dedup (#1593).

ONE test file for the one genuinely-new SQL mechanism: the two partial
unique indexes + ON CONFLICT DO NOTHING idempotency, and the anomaly
counter on a conflicting re-observation. Everything else is pure-logic
in tests/test_trade_events.py.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.providers.broker import BrokerClosedTrade
from app.services.trade_events import (
    TradeEventCounters,
    compute_history_min_date,
    events_from_history,
    ingest_trade_events,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401  (fixture)

pytestmark = pytest.mark.integration


def _seed_instrument(conn: psycopg.Connection[Any], iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _trade(position_id: int, units: str, close_at: datetime) -> BrokerClosedTrade:
    return BrokerClosedTrade(
        position_id=position_id,
        instrument_id=4077,
        is_buy=True,
        units=Decimal(units),
        open_rate=Decimal("97.3"),
        open_timestamp=datetime(2025, 8, 12, 16, 47, tzinfo=UTC),
        close_rate=Decimal("120.56"),
        close_timestamp=close_at,
        net_profit=Decimal("100"),
        fees=Decimal("0"),
        investment=Decimal("973"),
        initial_investment=Decimal("973"),
        leverage=1,
        order_id=1,
        social_trade_id=0,
        parent_position_id=0,
        raw_payload={"positionId": position_id},
    )


def test_reingest_is_idempotent_and_anomaly_is_loud(
    ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, 4077, "ILMN")
    close_at = datetime(2025, 11, 14, 19, 24, 35, tzinfo=UTC)
    trades = [_trade(900001, "82.135523", close_at)]

    # First ingest: open + close land, instrument resolved.
    first = ingest_trade_events(conn, events_from_history(trades, TradeEventCounters()), TradeEventCounters())
    assert first.inserted == 2
    assert first.duplicate == 0
    assert first.unresolved_instrument == 0

    # Second ingest of the SAME rows: pure no-op via the partial uniques.
    second = ingest_trade_events(conn, events_from_history(trades, TradeEventCounters()), TradeEventCounters())
    assert second.inserted == 0
    assert second.duplicate == 2
    assert second.conflict_anomaly == 0
    with conn.cursor() as cur:
        count = cur.execute("SELECT COUNT(*) FROM trade_events WHERE position_id = 900001").fetchone()
    assert count is not None and count[0] == 2

    # Conflicting re-observation (same keys, different units): first
    # observation kept, anomaly counted — never silently merged.
    conflicting = [_trade(900001, "50", close_at)]
    third = ingest_trade_events(conn, events_from_history(conflicting, TradeEventCounters()), TradeEventCounters())
    assert third.inserted == 0
    assert third.conflict_anomaly >= 1
    with conn.cursor() as cur:
        row = cur.execute(
            "SELECT units FROM trade_events WHERE position_id = 900001 AND event_kind = 'close'"
        ).fetchone()
    assert row is not None and Decimal(str(row[0])) == Decimal("82.135523")

    # Unresolved instrument: row still lands with NULL FK (no silent drops).
    ghost = [replace(_trade(900002, "1", close_at), instrument_id=999999991)]
    fourth = ingest_trade_events(conn, events_from_history(ghost, TradeEventCounters()), TradeEventCounters())
    assert fourth.inserted == 2
    assert fourth.unresolved_instrument == 2
    with conn.cursor() as cur:
        row = cur.execute(
            "SELECT instrument_id, etoro_instrument_id FROM trade_events "
            "WHERE position_id = 900002 AND event_kind = 'close'"
        ).fetchone()
    assert row is not None and row[0] is None and row[1] == 999999991

    # Watermark derives from the ingested closes.
    assert compute_history_min_date(conn) == close_at - timedelta(days=7)
