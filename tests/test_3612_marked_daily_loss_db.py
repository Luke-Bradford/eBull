"""#3612 slice 2 — ``observe_daily_marked_pnl``'s one-statement read against a real database.

The case logic is tested pure in ``test_3612_marked_daily_loss``; this pins the SQL: the anchor, the
engine book (ownership claimed after the snapshot included) and its ledger, read and decoded together.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, time, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
import pytest

from app.services.engine_pot_risk import daily_anchor_session, observe_daily_marked_pnl
from app.services.strategy_engine_capital import EngineCapitalObservationError
from tests.test_engine_pot_risk_db import _held, _snapshot
from tests.test_strategy_paper_executor import _NOW
from tests.test_strategy_position_manager import _POSITION_ID, _opened_trade

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("registered_strategy_test_candidates")]

S_AT = datetime.combine(daily_anchor_session(_NOW), time(16, 5), tzinfo=ZoneInfo("America/New_York"))


def _event(conn: psycopg.Connection[Any], kind: str, at: datetime, units: str = "1") -> None:
    conn.execute(
        """
        INSERT INTO trade_events (
            position_id,etoro_instrument_id,instrument_id,event_kind,side,executed_at,units,price,
            realized_pnl_usd,source,raw_payload
        ) VALUES (%s,2449001,2449001,%s,%s,%s,%s,100,%s,'etoro_sync','{}')
        """,
        (
            _POSITION_ID,
            kind,
            "buy" if kind == "open" else "sell",
            at,
            Decimal(units),
            Decimal("0") if kind == "close" else None,
        ),
    )


def _anchor(conn: psycopg.Connection[Any], *, pnl: str) -> None:
    conn.execute(
        """
        INSERT INTO broker_account_session_close_snapshots (
            environment, session_date, observed_at, account_currency_id, currency,
            official_direct_long_positions, official_direct_short_positions
        ) VALUES ('demo', %s, %s, 1, 'USD', 1, 0)
        """,
        (daily_anchor_session(_NOW), S_AT),
    )
    conn.execute(
        """
        INSERT INTO broker_account_session_close_marks (
            environment, session_date, position_id, instrument_id, is_buy, units, amount, unrealized_pnl,
            market_value, is_partially_altered, close_rate, close_conversion_rate, asset_currency_id,
            pnl_timestamp, total_fees
        ) VALUES ('demo', %s, %s, 2449001, true, 1, 100, %s, %s, false, 100, 1, 1, %s, 0)
        """,
        (
            daily_anchor_session(_NOW),
            _POSITION_ID,
            Decimal(pnl),
            Decimal("100") + Decimal(pnl),
            S_AT - timedelta(seconds=2),
        ),
    )


def test_the_marked_figure_reads_anchor_book_and_ledger_in_one_statement(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _opened_trade(conn, monkeypatch)
    conn.execute("UPDATE strategy_position_ownership SET claimed_at=%s", (_NOW - timedelta(days=2),))
    conn.execute("DELETE FROM trade_events WHERE position_id=%s", (_POSITION_ID,))
    # The executor fixture seeds an empty anchor; S is append-only, so clear it as the harness does.
    with conn.transaction():
        conn.execute("SET LOCAL session_replication_role = replica")
        conn.execute("DELETE FROM broker_account_session_close_snapshots")
    _event(conn, "open", _NOW - timedelta(days=2, minutes=1))
    live = replace(_held(_POSITION_ID, "-20"), pnl_timestamp=_NOW - timedelta(seconds=1), total_fees=Decimal("0"))
    snapshot = _snapshot(live)

    with pytest.raises(EngineCapitalObservationError) as missing:
        observe_daily_marked_pnl(conn, snapshot, decided_at=_NOW, max_mark_age_seconds=60)
    assert missing.value.reason_code == "engine_pot_daily_anchor_missing"

    _anchor(conn, pnl="5")
    assert observe_daily_marked_pnl(conn, snapshot, decided_at=_NOW, max_mark_age_seconds=60) == Decimal("-25")

    # A close recorded after the snapshot but before the decision is read through `decided_at`.
    _event(conn, "close", _NOW + timedelta(seconds=2), "0.5")
    with pytest.raises(EngineCapitalObservationError) as vetoed:
        observe_daily_marked_pnl(conn, snapshot, decided_at=_NOW + timedelta(seconds=3), max_mark_age_seconds=60)
    assert vetoed.value.reason_code == "engine_pot_daily_close_in_window"
