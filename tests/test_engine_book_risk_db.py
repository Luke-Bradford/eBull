"""#3543: one engine-book risk snapshot per session, append-only (sql/460)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services.engine_book_risk import ENGINE_BOOK_RISK_POLICY, run_engine_book_risk_snapshot
from app.services.market_calendar import latest_completed_us_session
from app.services.strategy_control_plane import configure_paper_pool

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 10, 2, 9, tzinfo=UTC)
_SPY_ID = 954300


def _seed_spy(conn: psycopg.Connection[Any], session: date) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable, currency) "
        "VALUES (%s, 'SPY', 'SPDR S&P 500', true, 'USD')",
        (_SPY_ID,),
    )
    for k in range(300):
        conn.execute(
            "INSERT INTO price_daily (instrument_id, price_date, close) VALUES (%s, %s, %s)",
            (_SPY_ID, session - timedelta(days=k), Decimal("500") + (k % 5)),
        )


def test_an_empty_book_writes_one_append_only_row(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    session = latest_completed_us_session(_NOW)
    configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("10000"),
        risk_profile="growth",
        approval_mode="manual",
        max_concurrent_positions_override=None,
        changed_by="test",
        reason="#3543 fixture",
    )
    _seed_spy(conn, session)
    conn.commit()

    snap = run_engine_book_risk_snapshot(conn, _NOW)
    assert snap is not None and snap.history_status == "empty_book"
    # snapshot_read commits on exit, so the writer's transaction is top-level and committed.
    assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
    row = conn.execute(
        "SELECT capital_usd, gross_usd, history_status, checks->'stale_marks'->>'flagged' "
        "FROM engine_book_risk_snapshots WHERE session_date = %s AND policy_version = %s",
        (session, ENGINE_BOOK_RISK_POLICY),
    ).fetchone()
    assert row == (Decimal("10000.000000"), Decimal("0E-6"), "empty_book", "false")

    # A second run for the session is a no-op; the row cannot be rewritten or removed.
    assert run_engine_book_risk_snapshot(conn, _NOW) is None
    for statement in (
        "UPDATE engine_book_risk_snapshots SET gross_usd = 1",
        "DELETE FROM engine_book_risk_snapshots",
    ):
        with pytest.raises(psycopg.errors.RaiseException, match="stated once"), conn.transaction():
            conn.execute(statement)  # type: ignore[arg-type]
