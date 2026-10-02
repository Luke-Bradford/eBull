"""#3543: one engine-book risk snapshot per session, append-only (sql/460)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services.engine_book_risk import ENGINE_BOOK_RISK_POLICY, run_engine_book_risk_snapshot
from app.services.engine_book_risk_status import load_engine_book_risk_status
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


def test_the_read_model_shows_the_running_policy_newest_first(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """#3543 slice 2: the endpoint's read -- running policy only, newest first, symbols joined, flags named."""
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
    assert load_engine_book_risk_status(conn, now=_NOW).latest is None

    assert run_engine_book_risk_snapshot(conn, _NOW) is not None
    copy = """
        INSERT INTO engine_book_risk_snapshots
        SELECT (s.session_date + %s)::date, %s, s.measured_at, s.pool_event_id, s.capital_usd, s.gross_usd,
               s.position_count, s.instrument_count, s.open_trade_count, s.cost_marked_count, s.stale_count,
               s.largest_share_pct, s.top5_share_pct, s.hhi, s.hist_vol_pct, s.ewma_vol_pct, s.beta, s.vol_n_obs,
               s.beta_n_obs, s.sample_first, s.sample_last, s.history_status, s.beta_defaulted_count,
               s.beta_defaulted_weight_pct, s.stress_2020_pct, s.stress_2022_pct, %s::jsonb, %s::jsonb
        FROM engine_book_risk_snapshots s WHERE s.policy_version = %s
    """
    # A newer row under another policy version answers a different question: never shown.
    conn.execute(copy, (2, "engine-book-risk-v0", "{}", "[]", ENGINE_BOOK_RISK_POLICY))
    # A newer row under the running policy, with one flagged check and one position.
    conn.execute(
        copy,
        (
            1,
            ENGINE_BOOK_RISK_POLICY,
            '{"stale_marks": {"status": "evaluated", "flagged": true}, "x": {"status": "no_limit", "flagged": false}}',
            f'[{{"instrument_id": {_SPY_ID}}}]',
            ENGINE_BOOK_RISK_POLICY,
        ),
    )
    conn.commit()

    status = load_engine_book_risk_status(conn, now=_NOW)
    assert status.policy_version == ENGINE_BOOK_RISK_POLICY
    assert status.latest is not None and status.latest.session_date == session + timedelta(days=1)
    assert status.latest.positions == [{"instrument_id": _SPY_ID, "symbol": "SPY"}]
    assert [(r.session_date, r.flagged) for r in status.recent] == [
        (session + timedelta(days=1), ["stale_marks"]),
        (session, []),
    ]
