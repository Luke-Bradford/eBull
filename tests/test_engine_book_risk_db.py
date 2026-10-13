"""#3543: one engine-book risk snapshot per session, append-only (sql/460)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services.engine_book_risk import (
    ENGINE_BOOK_RISK_POLICY,
    EngineBookRiskRefusal,
    run_engine_book_risk_snapshot,
)
from app.services.engine_book_risk_status import load_engine_book_risk_status
from app.services.market_calendar import latest_completed_us_session
from app.services.strategy_control_plane import configure_paper_pool
from tests.test_strategy_paper_executor import _NOW as _EXECUTOR_NOW
from tests.test_strategy_position_manager import _POSITION_ID, _opened_trade

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("registered_strategy_test_candidates")]

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

    # sql/481: a v1 row predates the 2008 scenario and may omit it; any later policy must state it.
    copy_without_2008 = """
        INSERT INTO engine_book_risk_snapshots (
            session_date, policy_version, measured_at, pool_event_id, capital_usd, gross_usd, net_usd, position_count,
            instrument_count, open_trade_count, cost_marked_count, stale_count, vol_n_obs, beta_n_obs,
            hist_vol_pct, ewma_vol_pct, beta, history_status, beta_defaulted_count, beta_defaulted_weight_pct,
            stress_2020_pct, stress_2022_pct, checks, positions)
        SELECT session_date, %s, measured_at, pool_event_id, capital_usd, gross_usd, net_usd, position_count,
               instrument_count, open_trade_count, cost_marked_count, stale_count, vol_n_obs, beta_n_obs,
               hist_vol_pct, ewma_vol_pct, beta, history_status, beta_defaulted_count, beta_defaulted_weight_pct,
               stress_2020_pct, stress_2022_pct, checks, positions
        FROM engine_book_risk_snapshots WHERE policy_version = %s
    """
    conn.execute(copy_without_2008, ("engine-book-risk-v1", ENGINE_BOOK_RISK_POLICY))
    with pytest.raises(psycopg.errors.CheckViolation, match="stress_2008_shape"), conn.transaction():
        conn.execute(copy_without_2008, ("engine-book-risk-v9", ENGINE_BOOK_RISK_POLICY))
    # sql/483: v1/v2 rows predate net_usd; any later policy must state it.
    copy_without_net = """
        INSERT INTO engine_book_risk_snapshots (
            session_date, policy_version, measured_at, pool_event_id, capital_usd, gross_usd, position_count,
            instrument_count, open_trade_count, cost_marked_count, stale_count, vol_n_obs, beta_n_obs,
            hist_vol_pct, ewma_vol_pct, beta, history_status, beta_defaulted_count, beta_defaulted_weight_pct,
            stress_2008_pct, stress_2020_pct, stress_2022_pct, checks, positions)
        SELECT session_date, %s, measured_at, pool_event_id, capital_usd, gross_usd, position_count,
               instrument_count, open_trade_count, cost_marked_count, stale_count, vol_n_obs, beta_n_obs,
               hist_vol_pct, ewma_vol_pct, beta, history_status, beta_defaulted_count, beta_defaulted_weight_pct,
               stress_2008_pct, stress_2020_pct, stress_2022_pct, checks, positions
        FROM engine_book_risk_snapshots WHERE policy_version = %s
    """
    conn.execute(copy_without_net, ("engine-book-risk-v2", ENGINE_BOOK_RISK_POLICY))
    with pytest.raises(psycopg.errors.CheckViolation, match="net_usd_shape"), conn.transaction():
        conn.execute(copy_without_net, ("engine-book-risk-v9", ENGINE_BOOK_RISK_POLICY))
    conn.commit()

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
    # No column list: positional over the table's column order. sql/481 appended stress_2008_pct after
    # `positions`, then sql/483 net_usd; every copied row's policy is post-v2 and carries both.
    copy = """
        INSERT INTO engine_book_risk_snapshots
        SELECT (s.session_date + %s)::date, %s, s.measured_at, s.pool_event_id, s.capital_usd, s.gross_usd,
               s.position_count, s.instrument_count, s.open_trade_count, s.cost_marked_count, s.stale_count,
               s.largest_share_pct, s.top5_share_pct, s.hhi, s.hist_vol_pct, s.ewma_vol_pct, s.beta, s.vol_n_obs,
               s.beta_n_obs, s.sample_first, s.sample_last, s.history_status, s.beta_defaulted_count,
               s.beta_defaulted_weight_pct, s.stress_2020_pct, s.stress_2022_pct, %s::jsonb, %s::jsonb,
               s.stress_2008_pct, s.net_usd
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


def test_a_short_is_measured_and_leverage_still_refuses(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """#3612 v3: an owned x1 short writes a row with a negative MV; a leveraged position writes none."""
    conn = ebull_test_conn
    _opened_trade(conn, monkeypatch)
    conn.execute("UPDATE strategy_position_ownership SET claimed_at=%s", (_EXECUTOR_NOW - timedelta(days=1),))
    measured_at = _EXECUTOR_NOW + timedelta(days=1)
    session = latest_completed_us_session(measured_at)
    instrument_id = conn.execute(
        "SELECT t.instrument_id FROM strategy_trades t JOIN strategy_position_ownership o "
        "ON o.strategy_trade_id = t.strategy_trade_id WHERE o.broker_position_id = %s",
        (_POSITION_ID,),
    ).fetchone()
    assert instrument_id is not None
    conn.execute("UPDATE instruments SET currency = 'USD' WHERE instrument_id = %s", (instrument_id[0],))
    conn.execute(
        """
        INSERT INTO broker_positions (
            position_id, instrument_id, is_buy, units, initial_units, amount, initial_amount_in_dollars,
            open_rate, open_conversion_rate, open_date_time, leverage, raw_payload)
        VALUES (%s, %s, false, 2, 2, 200, 200, 100, 1, %s, 1, '{}')
        ON CONFLICT (position_id) DO UPDATE SET is_buy = false, units = 2, initial_units = 2, leverage = 1
        """,
        (_POSITION_ID, instrument_id[0], _EXECUTOR_NOW),
    )
    conn.execute(
        "INSERT INTO price_daily (instrument_id, price_date, close) VALUES (%s, %s, 110) "
        "ON CONFLICT (instrument_id, price_date) DO UPDATE SET close = 110",
        (instrument_id[0], session),
    )
    _seed_spy(conn, session)
    conn.commit()

    snap = run_engine_book_risk_snapshot(conn, measured_at)
    assert snap is not None and snap.policy_version == "engine-book-risk-v3"
    assert [(p["side"], p["market_value_usd"]) for p in snap.positions] == [("short", "-220.00000000")]
    row = conn.execute(
        "SELECT gross_usd, net_usd FROM engine_book_risk_snapshots WHERE session_date = %s AND policy_version = %s",
        (session, ENGINE_BOOK_RISK_POLICY),
    ).fetchone()
    assert row == (Decimal("220.000000"), Decimal("-220.000000"))

    # The refusal precedes any write, so the stored v3 row needs no removal for the next run to reach it.
    conn.execute("UPDATE broker_positions SET leverage = 2 WHERE position_id = %s", (_POSITION_ID,))
    conn.commit()
    with pytest.raises(EngineBookRiskRefusal, match="book_shape_unsupported"):
        run_engine_book_risk_snapshot(conn, measured_at)
