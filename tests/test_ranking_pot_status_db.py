"""#2842 slice 7 — the page reader over a real executed book: held and recent positions, tickets, P&L, rebalances."""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services import ranking_pot_status as status
from app.services.strategy_control_plane import configure_deployment
from tests.test_ranking_pot_exec_db import _decide_month, _fund_open, _next_month_session, _universes
from tests.test_ranking_pot_job_db import _first_window
from tests.test_ranking_pot_schema_db import POT, _frozen, _move

# #3610: freezes a claim with no TrialDesign while testing something else (tests/conftest.py).
pytestmark = pytest.mark.usefixtures("assume_trial_powered")

Conn = psycopg.Connection[Any]


def _trade(conn: Conn, lifecycle_id: int) -> int:
    row = conn.execute(
        "SELECT t.strategy_trade_id FROM ranking_pot_exec_lifecycles l "
        "JOIN strategy_funding_decisions fd ON fd.signal_id = l.signal_id "
        "JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id WHERE l.lifecycle_id = %s",
        (lifecycle_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_status_shows_positions_with_tickets_pnl_and_rebalances(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    empty = status.load_status(conn)
    assert (empty.declaration, empty.held, empty.build_complete) == (None, (), True)
    assert [j.job_name for j in empty.jobs] == ["ranking_pot_rebalance", "ranking_pot_step", "ranking_pot_execute"]
    assert status.load_readout(conn).reason == "not_declared"

    decl_id = _frozen(conn)
    _move(conn, decl_id, "shadow_only", "executing", "supervisor")
    deployment = configure_deployment(
        conn,
        strategy_id=POT,
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal(1000),
        enabled=True,
        changed_by="operator",
        reason="#2842 test",
    ).deployment_id
    conn.commit()
    conn.autocommit = True
    d, _ = _first_window(conn)

    # Month 1: 2842 (slot 1) and 2843 (slot 2) enter; 2842 fills and is marked, 2843 is not yet submitted.
    _decide_month(conn, monkeypatch, d, _universes({2842: "0.9", 2843: "0.8"}, {2842, 2843}))
    lc1, lc2 = (r[0] for r in conn.execute("SELECT lifecycle_id FROM ranking_pot_exec_lifecycles ORDER BY 1"))
    _fund_open(conn, lc1, deployment)
    trade = _trade(conn, lc1)
    conn.execute(
        "INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id, status) "
        "VALUES (%s, 98001, 'active')",
        (trade,),
    )
    for day, currency in ((d, 1), (_next_month_session(d), 1), (_next_month_session(d) + timedelta(days=1), None)):
        conn.execute(
            "INSERT INTO broker_account_equity_snapshots (environment, snapshot_date, observed_at, source_version, "
            " account_currency_id, currency, available_cash, total_invested, unrealised_pnl, equity) "
            "VALUES ('demo', %s, %s, 'etoro-pnl-v1', %s, 'USD', 500, 400, 100, 1000)",
            (day, datetime.combine(day, time(21), UTC), currency),
        )
    conn.execute(
        "INSERT INTO broker_account_position_marks (environment, snapshot_date, position_id, instrument_id, is_buy, "
        " units, amount, unrealized_pnl, market_value, is_partially_altered) VALUES "
        "('demo', %s, 98001, 2842, TRUE, 1, 100, 9, 109, FALSE), ('demo', %s, 98001, 2842, TRUE, 1, 100, 12.5, 112.5, "
        "FALSE), ('demo', %s, 98001, 2842, TRUE, 1, 100, 99, 199, FALSE)",
        (d, _next_month_session(d), _next_month_session(d) + timedelta(days=1)),
    )

    s = status.load_status(conn, now=datetime.combine(d, time(23, 45), UTC))
    assert s.declaration is not None
    assert (s.declaration.declaration_id, s.declaration.state, s.declaration.pot_capital) == (
        decl_id,
        "executing",
        Decimal(1000),
    )
    assert [(p.lifecycle_id, p.slot, p.symbol, p.status) for p in s.held] == [
        (lc1, 1, "POT2842", "open"),
        (lc2, 2, "POT2843", "entry_pending"),
    ]
    first = s.held[0]
    assert first.ticket_verified and first.ticket["rule_id"] == "ranking-pot-v1:enter-frank-le-N"
    assert first.ticket["r_rank"] == 1 and first.ticket["score"]["families"] == {"quality": "0.5"}
    # The latest mark from an observed-USD snapshot (the later one's currency is assumed), dated; nothing booked.
    assert (first.unrealized_pnl_usd, first.marked_on, first.realized_pnl_usd) == (
        Decimal("12.5"),
        _next_month_session(d),
        None,
    )
    assert s.held[1].unrealized_pnl_usd is None and s.held[1].amount is None
    assert [(r.outcome, r.executed_state, r.entries_allowed, r.v1_active) for r in s.rebalances] == [
        ("decided", "executing", True, False)
    ]
    assert status.load_readout(conn).reason == "not_stepped"

    # Month 2: 2842 leaves R (stamped), then closes with a booked profit; it moves to `recent` with its exit.
    d2 = _next_month_session(d)
    _decide_month(conn, monkeypatch, d2, _universes({2843: "0.8"}, {2843}))
    conn.execute("UPDATE strategy_trades SET status = 'closed' WHERE strategy_trade_id = %s", (trade,))
    conn.execute(
        "UPDATE strategy_position_ownership SET status = 'released', released_at = now(), release_reason = 'closed' "
        "WHERE strategy_trade_id = %s",
        (trade,),
    )
    conn.execute(
        "INSERT INTO trade_events (position_id, etoro_instrument_id, instrument_id, event_kind, side, units, price, "
        " executed_at, fees_usd, realized_pnl_usd, source, raw_payload) "
        "VALUES (98001, 2842, 2842, 'close', 'sell', 1, 107.25, now(), 0, 7.25, 'etoro_history', '{}')"
    )

    s2 = status.load_status(conn, now=datetime.combine(d2, time(23, 45), UTC))
    closed = next(p for p in s2.recent if p.lifecycle_id == lc1)
    assert (closed.status, closed.exit_reason, closed.realized_pnl_usd, closed.unrealized_pnl_usd) == (
        "closed",
        "ineligible:not_tradable",
        Decimal("7.25"),
        None,
    )
    # 2843's month-1 lifecycle expired (never submitted); its month-2 re-entry holds a slot.
    assert next(p for p in s2.recent if p.lifecycle_id == lc2).status == "expired"
    assert [p.symbol for p in s2.held] == ["POT2843"]
    assert [r.target_session for r in s2.rebalances] == sorted((r.target_session for r in s2.rebalances), reverse=True)


def test_a_completed_declaration_keeps_its_page(ebull_test_conn: Conn) -> None:
    """Codex ckpt-2: ``rb.load_declaration`` returns only a live declaration; the page falls back to the newest
    completed one rather than reading as never frozen."""
    conn = ebull_test_conn
    decl_id = _frozen(conn)
    _move(conn, decl_id, "shadow_only", "winding_down", "supervisor", wind_down="operator")
    _move(conn, decl_id, "winding_down", "completed", "engine")
    s = status.load_status(conn)
    assert s.declaration is not None
    assert (s.declaration.declaration_id, s.declaration.state) == (decl_id, "completed")
    assert status.load_readout(conn).reason == "not_stepped"
