"""#3542 trade tickets in tests: assert a path's ticket, or seed one for a hand-built entry link."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import psycopg

from app.services.strategy_entry_ticket import EntryTicket, format_rate, write_entry_ticket


def seed_entry_ticket(conn: psycopg.Connection[Any], order_id: int, strategy_trade_id: int) -> None:
    """A placeholder ticket for a test that links an entry by hand.

    sql/459 refuses to commit an entry link without a ticket for the same order and trade, so every
    fixture that inserts one must seed one in the same transaction.
    """
    write_entry_ticket(
        conn,
        EntryTicket(
            order_id=order_id,
            strategy_trade_id=strategy_trade_id,
            rationale_class="signal",
            rule_id="test-fixture",
            evidence_kind="strategy_promotion",
            evidence_id=1,
            why_now="test fixture entry",
            exit_rule="not_applicable: test fixture",
            expected_cost_usd=Decimal("0"),
            cost_basis="test_fixture",
        ),
    )


def assert_entry_ticket(
    conn: psycopg.Connection[Any], signal_id: int, *, rationale_class: str, evidence_kind: str, evidence_id: int
) -> None:
    row = conn.execute(
        """
        SELECT tk.rationale_class, tk.evidence_kind, tk.evidence_id, tk.expected_cost_usd, tk.cost_basis,
               tk.exit_rule, pf.stressed_cost_amount, pf.cost_basis, pf.stop_loss_rate, pf.take_profit_rate
        FROM strategy_funding_decisions fd
        JOIN strategy_entry_preflights pf ON pf.signal_id = fd.signal_id
        JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
        JOIN strategy_trade_orders sto ON sto.strategy_trade_id = t.strategy_trade_id AND sto.purpose = 'entry'
        JOIN strategy_entry_tickets tk ON tk.order_id = sto.order_id AND tk.strategy_trade_id = t.strategy_trade_id
        WHERE fd.signal_id = %s
        """,
        (signal_id,),
    ).fetchone()
    assert row is not None, f"signal {signal_id}: entry has no trade ticket"
    assert row[0:3] == (rationale_class, evidence_kind, evidence_id)
    # Cost and basis are the preflight's, never re-derived; the exit names the levels sent.
    assert (row[3], row[4]) == (row[6], row[7])
    assert f"stop loss {format_rate(row[8])} / take profit {format_rate(row[9])}" in row[5]
