"""Assert an entry's #3542 trade ticket against the preflight its path wrote."""

from __future__ import annotations

from typing import Any

import psycopg

from app.services.strategy_entry_ticket import format_rate


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
