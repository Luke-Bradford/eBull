"""#3542 slice 1: the ticket table refuses an unstated field."""

from __future__ import annotations

from typing import Any

import psycopg
import pytest
from psycopg import sql

from tests.test_strategy_position_manager import _opened_trade

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("registered_strategy_test_candidates")]


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("why_now", "   "),
        ("exit_rule", "not_applicable"),
        ("exit_rule", "not_applicable:   "),
        ("expected_cost_usd", "NaN"),
        ("expected_cost_usd", "-1"),
    ],
)
def test_an_unstated_field_is_refused(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch, column: str, value: str
) -> None:
    conn = ebull_test_conn
    trade_id, _deployment, _broker, _manual = _opened_trade(conn, monkeypatch)
    assert conn.execute(
        "SELECT count(*) FROM strategy_entry_tickets WHERE strategy_trade_id=%s", (trade_id,)
    ).fetchone() == (1,)
    # A passive hold may say why it has no exit.
    conn.execute(
        "UPDATE strategy_entry_tickets SET exit_rule='not_applicable: passive hold' WHERE strategy_trade_id=%s",
        (trade_id,),
    )
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            sql.SQL("UPDATE strategy_entry_tickets SET {} = %s WHERE strategy_trade_id = %s").format(
                sql.Identifier(column)
            ),
            (value, trade_id),
        )
