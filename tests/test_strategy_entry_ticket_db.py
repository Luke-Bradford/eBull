"""#3542: the ticket table refuses an unstated field (slice 1); an entry cannot commit without its
ticket, and neither the ticket nor its link can be rewritten (slice 2, sql/459)."""

from __future__ import annotations

from typing import Any, LiteralString

import psycopg
import pytest
from psycopg import sql

from tests.fixtures.entry_ticket import seed_entry_ticket
from tests.test_strategy_order_reconciliation import _seed_deployment, _seed_funded_trade, _seed_order

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("registered_strategy_test_candidates")]

_VALID: dict[str, object] = {
    "rationale_class": "signal",
    "rule_id": "test-rule",
    "evidence_kind": "strategy_promotion",
    "evidence_id": 1,
    "why_now": "test entry",
    "exit_rule": "not_applicable: passive hold",
    "expected_cost_usd": "0",
    "cost_basis": "test",
}


def _new_order(conn: psycopg.Connection[Any], trade_id: int) -> int:
    row = conn.execute(
        """
        INSERT INTO orders (instrument_id, action, order_type, requested_amount, status, execution_origin)
        SELECT instrument_id, 'BUY', 'MARKET', 100, 'submitted', 'strategy'
        FROM strategy_trades WHERE strategy_trade_id = %s
        RETURNING order_id
        """,
        (trade_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _insert_ticket(conn: psycopg.Connection[Any], order_id: int, trade_id: int, **override: str) -> None:
    values = {**_VALID, **override, "order_id": order_id, "strategy_trade_id": trade_id}
    conn.execute(
        sql.SQL("INSERT INTO strategy_entry_tickets ({}) VALUES ({})").format(
            sql.SQL(", ").join(sql.Identifier(c) for c in values),
            sql.SQL(", ").join(sql.Placeholder() for _ in values),
        ),
        tuple(values.values()),
    )


def _link(conn: psycopg.Connection[Any], trade_id: int, order_id: int, purpose: str) -> None:
    conn.execute(
        "INSERT INTO strategy_trade_orders (strategy_trade_id, order_id, purpose) VALUES (%s, %s, %s)",
        (trade_id, order_id, purpose),
    )


@pytest.fixture
def deployment_id(ebull_test_conn: psycopg.Connection[Any]) -> int:
    deployment = _seed_deployment(ebull_test_conn)
    ebull_test_conn.commit()
    return deployment


def _unlinked_trade(conn: psycopg.Connection[Any], deployment_id: int, instrument_id: int) -> int:
    trade_id = _seed_funded_trade(conn, deployment_id=deployment_id, instrument_id=instrument_id)
    conn.commit()
    return trade_id


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
    ebull_test_conn: psycopg.Connection[Any], deployment_id: int, column: str, value: str
) -> None:
    conn = ebull_test_conn
    trade_id = _unlinked_trade(conn, deployment_id, 2458001)
    # A passive hold may say why it has no exit. No link, so nothing is enforced at commit.
    with conn.transaction():
        _insert_ticket(conn, _new_order(conn, trade_id), trade_id)
    with pytest.raises(psycopg.errors.CheckViolation), conn.transaction():
        _insert_ticket(conn, _new_order(conn, trade_id), trade_id, **{column: value})


def test_an_entry_link_without_its_ticket_cannot_commit(
    ebull_test_conn: psycopg.Connection[Any], deployment_id: int
) -> None:
    conn = ebull_test_conn
    trade_id = _unlinked_trade(conn, deployment_id, 2458002)
    _link(conn, trade_id, _new_order(conn, trade_id), "entry")
    with pytest.raises(psycopg.errors.RaiseException, match="has no trade ticket"):
        conn.commit()
    conn.rollback()


def test_a_ticket_naming_another_trade_does_not_satisfy_the_link(
    ebull_test_conn: psycopg.Connection[Any], deployment_id: int
) -> None:
    conn = ebull_test_conn
    trade_id = _unlinked_trade(conn, deployment_id, 2458003)
    other_trade_id = _unlinked_trade(conn, deployment_id, 2458004)
    order_id = _new_order(conn, trade_id)
    _link(conn, trade_id, order_id, "entry")
    _insert_ticket(conn, order_id, other_trade_id)
    with pytest.raises(psycopg.errors.RaiseException, match="has no trade ticket"):
        conn.commit()
    conn.rollback()


def test_a_ticket_written_after_its_link_commits(ebull_test_conn: psycopg.Connection[Any], deployment_id: int) -> None:
    conn = ebull_test_conn
    trade_id = _unlinked_trade(conn, deployment_id, 2458005)
    order_id = _new_order(conn, trade_id)
    _link(conn, trade_id, order_id, "entry")
    seed_entry_ticket(conn, order_id, trade_id)
    conn.commit()


def test_an_exit_link_needs_no_ticket(ebull_test_conn: psycopg.Connection[Any], deployment_id: int) -> None:
    conn = ebull_test_conn
    trade_id, _entry_order = _seed_order(conn, deployment_id=deployment_id, instrument_id=2458006)
    _link(conn, trade_id, _new_order(conn, trade_id), "exit")
    conn.commit()


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE strategy_entry_tickets SET why_now = 'rewritten' WHERE strategy_trade_id = %s",
        "DELETE FROM strategy_entry_tickets WHERE strategy_trade_id = %s",
    ],
)
def test_a_ticket_is_stated_once(
    ebull_test_conn: psycopg.Connection[Any], deployment_id: int, statement: LiteralString
) -> None:
    conn = ebull_test_conn
    trade_id, _order = _seed_order(conn, deployment_id=deployment_id, instrument_id=2458007)
    conn.commit()
    with pytest.raises(psycopg.errors.RaiseException, match="a ticket is stated once"), conn.transaction():
        conn.execute(statement, (trade_id,))


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE strategy_trade_orders SET purpose = 'exit' WHERE strategy_trade_id = %s",
        "UPDATE strategy_trade_orders SET order_id = order_id + 1000000 WHERE strategy_trade_id = %s",
        "UPDATE strategy_trade_orders SET strategy_trade_id = strategy_trade_id + 1000000 WHERE strategy_trade_id = %s",
        "DELETE FROM strategy_trade_orders WHERE strategy_trade_id = %s",
    ],
)
def test_a_link_is_immutable(
    ebull_test_conn: psycopg.Connection[Any], deployment_id: int, statement: LiteralString
) -> None:
    conn = ebull_test_conn
    trade_id, _order = _seed_order(conn, deployment_id=deployment_id, instrument_id=2458008)
    conn.commit()
    with pytest.raises(psycopg.errors.RaiseException, match="immutable once written"), conn.transaction():
        conn.execute(statement, (trade_id,))
    # Naming a column without changing it is not a rewrite.
    conn.execute("UPDATE strategy_trade_orders SET purpose = purpose WHERE strategy_trade_id = %s", (trade_id,))
