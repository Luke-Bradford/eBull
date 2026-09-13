"""#2451 crash-safe strategy order reconciliation integration tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from app.providers.broker import (
    BrokerOrderDetail,
    BrokerOrderLookupError,
    BrokerOrderNotFound,
    BrokerPositionExecution,
    BrokerProvider,
)
from app.services.strategy_control_plane import (
    StrategyControlError,
    configure_deployment,
    create_strategy_trade,
    decide_funding,
    link_strategy_order,
)
from app.services.strategy_order_reconciliation import (
    enforce_reconciliation_slo,
    ensure_strategy_request_id,
    reconcile_backlog,
    reconcile_strategy_order,
)

pytestmark = pytest.mark.integration


def _seed_deployment(conn: psycopg.Connection[Any], *, capital_limit: Decimal = Decimal("1000")) -> int:
    """Promote ``S-REC`` to paper and return one funded deployment id.

    ⚠ Callers MUST request the ``registered_strategy_test_candidates``
    fixture. ``configure_deployment`` refuses capital authority to any
    ``strategy_id`` whose ``STRATEGY_MANIFEST`` purpose is not
    ``capital_candidate``, and every production entry is
    ``harness_validation`` today — so ``S-REC`` only becomes fundable
    through that fixture's monkeypatched manifest.
    """
    conn.execute(
        """
        INSERT INTO strategy_promotions (
            strategy_id, strategy_version, from_stage, to_stage, gate_version,
            evidence_ref, promoted_by, reason
        ) VALUES
          ('S-REC', 'v1', NULL, 'research_candidate', 'test-v1', NULL, 'test', 'registered'),
          ('S-REC', 'v1', 'research_candidate', 'historical_validated', 'test-v1', 'e:h', 'test', 'historical'),
          ('S-REC', 'v1', 'historical_validated', 'forward_observation', 'test-v1', 'e:f', 'test', 'forward'),
          ('S-REC', 'v1', 'forward_observation', 'paper_enabled', 'test-v1', 'e:p', 'test', 'paper')
        """
    )
    deployment = configure_deployment(
        conn,
        strategy_id="S-REC",
        strategy_version="v1",
        mode="paper",
        capital_limit=capital_limit,
        enabled=True,
        changed_by="test",
        reason="reconciliation fixture",
    )
    return deployment.deployment_id


def _seed_order(conn: psycopg.Connection[Any], *, deployment_id: int, instrument_id: int) -> tuple[int, int]:
    """Seed one funded strategy trade and its submitted order.

    Split out of ``_seed_trade`` for #2948: a fairness test needs several
    orders under ONE deployment, and the promotion rows may only be inserted
    once.
    """
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, true)",
        (instrument_id, f"R{instrument_id}", "Reconciliation test"),
    )
    signal_row = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id, strategy_version, instrument_id, signal_bar_date,
            signal_kind, verdict, fill_bar_date, fill_price, universe,
            input_rule_set_versions
        ) VALUES ('S-REC', 'v1', %s, '2026-08-06', 'entry', 'fired',
                  '2026-08-07', 100, 'survivor_only',
                  '{"indicator_series":"rules-v1"}'::jsonb)
        RETURNING signal_id
        """,
        (instrument_id,),
    ).fetchone()
    assert signal_row is not None
    decision_id = decide_funding(
        conn,
        signal_id=int(signal_row[0]),
        verdict="allocated",
        deployment_id=deployment_id,
        amount=Decimal("100"),
        reason_code="test",
    )
    trade_id = create_strategy_trade(conn, decision_id)
    order_row = conn.execute(
        """
        INSERT INTO orders (
            instrument_id, action, order_type, requested_amount, status,
            execution_origin
        ) VALUES (%s, 'BUY', 'MARKET', 100, 'submitted', 'strategy')
        RETURNING order_id
        """,
        (instrument_id,),
    ).fetchone()
    assert order_row is not None
    order_id = order_row[0]
    link_strategy_order(conn, strategy_trade_id=trade_id, order_id=int(order_id), purpose="entry")
    return trade_id, int(order_id)


def _seed_trade(conn: psycopg.Connection[Any], *, instrument_id: int = 2451001) -> tuple[int, int]:
    """Seed a paper-funded strategy trade linked to a submitted order."""
    deployment_id = _seed_deployment(conn)
    return _seed_order(conn, deployment_id=deployment_id, instrument_id=instrument_id)


def _detail(
    *,
    broker_status: str = "Filled",
    position_ids: tuple[int, ...] = (910001, 910002),
) -> BrokerOrderDetail:
    executions = tuple(
        BrokerPositionExecution(
            position_id=position_id,
            state="open",
            remaining_units=Decimal("0.5"),
            opening_units=Decimal("0.5"),
            average_price=Decimal("100"),
            execution_time=datetime(2026, 8, 9, 9, 0, tzinfo=UTC),
            fees=Decimal("0.1"),
            raw_payload={"positionId": position_id},
        )
        for position_id in position_ids
    )
    return BrokerOrderDetail(
        broker_order_ref="13902598",
        reference_id=None,
        status="filled" if broker_status == "Filled" else "pending",
        broker_status=broker_status,
        instrument_id=2451001,
        position_executions=executions,
        last_update=datetime(2026, 8, 9, 9, 1, tzinfo=UTC),
        raw_payload={
            "orderId": 13902598,
            "status": {"name": broker_status},
            "asset": {"instrumentId": 2451001},
            "positionExecutions": [{"positionId": value} for value in position_ids],
        },
    )


def test_crash_identity_is_stable_and_recovery_is_idempotent(
    ebull_test_conn: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> None:
    conn = ebull_test_conn
    trade_id, order_id = _seed_trade(conn)

    # Before-call crash: the committed identity would survive, and a restart
    # never mints a second UUID.
    first_request_id = ensure_strategy_request_id(conn, order_id=order_id)
    assert ensure_strategy_request_id(conn, order_id=order_id) == first_request_id
    conn.commit()

    # During-call transport uncertainty: retain the same UUID and fail closed.
    uncertain_broker = MagicMock(spec=BrokerProvider)
    uncertain_broker.lookup_order.side_effect = BrokerOrderLookupError("timeout")
    uncertain = reconcile_strategy_order(conn, broker=uncertain_broker, order_id=order_id)
    assert uncertain.state == "error"
    assert uncertain_broker.lookup_order.call_args.kwargs == {"reference_id": str(first_request_id)}
    assert ensure_strategy_request_id(conn, order_id=order_id) == first_request_id
    conn.commit()

    # After broker acceptance: exact position cardinality and partial units are
    # durable. Replaying the same response creates no duplicate mappings/claims.
    accepted_broker = MagicMock(spec=BrokerProvider)
    accepted_broker.lookup_order.return_value = _detail()
    first = reconcile_strategy_order(conn, broker=accepted_broker, order_id=order_id)
    second = reconcile_strategy_order(conn, broker=accepted_broker, order_id=order_id)
    assert first.state == second.state == "resolved"
    assert first.position_ids == (910001, 910002)
    accepted_broker.lookup_order.assert_called_once()
    assert conn.execute(
        "SELECT count(*) FROM strategy_order_position_executions WHERE order_id = %s", (order_id,)
    ).fetchone() == (2,)
    assert conn.execute(
        "SELECT count(*) FROM strategy_position_ownership WHERE strategy_trade_id = %s", (trade_id,)
    ).fetchone() == (2,)
    assert conn.execute(
        "SELECT opening_units, average_price FROM strategy_order_position_executions "
        "WHERE order_id = %s ORDER BY broker_position_id LIMIT 1",
        (order_id,),
    ).fetchone() == (Decimal("0.5000000000"), Decimal("100.0000000000"))
    assert conn.execute("SELECT raw_payload_json FROM orders WHERE order_id = %s", (order_id,)).fetchone() == (None,)


def test_pending_partial_fill_is_owned_but_remains_in_backlog(
    ebull_test_conn: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> None:
    conn = ebull_test_conn
    trade_id, order_id = _seed_trade(conn)
    ensure_strategy_request_id(conn, order_id=order_id)
    conn.commit()
    broker = MagicMock(spec=BrokerProvider)
    broker.lookup_order.return_value = _detail(broker_status="Pending", position_ids=(910010,))

    result = reconcile_strategy_order(conn, broker=broker, order_id=order_id)

    assert result.state == "pending"
    assert conn.execute(
        "SELECT broker_position_id FROM strategy_position_ownership WHERE strategy_trade_id = %s", (trade_id,)
    ).fetchone() == (910010,)
    assert conn.execute("SELECT status FROM strategy_trades WHERE strategy_trade_id = %s", (trade_id,)).fetchone() == (
        "submitted",
    )


def test_same_instrument_manual_position_is_observed_but_never_claimed(
    ebull_test_conn: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> None:
    conn = ebull_test_conn
    trade_id, order_id = _seed_trade(conn)
    conn.execute(
        """
        INSERT INTO broker_positions (
            position_id, instrument_id, is_buy, units, amount,
            initial_amount_in_dollars, open_rate, open_conversion_rate,
            open_date_time, raw_payload
        ) VALUES (919999, 2451001, true, 1, 100, 100, 100, 1, now(), '{}'::jsonb)
        """
    )
    ensure_strategy_request_id(conn, order_id=order_id)
    conn.commit()
    broker = MagicMock(spec=BrokerProvider)
    broker.lookup_order.return_value = _detail(position_ids=(910020,))

    reconcile_strategy_order(conn, broker=broker, order_id=order_id)

    owned = conn.execute(
        "SELECT broker_position_id FROM strategy_position_ownership WHERE strategy_trade_id = %s", (trade_id,)
    ).fetchall()
    assert owned == [(910020,)]
    assert 919999 not in {row[0] for row in owned}


def test_not_found_and_overdue_backlog_activate_entry_kill(
    ebull_test_conn: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> None:
    conn = ebull_test_conn
    _, order_id = _seed_trade(conn)
    ensure_strategy_request_id(conn, order_id=order_id)
    conn.commit()
    broker = MagicMock(spec=BrokerProvider)
    broker.lookup_order.side_effect = BrokerOrderNotFound("not yet visible")

    result = reconcile_strategy_order(conn, broker=broker, order_id=order_id)
    assert result.state == "not_found"
    conn.execute(
        "UPDATE strategy_order_reconciliation_state SET first_unresolved_at = now() - interval '11 seconds' "
        "WHERE order_id = %s",
        (order_id,),
    )
    unhealthy = enforce_reconciliation_slo(conn, max_unresolved_seconds=10)
    assert unhealthy.active_block is True
    assert unhealthy.overdue_count == 1
    assert conn.execute(
        "SELECT active FROM strategy_execution_blocks WHERE source = 'order_reconciliation'"
    ).fetchone() == (True,)
    conn.commit()

    # A later exact resolution clears the same bounded state row; no heartbeat
    # or alert-event heap is accumulated.
    broker.lookup_order.side_effect = None
    broker.lookup_order.return_value = _detail(position_ids=(910030,))
    reconcile_strategy_order(conn, broker=broker, order_id=order_id)
    healthy = enforce_reconciliation_slo(conn, max_unresolved_seconds=10)
    assert healthy.active_block is False
    assert conn.execute("SELECT count(*) FROM strategy_execution_blocks").fetchone() == (1,)


def _not_found_broker() -> MagicMock:
    """A broker that answers every lookup ``not_found`` and counts the calls."""
    broker = MagicMock(spec=BrokerProvider)
    broker.lookup_order.side_effect = BrokerOrderNotFound("not visible")
    return broker


def _seed_backlog(
    conn: psycopg.Connection[Any], *, deployment_id: int, count: int, first_instrument_id: int
) -> list[int]:
    """Seed ``count`` submitted strategy orders that are already in the backlog."""
    order_ids: list[int] = []
    for offset in range(count):
        _, order_id = _seed_order(conn, deployment_id=deployment_id, instrument_id=first_instrument_id + offset)
        # ensure_strategy_request_id is what creates the unresolved state row.
        ensure_strategy_request_id(conn, order_id=order_id)
        order_ids.append(order_id)
    conn.commit()
    return order_ids


def _slo_ages(conn: psycopg.Connection[Any]) -> dict[int, datetime]:
    """The exact ``first_unresolved_at`` per order — the age the SLO measures."""
    rows = conn.execute("SELECT order_id, first_unresolved_at FROM strategy_order_reconciliation_state").fetchall()
    # reconcile_backlog refuses a connection that is not idle.
    conn.commit()
    return {int(order_id): unresolved_at for order_id, unresolved_at in rows}


def _age_every_attempt(conn: psycopg.Connection[Any]) -> None:
    """Push every attempt clock a day back, preserving the relative order.

    ⚠ Advancing a Python ``now`` does not advance SQL ``now()``, so cycle
    boundaries are simulated by moving the stored timestamps rather than by
    sleeping.  Ageing every row by the SAME interval leaves the rotation order
    untouched and puts everything past the cooldown cap, which isolates the
    fairness ordering from the backoff.
    """
    conn.execute(
        "UPDATE strategy_order_reconciliation_state "
        "SET last_attempt_at = last_attempt_at - interval '1 day' "
        "WHERE last_attempt_at IS NOT NULL"
    )
    conn.commit()


def test_backlog_rotates_and_never_strands_an_order_past_the_batch_limit(
    ebull_test_conn: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> None:
    """#2948: a stuck head must not own the batch forever.

    The old ``ORDER BY first_unresolved_at, order_id`` sorted on two keys that
    never change for a non-terminal row, so orders past ``limit`` were never
    visited again — this exact sequence would have been
    ``[o0,o1,o2]`` four times.
    """
    conn = ebull_test_conn
    deployment_id = _seed_deployment(conn)
    orders = _seed_backlog(conn, deployment_id=deployment_id, count=6, first_instrument_id=2451200)
    broker = _not_found_broker()
    slo_ages_at_seed = _slo_ages(conn)

    first = reconcile_backlog(conn, broker=broker, limit=3)
    assert [result.order_id for result in first] == orders[0:3]
    assert broker.lookup_order.call_count == 3
    _age_every_attempt(conn)

    second = reconcile_backlog(conn, broker=broker, limit=3)
    assert [result.order_id for result in second] == orders[3:6]
    assert broker.lookup_order.call_count == 6
    _age_every_attempt(conn)

    # Every one of the six was attempted within ceil(6 / 3) = 2 cycles.
    assert conn.execute(
        "SELECT count(*) FROM strategy_order_reconciliation_state WHERE last_attempt_at IS NULL"
    ).fetchone() == (0,)

    # A NEW order joins at the front (NULL sorts first) without resetting anyone
    # else's rotation — but only because fewer than ``limit`` rows are unattempted.
    _, arrival = _seed_order(conn, deployment_id=deployment_id, instrument_id=2451299)
    ensure_strategy_request_id(conn, order_id=arrival)
    conn.commit()
    slo_ages_at_seed[arrival] = _slo_ages(conn)[arrival]

    third = reconcile_backlog(conn, broker=broker, limit=3)
    assert [result.order_id for result in third] == [arrival, orders[0], orders[1]]
    assert broker.lookup_order.call_count == 9

    # A terminal order leaves the backlog and is never polled again.
    conn.execute(
        "UPDATE strategy_order_reconciliation_state SET state = 'resolved', reconciled_at = now() WHERE order_id = %s",
        (orders[2],),
    )
    conn.commit()
    _age_every_attempt(conn)

    fourth = reconcile_backlog(conn, broker=broker, limit=3)
    assert [result.order_id for result in fourth] == orders[3:6]
    assert orders[2] not in {result.order_id for result in fourth}
    assert broker.lookup_order.call_count == 12

    # The SLO age is measured on first_unresolved_at, which no attempt writes:
    # rotation must not be able to make an overdue backlog look fresh. Compared
    # as EXACT stored timestamps — a freshness window would both flake on a slow
    # runner and pass if reconciliation reset the column to now().
    assert _slo_ages(conn) == slo_ages_at_seed
    assert len(slo_ages_at_seed) == 7


def test_backlog_cooldown_defers_dead_orders_but_never_a_progressing_one(
    ebull_test_conn: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> None:
    """#2948: the cooldown is a venue-budget exclusion on no-progress states only."""
    conn = ebull_test_conn
    deployment_id = _seed_deployment(conn)
    orders = _seed_backlog(conn, deployment_id=deployment_id, count=4, first_instrument_id=2451300)
    inside_cooldown, at_boundary, capped, progressing = orders

    for order_id, state, attempts, age_seconds in (
        # attempt 1 -> 300 * (2^0 - 0.5) = 150s. 100s is inside it.
        (inside_cooldown, "not_found", 1, 100),
        # ...and one whole cycle later is outside it, which is the point of the
        # half-cycle slack: a delay of exactly 300s would be missed here.
        (at_boundary, "not_found", 1, 300),
        # attempt 10 -> capped at 3450s, so 3000s is still inside.
        (capped, "error", 10, 3000),
        # 'pending' made progress; a lifetime attempt_count must never defer it.
        (progressing, "pending", 99, 1),
    ):
        conn.execute(
            "UPDATE strategy_order_reconciliation_state "
            "SET state = %s, attempt_count = %s, last_attempt_at = now() - make_interval(secs => %s) "
            "WHERE order_id = %s",
            (state, attempts, age_seconds, order_id),
        )
    conn.commit()

    broker = _not_found_broker()
    results = reconcile_backlog(conn, broker=broker, limit=10)

    assert [result.order_id for result in results] == [at_boundary, progressing]
    assert broker.lookup_order.call_count == 2


def test_an_unmodelled_failure_does_not_abort_the_batch_or_own_the_next_one(
    ebull_test_conn: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> None:
    """#2948: a poison row is the second absorbing state.

    An exception outside the four modelled classes used to escape mid-generator:
    every later order was skipped AND the failing row's attempt clock never
    advanced, so the next cycle selected it first and failed identically.
    """
    conn = ebull_test_conn
    deployment_id = _seed_deployment(conn)
    orders = _seed_backlog(conn, deployment_id=deployment_id, count=3, first_instrument_id=2451400)
    poison = orders[0]

    broker = MagicMock(spec=BrokerProvider)
    broker.lookup_order.side_effect = [
        RuntimeError("unmodelled transport failure"),
        BrokerOrderNotFound("not visible"),
        BrokerOrderNotFound("not visible"),
    ]

    results = reconcile_backlog(conn, broker=broker, limit=3)
    assert [result.order_id for result in results] == orders
    assert results[0].state == "error"
    assert results[0].error_code == "reconcile_unexpected_error"
    assert [result.state for result in results[1:]] == ["not_found", "not_found"]
    assert conn.execute(
        "SELECT last_attempt_at IS NOT NULL, attempt_count FROM strategy_order_reconciliation_state "
        "WHERE order_id = %s",
        (poison,),
    ).fetchone() == (True, 1)

    # The poison row takes its turn in the rotation rather than monopolising it.
    _age_every_attempt(conn)
    broker.lookup_order.side_effect = BrokerOrderNotFound("not visible")
    again = reconcile_backlog(conn, broker=broker, limit=3)
    assert [result.order_id for result in again] == orders


def test_manual_order_cannot_receive_strategy_submission_identity(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (2451099, 'MANREC', 'Manual', true)"
    )
    manual_order_row = conn.execute(
        """
        INSERT INTO orders (instrument_id, action, order_type, requested_amount, status)
        VALUES (2451099, 'BUY', 'MARKET', 100, 'submitted') RETURNING order_id
        """
    ).fetchone()
    assert manual_order_row is not None
    manual_order_id = manual_order_row[0]
    with pytest.raises(StrategyControlError, match="strategy-origin"):
        ensure_strategy_request_id(conn, order_id=int(manual_order_id))
