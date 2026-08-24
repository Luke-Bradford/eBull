"""Crash-safe reconciliation of strategy orders to exact broker positions.

The submission UUID is committed before broker I/O and is never rotated.  A
restart can therefore resolve an accepted order through eToro's documented v2
``orders:lookup?referenceId=...`` contract.  This module never places or closes
an order and never infers position ownership from an instrument match.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, cast
from uuid import UUID, uuid4

import psycopg
import psycopg.rows
from psycopg.pq import TransactionStatus

from app.providers.broker import (
    BrokerOrderDetail,
    BrokerOrderLookupError,
    BrokerOrderNotFound,
    BrokerPositionExecution,
    BrokerProvider,
)
from app.services.strategy_control_plane import StrategyControlError, StrategyOwnershipError

ReconciliationState = Literal[
    "unresolved",
    "pending",
    "resolved",
    "rejected",
    "not_found",
    "ambiguous",
    "error",
]

_KNOWN_PENDING_BROKER_STATES = frozenset({"Pending"})
_KNOWN_FILLED_BROKER_STATES = frozenset({"Filled", "Executed"})
_KNOWN_REJECTED_BROKER_STATES = frozenset({"Rejected", "Failed", "Cancelled", "Canceled"})
_TERMINAL_RECONCILIATION_STATES = frozenset({"resolved", "rejected"})


class StrategyReconciliationError(StrategyControlError):
    """The broker response cannot safely advance a strategy order."""


@dataclass(frozen=True)
class ReconciliationResult:
    order_id: int
    state: ReconciliationState
    broker_order_ref: str | None
    broker_status: str | None
    position_ids: tuple[int, ...]
    error_code: str | None = None


@dataclass(frozen=True)
class ReconciliationHealth:
    active_block: bool
    overdue_count: int
    oldest_unresolved_at: datetime | None


def ensure_strategy_request_id(conn: psycopg.Connection[Any], *, order_id: int) -> UUID:
    """Assign a strategy order's immutable broker idempotency UUID once.

    The caller must commit after this function and before broker I/O. Repeated
    calls return the same UUID, including a retry after a pre-call crash.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT o.execution_origin, o.strategy_request_id
            FROM orders o
            JOIN strategy_trade_orders sto ON sto.order_id = o.order_id
            WHERE o.order_id = %s
            FOR UPDATE OF o
            """,
            (order_id,),
        )
        row = cur.fetchone()
    if row is None or row["execution_origin"] != "strategy":
        raise StrategyReconciliationError("only a linked strategy-origin order may receive a request id")
    stored_request_id = row["strategy_request_id"]
    request_id = UUID(str(stored_request_id)) if stored_request_id is not None else uuid4()
    if stored_request_id is None:
        conn.execute(
            "UPDATE orders SET strategy_request_id = %s WHERE order_id = %s",
            (request_id, order_id),
        )
    conn.execute(
        """
        INSERT INTO strategy_order_reconciliation_state (order_id)
        VALUES (%s)
        ON CONFLICT (order_id) DO NOTHING
        """,
        (order_id,),
    )
    return request_id


def _payload_hash(detail: BrokerOrderDetail) -> str:
    # Settled review-prevention decision #471 removed duplicate raw persistence
    # for etoro_broker once its decision-bearing fields land in SQL. Keep the
    # response process-local and retain a reproducibility fingerprint instead.
    canonical = json.dumps(detail.raw_payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _record_failure(
    conn: psycopg.Connection[Any],
    *,
    order_id: int,
    state: Literal["not_found", "ambiguous", "error"],
    error_code: str,
    broker_status: str | None = None,
) -> ReconciliationResult:
    conn.execute(
        """
        INSERT INTO strategy_order_reconciliation_state (
            order_id, state, last_attempt_at, attempt_count, broker_status,
            last_error_code, updated_at
        ) VALUES (%s, %s, now(), 1, %s, %s, now())
        ON CONFLICT (order_id) DO UPDATE SET
            state = EXCLUDED.state,
            last_attempt_at = now(),
            reconciled_at = NULL,
            attempt_count = strategy_order_reconciliation_state.attempt_count + 1,
            broker_status = EXCLUDED.broker_status,
            last_error_code = EXCLUDED.last_error_code,
            updated_at = now()
        """,
        (order_id, state, broker_status, error_code),
    )
    conn.execute(
        """
        UPDATE strategy_trades t
        SET status = 'reconcile_required', updated_at = now()
        FROM strategy_trade_orders sto
        WHERE sto.order_id = %s AND sto.strategy_trade_id = t.strategy_trade_id
          AND t.status NOT IN ('closed', 'failed')
        """,
        (order_id,),
    )
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT o.broker_order_ref,
                   ARRAY(
                       SELECT execution.broker_position_id
                       FROM strategy_order_position_executions execution
                       WHERE execution.order_id = o.order_id
                       ORDER BY execution.broker_position_id
                   ) AS position_ids
            FROM orders o
            WHERE o.order_id = %s
            """,
            (order_id,),
        )
        persisted = cur.fetchone()
    assert persisted is not None
    persisted_ref = persisted["broker_order_ref"]
    return ReconciliationResult(
        order_id,
        state,
        str(persisted_ref) if persisted_ref is not None else None,
        broker_status,
        tuple(int(value) for value in persisted["position_ids"]),
        error_code,
    )


def _record_execution(
    conn: psycopg.Connection[Any],
    *,
    order_id: int,
    execution: BrokerPositionExecution,
) -> None:
    row = conn.execute(
        """
        INSERT INTO strategy_order_position_executions (
            order_id, broker_position_id, position_state, remaining_units,
            opening_units, average_price, execution_time, fees,
            last_observed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (order_id, broker_position_id) DO UPDATE SET
            position_state = EXCLUDED.position_state,
            remaining_units = EXCLUDED.remaining_units,
            opening_units = COALESCE(
                strategy_order_position_executions.opening_units,
                EXCLUDED.opening_units
            ),
            average_price = COALESCE(
                strategy_order_position_executions.average_price,
                EXCLUDED.average_price
            ),
            execution_time = COALESCE(
                strategy_order_position_executions.execution_time,
                EXCLUDED.execution_time
            ),
            fees = COALESCE(strategy_order_position_executions.fees, EXCLUDED.fees),
            last_observed_at = now()
        WHERE (
            strategy_order_position_executions.opening_units IS NULL
            OR EXCLUDED.opening_units IS NULL
            OR strategy_order_position_executions.opening_units = EXCLUDED.opening_units
        ) AND (
            strategy_order_position_executions.average_price IS NULL
            OR EXCLUDED.average_price IS NULL
            OR strategy_order_position_executions.average_price = EXCLUDED.average_price
        ) AND (
            strategy_order_position_executions.execution_time IS NULL
            OR EXCLUDED.execution_time IS NULL
            OR strategy_order_position_executions.execution_time = EXCLUDED.execution_time
        ) AND (
            strategy_order_position_executions.fees IS NULL
            OR EXCLUDED.fees IS NULL
            OR strategy_order_position_executions.fees = EXCLUDED.fees
        )
        RETURNING broker_position_id
        """,
        (
            order_id,
            execution.position_id,
            execution.state,
            execution.remaining_units,
            execution.opening_units,
            execution.average_price,
            execution.execution_time,
            execution.fees,
        ),
    ).fetchone()
    if row is None:
        raise StrategyReconciliationError("broker changed immutable opening execution facts")


def _claim_entry_execution(
    conn: psycopg.Connection[Any],
    *,
    strategy_trade_id: int,
    broker_position_id: int,
) -> None:
    inserted = conn.execute(
        """
        INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id)
        VALUES (%s, %s)
        ON CONFLICT (broker_position_id) DO NOTHING
        RETURNING ownership_id
        """,
        (strategy_trade_id, broker_position_id),
    ).fetchone()
    if inserted is not None:
        return
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT strategy_trade_id FROM strategy_position_ownership WHERE broker_position_id = %s",
            (broker_position_id,),
        )
        existing = cur.fetchone()
    if existing is None or int(existing["strategy_trade_id"]) != strategy_trade_id:
        raise StrategyOwnershipError("broker position is already owned by a different strategy trade")


def _apply_detail(
    conn: psycopg.Connection[Any],
    *,
    order_id: int,
    detail: BrokerOrderDetail,
) -> ReconciliationResult:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT o.instrument_id, o.broker_order_ref, sto.strategy_trade_id, sto.purpose,
                   state.state AS reconciliation_state
            FROM orders o
            JOIN strategy_trade_orders sto ON sto.order_id = o.order_id
            LEFT JOIN strategy_order_reconciliation_state state ON state.order_id = o.order_id
            WHERE o.order_id = %s AND o.execution_origin = 'strategy'
            FOR UPDATE OF o
            """,
            (order_id,),
        )
        local = cur.fetchone()
    if local is None:
        raise StrategyReconciliationError("order is not linked strategy authority")
    instrument_id = local["instrument_id"]
    existing_ref = local["broker_order_ref"]
    strategy_trade_id = local["strategy_trade_id"]
    purpose = local["purpose"]
    prior_state = local["reconciliation_state"]
    if int(instrument_id) != detail.instrument_id:
        raise StrategyReconciliationError("broker order instrument differs from durable intent")
    if existing_ref is not None and str(existing_ref) != detail.broker_order_ref:
        raise StrategyReconciliationError("broker order id differs from the previously reconciled id")

    broker_status = detail.broker_status
    if broker_status in _KNOWN_FILLED_BROKER_STATES:
        state: ReconciliationState = "resolved"
        order_status = "filled"
    elif broker_status in _KNOWN_REJECTED_BROKER_STATES:
        state = "rejected"
        order_status = "rejected"
    elif broker_status in _KNOWN_PENDING_BROKER_STATES:
        state = "pending"
        order_status = "pending"
    else:
        raise StrategyReconciliationError(f"unknown broker order status: {broker_status}")

    if prior_state in _TERMINAL_RECONCILIATION_STATES and prior_state != state:
        raise StrategyReconciliationError("broker order attempted to regress or change terminal state")

    if state == "rejected" and detail.position_executions:
        raise StrategyReconciliationError("rejected broker order unexpectedly has position executions")
    if state == "resolved" and purpose == "entry" and not detail.position_executions:
        raise StrategyReconciliationError("filled strategy entry has no exact position executions")

    for execution in detail.position_executions:
        _record_execution(conn, order_id=order_id, execution=execution)
        if purpose == "entry":
            _claim_entry_execution(
                conn,
                strategy_trade_id=int(strategy_trade_id),
                broker_position_id=execution.position_id,
            )

    conn.execute(
        """
        INSERT INTO strategy_order_reconciliation_state (
            order_id, state, last_attempt_at, reconciled_at, attempt_count,
            broker_status, position_count, last_error_code,
            last_payload_sha256, updated_at
        ) VALUES (
            %s, %s, now(),
            CASE WHEN %s IN ('resolved', 'rejected') THEN now() ELSE NULL END,
            1, %s, %s, NULL, %s, now()
        )
        ON CONFLICT (order_id) DO UPDATE SET
            state = EXCLUDED.state,
            last_attempt_at = now(),
            reconciled_at = EXCLUDED.reconciled_at,
            attempt_count = strategy_order_reconciliation_state.attempt_count + 1,
            broker_status = EXCLUDED.broker_status,
            position_count = EXCLUDED.position_count,
            last_error_code = NULL,
            last_payload_sha256 = EXCLUDED.last_payload_sha256,
            updated_at = now()
        """,
        (order_id, state, state, broker_status, len(detail.position_executions), _payload_hash(detail)),
    )
    conn.execute(
        """
        UPDATE orders
        SET broker_order_ref = %s, status = %s
        WHERE order_id = %s
        """,
        (detail.broker_order_ref, order_status, order_id),
    )
    if purpose == "entry":
        trade_status = "open" if state == "resolved" else ("failed" if state == "rejected" else "submitted")
        conn.execute(
            "UPDATE strategy_trades SET status = %s, updated_at = now() WHERE strategy_trade_id = %s",
            (trade_status, strategy_trade_id),
        )
    return ReconciliationResult(
        order_id,
        state,
        detail.broker_order_ref,
        broker_status,
        tuple(execution.position_id for execution in detail.position_executions),
    )


def reconcile_strategy_order(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    order_id: int,
) -> ReconciliationResult:
    """Poll and reconcile one linked strategy order without any broker write."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise StrategyReconciliationError(
            "reconciliation requires an idle connection so broker I/O cannot run inside a DB transaction"
        )
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT o.broker_order_ref, o.strategy_request_id,
                   state.state AS reconciliation_state,
                   state.broker_status,
                   ARRAY(
                       SELECT execution.broker_position_id
                       FROM strategy_order_position_executions execution
                       WHERE execution.order_id = o.order_id
                       ORDER BY execution.broker_position_id
                   ) AS position_ids
            FROM orders o
            JOIN strategy_trade_orders sto ON sto.order_id = o.order_id
            LEFT JOIN strategy_order_reconciliation_state state ON state.order_id = o.order_id
            WHERE o.order_id = %s AND o.execution_origin = 'strategy'
            """,
            (order_id,),
        )
        identity = cur.fetchone()
    conn.commit()
    if identity is None:
        raise StrategyReconciliationError("order is not linked strategy authority")
    broker_ref = identity["broker_order_ref"]
    request_id = identity["strategy_request_id"]
    prior_state = identity["reconciliation_state"]
    if prior_state in _TERMINAL_RECONCILIATION_STATES:
        return ReconciliationResult(
            order_id=order_id,
            state=cast(ReconciliationState, prior_state),
            broker_order_ref=str(broker_ref) if broker_ref is not None else None,
            broker_status=identity["broker_status"],
            position_ids=tuple(int(value) for value in identity["position_ids"]),
        )
    if request_id is None:
        with conn.transaction():
            return _record_failure(
                conn,
                order_id=order_id,
                state="ambiguous",
                error_code="missing_submission_request_id",
            )
    try:
        if broker_ref is not None and str(broker_ref).isdigit() and int(str(broker_ref)) > 0:
            detail = broker.lookup_order(order_id=str(broker_ref))
        else:
            detail = broker.lookup_order(reference_id=str(request_id))
    except BrokerOrderNotFound:
        with conn.transaction():
            return _record_failure(
                conn,
                order_id=order_id,
                state="not_found",
                error_code="broker_order_not_found",
            )
    except BrokerOrderLookupError:
        with conn.transaction():
            return _record_failure(
                conn,
                order_id=order_id,
                state="error",
                error_code="broker_lookup_error",
            )
    try:
        with conn.transaction():
            return _apply_detail(conn, order_id=order_id, detail=detail)
    except StrategyReconciliationError, StrategyOwnershipError:
        with conn.transaction():
            return _record_failure(
                conn,
                order_id=order_id,
                state="ambiguous",
                error_code="unsafe_broker_detail",
            )


def reconcile_backlog(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    limit: int = 50,
) -> tuple[ReconciliationResult, ...]:
    """Reconcile a bounded oldest-first restart backlog."""
    if limit < 1 or limit > 100:
        raise ValueError("limit must be between 1 and 100")
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise StrategyReconciliationError("backlog reconciliation requires an idle connection")
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT state.order_id
            FROM strategy_order_reconciliation_state state
            JOIN orders o ON o.order_id = state.order_id
            JOIN strategy_trade_orders link ON link.order_id=o.order_id
            JOIN strategy_trades trade ON trade.strategy_trade_id=link.strategy_trade_id
            WHERE state.state NOT IN ('resolved', 'rejected')
              AND o.execution_origin = 'strategy'
              AND trade.core_rebalance_intent_id IS NULL
            ORDER BY state.first_unresolved_at, state.order_id
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    conn.commit()
    return tuple(reconcile_strategy_order(conn, broker=broker, order_id=int(row["order_id"])) for row in rows)


def enforce_reconciliation_slo(
    conn: psycopg.Connection[Any],
    *,
    max_unresolved_seconds: int,
) -> ReconciliationHealth:
    """Block new strategy entries when unresolved order identity exceeds policy.

    The threshold is an explicit deployment input, not a made-up constant. The
    current row is updated in place, so healthy polling does not grow the DB.
    """
    if max_unresolved_seconds <= 0:
        raise ValueError("max_unresolved_seconds must be positive")
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT count(*) AS overdue_count, min(first_unresolved_at) AS oldest_unresolved_at
            FROM strategy_order_reconciliation_state
            WHERE state NOT IN ('resolved', 'rejected')
              AND first_unresolved_at <= now() - make_interval(secs => %s)
            """,
            (max_unresolved_seconds,),
        )
        row = cur.fetchone()
    assert row is not None
    overdue_count = int(row["overdue_count"])
    oldest = row["oldest_unresolved_at"]
    active = overdue_count > 0
    reason = (
        f"{overdue_count} strategy order(s) exceed the configured reconciliation SLO"
        if active
        else "reconciliation backlog is within the configured SLO"
    )
    conn.execute(
        """
        INSERT INTO strategy_execution_blocks (
            source, active, reason, blocked_at, cleared_at, updated_at
        ) VALUES (
            'order_reconciliation', %s, %s,
            CASE WHEN %s THEN now() ELSE NULL END,
            CASE WHEN %s THEN NULL ELSE now() END,
            now()
        )
        ON CONFLICT (source) DO UPDATE SET
            active = EXCLUDED.active,
            reason = EXCLUDED.reason,
            blocked_at = CASE
                WHEN EXCLUDED.active AND NOT strategy_execution_blocks.active THEN now()
                WHEN EXCLUDED.active THEN strategy_execution_blocks.blocked_at
                ELSE NULL
            END,
            cleared_at = CASE WHEN EXCLUDED.active THEN NULL ELSE now() END,
            updated_at = now()
        """,
        (active, reason, active, active),
    )
    return ReconciliationHealth(active, overdue_count, oldest)


__all__ = [
    "ReconciliationHealth",
    "ReconciliationResult",
    "StrategyReconciliationError",
    "enforce_reconciliation_slo",
    "ensure_strategy_request_id",
    "reconcile_backlog",
    "reconcile_strategy_order",
]
