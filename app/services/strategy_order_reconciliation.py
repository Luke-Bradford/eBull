"""Crash-safe reconciliation of strategy orders to exact broker positions.

The submission UUID is committed before broker I/O and is never rotated.  A
restart can therefore resolve an accepted order through eToro's documented v2
``orders:lookup?referenceId=...`` contract.  This module never places or closes
an order and never infers position ownership from an instrument match.

Lock ordering (#2964 item 3) -- ONE order, stated once, for every writer of a
strategy order's identity, reconciliation state, executions or trade::

    advisory  1. PAPER_ALLOCATOR -> CORE_MANDATE -> CORE_SUBMISSION
    advisory  2. _position_lock(broker_position_id)
    advisory  3. reconciliation_order_lock(order_id)   <- ALWAYS acquired last
    rows      4. orders
              5. strategy_order_position_executions    (ascending position id)
              6. strategy_position_ownership           (ascending position id)
              7. strategy_order_reconciliation_state
              8. strategy_trades

The row order is ``_apply_detail``'s, transcribed rather than chosen: it writes
executions and ownership before the reconciliation row.  ``strategy_trades`` is
last because it is the only row two DIFFERENT orders of one trade both touch --
with it last a wait is a queue, not a cycle.  Ascending position id matters for
the same reason one level down: two orders whose broker details name the same
two positions in opposite order would each insert one ownership row and then
block on the other's unique-index conflict, a deadlock across two different
per-order locks.

⚠ Inserting a ``strategy_order_reconciliation_state`` row takes a ``KEY SHARE``
FK lock on its ``orders`` row, which conflicts with ``FOR UPDATE``.  A writer
that created state WITHOUT first locking ``orders`` could deadlock against
``_apply_detail``.  ``ensure_strategy_request_id`` -- the only such initialiser --
already takes ``FOR UPDATE OF o`` first, which is why rule 4 precedes rule 7.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
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

logger = logging.getLogger(__name__)

ReconciliationState = Literal[
    "unresolved",
    "pending",
    "resolved",
    "rejected",
    "not_found",
    "ambiguous",
    "error",
]

# Retry cadence for the reconciliation backlog (#2948). No published rule fixes a
# broker order-reconciliation cadence -- searched, none exists, and none is borrowed --
# so it is fixed BY CONSTRUCTION from the two constraints that do bind:
#   1. ``strategy_paper_cycle`` runs every five minutes
#      (``Cadence.every_n_minutes(interval=5)``, app/workers/scheduler.py:2322);
#   2. eToro's ordinary trading reads share 60/min and 429 carries no guaranteed
#      Retry-After (.claude/skills/data-sources/etoro-api.md, "Stable facts"), so the
#      client must supply its own capped backoff.
# Delays are ``base * (2**(n - 1) - 0.5)`` -- k cycles LESS A HALF CYCLE. The half is
# load-bearing: a delay of exactly one cycle is unreachable, because the batch selected
# at 00:00 records its attempts at 00:00:02 and the 00:05 selection then sees only
# 299.8s. The slack tolerates 150s of drift between a selection's grid time and the
# attempts it records. Sequence at these defaults: 150, 450, 1050, 2250, 3450, 3450...
RECONCILIATION_RETRY_BASE_SECONDS = 300
RECONCILIATION_RETRY_CAP_SECONDS = 3450
# Bounds the exponent so ``power(2, n)`` cannot overflow on a pathological
# attempt_count. 2**30 * 300s is ~10,000 years, so the seconds cap always binds
# first and this only ever guards the arithmetic.
_RECONCILIATION_RETRY_EXPONENT_CAP = 30

# Broker order status vocabulary. Source rule: the eToro live portal's documented
# ``status.name`` enum for ``GET /api/v2/trading/info/{demo|real}/orders:lookup``
# (portal slug ``trading--demo/get-order-information-and-position-details``,
# OpenAPI v1.375.0, verified 2026-09-13 per
# ``.claude/skills/data-sources/etoro-api.md``). The twelve documented values are:
#   Received, Placed, Filled, Rejected, PartiallyFilled, PendingCancel, Canceled,
#   Expired, CanceledPartiallyFilled, RejectedPartiallyFilled, WaitingForMarket,
#   PendingTriggeredRate
# An unrecognised status raises, and ``_apply_detail`` runs inside a handler that
# records the non-terminal ``ambiguous`` state -- so every documented value left
# out of these sets is a way for the broker to wedge an order (#2961, #2962).
#
# ``Pending``, ``Executed``, ``Failed`` and ``Cancelled`` are NOT in that enum but
# are retained: nothing establishes that the demo connection never emits them, and
# dropping one would be a narrowing change measured only on its admit side.
_KNOWN_PENDING_BROKER_STATES = frozenset({"Pending", "Received", "Placed", "WaitingForMarket", "PendingTriggeredRate"})
_KNOWN_FILLED_BROKER_STATES = frozenset({"Filled", "Executed"})
_KNOWN_REJECTED_BROKER_STATES = frozenset({"Rejected", "Failed", "Cancelled", "Canceled", "Expired"})
# Documented, and DELIBERATELY still unrecognised so they keep raising (#2965).
# These four are exactly the non-``Filled`` statuses that can carry
# ``positionExecutions``, so admitting one activates an ownership lifecycle that is
# not settled: ``_apply_detail`` claims ownership unconditional on state (:341-348)
# while ``strategy_engine_capital.load_engine_capital_authority`` refuses that
# combination, and ``_record_execution``'s ON CONFLICT treats the opening facts as
# immutable -- so if eToro grows one ``positionId`` across successive partial fills
# rather than emitting a new one, admitting these creates a fresh wedge on the
# second poll. Settling that needs a real partial fill observed against the broker.
# See docs/proposals/execution/2026-09-13-partial-fill-ownership-and-status-vocabulary.md.
_UNSETTLED_PARTIAL_FILL_BROKER_STATES = frozenset(
    {"PartiallyFilled", "PendingCancel", "CanceledPartiallyFilled", "RejectedPartiallyFilled"}
)
_TERMINAL_RECONCILIATION_STATES = frozenset({"resolved", "rejected"})


class StrategyReconciliationError(StrategyControlError):
    """The broker response cannot safely advance a strategy order."""


class StrategyReconciliationBusy(StrategyReconciliationError):
    """Another reconciler or submitter already holds this order's lock.

    Retryable and expected under contention -- NOT a broker fault and not a
    reason to record a reconciliation failure. ``reconcile_backlog`` skips the
    row; the core-resume endpoint maps it to HTTP 409.
    """


# ---------------------------------------------------------------------------
# Per-order reconciliation lock (#2964 items 2-4)
# ---------------------------------------------------------------------------
#
# ⚠⚠ Why a SESSION advisory lock and not the ``FOR UPDATE SKIP LOCKED`` the
# ticket proposed: ``reconcile_strategy_order`` refuses a non-idle connection
# (broker I/O must never run inside a DB transaction), so ``reconcile_backlog``
# MUST commit after selecting. A row lock dies at that commit, before a single
# broker call. The claim has to outlive commits, which is what a session
# advisory lock is and a row lock is not.
#
# ⚠ The key is ``hashtextextended`` over a namespaced identity, following
# ``strategy_position_manager._position_lock``. ``orders.order_id`` is BIGINT and
# the prevention log is explicit -- "Never cast a BIGINT id to int4 in a lock
# key" -- so the two-int4 shape used by ``core_submission_lock`` is unavailable.
# A hash collision makes two UNRELATED orders serialise (a reconciler skips one
# row for one cycle); it can never put two reconcilers on one order.
#
# ⚠ One key expression, spelled once. The prevention-log entry "Two writers
# sharing an advisory lock must use a BYTE-IDENTICAL lock-key SQL" is satisfied
# structurally here rather than by a grep tripwire: acquire (both flavours) and
# release all interpolate ``_ORDER_LOCK_KEY_SQL``.
_RECONCILIATION_ORDER_LOCK_SEED = 2964
_ORDER_LOCK_KEY_SQL = "hashtextextended(%s, %s)"
_ORDER_LOCK_TRY_SQL = f"SELECT pg_try_advisory_lock({_ORDER_LOCK_KEY_SQL})"
_ORDER_LOCK_WAIT_SQL = f"SELECT pg_advisory_lock({_ORDER_LOCK_KEY_SQL})"
_ORDER_LOCK_UNLOCK_SQL = f"SELECT pg_advisory_unlock({_ORDER_LOCK_KEY_SQL})"

# ⚠ Nesting is a silent correctness hole, not a no-op. Measured against the dev
# server: a second ``pg_try_advisory_lock`` on the same session returns TRUE and
# bumps the hold count, and the first ``pg_advisory_unlock`` returns TRUE while
# leaving the lock HELD. An inner context manager exiting would therefore report
# success and release nothing. No path nests today; this ContextVar (the shape of
# ``app/jobs/locks.py::_HELD_SOURCES``) makes a future one fail loudly instead.
_HELD_ORDER_LOCKS: ContextVar[frozenset[tuple[int, int]]] = ContextVar(
    "_reconciliation_held_order_locks", default=frozenset()
)


def _order_lock_params(order_id: int) -> tuple[str, int]:
    return (f"strategy-order-reconciliation:{order_id}", _RECONCILIATION_ORDER_LOCK_SEED)


@contextmanager
def _order_lock(conn: psycopg.Connection[Any], order_id: int, *, wait: bool) -> Iterator[None]:
    # ⚠ The helper owns its transaction. Under ``autocommit=False`` the acquire
    # SELECT itself opens one (measured: transaction_status INTRANS after it), so
    # without these commits the caller's next statement runs inside a stray
    # transaction and ``reconcile_strategy_order``'s idle check fails. The
    # CONTENTION path needs the commit just as much as the success path.
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise StrategyReconciliationError("the reconciliation order lock requires an idle connection")
    # ⚠ Keyed on (connection, order), not order alone. The hazard the guard exists
    # for is ONE session acquiring twice; two different connections taking the same
    # order's lock in one call context is the ordinary contention this module
    # handles at the server, and refusing it here would invent a conflict Postgres
    # does not have. ``id(conn)`` is sound as the key precisely because the
    # connection object is alive for the whole hold, so it cannot be reused while
    # the entry is in the set.
    key = (id(conn), order_id)
    held = _HELD_ORDER_LOCKS.get()
    if key in held:
        raise StrategyReconciliationError(
            f"reconciliation order lock for {order_id} is already held on this connection"
        )
    params = _order_lock_params(order_id)
    acquired = conn.execute(_ORDER_LOCK_WAIT_SQL if wait else _ORDER_LOCK_TRY_SQL, params).fetchone()
    conn.commit()
    if not wait and acquired != (True,):
        raise StrategyReconciliationBusy(f"order {order_id} is already being reconciled")
    token = _HELD_ORDER_LOCKS.set(held | {key})
    try:
        yield
    except BaseException:
        # ⚠ The body's exception WINS. Raising a lost-ownership error from a
        # `finally` while another exception is propagating replaces it -- the
        # broker or persistence failure the caller actually needs would surface as
        # a lock-ownership message, which is strictly less informative and points
        # at the wrong subsystem. Release, log loudly, and let the original
        # propagate. `BaseException` and not `Exception`: a `KeyboardInterrupt`
        # must still release the lock.
        _release_order_lock(conn, params=params, order_id=order_id, raise_on_loss=False)
        raise
    else:
        _release_order_lock(conn, params=params, order_id=order_id, raise_on_loss=True)
    finally:
        _HELD_ORDER_LOCKS.reset(token)


def _release_order_lock(
    conn: psycopg.Connection[Any],
    *,
    params: tuple[str, int],
    order_id: int,
    raise_on_loss: bool,
) -> None:
    if conn.info.transaction_status != TransactionStatus.IDLE:
        conn.rollback()
    released = conn.execute(_ORDER_LOCK_UNLOCK_SQL, params).fetchone()
    conn.commit()
    if released == (True,):
        return
    message = f"reconciliation order lock ownership for {order_id} was lost"
    if raise_on_loss:
        raise StrategyReconciliationError(message)
    # A lost lock means the critical section was not what it claimed to be, so it
    # must be loud even when it cannot be raised.
    logger.error("%s while another exception was propagating", message)


def reconciliation_order_lock(conn: psycopg.Connection[Any], order_id: int) -> AbstractContextManager[None]:
    """Block until this order's reconciliation lock is free -- for SUBMITTERS.

    ⚠⚠ Submitters BLOCK and reconcilers TRY, and the asymmetry is the design.
    A reconciler that loses can skip: the row is re-selected next cycle. A
    submitter that loses cannot. Its ``orders`` and reconciliation rows are
    already committed -- that is the crash-safe durable-before-I/O design -- and
    ``resume_core_submission`` is deliberately forbidden to resubmit, so refusing
    here would abandon a durable authority and wedge the arm permanently.

    The wait is bounded by the only thing that can hold this lock against a
    submitter: one reconciler's broker lookup, i.e. the provider HTTP timeout.
    No ``lock_timeout`` is set on purpose -- ``lock_timeout`` DOES apply to an
    advisory wait (measured: ``LockNotAvailable`` after 300ms), but converting
    the wait into a failure re-creates exactly the stranded authority this
    blocking acquire exists to prevent.
    """
    return _order_lock(conn, order_id, wait=True)


def try_reconciliation_order_lock(conn: psycopg.Connection[Any], order_id: int) -> AbstractContextManager[None]:
    """Take this order's reconciliation lock or raise -- for RECONCILERS.

    Raises :class:`StrategyReconciliationBusy` rather than waiting, so a batch
    never blocks behind an attended request holding one order.
    """
    return _order_lock(conn, order_id, wait=False)


def classify_broker_order_status(broker_status: str) -> tuple[ReconciliationState, str]:
    """Map a broker ``status.name`` to its reconciliation state and order status.

    Pure so the whole documented vocabulary can be asserted without a database.
    Raises rather than guessing: an unrecognised status must never advance an
    order, and the four partial-fill statuses get their own message because they
    are a known open question rather than an unrecognised string.
    """
    if broker_status in _KNOWN_FILLED_BROKER_STATES:
        return "resolved", "filled"
    if broker_status in _KNOWN_REJECTED_BROKER_STATES:
        return "rejected", "rejected"
    if broker_status in _KNOWN_PENDING_BROKER_STATES:
        return "pending", "pending"
    if broker_status in _UNSETTLED_PARTIAL_FILL_BROKER_STATES:
        raise StrategyReconciliationError(
            f"broker order status {broker_status} may carry position executions and its "
            "ownership lifecycle is unsettled (#2965)"
        )
    raise StrategyReconciliationError(f"unknown broker order status: {broker_status}")


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
    # ⚠⚠ #2964 item 1. A failure arriving for an ALREADY-TERMINAL order is an
    # OBSERVATION, never a state transition. Before the paired CASEs below this
    # was a bare `state = EXCLUDED.state, reconciled_at = NULL`, so a delayed
    # 404, a transport error or an unsafe-detail result could demote a
    # `resolved`/`rejected` row and clear its `reconciled_at`.
    #
    # ⚠ The CHECK cannot catch that: `strategy_order_reconciliation_resolved_shape`
    # (sql/285) requires `reconciled_at IS NOT NULL` exactly when the state is
    # terminal, and the demotion moved BOTH together -- every step of the
    # corruption produced a valid ROW. The constraint describes a valid row and
    # says nothing about a valid TRANSITION.
    #
    # ⚠ Paired CASE rather than a `DO UPDATE ... WHERE`: a WHERE that skipped
    # would leave the late failure with NO trace at all and would return no row,
    # replacing a corruption with a silence. The CASEs read the EXISTING row, so
    # both CHECK branches stay satisfied and the row's membership of the two
    # partial indexes (sql/285:81, sql/376:18) is unchanged.
    #
    # ⚠⚠ `broker_status` and `last_error_code` are preserved on the terminal
    # branch too, and that is not tidiness. Every call site passes
    # `broker_status=None`, so assigning it would replace a terminal `Filled` with
    # NULL and leave the row's provenance a mixture of the resolve and the late
    # failure. And a retained `last_error_code` makes
    # `app/api/strategies.py` add a `*_reconciliation_error` to the trade's
    # lifecycle `incomplete_reasons` FOR A RESOLVED TRADE -- which nothing would
    # ever clear, because the terminal early-return path in
    # `reconcile_strategy_order` never writes. So the terminal branch moves the
    # ATTEMPT COUNTERS only; the error detail lives in the WARNING logged below.
    #
    # ⚠ `terminal` is passed as a LIST: `_TERMINAL_RECONCILIATION_STATES` is a
    # frozenset and psycopg rejects it as a parameter. The whole statement uses
    # NAMED placeholders because mixing them with the positional ones this clause
    # used to carry raises "positional and named placeholders cannot be mixed".
    #
    # ⚠ This parameter keeps THIS statement in step with the Python constant and
    # nothing more -- the CHECK, both partial indexes, `_apply_detail`'s SQL and
    # the backlog/SLO predicates each hard-code terminal membership on their own.
    # Adding a terminal state still means touching those by hand.
    #
    # ⚠ The `= ANY(...)` predicate is repeated four times ON PURPOSE, and the
    # hoisted alternative was TESTED rather than dismissed (review nitpick on PR
    # #2974). `SET (state, reconciled_at, broker_status, last_error_code) = (SELECT
    # ... FROM (SELECT ... AS terminal) t)` is accepted by Postgres and behaves
    # correctly -- verified against the dev server, not reasoned about. It is not
    # adopted because the trade is bad in both directions: the saving is four
    # membership checks against a TWO-element array on at most `limit` rows per
    # five-minute batch, which is nothing, while the cost is that each field's rule
    # stops being readable at the field. That locality is the point here -- two of
    # these columns are PRESERVED and two are ASSIGNED under the same condition,
    # and that asymmetry is the whole defect being fixed.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            INSERT INTO strategy_order_reconciliation_state (
                order_id, state, last_attempt_at, attempt_count, broker_status,
                last_error_code, updated_at
            ) VALUES (
                %(order_id)s, %(state)s, now(), 1, %(broker_status)s, %(error_code)s, now()
            )
            ON CONFLICT (order_id) DO UPDATE SET
                state = CASE
                    WHEN strategy_order_reconciliation_state.state = ANY(%(terminal)s)
                    THEN strategy_order_reconciliation_state.state
                    ELSE EXCLUDED.state END,
                reconciled_at = CASE
                    WHEN strategy_order_reconciliation_state.state = ANY(%(terminal)s)
                    THEN strategy_order_reconciliation_state.reconciled_at
                    ELSE NULL END,
                broker_status = CASE
                    WHEN strategy_order_reconciliation_state.state = ANY(%(terminal)s)
                    THEN strategy_order_reconciliation_state.broker_status
                    ELSE EXCLUDED.broker_status END,
                last_error_code = CASE
                    WHEN strategy_order_reconciliation_state.state = ANY(%(terminal)s)
                    THEN strategy_order_reconciliation_state.last_error_code
                    ELSE EXCLUDED.last_error_code END,
                last_attempt_at = now(),
                attempt_count = strategy_order_reconciliation_state.attempt_count + 1,
                updated_at = now()
            RETURNING state, broker_status, last_error_code
            """,
            {
                "order_id": order_id,
                "state": state,
                "broker_status": broker_status,
                "error_code": error_code,
                "terminal": sorted(_TERMINAL_RECONCILIATION_STATES),
            },
        )
        stored = cur.fetchone()
    assert stored is not None, "ON CONFLICT DO UPDATE without a WHERE always returns a row"
    stored_state = cast(ReconciliationState, stored["state"])
    guarded = stored_state != state

    if guarded:
        # The error detail is deliberately NOT stored (see above), so this line is
        # the only place it exists. A failure arriving for a settled order is
        # worth an operator seeing.
        logger.warning(
            "reconciliation failure %r arrived for order %s which is already %s; "
            "recording the attempt without changing the outcome",
            error_code,
            order_id,
            stored_state,
        )
    else:
        # ⚠ Gated on the STORED state, not the attempted one: a terminal
        # reconciliation row must not flag its trade `reconcile_required`, which
        # would trade one wrong write for another.
        #
        # ⚠ The `NOT IN ('closed','failed')` predicate is PRE-EXISTING and
        # preserved, not replaced -- this gate is additional.
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
    # ⚠ The STORED row, not the attempted write. These differ exactly in the
    # guarded case, and a caller that believes a demotion happened is the same
    # defect one layer up. `error_code` follows the same rule: reporting the late
    # failure's code against a preserved terminal state would describe a row that
    # does not exist.
    return ReconciliationResult(
        order_id,
        stored_state,
        str(persisted_ref) if persisted_ref is not None else None,
        stored["broker_status"],
        tuple(int(value) for value in persisted["position_ids"]),
        stored["last_error_code"],
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
    state, order_status = classify_broker_order_status(broker_status)

    if prior_state in _TERMINAL_RECONCILIATION_STATES and prior_state != state:
        raise StrategyReconciliationError("broker order attempted to regress or change terminal state")

    if state == "rejected" and detail.position_executions:
        raise StrategyReconciliationError("rejected broker order unexpectedly has position executions")
    if state == "resolved" and purpose == "entry" and not detail.position_executions:
        raise StrategyReconciliationError("filled strategy entry has no exact position executions")

    # ⚠ Ascending position id, not the broker's order. Two orders whose details
    # name the same two positions in OPPOSITE order would each insert one
    # `strategy_position_ownership` row and then block on the other's unique-index
    # conflict -- a deadlock across two DIFFERENT per-order locks, which the
    # per-order lock cannot prevent. A deterministic order makes it a queue.
    for execution in sorted(detail.position_executions, key=lambda item: item.position_id):
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
        # Sorted, to match both the write order above and the `ORDER BY
        # broker_position_id` every other result path reads back.
        tuple(sorted(execution.position_id for execution in detail.position_executions)),
    )


def reconcile_strategy_order(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    order_id: int,
) -> ReconciliationResult:
    """Poll and reconcile one linked strategy order without any broker write.

    Raises :class:`StrategyReconciliationBusy` if another reconciler or a
    submitter already holds this order (#2964 item 2).
    """
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise StrategyReconciliationError(
            "reconciliation requires an idle connection so broker I/O cannot run inside a DB transaction"
        )
    # ⚠ The lock is taken BEFORE the identity read, not around the writes. That
    # ordering is what makes two workers which selected the same due row safe:
    # the loser reads the state the winner just wrote and short-circuits on the
    # terminal branch below, instead of re-polling the broker for an order that
    # is already settled.
    with try_reconciliation_order_lock(conn, order_id):
        return _reconcile_locked_strategy_order(conn, broker=broker, order_id=order_id)


def _reconcile_locked_strategy_order(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    order_id: int,
) -> ReconciliationResult:
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
    retry_base_seconds: int = RECONCILIATION_RETRY_BASE_SECONDS,
    retry_cap_seconds: int = RECONCILIATION_RETRY_CAP_SECONDS,
) -> tuple[ReconciliationResult, ...]:
    """Reconcile a bounded least-recently-attempted backlog (#2948).

    Selection is a round robin over ``last_attempt_at``, which every attempt
    path writes.  The previous ``ORDER BY first_unresolved_at, order_id`` sorted
    on two keys that never change for a non-terminal row, so once the oldest
    ``limit`` orders were stuck the order at ``limit + 1`` was never visited
    again -- an absorbing state, not a delay.

    A capped exponential cooldown then keeps a permanently dead order off the
    shared 60-requests/minute eToro trading-read budget.  It applies only to the
    no-progress states, because ``_apply_detail`` increments the same
    ``attempt_count`` on a successful poll that leaves an order ``pending``.

    Declared bound, conditional on a finite stable backlog, monotonic timestamps
    and no aborted batch: once a row is DUE it is selected within
    ``ceil(due_rows / limit)`` completed cycles.  The cooldown and that queue
    wait are ADDITIVE, and more than ``limit`` never-attempted rows still take
    more than one cycle -- see the spec, both were overstated in its first draft.

    ``first_unresolved_at`` is deliberately never written, so the age
    ``enforce_reconciliation_slo`` measures cannot be reset to look healthy.
    """
    if limit < 1 or limit > 100:
        raise ValueError("limit must be between 1 and 100")
    if retry_base_seconds < 1:
        raise ValueError("retry_base_seconds must be positive")
    if retry_cap_seconds < retry_base_seconds:
        raise ValueError("retry_cap_seconds must be at least retry_base_seconds")
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
              AND (
                    state.state NOT IN ('not_found', 'ambiguous', 'error')
                 OR state.last_attempt_at IS NULL
                 OR state.last_attempt_at <= now() - make_interval(secs => least(
                        %(cap)s::double precision,
                        %(base)s::double precision
                            * greatest(
                                power(2, least(state.attempt_count - 1, %(exponent_cap)s)) - 0.5, 0)))
              )
            ORDER BY state.last_attempt_at ASC NULLS FIRST,
                     state.first_unresolved_at, state.order_id
            LIMIT %(limit)s
            """,
            {
                "limit": limit,
                "base": retry_base_seconds,
                "cap": retry_cap_seconds,
                "exponent_cap": _RECONCILIATION_RETRY_EXPONENT_CAP,
            },
        )
        rows = cur.fetchall()
    conn.commit()
    results: list[ReconciliationResult] = []
    skipped_busy = 0
    for row in rows:
        order_id = int(row["order_id"])
        try:
            results.append(reconcile_strategy_order(conn, broker=broker, order_id=order_id))
        except StrategyReconciliationBusy:
            # Someone else holds this order: an attended resume, or a submitter
            # mid-flight. There is nothing to record -- `_record_failure` here
            # would invent an error that did not happen -- and `last_attempt_at`
            # is deliberately left alone, so the row keeps its place at the front
            # of the next rotation rather than being pushed to the back for
            # somebody else's work.
            skipped_busy += 1
            continue
        except Exception:  # noqa: BLE001 - one poison order must not abort the batch
            # ``reconcile_strategy_order`` models four failure classes; anything
            # else (a psycopg error, an unmodelled broker exception) used to
            # abort the whole batch mid-generator AND leave last_attempt_at
            # unchanged, so the same row re-selected first forever. Advance the
            # attempt clock and carry on: the trade is still marked
            # ``reconcile_required`` and the SLO block still fires on age.
            # The escaping exception may have left an aborted transaction.
            conn.rollback()
            # ⚠ RE-ACQUIRE. `reconcile_strategy_order`'s context manager has
            # already unwound and released by the time this handler runs, so
            # without this the fallback would write the reconciliation row
            # unlocked -- the exact race the lock exists to close. A busy
            # re-acquire means somebody else took the order the instant we
            # failed; skip rather than race them.
            try:
                with try_reconciliation_order_lock(conn, order_id):
                    with conn.transaction():
                        results.append(
                            _record_failure(
                                conn,
                                order_id=order_id,
                                state="error",
                                error_code="reconcile_unexpected_error",
                            )
                        )
            except StrategyReconciliationBusy:
                skipped_busy += 1
    if skipped_busy:
        # ⚠ #2948's absorbing-state lesson applies to contention too: with `b`
        # busy rows the declared selection bound degrades to
        # `ceil(due_rows / (limit - b))`, and an all-busy batch returns empty --
        # which is indistinguishable from an empty backlog unless it is said.
        logger.info(
            "reconciliation backlog skipped %s of %s selected order(s) held by another reconciler",
            skipped_busy,
            len(rows),
        )
    return tuple(results)


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
    "StrategyReconciliationBusy",
    "StrategyReconciliationError",
    "enforce_reconciliation_slo",
    "ensure_strategy_request_id",
    "reconcile_backlog",
    "reconcile_strategy_order",
    "reconciliation_order_lock",
    "try_reconciliation_order_lock",
]
