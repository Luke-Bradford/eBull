"""Exact-position, demo-only strategy risk manager (#2452).

The manager accepts a strategy trade and broker position id together. It never
looks up a position by instrument, so a manual position in the same instrument
is observational risk only and cannot be mutated. Only material PATCH/close
intents are persisted; unchanged bars and polling heartbeats write no rows.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from typing import Any, Literal, cast
from uuid import UUID, uuid4

import psycopg
import psycopg.rows
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from app.providers.broker import (
    BrokerInstrumentEligibility,
    BrokerPosition,
    BrokerPositionMutationError,
    BrokerPositionMutationUncertain,
    BrokerProvider,
)
from app.services.strategy_control_plane import (
    PAPER_ALLOCATOR_ADVISORY_LOCK,
    StrategyControlError,
    StrategyOwnershipError,
    link_strategy_order,
)
from app.services.strategy_core_arc_sql import core_arm_authorised, core_arm_joins

_RATE_QUANTUM = Decimal("0.000001")
_ADVISORY_HASH_SEED = 0


class StrategyPositionManagerError(StrategyControlError):
    """An exact-position mutation cannot safely proceed."""


@dataclass(frozen=True)
class RatchetBar:
    """Completed causal inputs for the registered hybrid ratchet formula."""

    completed_at: datetime
    level_known_at: datetime
    close: Decimal
    highest_close_since_entry: Decimal
    atr: Decimal
    broken_resistance: Decimal


@dataclass(frozen=True)
class PositionManagerResult:
    strategy_trade_id: int
    broker_position_id: int
    state: Literal["no_change", "submitted", "pending", "applied", "rejected", "reconcile_required"]
    reason_code: str
    position_operation_id: int | None = None


@dataclass(frozen=True)
class _OwnedPosition:
    ownership_id: int
    strategy_trade_id: int
    broker_position_id: int
    instrument_id: int
    # ⚠ The four fields below became nullable in #2603 step 2, when
    # ``strategy_trades`` gained the core/cash arm (sql/349).  A core position is
    # authorised by a mandate rebalance intent, so it has no deployment, no
    # entry preflight and no execution policy to read them from.
    #
    # ``is_core`` is the discriminator and is derived from the AUTHORISATION
    # column, never from these being NULL.  Null-by-absence would conflate "this
    # is a core holding" with "this deployment configured no such policy", and a
    # later default would then silently start applying strategy behaviour to a
    # mandate holding.
    is_core: bool
    deployment_id: int | None
    entry_stop: Decimal | None
    entry_take_profit: Decimal | None
    max_position_age_seconds: int | None
    max_quote_age_seconds: int | None
    ratchet_variant_id: int | None
    break_atr_multiple: Decimal | None
    chandelier_atr_multiple: Decimal | None
    structure_atr_multiple: Decimal | None
    quote_bid: Decimal | None
    quoted_at: datetime | None


@contextmanager
def _position_lock(conn: psycopg.Connection[Any], broker_position_id: int) -> Iterator[None]:
    # PostgreSQL's two-key advisory lock accepts signed 32-bit integers only,
    # while broker position ids are valid BIGINTs. Hash the namespaced identity
    # in PostgreSQL so every valid position id maps to one signed 64-bit key.
    lock_identity = f"strategy-position:{broker_position_id}"
    conn.execute(
        "SELECT pg_advisory_lock(hashtextextended(%s, %s))",
        (lock_identity, _ADVISORY_HASH_SEED),
    )
    conn.commit()
    try:
        yield
    finally:
        if conn.info.transaction_status != TransactionStatus.IDLE:
            conn.rollback()
        unlocked = conn.execute(
            "SELECT pg_advisory_unlock(hashtextextended(%s, %s))",
            (lock_identity, _ADVISORY_HASH_SEED),
        ).fetchone()
        conn.commit()
        if unlocked is None or unlocked[0] is not True:
            raise StrategyPositionManagerError("strategy position lock ownership was lost")


@contextmanager
def _paper_allocator_lock(conn: psycopg.Connection[Any]) -> Iterator[None]:
    """Serialize exact-position exits with every shared-pot commitment."""
    conn.execute("SELECT pg_advisory_lock(%s, %s)", PAPER_ALLOCATOR_ADVISORY_LOCK)
    conn.commit()
    try:
        yield
    finally:
        if conn.info.transaction_status != TransactionStatus.IDLE:
            conn.rollback()
        unlocked = conn.execute("SELECT pg_advisory_unlock(%s, %s)", PAPER_ALLOCATOR_ADVISORY_LOCK).fetchone()
        conn.commit()
        if unlocked is None or unlocked[0] is not True:
            raise StrategyPositionManagerError("paper allocator lock ownership was lost")


def register_ratchet_variant(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    promotion_id: int,
    rule_version: str,
    break_atr_multiple: Decimal,
    chandelier_atr_multiple: Decimal,
    structure_atr_multiple: Decimal,
    registered_by: str,
    reason: str,
) -> int:
    """Register immutable formula constants against a promoted backtest arm."""
    row = conn.execute(
        """
        SELECT p.strategy_id, p.strategy_version, p.to_stage,
               count(pr.result_id) AS result_count,
               count(pr.result_id) FILTER (
                   WHERE r.position_rule_set_version = %s
                     AND r.namespace = 'hold_out'
                     AND r.window_start >= DATE '2022-01-01'
               ) AS matching_recent_holdout_count
        FROM strategy_promotions p
        LEFT JOIN strategy_promotion_results pr ON pr.promotion_id=p.promotion_id
        LEFT JOIN strategy_results_store r ON r.result_id=pr.result_id
        WHERE p.promotion_id=%s
        GROUP BY p.promotion_id
        """,
        (rule_version, promotion_id),
    ).fetchone()
    if row is None or row[0] != strategy_id or row[1] != strategy_version:
        raise StrategyPositionManagerError("ratchet registration must match its promoted strategy variant")
    if (
        row[2] not in ("historical_validated", "forward_observation", "paper_enabled", "live_enabled")
        or int(row[3]) < 1
        or int(row[4]) < 1
    ):
        raise StrategyPositionManagerError("ratchet requires a promoted 2022+ hold-out backtest arm")
    inserted = conn.execute(
        """
        INSERT INTO strategy_ratchet_variants (
            strategy_id, strategy_version, promotion_id, rule_version,
            break_atr_multiple, chandelier_atr_multiple, structure_atr_multiple,
            registered_by, reason
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING ratchet_variant_id
        """,
        (
            strategy_id,
            strategy_version,
            promotion_id,
            rule_version,
            break_atr_multiple,
            chandelier_atr_multiple,
            structure_atr_multiple,
            registered_by,
            reason,
        ),
    ).fetchone()
    assert inserted is not None
    return int(inserted[0])


def configure_position_manager(
    conn: psycopg.Connection[Any],
    *,
    deployment_id: int,
    max_position_age_seconds: int | None,
    ratchet_variant_id: int | None,
    updated_by: str,
    reason: str,
) -> int:
    """Replace current paper-manager policy and append one bounded audit event."""
    if max_position_age_seconds is not None and max_position_age_seconds <= 0:
        raise ValueError("max_position_age_seconds must be positive")
    deployment = conn.execute(
        "SELECT strategy_id, strategy_version, mode FROM strategy_deployments WHERE deployment_id=%s FOR UPDATE",
        (deployment_id,),
    ).fetchone()
    if deployment is None or deployment[2] != "paper":
        raise StrategyPositionManagerError("the MVP position manager requires a paper deployment")
    if ratchet_variant_id is not None:
        variant = conn.execute(
            "SELECT strategy_id, strategy_version FROM strategy_ratchet_variants WHERE ratchet_variant_id=%s",
            (ratchet_variant_id,),
        ).fetchone()
        if variant is None or tuple(variant) != tuple(deployment[:2]):
            raise StrategyPositionManagerError("ratchet variant must match the deployment strategy identity")
    prior = conn.execute(
        "SELECT revision FROM strategy_position_manager_policies WHERE deployment_id=%s",
        (deployment_id,),
    ).fetchone()
    revision = int(prior[0]) + 1 if prior else 1
    conn.execute(
        """
        INSERT INTO strategy_position_manager_policies (
            deployment_id, revision, max_position_age_seconds,
            ratchet_variant_id, updated_by, reason
        ) VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (deployment_id) DO UPDATE SET
            revision=EXCLUDED.revision,
            max_position_age_seconds=EXCLUDED.max_position_age_seconds,
            ratchet_variant_id=EXCLUDED.ratchet_variant_id,
            updated_by=EXCLUDED.updated_by,
            reason=EXCLUDED.reason,
            updated_at=now()
        """,
        (deployment_id, revision, max_position_age_seconds, ratchet_variant_id, updated_by, reason),
    )
    conn.execute(
        """
        INSERT INTO strategy_position_manager_policy_events (
            deployment_id, revision, max_position_age_seconds,
            ratchet_variant_id, changed_by, reason
        ) VALUES (%s,%s,%s,%s,%s,%s)
        """,
        (deployment_id, revision, max_position_age_seconds, ratchet_variant_id, updated_by, reason),
    )
    return revision


def calculate_ratchet_stop(
    *,
    current_stop: Decimal,
    bar: RatchetBar,
    break_atr_multiple: Decimal,
    chandelier_atr_multiple: Decimal,
    structure_atr_multiple: Decimal,
) -> Decimal | None:
    """Return the registered causal candidate, or ``None`` when it did not fire."""
    values = (
        current_stop,
        bar.close,
        bar.highest_close_since_entry,
        bar.atr,
        bar.broken_resistance,
        break_atr_multiple,
        chandelier_atr_multiple,
        structure_atr_multiple,
    )
    if any(not value.is_finite() or value <= 0 for value in values):
        raise ValueError("ratchet prices and constants must be finite and positive")
    if bar.completed_at.tzinfo is None or bar.level_known_at.tzinfo is None:
        raise ValueError("ratchet timestamps must be timezone aware")
    if bar.level_known_at > bar.completed_at:
        raise ValueError("the resistance level was not causal at bar completion")
    if bar.highest_close_since_entry < bar.close:
        raise ValueError("highest close since entry cannot be below the completed close")
    if bar.close < bar.broken_resistance + break_atr_multiple * bar.atr:
        return None
    candidate = min(
        bar.highest_close_since_entry - chandelier_atr_multiple * bar.atr,
        bar.broken_resistance - structure_atr_multiple * bar.atr,
    ).quantize(_RATE_QUANTUM, rounding=ROUND_DOWN)
    return candidate if candidate > current_stop else None


# The manager's loader.  Signal-arm behaviour is unchanged: the former INNER
# chain (funding -> deployment mode='paper' -> preflight verdict='allocated' ->
# execution policy) is reproduced exactly by the LEFT chain plus the four
# witnesses below.  Converting an INNER JOIN that also FILTERS into a LEFT JOIN
# moves that filter into the WHERE clause or deletes it; there is no third
# outcome, so each is restated rather than assumed.
_LOAD_OWNED_SQL = f"""
            SELECT own.ownership_id, own.strategy_trade_id, own.broker_position_id,
                   t.instrument_id, d.deployment_id,
                   t.core_rebalance_intent_id,
                   pre.stop_loss_rate AS entry_stop,
                   pre.take_profit_rate AS entry_take_profit,
                   manager.max_position_age_seconds,
                   execution.max_quote_age_seconds,
                   manager.ratchet_variant_id,
                   variant.break_atr_multiple,
                   variant.chandelier_atr_multiple,
                   variant.structure_atr_multiple,
                   q.bid AS quote_bid, q.quoted_at
            FROM strategy_position_ownership own
            JOIN strategy_trades t ON t.strategy_trade_id=own.strategy_trade_id
            LEFT JOIN strategy_funding_decisions funding
              ON funding.funding_decision_id=t.funding_decision_id
            LEFT JOIN strategy_deployments d
              ON d.deployment_id=funding.deployment_id AND d.mode='paper'
            LEFT JOIN strategy_entry_preflights pre
              ON pre.signal_id=funding.signal_id AND pre.verdict='allocated'
            LEFT JOIN strategy_execution_policies execution
              ON execution.deployment_id=d.deployment_id
{core_arm_joins("t")}
            LEFT JOIN strategy_position_manager_policies manager ON manager.deployment_id=d.deployment_id
            LEFT JOIN strategy_ratchet_variants variant
              ON variant.ratchet_variant_id=manager.ratchet_variant_id
            LEFT JOIN quotes q ON q.instrument_id=t.instrument_id
            WHERE own.strategy_trade_id=%s AND own.broker_position_id=%s AND own.status='active'
              AND t.status IN ('open','closing','reconcile_required')
              AND (
                (
                  -- ⚠ EVERY LINK NEEDS ITS OWN WITNESS.  `pre` joins on
                  -- funding.signal_id and is independent of `d`, so witnessing
                  -- `pre` alone would let a LIVE deployment load (d NULL, pre
                  -- resolved).  `execution` likewise carries the quote-age
                  -- policy that the casts below assume; without its own witness
                  -- a signal trade could load with max_quote_age_seconds NULL
                  -- and fail at the quote check instead of never loading.
                  t.funding_decision_id IS NOT NULL
                  AND d.deployment_id IS NOT NULL          -- carries mode='paper'
                  AND execution.deployment_id IS NOT NULL  -- carries the quote-age policy
                  AND pre.signal_id IS NOT NULL            -- carries verdict='allocated'
                )
                OR (
                  {core_arm_authorised("t")}
                  -- ⚠ The arc alone does not bind the trade's instrument to the
                  -- intent's.  Without this, a malformed core trade would
                  -- authorise the manager to close a DIFFERENT instrument's
                  -- position.  The two live in different tables, so this is a
                  -- load-time predicate rather than a CHECK -- and here is where
                  -- the manager is about to act on it.
                  AND core_intent.core_instrument_id=t.instrument_id
                )
              )
"""


def _load_owned(conn: psycopg.Connection[Any], *, strategy_trade_id: int, broker_position_id: int) -> _OwnedPosition:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            _LOAD_OWNED_SQL,
            (strategy_trade_id, broker_position_id),
        )
        row = cur.fetchone()
    if row is None:
        raise StrategyOwnershipError("exact active strategy position ownership is required before broker I/O")
    is_core = row["core_rebalance_intent_id"] is not None
    return _OwnedPosition(
        ownership_id=int(row["ownership_id"]),
        strategy_trade_id=int(row["strategy_trade_id"]),
        broker_position_id=int(row["broker_position_id"]),
        instrument_id=int(row["instrument_id"]),
        is_core=is_core,
        deployment_id=int(row["deployment_id"]) if row["deployment_id"] is not None else None,
        entry_stop=Decimal(str(row["entry_stop"])) if row["entry_stop"] is not None else None,
        entry_take_profit=(Decimal(str(row["entry_take_profit"])) if row["entry_take_profit"] is not None else None),
        max_position_age_seconds=(
            int(row["max_position_age_seconds"]) if row["max_position_age_seconds"] is not None else None
        ),
        max_quote_age_seconds=(int(row["max_quote_age_seconds"]) if row["max_quote_age_seconds"] is not None else None),
        ratchet_variant_id=(int(row["ratchet_variant_id"]) if row["ratchet_variant_id"] is not None else None),
        break_atr_multiple=(Decimal(str(row["break_atr_multiple"])) if row["break_atr_multiple"] is not None else None),
        chandelier_atr_multiple=(
            Decimal(str(row["chandelier_atr_multiple"])) if row["chandelier_atr_multiple"] is not None else None
        ),
        structure_atr_multiple=(
            Decimal(str(row["structure_atr_multiple"])) if row["structure_atr_multiple"] is not None else None
        ),
        quote_bid=Decimal(str(row["quote_bid"])) if row["quote_bid"] is not None else None,
        quoted_at=cast(datetime | None, row["quoted_at"]),
    )


def _exact_broker_position(broker: BrokerProvider, owned: _OwnedPosition) -> BrokerPosition | None:
    portfolio = broker.get_portfolio()
    matches = [position for position in portfolio.positions if position.position_id == owned.broker_position_id]
    if len(matches) > 1:
        raise StrategyPositionManagerError("broker returned duplicate exact position ids")
    if matches and matches[0].instrument_id != owned.instrument_id:
        raise StrategyPositionManagerError("owned broker position changed instrument identity")
    return matches[0] if matches else None


def _eligibility_for_owned(broker: BrokerProvider, owned: _OwnedPosition) -> BrokerInstrumentEligibility:
    response = broker.check_instrument_eligibility([owned.instrument_id])
    matches = [row for row in response.eligibilities if row.instrument_id == owned.instrument_id]
    if response.currency.upper() != "USD" or len(matches) != 1:
        raise StrategyPositionManagerError("current broker eligibility is unresolved")
    return matches[0]


def _terminal(
    conn: psycopg.Connection[Any],
    *,
    operation_id: int,
    status: Literal["applied", "rejected", "reconcile_required"],
    error_code: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE strategy_position_operations
        SET status=%s, last_error_code=%s, resolved_at=now(), updated_at=now()
        WHERE position_operation_id=%s AND status IN ('intent_persisted','submitted')
        """,
        (status, error_code, operation_id),
    )


def _finish_close(conn: psycopg.Connection[Any], *, owned: _OwnedPosition, operation_id: int, reason: str) -> None:
    _terminal(conn, operation_id=operation_id, status="applied")
    conn.execute(
        """
        UPDATE strategy_position_ownership
        SET status='released', released_at=now(), release_reason=%s
        WHERE ownership_id=%s AND status='active'
        """,
        (reason, owned.ownership_id),
    )
    remaining = conn.execute(
        "SELECT count(*) FROM strategy_position_ownership WHERE strategy_trade_id=%s AND status='active'",
        (owned.strategy_trade_id,),
    ).fetchone()
    assert remaining is not None
    conn.execute(
        "UPDATE strategy_trades SET status=%s, updated_at=now() WHERE strategy_trade_id=%s",
        ("closed" if int(remaining[0]) == 0 else "open", owned.strategy_trade_id),
    )


def _persist_operation_response(
    conn: psycopg.Connection[Any], *, operation_id: int, raw_payload: dict[str, Any]
) -> None:
    """Persist the untouched broker object before its adapter normalises it."""
    with conn.transaction():
        conn.execute(
            "UPDATE strategy_position_operations SET broker_response_json=%s, updated_at=now() "
            "WHERE position_operation_id=%s",
            (Jsonb(raw_payload), operation_id),
        )


def _persist_order_response(conn: psycopg.Connection[Any], *, order_id: int, raw_payload: dict[str, Any]) -> None:
    """Persist one close-submission response on its already-durable order."""
    with conn.transaction():
        conn.execute(
            "UPDATE orders SET raw_payload_json=%s WHERE order_id=%s",
            (Jsonb(raw_payload), order_id),
        )


def _resume_operation(
    conn: psycopg.Connection[Any], *, broker: BrokerProvider, owned: _OwnedPosition
) -> PositionManagerResult | None:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT * FROM strategy_position_operations
            WHERE ownership_id=%s AND status IN ('intent_persisted','submitted')
            ORDER BY position_operation_id DESC LIMIT 1
            """,
            (owned.ownership_id,),
        )
        operation = cur.fetchone()
    conn.commit()
    if operation is None:
        return None
    operation_id = int(operation["position_operation_id"])
    # ⚠⚠ THIS FUNCTION RUNS IMMEDIATELY AFTER LOAD, BEFORE ANY GATE, so the core
    # exemption further down `manage_owned_position` cannot protect it.  A
    # pending edit operation attached to a core ownership would otherwise resume
    # a stop / take-profit mutation despite that exemption -- the exemption would
    # hold on every fresh cycle and leak on exactly the crash-recovery path.
    #
    # No such operation can be created today (nothing writes an edit on the core
    # arm), so this is refused rather than handled: a row that cannot legitimately
    # exist should stop the cycle for that position, not be interpreted.
    if owned.is_core and operation["operation_type"] != "close":
        with conn.transaction():
            _terminal(
                conn,
                operation_id=operation_id,
                status="rejected",
                error_code="core_mandate_position_exempt",
            )
        return PositionManagerResult(
            owned.strategy_trade_id,
            owned.broker_position_id,
            "rejected",
            "core_mandate_position_exempt",
            operation_id,
        )
    position = _exact_broker_position(broker, owned)
    if operation["status"] == "intent_persisted":
        # There is no edit/close lookup by request UUID. A crash can occur on
        # either side of broker I/O, so absence/non-landing is ambiguous.
        landed = (
            operation["operation_type"] != "close"
            and position is not None
            and position.stop_loss_rate == Decimal(str(operation["desired_stop_rate"]))
            and (
                operation["desired_take_profit_rate"] is None
                or position.take_profit_rate == Decimal(str(operation["desired_take_profit_rate"]))
            )
        )
        with conn.transaction():
            if landed:
                _terminal(conn, operation_id=operation_id, status="applied")
            else:
                _terminal(
                    conn,
                    operation_id=operation_id,
                    status="reconcile_required",
                    error_code="crash_before_submission_identity",
                )
                conn.execute(
                    "UPDATE strategy_trades SET status='reconcile_required', updated_at=now() "
                    "WHERE strategy_trade_id=%s",
                    (owned.strategy_trade_id,),
                )
        return PositionManagerResult(
            owned.strategy_trade_id,
            owned.broker_position_id,
            "applied" if landed else "reconcile_required",
            "broker_state_matches_intent" if landed else "crash_before_submission_identity",
            operation_id,
        )
    if operation["operation_type"] == "close":
        try:
            detail = broker.get_demo_close_order(
                order_id=str(operation["broker_order_ref"]),
                persist_response=lambda raw: _persist_operation_response(
                    conn, operation_id=operation_id, raw_payload=raw
                ),
            )
        except BrokerPositionMutationError:
            return PositionManagerResult(
                owned.strategy_trade_id, owned.broker_position_id, "pending", "close_lookup_unavailable", operation_id
            )
        if detail.status == "pending":
            return PositionManagerResult(
                owned.strategy_trade_id, owned.broker_position_id, "pending", "broker_close_pending", operation_id
            )
        exact_reference = detail.reference_id is None or detail.reference_id == operation["request_id"]
        exact = detail.status == "filled" and detail.position_ids == (owned.broker_position_id,) and exact_reference
        with conn.transaction():
            order_status = "filled" if exact else "rejected"
            conn.execute("UPDATE orders SET status=%s WHERE order_id=%s", (order_status, operation["order_id"]))
            if exact:
                _finish_close(
                    conn,
                    owned=owned,
                    operation_id=operation_id,
                    reason=str(operation["trigger_code"]),
                )
            else:
                _terminal(
                    conn,
                    operation_id=operation_id,
                    status="reconcile_required",
                    error_code="close_order_did_not_affect_exact_position",
                )
                conn.execute(
                    "UPDATE strategy_trades SET status='reconcile_required', updated_at=now() "
                    "WHERE strategy_trade_id=%s",
                    (owned.strategy_trade_id,),
                )
        return PositionManagerResult(
            owned.strategy_trade_id,
            owned.broker_position_id,
            "applied" if exact else "reconcile_required",
            "exact_position_closed" if exact else "close_order_did_not_affect_exact_position",
            operation_id,
        )
    landed = (
        position is not None
        and position.stop_loss_rate == Decimal(str(operation["desired_stop_rate"]))
        and (
            operation["desired_take_profit_rate"] is None
            or position.take_profit_rate == Decimal(str(operation["desired_take_profit_rate"]))
        )
    )
    if not landed:
        return PositionManagerResult(
            owned.strategy_trade_id, owned.broker_position_id, "pending", "broker_edit_pending", operation_id
        )
    with conn.transaction():
        _terminal(conn, operation_id=operation_id, status="applied")
    return PositionManagerResult(
        owned.strategy_trade_id, owned.broker_position_id, "applied", "broker_edit_applied", operation_id
    )


def _persist_edit_intent(
    conn: psycopg.Connection[Any],
    *,
    owned: _OwnedPosition,
    operation_type: Literal["fixed_exit_repair", "stop_ratchet"],
    trigger_code: Literal["entry_exit_gap", "causal_resistance_break"],
    prior_stop: Decimal | None,
    desired_stop: Decimal,
    desired_take: Decimal | None,
    bar: RatchetBar | None,
) -> tuple[int, UUID]:
    request_id = uuid4()
    row = conn.execute(
        """
        INSERT INTO strategy_position_operations (
            ownership_id, operation_type, trigger_code, request_id, status,
            prior_stop_rate, desired_stop_rate, desired_take_profit_rate,
            completed_bar_at, level_known_at, close_rate,
            highest_close_since_entry, atr_rate, resistance_rate,
            ratchet_variant_id
        ) VALUES (%s,%s,%s,%s,'intent_persisted',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING position_operation_id
        """,
        (
            owned.ownership_id,
            operation_type,
            trigger_code,
            request_id,
            prior_stop,
            desired_stop,
            desired_take,
            bar.completed_at if bar else None,
            bar.level_known_at if bar else None,
            bar.close if bar else None,
            bar.highest_close_since_entry if bar else None,
            bar.atr if bar else None,
            bar.broken_resistance if bar else None,
            owned.ratchet_variant_id if bar else None,
        ),
    ).fetchone()
    assert row is not None
    return int(row[0]), request_id


def _prior_same_edit(
    conn: psycopg.Connection[Any],
    *,
    owned: _OwnedPosition,
    operation_type: Literal["fixed_exit_repair", "stop_ratchet"],
    desired_stop: Decimal,
    desired_take: Decimal | None,
    bar: RatchetBar | None,
) -> PositionManagerResult | None:
    row = conn.execute(
        """
        SELECT position_operation_id, status,
               COALESCE(last_error_code, 'prior_material_operation')
        FROM strategy_position_operations
        WHERE ownership_id=%s AND operation_type=%s
          AND desired_stop_rate=%s
          AND desired_take_profit_rate IS NOT DISTINCT FROM %s
          AND completed_bar_at IS NOT DISTINCT FROM %s
        ORDER BY position_operation_id DESC LIMIT 1
        """,
        (
            owned.ownership_id,
            operation_type,
            desired_stop,
            desired_take,
            bar.completed_at if bar else None,
        ),
    ).fetchone()
    if row is None:
        return None
    state: Literal["rejected", "reconcile_required"] = (
        "reconcile_required" if row[1] == "reconcile_required" else "rejected"
    )
    return PositionManagerResult(
        owned.strategy_trade_id,
        owned.broker_position_id,
        state,
        str(row[2]),
        int(row[0]),
    )


def _submit_edit(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    owned: _OwnedPosition,
    operation_id: int,
    request_id: UUID,
    desired_stop: Decimal,
    desired_take: Decimal | None,
) -> PositionManagerResult:
    try:
        submission = broker.edit_demo_strategy_position(
            position_id=owned.broker_position_id,
            stop_loss_rate=desired_stop,
            take_profit_rate=desired_take,
            request_id=request_id,
            persist_response=lambda raw: _persist_operation_response(conn, operation_id=operation_id, raw_payload=raw),
        )
    except BrokerPositionMutationError as exc:
        uncertain = isinstance(exc, BrokerPositionMutationUncertain)
        with conn.transaction():
            _terminal(
                conn,
                operation_id=operation_id,
                status="reconcile_required" if uncertain else "rejected",
                error_code="broker_edit_uncertain" if uncertain else "broker_edit_rejected",
            )
            if uncertain:
                conn.execute(
                    "UPDATE strategy_trades SET status='reconcile_required', updated_at=now() "
                    "WHERE strategy_trade_id=%s",
                    (owned.strategy_trade_id,),
                )
        return PositionManagerResult(
            owned.strategy_trade_id,
            owned.broker_position_id,
            "reconcile_required" if uncertain else "rejected",
            "broker_edit_uncertain" if uncertain else "broker_edit_rejected",
            operation_id,
        )
    with conn.transaction():
        conn.execute(
            """
            UPDATE strategy_position_operations
            SET status='submitted', broker_operation_id=%s, submitted_at=now(), updated_at=now()
            WHERE position_operation_id=%s AND status='intent_persisted'
            """,
            (submission.operation_id, operation_id),
        )
    # The 202 response is acceptance only. A future invocation re-syncs the
    # exact position before changing this operation to applied.
    return PositionManagerResult(
        owned.strategy_trade_id, owned.broker_position_id, "submitted", "broker_edit_accepted", operation_id
    )


def _submit_close(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    owned: _OwnedPosition,
    trigger_code: Literal["timeout", "strategy_exit", "emergency_risk", "operator_close"],
) -> PositionManagerResult:
    request_id = uuid4()
    with conn.transaction():
        order = conn.execute(
            """
            INSERT INTO orders (
                instrument_id, action, order_type, status, raw_payload_json,
                execution_origin, strategy_request_id
            ) VALUES (%s,'EXIT','MARKET','submitted',NULL,'strategy',%s)
            RETURNING order_id
            """,
            (owned.instrument_id, request_id),
        ).fetchone()
        assert order is not None
        order_id = int(order[0])
        link_strategy_order(conn, strategy_trade_id=owned.strategy_trade_id, order_id=order_id, purpose="exit")
        operation = conn.execute(
            """
            INSERT INTO strategy_position_operations (
                ownership_id, order_id, operation_type, trigger_code, request_id, status
            ) VALUES (%s,%s,'close',%s,%s,'intent_persisted')
            RETURNING position_operation_id
            """,
            (owned.ownership_id, order_id, trigger_code, request_id),
        ).fetchone()
        assert operation is not None
        operation_id = int(operation[0])
        conn.execute(
            "UPDATE strategy_trades SET status='closing', updated_at=now() WHERE strategy_trade_id=%s",
            (owned.strategy_trade_id,),
        )
    try:
        submission = broker.close_demo_strategy_position(
            position_id=owned.broker_position_id,
            instrument_id=owned.instrument_id,
            request_id=request_id,
            persist_response=lambda raw: _persist_order_response(conn, order_id=order_id, raw_payload=raw),
        )
    except BrokerPositionMutationError as exc:
        uncertain = isinstance(exc, BrokerPositionMutationUncertain)
        with conn.transaction():
            conn.execute(
                "UPDATE orders SET status=%s WHERE order_id=%s",
                ("submitted" if uncertain else "rejected", order_id),
            )
            _terminal(
                conn,
                operation_id=operation_id,
                status="reconcile_required" if uncertain else "rejected",
                error_code="broker_close_uncertain" if uncertain else "broker_close_rejected",
            )
            conn.execute(
                "UPDATE strategy_trades SET status=%s, updated_at=now() WHERE strategy_trade_id=%s",
                ("reconcile_required" if uncertain else "open", owned.strategy_trade_id),
            )
        return PositionManagerResult(
            owned.strategy_trade_id,
            owned.broker_position_id,
            "reconcile_required" if uncertain else "rejected",
            "broker_close_uncertain" if uncertain else "broker_close_rejected",
            operation_id,
        )
    with conn.transaction():
        conn.execute(
            "UPDATE orders SET broker_order_ref=%s WHERE order_id=%s",
            (submission.broker_order_ref, order_id),
        )
        conn.execute(
            """
            UPDATE strategy_position_operations
            SET status='submitted', broker_order_ref=%s, submitted_at=now(), updated_at=now()
            WHERE position_operation_id=%s
            """,
            (int(submission.broker_order_ref), operation_id),
        )
    return PositionManagerResult(
        owned.strategy_trade_id, owned.broker_position_id, "submitted", "broker_close_accepted", operation_id
    )


def manage_owned_position(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    strategy_trade_id: int,
    broker_position_id: int,
    ratchet_bar: RatchetBar | None = None,
    close_reason: Literal["strategy_exit", "emergency_risk", "operator_close"] | None = None,
    now: datetime | None = None,
) -> PositionManagerResult:
    """Verify or de-risk one exact owned position.

    Kill switches intentionally do not block this path: they block new risk,
    while fixed-stop repair, ratcheting, timeout and explicit closes reduce
    already-owned risk. Live credentials are refused by the provider adapter.
    """
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise StrategyPositionManagerError("position management requires an idle connection")
    observed_at = (now or datetime.now(UTC)).astimezone(UTC)
    with _paper_allocator_lock(conn), _position_lock(conn, broker_position_id):
        owned = _load_owned(conn, strategy_trade_id=strategy_trade_id, broker_position_id=broker_position_id)
        conn.commit()
        resumed = _resume_operation(conn, broker=broker, owned=owned)
        if resumed is not None:
            return resumed
        position = _exact_broker_position(broker, owned)
        if position is None:
            with conn.transaction():
                conn.execute(
                    "UPDATE strategy_trades SET status='reconcile_required', updated_at=now() "
                    "WHERE strategy_trade_id=%s",
                    (strategy_trade_id,),
                )
            return PositionManagerResult(
                strategy_trade_id, broker_position_id, "reconcile_required", "owned_position_missing"
            )

        # ⚠ Age-out is exempted by an EXPLICIT `is_core` test, never by relying on
        # ``max_position_age_seconds`` being NULL for a core position.  Null-by-
        # absence conflates "core is exempt" with "this deployment configured no
        # age policy", so the day a default is introduced, mandate holdings would
        # silently start being closed for being old.  A core holding has no
        # horizon by construction -- that is what makes it core.
        age_seconds = None if owned.is_core else owned.max_position_age_seconds
        timed_out = (
            age_seconds is not None
            and position.open_date_time is not None
            and position.open_date_time <= observed_at - timedelta(seconds=age_seconds)
        )
        if age_seconds is not None and position.open_date_time is None:
            with conn.transaction():
                conn.execute(
                    "UPDATE strategy_trades SET status='reconcile_required', updated_at=now() "
                    "WHERE strategy_trade_id=%s",
                    (strategy_trade_id,),
                )
            return PositionManagerResult(
                strategy_trade_id,
                broker_position_id,
                "reconcile_required",
                "position_open_time_missing",
            )
        if close_reason is not None or timed_out:
            eligibility = _eligibility_for_owned(broker, owned)
            if not eligibility.allow_close_position:
                return PositionManagerResult(
                    strategy_trade_id, broker_position_id, "rejected", "broker_close_not_allowed"
                )
            return _submit_close(
                conn,
                broker=broker,
                owned=owned,
                trigger_code=close_reason or "timeout",
            )

        # ⚠⚠ THE CORE RETURN MUST PRECEDE THIS LINE, not merely guard the
        # `if stop_gap or take_gap` body: the two lines below already dereference
        # ``entry_stop`` / ``entry_take_profit``, which are NULL on the core arm.
        #
        # What a core position is exempt FROM: stop-forcing, take-profit-forcing
        # and ratcheting.  Each converts a mandate into a strategy.  A stop on a
        # benchmark holding sells the benchmark into a drawdown -- and "return to
        # core/cash" is the outcome the viability plan falls back TO, so a stop
        # underneath it would be giving the fallback a fallback.
        #
        # What it is NOT exempt from, and this is what makes the exemption safe:
        # it is still selected by the paper cycle's batch, loaded here,
        # reconciled when the broker disagrees, and closable on an explicit
        # close_reason.  Exempt from three behaviours, not absent from the system.
        #
        # ⚠ Its own reason code.  Reusing `position_protected` would assert that a
        # stop exists and is adequate; on this arm there is no stop at all.
        if owned.is_core:
            return PositionManagerResult(
                strategy_trade_id, broker_position_id, "no_change", "core_mandate_position_exempt"
            )

        # Past this point the signal arm is guaranteed by _LOAD_OWNED_SQL's
        # witnesses: a loaded non-core position has a preflight and an execution
        # policy, so these are non-null.
        #
        # ⚠ `raise`, NOT `assert`. `python -O` strips asserts, and this guard is
        # what stands between a mis-witnessed load predicate and a `NoneType`
        # comparison inside the stop/take-profit arithmetic below. Stripped, the
        # failure mode is not "no check" but "a confusing TypeError three lines
        # later, in the code that decides where a stop goes".
        if owned.entry_stop is None or owned.entry_take_profit is None or owned.max_quote_age_seconds is None:
            raise StrategyPositionManagerError(
                "a non-core owned position must carry its entry preflight and execution policy"
            )
        current_stop = position.stop_loss_rate
        desired_stop = max(current_stop, owned.entry_stop) if current_stop is not None else owned.entry_stop
        desired_take = owned.entry_take_profit
        stop_gap = position.is_no_stop_loss or current_stop is None or current_stop < owned.entry_stop
        take_gap = position.is_no_take_profit or position.take_profit_rate != desired_take
        if stop_gap or take_gap:
            eligibility = _eligibility_for_owned(broker, owned)
            arms = [
                arm
                for arm in eligibility.leverage_configs
                if arm.settlement_type.lower() == "real" and arm.direction.upper() == "LONG"
            ]
            if len(arms) != 1 or arms[0].allow_edit_stop_loss is not True or arms[0].allow_edit_take_profit is not True:
                return PositionManagerResult(
                    strategy_trade_id, broker_position_id, "rejected", "broker_fixed_exit_edit_not_allowed"
                )
            if (
                owned.quote_bid is None
                or owned.quoted_at is None
                or owned.quoted_at < observed_at - timedelta(seconds=owned.max_quote_age_seconds)
                or owned.quoted_at > observed_at + timedelta(seconds=5)
                or desired_stop >= owned.quote_bid
            ):
                return PositionManagerResult(
                    strategy_trade_id, broker_position_id, "rejected", "fixed_exit_quote_unsafe"
                )
            prior = _prior_same_edit(
                conn,
                owned=owned,
                operation_type="fixed_exit_repair",
                desired_stop=desired_stop,
                desired_take=desired_take,
                bar=None,
            )
            conn.commit()
            if prior is not None:
                return prior
            with conn.transaction():
                operation_id, request_id = _persist_edit_intent(
                    conn,
                    owned=owned,
                    operation_type="fixed_exit_repair",
                    trigger_code="entry_exit_gap",
                    prior_stop=current_stop,
                    desired_stop=desired_stop,
                    desired_take=desired_take,
                    bar=None,
                )
            return _submit_edit(
                conn,
                broker=broker,
                owned=owned,
                operation_id=operation_id,
                request_id=request_id,
                desired_stop=desired_stop,
                desired_take=desired_take,
            )

        if ratchet_bar is None or owned.ratchet_variant_id is None:
            return PositionManagerResult(strategy_trade_id, broker_position_id, "no_change", "position_protected")
        if ratchet_bar.completed_at > observed_at:
            raise ValueError("ratchet bar must be completed before evaluation")
        assert current_stop is not None
        assert owned.break_atr_multiple is not None
        assert owned.chandelier_atr_multiple is not None
        assert owned.structure_atr_multiple is not None
        candidate = calculate_ratchet_stop(
            current_stop=current_stop,
            bar=ratchet_bar,
            break_atr_multiple=owned.break_atr_multiple,
            chandelier_atr_multiple=owned.chandelier_atr_multiple,
            structure_atr_multiple=owned.structure_atr_multiple,
        )
        if candidate is None:
            return PositionManagerResult(strategy_trade_id, broker_position_id, "no_change", "ratchet_not_fired")
        if (
            owned.quote_bid is None
            or owned.quoted_at is None
            or owned.quoted_at < observed_at - timedelta(seconds=owned.max_quote_age_seconds)
            or owned.quoted_at > observed_at + timedelta(seconds=5)
            or candidate >= owned.quote_bid
        ):
            return PositionManagerResult(strategy_trade_id, broker_position_id, "rejected", "ratchet_quote_unsafe")
        eligibility = _eligibility_for_owned(broker, owned)
        arms = [
            arm
            for arm in eligibility.leverage_configs
            if arm.settlement_type.lower() == "real" and arm.direction.upper() == "LONG"
        ]
        if len(arms) != 1 or arms[0].allow_edit_stop_loss is not True:
            return PositionManagerResult(
                strategy_trade_id,
                broker_position_id,
                "rejected",
                "broker_ratchet_not_allowed",
            )
        prior = _prior_same_edit(
            conn,
            owned=owned,
            operation_type="stop_ratchet",
            desired_stop=candidate,
            desired_take=desired_take,
            bar=ratchet_bar,
        )
        conn.commit()
        if prior is not None:
            return prior
        with conn.transaction():
            operation_id, request_id = _persist_edit_intent(
                conn,
                owned=owned,
                operation_type="stop_ratchet",
                trigger_code="causal_resistance_break",
                prior_stop=current_stop,
                desired_stop=candidate,
                desired_take=desired_take,
                bar=ratchet_bar,
            )
        return _submit_edit(
            conn,
            broker=broker,
            owned=owned,
            operation_id=operation_id,
            request_id=request_id,
            desired_stop=candidate,
            desired_take=desired_take,
        )


__all__ = [
    "PositionManagerResult",
    "RatchetBar",
    "StrategyPositionManagerError",
    "calculate_ratchet_stop",
    "configure_position_manager",
    "manage_owned_position",
    "register_ratchet_variant",
]
