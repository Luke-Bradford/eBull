"""Operator-governed strategy promotion, allocation, and exact ownership.

This module is intentionally broker-free.  It creates the durable authority a
later executor must prove, but cannot place, patch, or close an order itself.
Manual order paths remain in :mod:`app.services.order_client` and never acquire
strategy ownership by instrument coincidence.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal, cast

import psycopg
import psycopg.rows

Stage = Literal[
    "research_candidate",
    "historical_validated",
    "forward_observation",
    "paper_enabled",
    "live_enabled",
    "paused",
    "retired",
]
Mode = Literal["paper", "live"]

GOVERNANCE_GATE_VERSION = "strategy-governance-v1"

# Two-key Postgres advisory-lock namespace.  2454 is this issue's stable,
# documented namespace; hashtext(strategy identity) supplies the per-version
# key.  Do not reuse 2454 for an unrelated advisory lock.
_ADVISORY_LOCK_NAMESPACE = 2454

_NEXT_STAGE: dict[Stage | None, frozenset[Stage]] = {
    None: frozenset({"research_candidate"}),
    "research_candidate": frozenset({"historical_validated", "paused"}),
    "historical_validated": frozenset({"forward_observation", "paused"}),
    "forward_observation": frozenset({"paper_enabled", "paused"}),
    "paper_enabled": frozenset({"live_enabled", "paused"}),
    "live_enabled": frozenset({"paused"}),
    "paused": frozenset({"retired"}),
    "retired": frozenset(),
}

_RESULT_EVIDENCE_STAGES = frozenset({"historical_validated", "forward_observation"})
_EXTERNAL_EVIDENCE_STAGES = frozenset({"historical_validated", "forward_observation", "paper_enabled", "live_enabled"})


class StrategyControlError(ValueError):
    """A fail-closed control-plane refusal."""


class StrategyOwnershipError(StrategyControlError):
    """The exact broker position is not actively owned by this trade."""


@dataclass(frozen=True)
class Promotion:
    promotion_id: int
    strategy_id: str
    strategy_version: str
    from_stage: Stage | None
    to_stage: Stage


@dataclass(frozen=True)
class Deployment:
    deployment_id: int
    strategy_id: str
    strategy_version: str
    mode: Mode
    capital_limit: Decimal
    enabled: bool
    revision: int


def _require_text(value: str, field: str) -> None:
    if not value.strip():
        raise StrategyControlError(f"{field} must be non-empty")


def _lock_strategy(conn: psycopg.Connection[Any], strategy_id: str, strategy_version: str) -> None:
    conn.execute(
        "SELECT pg_advisory_xact_lock(%s, hashtext(%s))",
        (_ADVISORY_LOCK_NAMESPACE, f"{strategy_id}\x1f{strategy_version}"),
    )


def current_stage(conn: psycopg.Connection[Any], strategy_id: str, strategy_version: str) -> Stage | None:
    row = conn.execute(
        """
        SELECT to_stage
        FROM strategy_promotions
        WHERE strategy_id = %s AND strategy_version = %s
        ORDER BY promotion_id DESC
        LIMIT 1
        """,
        (strategy_id, strategy_version),
    ).fetchone()
    return cast(Stage, row[0]) if row is not None else None


def promote_strategy(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    to_stage: Stage,
    promoted_by: str,
    reason: str,
    evidence_ref: str | None = None,
    result_ids: Sequence[int] = (),
    gate_version: str = GOVERNANCE_GATE_VERSION,
) -> Promotion:
    """Append one explicit, ordered promotion event.

    Evidence is pinned, never inferred from a current metric.  Result ids must
    belong to this exact strategy version.  Global auto/live switches are not
    read here and therefore cannot create or advance a promotion.
    """
    for value, field in (
        (strategy_id, "strategy_id"),
        (strategy_version, "strategy_version"),
        (promoted_by, "promoted_by"),
        (reason, "reason"),
        (gate_version, "gate_version"),
    ):
        _require_text(value, field)
    if evidence_ref is not None:
        _require_text(evidence_ref, "evidence_ref")
    if len(set(result_ids)) != len(result_ids):
        raise StrategyControlError("result_ids must be unique")

    _lock_strategy(conn, strategy_id, strategy_version)
    from_stage = current_stage(conn, strategy_id, strategy_version)
    if to_stage not in _NEXT_STAGE[from_stage]:
        raise StrategyControlError(f"invalid promotion transition: {from_stage!r} -> {to_stage!r}")

    if to_stage in _EXTERNAL_EVIDENCE_STAGES and evidence_ref is None:
        raise StrategyControlError(f"{to_stage} requires an immutable evidence_ref")
    if to_stage in _RESULT_EVIDENCE_STAGES and not result_ids:
        raise StrategyControlError(f"{to_stage} requires at least one pinned result_id")

    if result_ids:
        rows = conn.execute(
            """
            SELECT result_id
            FROM strategy_results_store
            WHERE strategy_id = %s AND strategy_version = %s
              AND result_id = ANY(%s)
            """,
            (strategy_id, strategy_version, list(result_ids)),
        ).fetchall()
        found = {int(row[0]) for row in rows}
        missing = set(result_ids) - found
        if missing:
            raise StrategyControlError(
                f"result_ids do not belong to {strategy_id}@{strategy_version}: {sorted(missing)}"
            )

    row = conn.execute(
        """
        INSERT INTO strategy_promotions (
            strategy_id, strategy_version, from_stage, to_stage, gate_version,
            evidence_ref, promoted_by, reason
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING promotion_id
        """,
        (
            strategy_id,
            strategy_version,
            from_stage,
            to_stage,
            gate_version,
            evidence_ref,
            promoted_by,
            reason,
        ),
    ).fetchone()
    assert row is not None
    promotion_id = int(row[0])
    if result_ids:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO strategy_promotion_results (promotion_id, result_id)
                VALUES (%s, %s)
                """,
                [(promotion_id, result_id) for result_id in result_ids],
            )
    return Promotion(promotion_id, strategy_id, strategy_version, from_stage, to_stage)


def configure_deployment(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    mode: Mode,
    capital_limit: Decimal,
    enabled: bool,
    changed_by: str,
    reason: str,
    currency: str = "USD",
) -> Deployment:
    """Create/update one operator capital ceiling and append its audit event."""
    for value, field in (
        (strategy_id, "strategy_id"),
        (strategy_version, "strategy_version"),
        (changed_by, "changed_by"),
        (reason, "reason"),
        (currency, "currency"),
    ):
        _require_text(value, field)
    if capital_limit < 0:
        raise StrategyControlError("capital_limit must be non-negative")

    _lock_strategy(conn, strategy_id, strategy_version)
    stage = current_stage(conn, strategy_id, strategy_version)
    eligible: dict[Mode, frozenset[Stage]] = {
        "paper": frozenset({"paper_enabled", "live_enabled"}),
        "live": frozenset({"live_enabled"}),
    }
    if enabled and stage not in eligible[mode]:
        raise StrategyControlError(f"{mode} deployment cannot be enabled at stage {stage!r}")

    existing = conn.execute(
        """
        SELECT deployment_id, revision
        FROM strategy_deployments
        WHERE strategy_id = %s AND strategy_version = %s AND mode = %s
        FOR UPDATE
        """,
        (strategy_id, strategy_version, mode),
    ).fetchone()
    if existing is None:
        revision = 1
        row = conn.execute(
            """
            INSERT INTO strategy_deployments (
                strategy_id, strategy_version, mode, capital_limit, currency,
                enabled, revision, updated_by, reason
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING deployment_id
            """,
            (
                strategy_id,
                strategy_version,
                mode,
                capital_limit,
                currency,
                enabled,
                revision,
                changed_by,
                reason,
            ),
        ).fetchone()
        assert row is not None
        deployment_id = int(row[0])
    else:
        deployment_id = int(existing[0])
        revision = int(existing[1]) + 1
        conn.execute(
            """
            UPDATE strategy_deployments
            SET capital_limit = %s, currency = %s, enabled = %s,
                revision = %s, updated_by = %s, reason = %s, updated_at = now()
            WHERE deployment_id = %s
            """,
            (capital_limit, currency, enabled, revision, changed_by, reason, deployment_id),
        )

    conn.execute(
        """
        INSERT INTO strategy_deployment_events (
            deployment_id, revision, capital_limit, currency, enabled,
            changed_by, reason
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (deployment_id, revision, capital_limit, currency, enabled, changed_by, reason),
    )
    return Deployment(
        deployment_id,
        strategy_id,
        strategy_version,
        mode,
        capital_limit,
        enabled,
        revision,
    )


def decide_funding(
    conn: psycopg.Connection[Any],
    *,
    signal_id: int,
    verdict: Literal["allocated", "rejected"],
    reason_code: str,
    deployment_id: int | None = None,
    amount: Decimal | None = None,
    detail: str | None = None,
) -> int:
    """Persist the sole funding verdict for a durable fired signal."""
    _require_text(reason_code, "reason_code")
    if detail is not None:
        _require_text(detail, "detail")
    signal = conn.execute(
        """
        SELECT strategy_id, strategy_version, signal_kind, verdict
        FROM strategy_signals WHERE signal_id = %s
        FOR UPDATE
        """,
        (signal_id,),
    ).fetchone()
    if signal is None or signal[3] != "fired":
        raise StrategyControlError("funding decisions require a fired durable signal")

    if verdict == "allocated":
        if signal[2] != "entry":
            raise StrategyControlError("capital may only be allocated to an entry signal")
        if deployment_id is None or amount is None or amount <= 0:
            raise StrategyControlError("allocated verdict requires deployment_id and positive amount")
        _lock_strategy(conn, str(signal[0]), str(signal[1]))
        deployment = conn.execute(
            """
            SELECT strategy_id, strategy_version, mode, capital_limit, enabled
            FROM strategy_deployments WHERE deployment_id = %s
            FOR UPDATE
            """,
            (deployment_id,),
        ).fetchone()
        if deployment is None or not bool(deployment[4]):
            raise StrategyControlError("allocation requires an enabled deployment")
        if (deployment[0], deployment[1]) != (signal[0], signal[1]):
            raise StrategyControlError("signal and deployment strategy versions do not match")
        stage = current_stage(conn, str(signal[0]), str(signal[1]))
        mode = cast(Mode, deployment[2])
        eligible: dict[Mode, frozenset[Stage]] = {
            "paper": frozenset({"paper_enabled", "live_enabled"}),
            "live": frozenset({"live_enabled"}),
        }
        if stage not in eligible[mode]:
            raise StrategyControlError(f"{mode} funding cannot be allocated at strategy stage {stage!r}")

        # The deployment lock serialises the reservation read with concurrent
        # decisions. Decisions with no trade are pending; reconciliations stay
        # reserved. Closed/failed trades release their allocation capacity.
        reserved_row = conn.execute(
            """
            SELECT COALESCE(SUM(d.amount), 0)
            FROM strategy_funding_decisions d
            LEFT JOIN strategy_trades t
              ON t.funding_decision_id = d.funding_decision_id
            WHERE d.deployment_id = %s AND d.verdict = 'allocated'
              AND (t.strategy_trade_id IS NULL OR t.status NOT IN ('closed', 'failed'))
            """,
            (deployment_id,),
        ).fetchone()
        assert reserved_row is not None
        reserved = Decimal(str(reserved_row[0]))
        if reserved + amount > Decimal(str(deployment[3])):
            raise StrategyControlError("allocation exceeds the deployment capital_limit")
    elif deployment_id is not None or amount is not None:
        raise StrategyControlError("rejected verdict cannot reserve deployment capital")

    row = conn.execute(
        """
        INSERT INTO strategy_funding_decisions (
            signal_id, deployment_id, verdict, amount, reason_code, detail
        ) VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING funding_decision_id
        """,
        (signal_id, deployment_id, verdict, amount, reason_code, detail),
    ).fetchone()
    assert row is not None
    return int(row[0])


def create_strategy_trade(conn: psycopg.Connection[Any], funding_decision_id: int) -> int:
    """Create a planned trade from one allocated decision; derive its instrument."""
    row = conn.execute(
        """
        SELECT d.verdict, s.instrument_id
        FROM strategy_funding_decisions d
        JOIN strategy_signals s ON s.signal_id = d.signal_id
        WHERE d.funding_decision_id = %s
        """,
        (funding_decision_id,),
    ).fetchone()
    if row is None or row[0] != "allocated":
        raise StrategyControlError("a strategy trade requires an allocated funding decision")
    created = conn.execute(
        """
        INSERT INTO strategy_trades (funding_decision_id, instrument_id)
        VALUES (%s, %s)
        RETURNING strategy_trade_id
        """,
        (funding_decision_id, row[1]),
    ).fetchone()
    assert created is not None
    return int(created[0])


def link_strategy_order(
    conn: psycopg.Connection[Any],
    *,
    strategy_trade_id: int,
    order_id: int,
    purpose: Literal["entry", "exit", "stop_loss", "take_profit", "stop_ratchet", "reconcile"],
) -> int:
    """Link an explicitly strategy-origin order to a same-instrument trade."""
    row = conn.execute(
        """
        SELECT t.instrument_id, o.instrument_id, o.execution_origin
        FROM strategy_trades t CROSS JOIN orders o
        WHERE t.strategy_trade_id = %s AND o.order_id = %s
        """,
        (strategy_trade_id, order_id),
    ).fetchone()
    if row is None:
        raise StrategyControlError("strategy trade or order does not exist")
    if row[2] != "strategy":
        raise StrategyControlError("manual orders cannot be linked to strategy trades")
    if row[0] != row[1]:
        raise StrategyControlError("strategy trade and order instruments do not match")
    linked = conn.execute(
        """
        INSERT INTO strategy_trade_orders (strategy_trade_id, order_id, purpose)
        VALUES (%s, %s, %s)
        RETURNING strategy_trade_order_id
        """,
        (strategy_trade_id, order_id, purpose),
    ).fetchone()
    assert linked is not None
    return int(linked[0])


def claim_exact_position(
    conn: psycopg.Connection[Any],
    *,
    strategy_trade_id: int,
    entry_order_id: int,
    broker_position_id: int,
) -> int:
    """Claim the exact position returned for this trade's strategy entry order."""
    row = conn.execute(
        """
        SELECT 1
        FROM strategy_trades t
        JOIN strategy_trade_orders sto
          ON sto.strategy_trade_id = t.strategy_trade_id
         AND sto.order_id = %s AND sto.purpose = 'entry'
        JOIN orders o ON o.order_id = sto.order_id AND o.execution_origin = 'strategy'
        JOIN strategy_order_position_executions execution
          ON execution.order_id = o.order_id
         AND execution.broker_position_id = %s
        WHERE t.strategy_trade_id = %s
        """,
        (entry_order_id, broker_position_id, strategy_trade_id),
    ).fetchone()
    if row is None:
        raise StrategyOwnershipError("position claim requires the exact strategy entry order and broker position id")
    claimed = conn.execute(
        """
        INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id)
        VALUES (%s, %s)
        RETURNING ownership_id
        """,
        (strategy_trade_id, broker_position_id),
    ).fetchone()
    assert claimed is not None
    return int(claimed[0])


def record_order_position_execution(conn: psycopg.Connection[Any], *, order_id: int, broker_position_id: int) -> None:
    """Persist one exact position id returned by detailed strategy-order lookup.

    The future reconciler owns the broker call. This broker-free control-plane
    method records its result and refuses manual-origin orders.
    """
    if broker_position_id <= 0:
        raise StrategyOwnershipError("broker_position_id must be positive")
    row = conn.execute(
        "SELECT execution_origin FROM orders WHERE order_id = %s",
        (order_id,),
    ).fetchone()
    if row is None or row[0] != "strategy":
        raise StrategyOwnershipError("position executions may be recorded only for a strategy-origin order")
    conn.execute(
        """
        INSERT INTO strategy_order_position_executions (order_id, broker_position_id)
        VALUES (%s, %s)
        """,
        (order_id, broker_position_id),
    )


def assert_exact_position_owned(
    conn: psycopg.Connection[Any], *, strategy_trade_id: int, broker_position_id: int
) -> None:
    """Fail unless this exact trade/id pair has active strategy ownership."""
    row = conn.execute(
        """
        SELECT 1 FROM strategy_position_ownership
        WHERE strategy_trade_id = %s AND broker_position_id = %s
          AND status = 'active'
        """,
        (strategy_trade_id, broker_position_id),
    ).fetchone()
    if row is None:
        raise StrategyOwnershipError(
            f"broker position {broker_position_id} is not actively owned by strategy trade {strategy_trade_id}"
        )


def release_exact_position(
    conn: psycopg.Connection[Any],
    *,
    strategy_trade_id: int,
    broker_position_id: int,
    reason: str,
) -> None:
    """Release only the exact active ownership pair, preserving its history."""
    _require_text(reason, "reason")
    row = conn.execute(
        """
        UPDATE strategy_position_ownership
        SET status = 'released', released_at = now(), release_reason = %s
        WHERE strategy_trade_id = %s AND broker_position_id = %s
          AND status = 'active'
        RETURNING ownership_id
        """,
        (reason, strategy_trade_id, broker_position_id),
    ).fetchone()
    if row is None:
        raise StrategyOwnershipError(
            f"broker position {broker_position_id} is not actively owned by strategy trade {strategy_trade_id}"
        )


__all__ = [
    "Deployment",
    "GOVERNANCE_GATE_VERSION",
    "Promotion",
    "StrategyControlError",
    "StrategyOwnershipError",
    "assert_exact_position_owned",
    "claim_exact_position",
    "configure_deployment",
    "create_strategy_trade",
    "current_stage",
    "decide_funding",
    "link_strategy_order",
    "promote_strategy",
    "record_order_position_execution",
    "release_exact_position",
]
