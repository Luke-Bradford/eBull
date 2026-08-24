"""Attended, demo-only execution path for the deterministic core/cash sleeve."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID, uuid4

import psycopg
from psycopg.pq import TransactionStatus

from app.providers.broker import (
    BrokerCoreOrder,
    BrokerOrderSubmissionError,
    BrokerOrderSubmissionUncertain,
    BrokerProvider,
)
from app.services.strategy_control_plane import link_strategy_order
from app.services.strategy_core_allocator import evaluate_core_rebalance
from app.services.strategy_core_broker_preflight import (
    CORE_BROKER_PREFLIGHT_POLICY_VERSION,
    assess_core_broker_preflight,
)
from app.services.strategy_core_eligibility import require_core_eligibility
from app.services.strategy_core_mandate import load_core_mandate
from app.services.strategy_core_preflight import CORE_PREFLIGHT_POLICY_VERSION, preflight_core_submission
from app.services.strategy_core_rebalance_intent import record_core_rebalance_intent
from app.services.strategy_core_selection import require_selected_core_instrument
from app.services.strategy_core_sleeve import observe_core_sleeve
from app.services.strategy_core_submission_gate import (
    CORE_SUBMISSION_POLICY_VERSION,
    admit_core_rebalance_intent,
    core_submission_lock,
)
from app.services.strategy_engine_capital import (
    EngineCapitalObservationError,
    load_engine_capital_authority,
    resolve_engine_capital_usage,
)
from app.services.strategy_order_reconciliation import reconcile_strategy_order

CoreExecutionState = Literal["held", "refused", "submitted", "submission_uncertain"]


class StrategyCoreExecutionError(RuntimeError):
    """The attended executor was called without a usable selected mandate."""


@dataclass(frozen=True)
class CoreExecutionResult:
    state: CoreExecutionState
    reason_code: str
    intent_id: int | None
    trade_id: int | None
    order_id: int | None
    amount: Decimal
    submission_policy_version: str = CORE_SUBMISSION_POLICY_VERSION
    preflight_policy_version: str = CORE_PREFLIGHT_POLICY_VERSION
    broker_preflight_policy_version: str = CORE_BROKER_PREFLIGHT_POLICY_VERSION


@dataclass(frozen=True)
class CoreResumeAuthority:
    intent_id: int
    trade_id: int
    order_id: int
    instrument_id: int
    amount: Decimal
    request_id: UUID
    broker_order_ref: str | None
    eligibility_proof_id: int
    operator_id: UUID
    api_key_credential_id: UUID
    user_key_credential_id: UUID


def _result(
    state: CoreExecutionState,
    reason_code: str,
    *,
    intent_id: int | None = None,
    trade_id: int | None = None,
    order_id: int | None = None,
    amount: Decimal = Decimal("0"),
) -> CoreExecutionResult:
    return CoreExecutionResult(state, reason_code, intent_id, trade_id, order_id, amount)


def load_core_resume_authority(conn: psycopg.Connection[Any]) -> CoreResumeAuthority | None:
    """Load the one non-terminal core order and the exact account that owns it."""
    row = conn.execute(
        """
        SELECT t.core_rebalance_intent_id, t.strategy_trade_id, o.order_id,
               o.instrument_id, o.requested_amount, o.strategy_request_id,
               o.broker_order_ref, proof.core_eligibility_proof_id,
               proof.operator_id, proof.api_key_credential_id,
               proof.user_key_credential_id
        FROM strategy_order_reconciliation_state state
        JOIN orders o ON o.order_id=state.order_id
        JOIN strategy_trade_orders link ON link.order_id=o.order_id
        JOIN strategy_trades t ON t.strategy_trade_id=link.strategy_trade_id
        JOIN strategy_core_eligibility_proofs proof
          ON proof.core_eligibility_proof_id=t.core_eligibility_proof_id
        WHERE t.core_rebalance_intent_id IS NOT NULL
          AND state.state NOT IN ('resolved','rejected')
        ORDER BY state.first_unresolved_at, state.order_id
        LIMIT 1
        """
    ).fetchone()
    conn.commit()
    if row is None:
        return None
    if row[0] is None or row[4] is None or row[5] is None:
        raise StrategyCoreExecutionError("core resume authority is incomplete")
    return CoreResumeAuthority(
        intent_id=int(row[0]),
        trade_id=int(row[1]),
        order_id=int(row[2]),
        instrument_id=int(row[3]),
        amount=Decimal(str(row[4])),
        request_id=UUID(str(row[5])),
        broker_order_ref=None if row[6] is None else str(row[6]),
        eligibility_proof_id=int(row[7]),
        operator_id=row[8],
        api_key_credential_id=row[9],
        user_key_credential_id=row[10],
    )


def _persist_core_acceptance(
    conn: psycopg.Connection[Any],
    *,
    authority: CoreResumeAuthority,
    broker_order_ref: str,
    response_digest: str,
) -> CoreExecutionResult:
    with conn.transaction():
        conn.execute(
            "UPDATE orders SET broker_order_ref=%s WHERE order_id=%s",
            (broker_order_ref, authority.order_id),
        )
        conn.execute(
            "UPDATE strategy_trades SET status='submitted', updated_at=now() WHERE strategy_trade_id=%s",
            (authority.trade_id,),
        )
        conn.execute(
            """
            UPDATE strategy_order_reconciliation_state
            SET state='pending', broker_status='accepted', last_attempt_at=now(),
                attempt_count=attempt_count+1, last_payload_sha256=%s,
                last_error_code=NULL, updated_at=now()
            WHERE order_id=%s
            """,
            (response_digest, authority.order_id),
        )
    return _result(
        "submitted",
        "broker_accepted_pending_reconciliation",
        intent_id=authority.intent_id,
        trade_id=authority.trade_id,
        order_id=authority.order_id,
        amount=authority.amount,
    )


def _reconcile_core_authority(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    authority: CoreResumeAuthority,
) -> CoreExecutionResult:
    reconciled = reconcile_strategy_order(conn, broker=broker, order_id=authority.order_id)
    if reconciled.state == "resolved":
        return _result(
            "held",
            "core_order_reconciled",
            intent_id=authority.intent_id,
            trade_id=authority.trade_id,
            order_id=authority.order_id,
            amount=authority.amount,
        )
    if reconciled.state == "rejected":
        return _result(
            "refused",
            "core_order_rejected",
            intent_id=authority.intent_id,
            trade_id=authority.trade_id,
            order_id=authority.order_id,
            amount=authority.amount,
        )
    return _result(
        "submission_uncertain",
        f"core_order_reconciliation_{reconciled.state}",
        intent_id=authority.intent_id,
        trade_id=authority.trade_id,
        order_id=authority.order_id,
        amount=authority.amount,
    )


def resume_core_submission(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    authority: CoreResumeAuthority,
) -> CoreExecutionResult:
    """Reconcile one committed authority without ever retrying its mutation.

    A broker lookup miss is only an observation that the order is not visible
    yet, not proof that an uncertain submission had no effect. It therefore
    remains unresolved and blocks every new core authority.
    """
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise StrategyCoreExecutionError("core resume requires an idle connection")
    with core_submission_lock(conn):
        current = load_core_resume_authority(conn)
        if current is None:
            return _result("held", "core_resume_already_resolved")
        if current.order_id != authority.order_id or current.request_id != authority.request_id:
            raise StrategyCoreExecutionError("core resume authority changed before use")
        return _reconcile_core_authority(conn, broker=broker, authority=current)


def _submit_core_authority(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    authority: CoreResumeAuthority,
) -> CoreExecutionResult:
    """Submit one durable authority while the caller retains the core lock."""
    try:
        submission = broker.place_demo_core_order(
            BrokerCoreOrder(instrument_id=authority.instrument_id, amount=authority.amount),
            request_id=authority.request_id,
        )
    except BrokerOrderSubmissionError as exc:
        uncertain = isinstance(exc, BrokerOrderSubmissionUncertain)
        with conn.transaction():
            if uncertain:
                conn.execute(
                    """
                    UPDATE strategy_trades SET status='reconcile_required', updated_at=now()
                    WHERE strategy_trade_id=%s
                    """,
                    (authority.trade_id,),
                )
                conn.execute(
                    """
                    UPDATE strategy_order_reconciliation_state
                    SET state='error', last_attempt_at=now(), attempt_count=attempt_count+1,
                        last_error_code='broker_submission_uncertain', updated_at=now()
                    WHERE order_id=%s
                    """,
                    (authority.order_id,),
                )
            else:
                conn.execute("UPDATE orders SET status='rejected' WHERE order_id=%s", (authority.order_id,))
                conn.execute(
                    "UPDATE strategy_trades SET status='failed', updated_at=now() WHERE strategy_trade_id=%s",
                    (authority.trade_id,),
                )
                conn.execute(
                    """
                    UPDATE strategy_order_reconciliation_state
                    SET state='rejected', reconciled_at=now(), last_attempt_at=now(),
                        attempt_count=attempt_count+1, last_error_code='broker_submission_rejected', updated_at=now()
                    WHERE order_id=%s
                    """,
                    (authority.order_id,),
                )
        return _result(
            "submission_uncertain" if uncertain else "refused",
            "broker_submission_uncertain" if uncertain else "broker_submission_rejected",
            intent_id=authority.intent_id,
            trade_id=authority.trade_id,
            order_id=authority.order_id,
            amount=authority.amount,
        )
    except Exception:
        # Acceptance may precede an unexpected provider error. Preserve the
        # request UUID and make reconciliation the only safe next action.
        with conn.transaction():
            conn.execute(
                """
                UPDATE strategy_trades SET status='reconcile_required', updated_at=now()
                WHERE strategy_trade_id=%s
                """,
                (authority.trade_id,),
            )
            conn.execute(
                """
                UPDATE strategy_order_reconciliation_state
                SET state='error', last_attempt_at=now(), attempt_count=attempt_count+1,
                    last_error_code='broker_submission_exception', updated_at=now()
                WHERE order_id=%s
                """,
                (authority.order_id,),
            )
        raise

    return _persist_core_acceptance(
        conn,
        authority=authority,
        broker_order_ref=submission.broker_order_ref,
        response_digest=submission.response_digest,
    )


def execute_core_rebalance(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    operator_id: UUID,
    api_key_credential_id: UUID,
    user_key_credential_id: UUID,
    recorded_by: str,
    provider: str = "etoro",
    environment: str = "demo",
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> CoreExecutionResult:
    """Observe, record, admit and submit one guarded core rebalance.

    The decisive broker observation and preflight happen while submissions are
    serialised, so a concurrent fill cannot leave this request sizing from a
    pre-fill snapshot. The durable request UUID, trade, order, proof provenance
    and reconciliation row commit before the sole mutating broker call.
    """
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise StrategyCoreExecutionError("core execution requires an idle connection")
    if environment != "demo":
        raise StrategyCoreExecutionError("core execution is demo-only")

    # Preliminary DB facts are deliberately re-proved under the submission lock
    # below. This short transaction exists only to source the broker reads without
    # keeping row/advisory locks across network I/O.
    with conn.transaction():
        mandate = load_core_mandate(conn)
        if mandate is None or not mandate.enabled or mandate.core_instrument_id is None:
            raise StrategyCoreExecutionError("an enabled core mandate is required")
        require_selected_core_instrument(conn, instrument_id=mandate.core_instrument_id)
        proof = require_core_eligibility(
            conn,
            instrument_id=mandate.core_instrument_id,
            operator_id=operator_id,
            provider=provider,
            environment=environment,
        )
        if (proof.api_key_credential_id, proof.user_key_credential_id) != (
            api_key_credential_id,
            user_key_credential_id,
        ):
            raise StrategyCoreExecutionError("loaded broker credentials do not match the eligibility proof")

    request_id: UUID | None = None
    trade_id: int | None = None
    order_id: int | None = None
    amount = Decimal("0")
    with core_submission_lock(conn):
        try:
            capital_authority = load_engine_capital_authority(conn)
        except EngineCapitalObservationError as exc:
            raise StrategyCoreExecutionError("the assigned-capital sandbox is incomplete") from exc
        if capital_authority is None:
            raise StrategyCoreExecutionError("an assigned paper pot is required")
        if not capital_authority.enabled:
            raise StrategyCoreExecutionError("the assigned paper pot is disabled")
        # Session advisory locks survive this commit; row snapshots do not need
        # to be held across broker I/O because the authority is re-proved below.
        conn.commit()
        # This observation must be inside the same serialised section as the
        # mutation. Otherwise request B can observe, wait while request A fills,
        # then submit a duplicate order from B's still-fresh pre-fill snapshot.
        try:
            snapshot = broker.get_account_risk_snapshot()
            usage = resolve_engine_capital_usage(
                capital_authority,
                snapshot,
                core_instrument_id=mandate.core_instrument_id,
            )
            if not usage.headroom.within_bound:
                return _result("refused", "sandbox_exceeded")
            state = observe_core_sleeve(
                snapshot,
                core_instrument_id=mandate.core_instrument_id,
                exact_owned_market_value=usage.core_market_value,
                assigned_cash_available=min(snapshot.available_cash, usage.headroom.remaining),
            )
        except Exception as exc:
            raise StrategyCoreExecutionError("the broker account snapshot could not describe the core sleeve") from exc
        decision = evaluate_core_rebalance(mandate, state)
        broker_verdict = None
        if decision.action in ("buy_core", "sell_core"):
            broker_verdict = assess_core_broker_preflight(
                broker,
                mandate=mandate,
                decision=decision,
                core_instrument_id=mandate.core_instrument_id,
                capital_authority=capital_authority,
                eligibility_response_currency=proof.response_currency,
                eligibility_min_position_exposure=proof.min_position_exposure,
                eligibility_min_position_amount=proof.min_position_amount,
                clock=clock,
            )
        with conn.transaction():
            current = load_core_mandate(conn)
            if current is None or not current.enabled or current.core_instrument_id is None:
                raise StrategyCoreExecutionError("the core mandate changed before submission")
            if current.event_id != mandate.event_id:
                raise StrategyCoreExecutionError("the core mandate was revised during broker preflight")
            try:
                current_capital = load_engine_capital_authority(conn)
            except EngineCapitalObservationError as exc:
                raise StrategyCoreExecutionError("the assigned-capital sandbox changed during preflight") from exc
            if current_capital != capital_authority:
                raise StrategyCoreExecutionError("the assigned-capital sandbox changed during broker preflight")
            require_selected_core_instrument(conn, instrument_id=current.core_instrument_id)
            intent = record_core_rebalance_intent(conn, state=state, recorded_by=recorded_by)
            intent_id = intent.core_rebalance_intent_id
            if intent.decision.action == "hold":
                return _result("held", intent.decision.reason_code or "core_hold", intent_id=intent_id)
            if intent.decision.action == "refused":
                return _result("refused", intent.decision.reason_code or "core_allocator_refused", intent_id=intent_id)

            admission = admit_core_rebalance_intent(
                conn,
                intent_id=intent_id,
                operator_id=operator_id,
                provider=provider,
                environment=environment,
            )
            if not admission.admitted or admission.eligibility_proof_id is None:
                return _result("refused", admission.reason_code or "core_submission_refused", intent_id=intent_id)
            binding_proof = require_core_eligibility(
                conn,
                instrument_id=current.core_instrument_id,
                operator_id=operator_id,
                provider=provider,
                environment=environment,
            )
            if (
                binding_proof.proof_id != proof.proof_id
                or binding_proof.proof_id != admission.eligibility_proof_id
                or (
                    binding_proof.api_key_credential_id,
                    binding_proof.user_key_credential_id,
                )
                != (api_key_credential_id, user_key_credential_id)
            ):
                return _result("refused", "core_credential_provenance_changed", intent_id=intent_id)

            db_preflight = preflight_core_submission(
                conn,
                core_instrument_id=current.core_instrument_id,
                action=intent.decision.action,
                now=clock(),
            )
            if not db_preflight.admitted:
                return _result("refused", db_preflight.reason_code or "core_preflight_refused", intent_id=intent_id)
            if broker_verdict is None or not broker_verdict.admitted:
                reason = None if broker_verdict is None else broker_verdict.reason_code
                return _result("refused", reason or "core_broker_preflight_refused", intent_id=intent_id)
            broker_evidence_age = (
                None
                if broker_verdict.snapshot_observed_at is None
                else (clock() - broker_verdict.snapshot_observed_at).total_seconds()
            )
            if broker_evidence_age is None or not (
                0 <= broker_evidence_age <= broker_verdict.max_account_risk_age_seconds
            ):
                return _result("refused", "core_account_risk_stale", intent_id=intent_id)

            amount = broker_verdict.amount
            request_id = uuid4()
            trade_row = conn.execute(
                """
                INSERT INTO strategy_trades (
                    core_rebalance_intent_id, core_eligibility_proof_id,
                    instrument_id, status
                ) VALUES (%s, %s, %s, 'planned')
                RETURNING strategy_trade_id
                """,
                (intent_id, binding_proof.proof_id, current.core_instrument_id),
            ).fetchone()
            if trade_row is None:
                raise StrategyCoreExecutionError("core trade INSERT did not return an id")
            trade_id = int(trade_row[0])
            order_row = conn.execute(
                """
                INSERT INTO orders (
                    instrument_id, action, order_type, requested_amount, status,
                    raw_payload_json, execution_origin, strategy_request_id
                ) VALUES (%s, 'BUY', 'MARKET', %s, 'submitted', NULL, 'strategy', %s)
                RETURNING order_id
                """,
                (current.core_instrument_id, amount, request_id),
            ).fetchone()
            if order_row is None:
                raise StrategyCoreExecutionError("core order INSERT did not return an id")
            order_id = int(order_row[0])
            link_strategy_order(conn, strategy_trade_id=trade_id, order_id=order_id, purpose="entry")
            conn.execute(
                "INSERT INTO strategy_order_reconciliation_state (order_id) VALUES (%s)",
                (order_id,),
            )

        if request_id is None or trade_id is None or order_id is None:
            raise StrategyCoreExecutionError("core submission authority was not persisted")
        return _submit_core_authority(
            conn,
            broker=broker,
            authority=CoreResumeAuthority(
                intent_id=intent_id,
                trade_id=trade_id,
                order_id=order_id,
                instrument_id=mandate.core_instrument_id,
                amount=amount,
                request_id=request_id,
                broker_order_ref=None,
                eligibility_proof_id=binding_proof.proof_id,
                operator_id=operator_id,
                api_key_credential_id=api_key_credential_id,
                user_key_credential_id=user_key_credential_id,
            ),
        )


__all__ = [
    "CoreExecutionResult",
    "CoreResumeAuthority",
    "StrategyCoreExecutionError",
    "execute_core_rebalance",
    "load_core_resume_authority",
    "resume_core_submission",
]
