"""Attended, demo-only execution path for the deterministic core/cash sleeve."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Final, Literal
from uuid import UUID, uuid4

import psycopg
from psycopg.pq import TransactionStatus

from app.providers.broker import (
    BrokerCoreOrder,
    BrokerOrderSubmissionError,
    BrokerOrderSubmissionUncertain,
    BrokerProvider,
)
from app.services.strategy_control_plane import link_strategy_order, load_paper_pool
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
from app.services.strategy_order_reconciliation import (
    reconcile_strategy_order,
    reconciliation_order_lock,
)

CoreExecutionState = Literal["held", "refused", "submitted", "submission_uncertain"]

CoreOrderLinkPurpose = Literal["entry", "exit"]

#: The ``(orders.action, strategy_trade_orders.purpose)`` pair each allocator action has a
#: BUILT submission path for.  Keyed by action so the write derives its side instead of
#: asserting one, and an action with no path refuses rather than borrowing another's.
_CORE_ORDER_SHAPE: Final[dict[str, tuple[str, CoreOrderLinkPurpose]]] = {
    "buy_core": ("BUY", "entry"),
}


def core_order_shape_for(action: str) -> tuple[str, CoreOrderLinkPurpose] | None:
    """The order side and link purpose to persist for an allocator ``action``.

    ``None`` means the action has no submission path and MUST be refused before durable
    order authority exists.

    ⚠⚠ This is a fail-closed backstop, and the reason it is needed is that every other
    layer here is buy-shaped while the gates ahead of it are sell-AWARE.
    ``admit_core_rebalance_intent`` admits ``sell_core`` (it spends a dedicated
    ``core_partial_close_unproved`` check on it), ``preflight_core_submission`` accepts it
    as a known action, and ``orders.action`` is bare ``TEXT`` with no CHECK -- so the only
    thing refusing a sell is one ``return`` in ``strategy_core_broker_preflight``, upstream
    of the write.  Lifting that refusal is step one of the sell leg (#2603 item 3), and
    without this map the next layer down would persist the sell as a ``'BUY'`` linked
    ``purpose="entry"`` and hand it to ``place_demo_core_order`` -- whose
    ``BrokerCoreOrder`` says in its own docstring that it is the buy-only shape.  A
    rebalance meant to REDUCE exposure would increase it, every field internally
    consistent and no refusal reachable (#3003).

    ⚠ Today this changes nothing: ``sell_core`` never reaches the caller's check, because
    the broker preflight refuses it first with the more informative
    ``core_close_side_cost_quote_unavailable``.  That ordering is deliberate -- a backstop
    that pre-empts the specific refusal would cost the operator the diagnosis.
    """
    return _CORE_ORDER_SHAPE.get(action)


logger = logging.getLogger(__name__)


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


def _observe_core_portfolio_drawdown(
    conn: psycopg.Connection[Any],
    *,
    equity: Decimal,
    observed_at: datetime,
    max_drawdown_pct: Decimal,
) -> str | None:
    """Advance the shared account high-water mark and enforce the pool mandate.

    The periodic health job derives its policy from enabled alpha deployments. A
    core-only pool therefore cannot rely on its global ``drawdown`` block. This
    submission-local observation uses the same shared risk-state row and refuses
    before durable order authority is created.
    """
    if (
        not equity.is_finite()
        or equity <= 0
        or not max_drawdown_pct.is_finite()
        or not (Decimal("0") < max_drawdown_pct < Decimal("100"))
        or observed_at.tzinfo is None
    ):
        return "core_account_risk_unobservable"
    row = conn.execute(
        """
        SELECT equity_high_water, observed_at
        FROM strategy_paper_account_risk_state
        WHERE id=true
        FOR UPDATE
        """
    ).fetchone()
    if row is not None and row[1] > observed_at:
        return "core_account_risk_stale"
    previous_high_water = Decimal(str(row[0])) if row is not None else equity
    if not previous_high_water.is_finite() or previous_high_water <= 0:
        return "core_account_risk_unobservable"
    high_water = max(previous_high_water, equity)
    drawdown = (high_water - equity) / high_water * Decimal("100")
    conn.execute(
        """
        INSERT INTO strategy_paper_account_risk_state (
            id,equity_high_water,last_equity,last_drawdown_pct,observed_at
        ) VALUES (true,%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
          equity_high_water=EXCLUDED.equity_high_water,
          last_equity=EXCLUDED.last_equity,
          last_drawdown_pct=EXCLUDED.last_drawdown_pct,
          observed_at=EXCLUDED.observed_at
        """,
        (high_water, equity, drawdown, observed_at),
    )
    return "portfolio_drawdown_limit" if drawdown >= max_drawdown_pct else None


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


def core_authority_is_stranded(conn: psycopg.Connection[Any], *, order_id: int) -> bool:
    """Has this core authority been looked up and found absent at the broker?

    Read-only, and deliberately NOT part of :class:`CoreResumeAuthority` — the
    execution path must never branch on this. It answers one operator-facing
    question: is telling the operator that "the next attended action resumes or
    reconciles that exact order" still honest for this row?

    True means both of:

    * ``orders.broker_order_ref IS NULL`` -- acceptance was never persisted, so
      this authority is a candidate for the crash window between the durable
      commit and ``place_demo_core_order`` (#2961);
    * the reconciliation state is ``not_found`` -- a lookup has ALREADY run and
      the broker said it has no such order.

    ⚠ True is NOT proof the broker never received it. ``orders:lookup``'s
    ``referenceId`` coverage is undocumented (the #2942 half-2 capability
    question), so a miss is an observation, not an absence proof -- which is
    exactly why resubmission stays refused and why this returns a flag for a
    read surface rather than a verdict for a writer.
    """
    row = conn.execute(
        """
        SELECT o.broker_order_ref, state.state
        FROM strategy_order_reconciliation_state state
        JOIN orders o ON o.order_id = state.order_id
        WHERE state.order_id = %s
        """,
        (order_id,),
    ).fetchone()
    # Same discipline as `load_core_resume_authority`: a read helper on a shared
    # connection must not leave a transaction open. `resume_core_submission`
    # refuses a non-idle connection outright, so omitting this turns a read into
    # a failure of the very action the caller is deciding whether to offer.
    conn.commit()
    if row is None:
        return False
    return row[0] is None and str(row[1]) == "not_found"


def _persist_core_acceptance(
    conn: psycopg.Connection[Any],
    *,
    authority: CoreResumeAuthority,
    broker_order_ref: str,
    response_digest: str,
) -> CoreExecutionResult:
    # ⚠ Stated lock ordering: orders -> reconciliation state -> strategy_trades.
    #
    # ⚠⚠ `state='pending'` is written WITHOUT clearing `reconciled_at`, which is
    # safe only because the caller holds this order's reconciliation lock across
    # the whole submission (#2964 item 4). A reconciler resolving the order
    # inside this window would leave a terminal `reconciled_at` beside a
    # non-terminal state, which `strategy_order_reconciliation_resolved_shape`
    # (sql/285:75) refuses -- failing the attended request. The lock makes that
    # schedule unreachable; a terminal-preserving CASE here would not, because
    # the correct outcome in that schedule is not "keep the terminal state"
    # either. Do not remove the lock and leave this statement.
    with conn.transaction():
        conn.execute(
            "UPDATE orders SET broker_order_ref=%s WHERE order_id=%s",
            (broker_order_ref, authority.order_id),
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
        conn.execute(
            "UPDATE strategy_trades SET status='submitted', updated_at=now() WHERE strategy_trade_id=%s",
            (authority.trade_id,),
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
    """Submit one durable authority while the caller retains the core lock.

    ⚠ #2964 items 2-4. The per-order reconciliation lock is taken for the WHOLE
    submission, from before broker I/O until after persistence, because the
    ``orders`` and reconciliation rows are already committed by the time this
    runs. Without it a reconciler can resolve the order inside this window, and
    ``_persist_core_acceptance``'s ``state='pending'`` then violates
    ``strategy_order_reconciliation_resolved_shape`` (``reconciled_at`` is set
    but the state is not terminal) and fails the attended request.

    ⚠ It BLOCKS rather than trying: refusing here would abandon a durable
    authority that ``resume_core_submission`` is deliberately forbidden to
    resubmit, wedging the core arm. ``core_submission_lock`` is already held by
    the caller and is acquired first; this lock is always last.
    """
    with reconciliation_order_lock(conn, authority.order_id):
        return _submit_core_authority_locked(conn, broker=broker, authority=authority)


def _submit_core_authority_locked(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    authority: CoreResumeAuthority,
) -> CoreExecutionResult:
    try:
        submission = broker.place_demo_core_order(
            BrokerCoreOrder(instrument_id=authority.instrument_id, amount=authority.amount),
            request_id=authority.request_id,
        )
    except BrokerOrderSubmissionError as exc:
        uncertain = isinstance(exc, BrokerOrderSubmissionUncertain)
        # ⚠ Statement order follows the module lock ordering stated in
        # `strategy_order_reconciliation`: orders -> reconciliation state ->
        # strategy_trades. These branches used to write the trade first.
        with conn.transaction():
            if uncertain:
                conn.execute(
                    """
                    UPDATE strategy_order_reconciliation_state
                    SET state='error', last_attempt_at=now(), attempt_count=attempt_count+1,
                        last_error_code='broker_submission_uncertain', updated_at=now()
                    WHERE order_id=%s
                    """,
                    (authority.order_id,),
                )
                conn.execute(
                    """
                    UPDATE strategy_trades SET status='reconcile_required', updated_at=now()
                    WHERE strategy_trade_id=%s
                    """,
                    (authority.trade_id,),
                )
            else:
                conn.execute("UPDATE orders SET status='rejected' WHERE order_id=%s", (authority.order_id,))
                conn.execute(
                    """
                    UPDATE strategy_order_reconciliation_state
                    SET state='rejected', reconciled_at=now(), last_attempt_at=now(),
                        attempt_count=attempt_count+1, last_error_code='broker_submission_rejected', updated_at=now()
                    WHERE order_id=%s
                    """,
                    (authority.order_id,),
                )
                conn.execute(
                    "UPDATE strategy_trades SET status='failed', updated_at=now() WHERE strategy_trade_id=%s",
                    (authority.trade_id,),
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
        # Stated lock ordering: reconciliation state before strategy_trades.
        with conn.transaction():
            conn.execute(
                """
                UPDATE strategy_order_reconciliation_state
                SET state='error', last_attempt_at=now(), attempt_count=attempt_count+1,
                    last_error_code='broker_submission_exception', updated_at=now()
                WHERE order_id=%s
                """,
                (authority.order_id,),
            )
            conn.execute(
                """
                UPDATE strategy_trades SET status='reconcile_required', updated_at=now()
                WHERE strategy_trade_id=%s
                """,
                (authority.trade_id,),
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
            # ⚠ REFUSED, not raised (#2979 half b).  An inconsistent shared population is
            # a steady state -- true on this cycle and every later one -- so a caller
            # gets a reason code it can act on rather than a 409 whose only text is the
            # OUTER sentence.  Still fail-closed: nothing is submitted either way.
            logger.warning("core rebalance refused as %s (%s)", exc.reason_code, exc)
            return _result("refused", exc.reason_code)
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
        except EngineCapitalObservationError as exc:
            # ⚠ This arm MUST precede the blanket one below and must not absorb it.  The
            # blanket arm still covers a failed broker read (an execution fault) and
            # `CoreSleeveObservationError` (input drift in one payload, which its own
            # docstring puts on the raising side).  Only the capital join's refusal --
            # steady state, unchanged by a retry -- becomes a verdict.
            #
            # ⚠⚠ Reachability, so nobody reads more into this than it does: in the
            # #2979 wedge `core_active_position_ids` is non-empty, so `read_core_sleeve`
            # leaves `capital_ready=False` and the operator's Rebalance button is
            # DISABLED (`app/api/strategies.py`, `StrategyPortfolioLens.tsx`).  This
            # verdict is reachable today only by calling the endpoint directly.  The
            # caller that reaches the same refusal on every unattended tick is the paper
            # cycle, which is fixed in `strategy_paper_executor._risk_and_amount`.
            logger.warning("core rebalance refused as %s (%s)", exc.reason_code, exc)
            return _result("refused", exc.reason_code)
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
                # ⚠ Deliberately still a RAISE, unlike the two sites above (#2979 half
                # b).  Reaching here means the first load and the broker observation
                # both succeeded and the population moved underneath them -- a
                # concurrency fault, and the message is the only place that WHEN is
                # recorded.  Returning the same code as the first load would discard it.
                raise StrategyCoreExecutionError("the assigned-capital sandbox changed during preflight") from exc
            if current_capital is None or current_capital != capital_authority:
                raise StrategyCoreExecutionError("the assigned-capital sandbox changed during broker preflight")
            current_pool = load_paper_pool(conn)
            if current_pool.event_id != current_capital.pool_event_id:
                raise StrategyCoreExecutionError("the portfolio mandate changed during broker preflight")
            drawdown_limit = current_pool.mandate.max_portfolio_drawdown_pct
            if drawdown_limit is None:
                raise StrategyCoreExecutionError("the portfolio mandate is unconfigured")
            require_selected_core_instrument(conn, instrument_id=current.core_instrument_id)
            intent = record_core_rebalance_intent(conn, state=state, recorded_by=recorded_by)
            intent_id = intent.core_rebalance_intent_id
            # Observe EVERY attended evaluation, not only buys. Otherwise an
            # in-band hold can be the account peak, and a later fall is measured
            # from an older lower watermark. That understates drawdown precisely
            # when this core-only path cannot rely on alpha's periodic health job.
            initial_drawdown_refusal = _observe_core_portfolio_drawdown(
                conn,
                equity=snapshot.equity,
                observed_at=snapshot.observed_at,
                max_drawdown_pct=drawdown_limit,
            )
            if initial_drawdown_refusal is not None:
                return _result("refused", initial_drawdown_refusal, intent_id=intent_id)
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
            snapshot_observed_at = broker_verdict.snapshot_observed_at
            broker_evidence_age = (
                None if snapshot_observed_at is None else (clock() - snapshot_observed_at).total_seconds()
            )
            if broker_evidence_age is None or not (
                0 <= broker_evidence_age <= broker_verdict.max_account_risk_age_seconds
            ):
                return _result("refused", "core_account_risk_stale", intent_id=intent_id)
            account_equity = broker_verdict.account_equity
            if account_equity is None or snapshot_observed_at is None:
                return _result("refused", "core_account_risk_unobservable", intent_id=intent_id)
            drawdown_refusal = _observe_core_portfolio_drawdown(
                conn,
                equity=account_equity,
                observed_at=snapshot_observed_at,
                max_drawdown_pct=drawdown_limit,
            )
            if drawdown_refusal is not None:
                return _result("refused", drawdown_refusal, intent_id=intent_id)

            # Last gate before durable authority: the write derives its own side.  See
            # `core_order_shape_for` for why an upstream-only refusal is not enough.
            order_shape = core_order_shape_for(intent.decision.action)
            if order_shape is None:
                return _result("refused", "core_submission_action_unbuilt", intent_id=intent_id)
            order_action, order_purpose = order_shape

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
                ) VALUES (%s, %s, 'MARKET', %s, 'submitted', NULL, 'strategy', %s)
                RETURNING order_id
                """,
                (current.core_instrument_id, order_action, amount, request_id),
            ).fetchone()
            if order_row is None:
                raise StrategyCoreExecutionError("core order INSERT did not return an id")
            order_id = int(order_row[0])
            link_strategy_order(conn, strategy_trade_id=trade_id, order_id=order_id, purpose=order_purpose)
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
    "CoreOrderLinkPurpose",
    "CoreResumeAuthority",
    "StrategyCoreExecutionError",
    "core_order_shape_for",
    "execute_core_rebalance",
    "load_core_resume_authority",
    "resume_core_submission",
]
