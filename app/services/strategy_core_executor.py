"""Attended, demo-only execution path for the deterministic core/cash sleeve."""

from __future__ import annotations

import logging
import os
import socket
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Final, Literal
from uuid import UUID, uuid4

import psycopg
from psycopg.pq import TransactionStatus

from app.providers.broker import (
    BrokerAccountRiskSnapshot,
    BrokerCoreOrder,
    BrokerOrderSubmissionError,
    BrokerOrderSubmissionUncertain,
    BrokerProvider,
    BrokerWhatIfOrder,
)
from app.services.broker_closed_release import RELEASE_REASON
from app.services.broker_settlement_arms import (
    UNDERLYING_SETTLEMENT_TYPE,
    UNLEVERAGED_LEVERAGE,
    effective_open_minimum,
)
from app.services.core_exit_levels import CoreExitLevels, CoreExitLevelsUnderivable, core_exit_levels
from app.services.strategy_control_plane import link_strategy_order, load_paper_pool
from app.services.strategy_core_allocator import CoreMandate, CoreSleeveState, evaluate_core_rebalance
from app.services.strategy_core_broker_preflight import (
    CORE_BROKER_PREFLIGHT_POLICY_VERSION,
    CORE_MAX_ACCOUNT_RISK_AGE_SECONDS,
    assess_core_broker_preflight,
)
from app.services.strategy_core_eligibility import CoreEligibilityProof, require_core_eligibility
from app.services.strategy_core_mandate import load_core_mandate
from app.services.strategy_core_preflight import CORE_PREFLIGHT_POLICY_VERSION, preflight_core_submission
from app.services.strategy_core_rebalance_intent import CoreRebalanceIntent, record_core_rebalance_intent
from app.services.strategy_core_selection import require_selected_core_instrument
from app.services.strategy_core_sizing import QuotedTradeCost, decode_quoted_trade_cost
from app.services.strategy_core_sleeve import observe_core_sleeve
from app.services.strategy_core_submission_gate import (
    CORE_SUBMISSION_POLICY_VERSION,
    admit_core_rebalance_intent,
    core_submission_lock,
)
from app.services.strategy_engine_capital import (
    EngineCapitalObservationError,
    EngineCapitalUsage,
    load_engine_capital_authority,
    resolve_engine_capital_usage,
)
from app.services.strategy_order_reconciliation import (
    reconcile_strategy_order,
    reconciliation_order_lock,
)
from app.services.strategy_position_manager import manage_owned_position

CoreExecutionState = Literal["held", "refused", "submitted", "submission_uncertain", "closed", "reconcile_required"]

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

    ⚠⚠ This is a fail-closed backstop.  ``orders.action`` is bare ``TEXT`` with no CHECK,
    and ``place_demo_core_order``'s ``BrokerCoreOrder`` is the buy-only shape, so a
    ``sell_core`` reaching the authority write would be persisted as a ``'BUY'`` linked
    ``purpose="entry"`` -- a rebalance meant to REDUCE exposure would increase it, every
    field internally consistent (#3003).

    ⚠ Since the #2603 sell leg a ``sell_core`` never reaches this: it branches to
    ``_execute_core_sell`` (a whole close through the position manager) before the buy
    remainder runs.  The map stays buy-only so that branch is the ONLY sell path.
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
    stop_loss_rate: Decimal | None
    """The committed stop.  See :attr:`take_profit_rate` -- the pair is one fact and the
    ``None`` contract below governs both.  ⚠ Split across the two fields deliberately: a
    bare string attaches to the field it FOLLOWS, so a single block after the second
    would leave this one undocumented in every tool that reads them."""

    take_profit_rate: Decimal | None
    """#3284 item 1.  The exit levels this authority was committed with, read back from
    ``strategy_core_entry_exit_levels``.

    ⚠ ``None`` IS A LEGAL VALUE, and making it illegal was a real defect caught at Codex
    checkpoint 2.  An authority committed before ``sql/407`` has no levels row, and the
    ONLY thing that can still happen to it is :func:`resume_core_submission` --  which
    RECONCILES and never resubmits (see its docstring).  Raising here would 500 both
    ``GET /core-sleeve`` and ``POST /core-sleeve/rebalance`` and make that authority
    permanently unreconcilable: a guard against submitting naked, blocking the one action
    that cannot submit anything at all.

    ⚠ The refusal that matters therefore lives at the SUBMIT site
    (:func:`_submit_core_authority_locked`), not at the load site.  "No committed levels"
    must stop a submission; it must not stop a read."""


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


def _capital_refusal_result(exc: EngineCapitalObservationError) -> CoreExecutionResult:
    """One shape for both steady-state capital refusals, logged then returned.

    The log is not decoration: only ``str(exc)`` names the trade or position id, and
    ``CoreExecutionResult`` carries the bucket alone.
    """
    logger.warning("core rebalance refused as %s (%s)", exc.reason_code, exc)
    return _result("refused", exc.reason_code)


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
               proof.user_key_credential_id,
               lv.stop_loss_rate, lv.take_profit_rate
        FROM strategy_order_reconciliation_state state
        JOIN orders o ON o.order_id=state.order_id
        JOIN strategy_trade_orders link ON link.order_id=o.order_id
        JOIN strategy_trades t ON t.strategy_trade_id=link.strategy_trade_id
        JOIN strategy_core_eligibility_proofs proof
          ON proof.core_eligibility_proof_id=t.core_eligibility_proof_id
        LEFT JOIN strategy_core_entry_exit_levels lv ON lv.order_id=o.order_id
        WHERE t.core_rebalance_intent_id IS NOT NULL
          AND state.state NOT IN ('resolved','rejected')
        ORDER BY state.first_unresolved_at, state.order_id
        LIMIT 1
        """
    ).fetchone()
    conn.commit()
    if row is None:
        return None
    # ⚠ LEFT JOIN, and the exit levels are deliberately NOT in the completeness check
    # below.  An INNER JOIN would make a pre-`sql/407` authority INVISIBLE, and adding
    # NULL rates to this raise would make it UNLOADABLE -- both break the only action
    # such an authority can still take, which is reconciliation.  The submit path is
    # where missing levels have to stop something, and that is where they do
    # (`_submit_core_authority_locked`).  #3284 item 1, Codex checkpoint 2.
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
        stop_loss_rate=None if row[11] is None else Decimal(str(row[11])),
        take_profit_rate=None if row[12] is None else Decimal(str(row[12])),
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

    ⚠ True is NOT proof the broker never received it, and the reason is now
    MEASURED rather than open.  ``orders:lookup?referenceId=`` does not resolve a
    v2-submitted order on demo at all: on 2026-09-17 a consented probe submitted
    through ``place_demo_core_order``, the response echoed our ``referenceId``
    exactly, the order FILLED, and the lookup still returned HTTP 404 -- only
    ``orderId`` resolved it (issue #2961).  So a miss is an observation, not an
    absence proof, and resubmission stays refused.

    ⚠ This is NOT the terminalisation discriminator, and must not be used as one.
    Since #2961 the provable case is carried by
    ``strategy_order_reconciliation_state.submission_phase``: only
    ``authority_committed`` proves the broker verb was never entered, and only
    ``terminalise_unsubmitted_core_entry`` may act on it -- under the two locks
    that establish no submitter is in flight.  This function answers the narrower
    read-surface question above and returns a flag, not a verdict.
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


def mark_core_submission_entered(conn: psycopg.Connection[Any], *, order_id: int) -> None:
    """Commit "the core broker verb is about to be entered" BEFORE entering it.

    The #2961 half of #2979's separation, and durable-before-the-call by
    construction: this UPDATE commits, and only then does the caller touch the
    broker.  So a crash leaving ``authority_committed`` PROVES the verb was never
    entered, while ``broker_verb_entered`` proves only that it MAY subsequently
    have been -- never what the broker then did.

    ⚠ ``broker_verb_entered`` is deliberately NOT read as "entered".  The marker
    can only bound its own commit; everything after it (``ResilientClient``'s
    throttle and shared-lock wait, header construction, the socket write) is on
    the unprovable side.  Naming it for what it proves rather than for the line
    that follows it is what keeps the discriminator honest.

    ⚠ Module-level and public rather than inlined, for the same reason
    ``mark_close_submitting`` is: ``tests/fixtures/core_restart_child.py`` has no
    other way to place a fault between the two commits, and a fault it can only
    arm by name is the difference between testing the ordering and asserting it.

    ⚠ Requires an IDLE connection.  ``conn.transaction()`` inside an already-open
    transaction opens a SAVEPOINT, not a transaction, so the "commit" would not be
    durable and the marker would be a lie told in the safe-looking direction.

    ⚠ Requires EXACTLY ONE affected row.  A zero-row UPDATE must not be allowed to
    fall through into the submission: a row that could not record "about to call"
    must not then call.

    The sender's pid, host and the marker's commit instant are written in the same
    UPDATE (``sql/410``). The attended window-B release reads them to prove the
    sender is dead and to time its wait (#2961).
    """
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise StrategyCoreExecutionError("the core submission marker requires an idle connection")
    with conn.transaction():
        updated = conn.execute(
            """
            UPDATE strategy_order_reconciliation_state
            SET submission_phase='broker_verb_entered', updated_at=now(),
                submission_entered_at=clock_timestamp(),
                submission_entered_pid=%s, submission_entered_host=%s
            WHERE order_id=%s AND submission_phase='authority_committed'
            """,
            (os.getpid(), socket.gethostname(), order_id),
        ).rowcount
    if updated != 1:
        raise StrategyCoreExecutionError(
            f"the core submission marker for order {order_id} did not advance exactly one row ({updated})"
        )


def _submit_core_authority_locked(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    authority: CoreResumeAuthority,
) -> CoreExecutionResult:
    # ⚠ OUTSIDE the `try` below, deliberately.  That `try` maps broker failures to
    # `BrokerOrderSubmissionUncertain`/`Error`; a marker failure is neither, and
    # reporting it as an uncertain submission would claim the broker may have been
    # reached by a call that provably never happened.
    #
    # ⚠ This is the LATEST point our own code can reach by name without changing
    # the `BrokerProvider` protocol.  A first draft put it inside the provider via
    # a `mark_submitting` callback; Codex checkpoint 1 falsified the gain -- the
    # blocking `reconciliation_order_lock` acquire in `_submit_core_authority`
    # already precedes this either way, so the callback bought only the unattended
    # guard and body construction, and it was not the last client-side instant
    # anyway (`ResilientClient` throttles after it).
    # ⚠ #3284 item 1, and BEFORE the marker deliberately.  This is where "no committed
    # exit levels" has to stop something: a marked authority reads as "may have reached
    # the broker", so refusing after the marker would cost the order its own provenance
    # to prevent a call that has not happened yet.
    #
    # ⚠ Unreachable today and kept anyway.  `_submit_core_authority` is only entered from
    # `execute_core_rebalance`, which derives the rates in this same lock hold -- so a
    # `None` here means a caller was added that submits a LOADED authority, which is
    # exactly the change that would otherwise reintroduce the naked open this ticket
    # closed.  A raise rather than a refusal because it is a caller-contract breach, not
    # a state of the world.
    if authority.stop_loss_rate is None or authority.take_profit_rate is None:
        raise StrategyCoreExecutionError(
            f"core authority {authority.order_id} has no committed exit levels and must not be submitted"
        )
    mark_core_submission_entered(conn, order_id=authority.order_id)
    try:
        submission = broker.place_demo_core_order(
            BrokerCoreOrder(
                instrument_id=authority.instrument_id,
                amount=authority.amount,
                # ⚠ From the AUTHORITY, never re-derived here.  This line runs on the
                # resume path too, replaying `authority.request_id`; a fresh derivation
                # would put a different body under an accepted idempotency key.
                stop_loss_rate=authority.stop_loss_rate,
                take_profit_rate=authority.take_profit_rate,
            ),
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


#: The only currency ``effective_open_minimum`` states a broker floor in (its docstring:
#: exposure "is always calculated in USD").
_OPEN_MINIMUM_CURRENCY: Final = "USD"

#: An operation the manager has not yet driven to a terminal state.
_UNRESOLVED_OPERATION_STATUSES: Final = ("intent_persisted", "submitting", "submitted")


@dataclass(frozen=True)
class _LinkedClose:
    """The close operation linked to one ``sell_core`` intent (sql/411)."""

    operation_id: int
    status: str
    last_error_code: str | None


def map_core_close_outcome(
    *,
    linked: _LinkedClose | None,
    manager_state: str | None,
    manager_reason: str | None,
    manager_operation_id: int | None,
) -> tuple[CoreExecutionState, str]:
    """The executor's verdict for one rebalance close, read off the LINKED operation.

    Spec §1 step 6.  The manager's result is trusted only to say what happened when no
    operation is linked to this intent; once one is, its stored status decides, so a
    result that belongs to a different operation (a resumed repair, a changed ownership)
    can never be reported as this intent's close.

    ``manager_state`` is ``None`` when the manager RAISED: the id comparison is then
    skipped (there is no result to compare), and a linked row maps by its status alone.
    """
    if linked is None:
        if manager_state == "reconcile_required":
            return "reconcile_required", manager_reason or "core_rebalance_close_unresolved"
        if manager_state == "applied" and manager_reason == RELEASE_REASON:
            return "refused", "core_position_closed_by_broker"
        return "refused", "core_rebalance_close_not_started"
    if manager_state is not None and manager_operation_id != linked.operation_id:
        return "reconcile_required", "core_rebalance_result_mismatch"
    if linked.status == "submitted":
        return "submitted", "core_rebalance_close_submitted"
    if linked.status == "applied":
        return "closed", "core_rebalance_close_applied"
    if linked.status == "rejected":
        return "refused", linked.last_error_code or "core_rebalance_close_rejected"
    # `reconcile_required`, or `intent_persisted`/`submitting` -- which a normal return
    # never leaves behind, so seeing one means the call raised midway: uncertain.
    return "reconcile_required", linked.last_error_code or "core_rebalance_close_unresolved"


def _load_linked_close(conn: psycopg.Connection[Any], *, intent_id: int) -> _LinkedClose | None:
    if conn.info.transaction_status != TransactionStatus.IDLE:
        conn.rollback()
    with conn.transaction():
        row = conn.execute(
            """
            SELECT position_operation_id, status, last_error_code
            FROM strategy_position_operations
            WHERE core_rebalance_intent_id = %s
            """,
            (intent_id,),
        ).fetchone()
    return None if row is None else _LinkedClose(int(row[0]), str(row[1]), row[2])


def _drive_core_close(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    intent_id: int,
    strategy_trade_id: int,
    broker_position_id: int,
    ownership_id: int,
    submit: bool,
) -> CoreExecutionResult:
    """Hand one core close to the manager and map the outcome through the linked row.

    ``submit`` distinguishes the sell path (a new ``core_rebalance`` close) from step 0
    (resume the one already in flight: the paper runtime's call shape).  Called only
    inside ``core_submission_lock``; the manager's allocator acquire is then a re-entry.
    """
    try:
        result = manage_owned_position(
            conn,
            broker=broker,
            strategy_trade_id=strategy_trade_id,
            broker_position_id=broker_position_id,
            close_reason="core_rebalance" if submit else None,
            core_rebalance_intent_id=intent_id if submit else None,
            expected_ownership_id=ownership_id,
        )
    except Exception as exc:
        linked = _load_linked_close(conn, intent_id=intent_id)
        if linked is None:
            # Nothing reached the broker for THIS intent (a `_load_owned` failure lands
            # here).  The caller gets the fault, not a verdict.
            raise StrategyCoreExecutionError("the core rebalance close failed before its operation existed") from exc
        state, reason = map_core_close_outcome(
            linked=linked, manager_state=None, manager_reason=None, manager_operation_id=None
        )
        logger.warning(
            "core rebalance close for intent %d raised %s; operation %d is %s",
            intent_id,
            type(exc).__name__,
            linked.operation_id,
            linked.status,
            exc_info=True,
        )
        return _result(state, reason, intent_id=intent_id, trade_id=strategy_trade_id)
    linked = _load_linked_close(conn, intent_id=intent_id)
    state, reason = map_core_close_outcome(
        linked=linked,
        manager_state=result.state,
        manager_reason=result.reason_code,
        manager_operation_id=result.position_operation_id,
    )
    if linked is None:
        logger.warning(
            "core rebalance close for intent %d did not start: manager %s/%s",
            intent_id,
            result.state,
            result.reason_code,
        )
    return _result(state, reason, intent_id=intent_id, trade_id=strategy_trade_id)


def _resolve_outstanding_core_rebalance_close(
    conn: psycopg.Connection[Any], *, broker: BrokerProvider, credentials: tuple[UUID, UUID]
) -> CoreExecutionResult | None:
    """Spec §1 step 0: drive an in-flight ``core_rebalance`` close before anything else.

    Runs BEFORE the mandate/eligibility block, so a disabled mandate or a changed proof
    cannot block resolving what was already sent.  Only this trigger: repairs and
    operator/emergency closes belong to the paper runtime and the operator endpoint,
    and the sell path's preconditions refuse while any of them is unresolved.

    ⚠ Bound to the credentials the position was bought under.  An exit order carries no
    reconciliation row, so ``sql/373``'s rotation guard does not cover it; driving the
    close through another account's credentials would query the wrong book.  A mismatch
    RAISES (a 409 at the endpoint): nothing is driven, and the close stays quarantined.
    """
    with core_submission_lock(conn):
        with conn.transaction():
            row = conn.execute(
                """
                SELECT op.core_rebalance_intent_id, own.ownership_id, own.status,
                       own.strategy_trade_id, own.broker_position_id,
                       proof.api_key_credential_id, proof.user_key_credential_id
                FROM strategy_position_operations op
                JOIN strategy_position_ownership own ON own.ownership_id = op.ownership_id
                JOIN strategy_trades t ON t.strategy_trade_id = own.strategy_trade_id
                LEFT JOIN strategy_core_eligibility_proofs proof
                       ON proof.core_eligibility_proof_id = t.core_eligibility_proof_id
                WHERE op.trigger_code = 'core_rebalance' AND op.status = ANY(%s)
                ORDER BY op.position_operation_id
                LIMIT 1
                """,
                (list(_UNRESOLVED_OPERATION_STATUSES),),
            ).fetchone()
        if row is None:
            return None
        intent_id, ownership_id, ownership_status, trade_id, position_id, api_key_id, user_key_id = row
        if (api_key_id, user_key_id) != credentials:
            raise StrategyCoreExecutionError(
                "the in-flight core rebalance close belongs to other broker credentials; it is not driven through these"
            )
        if ownership_status != "active":
            # Unreachable by construction (`_finish_close` terminalises then releases in
            # one transaction; the #3312 release runs only when nothing resumed).
            return _result("reconcile_required", "core_operation_unloadable", intent_id=int(intent_id))
        return _drive_core_close(
            conn,
            broker=broker,
            intent_id=int(intent_id),
            strategy_trade_id=int(trade_id),
            broker_position_id=int(position_id),
            ownership_id=int(ownership_id),
            submit=False,
        )


@dataclass(frozen=True)
class _CoreSellTarget:
    ownership_id: int
    strategy_trade_id: int
    broker_position_id: int
    credentials: tuple[UUID, UUID] | None
    """The (api, user) credential ids of the proof the position was BOUGHT under."""


def _core_sell_target(conn: psycopg.Connection[Any]) -> _CoreSellTarget | str:
    """Spec §1 step 3's DB preconditions, in one committed read: the target or a refusal."""
    with conn.transaction():
        owned = conn.execute(
            """
            SELECT own.ownership_id, own.strategy_trade_id, own.broker_position_id,
                   proof.api_key_credential_id, proof.user_key_credential_id
            FROM strategy_position_ownership own
            JOIN strategy_trades t ON t.strategy_trade_id = own.strategy_trade_id
            LEFT JOIN strategy_core_eligibility_proofs proof
                   ON proof.core_eligibility_proof_id = t.core_eligibility_proof_id
            WHERE own.status = 'active' AND t.core_rebalance_intent_id IS NOT NULL
            """
        ).fetchall()
        outstanding = conn.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM strategy_position_operations op
                JOIN strategy_position_ownership own ON own.ownership_id = op.ownership_id
                JOIN strategy_trades t ON t.strategy_trade_id = own.strategy_trade_id
                WHERE t.core_rebalance_intent_id IS NOT NULL
                  AND (op.status = ANY(%s)
                       OR (op.operation_type = 'close' AND op.status = 'reconcile_required'))
            )
            """,
            (list(_UNRESOLVED_OPERATION_STATUSES),),
        ).fetchone()
        # Every ENTRY order on ANY core trade has a terminal reconciliation state; a
        # missing row counts as non-terminal (the admission gate's rule, sql/285).
        entry_open = conn.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM strategy_trades t
                JOIN strategy_trade_orders link
                  ON link.strategy_trade_id = t.strategy_trade_id AND link.purpose = 'entry'
                LEFT JOIN strategy_order_reconciliation_state recon ON recon.order_id = link.order_id
                WHERE t.core_rebalance_intent_id IS NOT NULL
                  AND (recon.state IS NULL OR recon.state NOT IN ('resolved', 'rejected'))
            )
            """
        ).fetchone()
    if len(owned) != 1:
        return "core_sell_spans_positions"
    if outstanding is None or outstanding[0]:
        return "core_operation_outstanding"
    if entry_open is None or entry_open[0]:
        return "core_entry_not_terminal"
    ownership_id, trade_id, position_id, api_key_id, user_key_id = owned[0]
    credentials = None if api_key_id is None or user_key_id is None else (api_key_id, user_key_id)
    return _CoreSellTarget(int(ownership_id), int(trade_id), int(position_id), credentials)


def _evidence_fresh(observed_at: datetime, *, now: datetime) -> bool:
    age = (now - observed_at).total_seconds()
    return 0 <= age <= CORE_MAX_ACCOUNT_RISK_AGE_SECONDS


def _execute_core_sell(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    mandate: CoreMandate,
    intent: CoreRebalanceIntent,
    snapshot: BrokerAccountRiskSnapshot,
    usage: EngineCapitalUsage,
    state: CoreSleeveState,
    proof: CoreEligibilityProof,
    clock: Callable[[], datetime],
) -> CoreExecutionResult:
    """Spec §1 steps 3-6: a ``sell_core`` becomes a WHOLE close of the one core position.

    Called inside the caller's ``core_submission_lock`` hold with an idle connection,
    after the common re-proof, drawdown observation, admission, eligibility re-proof and
    DB preflight (kill switch included).  Every refusal precedes the close-quote INSERT,
    so a quote row exists only for an intent handed to the manager.  The rebuy is NOT
    here: the next attended POST sees 0% core and the unchanged allocator buys to
    ``lower`` (spec §3).
    """
    intent_id = intent.core_rebalance_intent_id

    def refuse(code: str) -> CoreExecutionResult:
        return _result("refused", code, intent_id=intent_id)

    target = _core_sell_target(conn)
    if isinstance(target, str):
        return refuse(target)
    if target.credentials != (proof.api_key_credential_id, proof.user_key_credential_id):
        # The position was bought under another account's proof: closing it through this
        # one would act on the wrong book (or on a colliding position id).
        return refuse("core_credential_provenance_changed")
    lower_pct = intent.decision.lower_pct
    if lower_pct is None or lower_pct <= 0:
        # With `lower = 0` the post-close 0% is in-band and the allocator never rebuys.
        return refuse("core_sell_would_strand_at_zero_lower")
    if mandate.rebalance_band_pct <= 0:
        # A zero-width band leaves the rebuy's cost bracket no room.
        return refuse("core_sell_zero_width_band")
    positions = [row for row in snapshot.direct_positions if row.position_id == target.broker_position_id]
    if len(positions) != 1:
        # `resolve_engine_capital_usage` already refused an unwitnessed active position,
        # so this is defensive: never quote or close a position this snapshot lacks.
        return refuse("core_sell_position_unobserved")
    position = positions[0]
    if position.market_value <= 0 or position.units <= 0:
        return refuse("core_sell_position_unobserved")

    # Step 4: the close-arm quote, bound to the selected position.  An open-arm quote
    # never bounds a close (`BrokerWhatIfOrder` docstring, #2712), so it is quoted here.
    if proof.response_currency.strip().upper() != mandate.base_currency.strip().upper():
        return refuse("core_close_side_cost_quote_unavailable")
    # Before any broker call, as in the buy preflight, which refuses the same case with
    # the same code: `effective_open_minimum` is USD-only and contracts that its callers
    # refuse a mismatch first.
    if proof.response_currency.strip().upper() != _OPEN_MINIMUM_CURRENCY:
        return refuse("core_minimum_currency_unsupported")
    try:
        response = broker.get_what_if_costs(
            BrokerWhatIfOrder(
                instrument_id=position.instrument_id,
                transaction="sell",
                settlement_type=UNDERLYING_SETTLEMENT_TYPE,
                amount=position.market_value,
                leverage=UNLEVERAGED_LEVERAGE,
                action="close",
                position_ids=(target.broker_position_id,),
            )
        )
    except Exception:
        logger.warning("core sell: close-side what-if quote unavailable", exc_info=True)
        return refuse("core_close_side_cost_quote_unavailable")
    cost = decode_quoted_trade_cost(
        response,
        instrument_id=position.instrument_id,
        ticket_amount=position.market_value,
        base_currency=mandate.base_currency,
        valuation_as_of=snapshot.observed_at,
    )
    if not isinstance(cost, QuotedTradeCost):
        logger.warning("core sell: close-side quote undecodable (%s)", cost)
        return refuse("core_close_side_cost_quote_unavailable")
    if cost.rate >= 1:
        return refuse("core_close_cost_implausible")

    # The lower-edge rebuy from the post-close state must clear the allocator's own
    # floor, computed by the allocator.  The UNCLAMPED headroom, so an over-bound
    # sleeve's deficit is netted rather than hidden by the observation clamp.
    broker_minimum = effective_open_minimum(
        response_currency=proof.response_currency,
        min_position_exposure=proof.min_position_exposure,
        min_position_amount=proof.min_position_amount,
    )
    if broker_minimum is None:
        return refuse("core_broker_open_minimum_unquoted")
    post_close = replace(
        state,
        core_market_value=Decimal("0"),
        cash_balance=min(snapshot.available_cash, usage.headroom.remaining)
        + usage.core_market_value
        - cost.cost_upper_bound,
    )
    if evaluate_core_rebalance(mandate, post_close, broker_minimum=broker_minimum).action != "buy_core":
        return refuse("core_rebuy_below_minimum")

    # Step 5: the evidence ages once more, right before the manager call.
    now = clock()
    if not _evidence_fresh(snapshot.observed_at, now=now) or not _evidence_fresh(cost.last_updated, now=now):
        return refuse("core_account_risk_stale")

    with conn.transaction():
        conn.execute(
            """
            INSERT INTO strategy_core_rebalance_close_quotes (
                core_rebalance_intent_id, broker_position_id, units, ticket_amount,
                cost_upper_bound, currency, quoted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                intent_id,
                target.broker_position_id,
                position.units,
                cost.ticket_amount,
                cost.cost_upper_bound,
                cost.currency,
                cost.last_updated,
            ),
        )
    return _drive_core_close(
        conn,
        broker=broker,
        intent_id=intent_id,
        strategy_trade_id=target.strategy_trade_id,
        broker_position_id=target.broker_position_id,
        ownership_id=target.ownership_id,
        submit=True,
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

    # #2603 sell leg, spec step 0: drive an in-flight rebalance close first, with no
    # evaluation, no new intent and no quote.
    outstanding = _resolve_outstanding_core_rebalance_close(
        conn, broker=broker, credentials=(api_key_credential_id, user_key_credential_id)
    )
    if outstanding is not None:
        return outstanding

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
    exit_levels: CoreExitLevels | None = None
    with core_submission_lock(conn):
        try:
            capital_authority = load_engine_capital_authority(conn)
        except EngineCapitalObservationError as exc:
            # ⚠ REFUSED, not raised (#2979 half b).  An inconsistent shared population is
            # a steady state -- true on this cycle and every later one -- so a caller
            # gets a reason code it can act on rather than a 409 whose only text is the
            # OUTER sentence.  Still fail-closed: nothing is submitted either way.
            return _capital_refusal_result(exc)
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
            # ⚠ Only the HEADROOM term is clamped: over the bound `remaining` is
            # negative, and a close must still be observable there (#2603 sell leg).  A
            # negative broker `available_cash` still flows through unchanged.
            state = observe_core_sleeve(
                snapshot,
                core_instrument_id=mandate.core_instrument_id,
                exact_owned_market_value=usage.core_market_value,
                assigned_cash_available=min(snapshot.available_cash, max(Decimal("0"), usage.headroom.remaining)),
            )
        except EngineCapitalObservationError as exc:
            # ⚠ This arm MUST precede the blanket one below and must not absorb it.  The
            # blanket arm still covers a failed broker read (an execution fault) and
            # `CoreSleeveObservationError` (input drift in one payload, which its own
            # docstring puts on the raising side).  Only the capital join's refusal --
            # steady state, unchanged by a retry -- becomes a verdict.
            #
            # ⚠ Reachability.  When this arm was written the operator's Rebalance button
            # was DISABLED whenever `core_active_position_ids` was non-empty, so this
            # verdict was reachable only by calling the endpoint directly.  #3123 fixed
            # that (`read_core_sleeve.capital_permits_rebalance`), so the attended path
            # now reaches it.  The caller that reaches the same refusal on every
            # UNATTENDED tick is still the paper cycle
            # (`strategy_paper_executor._risk_and_amount`).
            return _capital_refusal_result(exc)
        except Exception as exc:
            raise StrategyCoreExecutionError("the broker account snapshot could not describe the core sleeve") from exc
        decision = evaluate_core_rebalance(mandate, state)
        # The sandbox refuses only a BUY.  A close reduces exposure; refusing it over the
        # bound would pin the sleeve there.  Curing a breach is not the rebalancer's job
        # (#2844 refuses, #2843 alerts).
        if decision.action == "buy_core" and not usage.headroom.within_bound:
            return _result("refused", "sandbox_exceeded")
        broker_verdict = None
        # A sell is quoted on the CLOSE arm inside `_execute_core_sell`; the trim-sized
        # preflight describes a ticket that is never sent.
        if decision.action == "buy_core":
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
            # The drawdown refusal blocks new risk; a rebalance close reduces it.  The
            # observation above is recorded either way.
            if initial_drawdown_refusal is not None and intent.decision.action != "sell_core":
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
            # ⚠ The sell branch leaves this transaction (committed) before touching the
            # broker; the buy remainder stays in it, unchanged, so admission's FOR SHARE
            # on the credential rows still spans the authority INSERT.
            sell_intent = intent if intent.decision.action == "sell_core" else None
            if sell_intent is None:
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

                # #3284 item 1 -- the exit levels are derived BEFORE the durable authority
                # exists, so a body that cannot carry them is never committed.
                #
                # ⚠ The anchor is `db_preflight.price`, which for `buy_core` is the ASK, read
                # inside THIS hold of `core_submission_lock` and already refused as
                # `core_quote_stale` beyond `CORE_MAX_QUOTE_AGE_SECONDS` -- the very constant
                # `CORE_EXIT_MAX_QUOTE_AGE_SECONDS` re-exports.  So the anchor's freshness
                # bound and the repair's are ONE policy rather than two that happen to agree,
                # and no second quote read is needed.  This mirrors the signal arm, which
                # anchors on `intent.ask` (`strategy_paper_executor`).
                #
                # ⚠ The anchor is NOT the fill.  A market order fills where it fills, so the
                # submitted stop is approximately -50% of the fill rather than exactly; the
                # five-minute repair re-anchors on `broker_positions.open_price` (the true
                # fill) and corrects it. That approximation is the price of protecting the
                # position from the first instant, and it is the right trade: the alternative
                # is exactness with a naked window.
                anchor_rate = db_preflight.price
                anchor_quoted_at = db_preflight.quoted_at
                # An admitted verdict always carries both -- `_age_ok` cannot pass on a NULL
                # `quoted_at` and the price refusals precede it.  Re-checked anyway, because
                # "cannot happen" is how a naked position gets opened: the refusal costs one
                # cycle, the alternative is an unguarded `None` reaching the INSERT.
                if anchor_rate is None or not anchor_rate.is_finite() or anchor_rate <= 0 or anchor_quoted_at is None:
                    return _result("refused", "core_exit_anchor_unavailable", intent_id=intent_id)
                try:
                    exit_levels = core_exit_levels(anchor_rate)
                except CoreExitLevelsUnderivable:
                    # ⚠ The SPECIFIC exception, not a bare `ValueError` -- a bare catch would map
                    # any future failure inside `core_exit_levels` to this same refusal, which
                    # reads as a handled condition when it is an unhandled one (review bot,
                    # PR #3289).
                    #
                    # ⚠ A REFUSAL, not an exception, and the case is real rather than defensive:
                    # `core_exit_levels` quantizes the stop DOWN to a cent, so any anchor under
                    # two cents derives a stop of 0.00 and raises.  SPY cannot reach there, but
                    # the mandate instrument is configurable and an executor that raises where
                    # it could refuse turns a bad candidate into a failed attended request.
                    return _result("refused", "core_exit_levels_underivable", intent_id=intent_id)

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
                # ⚠ INSIDE the authority transaction, with the order and the reconciliation
                # row.  All three commit together or none do, so "a durable core authority
                # exists" and "its exit levels are known" are the same fact -- which is what
                # lets the submit path read them rather than re-derive them (#3284 item 1).
                conn.execute(
                    """
                    INSERT INTO strategy_core_entry_exit_levels (
                        order_id, anchor_rate, anchor_quoted_at,
                        stop_loss_rate, take_profit_rate, policy_version
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        order_id,
                        anchor_rate,
                        anchor_quoted_at,
                        exit_levels.stop_loss_rate,
                        exit_levels.take_profit_rate,
                        exit_levels.policy_version,
                    ),
                )
                link_strategy_order(conn, strategy_trade_id=trade_id, order_id=order_id, purpose=order_purpose)
                # ⚠ `submission_phase` is declared HERE, in the authority transaction,
                # and advanced by `mark_core_submission_entered` in a SEPARATE commit
                # before the broker verb (#2961).  Two distinct commits is the whole
                # of the separation: folding the marker into this statement would make
                # every core entry read as "may have reached the broker", which is the
                # vacuous shape `strategy_position_manager.py:854-857` warns about.
                conn.execute(
                    "INSERT INTO strategy_order_reconciliation_state (order_id, submission_phase) "
                    "VALUES (%s, 'authority_committed')",
                    (order_id,),
                )

        if sell_intent is not None:
            # Still inside the hold: the manager needs an idle connection, and session
            # advisory locks survive the commit.
            return _execute_core_sell(
                conn,
                broker=broker,
                mandate=current,
                intent=sell_intent,
                snapshot=snapshot,
                usage=usage,
                state=state,
                proof=binding_proof,
                clock=clock,
            )

        if request_id is None or trade_id is None or order_id is None or exit_levels is None:
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
                stop_loss_rate=exit_levels.stop_loss_rate,
                take_profit_rate=exit_levels.take_profit_rate,
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
    "mark_core_submission_entered",
    "resume_core_submission",
]
