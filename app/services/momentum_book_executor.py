"""Momentum book (#3822) — the demo entry executor.

One formed entry (``momentum_book_intent.load_intent``) is sized, priced, protected and submitted through the same
machinery as the paper, trial and pot paths: the allocator lock, the #2844 sandbox and every capacity term of
``_risk_and_amount``, broker eligibility and what-if costs, the durable-before-I/O order authority, the entry ticket,
and ``place_demo_strategy_order`` with broker-side stop-loss and take-profit from the execution policy.

An entry is decided once, with one exception: a refusal in ``PAPER_ENTRY_DEFERRALS`` (a stale quote or halt feed, a
closed session, a broker read that raised, an execution block) writes nothing, so the next hourly fire on the same
target session re-runs every gate. Every other refusal is persisted. An entry still undecided when its target session
ends is never selected again; the next month's formation re-emits a name still in the book. The hourly job body is
``momentum_book_executor_job``.

Broker costs are judged at the moment they are received, not at the start of the evaluation: eToro's what-if
``lastUpdated`` is the server time of the call, and the risk, eligibility and what-if reads before it take about 8
seconds, beyond the 5-second future-skew bound of ``_age_ok`` (#3822, 2026-10-15 16:10Z fire).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Final

import psycopg
from psycopg.pq import TransactionStatus

from app.providers.broker import BrokerAccountRiskSnapshot, BrokerProvider, BrokerWhatIfOrder
from app.services import momentum_book as mb
from app.services.momentum_book_intent import MomentumIntent, load_intent
from app.services.strategy_control_plane import (
    create_strategy_trade,
    decide_funding,
    link_strategy_order,
)
from app.services.strategy_entry_ticket import EntryTicket, protective_exit_rule, write_entry_ticket
from app.services.strategy_halt_identity import HALT_IDENTITY_RULE_VERSION
from app.services.strategy_order_reconciliation import ensure_strategy_request_id
from app.services.strategy_paper_executor import (
    PaperExecutionResult,
    StrategyPaperExecutionError,
    _allocator_lock,
    _eligibility_reason,
    _existing_result,
    _reconciliation_overdue,
    _refuse_paper_entry,
    _resume_uncertain_submission,
    _risk_and_amount,
    _stressed_cost,
    _submit_recorded_order,
    _trading_enabled_refusal,
)

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

ALLOCATED_REASON: Final = "all_momentum_entry_gates_passed"


def _commit_authority(
    conn: Conn,
    *,
    intent: MomentumIntent,
    amount: Decimal,
    evaluated_at: datetime,
    costs_at: datetime,
    risk: BrokerAccountRiskSnapshot,
    instrument_invested: Decimal,
    drawdown: Decimal,
    stressed_cost: Decimal,
    cost_basis: str,
) -> tuple[int, int, Any]:
    """Funding decision, trade, order, request id, preflight and entry ticket, in the caller's transaction."""
    decision_id = decide_funding(
        conn,
        signal_id=intent.signal_id,
        verdict="allocated",
        deployment_id=intent.deployment_id,
        amount=amount,
        reason_code=ALLOCATED_REASON,
    )
    trade_id = create_strategy_trade(conn, decision_id)
    order_row = conn.execute(
        """
        INSERT INTO orders (instrument_id, action, order_type, requested_amount, status, raw_payload_json,
                            execution_origin, broker_environment)
        VALUES (%s, 'BUY', 'MARKET', %s, 'submitted', NULL, 'strategy', 'demo')
        RETURNING order_id
        """,
        (intent.instrument_id, amount),
    ).fetchone()
    assert order_row is not None
    order_id = int(order_row[0])
    link_strategy_order(conn, strategy_trade_id=trade_id, order_id=order_id, purpose="entry")
    request_id = ensure_strategy_request_id(conn, order_id=order_id)
    conn.execute(
        """
        INSERT INTO strategy_entry_preflights (
            signal_id, deployment_id, policy_revision, verdict, reason_code,
            evaluated_at, quote_at, quote_ask, halt_feed_at, halt_identity_rule_version,
            eligibility_checked_at, costs_at, broker_available_cash,
            account_equity, account_invested, instrument_invested,
            account_drawdown_pct, allocated_amount, stressed_cost_amount, cost_basis,
            stop_loss_rate, take_profit_rate
        ) VALUES (%s, %s, %s, 'allocated', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            intent.signal_id,
            intent.deployment_id,
            intent.policy_revision,
            ALLOCATED_REASON,
            evaluated_at,
            intent.quote_at,
            intent.ask,
            intent.halt_feed_at,
            HALT_IDENTITY_RULE_VERSION,
            evaluated_at,
            costs_at,
            risk.available_cash,
            risk.equity,
            risk.total_invested,
            instrument_invested,
            drawdown,
            amount,
            stressed_cost,
            cost_basis,
            intent.stop_rate,
            intent.take_rate,
        ),
    )
    write_entry_ticket(
        conn,
        EntryTicket(
            order_id=order_id,
            strategy_trade_id=trade_id,
            rationale_class="signal",
            rule_id=f"{intent.strategy_id}@{intent.strategy_version}",
            evidence_kind="momentum_book_formation",
            evidence_id=intent.formation_id,
            why_now=(
                f"rank {intent.rank} of {mb.BOOK_SIZE} by risk-adjusted 12-1 momentum in the "
                f"{intent.formation_month:%Y-%m} formation (#3822); evidence "
                "docs/research/2026-10-15-3822-momentum-evidence.md"
            ),
            exit_rule=protective_exit_rule(
                intent.stop_rate, intent.take_rate, then="sold at the first monthly formation that drops it"
            ),
            expected_cost_usd=stressed_cost,
            cost_basis=cost_basis,
        ),
    )
    return trade_id, order_id, request_id


def _execute_locked(
    conn: Conn, *, broker: BrokerProvider, signal_id: int, clock: Callable[[], datetime]
) -> PaperExecutionResult:
    now = clock()
    owner = conn.execute("SELECT strategy_id FROM strategy_signals WHERE signal_id = %s", (signal_id,)).fetchone()
    conn.commit()
    # Raised, never persisted, and checked before any stored result is returned: a refusal written for another
    # strategy's signal would pre-empt the path that owns it, and its stored result is not this executor's to report.
    if owner is None or str(owner[0]) != mb.STRATEGY_ID:
        raise StrategyPaperExecutionError(f"signal {signal_id} is not a momentum-book signal")
    existing = _existing_result(conn, signal_id)
    conn.commit()
    if existing is not None and existing.verdict != "submission_uncertain":
        return existing
    refusal = _trading_enabled_refusal(conn)
    if refusal is not None:
        return _refuse_paper_entry(conn, signal_id=signal_id, reason_code=refusal, now=now)
    if existing is not None:
        return _resume_uncertain_submission(conn, broker=broker, existing=existing)
    if _reconciliation_overdue(conn, signal_id):
        return _refuse_paper_entry(conn, signal_id=signal_id, reason_code="reconciliation_overdue", now=now)

    intent, reason, halt_evaluated = load_intent(conn, signal_id=signal_id, now=now)
    conn.commit()
    if intent is None:
        return _refuse_paper_entry(
            conn,
            signal_id=signal_id,
            reason_code=reason or "preflight_unavailable",
            now=now,
            halt_identity_evaluated=halt_evaluated,
        )
    try:
        risk = broker.get_account_risk_snapshot()
    except Exception:
        return _refuse_paper_entry(
            conn, signal_id=signal_id, reason_code="account_risk_unavailable", now=now, intent=intent
        )
    ticket = intent.fixed_ticket_amount
    sized = _risk_and_amount(conn, intent=intent, risk=risk, now=now, requested_ticket=lambda _base: ticket)
    if isinstance(sized, str):
        return _refuse_paper_entry(conn, signal_id=signal_id, reason_code=sized, now=now, intent=intent, risk=risk)
    amount, instrument_invested, drawdown = sized
    try:
        eligibility = broker.check_instrument_eligibility([intent.instrument_id])
    except Exception:
        return _refuse_paper_entry(
            conn, signal_id=signal_id, reason_code="eligibility_unavailable", now=now, intent=intent, risk=risk
        )
    eligibility_reason = _eligibility_reason(eligibility, intent, amount)
    if eligibility_reason:
        return _refuse_paper_entry(
            conn, signal_id=signal_id, reason_code=eligibility_reason, now=now, intent=intent, risk=risk
        )
    try:
        costs = broker.get_what_if_costs(
            BrokerWhatIfOrder(
                instrument_id=intent.instrument_id, transaction="buy", settlement_type="real", amount=amount, leverage=1
            )
        )
    except Exception:
        return _refuse_paper_entry(
            conn, signal_id=signal_id, reason_code="costs_unavailable", now=now, intent=intent, risk=risk
        )
    priced = _stressed_cost(costs, intent=intent, now=clock())
    if isinstance(priced, str):
        return _refuse_paper_entry(conn, signal_id=signal_id, reason_code=priced, now=now, intent=intent, risk=risk)
    stressed_cost, cost_basis = priced

    # The durable authority, committed before any broker write; decide_funding re-checks the deployment cap.
    with conn.transaction():
        trade_id, order_id, request_id = _commit_authority(
            conn,
            intent=intent,
            amount=amount,
            evaluated_at=now,
            costs_at=costs.last_updated,
            risk=risk,
            instrument_invested=instrument_invested,
            drawdown=drawdown,
            stressed_cost=stressed_cost,
            cost_basis=cost_basis,
        )
    return _submit_recorded_order(
        conn,
        broker=broker,
        signal_id=signal_id,
        trade_id=trade_id,
        order_id=order_id,
        request_id=request_id,
        instrument_id=intent.instrument_id,
        amount=amount,
        stop_rate=intent.stop_rate,
        take_rate=intent.take_rate,
    )


def execute_momentum_signal(
    conn: Conn,
    *,
    broker: BrokerProvider,
    signal_id: int,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> PaperExecutionResult:
    """Evaluate one entry and, only if every gate passes, submit it to demo. Serialised with the paper, trial and pot
    paths under the same allocator lock. ``clock`` is read at the start and again when the broker costs arrive."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise StrategyPaperExecutionError("momentum-book execution requires an idle connection")
    with _allocator_lock(conn):
        return _execute_locked(conn, broker=broker, signal_id=signal_id, clock=lambda: clock().astimezone(UTC))


__all__ = [
    "ALLOCATED_REASON",
    "MomentumIntent",
    "execute_momentum_signal",
    "load_intent",
]
