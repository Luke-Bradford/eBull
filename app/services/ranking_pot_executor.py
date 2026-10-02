"""Ranking-pot-v1's entry executor and its 15:00 UTC job body (#2842 slice 5b).

Spec ``docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md`` §7.2, "The loader, the slot ledger and the
executor". One executed-book entry (``ranking_pot_intent.load_pot_intent``) is sized from its slot's wealth, priced,
protected and submitted through the SAME machinery as the paper and #3471 trial paths — the allocator lock, the #2844
sandbox, every capacity term of ``_risk_and_amount``, broker eligibility and what-if costs, the durable-before-I/O
order authority, ``place_demo_strategy_order`` — with the pot's differences:

* the requested ticket is the slot's wealth (``ranking_pot_intent.slot_wealth``), which capacity may reduce (down to
  the broker open minimum) and nothing may raise;
* the stressed what-if cost may not exceed ``POT_COST_CAP_PCT`` of the amount (``pot_cost_cap``);
* the SL/TP are the frozen 3-ATR / 2R levels from the pre-submission ask, validated as sent (by the loader);
* ``pot_slot_not_released`` and ``pot_slot_ledger_incomplete`` are DEFERRALS: nothing is written, the lifecycle stays
  ``entry_pending`` and a later fire this session retries it. Every other refusal is persisted, so each entry has
  one attempt per rebalance.

Exits, protection repair and the §7.4 loss check are slice 5c's. Policy-hashed (``ranking_pot_policy``).
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any, Final
from zoneinfo import ZoneInfo

import psycopg
from psycopg.pq import TransactionStatus

from app.providers.broker import BrokerAccountRiskSnapshot, BrokerProvider, BrokerWhatIfOrder
from app.services import ranking_pot as pot
from app.services import ranking_pot_exec as exec_book
from app.services import ranking_pot_look as look
from app.services.ranking_pot_intent import DEFERRALS, PotIntent, SlotState, load_pot_intent, slot_state
from app.services.strategy_control_plane import (
    create_strategy_trade,
    decide_funding,
    link_strategy_order,
    registered_strategy_purpose,
)
from app.services.strategy_halt_identity import HALT_IDENTITY_RULE_VERSION
from app.services.strategy_order_reconciliation import ensure_strategy_request_id
from app.services.strategy_paper_executor import (
    PaperExecutionResult,
    StrategyPaperExecutionError,
    _allocator_lock,
    _eligibility_reason,
    _existing_result,
    _persist_rejection,
    _reconciliation_overdue,
    _resume_uncertain_submission,
    _risk_and_amount,
    _session_is_open,
    _stressed_cost,
    _submit_recorded_order,
    _trading_enabled_refusal,
)

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

_NEW_YORK: Final = ZoneInfo("America/New_York")
#: §7.2 "Cost cap", v1's constant by construction: stressed what-if cost ≤ 1.0% of the amount.
POT_COST_CAP_PCT: Final = Decimal("1.0")
#: §4 step 5: entries from 15:00 UTC on the target session (in session in both EDT and EST).
POT_ENTRY_TIME_UTC: Final = time(15, 0)
POT_ALLOCATED_REASON: Final = "all_pot_entry_gates_passed"


@dataclass(frozen=True)
class Deferred:
    """A deferral: nothing was written; the entry is retried by a later fire this session."""

    signal_id: int
    reason_code: str
    verdict: str = "deferred"


def pot_cost_cap_reason(stressed_cost: Decimal, amount: Decimal) -> str | None:
    """``pot_cost_cap`` when the stressed cost exceeds ``POT_COST_CAP_PCT`` of the amount (by multiplication;
    exactly 1.0% passes)."""
    if not (stressed_cost.is_finite() and amount.is_finite()) or amount <= 0 or stressed_cost < 0:
        return "pot_cost_cap"
    return "pot_cost_cap" if stressed_cost * Decimal("100") > POT_COST_CAP_PCT * amount else None


def _authority_refusal(conn: Conn, intent: PotIntent, *, now: datetime) -> SlotState | str:
    """Re-check, inside the authority transaction and under locks, every fact a concurrent writer can move (r3-113).

    * The state: the declaration row ``FOR SHARE`` waits for an in-flight state event (the transition trigger holds
      it ``FOR NO KEY UPDATE``), and the re-read then sees it.
    * ``v1_active``: ``ai_trial_declarations`` in SHARE mode and its rows ``FOR SHARE``, as ``sql/445``'s
      ``executing`` trigger locks them, so an in-flight AI-trial freeze commits first and a resumption (its state
      trigger takes the declaration row ``FOR NO KEY UPDATE``) waits.
    * ``look_pending`` at the target session (r3-89).
    * The slot, its wealth and the name, re-read from the ledger (the allocator lock serialises pot submissions).

    The job connection is READ COMMITTED, so each statement here reads after the lock it waited on.
    """
    conn.execute("SELECT 1 FROM ranking_pot_declarations WHERE declaration_id = %s FOR SHARE", (intent.declaration_id,))
    state = conn.execute(
        "SELECT to_state FROM ranking_pot_state_events WHERE declaration_id = %s ORDER BY event_id DESC LIMIT 1",
        (intent.declaration_id,),
    ).fetchone()
    if state is None or state[0] != "executing":
        return "pot_not_executing"
    conn.execute("LOCK TABLE ai_trial_declarations IN SHARE MODE")
    conn.execute("SELECT 1 FROM ai_trial_declarations FOR SHARE")
    v1 = conn.execute(exec_book.V1_ACTIVE_SQL).fetchone()
    if v1 is None or bool(v1[0]):
        return "v1_active"
    if look.look_pending(conn, intent.declaration_id, intent.target_session) is not None:
        return "look_pending"
    return slot_state(
        conn,
        declaration_id=intent.declaration_id,
        lifecycle_id=intent.lifecycle_id,
        instrument_id=intent.instrument_id,
        slot=intent.slot,
        n=intent.n,
        pot_capital=intent.pot_capital,
        now=now,
    )


def _commit_authority(
    conn: Conn,
    *,
    intent: PotIntent,
    slot: SlotState,
    amount: Decimal,
    evaluated_at: datetime,
    costs_at: datetime,
    risk: BrokerAccountRiskSnapshot,
    instrument_invested: Decimal,
    drawdown: Decimal,
    stressed_cost: Decimal,
    cost_basis: str,
) -> tuple[int, int, Any]:
    """Funding decision, trade, order, request id, preflight and submission row, in the caller's transaction."""
    decision_id = decide_funding(
        conn,
        signal_id=intent.signal_id,
        verdict="allocated",
        deployment_id=intent.deployment_id,
        amount=amount,
        reason_code=POT_ALLOCATED_REASON,
    )
    trade_id = create_strategy_trade(conn, decision_id)
    order_row = conn.execute(
        """
        INSERT INTO orders (
            instrument_id, action, order_type, requested_amount, status,
            raw_payload_json, execution_origin
        ) VALUES (%s, 'BUY', 'MARKET', %s, 'submitted', NULL, 'strategy')
        RETURNING order_id
        """,
        (intent.instrument_id, amount),
    ).fetchone()
    assert order_row is not None
    order_id = int(order_row[0])
    link_strategy_order(conn, strategy_trade_id=trade_id, order_id=order_id, purpose="entry")
    request_id = ensure_strategy_request_id(conn, order_id=order_id)
    # No forecast, ranking-member, scan or expectancy column: the pot has none.
    conn.execute(
        """
        INSERT INTO strategy_entry_preflights (
            signal_id, deployment_id, policy_revision, verdict, reason_code,
            evaluated_at, quote_at, quote_ask, halt_feed_at, halt_identity_rule_version,
            eligibility_checked_at, costs_at, broker_available_cash,
            account_equity, account_invested, instrument_invested,
            account_drawdown_pct, allocated_amount, stressed_cost_amount, cost_basis,
            stop_loss_rate, take_profit_rate
        ) VALUES (
            %s, %s, %s, 'allocated', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            intent.signal_id,
            intent.deployment_id,
            intent.policy_revision,
            POT_ALLOCATED_REASON,
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
    conn.execute(
        """
        INSERT INTO ranking_pot_exec_submissions (
            lifecycle_id, strategy_trade_id, slot, slot_wealth, requested_amount, amount,
            ask, quote_at, atr14, stop_loss_rate, take_profit_rate
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            intent.lifecycle_id,
            trade_id,
            intent.slot,
            slot.wealth,
            slot.wealth,
            amount,
            intent.ask,
            intent.quote_at,
            Decimal(intent.atr14.numerator) / Decimal(intent.atr14.denominator),
            intent.stop_rate,
            intent.take_rate,
        ),
    )
    return trade_id, order_id, request_id


def _execute_pot_signal_locked(
    conn: Conn, *, broker: BrokerProvider, signal_id: int, now: datetime | None
) -> PaperExecutionResult | Deferred:
    evaluated_at = (now or datetime.now(UTC)).astimezone(UTC)
    if evaluated_at.time() < POT_ENTRY_TIME_UTC:
        # A contract violation, not a refusal: a refusal would consume the entry's one attempt before its window.
        raise StrategyPaperExecutionError(f"ranking-pot entries execute from {POT_ENTRY_TIME_UTC} UTC")
    existing = _existing_result(conn, signal_id)
    conn.commit()
    if existing is not None and existing.verdict != "submission_uncertain":
        return existing
    signal_row = conn.execute("SELECT strategy_id FROM strategy_signals WHERE signal_id=%s", (signal_id,)).fetchone()
    conn.commit()
    # ⚠ RAISED, never persisted: a funding decision is one per signal, so a refusal written here for another
    # strategy's signal would pre-empt the path that owns it.
    if (
        signal_row is None
        or str(signal_row[0]) != pot.STRATEGY_ID
        or registered_strategy_purpose(str(signal_row[0])) != "demo_trial"
    ):
        raise StrategyPaperExecutionError(f"signal {signal_id} is not a ranking-pot signal")

    trading_refusal = _trading_enabled_refusal(conn)
    if trading_refusal is not None:
        return _persist_rejection(conn, signal_id=signal_id, reason_code=trading_refusal, now=evaluated_at)
    if existing is not None:
        # Resolving an already-committed authority with its own request id: not a new attempt (r3-119).
        return _resume_uncertain_submission(conn, broker=broker, existing=existing)
    if _reconciliation_overdue(conn, signal_id):
        return _persist_rejection(conn, signal_id=signal_id, reason_code="reconciliation_overdue", now=evaluated_at)

    intent, reason, halt_identity_evaluated = load_pot_intent(conn, signal_id=signal_id, now=evaluated_at)
    conn.commit()
    if intent is None and reason in DEFERRALS:
        return Deferred(signal_id, reason)
    if intent is None:
        return _persist_rejection(
            conn,
            signal_id=signal_id,
            reason_code=reason or "preflight_unavailable",
            now=evaluated_at,
            halt_identity_evaluated=halt_identity_evaluated,
        )

    try:
        risk = broker.get_account_risk_snapshot()
    except Exception:
        return _persist_rejection(
            conn, signal_id=signal_id, reason_code="account_risk_unavailable", now=evaluated_at, intent=intent
        )
    wealth = intent.slot_wealth
    sized = _risk_and_amount(conn, intent=intent, risk=risk, now=evaluated_at, requested_ticket=lambda _base: wealth)
    if isinstance(sized, str):
        return _persist_rejection(
            conn, signal_id=signal_id, reason_code=sized, now=evaluated_at, intent=intent, risk=risk
        )
    amount, instrument_invested, drawdown = sized
    try:
        eligibility = broker.check_instrument_eligibility([intent.instrument_id])
    except Exception:
        return _persist_rejection(
            conn, signal_id=signal_id, reason_code="eligibility_unavailable", now=evaluated_at, intent=intent, risk=risk
        )
    # Refuses below the broker open minimum: a reduced amount is accepted down to it, never under it (r3-118).
    eligibility_reason = _eligibility_reason(eligibility, intent, amount)
    if eligibility_reason:
        return _persist_rejection(
            conn, signal_id=signal_id, reason_code=eligibility_reason, now=evaluated_at, intent=intent, risk=risk
        )
    try:
        costs = broker.get_what_if_costs(
            BrokerWhatIfOrder(
                instrument_id=intent.instrument_id,
                transaction="buy",
                settlement_type="real",
                amount=amount,
                leverage=1,
            )
        )
    except Exception:
        return _persist_rejection(
            conn, signal_id=signal_id, reason_code="costs_unavailable", now=evaluated_at, intent=intent, risk=risk
        )
    priced = _stressed_cost(costs, intent=intent, now=evaluated_at)
    if isinstance(priced, str):
        return _persist_rejection(
            conn, signal_id=signal_id, reason_code=priced, now=evaluated_at, intent=intent, risk=risk
        )
    stressed_cost, cost_basis = priced
    cost_reason = pot_cost_cap_reason(stressed_cost, amount)
    if cost_reason is not None:
        return _persist_rejection(
            conn, signal_id=signal_id, reason_code=cost_reason, now=evaluated_at, intent=intent, risk=risk
        )

    # The durable authority, committed before any broker write. `decide_funding` re-checks the deployment cap under
    # its locks; nothing is written when a late check refuses.
    late: SlotState | str
    authority: tuple[int, int, Any] | None = None
    with conn.transaction():
        late = _authority_refusal(conn, intent, now=evaluated_at)
        if isinstance(late, SlotState) and late.wealth == wealth:
            authority = _commit_authority(
                conn,
                intent=intent,
                slot=late,
                amount=amount,
                evaluated_at=evaluated_at,
                costs_at=costs.last_updated,
                risk=risk,
                instrument_invested=instrument_invested,
                drawdown=drawdown,
                stressed_cost=stressed_cost,
                cost_basis=cost_basis,
            )
    if authority is None:
        # A slot whose wealth moved since sizing (a late booking) is retried, never submitted on the stale figure.
        code = late if isinstance(late, str) else "pot_slot_not_released"
        if code in DEFERRALS:
            return Deferred(signal_id, code)
        return _persist_rejection(
            conn, signal_id=signal_id, reason_code=code, now=evaluated_at, intent=intent, risk=risk
        )
    trade_id, order_id, request_id = authority
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


def execute_pot_signal(
    conn: Conn, *, broker: BrokerProvider, signal_id: int, now: datetime | None = None
) -> PaperExecutionResult | Deferred:
    """Evaluate one executed-book entry and, only if every gate passes, submit it to demo. Serialised with the paper
    and trial paths under the same allocator lock, so all three share one view of account risk and the sandbox."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise StrategyPaperExecutionError("ranking-pot execution requires an idle connection")
    with _allocator_lock(conn):
        return _execute_pot_signal_locked(conn, broker=broker, signal_id=signal_id, now=now)


# ---------------------------------------------------------------------------
# The job body
# ---------------------------------------------------------------------------
_DUE_SQL: Final = """
    SELECT l.signal_id
      FROM ranking_pot_exec_lifecycles l
      JOIN ranking_pot_rebalance_attempts a ON a.attempt_id = l.attempt_id
     WHERE a.target_session = %s
       AND NOT EXISTS (SELECT 1 FROM strategy_funding_decisions fd WHERE fd.signal_id = l.signal_id)
     ORDER BY l.attempt_id, l.lifecycle_id
"""


def due_pot_entries(conn: Conn, *, today: date) -> list[int]:
    """Undecided executed entries targeting ``today``, in entry order (5a inserts lifecycles in entry order). An
    earlier session's undecided entry is ``expired`` by classification and is never returned."""
    return [int(row[0]) for row in conn.execute(_DUE_SQL, (today,)).fetchall()]


@dataclass(frozen=True)
class PotExecutionJobResult:
    #: ``False`` outside the regular session or before ``POT_ENTRY_TIME_UTC``.
    session_open: bool
    verdicts: Mapping[str, int] = field(default_factory=dict)
    #: Entries whose executor call raised; each was logged and the batch continued.
    errors: int = 0

    @property
    def entries(self) -> int:
        return sum(self.verdicts.values())

    @property
    def note(self) -> str:
        if not self.session_open:
            return "session_closed"
        breakdown = " ".join(f"{k}={v}" for k, v in sorted(self.verdicts.items()))
        errors = f" errors={self.errors}" if self.errors else ""
        return f"entries={self.entries} {breakdown}".rstrip() + errors


def run_pot_execution(
    conn: Conn,
    *,
    broker: BrokerProvider,
    refresh_halts: Callable[[], object],
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> PotExecutionJobResult:
    observed = clock().astimezone(UTC)
    if not _session_is_open(observed) or observed.time() < POT_ENTRY_TIME_UTC:
        return PotExecutionJobResult(session_open=False)
    signal_ids = due_pot_entries(conn, today=observed.astimezone(_NEW_YORK).date())
    conn.commit()
    verdicts: Counter[str] = Counter()
    errors = 0
    if signal_ids:
        refresh_halts()
    for signal_id in signal_ids:
        # A fresh instant per entry: the executor refuses an account snapshot stamped more than 5 s after `now`.
        try:
            result = execute_pot_signal(conn, broker=broker, signal_id=signal_id, now=clock())
        except Exception:
            logger.exception("ranking_pot execute: signal %s raised; continuing with the batch", signal_id)
            if conn.info.transaction_status != TransactionStatus.IDLE:
                conn.rollback()
            errors += 1
            continue
        verdicts[result.verdict] += 1
    return PotExecutionJobResult(session_open=True, verdicts=dict(verdicts), errors=errors)


__all__ = [
    "POT_ALLOCATED_REASON",
    "POT_COST_CAP_PCT",
    "POT_ENTRY_TIME_UTC",
    "Deferred",
    "PotExecutionJobResult",
    "due_pot_entries",
    "execute_pot_signal",
    "pot_cost_cap_reason",
    "run_pot_execution",
]
