"""#3471 slice 2b-ii — the ``demo_trial`` executor (spec §8 "Sizing", "Cost cap", "Protective
levels", "Demo boundary").

One authorised trial leg (``ai_trial_intent.load_trial_intent``) is sized, priced, protected and
submitted through the SAME machinery as the paper path — the allocator lock, the #2844 sandbox,
every capacity term of ``_risk_and_amount``, broker eligibility and what-if costs, the
durable-before-I/O order authority, ``place_demo_strategy_order`` and the uncertain-submission
resume — with three trial-specific differences, each from §8:

* the requested ticket is the decision's ``size_tier`` amount, which capacity may reduce (down
  to the broker open minimum, refused below it) and nothing may raise; requested and actual are
  both recorded (O8);
* the expectancy-minus-cost gate becomes a cost CAP: the stressed what-if cost may not exceed
  ``TRIAL_COST_CAP_PCT`` of the amount (``trial_cost_cap``);
* ``0 < SL < ask < TP``, finite, is asserted before the order body is built
  (``protective_levels_invalid``), and each leg holds at most ``TRIAL_MAX_CONCURRENT_PER_LEG``
  open or pending trades (``trial_leg_slots_full``), counted under the allocator lock (O8).

Nothing here writes a forecast, ranking or expectancy field. The WS ``private`` event recorder,
the pair lifecycle, the exit deadline, the step-0 gate and the jobs are later slices; nothing
schedules this function yet.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Final
from uuid import UUID

import psycopg
from psycopg.pq import TransactionStatus

from app.providers.broker import BrokerAccountRiskSnapshot, BrokerProvider, BrokerWhatIfOrder
from app.services.ai_trial_intent import TrialIntent, load_trial_intent
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
    _protective_rates,
    _reconciliation_overdue,
    _resume_uncertain_submission,
    _risk_and_amount,
    _stressed_cost,
    _submit_recorded_order,
    _trading_enabled_refusal,
)

#: §8 "Cost cap", frozen by construction: stressed what-if cost ≤ 1.0% of the amount. Shares
#: the number with the §3.1 spread cap by construction only; they measure different things.
TRIAL_COST_CAP_PCT: Final = Decimal("1.0")
#: §8 trial caps: ``max_concurrent`` per leg. 12 × a 60-session cohort is the supervisor's §8 answer
#: (2026-09-30): the only power-grid cell (``scripts/ai_trial_power.py``) clearing the cohort minimum
#: at every horizon. Both legs count against the pool cap, hence ``TRIAL_POOL_MAX_CONCURRENT``.
TRIAL_MAX_CONCURRENT_PER_LEG: Final = 12
TRIAL_ALLOCATED_REASON: Final = "all_trial_entry_gates_passed"


def trial_cost_cap_reason(stressed_cost: Decimal, amount: Decimal) -> str | None:
    """``trial_cost_cap`` when the stressed cost exceeds ``TRIAL_COST_CAP_PCT`` of the amount.

    Compared by multiplication, never a quotient; exactly 1.0% passes (the cap is "exceeds").
    """
    if not (stressed_cost.is_finite() and amount.is_finite()) or amount <= 0 or stressed_cost < 0:
        return "trial_cost_cap"
    return "trial_cost_cap" if stressed_cost * Decimal("100") > TRIAL_COST_CAP_PCT * amount else None


def protective_levels_reason(*, ask: Decimal, stop_rate: Decimal, take_rate: Decimal) -> str | None:
    """§8: ``0 < SL < ask < TP``, all finite, or ``protective_levels_invalid``."""
    if not all(value.is_finite() for value in (ask, stop_rate, take_rate)):
        return "protective_levels_invalid"
    return None if Decimal("0") < stop_rate < ask < take_rate else "protective_levels_invalid"


def _open_leg_trades(conn: psycopg.Connection[Any], deployment_id: int) -> int:
    """Allocated trades on this leg's deployment that are not closed or failed.

    Pending, submitted, uncertain and reconcile-required trades all hold a slot (O8), as the
    ``reserved`` sum does; a decision with no trade row cannot exist (one transaction).
    """
    row = conn.execute(
        """
        SELECT count(*)
        FROM strategy_funding_decisions fd
        LEFT JOIN strategy_trades st ON st.funding_decision_id = fd.funding_decision_id
        WHERE fd.deployment_id = %s AND fd.verdict = 'allocated'
          AND (st.strategy_trade_id IS NULL OR st.status NOT IN ('closed', 'failed'))
        """,
        (deployment_id,),
    ).fetchone()
    conn.commit()
    assert row is not None
    return int(row[0])


def _execute_trial_signal_locked(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    signal_id: int,
    now: datetime | None,
) -> PaperExecutionResult:
    evaluated_at = (now or datetime.now(UTC)).astimezone(UTC)
    existing = _existing_result(conn, signal_id)
    conn.commit()
    if existing is not None and existing.verdict != "submission_uncertain":
        return existing
    signal_row = conn.execute("SELECT strategy_id FROM strategy_signals WHERE signal_id=%s", (signal_id,)).fetchone()
    conn.commit()
    # ⚠ RAISED, never persisted: a funding decision is one per signal, so a refusal written
    # here for a paper signal would pre-empt the path that owns it.
    if signal_row is None or registered_strategy_purpose(str(signal_row[0])) != "demo_trial":
        raise StrategyPaperExecutionError(f"signal {signal_id} is not a demo_trial signal")

    trading_refusal = _trading_enabled_refusal(conn)
    if trading_refusal is not None:
        return _persist_rejection(conn, signal_id=signal_id, reason_code=trading_refusal, now=evaluated_at)
    if existing is not None:
        return _resume_uncertain_submission(conn, broker=broker, existing=existing)
    if _reconciliation_overdue(conn, signal_id):
        return _persist_rejection(conn, signal_id=signal_id, reason_code="reconciliation_overdue", now=evaluated_at)

    intent, reason, halt_identity_evaluated = load_trial_intent(conn, signal_id=signal_id, now=evaluated_at)
    conn.commit()
    if intent is None:
        return _persist_rejection(
            conn,
            signal_id=signal_id,
            reason_code=reason or "preflight_unavailable",
            now=evaluated_at,
            halt_identity_evaluated=halt_identity_evaluated,
        )
    if _open_leg_trades(conn, intent.deployment_id) >= TRIAL_MAX_CONCURRENT_PER_LEG:
        return _persist_rejection(
            conn, signal_id=signal_id, reason_code="trial_leg_slots_full", now=evaluated_at, intent=intent
        )

    try:
        risk = broker.get_account_risk_snapshot()
    except Exception:
        return _persist_rejection(
            conn, signal_id=signal_id, reason_code="account_risk_unavailable", now=evaluated_at, intent=intent
        )
    sized = _risk_and_amount(
        conn,
        intent=intent,
        risk=risk,
        now=evaluated_at,
        requested_ticket=lambda _deployment_base: intent.requested_amount,
    )
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
    # Refuses below the broker open minimum (`below_broker_minimum`): a reduced amount is
    # accepted down to it, never under it (§8 "Sizing").
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
    cost_reason = trial_cost_cap_reason(stressed_cost, amount)
    if cost_reason is not None:
        return _persist_rejection(
            conn, signal_id=signal_id, reason_code=cost_reason, now=evaluated_at, intent=intent, risk=risk
        )
    stop_rate, take_rate = _protective_rates(intent)
    levels_reason = protective_levels_reason(ask=intent.ask, stop_rate=stop_rate, take_rate=take_rate)
    if levels_reason is not None:
        return _persist_rejection(
            conn, signal_id=signal_id, reason_code=levels_reason, now=evaluated_at, intent=intent, risk=risk
        )

    # The durable authority, committed before any broker write (the paper path's design).
    # `decide_funding` re-checks the deployment cap under its locks; `sql/434`'s trigger
    # refuses a trade link whose funded amount exceeds the requested ticket.
    with conn.transaction():
        authority = _commit_authority(
            conn,
            intent=intent,
            amount=amount,
            evaluated_at=evaluated_at,
            costs_at=costs.last_updated,
            risk=risk,
            instrument_invested=instrument_invested,
            drawdown=drawdown,
            stressed_cost=stressed_cost,
            cost_basis=cost_basis,
            stop_rate=stop_rate,
            take_rate=take_rate,
        )
    if isinstance(authority, str):
        return _persist_rejection(
            conn, signal_id=signal_id, reason_code=authority, now=evaluated_at, intent=intent, risk=risk
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
        stop_rate=stop_rate,
        take_rate=take_rate,
    )


#: `strategy_execution_policies` columns that are bookkeeping, not effective terms (O9).
_POLICY_BOOKKEEPING: Final = ["deployment_id", "revision", "updated_by", "reason", "created_at", "updated_at"]


def _authority_refusal(conn: psycopg.Connection[Any], declaration_id: int) -> str | None:
    """Re-check, inside the authority transaction, the two facts a concurrent writer can move.

    * Trial state (§8: "a harm or operator halt stops even already-queued decisions"). The
      loader saw ``active`` before the broker round trips; a halt may have landed since. The
      state-event writer locks the declaration row ``FOR NO KEY UPDATE`` (``sql/432``), so
      ``FOR SHARE`` here waits for an in-flight halt and the re-read then sees it; a halt that
      arrives after this commit finds an order already authorised, as the kill switch does.
    * O9 policy parity: both legs' deployments carry the same effective execution policy. The
      rows are held ``FOR SHARE`` so neither can change before this transaction commits.
    """
    declaration = conn.execute(
        "SELECT strategy_id, strategy_version FROM ai_trial_declarations WHERE declaration_id = %s FOR SHARE",
        (declaration_id,),
    ).fetchone()
    state = conn.execute(
        "SELECT to_state FROM ai_trial_state_events WHERE declaration_id = %s ORDER BY event_id DESC LIMIT 1",
        (declaration_id,),
    ).fetchone()
    if declaration is None or state is None or state[0] != "active":
        return "trial_not_active"
    return trial_policy_parity_refusal(conn, strategy_id=str(declaration[0]), strategy_version=str(declaration[1]))


def trial_policy_parity_refusal(
    conn: psycopg.Connection[Any], *, strategy_id: str, strategy_version: str
) -> str | None:
    """O9: both legs' paper deployments carry the same effective execution policy, or
    ``trial_policy_parity``. Keyed on the ARM's id and version, so the #3471 freeze can run it
    before a declaration exists. The policy rows are held ``FOR SHARE`` to the caller's commit."""
    policies = conn.execute(
        """
        SELECT d.strategy_id, (to_jsonb(p) - %s::text[])::text
        FROM strategy_deployments d
        JOIN strategy_execution_policies p ON p.deployment_id = d.deployment_id
        WHERE d.strategy_id IN (%s, %s || '-control') AND d.strategy_version = %s AND d.mode = 'paper'
        FOR SHARE OF p
        """,
        (_POLICY_BOOKKEEPING, strategy_id, strategy_id, strategy_version),
    ).fetchall()
    if len({row[0] for row in policies}) != 2 or len({row[1] for row in policies}) != 1:
        return "trial_policy_parity"
    return None


def _commit_authority(
    conn: psycopg.Connection[Any],
    *,
    intent: TrialIntent,
    amount: Decimal,
    evaluated_at: datetime,
    costs_at: datetime,
    risk: BrokerAccountRiskSnapshot,
    instrument_invested: Decimal,
    drawdown: Decimal,
    stressed_cost: Decimal,
    cost_basis: str,
    stop_rate: Decimal,
    take_rate: Decimal,
) -> tuple[int, int, UUID] | str:
    """Funding decision, trade, order, request id, preflight and trade link, in the caller's
    transaction; or the late refusal code, with nothing written."""
    late_refusal = _authority_refusal(conn, intent.declaration_id)
    if late_refusal is not None:
        return late_refusal
    signal_id = intent.signal_id
    decision_id = decide_funding(
        conn,
        signal_id=signal_id,
        verdict="allocated",
        deployment_id=intent.deployment_id,
        amount=amount,
        reason_code=TRIAL_ALLOCATED_REASON,
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
    # No forecast, ranking, scan or expectancy column: the trial has none (§8 table).
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
            signal_id,
            intent.deployment_id,
            intent.policy_revision,
            TRIAL_ALLOCATED_REASON,
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
            stop_rate,
            take_rate,
        ),
    )
    conn.execute(
        "INSERT INTO ai_trial_trade_links (pair_id, leg, strategy_trade_id, requested_amount) VALUES (%s, %s, %s, %s)",
        (intent.pair_id, intent.leg, trade_id, intent.requested_amount),
    )
    return trade_id, order_id, request_id


def execute_trial_signal(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    signal_id: int,
    now: datetime | None = None,
) -> PaperExecutionResult:
    """Evaluate one trial leg's fired entry and, only if every gate passes, submit it to demo.

    Serialised with the paper path under the same allocator lock, so the two share one view of
    account risk, the sandbox and pending orders.
    """
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise StrategyPaperExecutionError("trial execution requires an idle connection")
    with _allocator_lock(conn):
        return _execute_trial_signal_locked(conn, broker=broker, signal_id=signal_id, now=now)


__all__ = [
    "TRIAL_ALLOCATED_REASON",
    "TRIAL_COST_CAP_PCT",
    "TRIAL_MAX_CONCURRENT_PER_LEG",
    "execute_trial_signal",
    "protective_levels_reason",
    "trial_cost_cap_reason",
    "trial_policy_parity_refusal",
]
