"""Fail-closed allocator and demo-only executor for fired entry signals.

All observations are current and account-specific. Every refusal is retained as
the signal's compact shadow arm; every allocation commits its intent and broker
idempotency UUID before the sole demo broker write. Manual positions contribute
to account/instrument risk but can never acquire strategy ownership here.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from decimal import ROUND_DOWN, Decimal
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo

import psycopg
import psycopg.rows
from psycopg.pq import TransactionStatus

from app.providers.broker import (
    BrokerAccountRiskSnapshot,
    BrokerEligibilityResponse,
    BrokerOrderSubmissionError,
    BrokerOrderSubmissionUncertain,
    BrokerProvider,
    BrokerStrategyOrder,
    BrokerWhatIfCostResponse,
    BrokerWhatIfOrder,
)
from app.services.market_calendar import us_market_status
from app.services.runtime_config import RuntimeConfigCorrupt, get_runtime_config
from app.services.strategy_control_plane import (
    PAPER_ALLOCATOR_ADVISORY_LOCK,
    StrategyControlError,
    create_strategy_trade,
    decide_funding,
    link_strategy_order,
)
from app.services.strategy_monitoring import load_paper_realised_pnl
from app.services.strategy_order_reconciliation import (
    enforce_reconciliation_slo,
    ensure_strategy_request_id,
)

_NY = ZoneInfo("America/New_York")
_CENT = Decimal("0.01")
_RECURRING_COSTS = frozenset({"overnightfee", "overweekendfee"})
_ALLOCATOR_ADVISORY_LOCK = PAPER_ALLOCATOR_ADVISORY_LOCK


class StrategyPaperExecutionError(StrategyControlError):
    """The executor contract was called with unsafe process state."""


@dataclass(frozen=True)
class PaperExecutionResult:
    signal_id: int
    verdict: Literal["rejected", "submitted", "submission_uncertain", "broker_rejected"]
    reason_code: str
    amount: Decimal | None = None
    strategy_trade_id: int | None = None
    order_id: int | None = None


@dataclass(frozen=True)
class _Intent:
    signal_id: int
    strategy_id: str
    strategy_version: str
    instrument_id: int
    symbol: str
    deployment_id: int
    deployment_limit: Decimal
    pool_limit: Decimal
    capital_mode: Literal["fixed", "compound"]
    pool_reserved: Decimal
    policy_revision: int
    ticket_sizing_mode: Literal["percent", "fixed"]
    ticket_fraction: Decimal | None
    fixed_ticket_amount: Decimal | None
    max_ticket_amount: Decimal
    stop_loss_pct: Decimal
    take_profit_pct: Decimal
    max_quote_age_seconds: int
    max_scan_age_seconds: int
    max_halt_feed_age_seconds: int
    max_cost_age_seconds: int
    max_reconciliation_age_seconds: int
    max_instrument_exposure_pct: Decimal
    max_portfolio_exposure_pct: Decimal
    max_drawdown_pct: Decimal
    min_net_expectancy_pct: Decimal
    cost_stress_multiplier: Decimal
    quote_at: datetime
    ask: Decimal
    scan_at: datetime
    halt_feed_at: datetime
    gross_expectancy_ci_low_pct: Decimal
    reserved: Decimal


def _age_ok(observed_at: datetime, *, now: datetime, max_seconds: int) -> bool:
    return observed_at <= now + timedelta(seconds=5) and observed_at >= now - timedelta(seconds=max_seconds)


def _session_is_open(now: datetime) -> bool:
    local = now.astimezone(_NY)
    status = us_market_status(local.date())
    if status == "closed":
        return False
    close_at = time(13, 0) if status == "half_day" else time(16, 0)
    return time(9, 30) <= local.time().replace(tzinfo=None) < close_at


@contextmanager
def _allocator_lock(conn: psycopg.Connection[Any]) -> Iterator[None]:
    """Serialize the paper account's read-risk-reserve-submit critical section."""
    conn.execute("SELECT pg_advisory_lock(%s, %s)", _ALLOCATOR_ADVISORY_LOCK)
    conn.commit()
    try:
        yield
    finally:
        if conn.info.transaction_status != TransactionStatus.IDLE:
            conn.rollback()
        row = conn.execute("SELECT pg_advisory_unlock(%s, %s)", _ALLOCATOR_ADVISORY_LOCK).fetchone()
        conn.commit()
        if row is None or row[0] is not True:
            raise StrategyPaperExecutionError("paper allocator advisory lock ownership was lost")


def _existing_result(conn: psycopg.Connection[Any], signal_id: int) -> PaperExecutionResult | None:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT d.verdict, d.reason_code, d.amount, t.strategy_trade_id,
                   o.order_id, o.status, o.broker_order_ref
            FROM strategy_funding_decisions d
            LEFT JOIN strategy_trades t ON t.funding_decision_id = d.funding_decision_id
            LEFT JOIN strategy_trade_orders sto
              ON sto.strategy_trade_id = t.strategy_trade_id AND sto.purpose = 'entry'
            LEFT JOIN orders o ON o.order_id = sto.order_id
            WHERE d.signal_id = %s
            """,
            (signal_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    if row["verdict"] == "rejected":
        verdict: Literal["rejected", "submitted", "submission_uncertain", "broker_rejected"] = "rejected"
    elif row["status"] == "rejected":
        verdict = "broker_rejected"
    elif row["status"] == "submitted" and row["broker_order_ref"] is None:
        verdict = "submission_uncertain"
    else:
        verdict = "submitted"
    return PaperExecutionResult(
        signal_id=signal_id,
        verdict=verdict,
        reason_code=str(row["reason_code"]),
        amount=Decimal(str(row["amount"])) if row["amount"] is not None else None,
        strategy_trade_id=int(row["strategy_trade_id"]) if row["strategy_trade_id"] is not None else None,
        order_id=int(row["order_id"]) if row["order_id"] is not None else None,
    )


def _load_intent(conn: psycopg.Connection[Any], *, signal_id: int, now: datetime) -> tuple[_Intent | None, str | None]:
    """Load DB gates as one observation; return a closed reason on absence."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT s.signal_id, s.strategy_id, s.strategy_version, s.instrument_id,
                   i.symbol, i.is_tradable, e.asset_class,
                   d.deployment_id, d.capital_limit, d.enabled, d.currency,
                   pool.enabled AS pool_enabled, pool.capital_limit AS pool_limit,
                   pool.capital_mode,
                   p.revision AS policy_revision, p.*,
                   q.quoted_at, q.ask, q.spread_flag,
                   w.updated_at AS scan_at,
                   h.fetched_at AS halt_feed_at,
                   EXISTS (
                       SELECT 1 FROM strategy_market_halts mh
                       WHERE mh.source = 'nasdaq_trader_rss'
                         AND mh.symbol = upper(i.symbol) AND mh.resumed_at IS NULL
                   ) AS is_halted,
                   EXISTS (
                       SELECT 1 FROM strategy_execution_blocks b WHERE b.active
                   ) AS execution_blocked,
                   (
                       SELECT COALESCE(SUM(fd.amount), 0)
                       FROM strategy_funding_decisions fd
                       LEFT JOIN strategy_trades st ON st.funding_decision_id = fd.funding_decision_id
                       WHERE fd.deployment_id = d.deployment_id AND fd.verdict = 'allocated'
                         AND (st.strategy_trade_id IS NULL OR st.status NOT IN ('closed', 'failed'))
                   ) AS reserved,
                   (
                       SELECT COALESCE(SUM(fd.amount), 0)
                       FROM strategy_funding_decisions fd
                       JOIN strategy_deployments reserved_d
                         ON reserved_d.deployment_id = fd.deployment_id
                        AND reserved_d.mode = 'paper'
                       LEFT JOIN strategy_trades st ON st.funding_decision_id = fd.funding_decision_id
                       WHERE fd.verdict = 'allocated'
                         AND (st.strategy_trade_id IS NULL OR st.status NOT IN ('closed', 'failed'))
                   ) AS pool_reserved,
                   evidence.result_count, evidence.qualified_result_count,
                   evidence.expectancy_ci_low_pct
            FROM strategy_signals s
            JOIN instruments i ON i.instrument_id = s.instrument_id
            LEFT JOIN exchanges e ON e.exchange_id = i.exchange
            LEFT JOIN strategy_deployments d
              ON d.strategy_id = s.strategy_id AND d.strategy_version = s.strategy_version
             AND d.mode = 'paper'
            LEFT JOIN strategy_execution_policies p ON p.deployment_id = d.deployment_id
            LEFT JOIN LATERAL (
                SELECT enabled,capital_limit,capital_mode
                FROM strategy_paper_pool_events
                ORDER BY strategy_paper_pool_event_id DESC
                LIMIT 1
            ) pool ON true
            LEFT JOIN quotes q ON q.instrument_id = s.instrument_id
            LEFT JOIN strategy_scan_watermark w
              ON w.strategy_id = s.strategy_id AND w.strategy_version = s.strategy_version
            LEFT JOIN strategy_halt_feed_state h ON h.source = 'nasdaq_trader_rss'
            LEFT JOIN LATERAL (
                SELECT count(*) AS result_count,
                       count(*) FILTER (WHERE
                           r.expectancy_ci_low_pct IS NOT NULL
                           AND r.namespace = 'hold_out'
                           AND r.window_start >= DATE '2022-01-01'
                           AND r.universe_basis = 'survivorship_free'
                           AND r.carry_unmodelled = false
                           AND r.trial_count IS NOT NULL
                           AND r.deflated_sharpe IS NOT NULL
                           AND r.effective_sample_size IS NOT NULL
                           AND r.synthetic_control_passed = true
                       ) AS qualified_result_count,
                       min(r.expectancy_ci_low_pct) FILTER (WHERE
                           r.expectancy_ci_low_pct IS NOT NULL
                           AND r.namespace = 'hold_out'
                           AND r.window_start >= DATE '2022-01-01'
                           AND r.universe_basis = 'survivorship_free'
                           AND r.carry_unmodelled = false
                           AND r.trial_count IS NOT NULL
                           AND r.deflated_sharpe IS NOT NULL
                           AND r.effective_sample_size IS NOT NULL
                           AND r.synthetic_control_passed = true
                       ) AS expectancy_ci_low_pct
                FROM strategy_promotion_results pr
                JOIN strategy_promotions promotion ON promotion.promotion_id = pr.promotion_id
                JOIN strategy_results_store r ON r.result_id = pr.result_id
                WHERE promotion.strategy_id = s.strategy_id
                  AND promotion.strategy_version = s.strategy_version
            ) evidence ON true
            WHERE s.signal_id = %s AND s.signal_kind = 'entry' AND s.verdict = 'fired'
            """,
            (signal_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None, "signal_not_fired_entry"
    checks = (
        (bool(row["is_tradable"]), "instrument_not_tradable"),
        (row["asset_class"] == "us_equity", "unsupported_market_session"),
        (row["deployment_id"] is not None, "paper_deployment_missing"),
        (row["pool_limit"] is not None, "paper_pool_unconfigured"),
        (bool(row["pool_enabled"]), "paper_pool_disabled"),
        (bool(row["enabled"]), "paper_deployment_disabled"),
        (row["currency"] == "USD", "deployment_currency_unsupported"),
        (row["policy_revision"] is not None, "execution_policy_missing"),
        (not bool(row["execution_blocked"]), "execution_block_active"),
        (row["quoted_at"] is not None and row["ask"] is not None, "quote_missing"),
        (
            row["ask"] is not None and Decimal(str(row["ask"])).is_finite() and Decimal(str(row["ask"])) > 0,
            "quote_ask_invalid",
        ),
        (not bool(row["spread_flag"]), "quote_spread_flagged"),
        (row["scan_at"] is not None, "scan_watermark_missing"),
        (row["halt_feed_at"] is not None, "halt_feed_missing"),
        (not bool(row["is_halted"]), "instrument_halted"),
        (
            int(row["result_count"] or 0) > 0
            and int(row["qualified_result_count"] or 0) == int(row["result_count"])
            and row["expectancy_ci_low_pct"] is not None,
            "expectancy_evidence_missing",
        ),
    )
    for passed, reason in checks:
        if not passed:
            return None, reason
    quote_at = cast(datetime, row["quoted_at"])
    scan_at = cast(datetime, row["scan_at"])
    halt_feed_at = cast(datetime, row["halt_feed_at"])
    if not _age_ok(quote_at, now=now, max_seconds=int(row["max_quote_age_seconds"])):
        return None, "quote_stale"
    if not _age_ok(scan_at, now=now, max_seconds=int(row["max_scan_age_seconds"])):
        return None, "scan_stale"
    if not _age_ok(halt_feed_at, now=now, max_seconds=int(row["max_halt_feed_age_seconds"])):
        return None, "halt_feed_stale"
    if not _session_is_open(now):
        return None, "market_session_closed"
    return _Intent(
        signal_id=signal_id,
        strategy_id=str(row["strategy_id"]),
        strategy_version=str(row["strategy_version"]),
        instrument_id=int(row["instrument_id"]),
        symbol=str(row["symbol"]),
        deployment_id=int(row["deployment_id"]),
        deployment_limit=Decimal(str(row["capital_limit"])),
        pool_limit=Decimal(str(row["pool_limit"])),
        capital_mode=cast(Literal["fixed", "compound"], row["capital_mode"]),
        pool_reserved=Decimal(str(row["pool_reserved"])),
        policy_revision=int(row["policy_revision"]),
        ticket_sizing_mode=cast(Literal["percent", "fixed"], row["ticket_sizing_mode"]),
        ticket_fraction=Decimal(str(row["ticket_fraction"])) if row["ticket_fraction"] is not None else None,
        fixed_ticket_amount=(
            Decimal(str(row["fixed_ticket_amount"])) if row["fixed_ticket_amount"] is not None else None
        ),
        max_ticket_amount=Decimal(str(row["max_ticket_amount"])),
        stop_loss_pct=Decimal(str(row["stop_loss_pct"])),
        take_profit_pct=Decimal(str(row["take_profit_pct"])),
        max_quote_age_seconds=int(row["max_quote_age_seconds"]),
        max_scan_age_seconds=int(row["max_scan_age_seconds"]),
        max_halt_feed_age_seconds=int(row["max_halt_feed_age_seconds"]),
        max_cost_age_seconds=int(row["max_cost_age_seconds"]),
        max_reconciliation_age_seconds=int(row["max_reconciliation_age_seconds"]),
        max_instrument_exposure_pct=Decimal(str(row["max_instrument_exposure_pct"])),
        max_portfolio_exposure_pct=Decimal(str(row["max_portfolio_exposure_pct"])),
        max_drawdown_pct=Decimal(str(row["max_drawdown_pct"])),
        min_net_expectancy_pct=Decimal(str(row["min_net_expectancy_pct"])),
        cost_stress_multiplier=Decimal(str(row["cost_stress_multiplier"])),
        quote_at=quote_at,
        ask=Decimal(str(row["ask"])),
        scan_at=scan_at,
        halt_feed_at=halt_feed_at,
        gross_expectancy_ci_low_pct=Decimal(str(row["expectancy_ci_low_pct"])),
        reserved=Decimal(str(row["reserved"])),
    ), None


def _persist_rejection(
    conn: psycopg.Connection[Any],
    *,
    signal_id: int,
    reason_code: str,
    now: datetime,
    intent: _Intent | None = None,
    risk: BrokerAccountRiskSnapshot | None = None,
) -> PaperExecutionResult:
    existing = _existing_result(conn, signal_id)
    conn.commit()
    if existing is not None:
        return existing
    with conn.transaction():
        decide_funding(conn, signal_id=signal_id, verdict="rejected", reason_code=reason_code)
        conn.execute(
            """
            INSERT INTO strategy_entry_preflights (
                signal_id, deployment_id, policy_revision, verdict, reason_code,
                evaluated_at, quote_at, scan_at, halt_feed_at,
                broker_available_cash, account_equity, account_invested,
                instrument_invested, gross_expectancy_ci_low_pct
            ) VALUES (%s, %s, %s, 'rejected', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                signal_id,
                intent.deployment_id if intent else None,
                intent.policy_revision if intent else None,
                reason_code,
                now,
                intent.quote_at if intent else None,
                intent.scan_at if intent else None,
                intent.halt_feed_at if intent else None,
                risk.available_cash if risk else None,
                risk.equity if risk else None,
                risk.total_invested if risk else None,
                next(
                    (x.amount for x in risk.instrument_investments if x.instrument_id == intent.instrument_id),
                    Decimal("0"),
                )
                if risk and intent
                else None,
                intent.gross_expectancy_ci_low_pct if intent else None,
            ),
        )
    return PaperExecutionResult(signal_id, "rejected", reason_code)


def _eligibility_reason(response: BrokerEligibilityResponse, intent: _Intent, amount: Decimal) -> str | None:
    if response.currency.upper() != "USD" or intent.instrument_id in response.not_found_instrument_ids:
        return "eligibility_unresolved"
    matches = [row for row in response.eligibilities if row.instrument_id == intent.instrument_id]
    if len(matches) != 1 or not matches[0].allow_open_position:
        return "instrument_ineligible"
    arms = [
        arm
        for arm in matches[0].leverage_configs
        if arm.settlement_type.lower() == "real" and arm.direction.upper() == "LONG" and 1 in arm.leverage_values
    ]
    if len(arms) != 1:
        return "eligibility_arm_ambiguous"
    arm = arms[0]
    if arm.allow_stop_loss_take_profit is not True:
        return "fixed_exit_not_allowed"
    minimum = arm.min_position_amount or matches[0].min_position_exposure
    if minimum is None or amount < minimum:
        return "below_broker_minimum"
    return None


def _costs(
    response: BrokerWhatIfCostResponse,
    *,
    intent: _Intent,
    amount: Decimal,
    now: datetime,
) -> tuple[Decimal, Decimal] | str:
    if response.instrument_id != intent.instrument_id or not _age_ok(
        response.last_updated, now=now, max_seconds=intent.max_cost_age_seconds
    ):
        return "costs_stale_or_mismatched"
    total = Decimal("0")
    if not response.costs:
        return "costs_missing"
    for component in response.costs:
        if component.amount is None or component.value is not None:
            return "cost_unit_undocumented"
        if component.currency.upper() != "USD" or component.amount < 0:
            return "cost_currency_or_value_invalid"
        if component.cost_type.replace("_", "").lower() in _RECURRING_COSTS and component.amount > 0:
            return "recurring_cost_horizon_unmodelled"
        total += component.amount
    stressed = total * intent.cost_stress_multiplier
    net = intent.gross_expectancy_ci_low_pct - (stressed / amount * Decimal("100"))
    return stressed, net


def _risk_and_amount(
    conn: psycopg.Connection[Any],
    *,
    intent: _Intent,
    risk: BrokerAccountRiskSnapshot,
    now: datetime,
) -> tuple[Decimal, Decimal, Decimal] | str:
    if not _age_ok(risk.observed_at, now=now, max_seconds=intent.max_quote_age_seconds):
        return "account_risk_stale"
    current_instrument = next(
        (row.amount for row in risk.instrument_investments if row.instrument_id == intent.instrument_id),
        Decimal("0"),
    )
    # A just-accepted strategy order may not yet be visible in the provider's
    # account snapshot. Count every unresolved local submission as additional
    # risk. Once reconciliation is terminal, the provider snapshot is authority.
    pending_row = conn.execute(
        """
        SELECT COALESCE(SUM(d.amount), 0),
               COALESCE(SUM(d.amount) FILTER (WHERE t.instrument_id=%s), 0)
        FROM strategy_funding_decisions d
        JOIN strategy_trades t ON t.funding_decision_id=d.funding_decision_id
        JOIN strategy_trade_orders sto ON sto.strategy_trade_id=t.strategy_trade_id
          AND sto.purpose='entry'
        JOIN orders o ON o.order_id=sto.order_id AND o.execution_origin='strategy'
        LEFT JOIN strategy_order_reconciliation_state r ON r.order_id=o.order_id
        WHERE d.verdict='allocated' AND t.status NOT IN ('closed', 'failed')
          AND (r.state IS NULL OR r.state NOT IN ('resolved', 'rejected'))
        """,
        (intent.instrument_id,),
    ).fetchone()
    assert pending_row is not None
    pending_total = Decimal(str(pending_row[0]))
    pending_instrument = Decimal(str(pending_row[1]))
    capital_bases = _effective_capital_bases(conn, intent)
    conn.commit()
    if isinstance(capital_bases, str):
        return capital_bases
    deployment_base, pool_base = capital_bases
    with conn.transaction():
        row = conn.execute(
            "SELECT equity_high_water FROM strategy_paper_account_risk_state WHERE id = true FOR UPDATE"
        ).fetchone()
        high_water = max(Decimal(str(row[0])) if row else risk.equity, risk.equity)
        drawdown = (high_water - risk.equity) / high_water * Decimal("100")
        conn.execute(
            """
            INSERT INTO strategy_paper_account_risk_state (
                id, equity_high_water, last_equity, last_drawdown_pct, observed_at
            ) VALUES (true, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                equity_high_water = EXCLUDED.equity_high_water,
                last_equity = EXCLUDED.last_equity,
                last_drawdown_pct = EXCLUDED.last_drawdown_pct,
                observed_at = EXCLUDED.observed_at
            """,
            (high_water, risk.equity, drawdown, risk.observed_at),
        )
    if drawdown > intent.max_drawdown_pct:
        return "account_drawdown_limit"
    deployment_remaining = max(Decimal("0"), deployment_base - intent.reserved)
    pool_remaining = max(Decimal("0"), pool_base - intent.pool_reserved)
    portfolio_capacity = max(
        Decimal("0"),
        risk.equity * intent.max_portfolio_exposure_pct / Decimal("100") - risk.total_invested - pending_total,
    )
    instrument_capacity = max(
        Decimal("0"),
        risk.equity * intent.max_instrument_exposure_pct / Decimal("100") - current_instrument - pending_instrument,
    )
    if intent.ticket_sizing_mode == "fixed":
        if intent.fixed_ticket_amount is None:
            return "execution_policy_invalid"
        requested_ticket = intent.fixed_ticket_amount
    else:
        if intent.ticket_fraction is None:
            return "execution_policy_invalid"
        requested_ticket = deployment_base * intent.ticket_fraction
    amount = min(
        requested_ticket,
        intent.max_ticket_amount,
        deployment_remaining,
        pool_remaining,
        max(Decimal("0"), risk.available_cash - pending_total),
        portfolio_capacity,
        instrument_capacity,
    ).quantize(_CENT, rounding=ROUND_DOWN)
    if amount <= 0:
        return "risk_capacity_exhausted"
    return amount, current_instrument, drawdown


def _effective_capital_bases(
    conn: psycopg.Connection[Any],
    intent: _Intent,
) -> tuple[Decimal, Decimal] | str:
    """Resolve capped or compounding bases from realised P&L, never open marks."""
    realised_by_strategy = load_paper_realised_pnl(conn)
    if realised_by_strategy is None:
        return "realised_pnl_incomplete"

    strategy_realised = realised_by_strategy.get((intent.strategy_id, intent.strategy_version), Decimal("0"))
    pool_realised = sum(realised_by_strategy.values(), Decimal("0"))
    if intent.capital_mode == "fixed":
        strategy_realised = min(strategy_realised, Decimal("0"))
        pool_realised = min(pool_realised, Decimal("0"))
    return (
        max(Decimal("0"), intent.deployment_limit + strategy_realised),
        max(Decimal("0"), intent.pool_limit + pool_realised),
    )


def _resume_uncertain_submission(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    existing: PaperExecutionResult,
) -> PaperExecutionResult:
    """Retry a committed intent with its original idempotency identity."""
    if existing.order_id is None or existing.strategy_trade_id is None or existing.amount is None:
        raise StrategyPaperExecutionError("uncertain strategy submission is missing durable authority")
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT o.instrument_id, o.strategy_request_id,
                   p.stop_loss_rate, p.take_profit_rate
            FROM orders o
            JOIN strategy_trade_orders sto
              ON sto.order_id=o.order_id AND sto.purpose='entry'
            JOIN strategy_trades t ON t.strategy_trade_id=sto.strategy_trade_id
            JOIN strategy_funding_decisions d ON d.funding_decision_id=t.funding_decision_id
            JOIN strategy_entry_preflights p ON p.signal_id=d.signal_id AND p.verdict='allocated'
            WHERE o.order_id=%s AND o.execution_origin='strategy'
              AND o.status='submitted' AND o.broker_order_ref IS NULL
            """,
            (existing.order_id,),
        )
        row = cur.fetchone()
    conn.commit()
    if row is None or row["strategy_request_id"] is None:
        raise StrategyPaperExecutionError("uncertain strategy submission identity is incomplete")
    request_id = row["strategy_request_id"]
    try:
        submission = broker.place_demo_strategy_order(
            BrokerStrategyOrder(
                instrument_id=int(row["instrument_id"]),
                amount=existing.amount,
                settlement_type="real",
                stop_loss_rate=Decimal(str(row["stop_loss_rate"])),
                take_profit_rate=Decimal(str(row["take_profit_rate"])),
            ),
            request_id=request_id,
        )
    except BrokerOrderSubmissionError as exc:
        if isinstance(exc, BrokerOrderSubmissionUncertain):
            return existing
        with conn.transaction():
            conn.execute("UPDATE orders SET status='rejected' WHERE order_id=%s", (existing.order_id,))
            conn.execute(
                "UPDATE strategy_trades SET status='failed', updated_at=now() WHERE strategy_trade_id=%s",
                (existing.strategy_trade_id,),
            )
            conn.execute(
                """
                UPDATE strategy_order_reconciliation_state
                SET state='rejected', reconciled_at=now(), last_attempt_at=now(),
                    attempt_count=attempt_count+1,
                    last_error_code='broker_submission_rejected', updated_at=now()
                WHERE order_id=%s
                """,
                (existing.order_id,),
            )
        return PaperExecutionResult(
            existing.signal_id,
            "broker_rejected",
            "broker_submission_rejected",
            existing.amount,
            existing.strategy_trade_id,
            existing.order_id,
        )
    with conn.transaction():
        conn.execute(
            "UPDATE orders SET broker_order_ref=%s WHERE order_id=%s",
            (submission.broker_order_ref, existing.order_id),
        )
        conn.execute(
            "UPDATE strategy_trades SET status='submitted', updated_at=now() WHERE strategy_trade_id=%s",
            (existing.strategy_trade_id,),
        )
    return PaperExecutionResult(
        existing.signal_id,
        "submitted",
        "broker_accepted",
        existing.amount,
        existing.strategy_trade_id,
        existing.order_id,
    )


def _execute_fired_paper_signal_locked(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    signal_id: int,
    now: datetime | None = None,
) -> PaperExecutionResult:
    """Evaluate and, only if every gate aligns, submit one demo order."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise StrategyPaperExecutionError("paper execution requires an idle connection")
    evaluated_at = (now or datetime.now(UTC)).astimezone(UTC)
    existing = _existing_result(conn, signal_id)
    conn.commit()
    if existing is not None and existing.verdict != "submission_uncertain":
        return existing
    try:
        runtime = get_runtime_config(conn)
        kill_row = conn.execute("SELECT is_active FROM kill_switch WHERE id = true").fetchone()
        conn.commit()
    except RuntimeConfigCorrupt:
        return _persist_rejection(conn, signal_id=signal_id, reason_code="runtime_config_corrupt", now=evaluated_at)
    if not runtime.enable_auto_trading:
        return _persist_rejection(conn, signal_id=signal_id, reason_code="auto_trading_disabled", now=evaluated_at)
    if kill_row is None or bool(kill_row[0]):
        return _persist_rejection(
            conn, signal_id=signal_id, reason_code="kill_switch_active_or_missing", now=evaluated_at
        )
    if existing is not None:
        return _resume_uncertain_submission(conn, broker=broker, existing=existing)

    # This updates one bounded current-state block. It never calls the broker.
    provisional = conn.execute(
        """
        SELECT p.max_reconciliation_age_seconds
        FROM strategy_signals s
        JOIN strategy_deployments d ON d.strategy_id=s.strategy_id AND d.strategy_version=s.strategy_version
          AND d.mode='paper'
        JOIN strategy_execution_policies p ON p.deployment_id=d.deployment_id
        WHERE s.signal_id=%s
        """,
        (signal_id,),
    ).fetchone()
    conn.commit()
    if provisional is not None:
        health = enforce_reconciliation_slo(conn, max_unresolved_seconds=int(provisional[0]))
        conn.commit()
        if health.active_block:
            return _persist_rejection(conn, signal_id=signal_id, reason_code="reconciliation_overdue", now=evaluated_at)

    intent, reason = _load_intent(conn, signal_id=signal_id, now=evaluated_at)
    conn.commit()
    if intent is None:
        return _persist_rejection(
            conn,
            signal_id=signal_id,
            reason_code=reason or "preflight_unavailable",
            now=evaluated_at,
        )

    try:
        risk = broker.get_account_risk_snapshot()
    except Exception:
        return _persist_rejection(
            conn, signal_id=signal_id, reason_code="account_risk_unavailable", now=evaluated_at, intent=intent
        )
    sized = _risk_and_amount(conn, intent=intent, risk=risk, now=evaluated_at)
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
    assessed = _costs(costs, intent=intent, amount=amount, now=evaluated_at)
    if isinstance(assessed, str):
        return _persist_rejection(
            conn, signal_id=signal_id, reason_code=assessed, now=evaluated_at, intent=intent, risk=risk
        )
    stressed_cost, net_expectancy = assessed
    if net_expectancy < intent.min_net_expectancy_pct:
        return _persist_rejection(
            conn,
            signal_id=signal_id,
            reason_code="net_expectancy_below_policy",
            now=evaluated_at,
            intent=intent,
            risk=risk,
        )
    stop_rate = (intent.ask * (Decimal("1") - intent.stop_loss_pct / Decimal("100"))).quantize(
        Decimal("0.000001"), rounding=ROUND_DOWN
    )
    take_rate = (intent.ask * (Decimal("1") + intent.take_profit_pct / Decimal("100"))).quantize(
        Decimal("0.000001"), rounding=ROUND_DOWN
    )

    # Re-load under locks through the control-plane allocator. Any concurrent
    # reservation that consumes the cap makes this transaction fail closed.
    with conn.transaction():
        decision_id = decide_funding(
            conn,
            signal_id=signal_id,
            verdict="allocated",
            deployment_id=intent.deployment_id,
            amount=amount,
            reason_code="all_paper_entry_gates_passed",
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
        conn.execute(
            """
            INSERT INTO strategy_entry_preflights (
                signal_id, deployment_id, policy_revision, verdict, reason_code,
                evaluated_at, quote_at, scan_at, halt_feed_at,
                eligibility_checked_at, costs_at, broker_available_cash,
                account_equity, account_invested, instrument_invested,
                account_drawdown_pct, allocated_amount,
                gross_expectancy_ci_low_pct, stressed_cost_amount,
                net_expectancy_pct, stop_loss_rate, take_profit_rate
            ) VALUES (
                %s, %s, %s, 'allocated', 'all_paper_entry_gates_passed',
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                signal_id,
                intent.deployment_id,
                intent.policy_revision,
                evaluated_at,
                intent.quote_at,
                intent.scan_at,
                intent.halt_feed_at,
                evaluated_at,
                costs.last_updated,
                risk.available_cash,
                risk.equity,
                risk.total_invested,
                instrument_invested,
                drawdown,
                amount,
                intent.gross_expectancy_ci_low_pct,
                stressed_cost,
                net_expectancy,
                stop_rate,
                take_rate,
            ),
        )
    # The transaction context commits before this broker call.
    try:
        submission = broker.place_demo_strategy_order(
            BrokerStrategyOrder(
                instrument_id=intent.instrument_id,
                amount=amount,
                settlement_type="real",
                stop_loss_rate=stop_rate,
                take_profit_rate=take_rate,
            ),
            request_id=request_id,
        )
    except BrokerOrderSubmissionError as exc:
        uncertain = isinstance(exc, BrokerOrderSubmissionUncertain)
        with conn.transaction():
            if uncertain:
                conn.execute(
                    "UPDATE strategy_trades SET status='reconcile_required', updated_at=now() "
                    "WHERE strategy_trade_id=%s",
                    (trade_id,),
                )
            else:
                conn.execute("UPDATE orders SET status='rejected' WHERE order_id=%s", (order_id,))
                conn.execute(
                    "UPDATE strategy_trades SET status='failed', updated_at=now() WHERE strategy_trade_id=%s",
                    (trade_id,),
                )
                conn.execute(
                    """
                    UPDATE strategy_order_reconciliation_state
                    SET state='rejected', reconciled_at=now(), last_attempt_at=now(),
                        attempt_count=attempt_count+1, last_error_code='broker_submission_rejected', updated_at=now()
                    WHERE order_id=%s
                    """,
                    (order_id,),
                )
        return PaperExecutionResult(
            signal_id,
            "submission_uncertain" if uncertain else "broker_rejected",
            "submission_uncertain" if uncertain else "broker_submission_rejected",
            amount,
            trade_id,
            order_id,
        )
    except Exception:
        # The broker contract translates transport/response uncertainty into
        # BrokerOrderSubmissionUncertain. Preserve exact reconciliation
        # authority here, but let programming/contract bugs fail loudly.
        with conn.transaction():
            conn.execute(
                "UPDATE strategy_trades SET status='reconcile_required', updated_at=now() WHERE strategy_trade_id=%s",
                (trade_id,),
            )
        raise
    with conn.transaction():
        conn.execute(
            "UPDATE orders SET broker_order_ref=%s WHERE order_id=%s",
            (submission.broker_order_ref, order_id),
        )
        conn.execute(
            "UPDATE strategy_trades SET status='submitted', updated_at=now() WHERE strategy_trade_id=%s",
            (trade_id,),
        )
    return PaperExecutionResult(signal_id, "submitted", "broker_accepted", amount, trade_id, order_id)


def execute_fired_paper_signal(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    signal_id: int,
    now: datetime | None = None,
) -> PaperExecutionResult:
    """Serialize and evaluate one fired signal against whole-account demo risk."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise StrategyPaperExecutionError("paper execution requires an idle connection")
    with _allocator_lock(conn):
        return _execute_fired_paper_signal_locked(conn, broker=broker, signal_id=signal_id, now=now)


__all__ = ["PaperExecutionResult", "StrategyPaperExecutionError", "execute_fired_paper_signal"]
