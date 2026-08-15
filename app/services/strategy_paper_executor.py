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
from datetime import UTC, date, datetime, time, timedelta
from decimal import ROUND_DOWN, Decimal
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo

import psycopg
import psycopg.rows
from psycopg.pq import TransactionStatus

from app.providers.broker import (
    BrokerAccountRiskSnapshot,
    BrokerCostComponent,
    BrokerEligibilityResponse,
    BrokerOrderSubmissionError,
    BrokerOrderSubmissionUncertain,
    BrokerProvider,
    BrokerStrategyOrder,
    BrokerWhatIfCostResponse,
    BrokerWhatIfOrder,
)
from app.services.broker_settlement_arms import select_underlying_long_arms
from app.services.cost_model import COST_MODEL_ID
from app.services.market_calendar import us_market_status
from app.services.price_masked_bars import QUARANTINE_RULE_SET_VERSION
from app.services.runtime_config import RuntimeConfigCorrupt, get_runtime_config
from app.services.strategy_base_currency import (
    DEPLOYMENT_CURRENCY_UNSUPPORTED,
    SUPPORTED_DEPLOYMENT_CURRENCIES,
)
from app.services.strategy_control_plane import (
    PAPER_ALLOCATOR_ADVISORY_LOCK,
    StrategyControlError,
    create_strategy_trade,
    decide_funding,
    link_strategy_order,
    registered_strategy_purpose,
    validate_paper_promotion_evidence,
)
from app.services.strategy_forecast_outcome_resolution import RESOLVER_VERSION as FORECAST_OUTCOME_RESOLVER_VERSION
from app.services.strategy_halt_identity import (
    HALT_IDENTITY_RULE_VERSION,
    INSTRUMENT_HALT_SYMBOL_SQL,
)
from app.services.strategy_monitoring import load_paper_realised_pnl
from app.services.strategy_opportunity_forecast import FORECAST_POLICY_VERSION
from app.services.strategy_opportunity_ranker import RANKING_POLICY_VERSION
from app.services.strategy_order_reconciliation import (
    enforce_reconciliation_slo,
    ensure_strategy_request_id,
    reconcile_strategy_order,
)

_NY = ZoneInfo("America/New_York")
_CENT = Decimal("0.01")
_RECURRING_COSTS = frozenset({"overnightfee", "overweekendfee"})

#: What priced ``strategy_entry_preflights.stressed_cost_amount`` (#2598 steps 3-4).
#:
#: ``_costs`` is the only producer — the broker's own what-if components, summed and
#: multiplied by the deployment's ``cost_stress_multiplier`` — but WHICH FIELD carried
#: the money is the audit fact that matters, because one of the two is off-spec.
#: ``amount`` is documented; ``value`` is not, and is what the live demo response
#: actually sends. A row already written can never be repaired to say which it was.
COST_BASIS_BROKER_PREFLIGHT_AMOUNT = "broker_preflight_amount"
COST_BASIS_BROKER_PREFLIGHT_VALUE = "broker_preflight_value"

#: ⚠ BOTH MEMBERS ARE REACHABLE — each is returned by its own branch of
#: ``_component_amount``, and both branches are tested. That is the bar this set is
#: held to: #2598's scope also names ``static_band_bound`` (the banded static model as
#: a declared execution bound) and it is deliberately absent, because no code produces
#: it and the census measurement argues against ever writing one (`sql/342` header).
#:
#: ⚠ ADDING A MEMBER IS A COORDINATED CHANGE — `sql/343`'s
#: ``strategy_entry_preflights_cost_basis_vocabulary`` CHECK does not read this
#: constant, so a value added here alone fails at INSERT rather than at review.
#: ``tests/test_2598_preflight_cost_basis.py`` fails when either side moves alone.
COST_BASES: frozenset[str] = frozenset({COST_BASIS_BROKER_PREFLIGHT_AMOUNT, COST_BASIS_BROKER_PREFLIGHT_VALUE})
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
    # The deployment's own currency, already proven supported by the `checks` tuple
    # (which short-circuits before this dataclass is built). Broker responses are
    # compared for EQUALITY against this, never for membership of the supported set --
    # see `_eligibility_reason`.
    currency: str
    deployment_limit: Decimal
    pool_limit: Decimal
    capital_mode: Literal["fixed", "compound"]
    pool_reserved: Decimal
    mandate_max_drawdown_pct: Decimal
    mandate_max_loss_per_position_pct: Decimal
    mandate_max_daily_loss_pct: Decimal
    mandate_active_risk_budget_pct: Decimal
    mandate_cash_reserve_pct: Decimal
    mandate_max_concurrent_positions: int
    forecast_id: int
    ranking_member_id: int
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
                   o.order_id, o.status, o.broker_order_ref, r.last_error_code
            FROM strategy_funding_decisions d
            LEFT JOIN strategy_trades t ON t.funding_decision_id = d.funding_decision_id
            LEFT JOIN strategy_trade_orders sto
              ON sto.strategy_trade_id = t.strategy_trade_id AND sto.purpose = 'entry'
            LEFT JOIN orders o ON o.order_id = sto.order_id
            LEFT JOIN strategy_order_reconciliation_state r ON r.order_id = o.order_id
            WHERE d.signal_id = %s
            """,
            (signal_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    if row["verdict"] == "rejected":
        verdict: Literal["rejected", "submitted", "submission_uncertain", "broker_rejected"] = "rejected"
        reason_code = str(row["reason_code"])
    elif row["status"] == "rejected":
        verdict = "broker_rejected"
        reason_code = str(row["last_error_code"] or "broker_order_rejected")
    elif row["status"] == "submitted" and row["broker_order_ref"] is None:
        verdict = "submission_uncertain"
        reason_code = "submission_uncertain"
    else:
        verdict = "submitted"
        reason_code = "broker_accepted"
    return PaperExecutionResult(
        signal_id=signal_id,
        verdict=verdict,
        reason_code=reason_code,
        amount=Decimal(str(row["amount"])) if row["amount"] is not None else None,
        strategy_trade_id=int(row["strategy_trade_id"]) if row["strategy_trade_id"] is not None else None,
        order_id=int(row["order_id"]) if row["order_id"] is not None else None,
    )


def _load_intent(
    conn: psycopg.Connection[Any],
    *,
    signal_id: int,
    ranking_member_id: int | None,
    now: datetime,
) -> tuple[_Intent | None, str | None, bool]:
    """Load one DB observation; the bool says its halt SQL expression ran.

    A fetched row carries ``True`` even when an earlier Python gate rejects it:
    ``is_halted`` was still selected by the same SQL statement. No row means the
    halt identity rule was not observed and its audit version must remain NULL.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT s.signal_id, s.strategy_id, s.strategy_version, s.instrument_id,
                   i.symbol, i.is_tradable, e.asset_class,
                   d.deployment_id, d.capital_limit, d.enabled, d.currency,
                   pool.strategy_paper_pool_event_id AS current_pool_event_id,
                   pool.enabled AS pool_enabled, pool.capital_limit AS pool_limit,
                   pool.capital_mode, pool.risk_profile,
                   pool.max_portfolio_drawdown_pct AS mandate_max_drawdown_pct,
                   pool.max_loss_per_position_pct AS mandate_max_loss_per_position_pct,
                   pool.max_daily_loss_pct AS mandate_max_daily_loss_pct,
                   pool.active_risk_budget_pct AS mandate_active_risk_budget_pct,
                   pool.cash_reserve_pct AS mandate_cash_reserve_pct,
                   pool.max_concurrent_positions AS mandate_max_concurrent_positions,
                   p.revision AS policy_revision, p.*,
                   q.quoted_at, q.ask, q.spread_flag,
                   w.updated_at AS scan_at,
                   h.fetched_at AS halt_feed_at,
                   EXISTS (
                       SELECT 1 FROM strategy_market_halts mh
                       WHERE mh.source = 'nasdaq_trader_rss'
                         AND mh.symbol = {INSTRUMENT_HALT_SYMBOL_SQL} AND mh.resumed_at IS NULL
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
                   evidence.expectancy_ci_low_pct,
                   forecast.forecast_id,forecast.forecast_policy_version,
                   forecast.decided_at AS forecast_decided_at,
                   forecast.valid_through AS forecast_valid_through,
                   forecast.target_barrier_pct AS forecast_target_barrier_pct,
                   forecast.stop_barrier_pct AS forecast_stop_barrier_pct,
                   forecast.conservative_net_expectancy_pct AS forecast_conservative_expectancy,
                   forecast.cost_model_id AS forecast_cost_model_id,
                   calibration.passed AS calibration_passed,
                   calibration.model_version AS calibration_model_version,
                   calibration.holdout_end AS calibration_holdout_end,
                   assessment_policy.policy_id AS assessment_policy_id,
                   assessment_policy.max_assessment_age_days,
                   current_assessment.checked_at AS prospective_assessment_checked_at,
                   prospective_assessment.assessment_id AS prospective_assessment_id,
                   prospective_assessment.passed AS prospective_assessment_passed,
                   prospective_assessment.window_start AS prospective_assessment_window_start,
                   prospective_assessment.window_end AS prospective_assessment_window_end,
                   current_stage.to_stage AS current_stage,
                   paper_approval.forward_started_at,
                   paper_approval.forward_evidence_ready,
                   ranking.ranking_member_id,ranking.selected AS ranking_selected,
                   ranking_batch.ranking_policy_version,
                   ranking_batch.strategy_paper_pool_event_id AS ranking_pool_event_id
            FROM strategy_signals s
            JOIN instruments i ON i.instrument_id = s.instrument_id
            LEFT JOIN exchanges e ON e.exchange_id = i.exchange
            LEFT JOIN strategy_deployments d
              ON d.strategy_id = s.strategy_id AND d.strategy_version = s.strategy_version
             AND d.mode = 'paper'
            LEFT JOIN strategy_execution_policies p ON p.deployment_id = d.deployment_id
            LEFT JOIN LATERAL (
                SELECT promotion.to_stage
                FROM strategy_promotions promotion
                WHERE promotion.strategy_id=s.strategy_id
                  AND promotion.strategy_version=s.strategy_version
                ORDER BY promotion.promotion_id DESC
                LIMIT 1
            ) current_stage ON true
            LEFT JOIN LATERAL (
                SELECT forward_promotion.promoted_at AS forward_started_at,
                       (forward_evidence.promotion_id IS NOT NULL) AS forward_evidence_ready
                FROM strategy_promotions paper_promotion
                JOIN strategy_promotions forward_promotion
                  ON forward_promotion.strategy_id=paper_promotion.strategy_id
                 AND forward_promotion.strategy_version=paper_promotion.strategy_version
                 AND forward_promotion.to_stage='forward_observation'
                LEFT JOIN strategy_promotion_forward_evidence forward_evidence
                  ON forward_evidence.promotion_id=paper_promotion.promotion_id
                WHERE paper_promotion.strategy_id=s.strategy_id
                  AND paper_promotion.strategy_version=s.strategy_version
                  AND paper_promotion.to_stage='paper_enabled'
                LIMIT 1
            ) paper_approval ON true
            LEFT JOIN LATERAL (
                SELECT strategy_paper_pool_event_id,enabled,capital_limit,capital_mode,risk_profile,
                       max_portfolio_drawdown_pct,max_loss_per_position_pct,
                       max_daily_loss_pct,active_risk_budget_pct,cash_reserve_pct,
                       max_concurrent_positions
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
                           AND r.fx_unmodelled = false
                           AND r.trial_count IS NOT NULL
                           AND r.deflated_sharpe IS NOT NULL
                           AND r.effective_sample_size IS NOT NULL
                           AND control_support.candidate_count = 1
                           AND control_result.synthetic_control_passed = true
                       ) AS qualified_result_count,
                       min(r.expectancy_ci_low_pct) FILTER (WHERE
                           r.expectancy_ci_low_pct IS NOT NULL
                           AND r.namespace = 'hold_out'
                           AND r.window_start >= DATE '2022-01-01'
                           AND r.universe_basis = 'survivorship_free'
                           AND r.carry_unmodelled = false
                           AND r.fx_unmodelled = false
                           AND r.trial_count IS NOT NULL
                           AND r.deflated_sharpe IS NOT NULL
                           AND r.effective_sample_size IS NOT NULL
                           AND control_support.candidate_count = 1
                           AND control_result.synthetic_control_passed = true
                       ) AS expectancy_ci_low_pct
                FROM strategy_promotion_results pr
                JOIN strategy_promotions promotion ON promotion.promotion_id = pr.promotion_id
                JOIN strategy_results_store r ON r.result_id = pr.result_id
                LEFT JOIN strategy_result_control_support control_support
                  ON control_support.holdout_result_id = r.result_id
                LEFT JOIN strategy_results_store control_result
                  ON control_result.result_id = control_support.control_result_id
                WHERE promotion.strategy_id = s.strategy_id
                  AND promotion.strategy_version = s.strategy_version
            ) evidence ON true
            LEFT JOIN strategy_opportunity_forecasts forecast ON forecast.signal_id=s.signal_id
            LEFT JOIN strategy_forecast_calibrations calibration
              ON calibration.calibration_id=forecast.calibration_id
            LEFT JOIN LATERAL (
                SELECT policy_id,max_assessment_age_days
                FROM strategy_forecast_assessment_policies
                WHERE effective_from <= %s
                ORDER BY effective_from DESC LIMIT 1
            ) assessment_policy ON true
            LEFT JOIN strategy_forecast_assessment_current current_assessment
             ON current_assessment.policy_id=assessment_policy.policy_id
             AND current_assessment.strategy_id=s.strategy_id
             AND current_assessment.strategy_version=s.strategy_version
             AND current_assessment.forecast_policy_version=forecast.forecast_policy_version
             AND current_assessment.model_version=calibration.model_version
             AND current_assessment.calibration_id=calibration.calibration_id
             AND current_assessment.setup_version=forecast.setup_version
             AND current_assessment.exit_policy_version=forecast.exit_policy_version
             AND current_assessment.resolver_version=%s
             AND current_assessment.input_rule_set_version=%s
            LEFT JOIN strategy_forecast_assessments prospective_assessment
              ON prospective_assessment.assessment_id=current_assessment.assessment_id
             AND prospective_assessment.policy_id=current_assessment.policy_id
             AND prospective_assessment.strategy_id=current_assessment.strategy_id
             AND prospective_assessment.strategy_version=current_assessment.strategy_version
             AND prospective_assessment.forecast_policy_version=current_assessment.forecast_policy_version
             AND prospective_assessment.model_version=current_assessment.model_version
             AND prospective_assessment.calibration_id=current_assessment.calibration_id
             AND prospective_assessment.setup_version=current_assessment.setup_version
             AND prospective_assessment.exit_policy_version=current_assessment.exit_policy_version
             AND prospective_assessment.resolver_version=current_assessment.resolver_version
             AND prospective_assessment.input_rule_set_version=current_assessment.input_rule_set_version
            LEFT JOIN strategy_opportunity_ranking_members ranking
              ON ranking.ranking_member_id=%s AND ranking.forecast_id=forecast.forecast_id
            LEFT JOIN strategy_opportunity_ranking_batches ranking_batch
              ON ranking_batch.ranking_batch_id=ranking.ranking_batch_id
            WHERE s.signal_id = %s AND s.signal_kind = 'entry' AND s.verdict = 'fired'
            """,
            (now, FORECAST_OUTCOME_RESOLVER_VERSION, QUARANTINE_RULE_SET_VERSION, ranking_member_id, signal_id),
        )
        row = cur.fetchone()
    if row is None:
        return None, "signal_not_fired_entry", False
    try:
        validate_paper_promotion_evidence(
            conn,
            strategy_id=str(row["strategy_id"]),
            strategy_version=str(row["strategy_version"]),
        )
    except (StrategyControlError, RuntimeError):
        return None, "pinned_promotion_evidence_invalid", True
    checks = (
        (bool(row["is_tradable"]), "instrument_not_tradable"),
        (row["asset_class"] == "us_equity", "unsupported_market_session"),
        (row["deployment_id"] is not None, "paper_deployment_missing"),
        (row["pool_limit"] is not None, "paper_pool_unconfigured"),
        (bool(row["pool_enabled"]), "paper_pool_disabled"),
        (row["risk_profile"] is not None and row["risk_profile"] != "unconfigured", "portfolio_mandate_unconfigured"),
        (
            all(
                row[field] is not None
                for field in (
                    "mandate_max_drawdown_pct",
                    "mandate_max_loss_per_position_pct",
                    "mandate_max_daily_loss_pct",
                    "mandate_active_risk_budget_pct",
                    "mandate_cash_reserve_pct",
                    "mandate_max_concurrent_positions",
                )
            ),
            "portfolio_mandate_incomplete",
        ),
        (bool(row["enabled"]), "paper_deployment_disabled"),
        (row["current_stage"] in {"paper_enabled", "live_enabled"}, "paper_stage_required"),
        (bool(row["forward_evidence_ready"]), "paper_forward_evidence_missing"),
        (row["currency"] in SUPPORTED_DEPLOYMENT_CURRENCIES, DEPLOYMENT_CURRENCY_UNSUPPORTED),
        (row["policy_revision"] is not None, "execution_policy_missing"),
        (row["forecast_id"] is not None, "opportunity_forecast_missing"),
        (row["forecast_policy_version"] == FORECAST_POLICY_VERSION, "opportunity_forecast_policy_stale"),
        (bool(row["calibration_passed"]), "opportunity_calibration_not_passed"),
        (row["assessment_policy_id"] is not None, "opportunity_assessment_policy_missing"),
        (row["prospective_assessment_id"] is not None, "opportunity_assessment_missing"),
        (bool(row["prospective_assessment_passed"]), "opportunity_assessment_not_passed"),
        (row["forecast_cost_model_id"] == COST_MODEL_ID, "opportunity_forecast_cost_model_stale"),
        (
            row["forecast_target_barrier_pct"] is not None
            and Decimal(str(row["forecast_target_barrier_pct"])).is_finite()
            and Decimal(str(row["forecast_target_barrier_pct"])) > 0,
            "opportunity_forecast_target_barrier_missing",
        ),
        (
            row["forecast_stop_barrier_pct"] is not None
            and Decimal(str(row["forecast_stop_barrier_pct"])).is_finite()
            and Decimal(str(row["forecast_stop_barrier_pct"])) > 0,
            "opportunity_forecast_stop_barrier_missing",
        ),
        (
            row["forecast_stop_barrier_pct"] is not None
            and row["stop_loss_pct"] is not None
            and Decimal(str(row["forecast_stop_barrier_pct"])) <= Decimal(str(row["stop_loss_pct"])),
            "opportunity_forecast_stop_exceeds_policy",
        ),
        (
            row["forecast_conservative_expectancy"] is not None
            and Decimal(str(row["forecast_conservative_expectancy"])) > 0,
            "opportunity_forecast_expectancy_not_positive",
        ),
        (row["ranking_member_id"] is not None, "opportunity_ranking_member_missing"),
        (bool(row["ranking_selected"]), "opportunity_ranking_member_not_selected"),
        (row["ranking_policy_version"] == RANKING_POLICY_VERSION, "opportunity_ranking_policy_stale"),
        (row["ranking_pool_event_id"] == row["current_pool_event_id"], "opportunity_ranking_mandate_stale"),
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
            return None, reason, True
    quote_at = cast(datetime, row["quoted_at"])
    scan_at = cast(datetime, row["scan_at"])
    halt_feed_at = cast(datetime, row["halt_feed_at"])
    if not _age_ok(quote_at, now=now, max_seconds=int(row["max_quote_age_seconds"])):
        return None, "quote_stale", True
    if not _age_ok(scan_at, now=now, max_seconds=int(row["max_scan_age_seconds"])):
        return None, "scan_stale", True
    if not _age_ok(halt_feed_at, now=now, max_seconds=int(row["max_halt_feed_age_seconds"])):
        return None, "halt_feed_stale", True
    forecast_decided_at = cast(datetime, row["forecast_decided_at"])
    forecast_valid_through = cast(datetime, row["forecast_valid_through"])
    if forecast_decided_at > now or forecast_valid_through < now:
        return None, "opportunity_forecast_not_current", True
    if (
        row["calibration_holdout_end"] is None
        or cast(date, row["calibration_holdout_end"]) >= forecast_decided_at.date()
    ):
        return None, "opportunity_calibration_knowledge_time_invalid", True
    if row["prospective_assessment_checked_at"] is None or not _age_ok(
        cast(datetime, row["prospective_assessment_checked_at"]),
        now=now,
        max_seconds=int(row["max_assessment_age_days"]) * 86_400,
    ):
        return None, "opportunity_assessment_stale", True
    assessment_checked_at = cast(datetime, row["prospective_assessment_checked_at"])
    assessment_window_start = cast(date | None, row["prospective_assessment_window_start"])
    assessment_window_end = cast(date | None, row["prospective_assessment_window_end"])
    forward_started_at = cast(datetime | None, row["forward_started_at"])
    if (
        assessment_window_start is None
        or assessment_window_end is None
        or forward_started_at is None
        or assessment_window_start <= forward_started_at.date()
        or assessment_window_end > assessment_checked_at.date()
        or assessment_checked_at <= forward_started_at
    ):
        return None, "opportunity_assessment_pre_forward", True
    if not _session_is_open(now):
        return None, "market_session_closed", True
    return (
        _Intent(
            signal_id=signal_id,
            strategy_id=str(row["strategy_id"]),
            strategy_version=str(row["strategy_version"]),
            instrument_id=int(row["instrument_id"]),
            symbol=str(row["symbol"]),
            deployment_id=int(row["deployment_id"]),
            currency=str(row["currency"]),
            deployment_limit=Decimal(str(row["capital_limit"])),
            pool_limit=Decimal(str(row["pool_limit"])),
            capital_mode=cast(Literal["fixed", "compound"], row["capital_mode"]),
            pool_reserved=Decimal(str(row["pool_reserved"])),
            mandate_max_drawdown_pct=Decimal(str(row["mandate_max_drawdown_pct"])),
            mandate_max_loss_per_position_pct=Decimal(str(row["mandate_max_loss_per_position_pct"])),
            mandate_max_daily_loss_pct=Decimal(str(row["mandate_max_daily_loss_pct"])),
            mandate_active_risk_budget_pct=Decimal(str(row["mandate_active_risk_budget_pct"])),
            mandate_cash_reserve_pct=Decimal(str(row["mandate_cash_reserve_pct"])),
            mandate_max_concurrent_positions=int(row["mandate_max_concurrent_positions"]),
            forecast_id=int(row["forecast_id"]),
            ranking_member_id=int(row["ranking_member_id"]),
            policy_revision=int(row["policy_revision"]),
            ticket_sizing_mode=cast(Literal["percent", "fixed"], row["ticket_sizing_mode"]),
            ticket_fraction=Decimal(str(row["ticket_fraction"])) if row["ticket_fraction"] is not None else None,
            fixed_ticket_amount=(
                Decimal(str(row["fixed_ticket_amount"])) if row["fixed_ticket_amount"] is not None else None
            ),
            max_ticket_amount=Decimal(str(row["max_ticket_amount"])),
            stop_loss_pct=Decimal(str(row["forecast_stop_barrier_pct"])),
            take_profit_pct=Decimal(str(row["forecast_target_barrier_pct"])),
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
        ),
        None,
        True,
    )


def _persist_rejection(
    conn: psycopg.Connection[Any],
    *,
    signal_id: int,
    reason_code: str,
    now: datetime,
    intent: _Intent | None = None,
    risk: BrokerAccountRiskSnapshot | None = None,
    halt_identity_evaluated: bool = False,
    costs_at: datetime | None = None,
    cost_assessment: _CostAssessment | None = None,
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
                signal_id, deployment_id, policy_revision, forecast_id, ranking_member_id,
                verdict, reason_code,
                evaluated_at, quote_at, scan_at, halt_feed_at, halt_identity_rule_version,
                costs_at,
                broker_available_cash, account_equity, account_invested,
                instrument_invested, gross_expectancy_ci_low_pct,
                stressed_cost_amount, net_expectancy_pct, cost_basis
            ) VALUES (
                %s, %s, %s, %s, %s, 'rejected', %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                signal_id,
                intent.deployment_id if intent else None,
                intent.policy_revision if intent else None,
                intent.forecast_id if intent else None,
                intent.ranking_member_id if intent else None,
                reason_code,
                now,
                intent.quote_at if intent else None,
                intent.scan_at if intent else None,
                intent.halt_feed_at if intent else None,
                HALT_IDENTITY_RULE_VERSION if intent is not None or halt_identity_evaluated else None,
                costs_at,
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
                cost_assessment.stressed if cost_assessment else None,
                cost_assessment.net if cost_assessment else None,
                cost_assessment.basis if cost_assessment else None,
            ),
        )
    return PaperExecutionResult(signal_id, "rejected", reason_code)


def _eligibility_reason(response: BrokerEligibilityResponse, intent: _Intent, amount: Decimal) -> str | None:
    # EQUALITY against the deployment's currency, not membership of the supported set.
    # The two coincide while only USD is supported, but membership is the shape that
    # breaks on widening: with {"USD","GBP"} a response quoted in GBP would satisfy a
    # membership test on a USD deployment, and `_costs` would then sum a USD component
    # and a GBP one into a single total with no FX -- the arithmetic #2363 refused.
    # Equality here and in `_costs` also ties eligibility and costs to each other.
    #
    # `.upper()` only, no strip(): what the broker may put in this field is its
    # contract's business, not ISO's, and `" USD "` is rejected today.
    if response.currency.upper() != intent.currency or intent.instrument_id in response.not_found_instrument_ids:
        return "eligibility_unresolved"
    matches = [row for row in response.eligibilities if row.instrument_id == intent.instrument_id]
    if len(matches) != 1 or not matches[0].allow_open_position:
        return "instrument_ineligible"
    # One definition of "the underlying product, held long, unleveraged", shared
    # with #2603 item 2's core eligibility proof. It is also stricter than the
    # inline version it replaces: `bool` is an `int`, so `1 in leverage_values`
    # was true for `leverageValues: [true]`, which the provider parser admits.
    arms = select_underlying_long_arms(matches[0])
    # ⚠ Zero arms and many arms are DIFFERENT answers and get different codes
    # (#2678). Zero means the broker read the request fine and this instrument is
    # simply not offered as the underlying product on this account -- a fact about
    # the INSTRUMENT. More than one means the response cannot be read -- a fact
    # about the RESPONSE. Collapsing them sent triage looking for a parser bug
    # that does not exist: SPY (3000) returns three arms, all `cfd`, so zero
    # qualify and it filed as "ambiguous" when nothing was ambiguous.
    if not arms:
        return "no_underlying_arm"
    if len(arms) > 1:
        return "eligibility_arm_ambiguous"
    # `next(iter(...))`, not `arms[0]`: pyright narrows a tuple through the two
    # length guards above to `tuple[()]` and rejects the subscript.
    arm = next(iter(arms))
    if arm.allow_stop_loss_take_profit is not True:
        return "fixed_exit_not_allowed"
    minimum = arm.min_position_amount or matches[0].min_position_exposure
    if minimum is None or amount < minimum:
        return "below_broker_minimum"
    return None


@dataclass(frozen=True)
class _CostAssessment:
    """A priced preflight, WITH the field its money came out of.

    ⚠ The basis is part of the result rather than re-derived at the INSERT, because
    the caller cannot see which field each component used and would have to guess.
    """

    stressed: Decimal
    net: Decimal
    basis: str


def _component_amount(component: BrokerCostComponent) -> tuple[Decimal, str] | None:
    """The component's monetary amount and which field carried it, or ``None``.

    ⚠⚠ eToro SHIPS THE DENOMINATOR OF A FIELD IT DOES NOT SHIP. Verified against the
    live portal 2026-08-13 (`.claude/skills/data-sources/etoro-api.md` protocol): the
    documented row is ``costType`` + ``amount`` (*"the monetary value of this cost
    component, expressed in currency"*) + ``currency`` (*"ISO 4217 currency code in
    which amount is denominated"*), and ``value`` appears NOWHERE in the docs. The
    live demo response carries keys ``['costType', 'currency', 'value']`` — ``amount``
    is absent as a KEY, not present-and-null — with ``currency`` returned as ``USD``.

    Accepting ``value`` is #2598 scope 4, and it rests on a measurement rather than on
    the drift being tolerable: over 60 instruments stratified across the four cost
    bands, ``value / ticket`` lands on our separately observed quoted spread at a
    population median of **0.995x** (`--replay
    tests/fixtures/etoro_preflight_2598/band_census_2026-08-13.json`), the rounding
    quantum behaves as a monetary field must and a rate cannot, and ``currency`` is
    present on every observation. ⚠ That decodes the UNIT only — both sides trace to
    eToro, so it is not corroboration of the spread's LEVEL.

    ⚠ BOTH FIELDS PRESENT IS STILL A REFUSAL. It has never been observed, the two
    could disagree, and there is no documented rule for which wins.
    """
    if component.amount is not None and component.value is None:
        return component.amount, COST_BASIS_BROKER_PREFLIGHT_AMOUNT
    if component.amount is None and component.value is not None:
        return component.value, COST_BASIS_BROKER_PREFLIGHT_VALUE
    return None


def _costs(
    response: BrokerWhatIfCostResponse,
    *,
    intent: _Intent,
    amount: Decimal,
    now: datetime,
) -> _CostAssessment | str:
    if response.instrument_id != intent.instrument_id or not _age_ok(
        response.last_updated, now=now, max_seconds=intent.max_cost_age_seconds
    ):
        return "costs_stale_or_mismatched"
    total = Decimal("0")
    if not response.costs:
        return "costs_missing"
    bases: set[str] = set()
    for component in response.costs:
        resolved = _component_amount(component)
        if resolved is None:
            return "cost_unit_undocumented"
        component_amount, basis = resolved
        bases.add(basis)
        # Equality against the deployment currency for the reason given in
        # `_eligibility_reason`: this is the loop whose `total +=` would otherwise add
        # two currencies together.
        if component.currency.upper() != intent.currency or component_amount < 0:
            return "cost_currency_or_value_invalid"
        # ⚠⚠ THIS READS THE RESOLVED AMOUNT, AND THAT IS THE WHOLE CARE IN THIS
        # FUNCTION. It used to read `component.amount`, which was guaranteed non-NULL
        # only because the refusal above rejected every row that lacked it. Now that
        # `value` is accepted, an `overnightFee` carrying its charge in `value` would
        # compare `None > 0` — a TypeError at best, and at worst a rewrite that
        # skipped the row and let an unmodelled carry through the gate that exists to
        # stop it (#2363's standing refusal).
        if component.cost_type.replace("_", "").lower() in _RECURRING_COSTS and component_amount > 0:
            return "recurring_cost_horizon_unmodelled"
        total += component_amount
    if len(bases) != 1:
        # ⚠ A MIXED response is unobserved and unexplained: summing a documented field
        # and an off-spec one into a single total assumes they mean the same thing,
        # which is exactly what has not been established for a response that uses both.
        return "cost_unit_undocumented"
    stressed = total * intent.cost_stress_multiplier
    net = intent.gross_expectancy_ci_low_pct - (stressed / amount * Decimal("100"))
    return _CostAssessment(stressed=stressed, net=net, basis=bases.pop())


# The two PORTFOLIO-MANDATE observations that are sourced from OUR tables rather
# than from the broker account snapshot -- and therefore the only two that went
# blind to a core position when sql/349 added the core/cash arm.
#
# ⚠⚠ BOTH FEED HARD GATES, NOT REPORTS.  `open_strategy_lifecycles` is compared
# against `mandate_max_concurrent_positions`, and `daily_realised_pnl` against
# the daily loss limit; each returns a refusal.  The first inventory of this
# slice classified these by JOIN SHAPE and so filed the second as cosmetic P&L
# history and missed the first entirely.  Classify a query by the DECISION IT
# FEEDS, not by the tables it touches.
#
# Why every OTHER mandate control needed no change: `portfolio_capacity`,
# `instrument_capacity` and `drawdown` read `risk.total_invested`,
# `risk.instrument_investments` and `risk.equity` -- the broker account snapshot,
# which already counts a core position with no code at all.  So the split was
# never a policy decision; it was an artefact of where each number came from.
#
# Counting core here is settled by sql/311, not by preference: the mandate is a
# PORTFOLIO mandate (stored on `strategy_paper_pool_events`, every limit
# denominated on `pool_base`), so leaving these two alpha-only would make one
# mandate enforce two different populations depending on each limit's source.
# It also errs toward the tighter cap, the correct direction for a risk control.
#
# The core arm is admitted by PRESENCE, deliberately.  These are caps: counting a
# position makes a refusal MORE likely, so presence is the fail-closed choice and
# the authorised witness chain would be the fail-open one.
_MANDATE_OBSERVATION_SQL = """
            SELECT
                (
                    SELECT count(*)
                    FROM strategy_trades trade
                    LEFT JOIN strategy_funding_decisions decision
                      ON decision.funding_decision_id=trade.funding_decision_id
                     AND decision.verdict='allocated'
                    LEFT JOIN strategy_deployments deployment
                      ON deployment.deployment_id=decision.deployment_id
                     AND deployment.mode='paper'
                    WHERE trade.status NOT IN ('closed','failed')
                      AND (
                        deployment.deployment_id IS NOT NULL
                        OR trade.core_rebalance_intent_id IS NOT NULL
                      )
                ),
                (
                    SELECT COALESCE(SUM(event.realized_pnl_usd),0)
                    FROM strategy_position_ownership ownership
                    JOIN strategy_trades trade
                      ON trade.strategy_trade_id=ownership.strategy_trade_id
                    LEFT JOIN strategy_funding_decisions decision
                      ON decision.funding_decision_id=trade.funding_decision_id
                    LEFT JOIN strategy_deployments deployment
                      ON deployment.deployment_id=decision.deployment_id
                     AND deployment.mode='paper'
                    JOIN trade_events event
                      ON event.position_id=ownership.broker_position_id
                     AND event.event_kind='close'
                    WHERE event.executed_at >= %s AND event.executed_at < %s
                      AND (
                        deployment.deployment_id IS NOT NULL
                        OR trade.core_rebalance_intent_id IS NOT NULL
                      )
                )
"""


def _observe_local_mandate_risk(
    conn: psycopg.Connection[Any],
    *,
    intent: _Intent,
    risk: BrokerAccountRiskSnapshot,
    now: datetime,
) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal] | str:
    """Read local allocation risk and advance the high-water mark atomically."""
    with conn.transaction():
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
        if pending_row is None:  # pragma: no cover - aggregate SELECT always returns one row
            raise StrategyPaperExecutionError("pending strategy risk observation was unavailable")
        pending_total = Decimal(str(pending_row[0]))
        pending_instrument = Decimal(str(pending_row[1]))
        capital_bases = _effective_capital_bases(conn, intent)
        if isinstance(capital_bases, str):
            return capital_bases
        deployment_base, pool_base = capital_bases
        market_date = now.astimezone(_NY).date()
        market_day_start = datetime.combine(market_date, time.min, tzinfo=_NY).astimezone(UTC)
        market_day_end = (datetime.combine(market_date, time.min, tzinfo=_NY) + timedelta(days=1)).astimezone(UTC)
        mandate_row = conn.execute(
            _MANDATE_OBSERVATION_SQL,
            (market_day_start, market_day_end),
        ).fetchone()
        if mandate_row is None:  # pragma: no cover - scalar SELECT always returns one row
            raise StrategyPaperExecutionError("portfolio mandate observation was unavailable")
        open_strategy_lifecycles = int(mandate_row[0])
        daily_realised_pnl = Decimal(str(mandate_row[1]))
        if open_strategy_lifecycles >= intent.mandate_max_concurrent_positions:
            return "portfolio_concurrency_limit"
        daily_loss_limit = pool_base * intent.mandate_max_daily_loss_pct / Decimal("100")
        if daily_realised_pnl <= -daily_loss_limit:
            return "portfolio_daily_loss_limit"
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
    return deployment_base, pool_base, pending_total, pending_instrument, drawdown


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
    # account snapshot. The local observation counts every unresolved order and
    # advances account high-water state in one explicit transaction.
    observed = _observe_local_mandate_risk(conn, intent=intent, risk=risk, now=now)
    if isinstance(observed, str):
        return observed
    deployment_base, pool_base, pending_total, pending_instrument, drawdown = observed
    if drawdown >= intent.max_drawdown_pct:
        return "account_drawdown_limit"
    if drawdown >= intent.mandate_max_drawdown_pct:
        return "portfolio_drawdown_limit"
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
    cash_reserve_capacity = max(
        Decimal("0"),
        pool_base * (Decimal("100") - intent.mandate_cash_reserve_pct) / Decimal("100") - intent.pool_reserved,
    )
    if cash_reserve_capacity.quantize(_CENT, rounding=ROUND_DOWN) <= 0:
        return "portfolio_cash_reserve_limit"
    active_risk_capacity = max(
        Decimal("0"),
        pool_base * intent.mandate_active_risk_budget_pct / Decimal("100") - intent.pool_reserved,
    )
    if active_risk_capacity.quantize(_CENT, rounding=ROUND_DOWN) <= 0:
        return "portfolio_active_risk_limit"
    if intent.stop_loss_pct <= 0:
        return "execution_policy_invalid"
    # Both inputs are percentage points, so their /100 factors cancel when
    # solving amount * stop_pct/100 <= pool_base * loss_limit_pct/100.
    loss_at_stop_capacity = pool_base * intent.mandate_max_loss_per_position_pct / intent.stop_loss_pct
    if loss_at_stop_capacity.quantize(_CENT, rounding=ROUND_DOWN) <= 0:
        return "portfolio_position_loss_limit"
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
        active_risk_capacity,
        cash_reserve_capacity,
        loss_at_stop_capacity,
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
    ranking_member_id: int | None,
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
    signal_row = conn.execute("SELECT strategy_id FROM strategy_signals WHERE signal_id=%s", (signal_id,)).fetchone()
    conn.commit()
    purpose = None if signal_row is None else registered_strategy_purpose(str(signal_row[0]))
    if signal_row is not None and purpose != "capital_candidate":
        if existing is not None:
            if existing.order_id is None:
                raise StrategyPaperExecutionError(
                    "uncertain non-capital-candidate submission is missing durable order authority"
                )
            reconcile_strategy_order(conn, broker=broker, order_id=existing.order_id)
            refreshed = _existing_result(conn, signal_id)
            conn.commit()
            return refreshed or existing
        reason_code = "harness_validation_only" if purpose == "harness_validation" else "strategy_not_capital_candidate"
        return _persist_rejection(conn, signal_id=signal_id, reason_code=reason_code, now=evaluated_at)
    try:
        runtime = get_runtime_config(conn)
        kill_row = conn.execute("SELECT is_active FROM kill_switch WHERE id = true").fetchone()
        conn.commit()
    except RuntimeConfigCorrupt:
        return _persist_rejection(conn, signal_id=signal_id, reason_code="runtime_config_corrupt", now=evaluated_at)
    if runtime.enable_live_trading:
        return _persist_rejection(conn, signal_id=signal_id, reason_code="live_trading_enabled", now=evaluated_at)
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

    intent, reason, halt_identity_evaluated = _load_intent(
        conn,
        signal_id=signal_id,
        ranking_member_id=ranking_member_id,
        now=evaluated_at,
    )
    conn.commit()
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
    stressed_cost, net_expectancy = assessed.stressed, assessed.net
    if net_expectancy <= 0:
        return _persist_rejection(
            conn,
            signal_id=signal_id,
            reason_code="net_expectancy_not_positive",
            now=evaluated_at,
            intent=intent,
            risk=risk,
            costs_at=costs.last_updated,
            cost_assessment=assessed,
        )
    if net_expectancy < intent.min_net_expectancy_pct:
        return _persist_rejection(
            conn,
            signal_id=signal_id,
            reason_code="net_expectancy_below_policy",
            now=evaluated_at,
            intent=intent,
            risk=risk,
            costs_at=costs.last_updated,
            cost_assessment=assessed,
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
                signal_id, deployment_id, policy_revision, forecast_id, ranking_member_id,
                verdict, reason_code,
                evaluated_at, quote_at, scan_at, halt_feed_at, halt_identity_rule_version,
                eligibility_checked_at, costs_at, broker_available_cash,
                account_equity, account_invested, instrument_invested,
                account_drawdown_pct, allocated_amount,
                gross_expectancy_ci_low_pct, stressed_cost_amount, cost_basis,
                net_expectancy_pct, stop_loss_rate, take_profit_rate
            ) VALUES (
                %s, %s, %s, %s, %s, 'allocated', 'all_paper_entry_gates_passed',
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                signal_id,
                intent.deployment_id,
                intent.policy_revision,
                intent.forecast_id,
                intent.ranking_member_id,
                evaluated_at,
                intent.quote_at,
                intent.scan_at,
                intent.halt_feed_at,
                HALT_IDENTITY_RULE_VERSION,
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
                # ⚠ From the assessment, never re-derived here: this INSERT cannot
                # see which field each cost component used, so a literal at this line
                # would be a second, unverifiable claim about the same response.
                assessed.basis,
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
    ranking_member_id: int | None = None,
    now: datetime | None = None,
) -> PaperExecutionResult:
    """Serialize and evaluate one fired signal against whole-account demo risk."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise StrategyPaperExecutionError("paper execution requires an idle connection")
    if ranking_member_id is None:
        row = conn.execute(
            """
            SELECT member.ranking_member_id
            FROM strategy_opportunity_ranking_members member
            JOIN strategy_opportunity_forecasts forecast
              ON forecast.forecast_id=member.forecast_id
            WHERE forecast.signal_id=%s AND member.selected
            ORDER BY member.ranking_batch_id DESC
            LIMIT 1
            """,
            (signal_id,),
        ).fetchone()
        conn.commit()
        ranking_member_id = int(row[0]) if row is not None else None
    with _allocator_lock(conn):
        return _execute_fired_paper_signal_locked(
            conn,
            broker=broker,
            signal_id=signal_id,
            ranking_member_id=ranking_member_id,
            now=now,
        )


__all__ = ["PaperExecutionResult", "StrategyPaperExecutionError", "execute_fired_paper_signal"]
