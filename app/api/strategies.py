"""Strategy evidence, exact-owned P&L, attribution and allocation (#2447/#2453)."""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Literal, cast

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from psycopg.pq import TransactionStatus
from pydantic import BaseModel, Field, model_validator

from app.api.auth import require_session, require_session_or_service_token
from app.api.portfolio import get_portfolio
from app.config import settings
from app.db import get_conn
from app.providers.implementations.etoro_broker import EtoroBrokerProvider
from app.security.master_key import MasterKeyError, ensure_broker_key_loaded
from app.security.secrets_crypto import CredentialCryptoConfigError
from app.security.sessions import SessionRow
from app.services.account_equity_evidence import load_account_equity_evidence
from app.services.backtest_run import BACKTEST_UNIVERSE, corpus_version_for, runnable_strategies
from app.services.broker_credentials import (
    CredentialDecryptError,
    CredentialNotFound,
    CredentialValidationError,
    load_credential_for_provider_use,
    normalise_environment,
    normalise_provider,
)
from app.services.cost_model import COST_MODEL_ID
from app.services.equity_curve import BENCHMARK_RULE_ID, SIZING_RULE_ID
from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.position_builder import RULE_SET_VERSION as POSITION_RULE_SET_VERSION
from app.services.research_price_structure_store import QUARANTINE_RULE_SET_VERSION
from app.services.runtime_config import (
    RuntimeConfigCorrupt,
    RuntimeConfigNoOp,
    get_runtime_config,
    update_runtime_config,
)
from app.services.strategy_ambiguity_policy import AMBIGUITY_RULE_VERSION
from app.services.strategy_base_currency import (
    DEPLOYMENT_CURRENCY_UNSUPPORTED,
    SUPPORTED_DEPLOYMENT_CURRENCIES,
)
from app.services.strategy_control_plane import (
    PAPER_ALLOCATOR_ADVISORY_LOCK,
    StrategyControlError,
    StrategyOwnershipError,
    configure_deployment,
    configure_execution_policy,
    configure_paper_pool,
    current_stage,
    is_risk_reducing_deployment_change,
    load_paper_pool,
    lock_strategy_control,
    mandate_for_profile,
    promote_strategy,
)
from app.services.strategy_core_eligibility import CoreEligibilityError
from app.services.strategy_core_mandate import (
    CORE_MANDATE_SERIES_ID,
    CORE_MANDATE_SERIES_TITLE,
    CoreMandate,
    CoreMandateError,
    configure_core_mandate,
    load_core_mandate,
)
from app.services.strategy_live_gate import (
    REQUIRED_KILL_DRILLS,
    LiveGateReport,
    assess_live_gate,
    record_live_promotion_attempt,
    register_live_gate_policy,
    run_kill_drill,
)
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_monitoring import (
    ShareUnavailableReason,
    StrategyAttribution,
    StrategyControlState,
    StrategyFireRate,
    StrategyPnl,
    WeeklyRateUnavailableReason,
    load_attribution,
    load_control_state,
    load_entry_block_state,
    load_fire_rate,
    load_owned_pnl,
    realised_pnl_for_keys,
)
from app.services.strategy_position_manager import (
    StrategyPositionManagerError,
    manage_owned_position,
)
from app.services.strategy_recent_evidence import RECENT_EVIDENCE_WINDOWS
from app.services.strategy_result import TOTAL_RETURN_BASIS
from app.services.strategy_result_ambiguity import (
    AmbiguityRecord,
    ComparisonBasis,
    ambiguity_promotion_refusals,
    composed_holdout_ambiguity_refusals,
    record_sha256,
)
from app.services.strategy_wealth import load_strategy_wealth_history
from app.services.sync_orchestrator.dispatcher import publish_manual_job_request_with_conn
from app.services.trial_register import TRIAL_REGISTER, TRIAL_REGISTER_VERSION

router = APIRouter(
    prefix="/strategies",
    tags=["strategies"],
    dependencies=[Depends(require_session_or_service_token)],
)

logger = logging.getLogger(__name__)

_STRATEGY_BACKTEST_JOB = "strategy_backtest_run"

_TITLES = {
    "s1-time-series-momentum": "Time-series momentum",
    "s2-cross-sectional-momentum": "Cross-sectional momentum",
    "s3-mean-reversion-in-trend": "Mean reversion in trend",
    "s4-volatility-compression-breakout": "Volatility compression breakout",
    "s5-support-bounce": "Support bounce",
    "s6-resistance-breakout": "Resistance breakout",
    "s7-trend-pullback": "Trend pullback",
    "s8-range-mean-reversion": "Range mean reversion",
    "s9-squeeze-expansion": "Squeeze expansion",
    "s10-relative-strength-leader": "Relative-strength leader",
}

_PRESENTATION = {
    "s1-time-series-momentum": (
        "Follows established price trends and exits when the trend turns.",
        "Until the trend turns",
    ),
    "s2-cross-sectional-momentum": (
        "Selects the strongest shares and refreshes them on the monthly rebalance.",
        "Next monthly rebalance",
    ),
    "s3-mean-reversion-in-trend": (
        "Buys short pullbacks that occur inside longer-term uptrends.",
        "Up to 10 market days",
    ),
    "s4-volatility-compression-breakout": (
        "Looks for price breakouts after volatility has contracted.",
        "Up to 40 market days",
    ),
    "s5-support-bounce": (
        "Buys a rejection back above established support in bullish regimes.",
        "Up to 30 market days",
    ),
    "s6-resistance-breakout": (
        "Buys a volume-confirmed close through established resistance in quiet bull markets.",
        "Up to 40 market days",
    ),
    "s7-trend-pullback": (
        "Buys RSI recoveries inside established uptrends and exits when the trend weakens.",
        "Up to 60 market days",
    ),
    "s8-range-mean-reversion": (
        "Buys rebounds from below the lower Bollinger band when ADX indicates a range.",
        "Up to 15 market days",
    ),
    "s9-squeeze-expansion": (
        "Buys 20-day price breakouts after Bollinger BandWidth reaches a six-month squeeze.",
        "Up to 40 market days",
    ),
    "s10-relative-strength-leader": (
        "Holds top-decile 63-day leaders above their 50-day average in quiet bull markets.",
        "Reviewed at each monthly rebalance",
    ),
}


class ScanRotation(BaseModel):
    """The scan that belongs to the version this one replaced (#2624 scope 2).

    Present exactly when ``ScanHealth.status == "rotated"``. Both dates come from
    the same ``strategy_scan_watermark`` row, so neither can be null in practice;
    they stay optional only because the column types are.
    """

    previous_version: str
    previous_frontier_date: date | None
    previous_scanned_at: datetime | None


class ScanHealth(BaseModel):
    frontier_date: date | None
    updated_at: datetime | None
    # ``rotated`` (#2624 scope 2): the CURRENT version has never scanned but a
    # previous one did. Previously indistinguishable from ``never_run``, which
    # is the operator-facing lie — after any registry-touching merge the page
    # said a strategy with a full history had never run.
    status: Literal["never_run", "rotated", "current", "stale"]
    rotation: ScanRotation | None = None
    fired_entries: int = 0
    fired_exits: int = 0
    not_fired: int = 0
    not_evaluable: int = 0
    exclusions_by_reason: dict[str, int] = Field(default_factory=dict)


class PriorVersionTrackRecord(BaseModel):
    """A version this strategy used to run under, and why its numbers are absent.

    ⚠ Deliberately carries NO ``ResultArm``, and that is a measurement, not a
    simplification (#2624 scope 1, spec
    ``docs/proposals/ta/2026-08-13-prior-version-track-records.md``):

    * **A prior version need not be on the current measurement basis, and today
      none is.** Grouping every stored row by its identity pins, the ones that
      match today's constants are exactly the four CURRENT versions; each version
      they replaced differs on at least ``cost_model_id`` and ``return_basis``.
      Those pins ARE the result identity, so putting an old expectancy beside a
      new one is a cross-basis splice. ⚠ This is a property of the data, not of
      rotation: immediately after a registry-only merge the version just replaced
      IS comparable, which is why ``comparable`` is computed per version rather
      than assumed.
    * **``promotion_refusals`` cannot be reconstructed.** ``_promotion_refusals``
      computes it at read time from TODAY's gate, and what was true when the row
      was written is not stored — so reusing ``ResultArm`` would re-judge history
      under rules it never faced.

    So this answers "where did my track record go?" and names the refusal, in the
    posture #2602 item 5 sets for benchmark fields: never substitute.
    """

    strategy_version: str
    result_count: int
    last_scan_frontier_date: date | None
    last_scan_at: datetime | None
    comparable: bool
    incomparable_reasons: list[str]


class ResultArm(BaseModel):
    result_version: str
    purpose: Literal["harness_validation", "capital_candidate"]
    ambiguity_arm: str
    quarantine_arm: str
    universe_basis: str
    corpus_version: str
    cost_model_id: str
    sizing_rule: str
    benchmark_rule: str
    return_basis: str
    ambiguity_rule_version: str
    position_rule_set_version: str
    outcome_rule_set_version: str
    input_rule_set_version: str
    evaluated_instrument_count: int
    trade_count: int
    losing_trade_count: int
    open_trade_count: int
    unpriced_trade_count: int
    expectancy_per_trade_pct: Decimal
    expectancy_ci_low_pct: Decimal | None
    expectancy_ci_high_pct: Decimal | None
    total_return_pct: Decimal
    cagr_pct: Decimal
    sharpe: Decimal
    sortino: Decimal | None
    max_drawdown_pct: Decimal
    profit_factor: Decimal | None
    exposure_time_pct: Decimal
    turnover_annualised: Decimal
    return_vs_buy_and_hold_pct: Decimal
    deflated_sharpe: Decimal | None
    # #2623 gap 1. ⚠ The three hold figures are null for every one of the 324
    # stored rows and stay that way: they populate FORWARD ONLY, because
    # backfilling means re-running the backtests, which charges the trial
    # register (#2599/#2616). `metric_set_id` is what makes that null readable —
    # `criterion7-v1` is a row written before the measurement existed, whereas a
    # null under `criterion7-v2` is a writer defect (`sql/347` CHECKs it). The
    # reader must render those two differently, so the id ships with them.
    #
    # ⚠ `median_hold_days` must never be displayed alone. It is right-censored
    # and the direction of the bias is NOT determinable a priori — a position
    # opened just before the window end is still open and short — so
    # `open_trade_count` and `unpriced_trade_count` belong beside it. They are
    # separate exclusions and neither implies the other.
    metric_set_id: str
    median_hold_days: Decimal | None
    hold_days_p25: Decimal | None
    hold_days_p75: Decimal | None
    promotion_refusals: list[str]


class EvidenceWindow(BaseModel):
    window_id: str
    label: str
    window_start: date
    window_end: date
    status: Literal["missing", "partial", "complete"]
    arms: list[ResultArm]


class StrategyOverview(BaseModel):
    strategy_id: str
    strategy_version: str
    purpose: Literal["harness_validation", "capital_candidate"]
    title: str
    description: str
    exit_timing: str
    runnable: bool
    forward_outcome_supported: bool
    exclusion_reason: str | None
    scan: ScanHealth
    evidence_windows: list[EvidenceWindow]
    prior_versions: list[PriorVersionTrackRecord]
    # ⚠ NOT a prior-version summary, despite the name: `_RESULT_COUNTS_SQL`
    # filters on the CURRENT versions, so this counts rows under the current
    # version that match no declared window. Reads 0 for all four strategies.
    legacy_result_count: int
    all_recent_evidence_complete: bool
    stage: str | None
    attribution: StrategyAttributionView
    fire_rate: StrategyFireRateView
    pnl: StrategyPnlView
    allocation: StrategyAllocationView
    allocation_ready: bool
    allocation_refusals: list[str]


class StrategyAttributionView(BaseModel):
    fired_entries: int
    funded_entries: int
    rejected_entries: int
    resolved_entries: int
    winning_entries: int
    win_rate: Decimal | None
    median_days_to_outcome: Decimal | None
    signals_last_30_days: int
    shadow_average_return_pct: Decimal | None
    funded_shadow_average_return_pct: Decimal | None
    rejected_shadow_average_return_pct: Decimal | None
    opportunity_gap_pct: Decimal | None
    funded_capture_rate: Decimal | None
    filled_entries: int
    broker_rejected_entries: int
    fill_rate: Decimal | None
    broker_rejection_rate: Decimal | None
    average_slippage_pct: Decimal | None
    average_stressed_cost_usd: Decimal | None
    max_observed_account_drawdown_pct: Decimal | None


class StrategyFireRateView(BaseModel):
    """Entry fire evidence for the current version, from the durable census.

    Source and construction: ``app/services/strategy_monitoring.py::derive_fire_rate``
    and ``docs/proposals/ta/2026-08-14-strategy-fire-rate.md``. #2623 gap 2.

    ⚠ ``fired_share_of_evaluable`` is the comparable number — dimensionless, so
    universe size does not move it. ``entries_per_calendar_week`` is throughput and
    is universe-size-dependent by design; it is ``None`` until the version has a
    bar-date axis that can carry a rate.

    ⚠ Each nullable value carries its OWN reason, because the two nulls are
    independent: a version scanned over several days on which every bar was
    ``not_evaluable`` rates at ``0.00``/week and has no share at all.
    """

    universe: str
    scanned_days: int
    fired_days: int
    fired_entry_signals: int
    evaluable_entry_decisions: int
    not_evaluable_entry_decisions: int
    fired_share_of_evaluable: Decimal | None
    entries_per_calendar_week: Decimal | None
    first_scanned_bar: date | None
    last_scanned_bar: date | None
    share_unavailable_reason: ShareUnavailableReason | None
    weekly_rate_unavailable_reason: WeeklyRateUnavailableReason | None


class StrategyPnlView(BaseModel):
    currency: Literal["USD"] = "USD"
    strategy_trade_count: int
    owned_position_count: int
    active_position_count: int
    close_event_count: int
    invested_capital: Decimal | None
    realised_pnl: Decimal | None
    unrealised_pnl: Decimal | None
    total_pnl: Decimal | None
    observed_fees: Decimal | None
    complete: bool
    incomplete_reasons: list[str]


class StrategyAllocationView(BaseModel):
    deployment_id: int | None
    capital_limit: Decimal
    currency: str
    enabled: bool
    revision: int | None
    reserved_capital: Decimal
    invested_capital: Decimal | None
    remaining_capital: Decimal
    policy_configured: bool
    max_drawdown_limit_pct: Decimal | None
    ticket_sizing_mode: Literal["percent", "fixed"] | None
    ticket_value: Decimal | None
    max_ticket_amount: Decimal | None


class StrategyEntryBlockView(BaseModel):
    new_entries_blocked: bool
    global_kill_active: bool
    global_kill_reason: str | None
    global_kill_activated_at: datetime | None
    global_kill_activated_by: str | None
    execution_block_reasons: list[str]


class StrategyPortfolioMandateView(BaseModel):
    configured: bool
    policy_version: str
    risk_profile: Literal["unconfigured", "cautious", "balanced", "growth"]
    target_volatility_pct: Decimal | None
    max_portfolio_drawdown_pct: Decimal | None
    max_loss_per_position_pct: Decimal | None
    max_daily_loss_pct: Decimal | None
    active_risk_budget_pct: Decimal | None
    cash_reserve_pct: Decimal | None
    max_concurrent_positions: int | None
    shorts_allowed: bool
    leverage_allowed: bool


class StrategyPaperPoolView(BaseModel):
    configured: bool
    enabled: bool
    capital_limit: Decimal
    capital_mode: Literal["fixed", "compound"]
    effective_capital: Decimal | None
    currency: Literal["USD"] = "USD"
    reserved_capital: Decimal
    invested_capital: Decimal | None
    remaining_capital: Decimal | None
    mandate: StrategyPortfolioMandateView
    available_mandates: list[StrategyPortfolioMandateView]


def _mandate_view(
    risk_profile: Literal["unconfigured", "cautious", "balanced", "growth"],
) -> StrategyPortfolioMandateView:
    mandate = mandate_for_profile(risk_profile)
    return StrategyPortfolioMandateView(
        configured=mandate.configured,
        policy_version=mandate.policy_version,
        risk_profile=mandate.risk_profile,
        target_volatility_pct=mandate.target_volatility_pct,
        max_portfolio_drawdown_pct=mandate.max_portfolio_drawdown_pct,
        max_loss_per_position_pct=mandate.max_loss_per_position_pct,
        max_daily_loss_pct=mandate.max_daily_loss_pct,
        active_risk_budget_pct=mandate.active_risk_budget_pct,
        cash_reserve_pct=mandate.cash_reserve_pct,
        max_concurrent_positions=mandate.max_concurrent_positions,
        shorts_allowed=mandate.shorts_allowed,
        leverage_allowed=mandate.leverage_allowed,
    )


class EvidenceRefreshView(BaseModel):
    frozen_through: date
    completed_windows: int
    partial_windows: int
    total_windows: int
    status: Literal["idle", "queued", "running", "failed", "complete"]
    request_id: int | None
    requested_at: datetime | None
    finished_at: datetime | None
    last_error: str | None
    progress: dict[str, object] | None


AutomationReadinessState = Literal[
    "no_capital_candidates",
    "historical_validation_incomplete",
    "assessment_policy_missing",
    "prospective_evidence_missing",
    "prospective_evidence_failed",
    "prospective_evidence_stale",
    "candidate_evidence_incomplete",
    "ready",
]


class AutomationReadinessView(BaseModel):
    ready: bool
    state: AutomationReadinessState
    capital_candidate_count: int
    historically_ready_candidate_count: int
    prospectively_ready_candidate_count: int
    assessment_policy_id: str | None
    assessed_scope_count: int
    passed_scope_count: int
    fresh_passed_scope_count: int
    resolved_forecasts: int
    target_first_count: int
    stop_first_count: int
    timeout_count: int
    latest_checked_at: datetime | None
    worst_normalized_brier_score: Decimal | None
    weakest_brier_skill_score: Decimal | None
    worst_classwise_calibration_error: Decimal | None
    blockers: list[str]


class AccountEquityEvidenceView(BaseModel):
    status: Literal["unavailable", "collecting"]
    days_collected: int
    snapshot_date: date | None
    observed_at: datetime | None
    account_currency_id: int | None
    currency: str | None
    official_equity: Decimal | None
    official_available_cash: Decimal | None
    official_total_invested: Decimal | None
    official_unrealised_pnl: Decimal | None
    local_eod_currency: str | None
    local_eod_value: Decimal | None
    local_eod_positions_priced: int | None
    local_eod_stale_mark_positions: int | None
    difference: Decimal | None
    comparable: Literal[False]
    incomplete_reasons: list[str]


class StrategyOverviewResponse(BaseModel):
    as_of: datetime
    demo_connection: bool
    execution_enabled: bool
    live_execution_enabled: bool
    live_strategy_activation_available: Literal[False] = False
    live_strategy_activation_blocker: Literal["live_strategy_broker_contract_not_validated"] = (
        "live_strategy_broker_contract_not_validated"
    )
    storage_policy: Literal["fired_signals_and_material_mutations_only"] = "fired_signals_and_material_mutations_only"
    entry_block: StrategyEntryBlockView
    paper_pool: StrategyPaperPoolView
    automation_readiness: AutomationReadinessView
    account_equity_evidence: AccountEquityEvidenceView
    evidence_refresh: EvidenceRefreshView
    strategies: list[StrategyOverview]


class EvidenceRefreshRequestResponse(BaseModel):
    request_id: int
    status: Literal["queued", "running"]
    already_active: bool


class StrategyPaperPoolUpdateRequest(BaseModel):
    enabled: bool
    capital_limit: Decimal = Field(ge=0, max_digits=18, decimal_places=6)
    capital_mode: Literal["fixed", "compound"] = "fixed"
    risk_profile: Literal["unconfigured", "cautious", "balanced", "growth"]
    reason: str = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def enabled_requires_capital(self) -> StrategyPaperPoolUpdateRequest:
        if self.enabled and self.capital_limit <= 0:
            raise ValueError("enabled paper pool requires positive capital")
        if self.enabled and self.risk_profile == "unconfigured":
            raise ValueError("enabled paper pool requires a configured portfolio risk mandate")
        return self


class CoreMandateUpdateRequest(BaseModel):
    """One core/cash mandate revision, plus the ACCOUNT to prove it against.

    ⚠ The mandate itself is account-agnostic — one series of revisions, no
    account column — but its eligibility proof is not: ``require_core_eligibility``
    resolves the live credential pair for ``(operator, provider, environment)``
    and refuses a proof observed under any other. So the account has to be named
    on the request. Same shape as ``app/api/broker_credentials.py``, including
    the ``"demo"`` default, rather than a second convention.

    ⚠⚠ NO MANDATE INVARIANT IS RESTATED HERE. The percentage bounds, the
    complement rule and the enabled-needs-an-instrument rule all live in
    ``validate_core_mandate``, which is also what the tests and any future caller
    exercise. A Pydantic copy would be a second place for them to drift, and the
    #2623 lesson is that the copy nobody edits is the one that goes wrong. These
    fields carry SHAPE only.
    """

    enabled: bool
    core_instrument_id: int | None = None
    core_target_pct: Decimal = Field(max_digits=9, decimal_places=6)
    liquidity_reserve_pct: Decimal = Field(max_digits=9, decimal_places=6)
    rebalance_band_pct: Decimal = Field(max_digits=9, decimal_places=6)
    min_rebalance_amount: Decimal = Field(max_digits=18, decimal_places=6)
    reason: str = Field(min_length=1, max_length=1000)
    provider: str = Field(default="etoro", min_length=1, max_length=64)
    environment: str = Field(default="demo", min_length=1, max_length=16)


class CoreMandateResponse(BaseModel):
    """The stored mandate, or its absence.

    ⚠ ``cash_target_pct`` is the service's derived complement, never a stored
    second column — rendering it beside the target is what stops a reader
    inventing their own subtraction against a different basis.
    """

    configured: bool
    event_id: int | None = None
    revision: int | None = None
    enabled: bool | None = None
    base_currency: str | None = None
    core_instrument_id: int | None = None
    core_target_pct: Decimal | None = None
    cash_target_pct: Decimal | None = None
    liquidity_reserve_pct: Decimal | None = None
    rebalance_band_pct: Decimal | None = None
    min_rebalance_amount: Decimal | None = None
    policy_version: str | None = None


def _core_mandate_response(mandate: CoreMandate | None) -> CoreMandateResponse:
    if mandate is None:
        return CoreMandateResponse(configured=False)
    return CoreMandateResponse(
        configured=True,
        event_id=mandate.event_id,
        revision=mandate.revision,
        enabled=mandate.enabled,
        base_currency=mandate.base_currency,
        core_instrument_id=mandate.core_instrument_id,
        core_target_pct=mandate.core_target_pct,
        cash_target_pct=mandate.cash_target_pct,
        liquidity_reserve_pct=mandate.liquidity_reserve_pct,
        rebalance_band_pct=mandate.rebalance_band_pct,
        min_rebalance_amount=mandate.min_rebalance_amount,
        policy_version=mandate.policy_version,
    )


class StrategyPnlHistoryPoint(BaseModel):
    date: date
    total_pnl: Decimal
    strategy_pnl: dict[str, Decimal]


class StrategyPnlHistoryResponse(BaseModel):
    basis: Literal["exact_owned_realised_pnl_only"] = "exact_owned_realised_pnl_only"
    total_return_available: Literal[False] = False
    benchmark_comparison_available: Literal[False] = False
    points: list[StrategyPnlHistoryPoint]


class StrategyWealthHistoryPoint(BaseModel):
    date: date
    principal: Decimal
    external_flow: Decimal
    realised_pnl: Decimal | None
    unrealised_pnl: Decimal | None
    total_pnl: Decimal | None
    pot_value: Decimal | None
    complete: bool
    incomplete_reasons: list[str]


class StrategyWealthHistoryResponse(BaseModel):
    basis: Literal["exact_owned_mark_to_market_nav"] = "exact_owned_mark_to_market_nav"
    total_return_available: Literal[False] = False
    benchmark_comparison_available: Literal[False] = False
    points: list[StrategyWealthHistoryPoint]


class StrategyOwnedPosition(BaseModel):
    strategy_trade_id: int
    broker_position_id: int
    # NULL on the core/cash arm (#2603, sql/349): a mandate holding is authorised
    # by a rebalance intent, not by a signal, so it has no strategy identity to
    # report.  `strategy_title` stays non-null and carries a mandate label
    # instead, because the column is what the operator reads to tell the two
    # populations apart -- a blank there would render as an unexplained gap.
    strategy_id: str | None
    strategy_version: str | None
    strategy_title: str
    instrument_id: int
    symbol: str
    company_name: str | None
    direction: Literal["long", "short"] | None
    units: Decimal | None
    assigned_value: Decimal | None
    current_value: Decimal | None
    unrealised_pnl: Decimal | None
    unrealised_return_pct: Decimal | None
    open_rate: Decimal | None
    current_price: Decimal | None
    stop_loss_rate: Decimal | None
    take_profit_rate: Decimal | None
    opened_at: datetime | None
    currency: str
    trade_status: Literal["open", "closing", "reconcile_required"]
    valuation_available: bool


class StrategyOwnedPositionsResponse(BaseModel):
    positions: list[StrategyOwnedPosition]
    live_quote_instrument_ids: list[int]


class StrategyPositionCloseResponse(BaseModel):
    strategy_trade_id: int
    broker_position_id: int
    state: Literal["submitted", "pending", "applied"]
    reason_code: str
    operation_id: int | None


StrategyTradeStatus = Literal["planned", "submitted", "open", "closing", "closed", "failed", "reconcile_required"]
StrategyOperationStatus = Literal["intent_persisted", "submitted", "applied", "rejected", "reconcile_required"]
StrategyReconciliationState = Literal[
    "unresolved", "pending", "resolved", "rejected", "not_found", "ambiguous", "error"
]
StrategyCloseHistoryStatus = Literal["not_applicable", "not_closed", "complete", "incomplete", "unavailable"]


class StrategyTradeLifecycle(BaseModel):
    trade_status: StrategyTradeStatus | None
    ownership_count: int
    broker_position_id: int | None
    ownership_status: Literal["active", "released"] | None
    position_claimed_at: datetime | None
    position_released_at: datetime | None
    position_release_reason: str | None
    latest_operation_type: Literal["fixed_exit_repair", "stop_ratchet", "close"] | None
    latest_operation_id: int | None
    latest_operation_order_id: int | None
    latest_operation_trigger: str | None
    latest_operation_status: StrategyOperationStatus | None
    latest_operation_created_at: datetime | None
    latest_operation_submitted_at: datetime | None
    latest_operation_resolved_at: datetime | None
    latest_operation_error: str | None
    latest_reconciliation_state: StrategyReconciliationState | None
    latest_reconciliation_broker_status: str | None
    latest_reconciliation_attempt_count: int | None
    latest_reconciliation_updated_at: datetime | None
    latest_reconciliation_error: str | None
    close_event_count: int | None
    realised_pnl_usd: Decimal | None
    observed_fees_usd: Decimal | None
    close_history_status: StrategyCloseHistoryStatus
    incomplete_reasons: list[str]


class FiredSignal(BaseModel):
    signal_id: int
    strategy_id: str
    strategy_version: str
    instrument_id: int
    symbol: str
    company_name: str | None
    signal_bar_date: date
    signal_kind: str
    fill_bar_date: date
    fill_price: Decimal
    universe: str
    outcome: str | None
    exit_bar_date: date | None
    exit_price: Decimal | None
    gross_return_pct: Decimal | None
    outcome_reason: str | None
    funding_status: Literal["funded", "rejected", "not_applicable"]
    funding_reason: str
    funded_amount: Decimal | None
    strategy_trade_id: int | None
    execution_status: str | None
    actual_fill_price: Decimal | None
    slippage_pct: Decimal | None
    trade_lifecycle: StrategyTradeLifecycle | None


class FiredSignalsResponse(BaseModel):
    items: list[FiredSignal]
    next_cursor: int | None


class AllocationUpdateRequest(BaseModel):
    strategy_version: str = Field(min_length=1, max_length=200)
    capital_limit: Decimal = Field(ge=0, max_digits=18, decimal_places=6)
    enabled: bool
    reason: str = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def enabled_requires_capital(self) -> AllocationUpdateRequest:
        if self.enabled and self.capital_limit <= 0:
            raise ValueError("enabled allocation requires positive capital")
        return self


class AllocationUpdateResponse(BaseModel):
    strategy_id: str
    strategy_version: str
    deployment_id: int
    capital_limit: Decimal
    currency: str
    enabled: bool
    revision: int


class StrategySizingUpdateRequest(BaseModel):
    strategy_version: str = Field(min_length=1, max_length=200)
    ticket_sizing_mode: Literal["percent", "fixed"]
    ticket_value: Decimal = Field(gt=0, max_digits=18, decimal_places=6)
    max_ticket_amount: Decimal = Field(gt=0, max_digits=18, decimal_places=6)
    reason: str = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def valid_shape(self) -> StrategySizingUpdateRequest:
        if self.ticket_sizing_mode == "percent" and self.ticket_value > 100:
            raise ValueError("percent ticket value must be in (0, 100]")
        if self.ticket_sizing_mode == "fixed" and self.max_ticket_amount < self.ticket_value:
            raise ValueError("fixed ticket value cannot exceed its hard maximum")
        return self


class StrategySizingUpdateResponse(BaseModel):
    strategy_id: str
    strategy_version: str
    deployment_id: int
    revision: int
    ticket_sizing_mode: Literal["percent", "fixed"]
    ticket_value: Decimal
    max_ticket_amount: Decimal


class LiveGatePolicyRequest(BaseModel):
    strategy_version: str = Field(min_length=1, max_length=200)
    min_forward_resolved_signals: int = Field(gt=0)
    min_forward_days: int = Field(gt=0)
    min_paper_closed_trades: int = Field(gt=0)
    min_paper_days: int = Field(gt=0)
    max_reconciliation_age_seconds: int = Field(gt=0)
    min_shadow_alpha_pct: Decimal
    max_cost_drift_pct: Decimal = Field(ge=0)
    max_average_slippage_pct: Decimal = Field(ge=0)
    max_drawdown_pct: Decimal = Field(gt=0, lt=100)
    max_scan_age_seconds: int = Field(gt=0)
    max_quote_age_seconds: int = Field(gt=0)
    max_broker_health_age_seconds: int = Field(gt=0)
    max_live_capital: Decimal = Field(gt=0, max_digits=18, decimal_places=6)
    reason: str = Field(min_length=1, max_length=1000)


class LiveGatePolicyView(BaseModel):
    live_gate_policy_id: int
    strategy_id: str
    strategy_version: str
    min_forward_resolved_signals: int
    min_forward_days: int
    min_paper_closed_trades: int
    min_paper_days: int
    max_reconciliation_age_seconds: int
    min_shadow_alpha_pct: Decimal
    max_cost_drift_pct: Decimal
    max_average_slippage_pct: Decimal
    max_drawdown_pct: Decimal
    max_scan_age_seconds: int
    max_quote_age_seconds: int
    max_broker_health_age_seconds: int
    max_live_capital: Decimal
    currency: str
    leverage: int
    registered_at: datetime
    #: #2599 — the frozen preregistration declaration this policy is bound to.
    #: None on a policy registered before #2599, which the gate refuses.
    declaration_id: int | None


class ForwardShadowFloorView(BaseModel):
    """#2599's contract-frozen forward-shadow floor, as the gate read it."""

    min_independent_decision_dates: int
    min_calendar_weeks: int
    derivation: str


class LiveGateFactsView(BaseModel):
    stage: str | None
    forward_resolved_signals: int
    forward_decision_dates: int
    forward_days: int
    paper_closed_trades: int
    paper_days: int
    #: #2612 — the single-entry arrival counts the two windows above are anchored
    #: on. Mirrored here deliberately: `LiveGateFactsView(**facts.__dict__)`
    #: splats every field, so a fact absent from this sibling model is dropped
    #: silently rather than loudly (the #1955 class), and an operator reading
    #: `forward_window_ambiguous` needs the count that produced it.
    forward_observation_entries: int
    paper_enabled_entries: int
    funded_shadow_average_return_pct: Decimal | None
    unfunded_shadow_average_return_pct: Decimal | None
    shadow_alpha_pct: Decimal | None
    average_slippage_pct: Decimal | None
    cost_drift_pct: Decimal | None
    max_observed_drawdown_pct: Decimal | None
    reconciliation_order_count: int
    reconciliation_breach_count: int
    scan_age_seconds: Decimal | None
    active_owned_instrument_count: int
    oldest_owned_quote_age_seconds: Decimal | None
    halt_feed_age_seconds: Decimal | None
    broker_health_age_seconds: Decimal | None
    broker_health_active_block: bool | None
    paper_pnl_complete: bool
    completed_kill_drills: list[str]
    auto_trading_enabled: bool
    live_trading_enabled: bool
    global_kill_active: bool
    active_execution_block_count: int


class LiveGateResponse(BaseModel):
    strategy_id: str
    strategy_version: str
    requested_capital: Decimal
    policy: LiveGatePolicyView | None
    facts: LiveGateFactsView
    required_kill_drills: list[str]
    passed: bool
    refusal_codes: list[str]
    #: None means no floor was frozen for this policy — the gate refuses with
    #: `forward_shadow_floor_missing` rather than treating it as unbounded.
    forward_shadow_floor: ForwardShadowFloorView | None


class LivePromotionAttemptRequest(BaseModel):
    strategy_version: str = Field(min_length=1, max_length=200)
    requested_capital: Decimal = Field(gt=0, max_digits=18, decimal_places=6)
    reason: str = Field(min_length=1, max_length=1000)


class LivePromotionAttemptResponse(BaseModel):
    assessment_id: int
    report: LiveGateResponse


class KillDrillRequest(BaseModel):
    strategy_version: str = Field(min_length=1, max_length=200)
    reason: str = Field(min_length=1, max_length=1000)


class KillDrillResponse(BaseModel):
    kill_drill_event_id: int
    drill_kind: str
    passed: bool


class StrategyLifecycleRequest(BaseModel):
    strategy_version: str = Field(min_length=1, max_length=200)
    action: Literal["pause", "retire"]
    reason: str = Field(min_length=1, max_length=1000)


class StrategyLifecycleResponse(BaseModel):
    strategy_id: str
    strategy_version: str
    stage: Literal["paused", "retired"]
    promotion_id: int


def _live_gate_view(report: LiveGateReport) -> LiveGateResponse:
    return LiveGateResponse(
        strategy_id=report.strategy_id,
        strategy_version=report.strategy_version,
        requested_capital=report.requested_capital,
        policy=(LiveGatePolicyView(**report.policy.__dict__) if report.policy else None),
        facts=LiveGateFactsView(
            **{
                **report.facts.__dict__,
                "completed_kill_drills": list(report.facts.completed_kill_drills),
            }
        ),
        required_kill_drills=list(REQUIRED_KILL_DRILLS),
        passed=report.passed,
        refusal_codes=list(report.refusal_codes),
        forward_shadow_floor=(
            None
            if report.forward_shadow_floor is None
            else ForwardShadowFloorView(**report.forward_shadow_floor.__dict__)
        ),
    )


def _current_versions() -> dict[str, str]:
    return {
        strategy_id: entry.identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID).version
        for strategy_id, entry in STRATEGY_MANIFEST.items()
    }


def _ambiguity_record_from_result_row(
    row: dict[str, object],
    *,
    prefix: str = "",
) -> AmbiguityRecord | None:
    """Rebuild one API query's frozen ambiguity payload.

    The API query aliases the local and exact-support records into the same
    field shape. Keeping one constructor prevents the operator surface from
    interpreting the two records under subtly different null or numeric rules.
    """
    rule = row.get(f"{prefix}ambiguity_record_rule_version")
    if rule is None:
        return None
    record = AmbiguityRecord(
        ambiguity_rule_version=str(rule),
        comparison_basis=cast(ComparisonBasis, row[f"{prefix}ambiguity_comparison_basis"]),
        best_case_sharpe=(
            None
            if row.get(f"{prefix}ambiguity_best_case_sharpe") is None
            else float(cast(Decimal, row[f"{prefix}ambiguity_best_case_sharpe"]))
        ),
        worst_case_sharpe=(
            None
            if row.get(f"{prefix}ambiguity_worst_case_sharpe") is None
            else float(cast(Decimal, row[f"{prefix}ambiguity_worst_case_sharpe"]))
        ),
        cohort_gap_threshold=(
            None
            if row.get(f"{prefix}ambiguity_cohort_gap_threshold") is None
            else float(cast(Decimal, row[f"{prefix}ambiguity_cohort_gap_threshold"]))
        ),
    )
    payload_key = f"{prefix}ambiguity_payload_sha256"
    if payload_key in row and record_sha256(record) != str(row[payload_key]):
        raise RuntimeError(f"strategy overview {prefix or 'local '}ambiguity payload hash mismatch")
    return record


def _promotion_refusals(
    row: dict[str, object],
    *,
    ambiguity_complete: bool,
    quarantine_complete: bool,
    accesses_complete: bool,
) -> list[str]:
    """Reproduce every gate input recoverable from a compact stored result.

    The writer already proved evaluated membership against the validated
    universe before insert; the table intentionally stores only its count.  We
    do not fabricate a second population snapshot here. Hold-out controls are
    composed from the same exact in-sample support view promotion and execution
    use; the withheld row itself correctly keeps its control columns empty.
    """
    refusals: list[str] = []
    if row["purpose"] == "harness_validation":
        refusals.append("harness_validation_only")
    if not row["universe_basis"]:
        refusals.append("universe_basis_absent")
    elif row["universe_basis"] != "survivorship_free":
        refusals.append("universe_basis_not_survivorship_free")
    if row["carry_unmodelled"]:
        refusals.append("carry_unmodelled")
    if row["fx_unmodelled"]:
        refusals.append("fx_unmodelled")
    if cast(int, row["evaluated_instrument_count"]) == 0:
        refusals.append("no_instruments_evaluated")
    if not accesses_complete:
        refusals.append("holdout_accesses_unrecorded")
    if row["deflated_sharpe"] is None:
        refusals.append("deflated_sharpe_not_computed")
    if row["trial_count"] is None:
        refusals.append("trial_count_undeclared")
    if row["deflated_sharpe"] is not None and (
        row.get("trial_register_version") != TRIAL_REGISTER_VERSION
        or (row.get("trial_count") is not None and row.get("trial_count") != TRIAL_REGISTER.declared_count)
    ):
        refusals.append("trial_register_superseded")
    if row["effective_sample_size"] is None:
        refusals.append("effective_sample_size_not_computed")
    is_holdout = row.get("namespace") == "hold_out"
    if not ambiguity_complete:
        refusals.append("ambiguity_arms_not_compared")
    else:
        local_record = _ambiguity_record_from_result_row(row)
        if is_holdout:
            support_record = (
                _ambiguity_record_from_result_row(row, prefix="support_")
                if row.get("control_support_candidate_count") == 1
                else None
            )
            refusals.extend(composed_holdout_ambiguity_refusals(local_record, support_record))
        else:
            refusals.extend(ambiguity_promotion_refusals(local_record))
    if not quarantine_complete:
        refusals.append("quarantine_arms_not_compared")
    control_prefix = "control_" if is_holdout else ""
    support_is_unique = row.get("control_support_candidate_count") == 1 if is_holdout else True
    if not support_is_unique or row.get(f"{control_prefix}synthetic_control_model_id") is None:
        refusals.append("synthetic_control_not_run")
    else:
        ci_low = cast(Decimal, row[f"{control_prefix}synthetic_control_mean_return_ci_low_pct"])
        ci_high = cast(Decimal, row[f"{control_prefix}synthetic_control_mean_return_ci_high_pct"])
        strategy_sharpe = cast(Decimal, row[f"{control_prefix}sharpe"])
        cohort_threshold = cast(Decimal, row[f"{control_prefix}synthetic_control_sharpe_threshold"])
        if ci_low > 0 or ci_high < 0:
            refusals.append("synthetic_control_cohort_shows_edge")
        if strategy_sharpe <= cohort_threshold:
            refusals.append("synthetic_control_sharpe_below_cohort")
    return refusals


_RESULTS_SQL = """
    SELECT
        r.*,
        COALESCE(a.accesses, 0) AS accesses,
        COALESCE(a.evaluations, 0) AS evaluations,
        COALESCE(control_support.candidate_count, 0) AS control_support_candidate_count,
        control_result.synthetic_control_model_id AS control_synthetic_control_model_id,
        control_result.synthetic_control_mean_return_ci_low_pct
            AS control_synthetic_control_mean_return_ci_low_pct,
        control_result.synthetic_control_mean_return_ci_high_pct
            AS control_synthetic_control_mean_return_ci_high_pct,
        control_result.sharpe AS control_sharpe,
        control_result.synthetic_control_sharpe_threshold
            AS control_synthetic_control_sharpe_threshold,
        ambiguity.ambiguity_rule_version AS ambiguity_record_rule_version,
        ambiguity.comparison_basis AS ambiguity_comparison_basis,
        ambiguity.best_case_sharpe AS ambiguity_best_case_sharpe,
        ambiguity.worst_case_sharpe AS ambiguity_worst_case_sharpe,
        ambiguity.cohort_gap_threshold AS ambiguity_cohort_gap_threshold,
        ambiguity.payload_sha256 AS ambiguity_payload_sha256,
        support_ambiguity.ambiguity_rule_version AS support_ambiguity_record_rule_version,
        support_ambiguity.comparison_basis AS support_ambiguity_comparison_basis,
        support_ambiguity.best_case_sharpe AS support_ambiguity_best_case_sharpe,
        support_ambiguity.worst_case_sharpe AS support_ambiguity_worst_case_sharpe,
        support_ambiguity.cohort_gap_threshold AS support_ambiguity_cohort_gap_threshold,
        support_ambiguity.payload_sha256 AS support_ambiguity_payload_sha256
    FROM strategy_results_store r
    LEFT JOIN (
        SELECT strategy_id, strategy_version,
               COUNT(*) FILTER (WHERE access_kind = 'evaluate') AS evaluations,
               COUNT(*) AS accesses
        FROM strategy_holdout_accesses
        GROUP BY strategy_id, strategy_version
    ) a USING (strategy_id, strategy_version)
    LEFT JOIN strategy_result_control_support control_support
      ON control_support.holdout_result_id = r.result_id
    LEFT JOIN strategy_results_store control_result
      ON control_result.result_id = control_support.control_result_id
    LEFT JOIN strategy_result_ambiguity ambiguity
      ON ambiguity.result_id = r.result_id
    LEFT JOIN strategy_result_ambiguity support_ambiguity
      ON support_ambiguity.result_id = control_support.control_result_id
    WHERE r.strategy_version = ANY(%(versions)s)
      AND r.namespace = 'hold_out'
      AND r.corpus_version = %(corpus_version)s
      AND r.cost_model_id = %(cost_model_id)s
      AND r.sizing_rule = %(sizing_rule)s
      AND r.benchmark_rule = %(benchmark_rule)s
      AND r.return_basis = %(return_basis)s
      AND r.ambiguity_rule_version = %(ambiguity_rule_version)s
      AND r.position_rule_set_version = %(position_version)s
      AND r.outcome_rule_set_version = %(outcome_version)s
      AND r.input_rule_set_version = %(input_version)s
    ORDER BY r.strategy_id, r.window_start, r.window_end, r.ambiguity_arm, r.quarantine_arm
"""

_RESULT_COUNTS_SQL = """
    SELECT strategy_id, COUNT(*) AS count
    FROM strategy_results_store
    WHERE strategy_version = ANY(%(versions)s)
    GROUP BY strategy_id
"""

_SCAN_SQL = """
    SELECT
        w.strategy_id,
        w.strategy_version,
        w.frontier_date,
        w.updated_at,
        COALESCE(SUM(s.row_count) FILTER (
            WHERE s.verdict = 'fired' AND s.signal_kind = 'entry'
        ), 0) AS fired_entries,
        COALESCE(SUM(s.row_count) FILTER (
            WHERE s.verdict = 'fired' AND s.signal_kind = 'exit'
        ), 0) AS fired_exits,
        COALESCE(SUM(s.row_count) FILTER (WHERE s.verdict = 'not_fired'), 0) AS not_fired,
        COALESCE(SUM(s.row_count) FILTER (WHERE s.verdict = 'not_evaluable'), 0) AS not_evaluable
    FROM strategy_scan_watermark w
    LEFT JOIN strategy_signal_daily_counts s USING (strategy_id, strategy_version)
    WHERE w.strategy_version = ANY(%(versions)s)
    GROUP BY w.strategy_id, w.strategy_version, w.frontier_date, w.updated_at
"""

# Every stored result row for a manifest strategy that is NOT under its current
# version, grouped by the identity pins so the reader can say WHICH pins differ
# rather than silently dropping the row (#2624 scope 1).
#
# ⚠ Keyed on the PAIR `(strategy_id, strategy_version)`, not on version alone.
# The identity hash is over registry bytes, so nothing structurally prevents two
# strategies sharing one; `= ANY(versions)` would then leak rows across cards.
#
# Cheap by construction, not by luck: `strategy_results_store` is written only
# through `result_ledger` (`:460`), i.e. once per deliberate, trial-register
# charged run — 324 rows on dev in total, against 6.7M in `price_daily`.
_PRIOR_VERSION_RESULTS_SQL = """
    SELECT
        r.strategy_id,
        r.strategy_version,
        r.namespace,
        r.corpus_version,
        r.cost_model_id,
        r.sizing_rule,
        r.benchmark_rule,
        r.return_basis,
        r.ambiguity_rule_version,
        r.position_rule_set_version,
        r.outcome_rule_set_version,
        r.input_rule_set_version,
        COUNT(*) AS count
    FROM strategy_results_store r
    WHERE r.strategy_id = ANY(%(strategy_ids)s)
      AND NOT EXISTS (
          SELECT 1 FROM unnest(%(strategy_ids)s::text[], %(versions)s::text[]) AS cur(id, version)
          WHERE cur.id = r.strategy_id AND cur.version = r.strategy_version
      )
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
"""

# Watermarks for versions OTHER than the current one, newest frontier first.
# Same pair-keying rationale as above.
_PRIOR_VERSION_SCANS_SQL = """
    SELECT w.strategy_id, w.strategy_version, w.frontier_date, w.updated_at
    FROM strategy_scan_watermark w
    WHERE w.strategy_id = ANY(%(strategy_ids)s)
      AND NOT EXISTS (
          SELECT 1 FROM unnest(%(strategy_ids)s::text[], %(versions)s::text[]) AS cur(id, version)
          WHERE cur.id = w.strategy_id AND cur.version = w.strategy_version
      )
    ORDER BY w.strategy_id, w.frontier_date DESC, w.strategy_version DESC
"""

_EXCLUSIONS_SQL = """
    SELECT strategy_id, strategy_version, reason_code AS not_evaluable_reason,
           SUM(row_count) AS count
    FROM strategy_signal_daily_counts
    WHERE strategy_version = ANY(%(versions)s) AND verdict = 'not_evaluable'
    GROUP BY strategy_id, strategy_version, reason_code
"""

# The ingest transaction maintains this census on every series. Reading its
# ~one-row-per-symbol metadata avoids a full scan of the multi-million-row bar
# corpus on every Strategies page load.
_LATEST_CORPUS_SQL = "SELECT MAX(last_bar) FROM research_price_series"

_LATEST_EVIDENCE_REFRESH_SQL = """
    SELECT p.request_id, p.status AS request_status, p.requested_at, p.error_msg AS request_error,
           j.status AS run_status, j.finished_at, j.error_msg AS run_error, j.progress_json
    FROM pending_job_requests p
    LEFT JOIN LATERAL (
        SELECT status, finished_at, error_msg, progress_json
        FROM job_runs
        WHERE linked_request_id = p.request_id
        ORDER BY run_id DESC
        LIMIT 1
    ) j ON true
    WHERE p.job_name = %(job_name)s
      AND p.request_kind = 'manual_job'
      AND p.payload -> 'params' ->> 'refresh_recent' = 'true'
    ORDER BY p.request_id DESC
    LIMIT 1
"""

_CURRENT_FORECAST_ASSESSMENTS_SQL = """
    SELECT p.policy_id,p.max_assessment_age_days,
           c.strategy_id,c.strategy_version,c.checked_at,
           a.passed,a.resolved_forecasts,a.target_first_count,a.stop_first_count,a.timeout_count,
           a.normalized_brier_score,a.brier_skill_score,a.max_classwise_calibration_error
    FROM strategy_forecast_assessment_policies p
    JOIN strategy_forecast_assessment_current c ON c.policy_id=p.policy_id
    JOIN strategy_forecast_assessments a
      ON a.assessment_id=c.assessment_id
     AND a.policy_id=c.policy_id
     AND a.strategy_id=c.strategy_id
     AND a.strategy_version=c.strategy_version
     AND a.forecast_policy_version=c.forecast_policy_version
     AND a.model_version=c.model_version
     AND a.calibration_id=c.calibration_id
     AND a.setup_version=c.setup_version
     AND a.exit_policy_version=c.exit_policy_version
     AND a.resolver_version=c.resolver_version
     AND a.input_rule_set_version=c.input_rule_set_version
    WHERE p.policy_id = (
        SELECT policy_id FROM strategy_forecast_assessment_policies
        WHERE effective_from <= %(as_of)s
        ORDER BY effective_from DESC LIMIT 1
    )
"""


def _evidence_refresh_status(
    row: dict[str, object] | None,
) -> tuple[Literal["idle", "queued", "running", "failed", "complete"], str | None]:
    if row is None:
        return "idle", None
    run_status = row["run_status"]
    request_status = row["request_status"]
    if run_status == "failure" or request_status == "rejected":
        return "failed", cast(str | None, row["run_error"] or row["request_error"])
    if run_status == "running":
        return "running", None
    if request_status == "completed" and run_status in {"success", "degraded"}:
        return "complete", cast(str | None, row["run_error"])
    if request_status in {"pending", "claimed", "dispatched"}:
        return "queued", None
    return "idle", None


def _evidence_window_counts(strategies: list[StrategyOverview]) -> tuple[int, int]:
    """Return complete and partial pinned windows across runnable strategies.

    A missing member is missing evidence, never an exception. With no runnable
    strategies there is no evidence population, so the completed denominator
    is zero rather than vacuously all declared windows.
    """
    statuses = [
        {window.window_id: window.status for window in strategy.evidence_windows}
        for strategy in strategies
        if strategy.runnable
    ]
    if not statuses:
        return 0, 0
    completed = sum(
        all(strategy_windows.get(window_id) == "complete" for strategy_windows in statuses)
        for window_id in RECENT_EVIDENCE_WINDOWS
    )
    partial = sum(
        any(strategy_windows.get(window_id) == "partial" for strategy_windows in statuses)
        for window_id in RECENT_EVIDENCE_WINDOWS
    )
    return completed, partial


def _current_identity_pins() -> dict[str, str]:
    """The pins a stored row must match to sit on today's measurement basis.

    Exactly the equality predicates ``_RESULTS_SQL`` applies, named once so the
    prior-version reader cannot drift from the current-version one. These ARE the
    result identity — differing on any of them means the numbers are not
    comparable, which is why the reader reports the difference instead of either
    hiding the version or splicing its figures in.
    """
    return {
        "namespace": "hold_out",
        "corpus_version": corpus_version_for(BACKTEST_UNIVERSE),
        "cost_model_id": COST_MODEL_ID,
        "sizing_rule": SIZING_RULE_ID,
        "benchmark_rule": BENCHMARK_RULE_ID,
        "return_basis": TOTAL_RETURN_BASIS,
        "ambiguity_rule_version": AMBIGUITY_RULE_VERSION,
        "position_rule_set_version": POSITION_RULE_SET_VERSION,
        "outcome_rule_set_version": OUTCOME_RULE_SET_VERSION,
        "input_rule_set_version": QUARANTINE_RULE_SET_VERSION,
    }


def build_prior_versions(
    *,
    result_groups: Sequence[Mapping[str, object]],
    scan_rows: Sequence[Mapping[str, object]],
    pins: Mapping[str, str],
) -> dict[str, list[PriorVersionTrackRecord]]:
    """Group prior-version evidence into one record per ``(strategy_id, version)``.

    Pure so it can be table-tested without Postgres. ``result_groups`` is
    ``_PRIOR_VERSION_RESULTS_SQL``'s output (one row per identity-pin combination),
    ``scan_rows`` is ``_PRIOR_VERSION_SCANS_SQL``'s.

    ⚠ **Stored results are the qualifier; a watermark is not.** A version that
    scanned and stored nothing has no track record — it has a scan, and the scan
    is what ``ScanHealth.rotation`` carries. This is also what BOUNDS the list:
    ``strategy_results_store`` gains rows once per deliberate, trial-register
    charged run, whereas ``strategy_scan_watermark`` gains one per scan-day per
    version, so admitting watermark-only versions would grow the payload with
    every registry version ever scanned. ``scan_rows`` therefore only ENRICHES a
    result-bearing version (and separately selects the rotation at the call site).
    Measured on dev: ``+6c7cff76dcde`` is exactly this case — scanned 2026-08-07,
    zero stored rows.

    ``comparable`` is conservative: true only when the version HAS stored rows and
    every one of them matches every pin. A version with a mix is reported
    incomparable, because "partly on the current basis" is not a state an operator
    can act on. The ``has rows`` half of that is belt-and-braces given the
    qualifier above — "comparable" over an empty set is vacuously true, and that
    would read on the page as a track record that exists.

    Ordering is ``(last_scan_at, strategy_version)`` descending, nulls last, so
    the list is deterministic when two versions share a scan time or neither ever
    scanned — a bare date sort is not (Codex checkpoint 1).
    """
    counts: dict[tuple[str, str], int] = defaultdict(int)
    reasons: dict[tuple[str, str], set[str]] = defaultdict(set)
    for group in result_groups:
        key = (str(group["strategy_id"]), str(group["strategy_version"]))
        counts[key] += int(cast(int, group["count"]))
        reasons[key].update(name for name, expected in pins.items() if group.get(name) != expected)

    # One row per pair: `(strategy_id, strategy_version)` is the PRIMARY KEY of
    # `strategy_scan_watermark` (sql/272:59), so this is a lookup, not a dedup.
    scans: dict[tuple[str, str], Mapping[str, object]] = {
        (str(row["strategy_id"]), str(row["strategy_version"])): row for row in scan_rows
    }

    by_strategy: dict[str, list[PriorVersionTrackRecord]] = defaultdict(list)
    for strategy_id, version in sorted(counts):
        scan = scans.get((strategy_id, version))
        by_strategy[strategy_id].append(
            PriorVersionTrackRecord(
                strategy_version=version,
                result_count=counts.get((strategy_id, version), 0),
                last_scan_frontier_date=None if scan is None else cast(date | None, scan["frontier_date"]),
                last_scan_at=None if scan is None else cast(datetime | None, scan["updated_at"]),
                comparable=bool(counts.get((strategy_id, version), 0)) and not reasons.get((strategy_id, version)),
                incomparable_reasons=sorted(reasons.get((strategy_id, version), set())),
            )
        )

    for records in by_strategy.values():
        records.sort(
            key=lambda record: (
                record.last_scan_at is not None,
                record.last_scan_at or datetime.min.replace(tzinfo=UTC),
                record.strategy_version,
            ),
            reverse=True,
        )
    return by_strategy


@router.get("/overview", response_model=StrategyOverviewResponse)
def get_strategy_overview(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> StrategyOverviewResponse:
    as_of = datetime.now(tz=UTC)
    versions = _current_versions()
    version_values = list(versions.values())
    params = {
        "versions": version_values,
        "corpus_version": corpus_version_for(BACKTEST_UNIVERSE),
        "cost_model_id": COST_MODEL_ID,
        "sizing_rule": SIZING_RULE_ID,
        "benchmark_rule": BENCHMARK_RULE_ID,
        "return_basis": TOTAL_RETURN_BASIS,
        "ambiguity_rule_version": AMBIGUITY_RULE_VERSION,
        "position_version": POSITION_RULE_SET_VERSION,
        "outcome_version": OUTCOME_RULE_SET_VERSION,
        "input_version": QUARANTINE_RULE_SET_VERSION,
    }
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_RESULTS_SQL, params)
        result_rows = list(cur.fetchall())
        cur.execute(_RESULT_COUNTS_SQL, params)
        result_count_rows = list(cur.fetchall())
        cur.execute(_SCAN_SQL, params)
        scan_rows = list(cur.fetchall())
        cur.execute(_EXCLUSIONS_SQL, params)
        exclusion_rows = list(cur.fetchall())
        # Two parallel arrays `unnest`ed into current `(id, version)` pairs. Built
        # from one `items()` pass so the pairing cannot drift on a later edit.
        prior_params = {
            "strategy_ids": [strategy_id for strategy_id, _ in versions.items()],
            "versions": [version for _, version in versions.items()],
        }
        cur.execute(_PRIOR_VERSION_RESULTS_SQL, prior_params)
        prior_result_rows = list(cur.fetchall())
        cur.execute(_PRIOR_VERSION_SCANS_SQL, prior_params)
        prior_scan_rows = list(cur.fetchall())
        cur.execute(_LATEST_CORPUS_SQL)
        latest_row = cur.fetchone()
        latest_corpus_date = None if latest_row is None else latest_row["max"]
        cur.execute(_LATEST_EVIDENCE_REFRESH_SQL, {"job_name": _STRATEGY_BACKTEST_JOB})
        refresh_row = cur.fetchone()
        cur.execute(
            """
            SELECT policy_id,max_assessment_age_days
            FROM strategy_forecast_assessment_policies
            WHERE effective_from <= %s
            ORDER BY effective_from DESC LIMIT 1
            """,
            (as_of,),
        )
        assessment_policy_row = cur.fetchone()
        cur.execute(_CURRENT_FORECAST_ASSESSMENTS_SQL, {"as_of": as_of})
        assessment_rows = list(cur.fetchall())
        cur.execute("SELECT DISTINCT strategy_id,strategy_version FROM strategy_deployments WHERE mode='paper'")
        paper_deployment_keys = [(str(row["strategy_id"]), str(row["strategy_version"])) for row in cur.fetchall()]

    attribution_by_strategy = load_attribution(
        conn,
        versions=version_values,
        outcome_version=OUTCOME_RULE_SET_VERSION,
        input_version=QUARANTINE_RULE_SET_VERSION,
    )
    # Current versions only: a strategy_version is a rule set, so pooling two
    # would average two arithmetics into one rate (#2670's lesson).
    fire_rate_by_strategy = load_fire_rate(conn, versions=version_values)
    pnl_versions = sorted({*version_values, *(version for _strategy_id, version in paper_deployment_keys)})
    pnl_by_strategy = load_owned_pnl(conn, versions=pnl_versions)
    control_by_strategy = load_control_state(conn, versions=version_values)
    entry_block = load_entry_block_state(conn)
    paper_pool = load_paper_pool(conn)
    account_equity = load_account_equity_evidence(
        conn,
        environment=cast(Literal["demo", "real"], settings.etoro_env),
    )

    results_by_strategy: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in result_rows:
        results_by_strategy[str(row["strategy_id"])].append(row)
    result_counts = {str(row["strategy_id"]): int(row["count"]) for row in result_count_rows}
    scan_by_strategy = {str(row["strategy_id"]): row for row in scan_rows}
    exclusions: dict[str, Counter[str]] = defaultdict(Counter)
    for row in exclusion_rows:
        exclusions[str(row["strategy_id"])][str(row["not_evaluable_reason"])] += int(row["count"])

    prior_versions_by_strategy = build_prior_versions(
        result_groups=prior_result_rows,
        scan_rows=prior_scan_rows,
        pins=_current_identity_pins(),
    )
    # The rotation SELECTION rule is scope 3's, not a second one: the greatest
    # frontier date wins, which is what `assess_scan_freshness` computes into its
    # `fallback` basis. `_PRIOR_VERSION_SCANS_SQL` orders to match, and breaks a
    # frontier tie on the version so the choice is deterministic.
    previous_scan_by_strategy: dict[str, Mapping[str, object]] = {}
    for row in prior_scan_rows:
        previous_scan_by_strategy.setdefault(str(row["strategy_id"]), row)

    runnable, excluded = runnable_strategies()
    excluded_by_id = {item.strategy_id: item.reason for item in excluded}
    assessments_by_strategy: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in assessment_rows:
        assessments_by_strategy[(str(row["strategy_id"]), str(row["strategy_version"]))].append(row)
    assessment_policy_id = None if assessment_policy_row is None else str(assessment_policy_row["policy_id"])
    historically_ready_keys: set[tuple[str, str]] = set()
    strategies: list[StrategyOverview] = []
    for strategy_id in sorted(STRATEGY_MANIFEST):
        entry = STRATEGY_MANIFEST[strategy_id]
        strategy_rows = results_by_strategy[strategy_id]
        exact = defaultdict(list)
        declared_pairs = {(item.window.start, item.window.end): item for item in RECENT_EVIDENCE_WINDOWS.values()}
        for row in strategy_rows:
            key = (row["window_start"], row["window_end"])
            if key in declared_pairs:
                exact[key].append(row)

        windows: list[EvidenceWindow] = []
        for item in RECENT_EVIDENCE_WINDOWS.values():
            rows = exact[(item.window.start, item.window.end)]
            arm_keys = {(row["ambiguity_arm"], row["quarantine_arm"]) for row in rows}
            expected_arm_keys = {
                ("best_case", "masked"),
                ("best_case", "admitted"),
                ("worst_case", "masked"),
                ("worst_case", "admitted"),
            }
            arms_complete = arm_keys == expected_arm_keys
            ambiguity_complete = all(
                {(ambiguity, quarantine) for ambiguity in ("best_case", "worst_case")}.issubset(arm_keys)
                for quarantine in ("masked", "admitted")
                if any(key[1] == quarantine for key in arm_keys)
            ) and bool(arm_keys)
            quarantine_complete = all(
                {(ambiguity, quarantine) for quarantine in ("masked", "admitted")}.issubset(arm_keys)
                for ambiguity in ("best_case", "worst_case")
                if any(key[0] == ambiguity for key in arm_keys)
            ) and bool(arm_keys)
            status: Literal["missing", "partial", "complete"] = (
                "complete" if arms_complete else "partial" if rows else "missing"
            )
            arms = []
            for row in rows:
                accesses_complete = int(row["accesses"]) >= int(row["evaluations"]) > 0
                arms.append(
                    ResultArm(
                        **{field: row[field] for field in ResultArm.model_fields if field != "promotion_refusals"},
                        promotion_refusals=_promotion_refusals(
                            row,
                            ambiguity_complete=ambiguity_complete,
                            quarantine_complete=quarantine_complete,
                            accesses_complete=accesses_complete,
                        ),
                    )
                )
            windows.append(
                EvidenceWindow(
                    window_id=item.window_id,
                    label=item.label,
                    window_start=item.window.start,
                    window_end=item.window.end,
                    status=status,
                    arms=arms,
                )
            )

        scan_row = scan_by_strategy.get(strategy_id)
        frontier = None if scan_row is None else scan_row["frontier_date"]
        # #2624 scope 2 — `rotated` splits the two states that both used to read
        # `never_run`: the current version has no watermark, but a previous one
        # does, so the strategy HAS run and its track record started over. The
        # discriminator is the WATERMARK and not stored results, because results
        # and watermarks are independent (a version can hold one without the
        # other) and it is the scan this field describes.
        previous_scan = previous_scan_by_strategy.get(strategy_id)
        rotation = (
            ScanRotation(
                previous_version=str(previous_scan["strategy_version"]),
                previous_frontier_date=cast(date | None, previous_scan["frontier_date"]),
                previous_scanned_at=cast(datetime | None, previous_scan["updated_at"]),
            )
            if frontier is None and previous_scan is not None
            else None
        )
        scan_status: Literal["never_run", "rotated", "current", "stale"] = (
            ("rotated" if rotation is not None else "never_run")
            if frontier is None
            else "current"
            if latest_corpus_date is not None and frontier >= latest_corpus_date
            else "stale"
        )
        scan = ScanHealth(
            frontier_date=frontier,
            updated_at=None if scan_row is None else scan_row["updated_at"],
            status=scan_status,
            rotation=rotation,
            fired_entries=0 if scan_row is None else int(scan_row["fired_entries"]),
            fired_exits=0 if scan_row is None else int(scan_row["fired_exits"]),
            not_fired=0 if scan_row is None else int(scan_row["not_fired"]),
            not_evaluable=0 if scan_row is None else int(scan_row["not_evaluable"]),
            exclusions_by_reason=dict(exclusions[strategy_id]),
        )
        all_complete = all(window.status == "complete" for window in windows)
        evidence_refused = any(arm.promotion_refusals for window in windows for arm in window.arms)
        evidence_not_positive = (
            any(
                arm.expectancy_ci_low_pct is None or arm.expectancy_ci_low_pct <= 0
                for window in windows
                for arm in window.arms
            )
            or not windows
        )
        key = (strategy_id, versions[strategy_id])
        attribution = attribution_by_strategy.get(key, StrategyAttribution())
        # A key absent from the census has never been scanned, which the default
        # `rate_unavailable_reason` states rather than reading as a zero rate.
        fire_rate = fire_rate_by_strategy.get(key, StrategyFireRate())
        pnl = pnl_by_strategy.get(key, StrategyPnl())
        control = control_by_strategy.get(key, StrategyControlState())
        allocation_refusals: list[str] = []
        if entry.purpose == "harness_validation":
            allocation_refusals.append("harness_validation_only")
        if strategy_id not in runnable:
            allocation_refusals.append("strategy_not_runnable")
        if not all_complete:
            allocation_refusals.append("recent_evidence_incomplete")
        if evidence_refused:
            allocation_refusals.append("recent_evidence_gate_refused")
        if evidence_not_positive:
            allocation_refusals.append("recent_net_expectancy_not_positive")
        if control.stage not in {"paper_enabled", "live_enabled"}:
            allocation_refusals.append("paper_promotion_missing")
        if not control.pinned_evidence_ready:
            allocation_refusals.append("pinned_promotion_evidence_invalid")
        if not control.policy_configured:
            allocation_refusals.append("execution_policy_missing")
        if scan.status != "current":
            allocation_refusals.append("scan_not_current")
        # ⚠ CANNOT FIRE TODAY, DELIBERATELY KEPT (#2653). `control.currency` is
        # 'USD' for every strategy, by three independent routes: `sql/338`'s
        # `CHECK (currency = 'USD')` on `strategy_deployments` when a row exists,
        # `load_control_state`'s `str(row["currency"] or "USD")` when the LEFT
        # JOIN misses, and `StrategyControlState.currency`'s own default when the
        # key is absent entirely. So this reads as live protection in the
        # refusal vocabulary while being unreachable — which is the cost #2653
        # filed, and the reason it is named here rather than left to be
        # rediscovered.
        #
        # It stays because it is the chokepoint a WIDENING is supposed to trip.
        # `strategy_base_currency` lists this line among the sites to revisit
        # when support grows; widen the CHECK to two currencies while the
        # constant names one and this is the branch that refuses the third.
        # `test_the_deployment_currency_refusal_and_its_constraint_agree` fails
        # the moment either side moves alone.
        if control.currency not in SUPPORTED_DEPLOYMENT_CURRENCIES:
            allocation_refusals.append(DEPLOYMENT_CURRENCY_UNSUPPORTED)
        if entry.purpose == "capital_candidate" and not allocation_refusals:
            historically_ready_keys.add(key)
        if entry.purpose == "capital_candidate":
            current_assessments = assessments_by_strategy[key]
            if assessment_policy_row is None:
                allocation_refusals.append("prospective_assessment_policy_missing")
            elif not current_assessments:
                allocation_refusals.append("prospective_assessment_missing")
            else:
                passed_assessments = [row for row in current_assessments if bool(row["passed"])]
                if not passed_assessments:
                    allocation_refusals.append("prospective_assessment_not_passed")
                elif not any(
                    cast(datetime, row["checked_at"])
                    >= as_of - timedelta(days=cast(int, row["max_assessment_age_days"]))
                    and cast(datetime, row["checked_at"]) <= as_of + timedelta(seconds=5)
                    for row in passed_assessments
                ):
                    allocation_refusals.append("prospective_assessment_stale")
        remaining = max(control.capital_limit - control.reserved_capital, Decimal("0"))
        strategies.append(
            StrategyOverview(
                strategy_id=strategy_id,
                strategy_version=versions[strategy_id],
                purpose=entry.purpose,
                title=_TITLES.get(strategy_id, strategy_id),
                description=_PRESENTATION.get(strategy_id, ("Evidence-backed automated strategy.", "Rule based"))[0],
                exit_timing=_PRESENTATION.get(strategy_id, ("Evidence-backed automated strategy.", "Rule based"))[1],
                runnable=strategy_id in runnable,
                forward_outcome_supported=entry.exit_levels is not None,
                exclusion_reason=excluded_by_id.get(strategy_id),
                scan=scan,
                evidence_windows=windows,
                prior_versions=prior_versions_by_strategy.get(strategy_id, []),
                legacy_result_count=result_counts.get(strategy_id, 0) - sum(len(rows) for rows in exact.values()),
                all_recent_evidence_complete=all_complete,
                stage=control.stage,
                attribution=StrategyAttributionView(**attribution.__dict__),
                fire_rate=StrategyFireRateView(**fire_rate.__dict__),
                pnl=StrategyPnlView(
                    currency="USD",
                    strategy_trade_count=pnl.strategy_trade_count,
                    owned_position_count=pnl.owned_position_count,
                    active_position_count=pnl.active_position_count,
                    close_event_count=pnl.close_event_count,
                    invested_capital=pnl.invested_capital,
                    realised_pnl=pnl.realised_pnl,
                    unrealised_pnl=pnl.unrealised_pnl,
                    total_pnl=pnl.total_pnl,
                    observed_fees=pnl.observed_fees,
                    complete=pnl.complete,
                    incomplete_reasons=list(pnl.incomplete_reasons),
                ),
                allocation=StrategyAllocationView(
                    deployment_id=control.deployment_id,
                    capital_limit=control.capital_limit,
                    currency=control.currency,
                    enabled=control.enabled,
                    revision=control.revision,
                    reserved_capital=control.reserved_capital,
                    invested_capital=pnl.invested_capital,
                    remaining_capital=remaining,
                    policy_configured=control.policy_configured,
                    max_drawdown_limit_pct=control.max_drawdown_limit_pct,
                    ticket_sizing_mode=cast(
                        Literal["percent", "fixed"] | None,
                        control.ticket_sizing_mode,
                    ),
                    ticket_value=(
                        control.ticket_fraction * Decimal("100")
                        if control.ticket_sizing_mode == "percent" and control.ticket_fraction is not None
                        else control.fixed_ticket_amount
                    ),
                    max_ticket_amount=control.max_ticket_amount,
                ),
                allocation_ready=not allocation_refusals,
                allocation_refusals=allocation_refusals,
            )
        )
    reserved_total = sum((item.allocation.reserved_capital for item in strategies), Decimal("0"))
    invested_values = [item.allocation.invested_capital for item in strategies]
    invested_total = (
        sum((value for value in invested_values if value is not None), Decimal("0"))
        if all(value is not None for value in invested_values)
        else None
    )
    paper_realised = realised_pnl_for_keys(pnl_by_strategy, paper_deployment_keys)
    realised_delta = None if paper_realised is None else sum(paper_realised.values(), Decimal("0"))
    if realised_delta is not None and paper_pool.capital_mode == "fixed":
        realised_delta = min(realised_delta, Decimal("0"))
    effective_pool_capital = (
        max(Decimal("0"), paper_pool.capital_limit + realised_delta) if realised_delta is not None else None
    )
    completed_windows, partial_windows = _evidence_window_counts(strategies)
    refresh_status, refresh_error = _evidence_refresh_status(refresh_row)
    capital_candidates = [item for item in strategies if item.purpose == "capital_candidate"]
    capital_candidate_keys = {(item.strategy_id, item.strategy_version) for item in capital_candidates}
    candidate_assessments = [
        row for key, rows in assessments_by_strategy.items() if key in capital_candidate_keys for row in rows
    ]
    passed_assessments = [row for row in candidate_assessments if bool(row["passed"])]
    fresh_passed_assessments = [
        row
        for row in passed_assessments
        if cast(datetime, row["checked_at"]) >= as_of - timedelta(days=cast(int, row["max_assessment_age_days"]))
        and cast(datetime, row["checked_at"]) <= as_of + timedelta(seconds=5)
    ]
    prospectively_ready_count = sum(item.allocation_ready for item in capital_candidates)
    if not capital_candidates:
        readiness_state: AutomationReadinessState = "no_capital_candidates"
        readiness_blockers = ["no_capital_candidates"]
    elif not historically_ready_keys:
        readiness_state = "historical_validation_incomplete"
        readiness_blockers = ["historical_validation_incomplete"]
    elif assessment_policy_row is None:
        readiness_state = "assessment_policy_missing"
        readiness_blockers = ["prospective_assessment_policy_missing"]
    elif not candidate_assessments:
        readiness_state = "prospective_evidence_missing"
        readiness_blockers = ["prospective_assessment_missing"]
    elif not passed_assessments:
        readiness_state = "prospective_evidence_failed"
        readiness_blockers = ["prospective_assessment_not_passed"]
    elif not fresh_passed_assessments:
        readiness_state = "prospective_evidence_stale"
        readiness_blockers = ["prospective_assessment_stale"]
    elif not prospectively_ready_count:
        readiness_state = "candidate_evidence_incomplete"
        readiness_blockers = ["no_historically_ready_candidate_has_fresh_prospective_evidence"]
    else:
        readiness_state = "ready"
        readiness_blockers = []

    def _metric_values(field: str) -> list[Decimal]:
        return [Decimal(str(row[field])) for row in candidate_assessments if row[field] is not None]

    brier_scores = _metric_values("normalized_brier_score")
    skill_scores = _metric_values("brier_skill_score")
    calibration_errors = _metric_values("max_classwise_calibration_error")
    return StrategyOverviewResponse(
        as_of=as_of,
        demo_connection=settings.etoro_env == "demo",
        execution_enabled=entry_block.auto_trading_enabled,
        live_execution_enabled=entry_block.live_trading_enabled,
        entry_block=StrategyEntryBlockView(
            new_entries_blocked=entry_block.new_entries_blocked,
            global_kill_active=entry_block.global_kill_active,
            global_kill_reason=entry_block.global_kill_reason,
            global_kill_activated_at=entry_block.global_kill_activated_at,
            global_kill_activated_by=entry_block.global_kill_activated_by,
            execution_block_reasons=list(entry_block.execution_block_reasons),
        ),
        paper_pool=StrategyPaperPoolView(
            configured=paper_pool.event_id is not None,
            enabled=paper_pool.enabled,
            capital_limit=paper_pool.capital_limit,
            capital_mode=paper_pool.capital_mode,
            effective_capital=effective_pool_capital,
            reserved_capital=reserved_total,
            invested_capital=invested_total,
            remaining_capital=(
                max(effective_pool_capital - reserved_total, Decimal("0"))
                if effective_pool_capital is not None
                else None
            ),
            mandate=StrategyPortfolioMandateView(
                configured=paper_pool.mandate.configured,
                policy_version=paper_pool.mandate.policy_version,
                risk_profile=paper_pool.mandate.risk_profile,
                target_volatility_pct=paper_pool.mandate.target_volatility_pct,
                max_portfolio_drawdown_pct=paper_pool.mandate.max_portfolio_drawdown_pct,
                max_loss_per_position_pct=paper_pool.mandate.max_loss_per_position_pct,
                max_daily_loss_pct=paper_pool.mandate.max_daily_loss_pct,
                active_risk_budget_pct=paper_pool.mandate.active_risk_budget_pct,
                cash_reserve_pct=paper_pool.mandate.cash_reserve_pct,
                max_concurrent_positions=paper_pool.mandate.max_concurrent_positions,
                shorts_allowed=paper_pool.mandate.shorts_allowed,
                leverage_allowed=paper_pool.mandate.leverage_allowed,
            ),
            available_mandates=[_mandate_view(profile) for profile in ("cautious", "balanced", "growth")],
        ),
        automation_readiness=AutomationReadinessView(
            ready=readiness_state == "ready",
            state=readiness_state,
            capital_candidate_count=len(capital_candidates),
            historically_ready_candidate_count=len(historically_ready_keys),
            prospectively_ready_candidate_count=prospectively_ready_count,
            assessment_policy_id=assessment_policy_id,
            assessed_scope_count=len(candidate_assessments),
            passed_scope_count=len(passed_assessments),
            fresh_passed_scope_count=len(fresh_passed_assessments),
            resolved_forecasts=sum(cast(int, row["resolved_forecasts"]) for row in candidate_assessments),
            target_first_count=sum(cast(int, row["target_first_count"]) for row in candidate_assessments),
            stop_first_count=sum(cast(int, row["stop_first_count"]) for row in candidate_assessments),
            timeout_count=sum(cast(int, row["timeout_count"]) for row in candidate_assessments),
            latest_checked_at=max(
                (cast(datetime, row["checked_at"]) for row in candidate_assessments),
                default=None,
            ),
            worst_normalized_brier_score=max(brier_scores, default=None),
            weakest_brier_skill_score=min(skill_scores, default=None),
            worst_classwise_calibration_error=max(calibration_errors, default=None),
            blockers=readiness_blockers,
        ),
        account_equity_evidence=AccountEquityEvidenceView(
            status=account_equity.status,
            days_collected=account_equity.days_collected,
            snapshot_date=account_equity.snapshot_date,
            observed_at=account_equity.observed_at,
            account_currency_id=account_equity.account_currency_id,
            currency=account_equity.currency,
            official_equity=account_equity.official_equity,
            official_available_cash=account_equity.official_available_cash,
            official_total_invested=account_equity.official_total_invested,
            official_unrealised_pnl=account_equity.official_unrealised_pnl,
            local_eod_currency=account_equity.local_eod_currency,
            local_eod_value=account_equity.local_eod_value,
            local_eod_positions_priced=account_equity.local_eod_positions_priced,
            local_eod_stale_mark_positions=account_equity.local_eod_stale_mark_positions,
            difference=account_equity.difference,
            comparable=account_equity.comparable,
            incomplete_reasons=list(account_equity.incomplete_reasons),
        ),
        evidence_refresh=EvidenceRefreshView(
            frozen_through=max(item.window.end for item in RECENT_EVIDENCE_WINDOWS.values()),
            completed_windows=completed_windows,
            partial_windows=partial_windows,
            total_windows=len(RECENT_EVIDENCE_WINDOWS),
            status=refresh_status,
            request_id=None if refresh_row is None else int(refresh_row["request_id"]),
            requested_at=None if refresh_row is None else cast(datetime, refresh_row["requested_at"]),
            finished_at=None if refresh_row is None else cast(datetime | None, refresh_row["finished_at"]),
            last_error=refresh_error,
            progress=None
            if refresh_row is None or refresh_row["progress_json"] is None
            else cast(dict[str, object], refresh_row["progress_json"]),
        ),
        strategies=strategies,
    )


@router.post(
    "/evidence-refresh",
    response_model=EvidenceRefreshRequestResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
def request_evidence_refresh(
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> EvidenceRefreshRequestResponse:
    """Queue completion of the declared recent-regime evidence denominator.

    There are deliberately no window or date inputs on this surface. The job
    skips immutable completed identities, commits each missing pinned window as
    a restart boundary, and refuses partial identities rather than overwriting
    an audit record.
    """
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("strategy-evidence-refresh",))
        active = conn.execute(
            """
            SELECT p.request_id, p.status,
                   EXISTS (
                       SELECT 1 FROM job_runs j
                       WHERE j.linked_request_id = p.request_id AND j.status = 'running'
                   ) AS running
            FROM pending_job_requests p
            WHERE p.job_name = %(job_name)s
              AND p.request_kind = 'manual_job'
              AND p.status IN ('pending', 'claimed', 'dispatched')
              AND p.payload -> 'params' ->> 'refresh_recent' = 'true'
              AND NOT EXISTS (
                  SELECT 1 FROM job_runs j
                  WHERE j.linked_request_id = p.request_id
                    AND j.status IN ('success', 'failure', 'degraded')
              )
            ORDER BY p.request_id DESC
            LIMIT 1
            FOR UPDATE OF p
            """,
            {"job_name": _STRATEGY_BACKTEST_JOB},
        ).fetchone()
        if active is not None:
            active_row = cast(tuple[object, object, object], active)
            return EvidenceRefreshRequestResponse(
                request_id=int(cast(int, active_row[0])),
                status="running" if active_row[2] else "queued",
                already_active=True,
            )
        request_id = publish_manual_job_request_with_conn(
            conn,
            _STRATEGY_BACKTEST_JOB,
            requested_by=session.username,
            payload={
                "params": {
                    "refresh_recent": True,
                    "holdout_purpose": "complete declared recent-regime evidence denominator",
                    "holdout_accessed_by": session.username,
                },
                "control": {"override_bootstrap_gate": False},
            },
        )
    return EvidenceRefreshRequestResponse(request_id=request_id, status="queued", already_active=False)


_FIRED_SIGNALS_SQL = """
    WITH entry_execution AS (
        SELECT sto.strategy_trade_id,
               SUM(e.opening_units * e.average_price)
                   / NULLIF(SUM(e.opening_units), 0) AS average_price
        FROM strategy_trade_orders sto
        JOIN strategy_order_position_executions e ON e.order_id = sto.order_id
        WHERE sto.purpose = 'entry'
          AND e.opening_units > 0 AND e.average_price > 0
        GROUP BY sto.strategy_trade_id
    ), entry_order AS (
        SELECT DISTINCT ON (sto.strategy_trade_id)
               sto.strategy_trade_id, sto.order_id, ord.status
        FROM strategy_trade_orders sto
        JOIN orders ord ON ord.order_id = sto.order_id
        WHERE sto.purpose = 'entry'
        ORDER BY sto.strategy_trade_id, sto.linked_at DESC, sto.order_id DESC
    ), ownership_rollup AS (
        SELECT t.strategy_trade_id,
               COUNT(own.ownership_id)::integer AS ownership_count,
               CASE WHEN COUNT(own.ownership_id) = 1 THEN MAX(own.broker_position_id) END
                   AS broker_position_id,
               CASE WHEN COUNT(own.ownership_id) = 1 THEN MAX(own.status) END AS ownership_status,
               CASE WHEN COUNT(own.ownership_id) = 1 THEN MAX(own.claimed_at) END AS claimed_at,
               CASE WHEN COUNT(own.ownership_id) = 1 THEN MAX(own.released_at) END AS released_at,
               CASE WHEN COUNT(own.ownership_id) = 1 THEN MAX(own.release_reason) END AS release_reason
        FROM strategy_trades t
        LEFT JOIN strategy_position_ownership own ON own.strategy_trade_id = t.strategy_trade_id
        GROUP BY t.strategy_trade_id
    ), latest_operation AS (
        SELECT DISTINCT ON (own.strategy_trade_id)
               own.strategy_trade_id, op.position_operation_id, op.order_id,
               op.operation_type, op.trigger_code, op.status,
               op.created_at, op.submitted_at, op.resolved_at, op.last_error_code
        FROM strategy_position_ownership own
        JOIN strategy_position_operations op ON op.ownership_id = own.ownership_id
        ORDER BY own.strategy_trade_id, op.position_operation_id DESC
    ), close_history AS (
        SELECT own.strategy_trade_id,
               COUNT(event.event_id)::integer AS close_event_count,
               SUM(event.realized_pnl_usd) AS realised_pnl_usd,
               SUM(event.fees_usd) AS observed_fees_usd,
               BOOL_AND(event.realized_pnl_usd IS NOT NULL)
                   FILTER (WHERE event.event_id IS NOT NULL) AS pnl_complete,
               BOOL_AND(event.fees_usd IS NOT NULL)
                   FILTER (WHERE event.event_id IS NOT NULL) AS fees_complete
        FROM strategy_position_ownership own
        LEFT JOIN trade_events event
          ON event.position_id = own.broker_position_id
         AND event.event_kind = 'close'
        GROUP BY own.strategy_trade_id
    )
    SELECT
        s.signal_id, s.strategy_id, s.strategy_version, s.instrument_id,
        i.symbol, i.company_name, s.signal_bar_date, s.signal_kind,
        s.fill_bar_date, s.fill_price, s.universe,
        o.outcome, o.exit_bar_date, o.exit_price, o.gross_return_pct,
        o.reason AS outcome_reason,
        CASE
            WHEN s.signal_kind <> 'entry' THEN 'not_applicable'
            WHEN fd.verdict = 'allocated' THEN 'funded'
            ELSE 'rejected'
        END AS funding_status,
        CASE
            WHEN s.signal_kind <> 'entry' THEN 'capital_not_required_for_exit'
            ELSE COALESCE(fd.reason_code, 'not_evaluated_by_allocator')
        END AS funding_reason,
        fd.amount AS funded_amount,
        t.strategy_trade_id,
        CASE
            WHEN ee.average_price IS NOT NULL THEN 'filled'
            ELSE COALESCE(eo.status, t.status)
        END AS execution_status,
        ee.average_price AS actual_fill_price,
        ((ee.average_price - s.fill_price) / NULLIF(s.fill_price, 0)) * 100 AS slippage_pct,
        t.status AS lifecycle_trade_status,
        COALESCE(ownership.ownership_count, 0) AS lifecycle_ownership_count,
        ownership.broker_position_id AS lifecycle_broker_position_id,
        ownership.ownership_status AS lifecycle_ownership_status,
        ownership.claimed_at AS lifecycle_claimed_at,
        ownership.released_at AS lifecycle_released_at,
        ownership.release_reason AS lifecycle_release_reason,
        operation.operation_type AS lifecycle_operation_type,
        operation.position_operation_id AS lifecycle_operation_id,
        operation.order_id AS lifecycle_operation_order_id,
        operation.trigger_code AS lifecycle_operation_trigger,
        operation.status AS lifecycle_operation_status,
        operation.created_at AS lifecycle_operation_created_at,
        operation.submitted_at AS lifecycle_operation_submitted_at,
        operation.resolved_at AS lifecycle_operation_resolved_at,
        operation.last_error_code AS lifecycle_operation_error,
        reconciliation.state AS lifecycle_reconciliation_state,
        reconciliation.broker_status AS lifecycle_reconciliation_broker_status,
        reconciliation.attempt_count AS lifecycle_reconciliation_attempt_count,
        reconciliation.updated_at AS lifecycle_reconciliation_updated_at,
        reconciliation.last_error_code AS lifecycle_reconciliation_error,
        COALESCE(history.close_event_count, 0) AS lifecycle_close_event_count,
        history.realised_pnl_usd AS lifecycle_realised_pnl_usd,
        history.observed_fees_usd AS lifecycle_observed_fees_usd,
        history.pnl_complete AS lifecycle_pnl_complete,
        history.fees_complete AS lifecycle_fees_complete
    FROM strategy_signals s
    JOIN instruments i ON i.instrument_id = s.instrument_id
    LEFT JOIN strategy_outcomes o
      ON o.signal_id = s.signal_id
     AND o.rule_set_version = %(outcome_version)s
     AND o.input_rule_set_version = %(input_version)s
    LEFT JOIN strategy_funding_decisions fd ON fd.signal_id = s.signal_id
    LEFT JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
    LEFT JOIN entry_execution ee ON ee.strategy_trade_id = t.strategy_trade_id
    LEFT JOIN entry_order eo ON eo.strategy_trade_id = t.strategy_trade_id
    LEFT JOIN ownership_rollup ownership ON ownership.strategy_trade_id = t.strategy_trade_id
    LEFT JOIN latest_operation operation
      ON operation.strategy_trade_id = t.strategy_trade_id
     AND ownership.ownership_count = 1
    LEFT JOIN strategy_order_reconciliation_state reconciliation
      ON reconciliation.order_id = COALESCE(operation.order_id, eo.order_id)
    LEFT JOIN close_history history ON history.strategy_trade_id = t.strategy_trade_id
    WHERE s.verdict = 'fired'
      AND s.strategy_version = ANY(%(versions)s)
      AND (%(strategy_id)s::text IS NULL OR s.strategy_id = %(strategy_id)s)
      AND (%(cursor)s::bigint IS NULL OR s.signal_id < %(cursor)s)
    ORDER BY s.signal_id DESC
    LIMIT %(limit)s
"""


def _trade_lifecycle(row: Mapping[str, object]) -> StrategyTradeLifecycle | None:
    if row["funding_status"] != "funded":
        return None

    trade_status = cast(str | None, row["lifecycle_trade_status"])
    ownership_count = int(cast(int, row["lifecycle_ownership_count"]))
    raw_close_event_count = int(cast(int, row["lifecycle_close_event_count"]))
    ownership_status = cast(str | None, row["lifecycle_ownership_status"])
    reasons: list[str] = []
    history_reasons: list[str] = []

    if row["strategy_trade_id"] is None:
        reasons.append("funding_not_reconciled_to_trade")
    elif ownership_count == 0:
        if trade_status not in {"planned", "submitted", "failed"}:
            reasons.append("trade_not_reconciled_to_position")
    elif ownership_count > 1:
        reasons.append("position_ownership_ambiguous")
    else:
        if row["lifecycle_broker_position_id"] is None or ownership_status is None:
            reasons.append("position_ownership_incomplete")
        if raw_close_event_count == 0 and ownership_status == "released":
            history_reasons.append("released_position_missing_close_history")
        if raw_close_event_count > 0:
            if row["lifecycle_pnl_complete"] is not True:
                history_reasons.append("realised_pnl_missing_from_history")
            if row["lifecycle_fees_complete"] is not True:
                history_reasons.append("fees_missing_from_history")
        if trade_status == "closed" and ownership_status == "active":
            reasons.append("closed_trade_has_active_ownership")
        if ownership_status == "released" and trade_status not in {"closed", "failed"}:
            reasons.append("released_ownership_trade_not_closed")

    operation_status = row["lifecycle_operation_status"]
    reconciliation_state = row["lifecycle_reconciliation_state"]
    reconciliation_scope = "position_operation" if row["lifecycle_operation_order_id"] is not None else "entry_order"
    if operation_status == "rejected":
        reasons.append("position_operation_rejected")
    elif operation_status == "reconcile_required":
        reasons.append("position_operation_reconciliation_required")
    if row["lifecycle_operation_error"] is not None:
        reasons.append("position_operation_error")
    if reconciliation_state in {"not_found", "ambiguous", "error", "rejected"}:
        reasons.append(f"{reconciliation_scope}_reconciliation_{reconciliation_state}")
    if row["lifecycle_reconciliation_error"] is not None and reconciliation_state not in {
        "not_found",
        "ambiguous",
        "error",
    }:
        reasons.append(f"{reconciliation_scope}_reconciliation_error")
    reasons.extend(history_reasons)

    if trade_status == "failed" and ownership_count == 0:
        close_history_status: StrategyCloseHistoryStatus = "not_applicable"
    elif ownership_count == 0 and trade_status in {"planned", "submitted"}:
        close_history_status = "not_closed"
    elif row["strategy_trade_id"] is None or ownership_count != 1:
        close_history_status = "unavailable"
    elif history_reasons:
        close_history_status = "incomplete"
    elif raw_close_event_count == 0:
        close_history_status = "not_closed"
    else:
        close_history_status = "complete"

    history_complete = (
        ownership_count == 1
        and raw_close_event_count > 0
        and row["lifecycle_pnl_complete"] is True
        and row["lifecycle_fees_complete"] is True
    )
    return StrategyTradeLifecycle(
        trade_status=cast(StrategyTradeStatus | None, trade_status),
        ownership_count=ownership_count,
        broker_position_id=(
            int(cast(int, row["lifecycle_broker_position_id"]))
            if ownership_count == 1 and row["lifecycle_broker_position_id"] is not None
            else None
        ),
        ownership_status=cast(Literal["active", "released"] | None, ownership_status),
        position_claimed_at=cast(datetime | None, row["lifecycle_claimed_at"]),
        position_released_at=cast(datetime | None, row["lifecycle_released_at"]),
        position_release_reason=cast(str | None, row["lifecycle_release_reason"]),
        latest_operation_type=cast(
            Literal["fixed_exit_repair", "stop_ratchet", "close"] | None,
            row["lifecycle_operation_type"],
        ),
        latest_operation_id=(
            int(cast(int, row["lifecycle_operation_id"])) if row["lifecycle_operation_id"] is not None else None
        ),
        latest_operation_order_id=(
            int(cast(int, row["lifecycle_operation_order_id"]))
            if row["lifecycle_operation_order_id"] is not None
            else None
        ),
        latest_operation_trigger=cast(str | None, row["lifecycle_operation_trigger"]),
        latest_operation_status=cast(StrategyOperationStatus | None, row["lifecycle_operation_status"]),
        latest_operation_created_at=cast(datetime | None, row["lifecycle_operation_created_at"]),
        latest_operation_submitted_at=cast(datetime | None, row["lifecycle_operation_submitted_at"]),
        latest_operation_resolved_at=cast(datetime | None, row["lifecycle_operation_resolved_at"]),
        latest_operation_error=cast(str | None, row["lifecycle_operation_error"]),
        latest_reconciliation_state=cast(StrategyReconciliationState | None, row["lifecycle_reconciliation_state"]),
        latest_reconciliation_broker_status=cast(str | None, row["lifecycle_reconciliation_broker_status"]),
        latest_reconciliation_attempt_count=(
            int(cast(int, row["lifecycle_reconciliation_attempt_count"]))
            if row["lifecycle_reconciliation_attempt_count"] is not None
            else None
        ),
        latest_reconciliation_updated_at=cast(datetime | None, row["lifecycle_reconciliation_updated_at"]),
        latest_reconciliation_error=cast(str | None, row["lifecycle_reconciliation_error"]),
        close_event_count=(
            raw_close_event_count
            if ownership_count == 1 or (ownership_count == 0 and trade_status in {"planned", "submitted", "failed"})
            else None
        ),
        realised_pnl_usd=(Decimal(str(row["lifecycle_realised_pnl_usd"])) if history_complete else None),
        observed_fees_usd=(Decimal(str(row["lifecycle_observed_fees_usd"])) if history_complete else None),
        close_history_status=close_history_status,
        incomplete_reasons=reasons,
    )


def _fired_signal(row: Mapping[str, object]) -> FiredSignal:
    base = {key: value for key, value in row.items() if not key.startswith("lifecycle_")}
    return FiredSignal.model_validate({**base, "trade_lifecycle": _trade_lifecycle(row)})


@router.get("/signals", response_model=FiredSignalsResponse)
def get_fired_signals(
    cursor: int | None = Query(default=None, ge=1),
    limit: int = Query(default=50, ge=1, le=100),
    strategy_id: str | None = Query(default=None, min_length=1, max_length=200),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> FiredSignalsResponse:
    # Direct service/test callers do not receive FastAPI's default coercion.
    selected_strategy = strategy_id if isinstance(strategy_id, str) else None
    params = {
        "versions": list(_current_versions().values()),
        "cursor": cursor,
        "limit": limit,
        "strategy_id": selected_strategy,
        "outcome_version": OUTCOME_RULE_SET_VERSION,
        "input_version": QUARANTINE_RULE_SET_VERSION,
    }
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_FIRED_SIGNALS_SQL, params)
        rows = list(cur.fetchall())
    items = [_fired_signal(row) for row in rows]
    return FiredSignalsResponse(items=items, next_cursor=items[-1].signal_id if len(items) == limit else None)


@router.get("/positions", response_model=StrategyOwnedPositionsResponse)
def get_strategy_owned_positions(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> StrategyOwnedPositionsResponse:
    """Return only exact, active automated positions.

    Valuation is deliberately sourced through the Portfolio contract so the
    same broker position cannot show a different assigned value or P&L on the
    two pages.  Ownership metadata remains visible if the local broker snapshot
    is temporarily missing; that is a reconciliation state, not a reason to
    infer ownership from another position in the same instrument.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT ownership.strategy_trade_id,ownership.broker_position_id,
                   trade.status AS trade_status,
                   trade.core_rebalance_intent_id,
                   signal.strategy_id,signal.strategy_version,
                   instrument.instrument_id,instrument.symbol,instrument.company_name
            FROM strategy_position_ownership ownership
            JOIN strategy_trades trade
              ON trade.strategy_trade_id=ownership.strategy_trade_id
            LEFT JOIN strategy_funding_decisions funding
              ON funding.funding_decision_id=trade.funding_decision_id
            LEFT JOIN strategy_signals signal ON signal.signal_id=funding.signal_id
            JOIN instruments instrument ON instrument.instrument_id=trade.instrument_id
            WHERE ownership.status='active'
              AND trade.status IN ('open','closing','reconcile_required')
            ORDER BY ownership.claimed_at DESC,ownership.ownership_id DESC
            """
        )
        rows = list(cur.fetchall())

    portfolio = get_portfolio(conn)
    broker_positions = {trade.position_id: trade for position in portfolio.positions for trade in position.trades}
    positions: list[StrategyOwnedPosition] = []
    for row in rows:
        broker_position_id = int(row["broker_position_id"])
        broker_position = broker_positions.get(broker_position_id)
        assigned = Decimal(str(broker_position.amount)) if broker_position is not None else None
        unrealised = Decimal(str(broker_position.unrealized_pnl)) if broker_position is not None else None
        return_pct = (
            unrealised / assigned * Decimal("100")
            if unrealised is not None and assigned is not None and assigned != 0
            else None
        )
        # The core arm is identified by its AUTHORISATION column, not by
        # strategy_id being NULL -- the two would coincide today, but only the
        # former stays true if a signal row ever goes missing.
        is_core = row["core_rebalance_intent_id"] is not None
        strategy_id = None if row["strategy_id"] is None else str(row["strategy_id"])
        positions.append(
            StrategyOwnedPosition(
                strategy_trade_id=int(row["strategy_trade_id"]),
                broker_position_id=broker_position_id,
                strategy_id=strategy_id,
                strategy_version=(None if row["strategy_version"] is None else str(row["strategy_version"])),
                strategy_title=(
                    CORE_MANDATE_SERIES_TITLE if is_core else _TITLES.get(strategy_id or "", strategy_id or "")
                ),
                instrument_id=int(row["instrument_id"]),
                symbol=str(row["symbol"]),
                company_name=(str(row["company_name"]) if row["company_name"] is not None else None),
                direction=("long" if broker_position.is_buy else "short") if broker_position is not None else None,
                units=Decimal(str(broker_position.units)) if broker_position is not None else None,
                assigned_value=assigned,
                current_value=(Decimal(str(broker_position.market_value)) if broker_position is not None else None),
                unrealised_pnl=unrealised,
                unrealised_return_pct=return_pct,
                open_rate=Decimal(str(broker_position.open_rate)) if broker_position is not None else None,
                current_price=(
                    Decimal(str(broker_position.current_price))
                    if broker_position is not None and broker_position.current_price is not None
                    else None
                ),
                stop_loss_rate=(
                    Decimal(str(broker_position.stop_loss_rate))
                    if broker_position is not None and broker_position.stop_loss_rate is not None
                    else None
                ),
                take_profit_rate=(
                    Decimal(str(broker_position.take_profit_rate))
                    if broker_position is not None and broker_position.take_profit_rate is not None
                    else None
                ),
                opened_at=broker_position.open_date_time if broker_position is not None else None,
                currency=broker_position.currency if broker_position is not None else "USD",
                trade_status=cast(Literal["open", "closing", "reconcile_required"], row["trade_status"]),
                valuation_available=broker_position is not None,
            )
        )
    return StrategyOwnedPositionsResponse(
        positions=positions,
        live_quote_instrument_ids=sorted({position.instrument_id for position in positions}),
    )


def _load_strategy_broker_credentials(
    conn: psycopg.Connection[object],
    *,
    request: Request,
    session: SessionRow,
) -> tuple[str, str]:
    audit_pool = getattr(request.app.state, "audit_pool", None)
    try:
        if not ensure_broker_key_loaded(conn):
            raise CredentialCryptoConfigError("broker credential key is unavailable")
        api_key = load_credential_for_provider_use(
            conn,
            operator_id=session.operator_id,
            provider="etoro",
            label="api_key",
            environment="demo",
            caller="strategy_operator_close",
            audit_pool=audit_pool,
        )
        conn.commit()
        user_key = load_credential_for_provider_use(
            conn,
            operator_id=session.operator_id,
            provider="etoro",
            label="user_key",
            environment="demo",
            caller="strategy_operator_close",
            audit_pool=audit_pool,
        )
        conn.commit()
    except CredentialNotFound as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Demo broker credentials are not configured.",
        ) from exc
    except (CredentialDecryptError, CredentialCryptoConfigError, MasterKeyError) as exc:
        logger.error("strategy operator close: credential loading failed", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Demo broker credentials could not be loaded.",
        ) from exc
    return api_key, user_key


@router.post(
    "/positions/{strategy_trade_id}/{broker_position_id}/close",
    response_model=StrategyPositionCloseResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
def close_strategy_owned_position(
    strategy_trade_id: int,
    broker_position_id: int,
    request: Request,
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> StrategyPositionCloseResponse:
    """Submit a full close for one exact demo strategy-owned position."""
    if settings.etoro_env != "demo":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Strategy position close is available only for the demo account.",
        )
    owned = conn.execute(
        """
        SELECT 1
        FROM strategy_position_ownership ownership
        JOIN strategy_trades trade
          ON trade.strategy_trade_id=ownership.strategy_trade_id
        WHERE ownership.strategy_trade_id=%s
          AND ownership.broker_position_id=%s
          AND ownership.status='active'
          AND trade.status IN ('open','closing','reconcile_required')
        """,
        (strategy_trade_id, broker_position_id),
    ).fetchone()
    if owned is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Strategy-owned position not found.")

    api_key, user_key = _load_strategy_broker_credentials(conn, request=request, session=session)
    conn.commit()
    try:
        with EtoroBrokerProvider(api_key=api_key, user_key=user_key, env="demo") as broker:
            result = manage_owned_position(
                conn,
                broker=broker,
                strategy_trade_id=strategy_trade_id,
                broker_position_id=broker_position_id,
                close_reason="operator_close",
            )
    except (StrategyOwnershipError, StrategyPositionManagerError) as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    if result.state not in ("submitted", "pending", "applied"):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=result.reason_code)
    return StrategyPositionCloseResponse(
        strategy_trade_id=result.strategy_trade_id,
        broker_position_id=result.broker_position_id,
        state=result.state,
        reason_code=result.reason_code,
        operation_id=result.position_operation_id,
    )


@router.get("/pnl-history", response_model=StrategyPnlHistoryResponse)
def get_strategy_pnl_history(
    days: int = Query(default=365, ge=30, le=1825),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> StrategyPnlHistoryResponse:
    """Return bounded cumulative realised P&L for every exact-owned lifecycle.

    This is not portfolio return: capital revisions are external flows and open
    positions need historical marks.  Old/retired strategy versions remain in
    the shared pot, so history is deliberately not filtered to the manifest's
    current versions.
    """
    rows = cast(
        list[tuple[date, str, Decimal]],
        conn.execute(
            """
        -- A core close has no signal, so the arm is folded into one presentation
        -- series rather than dropped -- this endpoint's docstring promises EVERY
        -- exact-owned lifecycle, and an INNER JOIN to funding quietly broke that
        -- promise the moment sql/349 made a core trade storable.
        --
        -- GROUP BY / ORDER BY use ORDINALS, not a repeated COALESCE: repeating it
        -- would need the same positional parameter three times, and a positional
        -- list that has to stay aligned across three sites is the exact shape
        -- that shipped #2623's wrong-column bug. Naming the output column here
        -- would be worse -- Postgres resolves an ambiguous GROUP BY name to the
        -- INPUT column, so `GROUP BY strategy_id` would silently mean
        -- `signal.strategy_id`.
        SELECT (event.executed_at AT TIME ZONE 'UTC')::date AS pnl_date,
               COALESCE(signal.strategy_id, %(core_series)s) AS strategy_id,
               SUM(event.realized_pnl_usd) AS daily_pnl
        FROM strategy_position_ownership ownership
        JOIN strategy_trades trade ON trade.strategy_trade_id=ownership.strategy_trade_id
        LEFT JOIN strategy_funding_decisions funding ON funding.funding_decision_id=trade.funding_decision_id
        LEFT JOIN strategy_signals signal ON signal.signal_id=funding.signal_id
        JOIN trade_events event ON event.position_id=ownership.broker_position_id
        WHERE event.event_kind='close' AND event.realized_pnl_usd IS NOT NULL
          AND event.executed_at >= now() - make_interval(days => %(days)s)
        GROUP BY 1,2
        ORDER BY 1,2
        """,
            {"core_series": CORE_MANDATE_SERIES_ID, "days": days},
        ).fetchall(),
    )
    daily: dict[date, dict[str, Decimal]] = defaultdict(dict)
    for pnl_date, strategy_id, value in rows:
        daily[cast(date, pnl_date)][str(strategy_id)] = Decimal(str(value))
    running: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    points: list[StrategyPnlHistoryPoint] = []
    for pnl_date in sorted(daily):
        for strategy_id, value in daily[pnl_date].items():
            running[strategy_id] += value
        points.append(
            StrategyPnlHistoryPoint(
                date=pnl_date,
                total_pnl=sum(running.values(), Decimal("0")),
                strategy_pnl=dict(running),
            )
        )
    return StrategyPnlHistoryResponse(points=points)


@router.get("/wealth-history", response_model=StrategyWealthHistoryResponse)
def get_strategy_wealth_history(
    days: int = Query(default=365, ge=30, le=1825),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> StrategyWealthHistoryResponse:
    """Return principal plus realised and historical open marks for the sleeve."""
    return StrategyWealthHistoryResponse(
        points=[
            StrategyWealthHistoryPoint(
                date=point.date,
                principal=point.principal,
                external_flow=point.external_flow,
                realised_pnl=point.realised_pnl,
                unrealised_pnl=point.unrealised_pnl,
                total_pnl=point.total_pnl,
                pot_value=point.pot_value,
                complete=point.complete,
                incomplete_reasons=list(point.incomplete_reasons),
            )
            for point in load_strategy_wealth_history(conn, days=days)
        ]
    )


@router.put("/paper-pool", response_model=StrategyPaperPoolView)
def update_strategy_paper_pool(
    body: StrategyPaperPoolUpdateRequest,
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> StrategyPaperPoolView:
    """Set the shared strategy ceiling and its higher-level automation flag."""
    try:
        if body.enabled and settings.etoro_env != "demo":
            raise StrategyControlError("paper automation can only be enabled in the demo environment")
        readiness = get_strategy_overview(conn).automation_readiness if body.enabled else None
        conn.rollback()
        with conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", PAPER_ALLOCATOR_ADVISORY_LOCK)
            current_pool = load_paper_pool(conn)
            if body.enabled and not current_pool.enabled and readiness is not None and not readiness.ready:
                raise StrategyControlError("automation cannot be enabled: " + ", ".join(readiness.blockers))
            runtime = get_runtime_config(conn)
            pool_changed = (
                current_pool.enabled != body.enabled
                or current_pool.capital_limit != body.capital_limit
                or current_pool.capital_mode != body.capital_mode
                or current_pool.mandate.risk_profile != body.risk_profile
            )
            automation_changed = runtime.enable_auto_trading != body.enabled
            if not pool_changed and not automation_changed:
                raise StrategyControlError(
                    "automation change must alter enabled state, capital limit, capital mode, or mandate"
                )
            if pool_changed:
                configure_paper_pool(
                    conn,
                    enabled=body.enabled,
                    capital_limit=body.capital_limit,
                    capital_mode=body.capital_mode,
                    risk_profile=body.risk_profile,
                    changed_by=session.username,
                    reason=body.reason,
                )
            if automation_changed:
                update_runtime_config(
                    conn,
                    updated_by=session.username,
                    reason=body.reason,
                    enable_auto_trading=body.enabled,
                )
    except (StrategyControlError, RuntimeConfigCorrupt, RuntimeConfigNoOp) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return get_strategy_overview(conn).paper_pool


@router.get("/core-mandate", response_model=CoreMandateResponse, status_code=status.HTTP_200_OK)
def read_core_mandate(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> CoreMandateResponse:
    """The current core/cash mandate revision, or ``configured=false``.

    ⚠ Absence is a 200, not a 404. "No mandate has ever been configured" is a
    legitimate steady state of this system — it is where every install starts,
    and it is where the tree stood until this endpoint existed — so a reader must
    be able to distinguish it from a broken lookup. A 404 conflates the two.
    """
    return _core_mandate_response(load_core_mandate(conn))


@router.put("/core-mandate", response_model=CoreMandateResponse, status_code=status.HTTP_200_OK)
def update_core_mandate(
    body: CoreMandateUpdateRequest,
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> CoreMandateResponse:
    """Append one operator-authenticated core/cash mandate revision.

    ⚠⚠ THIS IS THE ENTRY CONDITION FOR #2603 ITEM 3, AND ITS ABSENCE WAS THE
    BLOCKER. ``configure_core_mandate`` had no caller anywhere in ``app/`` or
    ``scripts/`` — only tests — so no mandate could be configured, therefore no
    rebalance intent could cite one, therefore no core trade could exist. The
    whole arc was unreachable from outside the test suite.

    ⚠ Wiring this endpoint deliberately does NOT make the executor act on a
    mandate. That is the correct intermediate state (step 1's posture), not a
    gap to route around: a mandate becomes configurable here, and every
    execution-time control — the one-trade-per-intent guard, the
    no-open-core-trade precondition, the intent freshness bound — remains
    unbuilt and is tracked on #2603.

    ⚠ ``require_session``, not the router's ``require_session_or_service_token``.
    A mandate revision is an operator authorisation and is recorded with a named
    ``changed_by``; a service token has no operator identity to attribute it to,
    and ``configure_core_mandate`` needs a real ``operator_id`` to select the
    right eligibility proof.

    Both refusal families are 409 rather than 400: the request may be perfectly
    well formed and still be refused because the eligibility proof is stale, was
    observed under swapped credentials, or because nothing material changed.
    Those are states of the system, not faults in the payload.
    """
    # ⚠ NORMALISED BEFORE THE TRANSACTION, AND CAUGHT SEPARATELY (Codex ckpt-2).
    # `normalise_provider` / `normalise_environment` raise
    # `CredentialValidationError` on an unrecognised value, which is a 400 — a
    # malformed selector — and NOT one of the 409s below. Folding it into that
    # clause would report "the mandate was refused" for a typo in the account
    # name, and leaving it uncaught returned a 500 for ordinary bad input.
    # Hoisting it out of the transaction also keeps a pure-validation failure
    # from opening and rolling back a transaction for nothing.
    try:
        provider = normalise_provider(body.provider)
        environment = normalise_environment(body.environment)
    except CredentialValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        with conn.transaction():
            mandate = configure_core_mandate(
                conn,
                enabled=body.enabled,
                core_instrument_id=body.core_instrument_id,
                core_target_pct=body.core_target_pct,
                liquidity_reserve_pct=body.liquidity_reserve_pct,
                rebalance_band_pct=body.rebalance_band_pct,
                min_rebalance_amount=body.min_rebalance_amount,
                changed_by=session.username,
                reason=body.reason,
                operator_id=session.operator_id,
                provider=provider,
                environment=environment,
            )
    except (CoreMandateError, CoreEligibilityError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _core_mandate_response(mandate)


@router.put(
    "/{strategy_id}/allocation",
    response_model=AllocationUpdateResponse,
    status_code=status.HTTP_200_OK,
)
def update_strategy_allocation(
    strategy_id: str,
    body: AllocationUpdateRequest,
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> AllocationUpdateResponse:
    """Apply one operator-authenticated paper capital ceiling revision.

    Evidence-invalid strategies may only move toward safety by disabling and
    not increasing their existing ceiling.  The service appends the immutable
    deployment event in the same transaction as the current-state update.
    """
    current_version = _current_versions().get(strategy_id)
    if current_version is None:
        raise HTTPException(status_code=404, detail="strategy not found")
    if body.strategy_version != current_version:
        raise HTTPException(status_code=409, detail="strategy version changed; refresh required")

    try:
        with conn.transaction():
            lock_strategy_control(conn, strategy_id, current_version)
            overview = get_strategy_overview(conn)
            row = next(item for item in overview.strategies if item.strategy_id == strategy_id)
            risk_reducing = row.allocation.deployment_id is not None and is_risk_reducing_deployment_change(
                current_capital_limit=row.allocation.capital_limit,
                current_enabled=row.allocation.enabled,
                current_currency=row.allocation.currency,
                capital_limit=body.capital_limit,
                enabled=body.enabled,
                currency=row.allocation.currency,
            )
            if not row.allocation_ready and not risk_reducing:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "reason": "allocation_unavailable",
                        "refusals": row.allocation_refusals,
                    },
                )
            deployment = configure_deployment(
                conn,
                strategy_id=strategy_id,
                strategy_version=current_version,
                mode="paper",
                capital_limit=body.capital_limit,
                enabled=body.enabled,
                changed_by=session.username,
                reason=body.reason,
                currency=row.allocation.currency,
            )
    except StrategyControlError as exc:
        raise HTTPException(status_code=409, detail="allocation update refused") from exc
    return AllocationUpdateResponse(
        strategy_id=strategy_id,
        strategy_version=current_version,
        deployment_id=deployment.deployment_id,
        capital_limit=deployment.capital_limit,
        # The persisted, normalised value -- not the pre-call `row.allocation.currency`,
        # which could disagree with the row configure_deployment actually wrote.
        currency=deployment.currency,
        enabled=deployment.enabled,
        revision=deployment.revision,
    )


@router.put(
    "/{strategy_id}/sizing",
    response_model=StrategySizingUpdateResponse,
)
def update_strategy_sizing(
    strategy_id: str,
    body: StrategySizingUpdateRequest,
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> StrategySizingUpdateResponse:
    """Revise only an existing policy's per-signal sizing semantics."""
    _require_current_strategy_version(strategy_id, body.strategy_version)
    overview = get_strategy_overview(conn)
    strategy = next((item for item in overview.strategies if item.strategy_id == strategy_id), None)
    if strategy is None:
        conn.rollback()
        raise HTTPException(status_code=409, detail="strategy overview changed; refresh required")
    if not strategy.allocation_ready:
        conn.rollback()
        raise HTTPException(
            status_code=409,
            detail="sizing cannot be revised while strategy evidence or controls are invalid",
        )
    # End the read-only overview transaction before taking the control lock so
    # the lock is held only for the one policy row revision below.
    conn.rollback()
    try:
        with conn.transaction():
            lock_strategy_control(conn, strategy_id, body.strategy_version)
            if current_stage(conn, strategy_id, body.strategy_version) not in {"paper_enabled", "live_enabled"}:
                raise StrategyControlError("strategy is no longer approved for sizing")
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT p.*
                    FROM strategy_deployments d
                    JOIN strategy_execution_policies p USING (deployment_id)
                    WHERE d.strategy_id=%s AND d.strategy_version=%s AND d.mode='paper'
                    FOR UPDATE OF d,p
                    """,
                    (strategy_id, body.strategy_version),
                )
                current = cur.fetchone()
            if current is None:
                raise StrategyControlError("paper execution policy is unavailable")
            deployment_id = int(current["deployment_id"])
            policy = configure_execution_policy(
                conn,
                deployment_id=deployment_id,
                ticket_sizing_mode=body.ticket_sizing_mode,
                ticket_fraction=(body.ticket_value / Decimal("100") if body.ticket_sizing_mode == "percent" else None),
                fixed_ticket_amount=(body.ticket_value if body.ticket_sizing_mode == "fixed" else None),
                max_ticket_amount=body.max_ticket_amount,
                stop_loss_pct=Decimal(str(current["stop_loss_pct"])),
                take_profit_pct=Decimal(str(current["take_profit_pct"])),
                max_quote_age_seconds=int(current["max_quote_age_seconds"]),
                max_scan_age_seconds=int(current["max_scan_age_seconds"]),
                max_halt_feed_age_seconds=int(current["max_halt_feed_age_seconds"]),
                max_cost_age_seconds=int(current["max_cost_age_seconds"]),
                max_reconciliation_age_seconds=int(current["max_reconciliation_age_seconds"]),
                max_instrument_exposure_pct=Decimal(str(current["max_instrument_exposure_pct"])),
                max_portfolio_exposure_pct=Decimal(str(current["max_portfolio_exposure_pct"])),
                max_drawdown_pct=Decimal(str(current["max_drawdown_pct"])),
                min_net_expectancy_pct=Decimal(str(current["min_net_expectancy_pct"])),
                cost_stress_multiplier=Decimal(str(current["cost_stress_multiplier"])),
                changed_by=session.username,
                reason=body.reason,
            )
    except StrategyControlError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return StrategySizingUpdateResponse(
        strategy_id=strategy_id,
        strategy_version=body.strategy_version,
        deployment_id=deployment_id,
        revision=policy.revision,
        ticket_sizing_mode=body.ticket_sizing_mode,
        ticket_value=body.ticket_value,
        max_ticket_amount=body.max_ticket_amount,
    )


def _require_current_strategy_version(strategy_id: str, strategy_version: str) -> None:
    current_version = _current_versions().get(strategy_id)
    if current_version is None:
        raise HTTPException(status_code=404, detail="strategy not found")
    if strategy_version != current_version:
        raise HTTPException(status_code=409, detail="strategy version changed; refresh required")


@router.get("/{strategy_id}/live-gate", response_model=LiveGateResponse)
def get_live_gate(
    strategy_id: str,
    requested_capital: Decimal = Query(default=Decimal("0"), ge=0),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> LiveGateResponse:
    version = _current_versions().get(strategy_id)
    if version is None:
        raise HTTPException(status_code=404, detail="strategy not found")
    report = assess_live_gate(
        conn,
        strategy_id=strategy_id,
        strategy_version=version,
        requested_capital=requested_capital,
    )
    return _live_gate_view(report)


@router.post("/{strategy_id}/live-gate/policy", response_model=LiveGatePolicyView, status_code=201)
def create_live_gate_policy(
    strategy_id: str,
    body: LiveGatePolicyRequest,
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> LiveGatePolicyView:
    _require_current_strategy_version(strategy_id, body.strategy_version)
    try:
        with conn.transaction():
            policy = register_live_gate_policy(
                conn,
                strategy_id=strategy_id,
                strategy_version=body.strategy_version,
                min_forward_resolved_signals=body.min_forward_resolved_signals,
                min_forward_days=body.min_forward_days,
                min_paper_closed_trades=body.min_paper_closed_trades,
                min_paper_days=body.min_paper_days,
                max_reconciliation_age_seconds=body.max_reconciliation_age_seconds,
                min_shadow_alpha_pct=body.min_shadow_alpha_pct,
                max_cost_drift_pct=body.max_cost_drift_pct,
                max_average_slippage_pct=body.max_average_slippage_pct,
                max_drawdown_pct=body.max_drawdown_pct,
                max_scan_age_seconds=body.max_scan_age_seconds,
                max_quote_age_seconds=body.max_quote_age_seconds,
                max_broker_health_age_seconds=body.max_broker_health_age_seconds,
                max_live_capital=body.max_live_capital,
                registered_by=session.username,
                reason=body.reason,
            )
    except StrategyControlError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return LiveGatePolicyView(**policy.__dict__)


@router.post("/{strategy_id}/live-gate/drills/{drill_kind}", response_model=KillDrillResponse)
def execute_live_kill_drill(
    strategy_id: str,
    drill_kind: Literal["quote_lag", "scan_lag", "broker_outage", "reconciliation_backlog", "drawdown"],
    body: KillDrillRequest,
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> KillDrillResponse:
    _require_current_strategy_version(strategy_id, body.strategy_version)
    # The drill commits a synthetic block so concurrent workers can observe it.
    # End any dependency/read transaction before handing it the connection.
    if conn.info.transaction_status != TransactionStatus.IDLE:
        conn.rollback()
    try:
        event_id = run_kill_drill(
            conn,
            strategy_id=strategy_id,
            strategy_version=body.strategy_version,
            drill_kind=drill_kind,
            run_by=session.username,
            reason=body.reason,
        )
    except StrategyControlError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    event = cast(
        tuple[bool] | None,
        conn.execute(
            "SELECT passed FROM strategy_kill_drill_events WHERE kill_drill_event_id=%s",
            (event_id,),
        ).fetchone(),
    )
    if event is None:
        raise HTTPException(status_code=500, detail="kill drill audit result unavailable")
    return KillDrillResponse(kill_drill_event_id=event_id, drill_kind=drill_kind, passed=bool(event[0]))


@router.post("/{strategy_id}/live-promotion-attempt", response_model=LivePromotionAttemptResponse)
def attempt_live_promotion(
    strategy_id: str,
    body: LivePromotionAttemptRequest,
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> LivePromotionAttemptResponse:
    """Audit an explicit live request; real execution remains fail-closed."""
    _require_current_strategy_version(strategy_id, body.strategy_version)
    report = assess_live_gate(
        conn,
        strategy_id=strategy_id,
        strategy_version=body.strategy_version,
        requested_capital=body.requested_capital,
    )
    try:
        with conn.transaction():
            assessment_id = record_live_promotion_attempt(
                conn,
                report=report,
                assessed_by=session.username,
                reason=body.reason,
            )
    except StrategyControlError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return LivePromotionAttemptResponse(assessment_id=assessment_id, report=_live_gate_view(report))


@router.post("/{strategy_id}/lifecycle", response_model=StrategyLifecycleResponse)
def change_strategy_lifecycle(
    strategy_id: str,
    body: StrategyLifecycleRequest,
    session: SessionRow = Depends(require_session),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> StrategyLifecycleResponse:
    """Pause new entries or permanently retire an already-paused version."""
    _require_current_strategy_version(strategy_id, body.strategy_version)
    target: Literal["paused", "retired"] = "paused" if body.action == "pause" else "retired"
    try:
        with conn.transaction():
            lock_strategy_control(conn, strategy_id, body.strategy_version)
            stage = current_stage(conn, strategy_id, body.strategy_version)
            if target == "paused" and stage not in {
                "research_candidate",
                "historical_validated",
                "forward_observation",
                "paper_enabled",
                "live_enabled",
            }:
                raise StrategyControlError(f"strategy cannot be paused from stage {stage!r}")
            if target == "retired":
                if stage != "paused":
                    raise StrategyControlError("strategy must be paused before retirement")
                active = conn.execute(
                    """
                    SELECT 1
                    FROM strategy_position_ownership own
                    JOIN strategy_trades t ON t.strategy_trade_id=own.strategy_trade_id
                    JOIN strategy_funding_decisions fd ON fd.funding_decision_id=t.funding_decision_id
                    JOIN strategy_deployments d ON d.deployment_id=fd.deployment_id
                    WHERE d.strategy_id=%s AND d.strategy_version=%s AND own.status='active'
                    LIMIT 1
                    """,
                    (strategy_id, body.strategy_version),
                ).fetchone()
                if active is not None:
                    raise StrategyControlError("owned positions must be closed before retirement")
            deployments = conn.execute(
                """
                SELECT mode,capital_limit,currency,enabled
                FROM strategy_deployments
                WHERE strategy_id=%s AND strategy_version=%s
                ORDER BY mode
                """,
                (strategy_id, body.strategy_version),
            ).fetchall()
            for deployment_row in deployments:
                mode, capital, currency, enabled = cast(tuple[object, object, object, object], deployment_row)
                if enabled:
                    configure_deployment(
                        conn,
                        strategy_id=strategy_id,
                        strategy_version=body.strategy_version,
                        mode=cast(Literal["paper", "live"], mode),
                        capital_limit=Decimal(str(capital)),
                        currency=str(currency),
                        enabled=False,
                        changed_by=session.username,
                        reason=f"{body.action}: {body.reason}",
                    )
            promotion = promote_strategy(
                conn,
                strategy_id=strategy_id,
                strategy_version=body.strategy_version,
                to_stage=target,
                promoted_by=session.username,
                reason=body.reason,
            )
    except StrategyControlError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return StrategyLifecycleResponse(
        strategy_id=strategy_id,
        strategy_version=body.strategy_version,
        stage=target,
        promotion_id=promotion.promotion_id,
    )


__all__ = ["router"]
