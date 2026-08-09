"""Strategy evidence, exact-owned P&L, attribution and allocation (#2447/#2453)."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal, cast

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query, status
from psycopg.pq import TransactionStatus
from pydantic import BaseModel, Field, model_validator

from app.api.auth import require_session, require_session_or_service_token
from app.db import get_conn
from app.security.sessions import SessionRow
from app.services.backtest_run import BACKTEST_UNIVERSE, runnable_strategies
from app.services.cost_model import COST_MODEL_ID
from app.services.equity_curve import BENCHMARK_RULE_ID, SIZING_RULE_ID
from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.position_builder import RULE_SET_VERSION as POSITION_RULE_SET_VERSION
from app.services.research_price_structure_store import QUARANTINE_RULE_SET_VERSION
from app.services.strategy_control_plane import (
    StrategyControlError,
    configure_deployment,
    current_stage,
    is_risk_reducing_deployment_change,
    lock_strategy_control,
    promote_strategy,
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
    StrategyAttribution,
    StrategyControlState,
    StrategyPnl,
    load_attribution,
    load_control_state,
    load_entry_block_state,
    load_owned_pnl,
)
from app.services.strategy_recent_evidence import RECENT_EVIDENCE_WINDOWS
from app.services.strategy_result import CORPUS_VERSION

router = APIRouter(
    prefix="/strategies",
    tags=["strategies"],
    dependencies=[Depends(require_session_or_service_token)],
)

_TITLES = {
    "s1-time-series-momentum": "Time-series momentum",
    "s2-cross-sectional-momentum": "Cross-sectional momentum",
    "s3-mean-reversion-in-trend": "Mean reversion in trend",
    "s4-volatility-compression-breakout": "Volatility compression breakout",
}


class ScanHealth(BaseModel):
    frontier_date: date | None
    updated_at: datetime | None
    status: Literal["never_run", "current", "stale"]
    fired_entries: int = 0
    fired_exits: int = 0
    not_fired: int = 0
    not_evaluable: int = 0
    exclusions_by_reason: dict[str, int] = Field(default_factory=dict)


class ResultArm(BaseModel):
    result_version: str
    ambiguity_arm: str
    quarantine_arm: str
    universe_basis: str
    corpus_version: str
    cost_model_id: str
    sizing_rule: str
    benchmark_rule: str
    position_rule_set_version: str
    outcome_rule_set_version: str
    input_rule_set_version: str
    evaluated_instrument_count: int
    trade_count: int
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
    title: str
    runnable: bool
    exclusion_reason: str | None
    scan: ScanHealth
    evidence_windows: list[EvidenceWindow]
    legacy_result_count: int
    all_recent_evidence_complete: bool
    stage: str | None
    attribution: StrategyAttributionView
    pnl: StrategyPnlView
    allocation: StrategyAllocationView
    allocation_ready: bool
    allocation_refusals: list[str]


class StrategyAttributionView(BaseModel):
    fired_entries: int
    funded_entries: int
    rejected_entries: int
    resolved_entries: int
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


class StrategyEntryBlockView(BaseModel):
    new_entries_blocked: bool
    global_kill_active: bool
    global_kill_reason: str | None
    global_kill_activated_at: datetime | None
    global_kill_activated_by: str | None
    execution_block_reasons: list[str]


class StrategyOverviewResponse(BaseModel):
    as_of: datetime
    execution_enabled: bool
    live_execution_enabled: bool
    live_strategy_activation_available: Literal[False] = False
    live_strategy_activation_blocker: Literal["live_strategy_broker_contract_not_validated"] = (
        "live_strategy_broker_contract_not_validated"
    )
    storage_policy: Literal["fired_signals_and_material_mutations_only"] = "fired_signals_and_material_mutations_only"
    entry_block: StrategyEntryBlockView
    strategies: list[StrategyOverview]


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


class LiveGateFactsView(BaseModel):
    stage: str | None
    forward_resolved_signals: int
    forward_days: int
    paper_closed_trades: int
    paper_days: int
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
    passed: bool = True


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
    )


def _current_versions() -> dict[str, str]:
    return {
        strategy_id: entry.identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID).version
        for strategy_id, entry in STRATEGY_MANIFEST.items()
    }


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
    do not fabricate a second population snapshot here.  All current rows are
    refused independently by survivorship, carry and synthetic-control gaps.
    """
    refusals: list[str] = []
    if not row["universe_basis"]:
        refusals.append("universe_basis_absent")
    elif row["universe_basis"] != "survivorship_free":
        refusals.append("universe_basis_not_survivorship_free")
    if row["carry_unmodelled"]:
        refusals.append("carry_unmodelled")
    if cast(int, row["evaluated_instrument_count"]) == 0:
        refusals.append("no_instruments_evaluated")
    if not accesses_complete:
        refusals.append("holdout_accesses_unrecorded")
    if row["deflated_sharpe"] is None:
        refusals.append("deflated_sharpe_not_computed")
    if row["trial_count"] is None:
        refusals.append("trial_count_undeclared")
    if row["effective_sample_size"] is None:
        refusals.append("effective_sample_size_not_computed")
    # Runnable v1 strategies are non-level regimes: ambiguity is unreachable,
    # so the two labelled rows are intentionally the same measurement.
    if not ambiguity_complete:
        refusals.append("ambiguity_arms_not_compared")
    if not quarantine_complete:
        refusals.append("quarantine_arms_not_compared")
    if row["synthetic_control_model_id"] is None:
        refusals.append("synthetic_control_not_run")
    else:
        ci_low = cast(Decimal, row["synthetic_control_mean_return_ci_low_pct"])
        ci_high = cast(Decimal, row["synthetic_control_mean_return_ci_high_pct"])
        strategy_sharpe = cast(Decimal, row["sharpe"])
        cohort_threshold = cast(Decimal, row["synthetic_control_sharpe_threshold"])
        if ci_low > 0 or ci_high < 0:
            refusals.append("synthetic_control_cohort_shows_edge")
        if strategy_sharpe <= cohort_threshold:
            refusals.append("synthetic_control_sharpe_below_cohort")
    return refusals


_RESULTS_SQL = """
    SELECT
        r.*,
        COALESCE(a.accesses, 0) AS accesses,
        COALESCE(a.evaluations, 0) AS evaluations
    FROM strategy_results_store r
    LEFT JOIN (
        SELECT strategy_id, strategy_version,
               COUNT(*) FILTER (WHERE access_kind = 'evaluate') AS evaluations,
               COUNT(*) AS accesses
        FROM strategy_holdout_accesses
        GROUP BY strategy_id, strategy_version
    ) a USING (strategy_id, strategy_version)
    WHERE r.strategy_version = ANY(%(versions)s)
      AND r.namespace = 'hold_out'
      AND r.corpus_version = %(corpus_version)s
      AND r.cost_model_id = %(cost_model_id)s
      AND r.sizing_rule = %(sizing_rule)s
      AND r.benchmark_rule = %(benchmark_rule)s
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

_EXCLUSIONS_SQL = """
    SELECT strategy_id, strategy_version, reason_code AS not_evaluable_reason,
           SUM(row_count) AS count
    FROM strategy_signal_daily_counts
    WHERE strategy_version = ANY(%(versions)s) AND verdict = 'not_evaluable'
    GROUP BY strategy_id, strategy_version, reason_code
"""

_LATEST_CORPUS_SQL = "SELECT MAX(bar_date) FROM research_price_daily"


@router.get("/overview", response_model=StrategyOverviewResponse)
def get_strategy_overview(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> StrategyOverviewResponse:
    versions = _current_versions()
    version_values = list(versions.values())
    params = {
        "versions": version_values,
        "corpus_version": CORPUS_VERSION,
        "cost_model_id": COST_MODEL_ID,
        "sizing_rule": SIZING_RULE_ID,
        "benchmark_rule": BENCHMARK_RULE_ID,
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
        cur.execute(_LATEST_CORPUS_SQL)
        latest_row = cur.fetchone()
        latest_corpus_date = None if latest_row is None else latest_row["max"]

    attribution_by_strategy = load_attribution(
        conn,
        versions=version_values,
        outcome_version=OUTCOME_RULE_SET_VERSION,
        input_version=QUARANTINE_RULE_SET_VERSION,
    )
    pnl_by_strategy = load_owned_pnl(conn, versions=version_values)
    control_by_strategy = load_control_state(conn, versions=version_values)
    entry_block = load_entry_block_state(conn)

    results_by_strategy: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in result_rows:
        results_by_strategy[str(row["strategy_id"])].append(row)
    result_counts = {str(row["strategy_id"]): int(row["count"]) for row in result_count_rows}
    scan_by_strategy = {str(row["strategy_id"]): row for row in scan_rows}
    exclusions: dict[str, Counter[str]] = defaultdict(Counter)
    for row in exclusion_rows:
        exclusions[str(row["strategy_id"])][str(row["not_evaluable_reason"])] += int(row["count"])

    runnable, excluded = runnable_strategies()
    excluded_by_id = {item.strategy_id: item.reason for item in excluded}
    strategies: list[StrategyOverview] = []
    for strategy_id in sorted(STRATEGY_MANIFEST):
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
        scan_status: Literal["never_run", "current", "stale"] = (
            "never_run"
            if frontier is None
            else "current"
            if latest_corpus_date is not None and frontier >= latest_corpus_date
            else "stale"
        )
        scan = ScanHealth(
            frontier_date=frontier,
            updated_at=None if scan_row is None else scan_row["updated_at"],
            status=scan_status,
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
        pnl = pnl_by_strategy.get(key, StrategyPnl())
        control = control_by_strategy.get(key, StrategyControlState())
        allocation_refusals: list[str] = []
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
        if control.currency != "USD":
            allocation_refusals.append("deployment_currency_unsupported")
        remaining = max(control.capital_limit - control.reserved_capital, Decimal("0"))
        strategies.append(
            StrategyOverview(
                strategy_id=strategy_id,
                strategy_version=versions[strategy_id],
                title=_TITLES.get(strategy_id, strategy_id),
                runnable=strategy_id in runnable,
                exclusion_reason=excluded_by_id.get(strategy_id),
                scan=scan,
                evidence_windows=windows,
                legacy_result_count=result_counts.get(strategy_id, 0) - sum(len(rows) for rows in exact.values()),
                all_recent_evidence_complete=all_complete,
                stage=control.stage,
                attribution=StrategyAttributionView(**attribution.__dict__),
                pnl=StrategyPnlView(
                    **{
                        **pnl.__dict__,
                        "incomplete_reasons": list(pnl.incomplete_reasons),
                    }
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
                ),
                allocation_ready=not allocation_refusals,
                allocation_refusals=allocation_refusals,
            )
        )
    return StrategyOverviewResponse(
        as_of=datetime.now(tz=UTC),
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
        strategies=strategies,
    )


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
               sto.strategy_trade_id, ord.status
        FROM strategy_trade_orders sto
        JOIN orders ord ON ord.order_id = sto.order_id
        WHERE sto.purpose = 'entry'
        ORDER BY sto.strategy_trade_id, sto.linked_at DESC, sto.order_id DESC
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
        ((ee.average_price - s.fill_price) / NULLIF(s.fill_price, 0)) * 100 AS slippage_pct
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
    WHERE s.verdict = 'fired'
      AND s.strategy_version = ANY(%(versions)s)
      AND (%(cursor)s::bigint IS NULL OR s.signal_id < %(cursor)s)
    ORDER BY s.signal_id DESC
    LIMIT %(limit)s
"""


@router.get("/signals", response_model=FiredSignalsResponse)
def get_fired_signals(
    cursor: int | None = Query(default=None, ge=1),
    limit: int = Query(default=50, ge=1, le=100),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> FiredSignalsResponse:
    params = {
        "versions": list(_current_versions().values()),
        "cursor": cursor,
        "limit": limit,
        "outcome_version": OUTCOME_RULE_SET_VERSION,
        "input_version": QUARANTINE_RULE_SET_VERSION,
    }
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_FIRED_SIGNALS_SQL, params)
        rows = list(cur.fetchall())
    items = [FiredSignal(**row) for row in rows]
    return FiredSignalsResponse(items=items, next_cursor=items[-1].signal_id if len(items) == limit else None)


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
        currency=row.allocation.currency,
        enabled=deployment.enabled,
        revision=deployment.revision,
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
    return KillDrillResponse(kill_drill_event_id=event_id, drill_kind=drill_kind)


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
