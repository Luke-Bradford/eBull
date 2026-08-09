"""Read-only strategy evidence and fired-signal monitoring (#2447).

This surface intentionally has no mutation route.  It reports the current
manifest denominator, exact-version recent evidence, scan health, and every
fired signal whether or not capital was available.  Allocation and execution
remain disabled until later tickets add ownership-safe broker reconciliation.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal, cast

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.services.backtest_run import BACKTEST_UNIVERSE, runnable_strategies
from app.services.cost_model import COST_MODEL_ID
from app.services.equity_curve import BENCHMARK_RULE_ID, SIZING_RULE_ID
from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.position_builder import RULE_SET_VERSION as POSITION_RULE_SET_VERSION
from app.services.research_price_structure_store import QUARANTINE_RULE_SET_VERSION
from app.services.strategy_manifest import STRATEGY_MANIFEST
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
    allocation_ready: bool = False
    allocation_refusals: list[str]


class StrategyOverviewResponse(BaseModel):
    as_of: datetime
    observation_stage: Literal["forward_observation"] = "forward_observation"
    execution_enabled: bool = False
    storage_policy: str = "aggregate_results_only"
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
    observation_stage: Literal["forward_observation"] = "forward_observation"
    funding_status: Literal["unfunded"] = "unfunded"
    funding_reason: Literal["execution_not_enabled"] = "execution_not_enabled"


class FiredSignalsResponse(BaseModel):
    items: list[FiredSignal]
    next_cursor: int | None


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
        COUNT(*) FILTER (WHERE s.verdict = 'fired' AND s.signal_kind = 'entry') AS fired_entries,
        COUNT(*) FILTER (WHERE s.verdict = 'fired' AND s.signal_kind = 'exit') AS fired_exits,
        COUNT(*) FILTER (WHERE s.verdict = 'not_fired') AS not_fired,
        COUNT(*) FILTER (WHERE s.verdict = 'not_evaluable') AS not_evaluable
    FROM strategy_scan_watermark w
    LEFT JOIN strategy_signals s USING (strategy_id, strategy_version)
    WHERE w.strategy_version = ANY(%(versions)s)
    GROUP BY w.strategy_id, w.strategy_version, w.frontier_date, w.updated_at
"""

_EXCLUSIONS_SQL = """
    SELECT strategy_id, strategy_version, not_evaluable_reason, COUNT(*) AS count
    FROM strategy_signals
    WHERE strategy_version = ANY(%(versions)s) AND not_evaluable_reason IS NOT NULL
    GROUP BY strategy_id, strategy_version, not_evaluable_reason
"""

_LATEST_CORPUS_SQL = "SELECT MAX(bar_date) FROM research_price_daily"


@router.get("/overview", response_model=StrategyOverviewResponse)
def get_strategy_overview(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> StrategyOverviewResponse:
    versions = _current_versions()
    params = {
        "versions": list(versions.values()),
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
        allocation_refusals = ["execution_not_enabled", "broker_cost_unknown"]
        if strategy_id not in runnable:
            allocation_refusals.append("strategy_not_runnable")
        if not all_complete:
            allocation_refusals.append("recent_evidence_incomplete")
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
                allocation_refusals=allocation_refusals,
            )
        )
    return StrategyOverviewResponse(as_of=datetime.now(tz=UTC), strategies=strategies)


_FIRED_SIGNALS_SQL = """
    SELECT
        s.signal_id, s.strategy_id, s.strategy_version, s.instrument_id,
        i.symbol, i.company_name, s.signal_bar_date, s.signal_kind,
        s.fill_bar_date, s.fill_price, s.universe,
        o.outcome, o.exit_bar_date, o.exit_price, o.gross_return_pct,
        o.reason AS outcome_reason
    FROM strategy_signals s
    JOIN instruments i ON i.instrument_id = s.instrument_id
    LEFT JOIN strategy_outcomes o
      ON o.signal_id = s.signal_id
     AND o.rule_set_version = %(outcome_version)s
     AND o.input_rule_set_version = %(input_version)s
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


__all__ = ["router"]
