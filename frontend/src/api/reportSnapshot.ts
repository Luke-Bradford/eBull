/**
 * SnapshotV2 — typed mirror of the report builders' snapshot_json
 * (#1592 child 2; data contract from #1596 child 3).
 *
 * Field-for-field mirror of `app/services/reporting.py::
 * generate_weekly_report` / `generate_monthly_report` return dicts.
 * Type-tested against the BACKEND-EMITTED fixtures at
 * `tests/fixtures/report_snapshot_v2/*.json` (regenerate with
 * `REPORT_FIXTURE_WRITE=1 uv run pytest tests/test_reporting_v2_db.py`
 * — never handwrite them). See reportSnapshot.test.ts.
 *
 * NUMERIC BASIS (spec §6.9 + verified against the builders):
 * - All Decimal values are STRINGS (builder `_dec` convention).
 * - `*_pct` / return / weight / exposure / volatility / drawdown fields
 *   are FRACTION-basis ("0.02" = 2%) — feed `formatPct` /
 *   `formatUnsignedPct` directly, never multiply by 100. This includes
 *   `gross_return_pct` despite the suffix (verified:
 *   `return_attribution.py:370` computes a plain ratio).
 * - EXCEPTIONS (pre-multiplied 0–100 strings): `trade_stats.win_rate_pct`,
 *   top-level `win_rate`, and `thesis_summary.{buy,avoid}.hit_rate_pct`
 *   ("66.67" = 66.67%) — render with a literal "%" suffix, never formatPct.
 */

export interface BridgeV2 {
  opening_value: string | null;
  net_external_flows: string | null;
  realized_delta: string | null;
  unrealized_delta: string | null;
  broker_adjustments_residual: string | null;
  closing_value: string;
}

export interface CoverV2 {
  closing_value: string;
  opening_value: string | null;
  /** Fraction-basis Modified Dietz period return. */
  period_return: string | null;
  benchmark_return: string | null;
  excess_return: string | null;
  ytd_return: string | null;
  si_return: string | null;
  benchmark_ytd_return: string | null;
  benchmark_si_return: string | null;
  realized_delta: string | null;
  unrealized_delta: string | null;
  cash: string | null;
  mirror_equity: string | null;
  display_currency: string;
  return_method: string;
  bridge: BridgeV2;
}

export interface BenchmarkV2 {
  symbol: string;
  /** Display label — "S&P 500 (price index)", never the internal symbol. */
  label: string;
  close_start: string | null;
  close_end: string | null;
  return_pct: string | null;
}

export interface PerformanceV2 {
  portfolio_value: string;
  period_return: string | null;
  benchmark: BenchmarkV2 | null;
  observations: number;
  fx_mode: string;
  method: string;
}

export interface HoldingV2 {
  instrument_id: number;
  symbol: string;
  company_name: string;
  sector: string | null;
  units: string | null;
  price: string | null;
  market_value: string;
  cost_basis: string;
  weight_pct: string | null;
  since_entry_return_pct: string | null;
  unrealized_pnl: string;
  period_contribution: string | null;
  period_contribution_bps: string | null;
  valuation_source: string;
}

export interface ContributorRowV2 {
  instrument_id: number;
  symbol: string;
  pnl_delta: string | null;
  pnl_pct: string | null;
}

export interface PeriodContributionV2 {
  contributors: ContributorRowV2[];
  drags: ContributorRowV2[];
}

export interface ActivityRowV2 {
  instrument_id: number;
  symbol: string;
  action: string;
  rationale: string | null;
  price: string | null;
  units: string | null;
  fees: string | null;
  filled_at: string | null;
}

export interface ScoreChangeV2 {
  instrument_id: number;
  symbol: string;
  total_score: string | null;
  rank: number | null;
  rank_delta: number | null;
  scored_at: string | null;
}

export interface IncomeItemV2 {
  instrument_id: number;
  symbol: string;
  ex_date: string | null;
  pay_date: string | null;
  dps_declared: string | null;
  currency: string;
  units: string | null;
  estimated_amount: string | null;
}

export interface IncomeV2 {
  items: IncomeItemV2[];
  estimated_totals: Record<string, string>;
  basis: string;
}

export interface CostsV2 {
  fees_total: string;
  fill_count: number;
  scope: string;
  /** Spec §4.8 degraded-state flag — not emitted by the current
   *  builder (fees are summed without FX conversion), but the badge
   *  must light up the day the backend stamps it. */
  fx_unavailable?: boolean;
}

export interface RiskV2 {
  holding_count: number;
  concentration_top5_pct: string | null;
  sector_exposure: Record<string, string>;
  volatility: string | null;
  max_drawdown: string | null;
  observations: number;
  observation_label: string;
  insufficient_history: boolean;
}

export interface RollingCellV2 {
  portfolio: string | null;
  benchmark: string | null;
  excess: string | null;
}

export interface RollingReturnsV2 {
  "1m": RollingCellV2;
  "3m": RollingCellV2;
  "6m": RollingCellV2;
  "1y": RollingCellV2;
  si: RollingCellV2;
}

export interface TradeStatsV2 {
  total_closed: number;
  winners: number;
  losers: number;
  /** PERCENT-basis string ("66.67") — see module header. */
  win_rate_pct: string | null;
  payoff_ratio: string | null;
  avg_win_pct: string | null;
  avg_loss_pct: string | null;
  avg_holding_days: number | null;
}

export interface AttributionSummaryV2 {
  positions_attributed: number;
  weighting: string;
  avg_gross_return_pct: string | null;
  avg_market_return_pct: string | null;
  avg_sector_return_pct: string | null;
  avg_model_alpha_pct: string | null;
  avg_timing_alpha_pct: string | null;
  avg_cost_drag_pct: string | null;
}

export interface ThesisBucketV2 {
  n: number;
  hits: number;
  /** PERCENT-basis string ("66.67") — see module header. */
  hit_rate_pct: string | null;
}

export interface ThesisSummaryV2 {
  total: number;
  evaluated: number;
  hits: number;
  misses: number;
  not_evaluable: number;
  buy: ThesisBucketV2;
  avoid: ThesisBucketV2;
}

export interface TradeReviewRowV2 {
  instrument_id: number;
  symbol: string;
  gross_return_pct: string | null;
  hold_days: number | null;
  model_alpha_pct: string | null;
}

export interface PnlV2 {
  realized_pnl: string;
  unrealized_pnl: string;
  total_pnl: string;
  note: string;
}

export interface PositionRowV2 {
  instrument_id: number;
  symbol: string;
  company_name: string;
  cost_basis: string;
  current_units: string;
  realized_pnl: string | null;
  unrealized_pnl: string;
}

export interface BudgetV2 {
  cash_balance: string | null;
  deployed_capital: string | null;
  estimated_tax_usd: string | null;
  available_for_deployment: string | null;
  /** Present (true) only on the degraded GBP→USD-rate-missing path. */
  fx_unavailable?: boolean;
}

interface SnapshotV2Base {
  schema_version: number;
  report_type: string;
  period_start: string;
  period_end: string;
  generated_at: string;
  cover: CoverV2;
  performance: PerformanceV2;
  holdings: HoldingV2[];
  period_contribution: PeriodContributionV2;
  positions: PositionRowV2[];
  pnl: PnlV2;
  score_changes: ScoreChangeV2[];
}

export interface WeeklySnapshotV2 extends SnapshotV2Base {
  report_type: "weekly";
  budget: BudgetV2;
  positions_opened: ActivityRowV2[];
  positions_closed: ActivityRowV2[];
  /** Legacy keys kept by the builder; the v2 statement does not render
   *  them (§4 dropped sections). */
  top_performers: unknown[];
  bottom_performers: unknown[];
  upcoming_earnings: unknown[];
}

export interface MonthlySnapshotV2 extends SnapshotV2Base {
  report_type: "monthly";
  rolling_returns: RollingReturnsV2;
  income: IncomeV2;
  costs: CostsV2;
  risk: RiskV2;
  thesis_summary: ThesisSummaryV2;
  trade_stats: TradeStatsV2;
  attribution_summary: AttributionSummaryV2;
  best_trade: TradeReviewRowV2 | null;
  worst_trade: TradeReviewRowV2 | null;
  /** §4.6 Period activity is W+M. OPTIONAL because v2 monthly
   *  snapshots generated before the monthly builder gained these keys
   *  (#1592 child-2 Codex P1) legitimately lack them — the FE renders
   *  the missing-key EmptyState for those until regeneration. */
  positions_opened?: ActivityRowV2[];
  positions_closed?: ActivityRowV2[];
  /** Legacy top-level duplicates of trade_stats fields. */
  win_rate: string | null;
  avg_holding_days: number | null;
  thesis_accuracy: unknown[];
  tax_provision: Record<string, unknown>;
  position_pnl: unknown[];
}

export type SnapshotV2 = WeeklySnapshotV2 | MonthlySnapshotV2;

/** v2 discriminator: v1 snapshots have no schema_version key (spec §3.2). */
export function isSnapshotV2(json: Record<string, unknown>): json is Record<string, unknown> & SnapshotV2 {
  return json["schema_version"] === 2;
}

export function isMonthlyV2(snap: SnapshotV2): snap is MonthlySnapshotV2 {
  return snap.report_type === "monthly";
}
