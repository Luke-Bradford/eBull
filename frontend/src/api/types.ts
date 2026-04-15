/**
 * Response-shape types mirroring the FastAPI backend.
 *
 * These mirror the Pydantic response_models in app/api/*.py as of the
 * scaffold PR (#59). Source files:
 *   - /config              -> app/api/config.py
 *   - /system/status       -> app/api/system.py
 *   - /system/jobs         -> app/api/system.py
 *   - /instruments         -> app/api/instruments.py
 *   - /portfolio           -> app/api/portfolio.py
 *   - /recommendations     -> app/api/recommendations.py
 *   - /audit              -> app/api/audit.py
 *   - /rankings            -> app/api/scores.py
 *
 * Rule: when a backend response_model changes, update this file in the same
 * PR. Drift here breaks every page silently. There is no codegen yet (#59
 * keeps the toolchain minimal); revisit if drift becomes a recurring problem.
 */

// ---------------------------------------------------------------------------
// /config (app/api/config.py)
// ---------------------------------------------------------------------------

export interface RuntimeFlagsResponse {
  enable_auto_trading: boolean;
  enable_live_trading: boolean;
  display_currency: string;
  updated_at: string;
  updated_by: string;
  reason: string;
}

export interface KillSwitchResponse {
  active: boolean;
  activated_at: string | null;
  activated_by: string | null;
  reason: string | null;
}

export interface ConfigResponse {
  app_env: string;
  etoro_env: string;
  runtime: RuntimeFlagsResponse;
  kill_switch: KillSwitchResponse;
}

// ---------------------------------------------------------------------------
// /system/status, /system/jobs (app/api/system.py)
// ---------------------------------------------------------------------------

export type LayerStatus = "ok" | "stale" | "empty" | "error";
export type OverallStatus = "ok" | "degraded" | "down";
export type JobLastStatus = "running" | "success" | "failure" | "skipped" | null;
export type CadenceKind = "hourly" | "daily" | "weekly";

export interface LayerHealthResponse {
  layer: string;
  status: LayerStatus;
  latest: string | null;
  max_age_seconds: number | null;
  age_seconds: number | null;
  detail: string;
}

export interface KillSwitchStateResponse {
  active: boolean;
  activated_at: string | null;
  activated_by: string | null;
  reason: string | null;
}

export interface JobHealthResponse {
  name: string;
  last_status: JobLastStatus;
  last_started_at: string | null;
  last_finished_at: string | null;
  detail: string;
}

export interface SystemStatusResponse {
  checked_at: string;
  overall_status: OverallStatus;
  layers: LayerHealthResponse[];
  jobs: JobHealthResponse[];
  kill_switch: KillSwitchStateResponse;
}

export interface JobOverviewResponse {
  name: string;
  description: string;
  cadence: string;
  cadence_kind: CadenceKind;
  next_run_time: string;
  // Backend currently emits the literal "declared"; will gain "scheduler"
  // when APScheduler is wired (#13). Typed as string so a new value does not
  // silently misrepresent the payload.
  next_run_time_source: string;
  last_status: JobLastStatus;
  last_started_at: string | null;
  last_finished_at: string | null;
  detail: string;
}

export interface JobsListResponse {
  checked_at: string;
  jobs: JobOverviewResponse[];
}

// ---------------------------------------------------------------------------
// /jobs/runs (app/api/jobs.py — issue #13 PR B)
// ---------------------------------------------------------------------------

export type JobRunStatus = "running" | "success" | "failure" | "skipped";

export interface JobRunResponse {
  run_id: number;
  job_name: string;
  started_at: string;
  finished_at: string | null;
  status: JobRunStatus;
  row_count: number | null;
  error_msg: string | null;
}

export interface JobRunsListResponse {
  items: JobRunResponse[];
  // Number of rows in this response — NOT a paginated total. The
  // backend deliberately does not paginate this endpoint; if a
  // future requirement adds pagination, it will introduce a new
  // ``total_matching`` field rather than repurposing ``count``.
  count: number;
  limit: number;
  job_name: string | null;
}

// ---------------------------------------------------------------------------
// /instruments (app/api/instruments.py)
// ---------------------------------------------------------------------------

export interface QuoteSnapshot {
  bid: number;
  ask: number;
  last: number | null;
  spread_pct: number | null;
  quoted_at: string;
}

export interface ExternalIdentifier {
  provider: string;
  identifier_type: string;
  identifier_value: string;
}

export interface InstrumentListItem {
  instrument_id: number;
  symbol: string;
  company_name: string;
  exchange: string | null;
  currency: string | null;
  sector: string | null;
  is_tradable: boolean;
  coverage_tier: number | null;
  latest_quote: QuoteSnapshot | null;
}

export interface InstrumentListResponse {
  items: InstrumentListItem[];
  total: number;
  offset: number;
  limit: number;
}

export interface InstrumentDetail {
  instrument_id: number;
  symbol: string;
  company_name: string;
  exchange: string | null;
  currency: string | null;
  sector: string | null;
  industry: string | null;
  country: string | null;
  is_tradable: boolean;
  first_seen_at: string;
  last_seen_at: string;
  coverage_tier: number | null;
  latest_quote: QuoteSnapshot | null;
  external_identifiers: ExternalIdentifier[];
}

// ---------------------------------------------------------------------------
// /portfolio (app/api/portfolio.py)
// ---------------------------------------------------------------------------

export interface BrokerPositionItem {
  position_id: number;
  is_buy: boolean;
  units: number;
  amount: number;
  open_rate: number;
  open_date_time: string;
  current_price: number | null;
  market_value: number;
  unrealized_pnl: number;
  stop_loss_rate: number | null;
  take_profit_rate: number | null;
  is_tsl_enabled: boolean;
  leverage: number;
  total_fees: number;
}

export interface PositionItem {
  instrument_id: number;
  symbol: string;
  company_name: string;
  open_date: string | null;
  avg_cost: number | null;
  current_price: number | null;
  current_units: number;
  cost_basis: number;
  market_value: number;
  unrealized_pnl: number;
  valuation_source: "quote" | "daily_close" | "cost_basis";
  source: string;
  updated_at: string;
  trades: BrokerPositionItem[];
}

export interface FxRateUsed {
  rate: number;
  quoted_at: string;
}

export interface PortfolioMirrorItem {
  mirror_id: number;
  parent_username: string;
  active: boolean;
  funded: number;
  mirror_equity: number;
  unrealized_pnl: number;
  position_count: number;
  started_copy_date: string;
}

export interface PortfolioResponse {
  positions: PositionItem[];
  mirrors: PortfolioMirrorItem[];
  position_count: number;
  total_aum: number;
  cash_balance: number | null;
  mirror_equity: number;
  display_currency: string;
  fx_rates_used: Record<string, FxRateUsed>;
}

// /portfolio/instruments/:instrumentId — native currency drill-through
export interface NativeTradeItem {
  position_id: number;
  is_buy: boolean;
  units: number;
  amount: number;
  open_rate: number;
  open_date_time: string;
  current_price: number | null;
  market_value: number;
  unrealized_pnl: number;
  stop_loss_rate: number | null;
  take_profit_rate: number | null;
  is_tsl_enabled: boolean;
  leverage: number;
  total_fees: number;
}

export interface InstrumentPositionDetail {
  instrument_id: number;
  symbol: string;
  company_name: string;
  currency: string;
  current_price: number | null;
  total_units: number;
  avg_entry: number | null;
  total_invested: number;
  total_value: number;
  total_pnl: number;
  trades: NativeTradeItem[];
}

// ---------------------------------------------------------------------------
// /recommendations (app/api/recommendations.py)
// ---------------------------------------------------------------------------

export type RecommendationAction = "BUY" | "ADD" | "HOLD" | "EXIT";
export type RecommendationStatus = "proposed" | "approved" | "rejected" | "executed";

export interface RecommendationListItem {
  recommendation_id: number;
  instrument_id: number;
  symbol: string;
  company_name: string;
  // action/status are typed as `str` on the backend (not Literal), so mirror
  // that here. RecommendationAction / RecommendationStatus above are the
  // closed sets the backend currently emits — use them when narrowing.
  action: string;
  status: string;
  rationale: string;
  score_id: number | null;
  model_version: string | null;
  suggested_size_pct: number | null;
  target_entry: number | null;
  cash_balance_known: boolean | null;
  created_at: string;
}

export interface RecommendationsListResponse {
  items: RecommendationListItem[];
  total: number;
  offset: number;
  limit: number;
}

export interface RecommendationDetail {
  recommendation_id: number;
  instrument_id: number;
  symbol: string;
  company_name: string;
  action: string;
  status: string;
  rationale: string;
  score_id: number | null;
  model_version: string | null;
  suggested_size_pct: number | null;
  target_entry: number | null;
  cash_balance_known: boolean | null;
  total_score: number | null;
  created_at: string;
}

// ---------------------------------------------------------------------------
// /audit (app/api/audit.py)
// ---------------------------------------------------------------------------

export type AuditPassFail = "PASS" | "FAIL";
export type AuditStage = "execution_guard" | "order_client";

export interface AuditListItem {
  decision_id: number;
  decision_time: string;
  instrument_id: number | null;
  symbol: string | null;
  company_name: string | null;
  recommendation_id: number | null;
  stage: AuditStage;
  model_version: string | null;
  pass_fail: AuditPassFail;
  explanation: string;
}

export interface AuditDetail {
  decision_id: number;
  decision_time: string;
  instrument_id: number | null;
  symbol: string | null;
  company_name: string | null;
  recommendation_id: number | null;
  stage: AuditStage;
  model_version: string | null;
  pass_fail: AuditPassFail;
  explanation: string;
  evidence_json: Record<string, unknown> | Record<string, unknown>[] | null;
}

export interface AuditListResponse {
  items: AuditListItem[];
  total: number;
  offset: number;
  limit: number;
}

// ---------------------------------------------------------------------------
// /rankings (app/api/scores.py)
// ---------------------------------------------------------------------------

export interface RankingItem {
  instrument_id: number;
  symbol: string;
  company_name: string;
  sector: string | null;
  coverage_tier: number | null;
  rank: number | null;
  rank_delta: number | null;
  total_score: number | null;
  raw_total: number | null;
  quality_score: number | null;
  value_score: number | null;
  turnaround_score: number | null;
  momentum_score: number | null;
  sentiment_score: number | null;
  confidence_score: number | null;
  penalties_json: Record<string, unknown>[] | null;
  explanation: string | null;
  model_version: string;
  scored_at: string;
}

export interface RankingsListResponse {
  items: RankingItem[];
  total: number;
  offset: number;
  limit: number;
  model_version: string;
  scored_at: string | null;
}

// ---------------------------------------------------------------------------
// /rankings/history/{instrument_id} (app/api/scores.py)
// ---------------------------------------------------------------------------

export interface ScoreHistoryItem {
  scored_at: string;
  total_score: number | null;
  raw_total: number | null;
  quality_score: number | null;
  value_score: number | null;
  turnaround_score: number | null;
  momentum_score: number | null;
  sentiment_score: number | null;
  confidence_score: number | null;
  penalties_json: Record<string, unknown>[] | null;
  explanation: string | null;
  rank: number | null;
  rank_delta: number | null;
  model_version: string;
}

export interface ScoreHistoryResponse {
  instrument_id: number;
  items: ScoreHistoryItem[];
}

// ---------------------------------------------------------------------------
// /theses/{instrument_id} (app/api/theses.py)
// ---------------------------------------------------------------------------

export interface ThesisDetail {
  thesis_id: number;
  instrument_id: number;
  thesis_version: number;
  thesis_type: string;
  stance: string;
  confidence_score: number | null;
  buy_zone_low: number | null;
  buy_zone_high: number | null;
  base_value: number | null;
  bull_value: number | null;
  bear_value: number | null;
  break_conditions_json: string[] | null;
  memo_markdown: string;
  critic_json: Record<string, unknown> | null;
  created_at: string;
}

export interface ThesisHistoryResponse {
  instrument_id: number;
  items: ThesisDetail[];
  total: number;
  offset: number;
  limit: number;
}

// ---------------------------------------------------------------------------
// /filings/{instrument_id} (app/api/filings.py)
// ---------------------------------------------------------------------------

export interface FilingItem {
  filing_event_id: number;
  instrument_id: number;
  filing_date: string;
  filing_type: string | null;
  provider: string;
  source_url: string | null;
  primary_document_url: string | null;
  extracted_summary: string | null;
  red_flag_score: number | null;
  created_at: string;
}

export interface FilingsListResponse {
  instrument_id: number;
  symbol: string | null;
  items: FilingItem[];
  total: number;
  offset: number;
  limit: number;
}

// ---------------------------------------------------------------------------
// /news/{instrument_id} (app/api/news.py)
// ---------------------------------------------------------------------------

export interface NewsItem {
  news_event_id: number;
  instrument_id: number;
  event_time: string;
  source: string | null;
  headline: string;
  category: string | null;
  sentiment_score: number | null;
  importance_score: number | null;
  snippet: string | null;
  url: string | null;
}

export interface NewsListResponse {
  instrument_id: number;
  symbol: string | null;
  items: NewsItem[];
  total: number;
  offset: number;
  limit: number;
}

// ---------------------------------------------------------------------------
// /portfolio/copy-trading (app/api/copy_trading.py)
// ---------------------------------------------------------------------------

export interface MirrorPositionItem {
  position_id: number;
  instrument_id: number;
  symbol: string | null;
  company_name: string | null;
  is_buy: boolean;
  units: number;
  amount: number;
  open_rate: number;
  open_conversion_rate: number;
  open_date_time: string;
  current_price: number | null;
  market_value: number;
  unrealized_pnl: number;
}

export interface MirrorSummary {
  mirror_id: number;
  active: boolean;
  initial_investment: number;
  deposit_summary: number;
  withdrawal_summary: number;
  available_amount: number;
  closed_positions_net_profit: number;
  mirror_equity: number;
  position_count: number;
  positions: MirrorPositionItem[];
  started_copy_date: string;
  closed_at: string | null;
}

export interface CopyTraderSummary {
  parent_cid: number;
  parent_username: string;
  mirrors: MirrorSummary[];
  total_equity: number;
}

export interface CopyTradingResponse {
  traders: CopyTraderSummary[];
  total_mirror_equity: number;
  display_currency: string;
}

export interface MirrorDetailResponse {
  parent_username: string;
  mirror: MirrorSummary;
  display_currency: string;
}

// ---------------------------------------------------------------------------
// Budget (mirrors app/api/budget.py)
// ---------------------------------------------------------------------------

export interface BudgetStateResponse {
  cash_balance: number | null;
  deployed_capital: number;
  mirror_equity: number;
  working_budget: number | null;
  estimated_tax_gbp: number;
  estimated_tax_usd: number;
  gbp_usd_rate: number | null;
  cash_buffer_reserve: number;
  available_for_deployment: number | null;
  cash_buffer_pct: number;
  cgt_scenario: "basic" | "higher";
  tax_year: string;
}

export interface CapitalEventResponse {
  event_id: number;
  event_time: string;
  event_type: "injection" | "withdrawal" | "tax_provision" | "tax_release";
  amount: number;
  currency: "USD" | "GBP";
  source: "operator" | "system" | "broker_sync";
  note: string | null;
  created_by: string | null;
}

export interface BudgetConfigResponse {
  cash_buffer_pct: number;
  cgt_scenario: "basic" | "higher";
  updated_at: string;
  updated_by: string;
  reason: string;
}
