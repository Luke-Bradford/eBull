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
export type CadenceKind =
  | "hourly"
  | "daily"
  | "weekly"
  | "every_n_minutes";

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

// Phase 2.2 — per-ticker research summary
export interface InstrumentIdentity {
  symbol: string;
  display_name: string | null;
  sector: string | null;
  industry: string | null;
  exchange: string | null;
  country: string | null;
  currency: string | null;
  market_cap: string | null;
}

export interface InstrumentPrice {
  current: string | null;
  day_change: string | null;
  day_change_pct: string | null;
  week_52_high: string | null;
  week_52_low: string | null;
  currency: string | null;
}

// Closed set of values emitted in InstrumentKeyStats.field_source.
// Mirrors KeyStatsFieldSource in app/api/instruments.py. yfinance was
// retired in #498/#499 — every key stat is either SEC-derived or
// honestly absent.
//   - "sec_xbrl"                 → computed from XBRL concepts directly
//   - "sec_dividend_summary"     → from instrument_dividend_summary
//                                  (#426); distinct from raw XBRL so an
//                                  audit trail can tell them apart
//   - "unavailable"              → field genuinely absent
//   - "sec_xbrl_price_missing"   → local SEC inputs present but live
//                                  quote absent, so ratio is unresolvable
//                                  (distinct from "unavailable" so UI
//                                  can render a "waiting on price" hint)
export type KeyStatsFieldSource =
  | "sec_xbrl"
  | "sec_dividend_summary"
  | "unavailable"
  | "sec_xbrl_price_missing";

export interface InstrumentKeyStats {
  pe_ratio: string | null;
  pb_ratio: string | null;
  dividend_yield: string | null;
  payout_ratio: string | null;
  roe: string | null;
  roa: string | null;
  debt_to_equity: string | null;
  revenue_growth_yoy: string | null;
  earnings_growth_yoy: string | null;
  field_source?: Record<string, KeyStatsFieldSource> | null;
}

/** One (capability × instrument) cell in the summary response.
 *  Mirrors `app.services.capabilities.CapabilityCell` (#515 PR 3).
 */
export interface CapabilityCell {
  /** Operator-decided source list (possibly empty if no source
   *  picked, possibly multi-source). Closed enum kept loose
   *  here as `string[]` so a future provider added in a follow-up
   *  PR doesn't force a frontend release at the same time. */
  providers: string[];
  /** Per-provider data-presence (keyed identically to providers).
   *  True iff at least one row has been ingested for this
   *  instrument from this provider. */
  data_present: Record<string, boolean>;
}

export interface InstrumentSummary {
  instrument_id: number;
  is_tradable: boolean;
  coverage_tier: number | null;
  identity: InstrumentIdentity;
  price: InstrumentPrice | null;
  key_stats: InstrumentKeyStats | null;
  source: Record<string, string>;
  /** True iff the instrument has a primary SEC CIK in
   *  external_identifiers. Frontend uses this to gate the
   *  remaining SEC-specific panels (SecProfile, BusinessSections)
   *  not yet refactored into provider-agnostic shells. Crypto +
   *  non-US instruments see false. Retired as a shim for the
   *  three capability panels (Dividends / InsiderActivity /
   *  EightKEvents — #515 PR 3b); a follow-up PR removes it
   *  altogether once SecProfile + BusinessSections also land
   *  provider-agnostic shells. */
  has_sec_cik: boolean;
  /** Legacy filings-coverage flag.
   *  Frontend no longer reads this — the Filings tab and
   *  right-rail widget gate via
   *  ``capabilities.filings.data_present`` instead (#515 PR 3b).
   *  Field still ships on the wire for one release; a follow-up
   *  PR drops it from the response model + this interface. */
  has_filings_coverage: boolean;
  /** Per-capability resolution (#515 PR 3). Keyed by capability
   *  name (one of the 11 v1 keys: filings / fundamentals /
   *  dividends / insider / analyst / ratings / esg / ownership /
   *  corporate_events / business_summary / officers). Frontend
   *  renders a panel iff `providers.length > 0` AND any
   *  `data_present` value is true. */
  capabilities: Record<string, CapabilityCell>;
}

// #316 Slice A — daily OHLCV bars (existing daily endpoint contract).
// Note: `1w` is legacy; the chart UI no longer uses it (#601 swapped it
// for `5d` served by the intraday endpoint). Kept on the backend Literal
// so any external consumer that still passes `?range=1w` keeps working.
export type CandleRange =
  | "1w"
  | "1m"
  | "3m"
  | "6m"
  | "ytd"
  | "1y"
  | "5y"
  | "max";

export interface CandleBar {
  date: string;
  open: string | null;
  high: string | null;
  low: string | null;
  close: string | null;
  volume: string | null;
}

export interface InstrumentCandles {
  symbol: string;
  range: CandleRange;
  /** Resolved lookback in days; null when range="max". */
  days: number | null;
  rows: CandleBar[];
}

// #600 — intraday OHLCV bars served live by the eToro provider.
// Distinct from CandleBar: bars carry a UTC ISO timestamp instead of
// a YYYY-MM-DD date. Not persisted in price_daily.
export type IntradayInterval =
  | "OneMinute"
  | "FiveMinutes"
  | "TenMinutes"
  | "FifteenMinutes"
  | "ThirtyMinutes"
  | "OneHour"
  | "FourHours";

export interface IntradayBar {
  /** UTC ISO-8601 timestamp at bar open. */
  timestamp: string;
  open: string | null;
  high: string | null;
  low: string | null;
  close: string | null;
  volume: number | null;
}

export interface InstrumentIntradayCandles {
  symbol: string;
  interval: IntradayInterval;
  /** Number of bars actually returned (not the requested count). */
  count: number;
  /** Always false in v1 — intraday data is not stored in any DB table. */
  persisted: false;
  rows: IntradayBar[];
}

// #601 — chart UI range token (the union the chart buttons render).
// Translates to either a daily range (existing endpoint) or an
// intraday (interval, count) pair via CHART_RANGE_PLAN. The API
// boundary keeps two separate shapes; the chart consumes a unified
// normalised stream.
export type ChartRange =
  | "1d"
  | "5d"
  | "1m"
  | "3m"
  | "6m"
  | "ytd"
  | "1y"
  | "5y"
  | "max";

// Phase 2.3 — financials
export interface InstrumentFinancialRow {
  period_end: string;
  period_type: string;
  values: Record<string, string | null>;
}

export interface InstrumentFinancials {
  symbol: string;
  statement: "income" | "balance" | "cashflow";
  period: "quarterly" | "annual";
  currency: string | null;
  source: "financial_periods" | "unavailable";
  rows: InstrumentFinancialRow[];
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
  /** Held position ids ∪ active-mirror underlying ids. Drives the
   *  page-level LiveQuoteProvider so mirror equity recomputes as
   *  underlyings tick. */
  live_quote_instrument_ids: number[];
}

// /portfolio/rolling-pnl — #315 Phase 2 rolling unrealised P&L
export interface RollingPnlPeriod {
  period: string; // "1d" | "1w" | "1m"
  pnl: number;
  pnl_pct: number | null;
  coverage: number;
}

export interface RollingPnlResponse {
  display_currency: string;
  periods: RollingPnlPeriod[];
}

// /portfolio/value-history — #204 portfolio NAV over time
export type ValueHistoryRange = "1m" | "3m" | "6m" | "1y" | "5y" | "max";

export interface ValueHistoryPoint {
  date: string; // YYYY-MM-DD
  value: number;
}

export interface ValueHistoryResponse {
  display_currency: string;
  range: ValueHistoryRange;
  days: number;
  fx_mode: string; // "live" in v1 — flags whether historical FX was used
  fx_skipped: number; // rows dropped due to missing live FX pair
  points: ValueHistoryPoint[];
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
// /portfolio/orders, /portfolio/positions/{id}/close (app/api/orders.py)
// ---------------------------------------------------------------------------

export type OrderAction = "BUY" | "ADD";

export interface PlaceOrderRequest {
  instrument_id: number;
  action: OrderAction;
  amount: number | null;
  units: number | null;
  stop_loss_rate: number | null;
  take_profit_rate: number | null;
  is_tsl_enabled: boolean;
  leverage: number;
}

export interface ClosePositionRequest {
  units_to_deduct: number | null;
}

export interface OrderResponse {
  order_id: number;
  status: string;
  broker_order_ref: string | null;
  filled_price: number | null;
  filled_units: number | null;
  fees: number;
  explanation: string;
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

// Phase 2.4 — POST /instruments/{symbol}/thesis
export interface GenerateThesisResponse {
  cached: boolean;
  thesis: ThesisDetail;
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
  /**
   * Provider's primary filing identifier (#565). For SEC filings this
   * is the accession number; FilingsPane appends `?accession=...` to
   * the 10-K drilldown so non-latest rows route to their specific
   * filing instead of always landing on the latest.
   */
  accession_number: string | null;
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

// ---------------------------------------------------------------------------
// /coverage (app/api/coverage.py) — admin coverage surface (#268 Chunk H)
// ---------------------------------------------------------------------------

export type FilingsStatus =
  | "analysable"
  | "insufficient"
  | "fpi"
  | "no_primary_sec_cik"
  | "structurally_young"
  | "unknown";

export interface CoverageSummaryResponse {
  checked_at: string;
  analysable: number;
  insufficient: number;
  fpi: number;
  no_primary_sec_cik: number;
  structurally_young: number;
  unknown: number;
  null_rows: number;
  total_tradable: number;
}

export interface InsufficientRow {
  instrument_id: number;
  symbol: string;
  company_name: string | null;
  cik: string | null;
  filings_status: "insufficient" | "structurally_young";
  filings_backfill_attempts: number;
  filings_backfill_last_at: string | null;
  filings_backfill_reason: string | null;
  earliest_sec_filing_date: string | null;
}

export interface InsufficientListResponse {
  checked_at: string;
  rows: InsufficientRow[];
}

// ---------------------------------------------------------------------------
// /sync/layers/v2 (app/api/sync.py — A.5 chunk 0+)
// ---------------------------------------------------------------------------

export type LayerStateStr =
  | "healthy"
  | "running"
  | "retrying"
  | "degraded"
  | "action_needed"
  | "secret_missing"
  | "cascade_waiting"
  | "disabled";

export interface LayerEntry {
  layer: string;
  display_name: string;
  state: LayerStateStr;
  last_updated: string | null;
  plain_language_sla: string;
}

export interface ActionNeededItem {
  root_layer: string;
  display_name: string;
  category:
    | "auth_expired"
    | "rate_limited"
    | "source_down"
    | "schema_drift"
    | "db_constraint"
    | "data_gap"
    | "upstream_waiting"
    | "internal_error";
  operator_message: string;
  operator_fix: string | null;
  self_heal: boolean;
  consecutive_failures: number;
  affected_downstream: string[];
  /**
   * First line of the most recent captured exception
   * (sync_layer_progress.error_message). Populated by #645 forensics.
   * Null when the layer has never recorded a forensic message — older
   * pre-#645 rows stay null until the next failure is recorded.
   */
  error_excerpt?: string | null;
}

export interface SecretMissingItem {
  layer: string;
  display_name: string;
  missing_secret: string;
  operator_fix: string;
}

export interface LayerSummaryV2 {
  layer: string;
  display_name: string;
  last_updated: string | null;
}

export interface CascadeGroup {
  root: string;
  affected: string[];
}

export interface SyncLayersV2Response {
  generated_at: string;
  system_state: "ok" | "catching_up" | "needs_attention";
  system_summary: string;
  action_needed: ActionNeededItem[];
  degraded: LayerSummaryV2[];
  secret_missing: SecretMissingItem[];
  healthy: LayerSummaryV2[];
  disabled: LayerSummaryV2[];
  cascade_groups: CascadeGroup[];
  layers: LayerEntry[];
}

// ---------------------------------------------------------------------------
// /sync/layers/{name}/enabled (app/api/sync.py — A.5 chunk 2)
// ---------------------------------------------------------------------------

export interface LayerEnabledResponse {
  layer: string;
  display_name: string;
  is_enabled: boolean;
  warning: string | null;
}

// ---------------------------------------------------------------------------
// #315 Phase 3 — alerts strip (app/api/alerts.py)
// ---------------------------------------------------------------------------

export type GuardRejectionAction = "BUY" | "ADD" | "HOLD" | "EXIT";

export interface GuardRejection {
  decision_id: number;
  decision_time: string;  // ISO TIMESTAMPTZ
  instrument_id: number | null;
  symbol: string | null;
  action: GuardRejectionAction | null;
  explanation: string;
}

export interface GuardRejectionsResponse {
  alerts_last_seen_decision_id: number | null;
  unseen_count: number;
  rejections: GuardRejection[];
}

// ---------------------------------------------------------------------------
// #396/#401 position alerts (app/api/alerts.py)
// ---------------------------------------------------------------------------

export type PositionAlertType = "sl_breach" | "tp_breach" | "thesis_break";

export interface PositionAlert {
  alert_id: number;
  alert_type: PositionAlertType;
  instrument_id: number;
  symbol: string;
  opened_at: string;
  resolved_at: string | null;
  detail: string;
  current_bid: string | null; // Decimal serialized as string by pydantic
}

export interface PositionAlertsResponse {
  alerts_last_seen_position_alert_id: number | null;
  unseen_count: number;
  alerts: PositionAlert[];
}

// ---------------------------------------------------------------------------
// #397/#402 coverage status drops (app/api/alerts.py)
// ---------------------------------------------------------------------------

export interface CoverageStatusDrop {
  event_id: number;
  instrument_id: number;
  symbol: string;
  changed_at: string;
  old_status: string;
  new_status: string | null;
}

export interface CoverageStatusDropsResponse {
  alerts_last_seen_coverage_event_id: number | null;
  unseen_count: number;
  drops: CoverageStatusDrop[];
}
