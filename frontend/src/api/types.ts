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
export type JobLastStatus = "running" | "success" | "failure" | null;
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

export interface PositionItem {
  instrument_id: number;
  symbol: string;
  company_name: string;
  open_date: string | null;
  avg_cost: number | null;
  current_units: number;
  cost_basis: number;
  market_value: number;
  unrealized_pnl: number;
  updated_at: string;
}

export interface PortfolioResponse {
  positions: PositionItem[];
  position_count: number;
  total_aum: number;
  cash_balance: number | null;
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
