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
  llm_provider: string;
  llm_base_url: string;
  llm_model: string;
  updated_at: string;
  updated_by: string;
  reason: string;
}

// Request body for PATCH /config (ConfigPatchRequest in app/api/config.py).
// Partial: omit any field to leave it unchanged. `updated_by` and `reason`
// are required on every mutation so runtime_config_audit always carries
// attribution. (Trading flags are intentionally NOT exposed here — the
// kill-switch/trading surfaces own those flows.)
export interface ConfigPatchRequest {
  updated_by: string;
  reason: string;
  display_currency?: string;
  llm_provider?: string;
  llm_base_url?: string;
  llm_model?: string;
}

export interface KillSwitchResponse {
  active: boolean;
  activated_at: string | null;
  activated_by: string | null;
  reason: string | null;
}

// Request body for POST /config/kill-switch (KillSwitchRequest in
// app/api/config.py). `reason` and `activated_by` are required non-empty
// on every transition — the backend model_validator rejects blanks (422)
// so the runtime_config_audit row always carries identity + justification.
export interface KillSwitchRequest {
  active: boolean;
  reason: string;
  activated_by: string;
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
export type JobLastStatus =
  "running" | "success" | "failure" | "skipped" | null;
export type CadenceKind =
  "hourly" | "daily" | "weekly" | "monthly" | "yearly" | "every_n_minutes";

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

export type CredentialHealthState =
  "valid" | "untested" | "rejected" | "missing";

export interface CredentialHealthSummary {
  state: CredentialHealthState;
  last_recovered_at: string | null;
  last_error: string | null;
}

export interface SystemStatusResponse {
  checked_at: string;
  overall_status: OverallStatus;
  layers: LayerHealthResponse[];
  jobs: JobHealthResponse[];
  kill_switch: KillSwitchStateResponse;
  credential_health: CredentialHealthSummary;
  // True when the scheduler/worker process is not running (#1508 / C4 —
  // heartbeat table empty/all-stale, i.e. `jobs_process.state == "down"`).
  // When true the Processes page raises a hard-red "Jobs engine not running"
  // banner above the per-row clean-bill/attention summary, because every
  // per-row verdict is stale once the engine stops. Distinct from
  // `overall_status === "down"`, which also fires for kill-switch / layer error.
  engine_down: boolean;
}

export interface JobOverviewResponse {
  name: string;
  // Operator-facing label populated from `ScheduledJob.display_name`.
  // Render `display_name ?? name` — `null` means the job has no
  // dedicated label and falls back to the raw slug.
  display_name: string | null;
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
  // #1689 — the single computed verdict (same `compute_verdict` as the
  // Processes Hub). Render this as the status pill instead of the raw
  // `last_status` tone, so a transient/retrying/restart-reaped/aged-one-shot
  // run is never painted red. `last_status` stays for back-compat.
  health_verdict: HealthVerdict;
  self_healing: boolean;
  verdict_reason: string;
  // Page-scope role (#1530) for the collapsed Manual & backfill split; attempt
  // + next_retry_at (#1509) drive the "attempt N · next HH:MM" retrying label.
  role: ProcessRole;
  attempt: number | null;
  next_retry_at: string | null;
}

export interface JobsListResponse {
  checked_at: string;
  jobs: JobOverviewResponse[];
}

// ---------------------------------------------------------------------------
// ParamMetadata (PR1a #1064 — operator-exposable parameter declarations)
// ---------------------------------------------------------------------------
//
// Mirror of ``app/services/processes/param_metadata.py::ParamMetadata``.
// Drift between the two is a PREVENTION-grade risk — PR2's Advanced
// disclosure renders one form field per entry based on ``field_type``,
// so a contract drift means operators see wrong inputs or no inputs at all.
//
// Round-trip test: ``frontend/src/api/types.test.ts`` covers one canonical
// job's metadata round-tripping through JSON. Full coverage gated by review
// bot reading both files.

export type ParamFieldType =
  | "string"
  | "int"
  | "float"
  | "date"
  | "quarter"
  | "ticker"
  | "cik"
  | "bool"
  | "enum"
  | "multi_enum";

export interface ParamMetadata {
  name: string;
  label: string;
  help_text: string;
  field_type: ParamFieldType;
  default: unknown | null;
  advanced_group: boolean;
  enum_values: readonly string[] | null;
  min_value: number | null;
  max_value: number | null;
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
  // Raw eToro exchangeId (opaque numeric text, e.g. "21"); the filter key.
  // Render `exchange_name`, not this (#1904).
  exchange: string | null;
  // Human exchange label from `exchanges.description`; null when the id has no
  // row yet — fall back to the raw `exchange` (#1904).
  exchange_name: string | null;
  currency: string | null;
  sector: string | null;
  // #1675: real GICS sector + sector-SPDR resolved on-read from the SEC SIC.
  // null for ETFs / non-filers / unmapped SIC. `sector` above is the deprecated
  // opaque 1-9 code.
  gics_sector: string | null;
  sector_spdr: string | null;
  is_tradable: boolean;
  coverage_tier: number | null;
  latest_quote: QuoteSnapshot | null;
  /** #1924: close-to-close day-change from `price_daily` (last two positive
   *  closes) — a SEPARATE source from `latest_quote`. A FRACTION (-0.015 =
   *  -1.5%), fed straight to `formatPct`. `day_change_as_of` is the latest
   *  close's date (ISO), stamped so a stale close reads honestly. Both null
   *  when <2 positive closes exist. */
  day_change_pct: string | null;
  day_change_as_of: string | null;
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
  /** Intraday-chart session-shading profile (#609). Mirrors the backend
   *  `SessionProfile` Literal — drives which session bands the chart
   *  paints. See `classifySession` in `lib/chartFormatters`. */
  session_profile: SessionProfile;
  latest_quote: QuoteSnapshot | null;
  external_identifiers: ExternalIdentifier[];
}

/** Session-shading profile for the intraday chart (#609 Phase A). Mirrors
 *  `app/api/instruments.py::SessionProfile`. */
export type SessionProfile =
  "us_equity" | "us_equity_rth" | "foreign_equity" | "continuous";

// Phase 2.2 — per-ticker research summary
export interface InstrumentIdentity {
  symbol: string;
  display_name: string | null;
  sector: string | null;
  /** #1599: resolved eToro industry name (the bare `sector` is the opaque eToro
   * numeric id). The display fallback when `gics_sector` is null (non-SEC). */
  sector_name: string | null;
  industry: string | null;
  /** #1634: real GICS sector + its sector-SPDR, resolved from the SEC SIC
   * (the bare `sector` is an opaque 1-9 code). null when the instrument has
   * no SIC (ETFs / non-filers) or no confident mapping. */
  gics_sector: string | null;
  sector_spdr: string | null;
  /** Raw eToro numeric exchange id (filter key); NEVER render — use
   * `exchange_name` (#1904/#1955). */
  exchange: string | null;
  /** #1955: human exchange label from `exchanges.description`; the summary
   * header renders this, never the opaque numeric `exchange`. */
  exchange_name: string | null;
  country: string | null;
  currency: string | null;
  market_cap: string | null;
  /** #1665: per-class FLOAT value of THIS instrument's own share class — its
   * FSDS shares × price (GOOGL Class A ≈ $2.15T), a SEPARATE stat from
   * `market_cap` (the whole company, ≈ $4.45T, identical across siblings).
   * Non-null only on curated dual-class issuers where this instrument is a
   * priced per-class leg; null for single-class issuers. */
  class_market_value: string | null;
  /** #819: when set, this instrument is an operational duplicate
   * (e.g. ``AAPL.RTH``) of the named canonical symbol (``AAPL``).
   * The frontend redirects to the canonical symbol's page so
   * chart / ownership / fundamentals render under the security
   * with the actual SEC filings. */
  canonical_symbol: string | null;
}

export interface InstrumentPrice {
  /** Native listing price + currency — the tradable number, primary in the
   *  header. Never flips by quote path (#1906, operator decision 2026-07-04). */
  current: string | null;
  day_change: string | null;
  day_change_pct: string | null;
  /** #1924: date (ISO) of the latest `price_daily` close the day-change is
   *  computed from — stamped so a stale close reads honestly, not as "today".
   *  Null when no day-change is available. */
  day_change_as_of: string | null;
  week_52_high: string | null;
  week_52_low: string | null;
  currency: string | null;
  /** FX-converted companion in the operator's display currency (secondary,
   *  muted). Null when no FX rate is available or native === display. */
  display_current: string | null;
  display_currency: string | null;
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
  /** Intraday-chart session-shading profile (#609). Mirrors the backend
   *  `SessionProfile` Literal. */
  session_profile: SessionProfile;
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
  "1w" | "1m" | "3m" | "6m" | "ytd" | "1y" | "5y" | "max";

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

// ---------------------------------------------------------------------------
// /instruments/{symbol}/peer-comparison (app/api/instruments.py:277-305, #1751)
// ---------------------------------------------------------------------------

/** One radar factor: the instrument's value vs its sector median. */
export interface PeerFactor {
  key: string;
  label: string;
  instrument_value: number | null;
  sector_median: number | null;
  /** # sector members with a non-null value (low n → noisy median). */
  sector_n: number;
  /** True when thin (greyed + ⚠): price-gated (P/E) OR sector coverage <20% (#1836). */
  dev_limited: boolean;
  better_when: "higher" | "lower";
}

/** A sector peer with its factor row (for the heatmap). */
export interface PeerInstrument {
  instrument_id: number;
  symbol: string;
  company_name: string | null;
  size_proxy: number | null;
  factors: Record<string, number | null>;
}

export interface PeerComparison {
  symbol: string;
  instrument_id: number;
  /** Raw SIC division code "1".."9" — no name lookup table. */
  sector: string;
  sector_member_count: number;
  factors: PeerFactor[];
  peers: PeerInstrument[];
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

// ---------------------------------------------------------------------------
// Risk metrics (#591 PR-B/PR-C) — mirrors RiskWindowMetrics / RiskSeries /
// InstrumentRiskMetrics in app/api/instruments.py. Every persisted scalar is a
// Pydantic `Decimal | None` → JSON string | null on the wire (never coerce to
// number until the chart boundary). Statuses pass through verbatim as `str`.
// ---------------------------------------------------------------------------

/** Per-metric quality flag. Persisted as a bare string; this union documents
 *  the closed vocabulary the FE branches on for empty states. */
export type RiskStatus =
  | "ok"
  | "insufficient_history"
  | "partial_window"
  | "benchmark_missing"
  | "benchmark_insufficient_history"
  | "invalid_price_chain"
  | "stale";

export interface RiskWindowMetrics {
  window_key: string;
  cagr: string | null;
  excess_cagr_vs_spy: string | null;
  max_drawdown: string | null;
  current_drawdown: string | null;
  vol_annualized: string | null;
  beta: string | null;
  beta_r2: string | null;
  calmar: string | null;
  skew: string | null;
  excess_kurtosis: string | null;
  var_5: string | null;
  worst_day: string | null;
  best_day: string | null;
  trailing_1m: string | null;
  trailing_3m: string | null;
  trailing_6m: string | null;
  trailing_1y: string | null;
  excess_trailing_1m: string | null;
  excess_trailing_3m: string | null;
  excess_trailing_6m: string | null;
  excess_trailing_1y: string | null;
  n_returns: number | null;
  beta_n_obs: number | null;
  window_days: number | null;
  cagr_status: string | null;
  vol_status: string | null;
  beta_status: string | null;
  drawdown_status: string | null;
  distribution_status: string | null;
  calmar_status: string | null;
  trailing_status: string | null;
  excess_cagr_status: string | null;
}

export interface DrawdownPoint {
  date: string;
  drawdown: string;
}

export interface RollingVolPoint {
  date: string;
  vol: string;
}

export interface HistogramBin {
  lower: string;
  upper: string;
  count: number;
}

export interface BetaScatterPoint {
  spy_return: string;
  inst_return: string;
}

export interface RiskSeries {
  drawdown_curve: DrawdownPoint[];
  rolling_vol: RollingVolPoint[];
  return_histogram: HistogramBin[];
  beta_scatter: BetaScatterPoint[];
  beta: string | null;
  beta_r2: string | null;
}

export interface InstrumentRiskMetrics {
  symbol: string;
  as_of_date: string | null;
  benchmark_symbol: string | null;
  metric_version: string;
  windows: RiskWindowMetrics[];
  series: RiskSeries | null;
}

/** Candidate-vs-current-book risk (#1636). Mirrors PortfolioRelativeRiskResponse
 *  in app/api/instruments.py. Decimals → string | null; figures are fractions,
 *  vols annualized. A current-exposure covariance estimate (today's weights over
 *  past returns), NOT realized book history. */
export type PortfolioRiskStatus =
  | "ok"
  | "empty_book"
  | "book_history_unavailable"
  | "insufficient_history"
  | "single_holding_is_candidate";

export interface PortfolioRelativeRisk {
  symbol: string;
  as_of_date: string | null;
  status: PortfolioRiskStatus;
  holdings_count: number;
  already_held: boolean;
  current_weight: string | null;
  portfolio_beta: string | null;
  correlation: string | null;
  candidate_vol: string | null;
  portfolio_vol: string | null;
  marginal_risk_contribution: string | null;
  n_obs: number;
}

// #601 — chart UI range token (the union the chart buttons render).
// Translates to either a daily range (existing endpoint) or an
// intraday (interval, count) pair via CHART_RANGE_PLAN. The API
// boundary keeps two separate shapes; the chart consumes a unified
// normalised stream.
export type ChartRange =
  "1d" | "5d" | "1m" | "3m" | "6m" | "ytd" | "1y" | "5y" | "max";

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

// FCF yield trend (#671). Decimals → string | null on the wire (coerce at the
// chart boundary). `suppressed_reason` set → fail-closed (multi-class cap
// distortion #1662 / cross-currency FCF↔price); `points` is then empty.
export interface FcfYieldPoint {
  period_end: string;
  period_type: string;
  fcf_ttm: string | null;
  market_cap: string | null;
  fcf_yield_pct: string | null;
  price: string | null;
  price_as_of: string | null;
}

export interface FcfYieldSeries {
  symbol: string;
  suppressed_reason: "multiclass" | "currency_mismatch" | null;
  points: FcfYieldPoint[];
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

// Buy/sell chart marker (#1594). Sourced from the trade_events ledger:
// an open event is a BUY, a close event is a SELL — same basis as the
// value line, so markers and the curve never disagree.
export interface ValueHistoryEvent {
  date: string; // YYYY-MM-DD
  symbol: string;
  side: "BUY" | "SELL";
  units: number;
  source: "open" | "close";
}

export interface ValueHistoryResponse {
  display_currency: string;
  range: ValueHistoryRange;
  days: number;
  fx_mode: string; // "historical" (#1594 PR-B) — per-day ECB FX from fx_rates_daily
  fx_skipped: number; // distinct FX pairs dropped (no dated rate on/before a day)
  // Earliest date cash_ledger has a row; before it the cash side is
  // incomplete (a data limit, not a bug). null when the ledger is empty.
  cash_tracking_since: string | null;
  points: ValueHistoryPoint[];
  events: ValueHistoryEvent[];
}

// /portfolio/activity — broker-observed trade ledger (#1593 PR-2).
// fees / realized_pnl are FX-converted to ActivityResponse.display_currency
// (#1906); price is in the instrument's NATIVE currency (unrelated to
// account currency). symbol null = instrument absent from the current
// universe (deep history) — render `#${etoro_instrument_id}`.
export interface ActivityEventItem {
  event_id: number;
  position_id: number;
  event_kind: "open" | "close";
  side: "buy" | "sell";
  symbol: string | null;
  etoro_instrument_id: number;
  units: number;
  price: number | null;
  executed_at: string;
  fees: number | null;
  realized_pnl: number | null;
  holding_period_days: number | null; // closes only; fractional days
  source: "etoro_sync" | "etoro_history";
  is_mirror: boolean;
}

export interface ActivityResponse {
  events: ActivityEventItem[];
  total: number; // rows matching the filter; events capped at `limit`
  include_mirrors: boolean;
  display_currency: string; // currency of every event's fees / realized_pnl
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

export type RecommendationAction =
  "BUY" | "ADD" | "HOLD" | "EXIT" | "CONSIDERED";
export type RecommendationStatus =
  | "proposed"
  | "approved"
  | "rejected"
  | "executed"
  | "execution_pending"
  | "execution_failed"
  | "timing_deferred"
  | "timing_expired"
  | "cancelled"
  | "considered";

// Data-completeness tier (#1815 §4 / #1820): how well-evidenced the instrument
// is. `insufficient_data` (C<0.40) caps the action layer at HOLD.
export type CompletenessTier = "insufficient_data" | "thin_data" | "full";

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
  // Data-completeness from the linked score (#1820); null for rows with no
  // score_id or a pre-migration score row.
  data_completeness: number | null;
  completeness_tier: string | null;
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
  data_completeness: number | null;
  completeness_tier: string | null;
  created_at: string;
}

// ---------------------------------------------------------------------------
// /audit (app/api/audit.py)
// ---------------------------------------------------------------------------

// decision_audit.stage / .pass_fail are written by six independent subsystems
// (#1808). These unions enumerate the known vocabulary for the FILTER inputs
// (AuditQuery) + the label/tone maps only. The RESPONSE fields below are typed
// `string` to mirror the BE (which sends them as bare `str`) — the audit display
// must render any value a writer logged, so callers must never assume a closed
// set. The row renderer falls back to the raw string for unknown values.
export type AuditPassFail = "PASS" | "FAIL" | "KICK" | "RETRY" | "DEFER";
export type AuditStage =
  | "execution_guard"
  | "order_client"
  | "manual_order"
  | "liveness_kick"
  | "retry_backoff"
  | "entry_timing";

export interface AuditListItem {
  decision_id: number;
  decision_time: string;
  instrument_id: number | null;
  symbol: string | null;
  company_name: string | null;
  recommendation_id: number | null;
  stage: string;
  model_version: string | null;
  pass_fail: string;
  explanation: string;
}

export interface AuditDetail {
  decision_id: number;
  decision_time: string;
  instrument_id: number | null;
  symbol: string | null;
  company_name: string | null;
  recommendation_id: number | null;
  stage: string;
  model_version: string | null;
  pass_fail: string;
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
  // #1675: real GICS sector resolved on-read from the SEC SIC (null for
  // ETFs / non-filers / unmapped SIC). `sector` is the deprecated opaque code.
  gics_sector: string | null;
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
  // #1825: data-completeness surfaced so high-ranked thin-coverage names are
  // visibly flagged (on `scores` since #1820).
  data_completeness: number | null;
  completeness_tier: string | null;
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
// /rankings/coverage (app/api/scores.py) — ranked-vs-universe denominator (#1918)
// ---------------------------------------------------------------------------

export interface RankingsCoverageBucket {
  reason: string;
  label: string;
  count: number;
}

export interface RankingsCoverage {
  model_version: string;
  scored_at: string | null;
  // Tradable universe.
  universe: number;
  // Exactly the count `GET /rankings` returns unfiltered for this run.
  ranked: number;
  // MECE breakdown of (universe - ranked) by exclusion cause.
  not_ranked: RankingsCoverageBucket[];
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
// /rankings/verdict/{instrument_id} (app/api/scores.py — #1824, P3 of #1815)
//
// The Instrument Analytical Record (IAR, `iar_v1`) shipped by #1823 as the
// nullable JSONB `scores.analytics_json`. EVERY field below is OPTIONAL, not
// merely nullable: the real branches are sparse — a suppressed F/Z emits only
// `{suppressed, reason}` (no `score`/`components`), a missing-input F/Z emits
// `{score:null, reason}`, a standalone `peer_grade` omits `peer_key`/`peer_n`.
// The renderer guards each field's PRESENCE, never assumes a key. Absent /
// null signals render honestly ("not available") — never neutral-filled.
// ---------------------------------------------------------------------------

export interface IarPiotroski {
  score?: number | null;
  components_available?: number;
  band?: string | null;
  components?: Record<string, boolean>;
  suppressed?: boolean;
  reason?: string | null;
}

export interface IarAltmanZ {
  z?: number | null;
  band?: string | null;
  suppressed?: boolean;
  reason?: string | null;
}

export interface IarPositioningSignal {
  signal?: number | null;
  reason?: string | null;
  caveat?: string | null;
  source?: string;
  asof?: string;
  // insider
  net_shares?: number | null;
  shares_outstanding?: number;
  // 13F
  delta_shares_pct?: number;
  // short interest
  short_pct?: number;
  days_to_cover?: number;
  falling?: boolean;
}

export interface IarPeerFamily {
  absolute?: number;
  percentile?: number | null;
  hybrid?: number;
}

export interface IarPeerGrade {
  peer_key?: string;
  peer_n?: number;
  basis?: string;
  reason?: string;
  families?: Record<string, IarPeerFamily>;
}

export interface IarAnalytics {
  schema?: string;
  piotroski?: IarPiotroski;
  altman_z?: IarAltmanZ;
  positioning?: {
    insider_net_90d?: IarPositioningSignal;
    inst_13f_qoq?: IarPositioningSignal;
    short_interest?: IarPositioningSignal;
  };
  peer_grade?: IarPeerGrade;
}

export interface VerdictScore {
  scored_at: string;
  model_version: string;
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
  data_completeness: number | null;
  completeness_tier: string | null;
  penalties_json: Record<string, unknown>[] | null;
  explanation: string | null;
  analytics_json: IarAnalytics | null;
}

export interface VerdictResponse {
  instrument_id: number;
  score: VerdictScore | null;
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
  /** Server-computed staleness (#1902 single source: find_stale_instruments).
   *  Populated only on the latest-thesis GET; null on history/POST payloads. */
  is_stale?: boolean | null;
  stale_reason?: string | null;
}

export interface ThesisHistoryResponse {
  instrument_id: number;
  items: ThesisDetail[];
  total: number;
  offset: number;
  limit: number;
}

// #1902 — GET /theses (library: latest thesis per instrument + context).
// Thesis fields are null on the "held but no thesis yet" rows the library
// prepends so the dashboard staleness alert always lands on visible rows.
export interface ThesisLibraryItem {
  instrument_id: number;
  symbol: string;
  company_name: string;
  thesis_id: number | null;
  thesis_version: number | null;
  thesis_type: string | null;
  stance: string | null;
  confidence_score: number | null;
  buy_zone_low: number | null;
  buy_zone_high: number | null;
  created_at: string | null;
  critic_verdict: string | null;
  /** null = fresh, or outside refresh scope (not tradable / not analysable). */
  stale_reason: string | null;
  is_held: boolean;
  latest_score: number | null;
  latest_rank: number | null;
  run_status: string | null; // 'running' | 'ok' | 'failed' (DB CHECK) | null pre-#1919
  run_error: string | null;
  run_trigger: string | null;
  run_started_at: string | null;
}

export interface ThesisLibraryResponse {
  items: ThesisLibraryItem[];
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

/** Parsed Form 12b-25 fields for an NT 10-K / NT 10-Q row (#1015).
 *  Mirrors app/api/filings.py NtNoticeSummary. */
export interface NtNoticeSummary {
  late_form: string; // '10-K' | '10-Q'
  period_of_report: string | null;
  grace_period_days: number; // 15 for 10-K, 5 for 10-Q (Rule 12b-25(b))
  reason_excerpt: string | null;
  results_change_anticipated: boolean | null;
}

/** Parsed PRE 14A / PRER14A meeting-agenda proposal signal (#1892).
 *  Mirrors app/api/filings.py Pre14aSignalSummary. */
export interface Pre14aSignalSummary {
  proposal_count: number;
  reverse_stock_split_proposal: boolean;
  authorized_share_increase_proposal: boolean;
  say_on_pay_advisory_vote: boolean;
  agenda_items: string[];
}

/** Parsed 424B cover offering (Reg S-K Item 501(b)(3)) — #1816.
 *  Mirrors app/api/filings.py OfferingSummary. Every money field is
 *  nullable: NULL means the cover presentation was not resolvable
 *  (percent-of-principal notes, resale shelves, non-tabular covers) —
 *  never a guessed value. */
export interface OfferingSummary {
  subtype: string;
  is_issuer_offering: boolean | null;
  price_per_unit: number | null;
  unit_label: string | null;
  aggregate_offering_amount: number | null;
  underwriting_discount: number | null;
  net_proceeds_to_issuer: number | null;
  proceeds_to_selling_holders: number | null;
  currency: string;
  security_type: string | null;
}

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
  /** Form 12b-25 detail for NT 10-K / NT 10-Q rows (#1015); null otherwise. */
  nt_notice: NtNoticeSummary | null;
  /** Meeting-agenda proposal signal for PRE 14A / PRER14A rows (#1892);
   *  null otherwise. */
  pre14a_signal: Pre14aSignalSummary | null;
  /** Parsed 424B cover offering for tier-1 424B rows (#1816); null otherwise. */
  offering: OfferingSummary | null;
}

export interface FilingsListResponse {
  instrument_id: number;
  symbol: string | null;
  items: FilingItem[];
  total: number;
  offset: number;
  limit: number;
}

// Per-(quarter, filing_type) counts for the filings-analytics drill (#592).
export interface FilingQuarterCount {
  quarter: string; // "YYYY-Qn"
  filing_type: string;
  count: number;
}

export interface FilingQuarterlyCounts {
  instrument_id: number;
  symbol: string | null;
  counts: FilingQuarterCount[];
}

export interface RedFlagTrendPoint {
  quarter: string; // "YYYY-Qn"
  avg_score: number; // mean red_flag_score over scored filings that quarter
  n: number; // number of scored (non-null) filings in the quarter
}

export interface RedFlagTrend {
  instrument_id: number;
  symbol: string | null;
  points: RedFlagTrendPoint[];
}

// #1754 calendar-of-events
export type CalendarScope = "portfolio" | "watchlist" | "all";
export type MarketDayType = "open" | "half_day" | "closed" | "not_modelled";

export interface MarketStatusDay {
  date: string; // YYYY-MM-DD
  day_type: MarketDayType;
  reason: string | null; // why non-open (holiday / "Weekend"); null on open days
}

export interface MarketStatusRow {
  profile: SessionProfile;
  label: string;
  timezone: string;
  holidays_modelled: boolean;
  week: MarketStatusDay[];
}

export interface UpcomingExDividend {
  symbol: string;
  instrument_id: number;
  ex_date: string; // YYYY-MM-DD
  pay_date: string | null;
}

export interface UpcomingExpectedFiling {
  symbol: string;
  instrument_id: number;
  filing_type: string; // "10-Q" | "10-K"
  window_start: string; // YYYY-MM-DD — expected-window start (not an exact date)
  window_end: string; // YYYY-MM-DD
}

export interface CalendarEvents {
  scope: CalendarScope;
  as_of: string; // YYYY-MM-DD
  market_status: MarketStatusRow[];
  ex_dividends: UpcomingExDividend[];
  expected_filings: UpcomingExpectedFiling[];
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

/** A copied position the trader closed while we copied them (#1927).
 *  Surfaced as an exit event, not a valuation — we hold entry size +
 *  observed-close time but NOT the trader's exit price, so there is no
 *  current_price / realized_pnl. `amount` is in display currency;
 *  `open_rate` is intentionally absent. Mirrors
 *  app/api/copy_trading.py::MirrorClosedPositionItem. */
export interface MirrorClosedPositionItem {
  position_id: number;
  instrument_id: number;
  symbol: string | null;
  company_name: string | null;
  is_buy: boolean;
  units: number;
  amount: number;
  open_date_time: string;
  closed_detected_at: string;
}

export interface MirrorDetailResponse {
  parent_username: string;
  mirror: MirrorSummary;
  closed_positions: MirrorClosedPositionItem[];
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

// Capability-override drift (#531). Wraps GET /admin/capability-overrides —
// exchange rows whose ``capabilities`` JSONB diverges from the migration-071
// seed default for their asset_class.
export interface CapabilityCellDiff {
  capability: string;
  seed_providers: string[];
  current_providers: string[];
}

export interface ExchangeOverrideRow {
  exchange_id: string;
  exchange_name: string | null;
  asset_class: string | null;
  diffs: CapabilityCellDiff[];
}

export interface OverridesListResponse {
  checked_at: string;
  total_overrides: number;
  rows: ExchangeOverrideRow[];
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
    | "master_key_missing"
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
  decision_time: string; // ISO TIMESTAMPTZ
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

// ---------------------------------------------------------------------------
// #1922 rank moves on held instruments (app/api/alerts.py)
// ---------------------------------------------------------------------------

export interface RankMove {
  score_id: number;
  instrument_id: number;
  symbol: string;
  scored_at: string;
  rank: number;
  rank_delta: number; // prior_rank - new_rank: positive = moved up the board
}

export interface RankMovesResponse {
  alerts_last_seen_rank_event_id: number | null;
  unseen_count: number;
  moves: RankMove[];
}

// ---------------------------------------------------------------------------
// #1902 thesis-staleness snapshot (app/api/alerts.py)
// ---------------------------------------------------------------------------

/** Standing-condition snapshot — no cursor/seen semantics. The card clears
 *  when the thesis regenerates, not when the operator acknowledges it. */
export interface ThesisStalenessItem {
  instrument_id: number;
  symbol: string;
  reason: string; // find_stale_instruments StaleReason (open string)
  latest_thesis_at: string | null; // null = held instrument has no thesis at all
}

export interface ThesisStalenessResponse {
  items: ThesisStalenessItem[];
}

// ---------------------------------------------------------------------------
// #1076 / #1064 admin control hub (app/api/processes.py)
// ---------------------------------------------------------------------------
//
// Mirrors the Pydantic response models in app/api/processes.py at PR4
// (commit f6a9ac4). Drift from this shape silently breaks the
// ProcessesTable; keep the two in sync — see api-shape-and-types.md.

export type ProcessLane =
  | "setup"
  | "universe"
  | "candles"
  | "sec"
  | "ownership"
  | "fundamentals"
  | "ops"
  | "ai";

export type ProcessMechanism = "bootstrap" | "scheduled_job" | "ingest_sweep";

/**
 * #1530 C7 — page-scope role. Mirrors backend
 * `app/services/processes/__init__.py::ProcessRole`. The Processes page
 * shows only `steady_state` jobs (the ones that keep the system current)
 * in its main view; `bootstrap` / `backfill` rows (run at install or
 * manually) fold into a separate collapsed "Bootstrap & backfill" section.
 */
export type ProcessRole = "steady_state" | "bootstrap" | "backfill";

export type ProcessStatus =
  | "idle"
  | "pending_first_run"
  | "running"
  | "ok"
  | "failed"
  | "pending_retry"
  | "cancelled"
  | "disabled";

/**
 * #1512 — single computed health verdict that collapses the two
 * orthogonal axes (`status` + `stale_reasons`) into one signal. The
 * main Processes row renders this pill, not the raw axes, so
 * contradictory combos ("ok + schedule missed") are impossible. Derived
 * by `app/services/processes/health_verdict.py::compute_verdict`.
 */
export type HealthVerdict =
  | "current"
  | "working"
  | "self_healing"
  | "attention"
  // #1689 — muted: an aged, exhausted one-shot (bootstrap/backfill) failure.
  // Folds into the collapsed Manual & backfill section; not a steady-state red.
  | "stale_manual"
  // #1831 — grey: disabled by the global kill switch. The halt is the normal
  // unattended-loop state, so a paused job is NOT a "problem" (the banner
  // conveys the halt); a genuinely-failed halted job still reads `attention`.
  | "paused";

export type ProcessRunStatus =
  "success" | "failure" | "partial" | "cancelled" | "skipped";

export type CursorKind =
  | "filed_at"
  | "accession"
  | "instrument_offset"
  | "stage_index"
  | "epoch"
  | "atom_etag";

export interface ErrorClassSummaryResponse {
  error_class: string;
  count: number;
  last_seen_at: string;
  sample_message: string;
  sample_subject: string | null;
}

export interface ProcessRunSummaryResponse {
  run_id: number;
  started_at: string;
  finished_at: string;
  duration_seconds: number;
  rows_processed: number | null;
  rows_skipped_by_reason: Record<string, number>;
  rows_errored: number;
  status: ProcessRunStatus;
  cancelled_by_operator_id: string | null;
}

export interface ActiveRunSummaryResponse {
  run_id: number;
  started_at: string;
  rows_processed_so_far: number | null;
  progress_units_done: number | null;
  progress_units_total: number | null;
  last_progress_at: string | null;
  is_cancelling: boolean;
}

/**
 * Operator-amendment §A1 four-case stale model (PR8 / #1083).
 * Multiple reasons can fire on one row simultaneously; empty array
 * means the row is not stale.
 */
export type StaleReason =
  "schedule_missed" | "watermark_gap" | "queue_stuck" | "mid_flight_stuck";

export interface ProcessWatermarkResponse {
  cursor_kind: CursorKind;
  cursor_value: string;
  human: string;
  last_advanced_at: string;
}

export interface ProcessRowResponse {
  process_id: string;
  display_name: string;
  lane: ProcessLane;
  mechanism: ProcessMechanism;
  // #1530 C7 — page-scope role (steady_state | bootstrap | backfill).
  // The main Processes view shows only steady_state rows; bootstrap /
  // backfill rows fold into the collapsed "Bootstrap & backfill" section.
  role: ProcessRole;
  status: ProcessStatus;
  last_run: ProcessRunSummaryResponse | null;
  active_run: ActiveRunSummaryResponse | null;
  cadence_human: string;
  cadence_cron: string | null;
  next_fire_at: string | null;
  watermark: ProcessWatermarkResponse | null;
  can_iterate: boolean;
  can_full_wash: boolean;
  can_cancel: boolean;
  last_n_errors: ErrorClassSummaryResponse[];
  stale_reasons: StaleReason[];
  // #1512 — single computed health verdict + inline reason. The main
  // row renders `health_verdict`; `status` + `stale_reasons` stay on the
  // payload for the drill-in. `verdict_reason` is the inline explanation
  // (folds #1230 — reason visible without hover).
  health_verdict: HealthVerdict;
  self_healing: boolean;
  verdict_reason: string;
  // PR2 #1064 — operator-exposable params for the drill-in Advanced
  // disclosure. Empty list for bootstrap + ingest_sweep mechanisms.
  // Non-empty only for scheduled jobs that declare
  // ``ScheduledJob.params_metadata`` (e.g. sec_13f_quarterly_sweep).
  params_metadata: ParamMetadata[];
  // PR4 #1082 — operator-facing description for the ⓘ tooltip. Empty
  // when the registry entry has no description; the FE hides the
  // icon on empty rather than showing a blank popover.
  description: string;
}

export interface ProcessListResponse {
  rows: ProcessRowResponse[];
  partial: boolean;
}

export type TriggerMode = "iterate" | "full_wash";
export type CancelMode = "cooperative" | "terminate";

export interface TriggerRequestBody {
  mode: TriggerMode;
}

export interface TriggerResponse {
  request_id: number | null;
  mode: TriggerMode;
}

export interface CancelRequestBody {
  mode: CancelMode;
}

export interface CancelResponse {
  target_run_kind: "bootstrap_run" | "job_run" | "sync_run";
  target_run_id: number;
}

// Reasons emitted by app/api/processes.py 409 paths. The FE renders one
// tooltip per reason — anything else falls back to a generic copy.
export type TriggerConflictReason =
  | "kill_switch_active"
  | "bootstrap_already_running"
  | "bootstrap_state_missing"
  | "bootstrap_not_resumable"
  | "iterate_already_pending"
  | "full_wash_already_pending"
  | "active_run_in_progress"
  | "shared_source_active_run"
  | "shared_source_full_wash_pending"
  | "no_active_run"
  | "stop_already_pending"
  // PR6 (#1078) — ingest_sweep rows are READ-ONLY; trigger / cancel
  // surface these reasons to point the operator at the underlying
  // scheduled job.
  | "trigger_not_supported"
  | "cancel_not_supported"
  // PR1b-2 (#1064) — universal bootstrap-state gate. Emitted by the
  // jobs-process listener (NOT a synchronous API 409) when a manual
  // job request is rejected because bootstrap_state.status !=
  // 'complete' and no override flag was set. Surfaces to the operator
  // via the rejected pending_job_requests row's error_msg.
  | "bootstrap_not_complete"
  // #1139 — bootstrap "Re-run failed" (iterate) preconditions, raised as
  // synchronous 409s by app/api/processes.py::_apply_bootstrap_iterate_reset.
  // `bootstrap_not_resettable`: singleton status not in {partial_error,
  // cancelled}. `bootstrap_no_failed_stages`: latest run had no failed
  // stages to iterate (the common "Re-run failed on a clean bootstrap" case).
  | "bootstrap_not_resettable"
  | "bootstrap_no_failed_stages";

// ---------------------------------------------------------------------------
// Orchestrator DAG drill-in (#1078, umbrella #1064 — PR6)
// ---------------------------------------------------------------------------
//
// Mirrors app/api/processes.py::OrchestratorDagResponse. Only used on
// the /admin/processes/orchestrator_full_sync detail page; the fetch is
// gated on (process_id === "orchestrator_full_sync") AND (tab === "dag")
// so non-orchestrator detail pages never hit the endpoint.

export type OrchestratorSyncRunStatus =
  "running" | "complete" | "partial" | "failed" | "cancelled";

export type OrchestratorLayerStatus =
  | "pending"
  | "running"
  | "complete"
  | "failed"
  | "skipped"
  | "partial"
  | "cancelled";

export interface OrchestratorDagSyncRunResponse {
  sync_run_id: number;
  scope: string;
  scope_detail: string | null;
  trigger: string;
  started_at: string;
  finished_at: string | null;
  status: OrchestratorSyncRunStatus;
  layers_planned: number;
  layers_done: number;
  layers_failed: number;
  layers_skipped: number;
  error_category: string | null;
  cancel_requested_at: string | null;
}

export interface OrchestratorDagLayerResponse {
  name: string;
  display_name: string;
  tier: number | null;
  status: OrchestratorLayerStatus;
  started_at: string | null;
  finished_at: string | null;
  items_total: number | null;
  items_done: number | null;
  row_count: number | null;
  error_category: string | null;
  skip_reason: string | null;
  error_message: string | null;
}

export interface OrchestratorDagResponse {
  sync_run: OrchestratorDagSyncRunResponse | null;
  layers: OrchestratorDagLayerResponse[];
}

// ---------------------------------------------------------------------------
// Bootstrap timeline drill-in (#1080, umbrella #1064 — PR7)
// ---------------------------------------------------------------------------
//
// Mirrors app/api/processes.py::BootstrapTimelineResponse. Only used on
// the /admin/processes/bootstrap detail page; the fetch is gated on
// (process_id === "bootstrap") AND (tab === "timeline") so non-bootstrap
// detail pages never hit the endpoint.

export type BootstrapRunStatus =
  "running" | "complete" | "partial_error" | "cancelled";

export type BootstrapStageStatus =
  | "pending"
  | "running"
  | "success"
  | "error"
  | "skipped"
  | "blocked"
  // PR3c #1093: operator-cancelled mid-run. Distinct from ``error`` so
  // the Timeline can tone gray (operator-driven termination) instead
  // of red (genuine failure). Mirrors ``app/services/bootstrap_state.py``
  // sql/142 CHECK constraint extension.
  | "cancelled";

export interface BootstrapTimelineArchiveResponse {
  archive_name: string;
  rows_written: number;
  rows_skipped_by_reason: Record<string, number>;
  completed_at: string;
}

export interface BootstrapTimelineStageResponse {
  stage_key: string;
  display_name: string;
  stage_order: number;
  lane: string;
  job_name: string;
  status: BootstrapStageStatus;
  started_at: string | null;
  completed_at: string | null;
  last_error: string | null;
  rows_processed: number | null;
  processed_count: number;
  target_count: number | null;
  // #1409 P5 — live-timeline fields (server-computed). `last_progress_at`
  // is the per-stage heartbeat. `rate` is rows/sec, null when not
  // measurable (processed_count 0 / no window). `eta_seconds` is the
  // projected seconds-to-target, null when target unknown or already
  // met (no fake 100% / negative ETA). `heartbeat_age_seconds` is
  // now()−last_progress_at on the DB clock. `is_stale` is true only for
  // a running stage whose heartbeat exceeds the 1800s bootstrap
  // threshold — "slow but alive" vs "wedged".
  last_progress_at: string | null;
  rate: number | null;
  eta_seconds: number | null;
  heartbeat_age_seconds: number | null;
  is_stale: boolean;
  // #1273 PR2 — operator-readable cohort-definition fingerprint. Set
  // by `set_stage_target` at stage entry; null on legacy rows and on
  // stages that never instrument. Rendered as a `title=` tooltip on
  // the progress-bar wrapper.
  target_cohort_fingerprint: string | null;
  archives: BootstrapTimelineArchiveResponse[];
  // #1140 Task C — set when stage finished `success` but its
  // rows_processed fell short of a strict-gate capability floor it
  // provides. Frontend renders an amber chip + tooltip alongside the
  // success tick when present.
  warning: string | null;
}

export interface BootstrapTimelineRunResponse {
  run_id: number;
  status: BootstrapRunStatus;
  triggered_at: string;
  completed_at: string | null;
  cancel_requested_at: string | null;
  // #1140 Task C — derived: true iff any stage in the run carries a
  // non-null `warning`. Frontend renders an amber dot beside a
  // `complete` status when set; suppressed for `partial_error` runs
  // (the red signal is louder).
  has_warnings: boolean;
}

export interface BootstrapTimelineResponse {
  run: BootstrapTimelineRunResponse | null;
  stages: BootstrapTimelineStageResponse[];
}
