import { apiFetch } from "@/api/client";
import type {
  CandleRange,
  InstrumentCandles,
  InstrumentDetail,
  InstrumentFinancials,
  InstrumentIntradayCandles,
  InstrumentListResponse,
  InstrumentSummary,
  IntradayInterval,
} from "@/api/types";

export interface InstrumentsQuery {
  search: string | null;
  sector: string | null;
  coverage_tier: number | null;
  exchange: string | null;
  has_dividend: boolean | null;
  offset: number;
  limit: number;
}

export const INSTRUMENTS_PAGE_LIMIT = 50;

export function fetchInstruments(
  query: InstrumentsQuery,
): Promise<InstrumentListResponse> {
  const params = new URLSearchParams();
  if (query.search !== null) params.set("search", query.search);
  if (query.sector !== null) params.set("sector", query.sector);
  if (query.coverage_tier !== null)
    params.set("coverage_tier", String(query.coverage_tier));
  if (query.exchange !== null) params.set("exchange", query.exchange);
  if (query.has_dividend !== null)
    params.set("has_dividend", String(query.has_dividend));
  params.set("offset", String(query.offset));
  params.set("limit", String(query.limit));
  const qs = params.toString();
  return apiFetch<InstrumentListResponse>(`/instruments?${qs}`);
}

// ---------------------------------------------------------------------------
// Dividend + SEC profile endpoints (#426 / #427)
// ---------------------------------------------------------------------------

export interface DividendPeriod {
  period_end_date: string;
  period_type: string;
  fiscal_year: number;
  fiscal_quarter: number | null;
  dps_declared: string | null;
  dividends_paid: string | null;
  reported_currency: string | null;
}

export interface DividendSummary {
  has_dividend: boolean;
  ttm_dps: string | null;
  ttm_dividends_paid: string | null;
  ttm_yield_pct: string | null;
  latest_dps: string | null;
  latest_dividend_at: string | null;
  dividend_streak_q: number;
  dividend_currency: string | null;
}

export interface UpcomingDividend {
  source_accession: string;
  declaration_date: string | null;
  ex_date: string | null;
  record_date: string | null;
  pay_date: string | null;
  dps_declared: string | null;
  currency: string;
}

export interface InstrumentDividends {
  symbol: string;
  summary: DividendSummary;
  history: DividendPeriod[];
  upcoming: UpcomingDividend[];
}

export function fetchInstrumentDividends(
  symbol: string,
  provider?: string,
): Promise<InstrumentDividends> {
  const params = new URLSearchParams();
  if (provider !== undefined) params.set("provider", provider);
  const qs = params.toString();
  const suffix = qs.length > 0 ? `?${qs}` : "";
  return apiFetch<InstrumentDividends>(
    `/instruments/${encodeURIComponent(symbol)}/dividends${suffix}`,
  );
}

export interface FormerName {
  name: string;
  from_: string | null;
  to: string | null;
}

export interface InstrumentSecProfile {
  symbol: string;
  cik: string;
  sic: string | null;
  sic_description: string | null;
  owner_org: string | null;
  description: string | null;
  website: string | null;
  investor_website: string | null;
  ein: string | null;
  lei: string | null;
  state_of_incorporation: string | null;
  state_of_incorporation_desc: string | null;
  fiscal_year_end: string | null;
  category: string | null;
  exchanges: string[];
  former_names: FormerName[];
  has_insider_issuer: boolean | null;
  has_insider_owner: boolean | null;
}

/** Returns null on 404 (profile not seeded yet), rethrows other errors. */
export async function fetchInstrumentSecProfile(
  symbol: string,
): Promise<InstrumentSecProfile | null> {
  try {
    return await apiFetch<InstrumentSecProfile>(
      `/instruments/${encodeURIComponent(symbol)}/sec_profile`,
    );
  } catch (err) {
    const { ApiError } = await import("@/api/client");
    if (err instanceof ApiError && err.status === 404) return null;
    throw err;
  }
}

// ---------------------------------------------------------------------------
// 8-K structured events (#450)
// ---------------------------------------------------------------------------

export interface EightKItem {
  item_code: string;
  item_label: string;
  severity: string | null;
  body: string;
}

export interface EightKExhibit {
  exhibit_number: string;
  description: string | null;
}

export interface EightKFiling {
  accession_number: string;
  document_type: string;
  is_amendment: boolean;
  date_of_report: string | null;
  reporting_party: string | null;
  signature_name: string | null;
  signature_title: string | null;
  signature_date: string | null;
  primary_document_url: string | null;
  items: EightKItem[];
  exhibits: EightKExhibit[];
}

export interface EightKFilingsResponse {
  symbol: string;
  filings: EightKFiling[];
}

export function fetchEightKFilings(
  symbol: string,
  limit: number = 25,
  provider?: string,
): Promise<EightKFilingsResponse> {
  const params = new URLSearchParams({ limit: String(limit) });
  if (provider !== undefined) params.set("provider", provider);
  return apiFetch<EightKFilingsResponse>(
    `/instruments/${encodeURIComponent(symbol)}/eight_k_filings?${params.toString()}`,
  );
}


// ---------------------------------------------------------------------------
// 10-K Item 1 subsection breakdown (#449)
// ---------------------------------------------------------------------------

export interface BusinessCrossReference {
  reference_type: string;
  target: string;
  context: string;
}

export interface BusinessTable {
  order: number;
  headers: string[];
  rows: string[][];
}

export interface BusinessSection {
  section_order: number;
  section_key: string;
  section_label: string;
  body: string;
  cross_references: BusinessCrossReference[];
  tables: BusinessTable[];
}

/**
 * Why `sections` is empty (#648). Populated only when `sections` is
 * empty. Lets the operator UI distinguish "the parser hasn't run yet"
 * from "the parser tried and failed" from "the filing genuinely has
 * no Item 1" — all of which used to render as the same opaque
 * "No 10-K Item 1 on file" empty state.
 */
export interface BusinessSectionsParseStatus {
  state: "not_attempted" | "parse_failed" | "no_item_1" | "sections_pending";
  failure_reason: string | null;
  /** ISO timestamp (UTC). Backoff schedule applies; the next ingester
   * pass after this timestamp will retry. */
  next_retry_at: string | null;
  /** ISO timestamp (UTC) of the most recent parse attempt. */
  last_attempted_at: string | null;
}

export interface BusinessSectionsResponse {
  symbol: string;
  source_accession: string | null;
  /**
   * SEC entity CIK for the instrument. Plumbed through (#563) so the
   * frontend can build direct iXBRL viewer URLs
   * (`cgi-bin/viewer?cik=...&accession_number=...`) instead of falling
   * back to an EDGAR full-text search by accession. NULL for
   * instruments without a primary SEC CIK link.
   */
  cik: string | null;
  sections: BusinessSection[];
  /** #648 — explains WHY sections is empty when it is. NULL when
   * sections has any content. */
  parse_status?: BusinessSectionsParseStatus | null;
}

export function fetchBusinessSections(
  symbol: string,
  accession?: string,
): Promise<BusinessSectionsResponse> {
  const qs = accession !== undefined
    ? `?accession=${encodeURIComponent(accession)}`
    : "";
  return apiFetch<BusinessSectionsResponse>(
    `/instruments/${encodeURIComponent(symbol)}/business_sections${qs}`,
  );
}

export interface TenKHistoryFiling {
  accession_number: string;
  filing_date: string; // ISO yyyy-mm-dd
  filing_type: string; // "10-K" | "10-K/A"
}

export interface TenKHistoryResponse {
  symbol: string;
  filings: TenKHistoryFiling[];
}

export function fetchTenKHistory(symbol: string): Promise<TenKHistoryResponse> {
  return apiFetch<TenKHistoryResponse>(
    `/instruments/${encodeURIComponent(symbol)}/filings/10-k/history`,
  );
}


// ---------------------------------------------------------------------------
// Insider-transactions (Form 4) endpoints (#429)
// ---------------------------------------------------------------------------

export interface InsiderSummary {
  symbol: string;
  open_market_net_shares_90d: string;
  open_market_buy_count_90d: number;
  open_market_sell_count_90d: number;
  total_acquired_shares_90d: string;
  total_disposed_shares_90d: string;
  acquisition_count_90d: number;
  disposition_count_90d: number;
  unique_filers_90d: number;
  latest_txn_date: string | null;
  // Back-compat aliases the API also ships so pre-#458 consumers
  // keep working. Prefer the open_market_* fields in new code.
  net_shares_90d: string;
  buy_count_90d: number;
  sell_count_90d: number;
}

export interface InsiderTransactionDetail {
  accession_number: string;
  txn_row_num: number;
  document_type: string;
  txn_date: string;
  deemed_execution_date: string | null;
  filer_cik: string | null;
  filer_name: string;
  filer_role: string | null;
  security_title: string | null;
  txn_code: string;
  acquired_disposed_code: string | null;
  shares: string | null;
  price: string | null;
  post_transaction_shares: string | null;
  direct_indirect: string | null;
  nature_of_ownership: string | null;
  is_derivative: boolean;
  equity_swap_involved: boolean | null;
  transaction_timeliness: string | null;
  conversion_exercise_price: string | null;
  exercise_date: string | null;
  expiration_date: string | null;
  underlying_security_title: string | null;
  underlying_shares: string | null;
  underlying_value: string | null;
  footnotes: Record<string, string>;
}

export interface InsiderTransactionsList {
  symbol: string;
  rows: InsiderTransactionDetail[];
}

export function fetchInsiderSummary(
  symbol: string,
  provider?: string,
): Promise<InsiderSummary> {
  const params = new URLSearchParams();
  if (provider !== undefined) params.set("provider", provider);
  const qs = params.toString();
  const suffix = qs.length > 0 ? `?${qs}` : "";
  return apiFetch<InsiderSummary>(
    `/instruments/${encodeURIComponent(symbol)}/insider_summary${suffix}`,
  );
}

export function fetchInsiderTransactions(
  symbol: string,
  limit: number = 100,
  provider?: string,
): Promise<InsiderTransactionsList> {
  const params = new URLSearchParams({ limit: String(limit) });
  if (provider !== undefined) params.set("provider", provider);
  return apiFetch<InsiderTransactionsList>(
    `/instruments/${encodeURIComponent(symbol)}/insider_transactions?${params.toString()}`,
  );
}

export interface InstrumentHeadcount {
  symbol: string;
  employees: number;
  period_end_date: string;
  source_accession: string;
}

/** Latest reported employee count from SEC iXBRL DEI cover-page
 *  facts (#551). Returns null on 404 (non-SEC issuer or fact not
 *  ingested yet); rethrows other errors. */
export async function fetchInstrumentEmployees(
  symbol: string,
): Promise<InstrumentHeadcount | null> {
  try {
    return await apiFetch<InstrumentHeadcount>(
      `/instruments/${encodeURIComponent(symbol)}/employees`,
    );
  } catch (err) {
    const { ApiError } = await import("@/api/client");
    if (err instanceof ApiError && err.status === 404) return null;
    throw err;
  }
}

export function fetchInstrumentDetail(
  instrumentId: number,
): Promise<InstrumentDetail> {
  return apiFetch<InstrumentDetail>(`/instruments/${instrumentId}`);
}

export function fetchInstrumentSummary(
  symbol: string,
): Promise<InstrumentSummary> {
  return apiFetch<InstrumentSummary>(
    `/instruments/${encodeURIComponent(symbol)}/summary`,
  );
}

export interface InstrumentFinancialsQuery {
  statement: "income" | "balance" | "cashflow";
  period: "quarterly" | "annual";
}

export function fetchInstrumentFinancials(
  symbol: string,
  query: InstrumentFinancialsQuery,
): Promise<InstrumentFinancials> {
  const params = new URLSearchParams({
    statement: query.statement,
    period: query.period,
  });
  return apiFetch<InstrumentFinancials>(
    `/instruments/${encodeURIComponent(symbol)}/financials?${params.toString()}`,
  );
}

export function fetchInstrumentCandles(
  symbol: string,
  range: CandleRange,
): Promise<InstrumentCandles> {
  const params = new URLSearchParams({ range });
  return apiFetch<InstrumentCandles>(
    `/instruments/${encodeURIComponent(symbol)}/candles?${params.toString()}`,
  );
}

export function fetchInstrumentIntradayCandles(
  symbol: string,
  interval: IntradayInterval,
  count: number,
): Promise<InstrumentIntradayCandles> {
  const params = new URLSearchParams({
    interval,
    count: String(count),
  });
  return apiFetch<InstrumentIntradayCandles>(
    `/instruments/${encodeURIComponent(symbol)}/intraday-candles?${params.toString()}`,
  );
}
