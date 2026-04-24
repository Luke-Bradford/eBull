import { apiFetch } from "@/api/client";
import type {
  CandleRange,
  InstrumentCandles,
  InstrumentDetail,
  InstrumentFinancials,
  InstrumentListResponse,
  InstrumentSummary,
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
): Promise<InstrumentDividends> {
  return apiFetch<InstrumentDividends>(
    `/instruments/${encodeURIComponent(symbol)}/dividends`,
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
): Promise<EightKFilingsResponse> {
  const params = new URLSearchParams({ limit: String(limit) });
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

export interface BusinessSection {
  section_order: number;
  section_key: string;
  section_label: string;
  body: string;
  cross_references: BusinessCrossReference[];
}

export interface BusinessSectionsResponse {
  symbol: string;
  source_accession: string | null;
  sections: BusinessSection[];
}

export function fetchBusinessSections(
  symbol: string,
): Promise<BusinessSectionsResponse> {
  return apiFetch<BusinessSectionsResponse>(
    `/instruments/${encodeURIComponent(symbol)}/business_sections`,
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

export function fetchInsiderSummary(symbol: string): Promise<InsiderSummary> {
  return apiFetch<InsiderSummary>(
    `/instruments/${encodeURIComponent(symbol)}/insider_summary`,
  );
}

export function fetchInsiderTransactions(
  symbol: string,
  limit: number = 100,
): Promise<InsiderTransactionsList> {
  const params = new URLSearchParams({ limit: String(limit) });
  return apiFetch<InsiderTransactionsList>(
    `/instruments/${encodeURIComponent(symbol)}/insider_transactions?${params.toString()}`,
  );
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
