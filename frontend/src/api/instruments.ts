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
  params.set("offset", String(query.offset));
  params.set("limit", String(query.limit));
  const qs = params.toString();
  return apiFetch<InstrumentListResponse>(`/instruments?${qs}`);
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
