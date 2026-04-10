import { apiFetch } from "@/api/client";
import type { InstrumentListResponse } from "@/api/types";

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
