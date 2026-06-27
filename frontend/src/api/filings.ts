import { apiFetch } from "@/api/client";
import type { FilingQuarterlyCounts, FilingsListResponse, RedFlagTrend } from "@/api/types";

export interface FetchFilingsOpts {
  readonly filing_type?: string;
}

export function fetchFilings(
  instrumentId: number,
  offset = 0,
  limit = 10,
  opts: FetchFilingsOpts = {},
): Promise<FilingsListResponse> {
  const params = new URLSearchParams({
    offset: String(offset),
    limit: String(limit),
  });
  if (opts.filing_type !== undefined) {
    params.set("filing_type", opts.filing_type);
  }
  return apiFetch<FilingsListResponse>(
    `/filings/${instrumentId}?${params.toString()}`,
  );
}

export function fetchFilingQuarterlyCounts(
  instrumentId: number,
  years = 5,
): Promise<FilingQuarterlyCounts> {
  const params = new URLSearchParams({ years: String(years) });
  return apiFetch<FilingQuarterlyCounts>(
    `/filings/${instrumentId}/quarterly-counts?${params.toString()}`,
  );
}

export function fetchRedFlagTrend(
  instrumentId: number,
  years = 5,
): Promise<RedFlagTrend> {
  const params = new URLSearchParams({ years: String(years) });
  return apiFetch<RedFlagTrend>(
    `/filings/${instrumentId}/red-flag-trend?${params.toString()}`,
  );
}
