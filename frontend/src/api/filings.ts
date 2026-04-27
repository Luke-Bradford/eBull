import { apiFetch } from "@/api/client";
import type { FilingsListResponse } from "@/api/types";

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
