import { apiFetch } from "@/api/client";
import type { FilingsListResponse } from "@/api/types";

export function fetchFilings(
  instrumentId: number,
  offset = 0,
  limit = 10,
): Promise<FilingsListResponse> {
  const params = new URLSearchParams({
    offset: String(offset),
    limit: String(limit),
  });
  return apiFetch<FilingsListResponse>(
    `/filings/${instrumentId}?${params.toString()}`,
  );
}
