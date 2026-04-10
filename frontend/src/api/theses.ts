import { apiFetch } from "@/api/client";
import type { ThesisDetail, ThesisHistoryResponse } from "@/api/types";

export function fetchLatestThesis(
  instrumentId: number,
): Promise<ThesisDetail> {
  return apiFetch<ThesisDetail>(`/theses/${instrumentId}`);
}

export function fetchThesisHistory(
  instrumentId: number,
  offset = 0,
  limit = 20,
): Promise<ThesisHistoryResponse> {
  const params = new URLSearchParams({
    offset: String(offset),
    limit: String(limit),
  });
  return apiFetch<ThesisHistoryResponse>(
    `/theses/${instrumentId}/history?${params.toString()}`,
  );
}
