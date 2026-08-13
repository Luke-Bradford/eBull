import { apiFetch } from "@/api/client";
import type { ScoreHistoryResponse } from "@/api/types";

export function fetchScoreHistory(
  instrumentId: number,
  limit = 30,
): Promise<ScoreHistoryResponse> {
  const params = new URLSearchParams({ limit: String(limit) });
  return apiFetch<ScoreHistoryResponse>(
    `/rankings/history/${instrumentId}?${params.toString()}`,
  );
}
