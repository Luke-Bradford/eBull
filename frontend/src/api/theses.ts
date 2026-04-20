import { apiFetch } from "@/api/client";
import type {
  GenerateThesisResponse,
  ThesisDetail,
  ThesisHistoryResponse,
} from "@/api/types";

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

export function generateInstrumentThesis(
  symbol: string,
): Promise<GenerateThesisResponse> {
  return apiFetch<GenerateThesisResponse>(
    `/instruments/${encodeURIComponent(symbol)}/thesis`,
    { method: "POST" },
  );
}
