import { apiFetch } from "@/api/client";
import type {
  GenerateThesisResponse,
  ThesisDetail,
  ThesisHistoryResponse,
  ThesisLibraryResponse,
} from "@/api/types";

// #1902 — GET /theses (latest thesis per instrument + display context)
export interface ThesesLibraryParams {
  heldOnly?: boolean;
  stale?: boolean;
  stance?: string;
  offset?: number;
  limit?: number;
}

export function fetchThesesLibrary(
  params: ThesesLibraryParams = {},
): Promise<ThesisLibraryResponse> {
  const search = new URLSearchParams();
  if (params.heldOnly) search.set("held_only", "true");
  if (params.stale) search.set("stale", "true");
  if (params.stance) search.set("stance", params.stance);
  if (params.offset !== undefined) search.set("offset", String(params.offset));
  if (params.limit !== undefined) search.set("limit", String(params.limit));
  const qs = search.toString();
  return apiFetch<ThesisLibraryResponse>(`/theses${qs ? `?${qs}` : ""}`);
}

export function fetchLatestThesis(
  instrumentId: number,
): Promise<ThesisDetail | null> {
  // Returns null when no thesis exists yet — the endpoint now answers
  // 200 + null for the pre-analysis state instead of 404 (#1813).
  return apiFetch<ThesisDetail | null>(`/theses/${instrumentId}`);
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
  force = false,
): Promise<GenerateThesisResponse> {
  // force=true (#1919) bypasses the 24h cache — used by the library's
  // per-row "request fresh" action (#1902).
  return apiFetch<GenerateThesisResponse>(
    `/instruments/${encodeURIComponent(symbol)}/thesis${force ? "?force=true" : ""}`,
    { method: "POST" },
  );
}
