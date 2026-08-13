import { apiFetch } from "@/api/client";
import type {
  RecommendationDetail,
  RecommendationAction,
  RecommendationStatus,
  RecommendationsListResponse,
} from "@/api/types";

export interface RecommendationsQuery {
  action: RecommendationAction | null;
  status: RecommendationStatus | null;
  instrument_id: number | null;
}

export const RECOMMENDATIONS_PAGE_LIMIT = 50;

export function fetchRecommendations(
  query: RecommendationsQuery,
  offset = 0,
  limit = RECOMMENDATIONS_PAGE_LIMIT,
): Promise<RecommendationsListResponse> {
  const params = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
  });
  if (query.action !== null) params.set("action", query.action);
  if (query.status !== null) params.set("status", query.status);
  if (query.instrument_id !== null) params.set("instrument_id", String(query.instrument_id));
  return apiFetch<RecommendationsListResponse>(`/recommendations?${params.toString()}`);
}

export function fetchRecommendation(id: number): Promise<RecommendationDetail> {
  return apiFetch<RecommendationDetail>(`/recommendations/${id}`);
}
