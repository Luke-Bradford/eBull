import { apiFetch } from "@/api/client";
import type { RecommendationsListResponse } from "@/api/types";

export function fetchRecommendations(limit = 10): Promise<RecommendationsListResponse> {
  // Backend orders by created_at DESC, recommendation_id DESC and dedupes
  // consecutive HOLDs per instrument (see app/api/recommendations.py).
  const params = new URLSearchParams({ limit: String(limit), offset: "0" });
  return apiFetch<RecommendationsListResponse>(`/recommendations?${params.toString()}`);
}
