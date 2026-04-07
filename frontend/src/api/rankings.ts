import { apiFetch } from "@/api/client";
import type { RankingsListResponse } from "@/api/types";

/**
 * Server-side filter set for /rankings.
 *
 * Only the three filters that the backend understands live here. Sort
 * direction and the score-threshold filter are applied client-side in
 * RankingsPage and never reach the wire.
 *
 * model_version is intentionally not exposed: per docs/settled-decisions.md
 * the v1 default is "v1-balanced" and the backend defaults to it. Adding a
 * frontend selector is a separate ticket.
 */
export interface RankingsQuery {
  coverage_tier: number | null;
  sector: string | null;
  stance: "buy" | "hold" | "watch" | "avoid" | null;
}

// The backend caps `limit` at 200 (MAX_PAGE_LIMIT in app/api/scores.py).
// The issue notes the Tier 1+2 universe is at most ~200 instruments, so a
// single page is sufficient for v1. If `total > items.length` we surface a
// console warning rather than silently truncating — see RankingsPage.
export const RANKINGS_PAGE_LIMIT = 200;

export function fetchRankings(query: RankingsQuery): Promise<RankingsListResponse> {
  const params = new URLSearchParams();
  params.set("limit", String(RANKINGS_PAGE_LIMIT));
  if (query.coverage_tier !== null) {
    params.set("coverage_tier", String(query.coverage_tier));
  }
  if (query.sector !== null) {
    params.set("sector", query.sector);
  }
  if (query.stance !== null) {
    params.set("stance", query.stance);
  }
  return apiFetch<RankingsListResponse>(`/rankings?${params.toString()}`);
}
