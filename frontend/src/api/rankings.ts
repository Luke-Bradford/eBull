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

export async function fetchRankings(
  query: RankingsQuery,
): Promise<RankingsListResponse> {
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
  const response = await apiFetch<RankingsListResponse>(`/rankings?${params.toString()}`);

  // Backend invariant: scored_at is null only when there are zero rows in
  // `scores` for this model_version, which by construction means an empty
  // items list (see app/api/scores.py list_rankings step 1). If a future
  // backend change ever serves rows with a null scored_at, the page would
  // hide them behind the "no runs yet" empty state — surface the drift
  // loudly here so it is caught at the source rather than as a confused
  // empty state on the page.
  if (response.scored_at === null && response.items.length > 0) {
    console.error(
      "[rankings] backend contract violation: scored_at is null but items is non-empty",
      { total: response.total, items: response.items.length },
    );
  }

  return response;
}
