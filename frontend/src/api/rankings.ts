import { apiFetch } from "@/api/client";
import type { RankingsCoverage, RankingsListResponse } from "@/api/types";

/**
 * Server-side query set for /rankings (#1825 — fully server-authoritative).
 *
 * Every control the Rankings page exposes now reaches the wire: the three
 * filters (coverage_tier, sector_spdr, stance), free-text search (q),
 * min-total-score, and sort (column + direction). The page no longer filters
 * or sorts client-side over a truncated page, so a single page is a correct
 * slice of the WHOLE filtered, sorted population.
 *
 * model_version is intentionally not exposed: per docs/settled-decisions.md
 * the backend defaults to the live version. A frontend selector is a separate
 * ticket.
 */
export type RankingsSortField =
  | "rank"
  | "rank_delta"
  | "symbol"
  | "coverage_tier"
  | "total_score"
  | "quality_score"
  | "value_score"
  | "turnaround_score"
  | "momentum_score"
  | "sentiment_score"
  | "confidence_score"
  | "data_completeness";

export interface RankingsQuery {
  coverage_tier: number | null;
  // #1675: real GICS sector-SPDR symbol (e.g. "XLF"); the peer-grouping dimension.
  sector_spdr: string | null;
  stance: "buy" | "hold" | "watch" | "avoid" | null;
  // #1825 — optional so existing callers (e.g. RightRail) that pass only the
  // three filters still type-check.
  q?: string | null;
  min_total_score?: number | null;
  sort?: RankingsSortField;
  sort_dir?: "asc" | "desc";
}

// A page of rankings. The backend caps `limit` at 200 (MAX_PAGE_LIMIT). This is
// a PAGE size, not a universe cap — the page steps through the full population
// via `offset` (#1825).
export const RANKINGS_PAGE_SIZE = 50;

/**
 * Fetch one page of rankings. Positional `(query, limit, offset)` so the
 * existing `fetchRankings(query, 6)` RightRail caller is unaffected.
 */
export async function fetchRankings(
  query: RankingsQuery,
  limit: number = RANKINGS_PAGE_SIZE,
  offset = 0,
): Promise<RankingsListResponse> {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  params.set("offset", String(offset));
  if (query.coverage_tier !== null) {
    params.set("coverage_tier", String(query.coverage_tier));
  }
  if (query.sector_spdr !== null) {
    params.set("sector_spdr", query.sector_spdr);
  }
  if (query.stance !== null) {
    params.set("stance", query.stance);
  }
  if (query.q != null && query.q !== "") {
    params.set("q", query.q);
  }
  if (query.min_total_score != null) {
    params.set("min_total_score", String(query.min_total_score));
  }
  if (query.sort != null) {
    params.set("sort", query.sort);
  }
  if (query.sort_dir != null) {
    params.set("sort_dir", query.sort_dir);
  }
  const response = await apiFetch<RankingsListResponse>(`/rankings?${params.toString()}`);

  // Backend invariant: scored_at is null only when there are zero rows in
  // `scores` for this model_version, which by construction means an empty
  // items list (see app/api/scores.py list_rankings step 1). Surface any drift
  // loudly so it is caught at the source rather than as a confused empty state.
  if (response.scored_at === null && response.items.length > 0) {
    console.error(
      "[rankings] backend contract violation: scored_at is null but items is non-empty",
      { total: response.total, items: response.items.length },
    );
  }

  return response;
}

/**
 * Fetch the ranked-vs-universe coverage denominator (#1918). Independent of the
 * table page query — the header renders "Ranked N of M" plus a why-not-ranked
 * breakdown so absence reads as a correct exclusion, not a bug.
 */
export async function fetchRankingsCoverage(): Promise<RankingsCoverage> {
  return apiFetch<RankingsCoverage>("/rankings/coverage");
}
