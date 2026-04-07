import { useEffect, useMemo, useState } from "react";
import { ApiError } from "@/api/client";
import { fetchRankings, RANKINGS_PAGE_LIMIT, type RankingsQuery } from "@/api/rankings";
import { useAsync } from "@/lib/useAsync";
import { Section } from "@/components/dashboard/Section";
import { RankingsFilters } from "@/components/rankings/RankingsFilters";
import { RankingsTable, type RankingsView } from "@/components/rankings/RankingsTable";
import { formatDateTime } from "@/lib/format";
import type { RankingItem, RankingsListResponse } from "@/api/types";

/**
 * Rankings / candidates view (#61).
 *
 * Single async source: GET /rankings. The endpoint already joins instrument
 * metadata server-side (symbol, company_name, sector, coverage_tier), so
 * this page does NOT call /instruments — calling it would be a redundant
 * round-trip for data already in hand.
 *
 * Server-side filters: coverage_tier, sector, stance — included in the
 * query string and therefore in the useAsync deps so a refetch fires when
 * they change.
 *
 * Client-side filters / controls: minimum total_score, column sort. These
 * never trigger a refetch.
 *
 * Auth (#58 backend exists; frontend login route does not yet):
 *   401 → render an "Authentication required" state on this page only.
 *   No global redirect — see follow-up issue linked in the PR description.
 *
 * Strictly read-only: no mutations, no write actions.
 */
export function RankingsPage() {
  const [query, setQuery] = useState<RankingsQuery>({
    coverage_tier: null,
    sector: null,
    stance: null,
  });
  const [scoreThreshold, setScoreThreshold] = useState<number | null>(null);

  // Sector dropdown options must be derived from data the page has seen,
  // not from the *current* response. Once a sector filter is applied the
  // response only contains rows for that sector, which would otherwise
  // collapse the dropdown to one option. Cache grows monotonically.
  const [knownSectors, setKnownSectors] = useState<ReadonlyArray<string>>([]);

  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const rankings = useAsync(
    () => fetchRankings(query),
    [query.coverage_tier, query.sector, query.stance],
  );

  // Functional setState reads the freshest `prev` snapshot from React,
  // not the closure-captured `knownSectors` from the render in which this
  // effect was registered. Without this, two rapid data updates landing
  // in the same tick could re-seed from a stale snapshot and silently
  // drop sectors added in the first update.
  useEffect(() => {
    const data = rankings.data;
    if (data === null) return;
    setKnownSectors((prev) => {
      const next = new Set(prev);
      let added = false;
      for (const item of data.items) {
        if (item.sector !== null && !next.has(item.sector)) {
          next.add(item.sector);
          added = true;
        }
      }
      return added ? Array.from(next).sort() : prev;
    });
  }, [rankings.data]);

  const filtersDirty =
    query.coverage_tier !== null ||
    query.sector !== null ||
    query.stance !== null ||
    scoreThreshold !== null;

  const filteredItems = useMemo(() => {
    if (rankings.data === null) return [];
    if (scoreThreshold === null) return rankings.data.items;
    return rankings.data.items.filter(
      (i) => i.total_score !== null && i.total_score >= scoreThreshold,
    );
  }, [rankings.data, scoreThreshold]);

  // Surface the single edge case where the universe outgrew our single-page
  // assumption (>200 Tier 1+2 instruments). Loud in dev, harmless in prod.
  useEffect(() => {
    if (rankings.data !== null && rankings.data.total > rankings.data.items.length) {
      console.warn(
        `[rankings] total=${rankings.data.total} exceeds page limit ${RANKINGS_PAGE_LIMIT}; showing the first ${rankings.data.items.length} rows. Pagination is tracked as a follow-up.`,
      );
    }
  }, [rankings.data]);

  const onClearAll = () => {
    setQuery({ coverage_tier: null, sector: null, stance: null });
    setScoreThreshold(null);
  };

  const view: RankingsView = computeView({
    loading: rankings.loading,
    error: rankings.error,
    data: rankings.data,
    filteredItems,
    filtersDirty,
    onRetry: rankings.refetch,
    onClearFilters: onClearAll,
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-slate-800">Rankings</h1>
        <span className="text-xs text-slate-500">
          {rankings.data?.scored_at
            ? `Latest run: ${formatDateTime(rankings.data.scored_at)}`
            : null}
        </span>
      </div>

      <RankingsFilters
        query={query}
        onQueryChange={setQuery}
        scoreThreshold={scoreThreshold}
        onScoreThresholdChange={setScoreThreshold}
        knownSectors={knownSectors}
        onClearAll={onClearAll}
        filtersDirty={filtersDirty}
      />

      <Section title="Candidates">
        <RankingsTable view={view} />
      </Section>
    </div>
  );
}

interface ComputeViewArgs {
  loading: boolean;
  error: unknown;
  data: RankingsListResponse | null;
  filteredItems: ReadonlyArray<RankingItem>;
  filtersDirty: boolean;
  onRetry: () => void;
  onClearFilters: () => void;
}

/**
 * Map the {loading, error, data, filteredItems, filtersDirty} state set
 * to the discriminated `RankingsView` consumed by RankingsTable.
 *
 * Branch order matters and is enforced by the frontend skills:
 *   1. loading        — useAsync clears data to null on every refetch start
 *   2. error 401      — auth-required, no retry button (retry is pointless
 *                       without credentials; global redirect is a follow-up)
 *   3. error other    — generic retryable error
 *   4. empty no data  — backend returned [] before any client filter applied
 *   5. empty filtered — server returned rows but the client-side score
 *                       threshold removed them all (or the server-side
 *                       filters did and the user can clear them)
 *   6. data           — render rows
 */
function computeView(args: ComputeViewArgs): RankingsView {
  const { loading, error, data, filteredItems, filtersDirty, onRetry, onClearFilters } = args;

  if (loading) return { kind: "loading" };

  if (error !== null) {
    if (error instanceof ApiError && error.status === 401) {
      return { kind: "error401" };
    }
    return { kind: "error", onRetry };
  }

  if (data === null) {
    // Should not happen post-loading without an error, but the type
    // narrowing demands a branch — surface as a generic error.
    return { kind: "error", onRetry };
  }

  // Distinguish "engine has never run" from "engine ran but produced
  // nothing for this filter set". The backend sets scored_at=None only
  // when MAX(scored_at) is NULL — i.e. there are zero rows in the
  // `scores` table for this model_version (see app/api/scores.py
  // list_rankings step 1). The `&& items.length === 0` belt-and-braces
  // guard defends against a hypothetical malformed payload where the
  // backend serves rows with a null scored_at — without it, real rows
  // would be hidden behind the "no runs yet" message. fetchRankings
  // also console.warns on the same invariant violation so contract
  // drift surfaces immediately.
  if (data.scored_at === null && data.items.length === 0) {
    return {
      kind: "empty",
      title: "No scoring runs yet",
      description:
        "Candidate rankings will appear here once the scoring engine has completed its first run.",
    };
  }

  if (filteredItems.length === 0) {
    // The clear-filters button is the operator's escape hatch when a
    // filter combination produces zero rows. The filter bar above
    // already exposes the same control, but the issue spec requires
    // the affordance inside the empty state itself when filters are
    // dirty so the operator never has to hunt for the next action.
    return {
      kind: "empty",
      title: "No instruments match the current filters",
      description: filtersDirty
        ? "Loosen the filters or clear them to see the full ranked list."
        : "The latest scoring run produced no ranked instruments.",
      action: filtersDirty ? <ClearFiltersButton onClick={onClearFilters} /> : undefined,
    };
  }

  return { kind: "data", items: filteredItems.slice() };
}

function ClearFiltersButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="rounded border border-slate-300 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-100"
    >
      Clear filters
    </button>
  );
}
