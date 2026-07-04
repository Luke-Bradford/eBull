import { useEffect, useState } from "react";
import { ApiError } from "@/api/client";
import {
  fetchRankings,
  fetchRankingsCoverage,
  RANKINGS_PAGE_SIZE,
  type RankingsQuery,
  type RankingsSortField,
} from "@/api/rankings";
import { useAsync } from "@/lib/useAsync";
import { Section } from "@/components/dashboard/Section";
import { RankingsCoverageBanner } from "@/components/rankings/RankingsCoverageBanner";
import { RankingsFilters } from "@/components/rankings/RankingsFilters";
import { RankingsTable, type RankingsView } from "@/components/rankings/RankingsTable";
import { formatDateTime } from "@/lib/format";
import type { RankingsListResponse } from "@/api/types";

/**
 * Rankings / candidates view (#61, #1825 server-authoritative).
 *
 * Single async source: GET /rankings. EVERY control — filters, search,
 * min-score, sort, pagination — is a server query param, so one page is a
 * correct slice of the whole filtered, sorted population. The page no longer
 * filters/sorts/truncates a 200-row client buffer.
 *
 * Offset is reset to 0 in the same handler as any query change (never in a
 * useEffect reacting to `query`, which would double-fetch the new query at the
 * stale offset — #1825 / Codex ckpt-1).
 */

const DEFAULT_QUERY: RankingsQuery = {
  coverage_tier: null,
  sector_spdr: null,
  stance: null,
  q: null,
  min_total_score: null,
  sort: "rank",
  sort_dir: "asc",
};

export function RankingsPage() {
  const [query, setQuery] = useState<RankingsQuery>(DEFAULT_QUERY);
  const [offset, setOffset] = useState(0);

  // #194 — debounced symbol/name search. The debounced value feeds query.q
  // (server-side); searchInput drives the controlled input + dirty state.
  const [searchInput, setSearchInput] = useState("");
  useEffect(() => {
    const timer = setTimeout(() => {
      const next = searchInput.trim() === "" ? null : searchInput.trim();
      // No-op when q is unchanged so an identical-search settle doesn't refetch.
      setQuery((q) => (q.q === next ? q : { ...q, q: next }));
      // Reset to page 1 on a search (harmless no-op when already at offset 0).
      setOffset(0);
    }, 300);
    return () => clearTimeout(timer);
  }, [searchInput]);

  const rankings = useAsync(
    () => fetchRankings(query, RANKINGS_PAGE_SIZE, offset),
    [
      query.coverage_tier,
      query.sector_spdr,
      query.stance,
      query.q,
      query.min_total_score,
      query.sort,
      query.sort_dir,
      offset,
    ],
  );

  // Ranked-vs-universe denominator (#1918). Loaded once, independent of the
  // table query — a coverage failure hides the line, never blocks the table.
  const coverage = useAsync(() => fetchRankingsCoverage(), []);

  // Any query change resets paging to the first page (atomic with the change).
  const updateQuery = (next: RankingsQuery) => {
    setQuery(next);
    setOffset(0);
  };

  const onSortChange = (field: RankingsSortField, dir: "asc" | "desc") => {
    updateQuery({ ...query, sort: field, sort_dir: dir });
  };

  const onClearAll = () => {
    setQuery(DEFAULT_QUERY);
    setOffset(0);
    setSearchInput("");
  };

  const filtersDirty =
    query.coverage_tier !== null ||
    query.sector_spdr !== null ||
    query.stance !== null ||
    query.min_total_score != null ||
    searchInput !== "" ||
    query.sort !== "rank" ||
    query.sort_dir !== "asc";

  const view: RankingsView = computeView({
    loading: rankings.loading,
    error: rankings.error,
    data: rankings.data,
    filtersDirty,
    onRetry: rankings.refetch,
    onClearFilters: onClearAll,
  });

  const total = rankings.data?.total ?? 0;
  const pageCount = rankings.data?.items.length ?? 0;
  const rangeStart = total === 0 ? 0 : offset + 1;
  const rangeEnd = offset + pageCount;
  const hasPrev = offset > 0;
  const hasNext = offset + RANKINGS_PAGE_SIZE < total;

  return (
    <div className="flex h-full flex-col gap-6 pt-6">
      <div className="flex flex-shrink-0 items-start justify-between gap-4">
        <div className="space-y-1">
          <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">Rankings</h1>
          <RankingsCoverageBanner coverage={coverage.data ?? null} />
        </div>
        <span className="text-xs text-slate-500">
          {rankings.data?.scored_at
            ? `Latest run: ${formatDateTime(rankings.data.scored_at)}`
            : null}
        </span>
      </div>

      <div className="flex-shrink-0 space-y-3">
        <div>
          <label className="mb-1 block text-xs font-medium text-slate-600" htmlFor="rankings-search">
            Search
          </label>
          <input
            id="rankings-search"
            type="text"
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            placeholder="Symbol or company name…"
            className="w-full rounded border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 px-3 py-1.5 text-sm text-slate-700 placeholder:text-slate-400 focus:border-blue-400 focus:outline-none focus:ring-1 focus:ring-blue-400"
          />
        </div>

        <RankingsFilters
          query={query}
          onQueryChange={updateQuery}
          scoreThreshold={query.min_total_score ?? null}
          onScoreThresholdChange={(next) =>
            updateQuery({ ...query, min_total_score: next })
          }
          onClearAll={onClearAll}
          filtersDirty={filtersDirty}
        />
      </div>

      <Section title="Candidates" scrollable>
        <RankingsTable
          view={view}
          sort={query.sort ?? "rank"}
          sortDir={query.sort_dir ?? "asc"}
          onSortChange={onSortChange}
        />
      </Section>

      {view.kind === "data" && (
        <div className="flex flex-shrink-0 items-center justify-between border-t border-slate-200 dark:border-slate-800 pt-3 text-xs text-slate-500">
          <span className="tabular-nums">
            Showing {rangeStart}–{rangeEnd} of {total}
          </span>
          <div className="flex gap-2">
            <button
              type="button"
              onClick={() => setOffset((o) => Math.max(0, o - RANKINGS_PAGE_SIZE))}
              disabled={!hasPrev}
              className="rounded border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-900 px-3 py-1 font-medium text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-50"
            >
              ‹ Prev
            </button>
            <button
              type="button"
              onClick={() => setOffset((o) => o + RANKINGS_PAGE_SIZE)}
              disabled={!hasNext}
              className="rounded border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-900 px-3 py-1 font-medium text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-50"
            >
              Next ›
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

interface ComputeViewArgs {
  loading: boolean;
  error: unknown;
  data: RankingsListResponse | null;
  filtersDirty: boolean;
  onRetry: () => void;
  onClearFilters: () => void;
}

/**
 * Map the {loading, error, data} state set to the discriminated `RankingsView`.
 * The server already filtered + sorted + paged, so this operates on
 * `data.items` directly — no client filtering layer.
 *
 * Branch order (enforced by the frontend skills):
 *   1. loading        — useAsync clears data to null on every refetch start
 *   2. error 401      — auth-required, no retry button
 *   3. error other    — generic retryable error
 *   4. empty no runs  — scored_at null (engine never ran)
 *   5. empty filtered — server returned [] for a dirty filter/search set
 *   6. empty run      — server returned [] with no dirty filters
 *   7. data           — render the page
 */
function computeView(args: ComputeViewArgs): RankingsView {
  const { loading, error, data, filtersDirty, onRetry, onClearFilters } = args;

  if (loading) return { kind: "loading" };

  if (error !== null) {
    if (error instanceof ApiError && error.status === 401) {
      return { kind: "error401" };
    }
    return { kind: "error", onRetry };
  }

  if (data === null) {
    return { kind: "error", onRetry };
  }

  // "Engine never ran" vs "engine ran but this filter set is empty". The
  // backend sets scored_at=None only when there are zero rows for this
  // model_version (app/api/scores.py list_rankings step 1).
  if (data.scored_at === null && data.items.length === 0) {
    return {
      kind: "empty",
      title: "No scoring runs yet",
      description:
        "Candidate rankings will appear here once the scoring engine has completed its first run.",
    };
  }

  if (data.items.length === 0) {
    if (filtersDirty) {
      return {
        kind: "empty",
        title: "No instruments match the current filters",
        description: "Loosen the filters or clear them to see the full ranked list.",
        action: <ClearFiltersButton onClick={onClearFilters} />,
      };
    }
    return {
      kind: "empty",
      title: "Latest run produced no ranked instruments",
      description:
        "The most recent scoring run completed but did not surface any candidates. Check back after the next run.",
    };
  }

  return { kind: "data", items: data.items.slice() };
}

function ClearFiltersButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="rounded border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-900 px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:hover:bg-slate-800"
    >
      Clear filters
    </button>
  );
}
