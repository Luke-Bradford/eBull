import type { RankingsQuery } from "@/api/rankings";

/**
 * Filter bar for the rankings page.
 *
 * Server-side filters (sent to /rankings as query params):
 *   - coverage_tier (1 / 2 / 3)
 *   - sector
 *   - stance (buy / hold / watch / avoid)
 *
 * Client-side filter (applied to the in-memory result set):
 *   - score threshold (minimum total_score)
 *
 * Sort is also client-side and lives on the table component, not here.
 *
 * Sector options are derived from rows the page has *seen so far*.  Once
 * the user selects a sector the API only returns rows for that sector, so
 * a naive "derive from current items" would shrink the dropdown to a
 * single option.  The page passes a monotonically-growing set instead.
 */

export const STANCE_OPTIONS = ["buy", "hold", "watch", "avoid"] as const;
export type StanceOption = (typeof STANCE_OPTIONS)[number];

export const TIER_OPTIONS = [1, 2, 3] as const;

export interface RankingsFiltersProps {
  query: RankingsQuery;
  onQueryChange: (next: RankingsQuery) => void;
  scoreThreshold: number | null;
  onScoreThresholdChange: (next: number | null) => void;
  knownSectors: ReadonlyArray<string>;
  onClearAll: () => void;
  filtersDirty: boolean;
}

export function RankingsFilters({
  query,
  onQueryChange,
  scoreThreshold,
  onScoreThresholdChange,
  knownSectors,
  onClearAll,
  filtersDirty,
}: RankingsFiltersProps) {
  return (
    <div
      className="flex flex-wrap items-end gap-3 border-t border-slate-200 px-1 pt-3 pb-2"
      role="group"
      aria-label="Rankings filters"
    >
      <FilterField label="Coverage tier" htmlFor="rk-tier">
        <select
          id="rk-tier"
          className={fieldClass}
          value={query.coverage_tier ?? ""}
          onChange={(e) => {
            const v = e.target.value;
            onQueryChange({
              ...query,
              coverage_tier: v === "" ? null : Number(v),
            });
          }}
        >
          <option value="">All</option>
          {TIER_OPTIONS.map((t) => (
            <option key={t} value={t}>
              Tier {t}
            </option>
          ))}
        </select>
      </FilterField>

      <FilterField label="Sector" htmlFor="rk-sector">
        <select
          id="rk-sector"
          className={fieldClass}
          value={query.sector ?? ""}
          onChange={(e) => {
            const v = e.target.value;
            onQueryChange({ ...query, sector: v === "" ? null : v });
          }}
        >
          <option value="">All</option>
          {knownSectors.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
      </FilterField>

      <FilterField label="Stance" htmlFor="rk-stance">
        <select
          id="rk-stance"
          className={fieldClass}
          value={query.stance ?? ""}
          onChange={(e) => {
            const v = e.target.value;
            onQueryChange({
              ...query,
              stance: v === "" ? null : (v as StanceOption),
            });
          }}
        >
          <option value="">All</option>
          {STANCE_OPTIONS.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
      </FilterField>

      <FilterField label="Min total score" htmlFor="rk-score">
        <input
          id="rk-score"
          type="number"
          step="0.01"
          className={`${fieldClass} w-24`}
          value={scoreThreshold ?? ""}
          onChange={(e) => {
            const raw = e.target.value;
            if (raw === "") {
              onScoreThresholdChange(null);
              return;
            }
            const parsed = Number(raw);
            onScoreThresholdChange(Number.isFinite(parsed) ? parsed : null);
          }}
          placeholder="Any"
        />
      </FilterField>

      <button
        type="button"
        onClick={onClearAll}
        disabled={!filtersDirty}
        className="ml-auto rounded border border-slate-300 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:hover:bg-slate-800"
      >
        Clear filters
      </button>
    </div>
  );
}

function FilterField({
  label,
  htmlFor,
  children,
}: {
  label: string;
  htmlFor: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex flex-col">
      <label
        htmlFor={htmlFor}
        className="text-xs font-semibold uppercase tracking-wide text-slate-500"
      >
        {label}
      </label>
      {children}
    </div>
  );
}

const fieldClass =
  "mt-1 rounded border border-slate-300 bg-white px-2 py-1.5 text-sm text-slate-700 placeholder:text-slate-400 focus:border-blue-500 focus:outline-none dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:placeholder:text-slate-500";
