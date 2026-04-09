import type { RecommendationsQuery } from "@/api/recommendations";
import type { RecommendationAction, RecommendationStatus } from "@/api/types";

const ACTION_OPTIONS: RecommendationAction[] = ["BUY", "ADD", "HOLD", "EXIT"];
const STATUS_OPTIONS: RecommendationStatus[] = ["proposed", "approved", "rejected", "executed"];

export interface RecommendationsFiltersProps {
  query: RecommendationsQuery;
  onQueryChange: (next: RecommendationsQuery) => void;
  onClearAll: () => void;
  filtersDirty: boolean;
}

export function RecommendationsFilters({
  query,
  onQueryChange,
  onClearAll,
  filtersDirty,
}: RecommendationsFiltersProps) {
  return (
    <div
      className="flex flex-wrap items-end gap-3 rounded-md border border-slate-200 bg-white p-3 shadow-sm"
      role="group"
      aria-label="Recommendations filters"
    >
      <FilterField label="Action" htmlFor="rec-action">
        <select
          id="rec-action"
          className={fieldClass}
          value={query.action ?? ""}
          onChange={(e) => {
            const v = e.target.value;
            onQueryChange({
              ...query,
              action: v === "" ? null : (v as RecommendationAction),
            });
          }}
        >
          <option value="">All</option>
          {ACTION_OPTIONS.map((a) => (
            <option key={a} value={a}>
              {a}
            </option>
          ))}
        </select>
      </FilterField>

      <FilterField label="Status" htmlFor="rec-status">
        <select
          id="rec-status"
          className={fieldClass}
          value={query.status ?? ""}
          onChange={(e) => {
            const v = e.target.value;
            onQueryChange({
              ...query,
              status: v === "" ? null : (v as RecommendationStatus),
            });
          }}
        >
          <option value="">All</option>
          {STATUS_OPTIONS.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
      </FilterField>

      <button
        type="button"
        onClick={onClearAll}
        disabled={!filtersDirty}
        className="ml-auto rounded border border-slate-300 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-50"
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
  "mt-1 rounded border border-slate-300 bg-white px-2 py-1.5 text-sm text-slate-700 focus:border-blue-500 focus:outline-none";
