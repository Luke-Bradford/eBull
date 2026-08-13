import type { AuditQuery } from "@/api/audit";
import type { AuditPassFail, AuditStage } from "@/api/types";

const PASS_FAIL_OPTIONS: AuditPassFail[] = ["PASS", "FAIL", "KICK", "RETRY", "DEFER"];
const STAGE_OPTIONS: AuditStage[] = [
  "execution_guard",
  "order_client",
  "manual_order",
  "liveness_kick",
  "retry_backoff",
  "entry_timing",
];

// Full-vocabulary labels (#1808) — a lookup, not a binary ternary, so every
// stage reads correctly. Unknown values fall back to the raw key.
const STAGE_FILTER_LABEL: Record<string, string> = {
  execution_guard: "Execution guard",
  order_client: "Order client",
  manual_order: "Manual order",
  liveness_kick: "Liveness kick",
  retry_backoff: "Retry backoff",
  entry_timing: "Entry timing",
};

export interface AuditFiltersProps {
  query: AuditQuery;
  onQueryChange: (next: AuditQuery) => void;
  onClearAll: () => void;
  filtersDirty: boolean;
}

export function AuditFilters({
  query,
  onQueryChange,
  onClearAll,
  filtersDirty,
}: AuditFiltersProps) {
  return (
    <div
      className="flex flex-wrap items-end gap-3 border-t border-slate-200 dark:border-slate-800 px-1 pt-3 pb-2"
      role="group"
      aria-label="Audit filters"
    >
      <FilterField label="Result" htmlFor="aud-pf">
        <select
          id="aud-pf"
          className={fieldClass}
          value={query.pass_fail ?? ""}
          onChange={(e) => {
            const v = e.target.value;
            onQueryChange({
              ...query,
              pass_fail: v === "" ? null : (v as AuditPassFail),
            });
          }}
        >
          <option value="">All</option>
          {PASS_FAIL_OPTIONS.map((o) => (
            <option key={o} value={o}>
              {o}
            </option>
          ))}
        </select>
      </FilterField>

      <FilterField label="Stage" htmlFor="aud-stage">
        <select
          id="aud-stage"
          className={fieldClass}
          value={query.stage ?? ""}
          onChange={(e) => {
            const v = e.target.value;
            onQueryChange({
              ...query,
              stage: v === "" ? null : (v as AuditStage),
            });
          }}
        >
          <option value="">All</option>
          {STAGE_OPTIONS.map((s) => (
            <option key={s} value={s}>
              {STAGE_FILTER_LABEL[s] ?? s}
            </option>
          ))}
        </select>
      </FilterField>

      <FilterField label="From" htmlFor="aud-from">
        <input
          id="aud-from"
          type="date"
          className={fieldClass}
          value={query.date_from ?? ""}
          onChange={(e) => {
            const v = e.target.value;
            onQueryChange({
              ...query,
              date_from: v === "" ? null : v,
            });
          }}
        />
      </FilterField>

      <FilterField label="To" htmlFor="aud-to">
        <input
          id="aud-to"
          type="date"
          className={fieldClass}
          value={query.date_to ?? ""}
          onChange={(e) => {
            const v = e.target.value;
            onQueryChange({
              ...query,
              date_to: v === "" ? null : v,
            });
          }}
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
