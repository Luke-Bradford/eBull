import type { AuditQuery } from "@/api/audit";
import type { AuditPassFail, AuditStage } from "@/api/types";

const PASS_FAIL_OPTIONS: AuditPassFail[] = ["PASS", "FAIL"];
const STAGE_OPTIONS: AuditStage[] = ["execution_guard", "order_client"];

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
      className="flex flex-wrap items-end gap-3 border-t border-slate-200 px-1 pt-3 pb-2"
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
              {s === "execution_guard" ? "Execution guard" : "Order client"}
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
