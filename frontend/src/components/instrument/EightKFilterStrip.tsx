/**
 * EightKFilterStrip — controls for the 8-K filterable list (#559).
 * Severity dropdown + free-text item-code filter + date range.
 * State held in URL query string by the parent so deep-links work.
 */

import type { JSX } from "react";

export interface EightKFilters {
  readonly severity: "" | "high" | "medium" | "low";
  readonly itemCode: string;
  readonly dateFrom: string; // ISO yyyy-mm-dd, "" = no bound
  readonly dateTo: string;
}

export interface EightKFilterStripProps {
  readonly value: EightKFilters;
  readonly onChange: (next: EightKFilters) => void;
}

export function EightKFilterStrip({
  value,
  onChange,
}: EightKFilterStripProps): JSX.Element {
  const isDirty =
    value.severity !== "" ||
    value.itemCode !== "" ||
    value.dateFrom !== "" ||
    value.dateTo !== "";

  return (
    <div className="flex flex-wrap items-end gap-3 rounded border border-slate-200 bg-slate-50 p-3 text-xs">
      <label className="flex flex-col">
        <span className="text-slate-500">Severity</span>
        <select
          className="mt-0.5 rounded border border-slate-300 bg-white px-2 py-1"
          value={value.severity}
          onChange={(e) =>
            onChange({
              ...value,
              severity: e.target.value as EightKFilters["severity"],
            })
          }
        >
          <option value="">all</option>
          <option value="high">high</option>
          <option value="medium">medium</option>
          <option value="low">low</option>
        </select>
      </label>
      <label className="flex flex-col">
        <span className="text-slate-500">Item code</span>
        <input
          type="text"
          placeholder="e.g. 5.02"
          className="mt-0.5 rounded border border-slate-300 bg-white px-2 py-1"
          value={value.itemCode}
          onChange={(e) => onChange({ ...value, itemCode: e.target.value })}
        />
      </label>
      <label className="flex flex-col">
        <span className="text-slate-500">From</span>
        <input
          type="date"
          className="mt-0.5 rounded border border-slate-300 bg-white px-2 py-1"
          value={value.dateFrom}
          onChange={(e) => onChange({ ...value, dateFrom: e.target.value })}
        />
      </label>
      <label className="flex flex-col">
        <span className="text-slate-500">To</span>
        <input
          type="date"
          className="mt-0.5 rounded border border-slate-300 bg-white px-2 py-1"
          value={value.dateTo}
          onChange={(e) => onChange({ ...value, dateTo: e.target.value })}
        />
      </label>
      {isDirty && (
        <button
          type="button"
          className="ml-auto rounded border border-slate-300 px-2 py-1 hover:bg-white"
          onClick={() =>
            onChange({ severity: "", itemCode: "", dateFrom: "", dateTo: "" })
          }
        >
          Reset
        </button>
      )}
    </div>
  );
}
