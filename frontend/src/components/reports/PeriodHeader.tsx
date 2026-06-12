/**
 * §6.6 Period navigation — sticky header row: segmented Weekly|Monthly
 * control, ‹ › stepper (ends disabled, never hidden), clickable period
 * label opening a period picker over the fetched list. ArrowLeft/Right
 * step periods while focus is within the header. Navigation pushes
 * (back-button walks periods).
 */
import { useEffect, useRef, useState } from "react";

import type { ReportSnapshot } from "@/api/reports";
import { formatPeriodRange } from "@/components/reports/snapshotMath";

export type ReportTypeId = "weekly" | "monthly";

export function PeriodHeader({
  reportType,
  reports,
  selectedIndex,
  onTypeChange,
  onSelectIndex,
}: {
  reportType: ReportTypeId;
  /** Fetched list, latest first (API order). Empty while loading. */
  reports: ReportSnapshot[];
  /** Index into `reports` of the rendered period; -1 when none. */
  selectedIndex: number;
  onTypeChange: (t: ReportTypeId) => void;
  onSelectIndex: (i: number) => void;
}) {
  const [pickerOpen, setPickerOpen] = useState(false);
  const pickerRef = useRef<HTMLDivElement | null>(null);

  // Latest first: "newer" = lower index, "older" = higher index.
  const canNewer = selectedIndex > 0;
  const canOlder = selectedIndex >= 0 && selectedIndex < reports.length - 1;
  const selected = selectedIndex >= 0 ? reports[selectedIndex] : undefined;

  useEffect(() => {
    if (!pickerOpen) return;
    const onDocClick = (e: MouseEvent) => {
      if (pickerRef.current && !pickerRef.current.contains(e.target as Node)) {
        setPickerOpen(false);
      }
    };
    document.addEventListener("mousedown", onDocClick);
    return () => document.removeEventListener("mousedown", onDocClick);
  }, [pickerOpen]);

  const stepBtn =
    "rounded border border-slate-200 px-2 py-1 text-sm text-slate-600 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800";

  return (
    <div
      className="sticky top-0 z-10 flex flex-wrap items-center gap-3 border-b border-slate-200 bg-white py-2 dark:border-slate-800 dark:bg-slate-950"
      onKeyDown={(e) => {
        if (e.key === "ArrowLeft" && canOlder) {
          e.preventDefault();
          onSelectIndex(selectedIndex + 1);
        } else if (e.key === "ArrowRight" && canNewer) {
          e.preventDefault();
          onSelectIndex(selectedIndex - 1);
        }
      }}
    >
      <div className="flex overflow-hidden rounded border border-slate-200 dark:border-slate-700" role="tablist">
        {(["weekly", "monthly"] as const).map((id) => (
          <button
            key={id}
            type="button"
            role="tab"
            aria-selected={reportType === id}
            className={`px-3 py-1 text-sm ${
              reportType === id
                ? "bg-slate-800 font-medium text-white dark:bg-slate-200 dark:text-slate-900"
                : "text-slate-600 hover:bg-slate-50 dark:text-slate-300 dark:hover:bg-slate-800"
            }`}
            onClick={() => onTypeChange(id)}
          >
            {id === "weekly" ? "Weekly" : "Monthly"}
          </button>
        ))}
      </div>

      <div className="flex items-center gap-1.5">
        <button
          type="button"
          aria-label="Older period"
          className={stepBtn}
          disabled={!canOlder}
          onClick={() => onSelectIndex(selectedIndex + 1)}
        >
          ‹
        </button>
        <div className="relative" ref={pickerRef}>
          <button
            type="button"
            className="min-w-36 rounded px-2 py-1 text-sm font-medium text-slate-800 hover:bg-slate-50 dark:text-slate-100 dark:hover:bg-slate-800"
            onClick={() => setPickerOpen((v) => !v)}
            aria-haspopup="listbox"
            aria-expanded={pickerOpen}
          >
            {selected ? formatPeriodRange(selected.period_start, selected.period_end) : "—"}
          </button>
          {pickerOpen ? (
            <ul
              role="listbox"
              className="absolute left-0 top-full z-20 mt-1 max-h-72 w-56 overflow-y-auto rounded border border-slate-200 bg-white py-1 text-sm shadow-lg dark:border-slate-700 dark:bg-slate-900"
            >
              {reports.map((r, i) => (
                <li key={r.snapshot_id}>
                  <button
                    type="button"
                    role="option"
                    aria-selected={i === selectedIndex}
                    className={`w-full px-3 py-1.5 text-left ${
                      i === selectedIndex
                        ? "bg-slate-100 font-medium dark:bg-slate-800"
                        : "hover:bg-slate-50 dark:hover:bg-slate-800/60"
                    }`}
                    onClick={() => {
                      setPickerOpen(false);
                      onSelectIndex(i);
                    }}
                  >
                    {formatPeriodRange(r.period_start, r.period_end)}
                  </button>
                </li>
              ))}
            </ul>
          ) : null}
        </div>
        <button
          type="button"
          aria-label="Newer period"
          className={stepBtn}
          disabled={!canNewer}
          onClick={() => onSelectIndex(selectedIndex - 1)}
        >
          ›
        </button>
      </div>
    </div>
  );
}
