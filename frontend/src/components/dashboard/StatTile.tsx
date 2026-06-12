/**
 * StatTile — the established stat-tile chrome, extracted from
 * SummaryCards' private Card (#1592 child 2, spec §6.2): hairline
 * top-rule, small-caps label, `text-2xl tabular-nums` value, optional
 * tone and `hint` slot (benchmark delta / denominators / caveats).
 *
 * SummaryCards consumes this directly; the period-statement page is
 * the second consumer. Keep the hairline — it IS the tile chrome.
 */
import type { ReactNode } from "react";

export function StatTile({
  label,
  value,
  hint,
  tone,
}: {
  label: string;
  value: string;
  hint?: ReactNode;
  tone?: "positive" | "negative";
}) {
  const toneClass =
    tone === "positive"
      ? "text-emerald-600 dark:text-emerald-400"
      : tone === "negative"
        ? "text-rose-600 dark:text-rose-400"
        : "text-slate-900 dark:text-slate-100";
  return (
    <div className="border-t border-slate-200 dark:border-slate-800 px-1 pt-3 pb-1">
      <div className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate-500">
        {label}
      </div>
      <div className={`mt-1 text-2xl font-semibold tabular-nums ${toneClass}`}>
        {value}
      </div>
      {hint ? <div className="mt-1 text-xs tabular-nums text-slate-500">{hint}</div> : null}
    </div>
  );
}
