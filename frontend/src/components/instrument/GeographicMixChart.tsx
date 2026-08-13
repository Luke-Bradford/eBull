/**
 * GeographicMixChart — horizontal proportion bars for the geographic
 * revenue axis (#554). Pure presentational; CSS bars, no chart
 * library — the data is a handful of rows and the operator reads the
 * ranking + share, not a trend.
 */

import type { SegmentRow } from "@/api/instruments";
import { formatBigNumber } from "@/lib/format";

export interface GeographicMixChartProps {
  readonly rows: ReadonlyArray<SegmentRow>;
}

export function GeographicMixChart({ rows }: GeographicMixChartProps) {
  const max = rows.reduce((m, r) => Math.max(m, r.revenue ?? 0), 0);
  return (
    <ul className="space-y-2">
      {rows.map((row) => {
        const widthPct = max > 0 && row.revenue !== null ? Math.max((row.revenue / max) * 100, 1) : 0;
        return (
          <li key={row.member_qname} className="text-sm">
            <div className="mb-0.5 flex items-baseline justify-between gap-2">
              <span className="truncate text-slate-700 dark:text-slate-200">{row.member_label}</span>
              <span className="shrink-0 tabular-nums text-slate-500">
                {formatBigNumber(row.revenue)}
                {row.pct_of_total !== null && (
                  <span className="ml-1 text-xs">({(row.pct_of_total * 100).toFixed(1)}%)</span>
                )}
              </span>
            </div>
            <div className="h-2 w-full rounded bg-slate-100 dark:bg-slate-800">
              <div
                className="h-2 rounded bg-blue-600/70 dark:bg-blue-500/70"
                style={{ width: `${widthPct}%` }}
              />
            </div>
          </li>
        );
      })}
    </ul>
  );
}
