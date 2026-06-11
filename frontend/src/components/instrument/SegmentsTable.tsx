/**
 * SegmentsTable — latest-FY member rows for the business or product
 * axis (#554). Pure presentational; data + states owned by
 * SegmentsPane. Subtotal members are excluded server-side, so
 * ``pct_of_total`` columns sum to ~100%.
 */

import type { SegmentRow } from "@/api/instruments";
import { formatBigNumber } from "@/lib/format";

export interface SegmentsTableProps {
  readonly rows: ReadonlyArray<SegmentRow>;
  /** Business segments carry op income / assets; product rows are
   *  revenue-only — the extra columns collapse when entirely null. */
  readonly showOperatingIncome: boolean;
  readonly showAssets: boolean;
}

export function SegmentsTable({ rows, showOperatingIncome, showAssets }: SegmentsTableProps) {
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-slate-200 text-left text-xs uppercase tracking-wide text-slate-500 dark:border-slate-800">
          <th className="px-2 py-2 font-semibold">Segment</th>
          <th className="px-2 py-2 text-right font-semibold">Revenue</th>
          <th className="px-2 py-2 text-right font-semibold">% of total</th>
          {showOperatingIncome && <th className="px-2 py-2 text-right font-semibold">Op. income</th>}
          {showAssets && <th className="px-2 py-2 text-right font-semibold">Assets</th>}
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr
            key={row.member_qname}
            className="border-b border-slate-100 last:border-b-0 dark:border-slate-800/60"
          >
            <td className="px-2 py-2 text-slate-700 dark:text-slate-200">{row.member_label}</td>
            <td className="px-2 py-2 text-right tabular-nums">{formatBigNumber(row.revenue)}</td>
            <td className="px-2 py-2 text-right tabular-nums text-slate-500">
              {row.pct_of_total !== null ? `${(row.pct_of_total * 100).toFixed(1)}%` : "—"}
            </td>
            {showOperatingIncome && (
              <td className="px-2 py-2 text-right tabular-nums">{formatBigNumber(row.operating_income)}</td>
            )}
            {showAssets && (
              <td className="px-2 py-2 text-right tabular-nums">{formatBigNumber(row.assets)}</td>
            )}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
