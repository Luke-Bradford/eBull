import { useState, type JSX } from "react";

import type { CandleBar, CandleRange } from "@/api/types";
import { EmptyState } from "@/components/states/EmptyState";

export interface RawOhlcvTableProps {
  readonly rows: ReadonlyArray<CandleBar>;
  readonly symbol: string;
  readonly range: CandleRange;
}

type SortDir = "asc" | "desc";

function formatNum(v: string | null | undefined): string {
  if (v === null || v === undefined) return "—";
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function downloadCsv(rows: ReadonlyArray<CandleBar>, symbol: string, range: CandleRange): void {
  const header = "date,open,high,low,close,volume\n";
  const body = rows
    .map(
      (r) =>
        `${r.date},${r.open ?? ""},${r.high ?? ""},${r.low ?? ""},${r.close ?? ""},${r.volume ?? ""}`,
    )
    .join("\n");
  const csv = header + body;
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `${symbol}-${range}-ohlcv.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

export function RawOhlcvTable({ rows, symbol, range }: RawOhlcvTableProps): JSX.Element {
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  if (rows.length === 0) {
    return (
      <div className="p-4">
        <EmptyState
          title="No raw data"
          description="No candle rows in the local price_daily store for this range."
        />
      </div>
    );
  }

  const sorted = [...rows].sort((a, b) =>
    sortDir === "desc" ? b.date.localeCompare(a.date) : a.date.localeCompare(b.date),
  );

  return (
    <div className="p-4">
      <div className="mb-3 flex items-center justify-between">
        <span className="text-xs text-slate-500">
          {rows.length} rows · {range}
        </span>
        <button
          type="button"
          onClick={() => downloadCsv(sorted, symbol, range)}
          className="rounded bg-emerald-600 px-3 py-1 text-xs font-medium text-white hover:bg-emerald-700"
          data-testid="csv-download"
        >
          Download CSV
        </button>
      </div>
      <div className="overflow-auto">
        <table className="min-w-full text-sm">
          <thead className="border-b border-slate-200">
            <tr className="text-left text-xs text-slate-500">
              <th className="px-2 py-1">
                <button
                  type="button"
                  onClick={() => setSortDir((d) => (d === "asc" ? "desc" : "asc"))}
                  className="hover:underline"
                  data-testid="sort-date"
                >
                  Date {sortDir === "desc" ? "↓" : "↑"}
                </button>
              </th>
              <th className="px-2 py-1 text-right">Open</th>
              <th className="px-2 py-1 text-right">High</th>
              <th className="px-2 py-1 text-right">Low</th>
              <th className="px-2 py-1 text-right">Close</th>
              <th className="px-2 py-1 text-right">Volume</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((r) => (
              <tr key={r.date} className="border-b border-slate-100 last:border-0">
                <td className="px-2 py-1 tabular-nums">{r.date}</td>
                <td className="px-2 py-1 text-right tabular-nums">{formatNum(r.open)}</td>
                <td className="px-2 py-1 text-right tabular-nums">{formatNum(r.high)}</td>
                <td className="px-2 py-1 text-right tabular-nums">{formatNum(r.low)}</td>
                <td className="px-2 py-1 text-right tabular-nums font-medium">{formatNum(r.close)}</td>
                <td className="px-2 py-1 text-right tabular-nums">{formatNum(r.volume)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
