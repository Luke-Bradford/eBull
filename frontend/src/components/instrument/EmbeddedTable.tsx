/**
 * EmbeddedTable — renders one ParsedTable from a 10-K Item 1 section
 * body. Headers row + data rows, monospaced numeric columns, narrow
 * left rail spacing so it sits naturally inside reading prose (#559).
 */

import type { BusinessTable } from "@/api/instruments";

export interface EmbeddedTableProps {
  readonly table: BusinessTable;
}

export function EmbeddedTable({ table }: EmbeddedTableProps): JSX.Element {
  return (
    <table className="my-4 w-full border-collapse text-sm">
      <thead>
        <tr className="border-b border-slate-300 bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-600">
          {table.headers.map((h, i) => (
            <th key={i} className="px-3 py-2 font-medium">
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {table.rows.map((row, rIdx) => (
          <tr
            key={rIdx}
            className="border-b border-slate-100 last:border-0"
          >
            {row.map((cell, cIdx) => (
              <td
                key={cIdx}
                className={`px-3 py-1.5 ${cIdx === 0 ? "" : "tabular-nums text-right"}`}
              >
                {cell}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
