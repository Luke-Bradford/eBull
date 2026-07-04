/**
 * Per-trade table for the instrument Positions tab (#1899, slice 1).
 *
 * The instrument drill-through endpoint (GET /portfolio/instruments/:id)
 * already returns the individual broker trades behind the aggregate
 * position — but the Positions tab only rendered "Trades: N". This
 * surfaces each open trade (entry date, side, units, entry price, current
 * price, per-trade P&L, fees) so the operator can see the round-trips that
 * make up their holding, not just the blended total.
 *
 * All figures are in the instrument's NATIVE currency (the tab shows the
 * currency code once); this table therefore takes `currency` and formats
 * money consistently via `formatMoney`.
 */

import type { NativeTradeItem } from "@/api/types";
import { formatMoney, formatNumber } from "@/lib/format";

function SideBadge({ isBuy }: { isBuy: boolean }) {
  return (
    <span
      className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-medium ${
        isBuy
          ? "bg-emerald-50 dark:bg-emerald-950/40 text-emerald-700 dark:text-emerald-300"
          : "bg-red-50 dark:bg-red-950/40 text-red-700 dark:text-red-300"
      }`}
    >
      {isBuy ? "Buy" : "Sell"}
    </span>
  );
}

export function InstrumentTradesTable({
  trades,
  currency,
}: {
  trades: NativeTradeItem[];
  currency: string;
}) {
  if (trades.length === 0) return null;

  return (
    <div className="mt-4 overflow-x-auto">
      <h3 className="mb-2 text-xs font-medium uppercase tracking-wide text-slate-500">
        Open trades ({trades.length})
      </h3>
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="py-2 pr-4">Opened</th>
            <th className="py-2 pr-4">Side</th>
            <th className="py-2 pr-4 text-right">Units</th>
            <th className="py-2 pr-4 text-right">Entry</th>
            <th className="py-2 pr-4 text-right">Price</th>
            <th className="py-2 pr-4 text-right">P&amp;L</th>
            <th className="py-2 pr-0 text-right">Fees</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
          {trades.map((t) => {
            const pnlColor =
              t.unrealized_pnl > 0
                ? "text-emerald-600 dark:text-emerald-400"
                : t.unrealized_pnl < 0
                  ? "text-red-600 dark:text-red-400"
                  : "text-slate-600 dark:text-slate-300";
            return (
              <tr key={t.position_id} className="text-slate-700 dark:text-slate-200">
                <td className="py-2 pr-4 text-xs text-slate-500">
                  {t.open_date_time ? t.open_date_time.slice(0, 10) : "—"}
                </td>
                <td className="py-2 pr-4">
                  <SideBadge isBuy={t.is_buy} />
                </td>
                <td className="py-2 pr-4 text-right tabular-nums">
                  {formatNumber(t.units)}
                </td>
                <td className="py-2 pr-4 text-right tabular-nums">
                  {formatMoney(t.open_rate, currency)}
                </td>
                <td className="py-2 pr-4 text-right tabular-nums">
                  {formatMoney(t.current_price, currency)}
                </td>
                <td className={`py-2 pr-4 text-right tabular-nums ${pnlColor}`}>
                  {`${t.unrealized_pnl >= 0 ? "+" : ""}${formatMoney(t.unrealized_pnl, currency)}`}
                </td>
                <td className="py-2 pr-0 text-right tabular-nums text-slate-500">
                  {formatMoney(t.total_fees, currency)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
