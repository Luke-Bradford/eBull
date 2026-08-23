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
import { Badge } from "@/components/ui/Badge";

function SideBadge({ isBuy }: { isBuy: boolean }) {
  return <Badge tone={isBuy ? "ok" : "risk"}>{isBuy ? "Buy" : "Sell"}</Badge>;
}

/**
 * What the position actually IS (#2602 item 3), from eToro's own
 * `settlementTypeID`. Two trades on the same instrument can differ, so this
 * belongs per-row rather than on the tab header.
 *
 * `null` renders as "Unknown", NOT as a product: the broker reporting no type
 * we recognise is a different fact from the position being a derivative, and a
 * panel that guesses here tells the operator they own something they do not.
 *
 * ⚠ The non-underlying tooltip says "not the real asset held outright" and NOT
 * "a contract with the broker". Those are not the same claim: `3 - Crypto
 * MarginTrade` IS the real asset by the provider's own wording, just held on
 * margin, so calling it a contract would be wrong (Codex ckpt-2). The weaker
 * sentence is true of all four non-underlying types, and it avoids restating
 * eToro's taxonomy in the frontend where it would drift.
 */
function ProductBadge({
  investmentType,
  isUnderlying,
}: {
  investmentType: string | null;
  isUnderlying: boolean | null;
}) {
  if (investmentType === null) {
    return (
      <Badge tone="neutral" title="The broker did not report a product type for this position">
        Unknown
      </Badge>
    );
  }
  return (
    <Badge
      tone={isUnderlying ? "ok" : "info"}
      title={
        isUnderlying
          ? "Real asset held outright — you own the underlying"
          : `${investmentType} — not the real asset held outright`
      }
    >
      {investmentType}
    </Badge>
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
            <th className="py-2 pr-4">Product</th>
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
                <td className="py-2 pr-4">
                  <ProductBadge
                    investmentType={t.investment_type}
                    isUnderlying={t.is_underlying}
                  />
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
