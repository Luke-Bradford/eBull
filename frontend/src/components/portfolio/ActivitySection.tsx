import { useState } from "react";
import { fetchActivity } from "@/api/portfolio";
import type { ActivityEventItem } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { formatDateTime, formatMoney, formatNumber } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

/**
 * Activity tab — the broker-observed trade ledger (#1593 PR-2).
 *
 * One row per `trade_events` open/close. Mirror (copy-trading) rows are
 * excluded by default, consistent with the value-history chart's
 * own-portfolio basis; the toggle widens the server-side filter.
 *
 * Money columns (fees, realised P&L) are USD account-currency; price is
 * the instrument's native-currency rate, so it renders as a plain
 * number, not money.
 */
export function ActivitySection() {
  const [includeMirrors, setIncludeMirrors] = useState(false);
  // Filter-driven refetch: prior payload is semantically wrong for the
  // new filter, so default clear-on-refetch is correct here.
  const activity = useAsync(() => fetchActivity(includeMirrors), [includeMirrors]);

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <label className="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-400">
          <input
            type="checkbox"
            checked={includeMirrors}
            onChange={(e) => setIncludeMirrors(e.target.checked)}
            className="rounded border-slate-300 dark:border-slate-700"
          />
          Include copy-trading activity
        </label>
        {activity.data !== null && activity.data.events.length < activity.data.total ? (
          <span className="text-xs text-slate-400">
            showing {activity.data.events.length} of {activity.data.total}
          </span>
        ) : null}
      </div>

      <div className="overflow-hidden rounded-md border border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900">
        {activity.error !== null ? (
          <SectionError onRetry={activity.refetch} />
        ) : activity.loading || activity.data === null ? (
          <SectionSkeleton rows={6} />
        ) : activity.data.events.length === 0 ? (
          <EmptyState
            title="No trade activity yet"
            description="Events appear once the portfolio sync observes a position open or close — place an order or wait for the next sync."
          />
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-200 text-left text-xs uppercase tracking-wide text-slate-500 dark:border-slate-800 dark:text-slate-400">
                <th className="px-3 py-2 font-medium">When</th>
                <th className="px-3 py-2 font-medium">Action</th>
                <th className="px-3 py-2 font-medium">Symbol</th>
                <th className="px-3 py-2 text-right font-medium">Units</th>
                <th className="px-3 py-2 text-right font-medium">Price</th>
                <th className="px-3 py-2 text-right font-medium">Fees</th>
                <th className="px-3 py-2 text-right font-medium">Realised P&amp;L</th>
                <th className="px-3 py-2 text-right font-medium">Held</th>
              </tr>
            </thead>
            <tbody>
              {activity.data.events.map((e) => (
                <ActivityRow key={e.event_id} event={e} />
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

function ActivityRow({ event }: { event: ActivityEventItem }) {
  const sideLabel = event.side === "buy" ? "BUY" : "SELL";
  const sidePill =
    event.side === "buy"
      ? "bg-emerald-50 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-400"
      : "bg-red-50 text-red-700 dark:bg-red-900/30 dark:text-red-400";
  const pnl = event.realized_pnl_usd;
  const pnlClass =
    pnl === null
      ? "text-slate-400"
      : pnl >= 0
        ? "text-emerald-600 dark:text-emerald-400"
        : "text-red-600 dark:text-red-400";

  return (
    <tr className="border-b border-slate-100 last:border-0 dark:border-slate-800/60">
      <td className="whitespace-nowrap px-3 py-2 text-slate-600 dark:text-slate-400">
        {formatDateTime(event.executed_at)}
      </td>
      <td className="px-3 py-2">
        <span className={`rounded px-1.5 py-0.5 text-xs font-semibold ${sidePill}`}>
          {sideLabel}
        </span>
        {event.is_mirror ? (
          <span className="ml-1 rounded bg-slate-100 px-1.5 py-0.5 text-xs text-slate-500 dark:bg-slate-800 dark:text-slate-400">
            mirror
          </span>
        ) : null}
      </td>
      <td className="px-3 py-2 font-medium text-slate-800 dark:text-slate-100">
        {event.symbol ?? `#${event.etoro_instrument_id}`}
      </td>
      <td className="px-3 py-2 text-right tabular-nums text-slate-700 dark:text-slate-300">
        {formatNumber(event.units, 4)}
      </td>
      <td className="px-3 py-2 text-right tabular-nums text-slate-700 dark:text-slate-300">
        {formatNumber(event.price, 2)}
      </td>
      <td className="px-3 py-2 text-right tabular-nums text-slate-500 dark:text-slate-400">
        {formatMoney(event.fees_usd, "USD")}
      </td>
      <td className={`px-3 py-2 text-right tabular-nums font-medium ${pnlClass}`}>
        {formatMoney(pnl, "USD")}
      </td>
      <td className="px-3 py-2 text-right tabular-nums text-slate-500 dark:text-slate-400">
        {event.holding_period_days === null ? "—" : `${Math.round(event.holding_period_days)} d`}
      </td>
    </tr>
  );
}
