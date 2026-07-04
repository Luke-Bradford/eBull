/**
 * Closed round-trip history for the instrument Positions tab (#1926, slice 2
 * of #1899).
 *
 * The trade ledger (GET /portfolio/activity) is filtered to this instrument;
 * every `close` event is a realised round-trip — exit price, units, realised
 * P&L and holding period. It renders below the open-trades table so the
 * operator sees what they've already closed on this symbol, not just what's
 * still open.
 *
 * Currency: exit `price` is the instrument's NATIVE currency (the `currency`
 * prop, same as the open-trades table); realised P&L is the operator's DISPLAY
 * currency (#1906 — account-level realised P&L is display currency). Each is
 * self-labelled by `formatMoney`, so the mix is unambiguous.
 *
 * This is a supplementary section: it stays silent while loading, shows a muted
 * line on error, and renders nothing when there are no closed trades (the
 * caller always renders an anchor — open trades or the "Not held" state — so a
 * blank section here is unambiguous).
 *
 * We request the ledger's max page (`LIMIT`) scoped to this one instrument, so
 * a full trade history is fetched in a single call for all but pathological
 * cases. If the instrument's row count still exceeds the page, `total` will
 * exceed the returned events and we surface a "recent subset" hint rather than
 * silently understating the history.
 */

import { fetchActivity } from "@/api/portfolio";
import type { ActivityResponse } from "@/api/types";
import { formatDate, formatMoney, formatNumber } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

// GET /portfolio/activity caps `limit` at 500 (le=500). One instrument almost
// never has this many trade events, so a single max-page fetch is complete.
const MAX_LEDGER_PAGE = 500;

export function InstrumentTradeHistory({
  instrumentId,
  currency,
}: {
  instrumentId: number;
  currency: string;
}) {
  const { data, error, loading } = useAsync<ActivityResponse>(
    () => fetchActivity(false, instrumentId, MAX_LEDGER_PAGE),
    [instrumentId],
  );

  if (loading) return null;
  if (error !== null) {
    return (
      <p className="mt-4 text-xs text-slate-500">Trade history unavailable.</p>
    );
  }
  if (!data) return null;

  const closes = data.events.filter((e) => e.event_kind === "close");
  if (closes.length === 0) return null;
  // `total` counts every ledger row for this instrument (opens + closes); when
  // it exceeds the returned page some rows — possibly older closes — were
  // dropped. Be honest about it rather than implying the list is complete.
  const truncated = data.total > data.events.length;

  return (
    <div className="mt-6 overflow-x-auto">
      <h3 className="mb-2 text-xs font-medium uppercase tracking-wide text-slate-500">
        Trade history ({closes.length})
      </h3>
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="py-2 pr-4">Closed</th>
            <th className="py-2 pr-4 text-right">Units</th>
            <th className="py-2 pr-4 text-right">Exit</th>
            <th className="py-2 pr-4 text-right">Realised P&amp;L</th>
            <th className="py-2 pr-0 text-right">Held</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
          {closes.map((e) => {
            const pnl = e.realized_pnl;
            const pnlColor =
              pnl === null
                ? "text-slate-600 dark:text-slate-300"
                : pnl > 0
                  ? "text-emerald-600 dark:text-emerald-400"
                  : pnl < 0
                    ? "text-red-600 dark:text-red-400"
                    : "text-slate-600 dark:text-slate-300";
            return (
              <tr key={e.event_id} className="text-slate-700 dark:text-slate-200">
                <td className="py-2 pr-4 text-xs text-slate-500">
                  {formatDate(e.executed_at)}
                </td>
                <td className="py-2 pr-4 text-right tabular-nums">
                  {formatNumber(e.units)}
                </td>
                <td className="py-2 pr-4 text-right tabular-nums">
                  {formatMoney(e.price, currency)}
                </td>
                <td className={`py-2 pr-4 text-right tabular-nums ${pnlColor}`}>
                  {pnl === null
                    ? "—"
                    : `${pnl >= 0 ? "+" : ""}${formatMoney(pnl, data.display_currency)}`}
                </td>
                <td className="py-2 pr-0 text-right tabular-nums text-slate-500">
                  {e.holding_period_days === null
                    ? "—"
                    : `${Math.round(e.holding_period_days)}d`}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      {truncated && (
        <p className="mt-2 text-xs text-slate-500">
          Showing the most recent {data.events.length} events — older trades may
          be omitted.
        </p>
      )}
    </div>
  );
}
