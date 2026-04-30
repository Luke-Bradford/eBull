import { Link } from "react-router-dom";

import type { WatchlistItem } from "@/api/watchlist";
import { EmptyState } from "@/components/states/EmptyState";

function formatAddedAt(raw: string): string {
  // Parse the added_at timestamp through Date rather than assuming an
  // ISO 8601 YYYY-MM-DD prefix shape on the raw string.
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) return raw;
  return d.toLocaleDateString();
}

interface Props {
  items: WatchlistItem[];
  onRemove?: (symbol: string) => void;
}

export function WatchlistPanel({ items, onRemove }: Props) {
  if (items.length === 0) {
    return (
      <EmptyState
        title="Watchlist empty"
        description="Add tickers from the instrument page to track them here."
      />
    );
  }
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="border-b border-slate-200 text-left text-xs text-slate-500 dark:text-slate-400">
            <th className="px-2 py-1">Symbol</th>
            <th className="px-2 py-1">Name</th>
            <th className="px-2 py-1">Sector</th>
            <th className="px-2 py-1">Notes</th>
            <th className="px-2 py-1 text-xs text-slate-400 dark:text-slate-500">Added</th>
            {onRemove && <th className="px-2 py-1" />}
          </tr>
        </thead>
        <tbody>
          {items.map((item) => (
            <tr
              key={item.instrument_id}
              className="border-b border-slate-100 last:border-0"
            >
              <td className="px-2 py-1">
                <Link
                  to={`/instrument/${encodeURIComponent(item.symbol)}`}
                  className="font-medium text-blue-700 hover:underline"
                >
                  {item.symbol}
                </Link>
              </td>
              <td className="px-2 py-1">{item.company_name}</td>
              <td className="px-2 py-1 text-slate-500 dark:text-slate-400">{item.sector ?? "—"}</td>
              <td className="px-2 py-1 text-xs text-slate-500 dark:text-slate-400">
                {item.notes ?? ""}
              </td>
              <td className="px-2 py-1 text-xs text-slate-400 dark:text-slate-500">
                {formatAddedAt(item.added_at)}
              </td>
              {onRemove && (
                <td className="px-2 py-1 text-right">
                  <button
                    type="button"
                    className="text-xs text-red-600 hover:underline"
                    onClick={() => onRemove(item.symbol)}
                  >
                    Remove
                  </button>
                </td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
