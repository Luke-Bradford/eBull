import { Link } from "react-router-dom";
import type { RecommendationListItem } from "@/api/types";
import { formatDateTime } from "@/lib/format";
import { EmptyState } from "@/components/states/EmptyState";

const ACTION_TONE: Record<string, string> = {
  BUY: "bg-emerald-100 text-emerald-700",
  ADD: "bg-emerald-50 text-emerald-700",
  HOLD: "bg-slate-100 text-slate-600",
  EXIT: "bg-red-100 text-red-700",
};

const STATUS_TONE: Record<string, string> = {
  proposed: "bg-amber-100 text-amber-700",
  approved: "bg-blue-100 text-blue-700",
  rejected: "bg-red-100 text-red-700",
  executed: "bg-emerald-100 text-emerald-700",
};

export function RecentRecommendations({ items }: { items: RecommendationListItem[] }) {
  if (items.length === 0) {
    return (
      <EmptyState
        title="No recommendations yet"
        description="Recommendations will appear here once the portfolio manager has run."
      />
    );
  }
  return (
    <ul className="divide-y divide-slate-100">
      {items.map((r) => (
        <li
          key={r.recommendation_id}
          className="flex items-start justify-between gap-4 py-3 text-sm"
        >
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <Link
                to={`/instrument/${encodeURIComponent(r.symbol)}`}
                className="font-medium text-blue-600 hover:underline"
              >
                {r.symbol}
              </Link>
              <Badge tone={ACTION_TONE[r.action] ?? "bg-slate-100 text-slate-600"}>
                {r.action}
              </Badge>
              <Badge tone={STATUS_TONE[r.status] ?? "bg-slate-100 text-slate-600"}>
                {r.status}
              </Badge>
            </div>
            <p className="mt-1 line-clamp-2 text-xs text-slate-600">{r.rationale}</p>
          </div>
          <div className="shrink-0 text-right text-xs text-slate-500">
            {formatDateTime(r.created_at)}
          </div>
        </li>
      ))}
    </ul>
  );
}

function Badge({ tone, children }: { tone: string; children: React.ReactNode }) {
  return (
    <span className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-medium ${tone}`}>
      {children}
    </span>
  );
}
