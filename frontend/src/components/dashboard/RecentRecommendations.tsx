import { Link } from "react-router-dom";
import type { RecommendationListItem } from "@/api/types";
import { formatDateTime } from "@/lib/format";
import { actionTone, statusTone } from "@/lib/badgeTone";
import { EmptyState } from "@/components/states/EmptyState";
import { Badge } from "@/components/ui/Badge";

export function RecentRecommendations({ items }: { items: RecommendationListItem[] }) {
  if (items.length === 0) {
    // Empty = nothing awaiting the operator's action. This is the
    // steady-state "clean queue" signal on the dashboard; wording
    // intentionally avoids implying the system is broken.
    return (
      <EmptyState
        title="Nothing awaiting review"
        description="New recommendations surface here the moment the portfolio manager queues one."
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
              <Badge tone={actionTone(r.action)}>{r.action}</Badge>
              <Badge tone={statusTone(r.status)}>{r.status}</Badge>
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
