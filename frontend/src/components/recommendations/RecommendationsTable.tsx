import { useState } from "react";
import { Link } from "react-router-dom";
import { useAsync } from "@/lib/useAsync";
import { fetchRecommendation } from "@/api/recommendations";
import type { RecommendationListItem } from "@/api/types";
import { formatDateTime, formatNumber, formatPct } from "@/lib/format";
import { SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";

const ACTION_TONE: Record<string, string> = {
  BUY: "bg-emerald-100 text-emerald-700",
  ADD: "bg-emerald-50 text-emerald-700",
  HOLD: "bg-slate-100 dark:bg-slate-800 text-slate-600",
  EXIT: "bg-red-100 text-red-700",
};

const STATUS_TONE: Record<string, string> = {
  proposed: "bg-amber-100 text-amber-700",
  approved: "bg-blue-100 text-blue-700",
  rejected: "bg-red-100 text-red-700",
  executed: "bg-emerald-100 text-emerald-700",
};

export type RecommendationsView =
  | { kind: "loading" }
  | { kind: "error"; onRetry: () => void }
  | { kind: "error401" }
  | { kind: "empty"; title: string; description: string; action?: React.ReactNode }
  | { kind: "data"; items: ReadonlyArray<RecommendationListItem> };

export function RecommendationsTable({ view }: { view: RecommendationsView }) {
  if (view.kind === "loading") return <SectionSkeleton rows={5} />;

  if (view.kind === "error401") {
    return <EmptyState title="Authentication required" description="Log in to view recommendations." />;
  }

  if (view.kind === "error") {
    return (
      <div
        role="alert"
        className="flex items-center justify-between rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
      >
        <span>Failed to load. Check the browser console for details.</span>
        <button
          type="button"
          onClick={view.onRetry}
          className="rounded border border-red-300 bg-white dark:bg-slate-900 px-2 py-1 text-xs font-medium text-red-700 hover:bg-red-100"
        >
          Retry
        </button>
      </div>
    );
  }

  if (view.kind === "empty") {
    return (
      <EmptyState title={view.title} description={view.description}>
        {view.action}
      </EmptyState>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-200 dark:border-slate-800 text-left text-xs font-semibold uppercase tracking-wide text-slate-500">
            <th className="px-2 py-2">Symbol</th>
            <th className="px-2 py-2">Action</th>
            <th className="px-2 py-2">Status</th>
            <th className="px-2 py-2">Rationale</th>
            <th className="px-2 py-2 text-right tabular-nums">Size %</th>
            <th className="px-2 py-2">Date</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {view.items.map((r) => (
            <RecommendationRow key={r.recommendation_id} item={r} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function RecommendationRow({ item }: { item: RecommendationListItem }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <>
      <tr
 className="cursor-pointer hover:bg-slate-50 dark:hover:bg-slate-800/40"
        onClick={() => setExpanded((prev) => !prev)}
      >
        <td className="px-2 py-2">
          <Link
            to={`/instrument/${encodeURIComponent(item.symbol)}`}
            className="font-medium text-blue-600 hover:underline"
            onClick={(e) => e.stopPropagation()}
          >
            {item.symbol}
          </Link>
        </td>
        <td className="px-2 py-2">
          <Badge tone={ACTION_TONE[item.action] ?? "bg-slate-100 dark:bg-slate-800 text-slate-600"}>
            {item.action}
          </Badge>
        </td>
        <td className="px-2 py-2">
          <Badge tone={STATUS_TONE[item.status] ?? "bg-slate-100 dark:bg-slate-800 text-slate-600"}>
            {item.status}
          </Badge>
        </td>
        <td className="max-w-xs truncate px-2 py-2 text-slate-600">{item.rationale}</td>
        <td className="px-2 py-2 text-right tabular-nums text-slate-700">
          {formatPct(item.suggested_size_pct)}
        </td>
        <td className="px-2 py-2 text-xs text-slate-500">{formatDateTime(item.created_at)}</td>
      </tr>
      {expanded ? (
        <tr>
          <td colSpan={6} className="bg-slate-50 dark:bg-slate-900/40 px-4 py-3">
            <ExpandedDetail item={item} />
          </td>
        </tr>
      ) : null}
    </>
  );
}

function ExpandedDetail({ item }: { item: RecommendationListItem }) {
  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const detail = useAsync(
    () => fetchRecommendation(item.recommendation_id),
    [item.recommendation_id],
  );

  return (
    <div className="space-y-2 text-sm">
      <div>
        <span className="text-xs font-semibold uppercase tracking-wide text-slate-500">
          Full rationale
        </span>
        <p className="mt-1 whitespace-pre-wrap text-slate-700">{item.rationale}</p>
      </div>
      <div className="flex flex-wrap gap-4 text-xs text-slate-500">
        <span>Model: {item.model_version ?? "—"}</span>
        <span>Target entry: {item.target_entry !== null ? formatNumber(item.target_entry, 2) : "—"}</span>
        <span>Cash known: {item.cash_balance_known === null ? "—" : item.cash_balance_known ? "Yes" : "No"}</span>
        {detail.loading ? (
          <span className="animate-pulse text-slate-400">Loading score…</span>
        ) : detail.data?.total_score !== null && detail.data?.total_score !== undefined ? (
          <span>Total score: {formatNumber(detail.data.total_score, 2)}</span>
        ) : null}
      </div>
    </div>
  );
}

function Badge({ tone, children }: { tone: string; children: React.ReactNode }) {
  return (
    <span className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-medium ${tone}`}>
      {children}
    </span>
  );
}
