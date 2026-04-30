import { useState } from "react";
import { Link } from "react-router-dom";
import { useAsync } from "@/lib/useAsync";
import { fetchAuditDetail } from "@/api/audit";
import type { AuditListItem, AuditDetail } from "@/api/types";
import { formatDateTime } from "@/lib/format";
import { SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { EvidencePanel } from "@/components/recommendations/EvidencePanel";

const PASS_FAIL_TONE: Record<string, string> = {
  PASS: "text-emerald-600",
  FAIL: "text-red-600",
};

const STAGE_LABEL: Record<string, string> = {
  execution_guard: "Guard",
  order_client: "Order",
};

export type AuditView =
  | { kind: "loading" }
  | { kind: "error"; onRetry: () => void }
  | { kind: "error401" }
  | { kind: "empty"; title: string; description: string; action?: React.ReactNode }
  | { kind: "data"; items: ReadonlyArray<AuditListItem> };

export function AuditTrail({ view }: { view: AuditView }) {
  if (view.kind === "loading") return <SectionSkeleton rows={5} />;

  if (view.kind === "error401") {
    return <EmptyState title="Authentication required" description="Log in to view the audit trail." />;
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
            <th className="px-2 py-2">Time</th>
            <th className="px-2 py-2">Symbol</th>
            <th className="px-2 py-2">Stage</th>
            <th className="px-2 py-2">Result</th>
            <th className="px-2 py-2">Explanation</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {view.items.map((a) => (
            <AuditRow key={a.decision_id} item={a} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function AuditRow({ item }: { item: AuditListItem }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <>
      <tr
        className="cursor-pointer hover:bg-slate-50 dark:bg-slate-900/40 dark:hover:bg-slate-800/40"
        onClick={() => setExpanded((prev) => !prev)}
      >
        <td className="px-2 py-2 text-xs text-slate-500">{formatDateTime(item.decision_time)}</td>
        <td className="px-2 py-2">
          {item.instrument_id !== null && item.symbol ? (
            <Link
              to={`/instrument/${encodeURIComponent(item.symbol)}`}
              className="font-medium text-blue-600 hover:underline"
              onClick={(e) => e.stopPropagation()}
            >
              {item.symbol}
            </Link>
          ) : (
            <span className="text-slate-400">—</span>
          )}
        </td>
        <td className="px-2 py-2 text-xs text-slate-600">
          {STAGE_LABEL[item.stage] ?? item.stage}
        </td>
        <td className="px-2 py-2">
          <span className={`text-xs font-semibold ${PASS_FAIL_TONE[item.pass_fail] ?? "text-slate-600"}`}>
            {item.pass_fail}
          </span>
        </td>
        <td className="max-w-xs truncate px-2 py-2 text-slate-600">{item.explanation}</td>
      </tr>
      {expanded ? (
        <tr>
          <td colSpan={5} className="bg-slate-50 dark:bg-slate-900/40 px-4 py-3">
            <ExpandedAudit item={item} />
          </td>
        </tr>
      ) : null}
    </>
  );
}

function ExpandedAudit({ item }: { item: AuditListItem }) {
  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const detail = useAsync(
    () => fetchAuditDetail(item.decision_id),
    [item.decision_id],
  );

  if (detail.loading) {
    return <div className="animate-pulse text-xs text-slate-400">Loading evidence…</div>;
  }

  if (detail.error !== null) {
    return <div className="text-xs text-red-600">Failed to load evidence details.</div>;
  }

  const data: AuditDetail | null = detail.data;
  if (data === null) return null;

  return (
    <div className="space-y-2">
      <div className="text-xs text-slate-500">
        <span>Decision #{data.decision_id}</span>
        {data.recommendation_id !== null ? (
          <span className="ml-3">Recommendation #{data.recommendation_id}</span>
        ) : null}
        {data.model_version ? (
          <span className="ml-3">Model: {data.model_version}</span>
        ) : null}
      </div>
      <div>
        <span className="text-xs font-semibold uppercase tracking-wide text-slate-500">
          Evidence
        </span>
        <div className="mt-1">
          <EvidencePanel stage={data.stage} evidence={data.evidence_json} />
        </div>
      </div>
    </div>
  );
}
