import { useState } from "react";
import { Link } from "react-router-dom";
import { useAsync } from "@/lib/useAsync";
import { fetchRecommendation } from "@/api/recommendations";
import type { RecommendationListItem } from "@/api/types";
import { formatDateTime, formatNumber, formatPct } from "@/lib/format";
import { SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";

const ACTION_TONE: Record<string, string> = {
  BUY: "bg-emerald-100 dark:bg-emerald-900/40 text-emerald-700 dark:text-emerald-300",
  ADD: "bg-emerald-50 dark:bg-emerald-950/40 text-emerald-700 dark:text-emerald-300",
  HOLD: "bg-slate-100 dark:bg-slate-800 text-slate-600",
  EXIT: "bg-red-100 dark:bg-red-900/40 text-red-700 dark:text-red-300",
  // Informational only — evaluated but blocked from BUY (#1820).
  CONSIDERED: "bg-amber-50 dark:bg-amber-950/40 text-amber-700 dark:text-amber-300",
};

const STATUS_TONE: Record<string, string> = {
  proposed: "bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-300",
  approved: "bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300",
  rejected: "bg-red-100 dark:bg-red-900/40 text-red-700 dark:text-red-300",
  executed: "bg-emerald-100 dark:bg-emerald-900/40 text-emerald-700 dark:text-emerald-300",
  considered: "bg-slate-100 dark:bg-slate-800 text-slate-500",
};

const COMPLETENESS_TONE: Record<string, string> = {
  full: "bg-emerald-50 dark:bg-emerald-950/40 text-emerald-700 dark:text-emerald-300",
  thin_data: "bg-amber-50 dark:bg-amber-950/40 text-amber-700 dark:text-amber-300",
  insufficient_data: "bg-red-50 dark:bg-red-950/40 text-red-700 dark:text-red-300",
};

const COMPLETENESS_LABEL: Record<string, string> = {
  full: "full",
  thin_data: "thin",
  insufficient_data: "insufficient",
};

const COLSPAN = 7;

// Section grouping (#1820 §7): the page is a funnel, not a flat log. Each
// action maps to exactly one bucket; an unrecognised action falls into "Other"
// so nothing is silently hidden.
interface SectionDef {
  key: string;
  title: string;
  description: string;
  actions: ReadonlyArray<string>;
}

const SECTIONS: ReadonlyArray<SectionDef> = [
  { key: "to-buy", title: "To buy", description: "Cleared every BUY/ADD gate this run.", actions: ["BUY", "ADD"] },
  {
    key: "considered",
    title: "Considered — blocked",
    description: "Evaluated but blocked from a BUY; the reason is in the rationale.",
    actions: ["CONSIDERED"],
  },
  { key: "hold", title: "Hold", description: "No action trigger met.", actions: ["HOLD"] },
  { key: "exit", title: "Exit", description: "Thesis break, severe risk, or target reached.", actions: ["EXIT"] },
];

const KNOWN_ACTIONS = new Set(SECTIONS.flatMap((s) => s.actions));

export type RecommendationsView =
  | { kind: "loading" }
  | { kind: "error"; onRetry: () => void }
  | { kind: "error401" }
  | { kind: "empty"; title: string; description: string; action?: React.ReactNode }
  | { kind: "data"; items: ReadonlyArray<RecommendationListItem> };

export function RecommendationsTable({
  view,
  onViewAudit,
}: {
  view: RecommendationsView;
  onViewAudit?: (instrumentId: number) => void;
}) {
  if (view.kind === "loading") return <SectionSkeleton rows={5} />;

  if (view.kind === "error401") {
    return <EmptyState title="Authentication required" description="Log in to view recommendations." />;
  }

  if (view.kind === "error") {
    return (
      <div
        role="alert"
        className="flex items-center justify-between rounded-md border border-red-200 dark:border-red-900/60 bg-red-50 dark:bg-red-950/40 px-3 py-2 text-sm text-red-700 dark:text-red-300"
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

  // Group the current page's items into the funnel sections, preserving the
  // server's rank ordering within each bucket. "Other" catches any action not
  // in a defined section so an unexpected value is never dropped.
  const buckets = new Map<string, RecommendationListItem[]>();
  const other: RecommendationListItem[] = [];
  for (const item of view.items) {
    if (KNOWN_ACTIONS.has(item.action)) {
      const def = SECTIONS.find((s) => s.actions.includes(item.action))!;
      const list = buckets.get(def.key) ?? [];
      list.push(item);
      buckets.set(def.key, list);
    } else {
      other.push(item);
    }
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-200 dark:border-slate-800 text-left text-xs font-semibold uppercase tracking-wide text-slate-500">
            <th className="px-2 py-2">Symbol</th>
            <th className="px-2 py-2">Action</th>
            <th className="px-2 py-2">Status</th>
            <th className="px-2 py-2">Data</th>
            <th className="px-2 py-2">Rationale</th>
            <th className="px-2 py-2 text-right tabular-nums">Size %</th>
            <th className="px-2 py-2">Date</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
          {SECTIONS.map((section) => {
            const rows = buckets.get(section.key);
            if (!rows || rows.length === 0) return null;
            return (
              <SectionGroup key={section.key} section={section} rows={rows} onViewAudit={onViewAudit} />
            );
          })}
          {other.length > 0 ? (
            <SectionGroup
              section={{ key: "other", title: "Other", description: "", actions: [] }}
              rows={other}
              onViewAudit={onViewAudit}
            />
          ) : null}
        </tbody>
      </table>
    </div>
  );
}

function SectionGroup({
  section,
  rows,
  onViewAudit,
}: {
  section: SectionDef;
  rows: ReadonlyArray<RecommendationListItem>;
  onViewAudit?: (instrumentId: number) => void;
}) {
  return (
    <>
      <tr className="bg-slate-50 dark:bg-slate-900/40">
        <th colSpan={COLSPAN} className="px-2 py-1.5 text-left">
          <span className="text-xs font-semibold uppercase tracking-wide text-slate-600 dark:text-slate-300">
            {section.title}
          </span>
          <span className="ml-2 text-[11px] font-normal text-slate-400">
            {rows.length}
            {section.description ? ` · ${section.description}` : ""}
          </span>
        </th>
      </tr>
      {rows.map((r) => (
        <RecommendationRow key={r.recommendation_id} item={r} onViewAudit={onViewAudit} />
      ))}
    </>
  );
}

function RecommendationRow({
  item,
  onViewAudit,
}: {
  item: RecommendationListItem;
  onViewAudit?: (instrumentId: number) => void;
}) {
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
        <td className="px-2 py-2">
          <CompletenessBadge tier={item.completeness_tier} value={item.data_completeness} />
        </td>
        <td className="max-w-xs truncate px-2 py-2 text-slate-600">{item.rationale}</td>
        <td className="px-2 py-2 text-right tabular-nums text-slate-700">
          {formatPct(item.suggested_size_pct)}
        </td>
        <td className="px-2 py-2 text-xs text-slate-500">{formatDateTime(item.created_at)}</td>
      </tr>
      {expanded ? (
        <tr>
          <td colSpan={COLSPAN} className="bg-slate-50 dark:bg-slate-900/40 px-4 py-3">
            <ExpandedDetail item={item} onViewAudit={onViewAudit} />
          </td>
        </tr>
      ) : null}
    </>
  );
}

function ExpandedDetail({
  item,
  onViewAudit,
}: {
  item: RecommendationListItem;
  onViewAudit?: (instrumentId: number) => void;
}) {
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
      {item.status === "rejected" && onViewAudit ? (
        <div className="rounded border border-red-200 dark:border-red-900/60 bg-red-50 dark:bg-red-950/30 px-3 py-2">
          <p className="text-xs text-red-700 dark:text-red-300">
            Rejected by the execution guard (a hard rule failed). The per-rule evidence is in the audit trail.
          </p>
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              onViewAudit(item.instrument_id);
            }}
            className="mt-1 rounded border border-red-300 dark:border-red-800 bg-white dark:bg-slate-900 px-2 py-0.5 text-xs font-medium text-red-700 dark:text-red-300 hover:bg-red-100 dark:hover:bg-red-900/40"
          >
            View guard evidence ↓
          </button>
        </div>
      ) : null}
      <div className="flex flex-wrap gap-4 text-xs text-slate-500">
        <span>Model: {item.model_version ?? "—"}</span>
        <span>Target entry: {item.target_entry !== null ? formatNumber(item.target_entry, 2) : "—"}</span>
        <span>
          Data completeness:{" "}
          {item.data_completeness !== null ? formatNumber(item.data_completeness, 2) : "—"}
          {item.completeness_tier ? ` (${item.completeness_tier})` : ""}
        </span>
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

function CompletenessBadge({ tier, value }: { tier: string | null; value: number | null }) {
  if (tier === null) return <span className="text-xs text-slate-400">—</span>;
  const tone = COMPLETENESS_TONE[tier] ?? "bg-slate-100 dark:bg-slate-800 text-slate-600";
  const label = COMPLETENESS_LABEL[tier] ?? tier;
  const title = value !== null ? `C=${value.toFixed(2)}` : undefined;
  return (
    <span title={title}>
      <Badge tone={tone}>{label}</Badge>
    </span>
  );
}

function Badge({ tone, children }: { tone: string; children: React.ReactNode }) {
  return (
    <span className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-medium ${tone}`}>
      {children}
    </span>
  );
}
