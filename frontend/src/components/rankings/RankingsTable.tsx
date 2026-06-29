import { type ReactNode } from "react";
import { Link } from "react-router-dom";
import type { RankingsSortField } from "@/api/rankings";
import type { RankingItem } from "@/api/types";
import { formatNumber } from "@/lib/format";
import { RankDeltaCell } from "@/components/rankings/RankDeltaCell";

/**
 * Rankings table (#1825 — server-authoritative sort + pagination).
 *
 * One component, five render branches, one stable `<table>` element. The
 * `<thead>` column set is defined exactly once in COLUMNS and rendered in
 * every branch so the layout does not shift between loading / empty /
 * error / data states.
 *
 * Sorting is now SERVER-side: a header click calls `onSortChange(field, dir)`
 * and the page refetches the WHOLE filtered population reordered — the table
 * no longer sorts the current page in memory (which would only reorder the
 * visible slice). `sort` / `sortDir` come from props (the page's query state).
 */

interface ColumnDef {
  // null = not server-sortable (display-only, like Company; or the GICS sector
  // label which has no matching SQL sort expression — #1825 / Codex ckpt-1).
  sortKey: RankingsSortField | null;
  label: string;
  align: "left" | "right";
  // Default sort direction when this column is first clicked. Numeric score
  // columns want highest-first; rank/symbol/tier want ascending-first.
  defaultDir: "asc" | "desc";
}

const COLUMNS: ReadonlyArray<ColumnDef> = [
  { sortKey: "rank", label: "Rank", align: "right", defaultDir: "asc" },
  { sortKey: "rank_delta", label: "Δ", align: "right", defaultDir: "asc" },
  { sortKey: "symbol", label: "Symbol", align: "left", defaultDir: "asc" },
  { sortKey: null, label: "Sector", align: "left", defaultDir: "asc" },
  { sortKey: "coverage_tier", label: "Tier", align: "right", defaultDir: "asc" },
  { sortKey: "total_score", label: "Total", align: "right", defaultDir: "desc" },
  { sortKey: "quality_score", label: "Quality", align: "right", defaultDir: "desc" },
  { sortKey: "value_score", label: "Value", align: "right", defaultDir: "desc" },
  { sortKey: "turnaround_score", label: "Turn.", align: "right", defaultDir: "desc" },
  { sortKey: "momentum_score", label: "Mom.", align: "right", defaultDir: "desc" },
  { sortKey: "sentiment_score", label: "Sent.", align: "right", defaultDir: "desc" },
  { sortKey: "data_completeness", label: "Compl.", align: "right", defaultDir: "desc" },
];

const COLUMN_COUNT = COLUMNS.length + 1; // +1 for the company-name column

export type RankingsView =
  | { kind: "data"; items: RankingItem[] }
  | { kind: "loading" }
  | { kind: "empty"; title: string; description: string; action?: ReactNode }
  | { kind: "error401" }
  | { kind: "error"; onRetry: () => void };

export interface RankingsTableProps {
  readonly view: RankingsView;
  readonly sort: RankingsSortField;
  readonly sortDir: "asc" | "desc";
  readonly onSortChange: (field: RankingsSortField, dir: "asc" | "desc") => void;
}

export function RankingsTable({
  view,
  sort,
  sortDir,
  onSortChange,
}: RankingsTableProps) {
  const onHeaderClick = (col: ColumnDef) => {
    if (col.sortKey === null) return;
    // Toggle direction when re-clicking the active column; otherwise apply the
    // column's default direction.
    const nextDir =
      col.sortKey === sort ? (sortDir === "asc" ? "desc" : "asc") : col.defaultDir;
    onSortChange(col.sortKey, nextDir);
  };

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase text-slate-500 dark:text-slate-400">
          <tr>
            {COLUMNS.map((col) => {
              const active = col.sortKey !== null && col.sortKey === sort;
              const indicator = active ? (sortDir === "asc" ? " ↑" : " ↓") : "";
              return (
                <th
                  key={col.label}
                  scope="col"
                  className={`px-2 py-2 ${col.align === "right" ? "text-right" : "text-left"}`}
                  aria-sort={active ? (sortDir === "asc" ? "ascending" : "descending") : "none"}
                >
                  {col.sortKey === null ? (
                    <span className="font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400">
                      {col.label}
                    </span>
                  ) : (
                    <button
                      type="button"
                      onClick={() => onHeaderClick(col)}
                      className="font-semibold uppercase tracking-wide text-slate-500 hover:text-slate-800 dark:text-slate-400 dark:hover:text-slate-200"
                    >
                      {col.label}
                      {indicator}
                    </button>
                  )}
                </th>
              );
            })}
            {/* Company name column is not sortable. */}
            <th scope="col" className="px-2 py-2 text-left">
              Company
            </th>
          </tr>
        </thead>
        <tbody>
          {view.kind === "loading" ? (
            <SkeletonRows />
          ) : view.kind === "error401" ? (
            <MessageRow>
              <div
                role="alert"
                className="rounded-md border border-amber-200 dark:border-amber-900/60 bg-amber-50 dark:bg-amber-950/40 px-4 py-3 text-sm text-amber-800 dark:text-amber-200"
              >
                Authentication required. Sign in to view rankings.
              </div>
            </MessageRow>
          ) : view.kind === "error" ? (
            <MessageRow>
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
            </MessageRow>
          ) : view.kind === "empty" ? (
            <MessageRow>
              <div className="flex flex-col items-center justify-center rounded-md border border-dashed border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-8 text-center">
                <h2 className="text-base font-semibold text-slate-700">{view.title}</h2>
                <p className="mt-1 max-w-md text-sm text-slate-500 dark:text-slate-400">{view.description}</p>
                {view.action ? <div className="mt-4">{view.action}</div> : null}
              </div>
            </MessageRow>
          ) : (
            view.items.map((item) => <RankingRow key={item.instrument_id} item={item} />)
          )}
        </tbody>
      </table>
    </div>
  );
}

function MessageRow({ children }: { children: ReactNode }) {
  return (
    <tr>
      <td colSpan={COLUMN_COUNT} className="px-2 py-6">
        {children}
      </td>
    </tr>
  );
}

function SkeletonRows() {
  return (
    <>
      {Array.from({ length: 8 }).map((_, i) => (
        <tr key={i} className="animate-pulse border-t border-slate-100">
          {Array.from({ length: COLUMN_COUNT }).map((__, j) => (
            <td key={j} className="px-2 py-2">
              <div className="h-4 rounded bg-slate-100 dark:bg-slate-800" />
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}

function RankingRow({ item }: { item: RankingItem }) {
  return (
    <tr className="border-t border-slate-100">
      <td className="px-2 py-2 text-right tabular-nums">
        {item.rank === null ? "—" : item.rank}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        <RankDeltaCell delta={item.rank_delta} />
      </td>
      <td className="px-2 py-2">
        <Link
          to={`/instrument/${encodeURIComponent(item.symbol)}`}
          className="font-medium text-blue-600 hover:underline"
        >
          {item.symbol}
        </Link>
      </td>
      <td className="px-2 py-2 text-slate-700">{item.gics_sector ?? "—"}</td>
      <td className="px-2 py-2 text-right tabular-nums">
        {item.coverage_tier === null ? "—" : item.coverage_tier}
      </td>
      <td className="px-2 py-2 text-right font-semibold tabular-nums text-slate-800 dark:text-slate-100">
        {formatScore(item.total_score)}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">{formatScore(item.quality_score)}</td>
      <td className="px-2 py-2 text-right tabular-nums">{formatScore(item.value_score)}</td>
      <td className="px-2 py-2 text-right tabular-nums">{formatScore(item.turnaround_score)}</td>
      <td className="px-2 py-2 text-right tabular-nums">{formatScore(item.momentum_score)}</td>
      <td className="px-2 py-2 text-right tabular-nums">{formatScore(item.sentiment_score)}</td>
      <td className="px-2 py-2 text-right">
        <CompletenessChip tier={item.completeness_tier} pct={item.data_completeness} />
      </td>
      <td className="px-2 py-2 text-slate-600">{item.company_name}</td>
    </tr>
  );
}

/**
 * Completeness chip (#1825). Surfaces the scoring run's data-completeness tier
 * so a high-ranked thin-coverage name is visibly flagged. `full` is muted
 * (the expected good state); `thin_data` / `insufficient_data` are warning-
 * coloured. The fraction sits in the title for the exact figure on hover.
 */
function CompletenessChip({
  tier,
  pct,
}: {
  tier: string | null;
  pct: number | null;
}) {
  if (tier === null) return <span className="text-slate-400">—</span>;
  const cls =
    tier === "insufficient_data"
      ? "bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300"
      : tier === "thin_data"
        ? "bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300"
        : "bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-400";
  const label = tier.replace(/_/g, " ");
  const title = pct === null ? label : `${label} · ${(pct * 100).toFixed(0)}% complete`;
  return (
    <span
      className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${cls}`}
      title={title}
    >
      {label}
    </span>
  );
}

function formatScore(value: number | null): string {
  // Scores are unitless heuristic numbers; show two decimals via the existing
  // formatNumber helper rather than hand-rolling a toFixed (operator-ui-
  // conventions: never hand-format numerics).
  return formatNumber(value, 2);
}
