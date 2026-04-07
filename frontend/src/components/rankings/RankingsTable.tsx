import { useMemo, useState, type ReactNode } from "react";
import { Link } from "react-router-dom";
import type { RankingItem } from "@/api/types";
import { formatNumber } from "@/lib/format";
import { RankDeltaCell } from "@/components/rankings/RankDeltaCell";

/**
 * Rankings table.
 *
 * One component, five render branches, one stable `<table>` element. The
 * `<thead>` column set is defined exactly once in COLUMNS and rendered in
 * every branch so the layout does not shift between loading / empty /
 * error / data states (the loading-error-empty-states skill rule, plus
 * the #89 / #90 prevention finding).
 *
 * Non-data branches render their content as a single `<tr><td colSpan>`
 * row inside the same `<tbody>` so the markup stays valid HTML — never a
 * `<div>` swapped in alongside the table.
 *
 * Sort state lives in this component, not the page. Sorting is purely
 * client-side; changing sort never triggers a refetch. The dataset is
 * capped at 200 rows by RANKINGS_PAGE_LIMIT so an in-memory sort is fine.
 */

type SortKey =
  | "rank"
  | "symbol"
  | "sector"
  | "coverage_tier"
  | "total_score"
  | "quality_score"
  | "value_score"
  | "turnaround_score"
  | "momentum_score"
  | "sentiment_score"
  | "rank_delta";

interface ColumnDef {
  key: SortKey;
  label: string;
  align: "left" | "right";
  // Default sort direction when this column is first clicked. For numeric
  // score columns we want highest first; for rank we want best (lowest)
  // first.
  defaultDir: "asc" | "desc";
}

const COLUMNS: ReadonlyArray<ColumnDef> = [
  { key: "rank", label: "Rank", align: "right", defaultDir: "asc" },
  { key: "rank_delta", label: "Δ", align: "right", defaultDir: "asc" },
  { key: "symbol", label: "Symbol", align: "left", defaultDir: "asc" },
  { key: "sector", label: "Sector", align: "left", defaultDir: "asc" },
  { key: "coverage_tier", label: "Tier", align: "right", defaultDir: "asc" },
  { key: "total_score", label: "Total", align: "right", defaultDir: "desc" },
  { key: "quality_score", label: "Quality", align: "right", defaultDir: "desc" },
  { key: "value_score", label: "Value", align: "right", defaultDir: "desc" },
  { key: "turnaround_score", label: "Turn.", align: "right", defaultDir: "desc" },
  { key: "momentum_score", label: "Mom.", align: "right", defaultDir: "desc" },
  { key: "sentiment_score", label: "Sent.", align: "right", defaultDir: "desc" },
];

const COLUMN_COUNT = COLUMNS.length + 1; // +1 for the company-name column

export type RankingsView =
  | { kind: "data"; items: RankingItem[] }
  | { kind: "loading" }
  | { kind: "empty"; title: string; description: string; action?: ReactNode }
  | { kind: "error401" }
  | { kind: "error"; onRetry: () => void };

export function RankingsTable({ view }: { view: RankingsView }) {
  const [sortKey, setSortKey] = useState<SortKey>("rank");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");

  const sortedItems = useMemo(() => {
    if (view.kind !== "data") return [];
    return sortItems(view.items, sortKey, sortDir);
  }, [view, sortKey, sortDir]);

  const onHeaderClick = (col: ColumnDef) => {
    if (col.key === sortKey) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(col.key);
      setSortDir(col.defaultDir);
    }
  };

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase text-slate-500">
          <tr>
            {COLUMNS.map((col) => {
              const active = col.key === sortKey;
              const indicator = active ? (sortDir === "asc" ? " ↑" : " ↓") : "";
              return (
                <th
                  key={col.key}
                  scope="col"
                  className={`px-2 py-2 ${col.align === "right" ? "text-right" : "text-left"}`}
                  aria-sort={active ? (sortDir === "asc" ? "ascending" : "descending") : "none"}
                >
                  <button
                    type="button"
                    onClick={() => onHeaderClick(col)}
                    className="font-semibold uppercase tracking-wide text-slate-500 hover:text-slate-800"
                  >
                    {col.label}
                    {indicator}
                  </button>
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
                className="rounded-md border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800"
              >
                Authentication required. Sign in to view rankings.
              </div>
            </MessageRow>
          ) : view.kind === "error" ? (
            <MessageRow>
              <div
                role="alert"
                className="flex items-center justify-between rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
              >
                <span>Failed to load. Check the browser console for details.</span>
                <button
                  type="button"
                  onClick={view.onRetry}
                  className="rounded border border-red-300 bg-white px-2 py-1 text-xs font-medium text-red-700 hover:bg-red-100"
                >
                  Retry
                </button>
              </div>
            </MessageRow>
          ) : view.kind === "empty" ? (
            <MessageRow>
              <div className="flex flex-col items-center justify-center rounded-md border border-dashed border-slate-200 bg-white p-8 text-center">
                <h2 className="text-base font-semibold text-slate-700">{view.title}</h2>
                <p className="mt-1 max-w-md text-sm text-slate-500">{view.description}</p>
                {view.action ? <div className="mt-4">{view.action}</div> : null}
              </div>
            </MessageRow>
          ) : (
            sortedItems.map((item) => <RankingRow key={item.instrument_id} item={item} />)
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
              <div className="h-4 rounded bg-slate-100" />
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
          to={`/instruments/${item.instrument_id}`}
          className="font-medium text-blue-600 hover:underline"
        >
          {item.symbol}
        </Link>
      </td>
      <td className="px-2 py-2 text-slate-700">{item.sector ?? "—"}</td>
      <td className="px-2 py-2 text-right tabular-nums">
        {item.coverage_tier === null ? "—" : item.coverage_tier}
      </td>
      <td className="px-2 py-2 text-right font-semibold tabular-nums text-slate-800">
        {formatScore(item.total_score)}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">{formatScore(item.quality_score)}</td>
      <td className="px-2 py-2 text-right tabular-nums">{formatScore(item.value_score)}</td>
      <td className="px-2 py-2 text-right tabular-nums">{formatScore(item.turnaround_score)}</td>
      <td className="px-2 py-2 text-right tabular-nums">{formatScore(item.momentum_score)}</td>
      <td className="px-2 py-2 text-right tabular-nums">{formatScore(item.sentiment_score)}</td>
      <td className="px-2 py-2 text-slate-600">{item.company_name}</td>
    </tr>
  );
}

function formatScore(value: number | null): string {
  // Scores are unitless heuristic numbers in the 0..100-ish range; show two
  // decimals via the existing formatNumber helper rather than hand-rolling
  // a toFixed (operator-ui-conventions: never hand-format numerics).
  return formatNumber(value, 2);
}

// ---------------------------------------------------------------------------
// Sort
// ---------------------------------------------------------------------------

/**
 * Sort RankingItems by the given key. NULL values always sort to the end
 * regardless of direction — this matches the server's `ORDER BY rank ASC
 * NULLS LAST` and means an unscored instrument never displaces a scored
 * one to the top of the table.
 */
function sortItems(
  items: ReadonlyArray<RankingItem>,
  key: SortKey,
  dir: "asc" | "desc",
): RankingItem[] {
  const copy = items.slice();
  const mult = dir === "asc" ? 1 : -1;
  copy.sort((a, b) => {
    const av = a[key];
    const bv = b[key];
    if (av === null && bv === null) return 0;
    if (av === null) return 1;
    if (bv === null) return -1;
    if (typeof av === "number" && typeof bv === "number") {
      return (av - bv) * mult;
    }
    // String compare for symbol / sector.
    return String(av).localeCompare(String(bv)) * mult;
  });
  return copy;
}
