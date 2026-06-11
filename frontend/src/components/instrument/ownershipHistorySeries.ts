/**
 * Pure helpers for the ownership history pane (#922): selection →
 * fetch-mode resolution, filer-key → holder_id mapping, time-window
 * arithmetic, and point-list → recharts-row building. No fetching,
 * no React — everything here is table-testable.
 *
 * Mode semantics (spec
 * ``docs/specs/ui/2026-06-11-ownership-history-pane.md`` D4):
 * aggregate series exist only for ``institutions`` (13F is quarterly
 * by construction) and ``treasury`` (issuer-level). Event-driven
 * categories (insiders / blockholders / def14a) chart per-holder
 * only — a per-period sum would count just the holders who happened
 * to file that period.
 */

import type {
  OwnershipHistoryCategory,
  OwnershipHistoryPoint,
} from "@/api/ownershipHistory";
import { parseShareCount } from "@/components/instrument/ownershipMetrics";

export type HistoryWindow = "1Y" | "3Y" | "5Y" | "ALL";

export const HISTORY_WINDOWS: readonly HistoryWindow[] = ["1Y", "3Y", "5Y", "ALL"];

/** Inclusive ``from_date`` for a window, ISO date; undefined = no bound. */
export function windowFromDate(window: HistoryWindow, now: Date): string | undefined {
  if (window === "ALL") return undefined;
  const years = window === "1Y" ? 1 : window === "3Y" ? 3 : 5;
  const d = new Date(Date.UTC(now.getUTCFullYear() - years, now.getUTCMonth(), now.getUTCDate()));
  return d.toISOString().slice(0, 10);
}

/**
 * Map a chart/table filer key to the history endpoint's ``holder_id``
 * (#922). Key shapes are per-category (verified against the
 * builders in OwnershipPage / rollupToSunburstInputs):
 *
 *   institutions / etfs : raw ``filer_cik``
 *   insiders (Form 4)   : ``cik`` or ``name:{name}``
 *   insiders (baseline) : ``baseline:{cik}:d|n``
 *   blockholders        : ``block:{cik|name:…}``
 *
 * ``name:`` fallbacks have no CIK → ``null`` (caller renders the
 * no-CIK empty state).
 */
export function holderIdFromFilerKey(key: string): string | null {
  let raw = key;
  if (raw.startsWith("block:")) {
    raw = raw.slice("block:".length);
  } else if (raw.startsWith("baseline:")) {
    raw = raw.split(":")[1] ?? "";
  }
  return /^\d+$/.test(raw) ? raw : null;
}

export type HistoryMode =
  | { readonly kind: "aggregate"; readonly categories: readonly ("institutions" | "treasury")[] }
  | {
      readonly kind: "holder";
      readonly category: OwnershipHistoryCategory;
      readonly holder_id: string;
    }
  | { readonly kind: "unsupported"; readonly reason: "event_driven" | "etfs" | "no_cik" };

/**
 * Resolve the L2 page's ``?category=`` / ``?filer=`` selection into a
 * fetch plan. ``?filer=`` without ``?category=`` falls back to the
 * aggregate default (a filer key alone is ambiguous across
 * categories). ``etfs`` drills through ``institutions`` — ETF leaves
 * are 13F filers in the same observations table.
 */
export function resolveHistoryMode(
  categoryFilter: string | null,
  filerFilter: string | null,
): HistoryMode {
  if (filerFilter === null || categoryFilter === null) {
    if (categoryFilter === null) return { kind: "aggregate", categories: ["institutions", "treasury"] };
    if (categoryFilter === "institutions") return { kind: "aggregate", categories: ["institutions"] };
    if (categoryFilter === "treasury") return { kind: "aggregate", categories: ["treasury"] };
    if (categoryFilter === "etfs") return { kind: "unsupported", reason: "etfs" };
    return { kind: "unsupported", reason: "event_driven" };
  }
  // Treasury is issuer-level — a filer selection cannot re-scope it.
  if (categoryFilter === "treasury") return { kind: "aggregate", categories: ["treasury"] };
  const holder_id = holderIdFromFilerKey(filerFilter);
  if (holder_id === null) return { kind: "unsupported", reason: "no_cik" };
  const category: OwnershipHistoryCategory =
    categoryFilter === "etfs"
      ? "institutions"
      : categoryFilter === "insiders"
        ? "insiders"
        : categoryFilter === "blockholders"
          ? "blockholders"
          : "institutions";
  return { kind: "holder", category, holder_id };
}

/** Stable signature for useAsync deps — modes are plain objects. */
export function historyModeSignature(mode: HistoryMode): string {
  if (mode.kind === "aggregate") return `agg:${mode.categories.join("+")}`;
  if (mode.kind === "holder") return `holder:${mode.category}:${mode.holder_id}`;
  return `unsupported:${mode.reason}`;
}

export interface HistoryLine {
  /** recharts dataKey — unique per line. */
  readonly key: string;
  readonly label: string;
  readonly points: readonly OwnershipHistoryPoint[];
}

export interface HistoryRows {
  /** One row per distinct period_end, ascending; per-line values
   *  keyed by line key, missing periods absent (recharts leaves a
   *  gap — connectNulls stays false). */
  readonly rows: ReadonlyArray<Record<string, number | string>>;
  readonly lines: ReadonlyArray<{ readonly key: string; readonly label: string }>;
}

/** Join lines on period_end into recharts rows. */
export function buildHistoryRows(lines: readonly HistoryLine[]): HistoryRows {
  const byPeriod = new Map<string, Record<string, number | string>>();
  for (const line of lines) {
    for (const p of line.points) {
      const shares = parseShareCount(p.shares);
      if (shares === null) continue;
      let row = byPeriod.get(p.period_end);
      if (row === undefined) {
        row = { period_end: p.period_end };
        byPeriod.set(p.period_end, row);
      }
      row[line.key] = shares;
    }
  }
  const rows = [...byPeriod.values()].sort((a, b) =>
    String(a["period_end"]) < String(b["period_end"]) ? -1 : 1,
  );
  return {
    rows,
    lines: lines
      .filter((l) => l.points.some((p) => parseShareCount(p.shares) !== null))
      .map((l) => ({ key: l.key, label: l.label })),
  };
}

/**
 * Split a per-holder point list into one line per ownership_nature
 * (service contract: beneficial and direct are distinct series —
 * never summed).
 */
export function linesByNature(
  points: readonly OwnershipHistoryPoint[],
  holderLabel: string,
): HistoryLine[] {
  const natures = new Map<string, OwnershipHistoryPoint[]>();
  for (const p of points) {
    const bucket = natures.get(p.ownership_nature);
    if (bucket === undefined) natures.set(p.ownership_nature, [p]);
    else bucket.push(p);
  }
  return [...natures.entries()].map(([nature, pts]) => ({
    key: `nature-${nature}`,
    label: natures.size > 1 ? `${holderLabel} (${nature})` : holderLabel,
    points: pts,
  }));
}
