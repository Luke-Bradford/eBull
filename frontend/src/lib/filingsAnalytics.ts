/**
 * Pure aggregation for the filings-analytics drill (#592). Consumes the
 * server's per-(quarter, filing_type) counts (`/filings/{id}/quarterly-counts`)
 * and shapes them for the density timeline + the form-type heatmap.
 *
 * Insider Forms (3/4/5/144) are categorised but EXCLUDED from both charts: they
 * are routine and high-volume (an active issuer files hundreds of Form 4s,
 * ~80% of the bar), which buries the material-filing pattern these charts exist
 * to surface. Insider activity has its own drill (#588). The page shows a caveat.
 */
import type { FilingQuarterCount } from "@/api/types";

/** Material-filing categories shown in both charts (insider excluded). */
export const DENSITY_CATEGORIES = ["10-K", "10-Q", "8-K", "Proxy", "13D/G", "Other"] as const;
export type DensityCategory = (typeof DENSITY_CATEGORIES)[number];
export type FilingCategory = DensityCategory | "Insider";

/** Map a raw SEC `filing_type` to a chart category. */
export function categorizeFiling(filingType: string): FilingCategory {
  const t = filingType.trim().toUpperCase();
  if (t.startsWith("10-K")) return "10-K"; // 10-K, 10-K/A, 10-KSB
  if (t.startsWith("10-Q")) return "10-Q"; // 10-Q, 10-Q/A
  if (t.startsWith("8-K")) return "8-K"; // 8-K, 8-K/A
  if (t.includes("14A")) return "Proxy"; // DEF 14A, DEFA14A, PRE 14A
  if (t.includes("13D") || t.includes("13G")) return "13D/G"; // SC 13D/G, SCHEDULE 13D/G
  if (t === "144" || /^[345](\/A)?$/.test(t)) return "Insider"; // Forms 3/4/5 (+/A), 144
  return "Other"; // 424B*, 6-K, 20-F, S-1/S-3, NT, FWP, CORRESP, 11-K, ...
}

export interface DensityRow {
  readonly quarter: string;
  readonly "10-K": number;
  readonly "10-Q": number;
  readonly "8-K": number;
  readonly Proxy: number;
  readonly "13D/G": number;
  readonly Other: number;
  readonly total: number;
}

// "YYYY-Qn" <-> ordinal, for generating a gap-free quarter axis.
function quarterOrdinal(q: string): number {
  const [y, qq] = q.split("-Q");
  return Number(y) * 4 + (Number(qq) - 1);
}
function ordinalToQuarter(o: number): string {
  return `${Math.floor(o / 4)}-Q${(o % 4) + 1}`;
}

/** Continuous quarter axis from the earliest to the latest present quarter
 *  (inclusive), so a quiet/empty quarter renders as a gap, not a skipped
 *  column — clusters and droughts must read true. */
export function quarterRange(quarters: ReadonlyArray<string>): string[] {
  if (quarters.length === 0) return [];
  const ords = quarters.map(quarterOrdinal);
  const lo = Math.min(...ords);
  const hi = Math.max(...ords);
  const out: string[] = [];
  for (let o = lo; o <= hi; o++) out.push(ordinalToQuarter(o));
  return out;
}

function aggregate(
  counts: ReadonlyArray<FilingQuarterCount>,
): { byCell: Map<string, number>; axis: string[]; max: number } {
  const byCell = new Map<string, number>(); // `${category}|${quarter}` -> count
  const present = new Set<string>(); // all quarters with ANY filing (incl insider)
  let max = 0;
  for (const c of counts) {
    present.add(c.quarter);
    const cat = categorizeFiling(c.filing_type);
    if (cat === "Insider") continue;
    const key = `${cat}|${c.quarter}`;
    const v = (byCell.get(key) ?? 0) + c.count;
    byCell.set(key, v);
    if (v > max) max = v;
  }
  return { byCell, axis: quarterRange([...present]), max };
}

export function buildDensity(counts: ReadonlyArray<FilingQuarterCount>): DensityRow[] {
  const { byCell, axis } = aggregate(counts);
  const at = (cat: DensityCategory, q: string): number => byCell.get(`${cat}|${q}`) ?? 0;
  return axis.map((q) => {
    const cells = {
      "10-K": at("10-K", q),
      "10-Q": at("10-Q", q),
      "8-K": at("8-K", q),
      Proxy: at("Proxy", q),
      "13D/G": at("13D/G", q),
      Other: at("Other", q),
    };
    const total = Object.values(cells).reduce((a, b) => a + b, 0);
    return { quarter: q, ...cells, total };
  });
}

export interface FilingHeatmap {
  readonly quarters: string[];
  readonly categories: readonly DensityCategory[];
  /** Count for a (category, quarter) cell; 0 if none. */
  readonly get: (category: DensityCategory, quarter: string) => number;
  /** Largest single-cell count, for the colour scale (0 when empty). */
  readonly max: number;
}

export function buildHeatmap(counts: ReadonlyArray<FilingQuarterCount>): FilingHeatmap {
  const { byCell, axis, max } = aggregate(counts);
  return {
    quarters: axis,
    categories: DENSITY_CATEGORIES,
    get: (cat, q) => byCell.get(`${cat}|${q}`) ?? 0,
    max,
  };
}
