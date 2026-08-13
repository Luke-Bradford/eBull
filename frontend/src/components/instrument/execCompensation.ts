/**
 * Pure presentation helpers for the ExecCompensationPanel (#1969).
 *
 * The /exec-compensation endpoint returns a FLAT list of (executive,
 * fiscal_year) SCT rows ordered fiscal_year DESC, total_comp DESC
 * (17 CFR §229.402(c)). The panel renders them GROUPED by executive —
 * name + position once, up to three fiscal-year sub-rows beneath — which
 * is the conventional proxy Summary-Compensation-Table layout. This
 * module owns that grouping (pure, table-tested; the panel stays a thin
 * render wrapper).
 */

import type { ExecCompRow } from "@/api/instruments";

export interface ExecCompGroup {
  readonly executive_name: string;
  /** Position from the executive's most-recent fiscal-year row. */
  readonly principal_position: string | null;
  /** The executive's rows, one per fiscal year, ordered fiscal_year DESC. */
  readonly years: readonly ExecCompRow[];
}

/** Parse a Decimal-as-string comp cell to a finite number, or null.
 *  Empty/absent cells and non-numeric strings both collapse to null so
 *  callers render the "—" sentinel rather than "NaN"/"$0". */
export function parseComp(value: string | null): number | null {
  if (value === null) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

/**
 * Group flat SCT rows by executive.
 *
 * - Within each executive: rows ordered fiscal_year DESC (latest first).
 * - Executives ordered by their most-recent-fiscal-year total_comp DESC
 *   (highest-paid NEO first — typically the CEO), matching the SCT's own
 *   ordering intent. Executives whose latest row has a null total sort
 *   last; ties fall back to first-appearance order in `rows` (stable),
 *   so the ordering is deterministic regardless of Map iteration nuances.
 */
export function groupExecComp(rows: readonly ExecCompRow[]): ExecCompGroup[] {
  const byExec = new Map<string, ExecCompRow[]>();
  const firstSeen = new Map<string, number>();
  rows.forEach((row, index) => {
    const bucket = byExec.get(row.executive_name);
    if (bucket !== undefined) {
      bucket.push(row);
    } else {
      byExec.set(row.executive_name, [row]);
      firstSeen.set(row.executive_name, index);
    }
  });

  const groups = Array.from(byExec, ([name, execRows]) => {
    const years = [...execRows].sort((a, b) => b.fiscal_year - a.fiscal_year);
    return {
      executive_name: name,
      principal_position: years[0]?.principal_position ?? null,
      years,
    };
  });

  return groups.sort((a, b) => {
    const ta = parseComp(a.years[0]?.total_comp ?? null);
    const tb = parseComp(b.years[0]?.total_comp ?? null);
    if (ta !== null && tb !== null && ta !== tb) return tb - ta;
    if ((ta === null) !== (tb === null)) return ta === null ? 1 : -1;
    return (
      (firstSeen.get(a.executive_name) ?? 0) -
      (firstSeen.get(b.executive_name) ?? 0)
    );
  });
}
