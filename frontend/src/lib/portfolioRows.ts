import type { PositionItem, PortfolioMirrorItem } from "@/api/types";

/**
 * Unified position + mirror row model — one source for the RowItem union,
 * the market-value sort key, and the search predicate that PositionsTable
 * (dashboard) and PortfolioPage (workstation) each previously inlined
 * (#1901 PR-2).
 *
 * Positions and mirrors both contribute to "account worth", so they merge
 * into one list sorted by dollar value descending. Pure — table-tested
 * without React.
 */
export type RowItem =
  | { kind: "position"; data: PositionItem }
  | { kind: "mirror"; data: PortfolioMirrorItem };

/** Sort/worth key: a position is worth its market value, a mirror its equity. */
export function rowMarketValue(row: RowItem): number {
  return row.kind === "position" ? row.data.market_value : row.data.mirror_equity;
}

/** Merge positions + mirrors into one list sorted by market value descending. */
export function buildSortedRows(
  positions: PositionItem[],
  mirrors: PortfolioMirrorItem[] = [],
): RowItem[] {
  const rows: RowItem[] = [
    ...positions.map((data) => ({ kind: "position" as const, data })),
    ...mirrors.map((data) => ({ kind: "mirror" as const, data })),
  ];
  rows.sort((a, b) => rowMarketValue(b) - rowMarketValue(a));
  return rows;
}

/** Case-insensitive match: position by symbol/company, mirror by trader name. */
export function matchesRowSearch(row: RowItem, query: string): boolean {
  if (!query) return true;
  const lower = query.toLowerCase();
  if (row.kind === "position") {
    return (
      row.data.symbol.toLowerCase().includes(lower) ||
      row.data.company_name.toLowerCase().includes(lower)
    );
  }
  return row.data.parent_username.toLowerCase().includes(lower);
}
