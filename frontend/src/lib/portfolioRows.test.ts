import { describe, expect, it } from "vitest";
import type { PositionItem, PortfolioMirrorItem } from "@/api/types";
import {
  buildSortedRows,
  matchesRowSearch,
  rowMarketValue,
  type RowItem,
} from "@/lib/portfolioRows";

function position(
  instrumentId: number,
  symbol: string,
  overrides: Partial<PositionItem> = {},
): PositionItem {
  return {
    instrument_id: instrumentId,
    symbol,
    company_name: `${symbol} Inc.`,
    open_date: "2026-01-01",
    avg_cost: 100,
    current_price: 110,
    current_units: 1,
    cost_basis: 100,
    market_value: 110,
    unrealized_pnl: 10,
    valuation_source: "quote",
    source: "broker",
    updated_at: "2026-04-18T00:00:00Z",
    currency: "GBP",
    trades: [],
    ...overrides,
  };
}

function mirror(
  mirrorId: number,
  parentUsername: string,
  overrides: Partial<PortfolioMirrorItem> = {},
): PortfolioMirrorItem {
  return {
    mirror_id: mirrorId,
    parent_username: parentUsername,
    active: true,
    funded: 1000,
    mirror_equity: 1200,
    unrealized_pnl: 200,
    position_count: 5,
    started_copy_date: "2026-01-01",
    ...overrides,
  };
}

describe("rowMarketValue", () => {
  it("uses market_value for positions, mirror_equity for mirrors", () => {
    expect(
      rowMarketValue({ kind: "position", data: position(1, "AAA", { market_value: 42 }) }),
    ).toBe(42);
    expect(
      rowMarketValue({ kind: "mirror", data: mirror(9, "@x", { mirror_equity: 77 }) }),
    ).toBe(77);
  });
});

describe("buildSortedRows", () => {
  it("merges positions + mirrors and sorts by worth descending", () => {
    const rows = buildSortedRows(
      [
        position(1, "AAA", { market_value: 100 }),
        position(2, "BBB", { market_value: 300 }),
      ],
      [mirror(9, "@x", { mirror_equity: 200 })],
    );
    expect(rows.map((r) => rowMarketValue(r))).toEqual([300, 200, 100]);
    // The 200 slot is the mirror interleaved between the two positions.
    expect(rows[1]?.kind).toBe("mirror");
  });

  it("defaults mirrors to empty", () => {
    const rows = buildSortedRows([position(1, "AAA")]);
    expect(rows).toHaveLength(1);
    expect(rows[0]?.kind).toBe("position");
  });

  it("returns an empty list for no positions or mirrors", () => {
    expect(buildSortedRows([], [])).toEqual([]);
  });
});

describe("matchesRowSearch", () => {
  const pos: RowItem = { kind: "position", data: position(1, "AAPL") };
  const mir: RowItem = { kind: "mirror", data: mirror(9, "@gurutrader") };

  it("matches everything on an empty query", () => {
    expect(matchesRowSearch(pos, "")).toBe(true);
    expect(matchesRowSearch(mir, "")).toBe(true);
  });

  it("matches a position by symbol or company, case-insensitively", () => {
    expect(matchesRowSearch(pos, "aapl")).toBe(true);
    expect(matchesRowSearch(pos, "inc")).toBe(true);
    expect(matchesRowSearch(pos, "msft")).toBe(false);
  });

  it("matches a mirror by trader username", () => {
    expect(matchesRowSearch(mir, "GURU")).toBe(true);
    expect(matchesRowSearch(mir, "aapl")).toBe(false);
  });
});
