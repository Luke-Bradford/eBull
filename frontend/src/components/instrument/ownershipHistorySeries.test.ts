/**
 * Pure-helper tests for the ownership history pane (#922): mode
 * resolution, filer-key → holder_id mapping, window arithmetic, and
 * row building.
 */

import { describe, expect, it } from "vitest";

import type { AggregateCoverage, OwnershipHistoryPoint } from "@/api/ownershipHistory";

import {
  buildHistoryRows,
  coverageCaption,
  holderIdFromFilerKey,
  linesByNature,
  resolveHistoryMode,
  windowFromDate,
} from "./ownershipHistorySeries";

function point(overrides: Partial<OwnershipHistoryPoint>): OwnershipHistoryPoint {
  return {
    period_end: "2026-03-31",
    ownership_nature: "economic",
    shares: "100",
    source: "13f",
    source_accession: null,
    filed_at: null,
    holder_count: null,
    ...overrides,
  };
}

describe("windowFromDate", () => {
  const now = new Date(Date.UTC(2026, 5, 11)); // 2026-06-11
  it("subtracts whole years", () => {
    expect(windowFromDate("1Y", now)).toBe("2025-06-11");
    expect(windowFromDate("3Y", now)).toBe("2023-06-11");
    expect(windowFromDate("5Y", now)).toBe("2021-06-11");
  });
  it("ALL has no bound", () => {
    expect(windowFromDate("ALL", now)).toBeUndefined();
  });
});

describe("holderIdFromFilerKey", () => {
  it("maps per-category key shapes to a CIK", () => {
    expect(holderIdFromFilerKey("0000102909")).toBe("0000102909");
    expect(holderIdFromFilerKey("block:0001767470")).toBe("0001767470");
    expect(holderIdFromFilerKey("baseline:0001214156:d")).toBe("0001214156");
    expect(holderIdFromFilerKey("baseline:0001214156:n")).toBe("0001214156");
  });
  it("rejects name fallbacks anywhere", () => {
    expect(holderIdFromFilerKey("name:Cohen Ryan")).toBeNull();
    expect(holderIdFromFilerKey("block:name:Icahn Carl")).toBeNull();
    expect(holderIdFromFilerKey("")).toBeNull();
  });
});

describe("resolveHistoryMode", () => {
  it("defaults to the institutions + treasury aggregate", () => {
    expect(resolveHistoryMode(null, null)).toEqual({
      kind: "aggregate",
      categories: ["institutions", "treasury"],
    });
    // ?filer= without ?category= is ambiguous — filer ignored.
    expect(resolveHistoryMode(null, "0000102909")).toEqual({
      kind: "aggregate",
      categories: ["institutions", "treasury"],
    });
  });

  it("scopes the aggregate to a selected aggregable category", () => {
    expect(resolveHistoryMode("institutions", null)).toEqual({
      kind: "aggregate",
      categories: ["institutions"],
    });
    expect(resolveHistoryMode("treasury", null)).toEqual({
      kind: "aggregate",
      categories: ["treasury"],
    });
    // Treasury is issuer-level — filer selection cannot re-scope it.
    expect(resolveHistoryMode("treasury", "whatever")).toEqual({
      kind: "aggregate",
      categories: ["treasury"],
    });
  });

  it("marks event-driven and etf categories unsupported without a filer", () => {
    expect(resolveHistoryMode("insiders", null)).toEqual({
      kind: "unsupported",
      reason: "event_driven",
    });
    expect(resolveHistoryMode("blockholders", null)).toEqual({
      kind: "unsupported",
      reason: "event_driven",
    });
    expect(resolveHistoryMode("etfs", null)).toEqual({
      kind: "unsupported",
      reason: "etfs",
    });
  });

  it("resolves per-holder modes, drilling etfs through institutions", () => {
    expect(resolveHistoryMode("insiders", "0001767470")).toEqual({
      kind: "holder",
      category: "insiders",
      holder_id: "0001767470",
    });
    expect(resolveHistoryMode("blockholders", "block:0001767470")).toEqual({
      kind: "holder",
      category: "blockholders",
      holder_id: "0001767470",
    });
    expect(resolveHistoryMode("etfs", "0000036405")).toEqual({
      kind: "holder",
      category: "institutions",
      holder_id: "0000036405",
    });
  });

  it("flags no-CIK holders", () => {
    expect(resolveHistoryMode("insiders", "name:Cohen Ryan")).toEqual({
      kind: "unsupported",
      reason: "no_cik",
    });
  });

  it("treats unknown/typo categories as no selection, never institutions (Codex ckpt-2 S2)", () => {
    expect(resolveHistoryMode("garbage", "0000102909")).toEqual({
      kind: "aggregate",
      categories: ["institutions", "treasury"],
    });
    expect(resolveHistoryMode("garbage", null)).toEqual({
      kind: "aggregate",
      categories: ["institutions", "treasury"],
    });
    // def14a is event-driven AND name-keyed — never drillable.
    expect(resolveHistoryMode("def14a", "0000102909")).toEqual({
      kind: "unsupported",
      reason: "event_driven",
    });
    expect(resolveHistoryMode("def14a", null)).toEqual({
      kind: "unsupported",
      reason: "event_driven",
    });
  });
});

describe("linesByNature", () => {
  it("splits natures into separate lines, never summing", () => {
    const lines = linesByNature(
      [
        point({ ownership_nature: "direct", shares: "100" }),
        point({ ownership_nature: "indirect", shares: "50" }),
        point({ ownership_nature: "direct", shares: "120", period_end: "2026-06-30" }),
      ],
      "Cohen Ryan",
    );
    expect(lines).toHaveLength(2);
    const direct = lines.find((l) => l.key === "nature-direct");
    expect(direct?.label).toBe("Cohen Ryan (direct)");
    expect(direct?.points).toHaveLength(2);
  });

  it("omits the nature suffix for single-nature holders", () => {
    const lines = linesByNature([point({})], "Vanguard");
    expect(lines[0]?.label).toBe("Vanguard");
  });
});

describe("buildHistoryRows", () => {
  it("joins lines on period_end ascending, leaving gaps absent", () => {
    const { rows, lines } = buildHistoryRows([
      {
        key: "a",
        label: "A",
        points: [
          point({ period_end: "2026-03-31", shares: "10" }),
          point({ period_end: "2025-12-31", shares: "8" }),
        ],
      },
      { key: "b", label: "B", points: [point({ period_end: "2026-03-31", shares: "5" })] },
    ]);
    expect(rows).toEqual([
      { period_end: "2025-12-31", a: 8 },
      { period_end: "2026-03-31", a: 10, b: 5 },
    ]);
    expect(lines.map((l) => l.key)).toEqual(["a", "b"]);
  });

  it("drops lines with no parseable points from the legend", () => {
    const { lines } = buildHistoryRows([
      { key: "a", label: "A", points: [point({ shares: null })] },
      { key: "b", label: "B", points: [point({ shares: "5" })] },
    ]);
    expect(lines.map((l) => l.key)).toEqual(["b"]);
  });
});

function cov(partial: Partial<AggregateCoverage>): AggregateCoverage {
  return {
    bucket_count: 1,
    as_of_min: "2025-12-31",
    as_of_max: "2025-12-31",
    holder_count_min: null,
    holder_count_max: null,
    holder_count_latest: null,
    ...partial,
  };
}

describe("coverageCaption (#1648)", () => {
  it("returns null when coverage is absent", () => {
    expect(coverageCaption(null)).toBeNull();
  });

  it("returns null for an issuer-level (treasury) series — null holder counts", () => {
    expect(
      coverageCaption(cov({ bucket_count: 3, holder_count_min: null, holder_count_max: null })),
    ).toBeNull();
  });

  it("surfaces the filer spread across quarters", () => {
    const caption = coverageCaption(
      cov({ bucket_count: 5, holder_count_min: 209, holder_count_max: 6011, holder_count_latest: 6011 }),
    );
    expect(caption).not.toBeNull();
    expect(caption).toContain("209");
    expect(caption).toContain("6,011"); // thousands separator
    expect(caption).toContain("5 quarters");
    expect(caption).toContain("filing coverage, not net flow");
  });

  it("pluralises a single quarter and collapses an equal spread", () => {
    const caption = coverageCaption(
      cov({ bucket_count: 1, holder_count_min: 5, holder_count_max: 5, holder_count_latest: 5 }),
    );
    expect(caption).toContain("5 filers"); // not "5–5 filers"
    expect(caption).not.toContain("–");
    expect(caption).toContain("1 quarter");
    expect(caption).not.toContain("1 quarters");
  });
});
