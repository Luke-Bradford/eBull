import { describe, expect, it } from "vitest";

import { buildSunburstRings, visibilityThreshold } from "./ownershipRings";
import type { SunburstHolder, SunburstInputs } from "./ownershipRings";

const DEFAULT_INPUT: SunburstInputs = {
  total_shares: 1_000_000_000,
  holders: [],
  institutions_total: null,
  etfs_total: null,
  insiders_total: null,
  treasury_shares: null,
};

function holder(
  key: string,
  shares: number,
  category: SunburstHolder["category"] = "institutions",
): SunburstHolder {
  return { key, label: key, shares, category };
}

describe("visibilityThreshold", () => {
  it("returns 0.5% of denominator for normal-cap counts", () => {
    expect(visibilityThreshold(1_000_000_000)).toBe(5_000_000);
    expect(visibilityThreshold(15_000_000_000)).toBe(75_000_000);
  });

  it("clamps to 10,000-share floor for micro-cap counts", () => {
    expect(visibilityThreshold(1_000_000)).toBe(10_000);
    expect(visibilityThreshold(0)).toBe(10_000);
    expect(visibilityThreshold(-50_000)).toBe(10_000);
  });
});

describe("buildSunburstRings — denominator + null handling", () => {
  it("returns null on missing or non-positive total_shares", () => {
    expect(buildSunburstRings({ ...DEFAULT_INPUT, total_shares: 0 })).toBeNull();
    expect(buildSunburstRings({ ...DEFAULT_INPUT, total_shares: -1 })).toBeNull();
    expect(buildSunburstRings({ ...DEFAULT_INPUT, total_shares: NaN })).toBeNull();
  });

  it("preserves total_shares as the denominator and exposes reported_total", () => {
    const r = buildSunburstRings({ ...DEFAULT_INPUT });
    expect(r!.total_shares).toBe(1_000_000_000);
    expect(r!.reported_total).toBe(1_000_000_000);
  });

  it("renders no categories when every total is null/zero", () => {
    const r = buildSunburstRings({ ...DEFAULT_INPUT });
    expect(r!.categories).toEqual([]);
    expect(r!.category_residual).toBe(1_000_000_000);
  });
});

describe("buildSunburstRings — category sizing", () => {
  it("includes a category when its total is positive", () => {
    const r = buildSunburstRings({
      ...DEFAULT_INPUT,
      institutions_total: 500_000_000,
      holders: [holder("vanguard", 200_000_000, "institutions")],
    });
    const inst = r!.categories.find((c) => c.key === "institutions")!;
    expect(inst.shares).toBe(500_000_000);
    expect(inst.reported_total).toBe(500_000_000);
  });

  it("category_residual equals denom minus sum of category totals", () => {
    const r = buildSunburstRings({
      ...DEFAULT_INPUT,
      institutions_total: 500_000_000,
      etfs_total: 200_000_000,
      insiders_total: 50_000_000,
      treasury_shares: 30_000_000,
      holders: [
        holder("inst1", 200_000_000, "institutions"),
        holder("etf1", 100_000_000, "etfs"),
        holder("officer", 30_000_000, "insiders"),
      ],
    });
    // 1B − (500M + 200M + 50M + 30M) = 220M residual.
    expect(r!.category_residual).toBe(220_000_000);
  });

  it("category_residual stays at total_shares when no categories render", () => {
    const r = buildSunburstRings({ ...DEFAULT_INPUT });
    expect(r!.category_residual).toBe(1_000_000_000);
  });

  it("treasury renders as a single-leaf category", () => {
    const r = buildSunburstRings({
      ...DEFAULT_INPUT,
      treasury_shares: 50_000_000,
    });
    const treasury = r!.categories.find((c) => c.key === "treasury")!;
    expect(treasury.shares).toBe(50_000_000);
    expect(treasury.leaves).toHaveLength(1);
    expect(treasury.within_category_gap).toBe(0);
  });
});

describe("buildSunburstRings — within-category gaps", () => {
  it("renders within_category_gap = total − resolved when filers are incomplete", () => {
    // Institutions total = 500M; we resolve only 350M to named filers
    // (CUSIP-backfill gap). Outer ring should leave 150M empty.
    const r = buildSunburstRings({
      ...DEFAULT_INPUT,
      institutions_total: 500_000_000,
      holders: [
        holder("vanguard", 200_000_000, "institutions"),
        holder("blackrock", 150_000_000, "institutions"),
      ],
    });
    const inst = r!.categories.find((c) => c.key === "institutions")!;
    expect(inst.shares).toBe(500_000_000);
    expect(inst.resolved_leaf_shares).toBe(350_000_000);
    expect(inst.within_category_gap).toBe(150_000_000);
  });

  it("within_category_gap is zero when filers fully cover the total", () => {
    const r = buildSunburstRings({
      ...DEFAULT_INPUT,
      institutions_total: 350_000_000,
      holders: [
        holder("vanguard", 200_000_000, "institutions"),
        holder("blackrock", 150_000_000, "institutions"),
      ],
    });
    const inst = r!.categories.find((c) => c.key === "institutions")!;
    expect(inst.within_category_gap).toBe(0);
  });

  it("snapshot-lag: bumps category shares to sum(leaves) when filers oversubscribe", () => {
    // 13F filer detail can be slightly newer than the totals
    // snapshot — a holder may report shares the aggregate doesn't
    // yet reflect. ``shares`` becomes max(reported, sum_of_leaves)
    // so ring 3 fits inside ring 2; ``reported_total`` preserved for
    // diagnostics; within_category_gap becomes 0.
    const r = buildSunburstRings({
      ...DEFAULT_INPUT,
      institutions_total: 100_000_000,
      holders: [holder("vanguard", 120_000_000, "institutions")],
    });
    const inst = r!.categories.find((c) => c.key === "institutions")!;
    expect(inst.shares).toBe(120_000_000);
    expect(inst.reported_total).toBe(100_000_000);
    expect(inst.resolved_leaf_shares).toBe(120_000_000);
    expect(inst.within_category_gap).toBe(0);
  });

  it("within_category_gap = total when API reports a total but no per-filer detail", () => {
    // Common case: institutional total known via reader endpoint
    // but CUSIP-backfill (#740) hasn't run, filers list empty.
    // Whole category renders as a transparent within-category gap.
    const r = buildSunburstRings({
      ...DEFAULT_INPUT,
      institutions_total: 500_000_000,
      holders: [],
    });
    const inst = r!.categories.find((c) => c.key === "institutions")!;
    expect(inst.shares).toBe(500_000_000);
    expect(inst.resolved_leaf_shares).toBe(0);
    expect(inst.within_category_gap).toBe(500_000_000);
    expect(inst.leaves).toHaveLength(0);
  });
});

describe("buildSunburstRings — cross-category oversubscription", () => {
  it("bumps total_shares to sum_known when category totals exceed input denominator", () => {
    // Outstanding lag: XBRL period-end is slightly older than the
    // 13F snapshot, and reported institutional + insider shares
    // exceed the recorded outstanding. Bumping the effective denom
    // prevents Recharts from renormalising ring 2 to 360° at the
    // wrong proportions.
    const r = buildSunburstRings({
      ...DEFAULT_INPUT,
      total_shares: 1_000_000_000,
      institutions_total: 800_000_000,
      etfs_total: 300_000_000,
      holders: [
        holder("vanguard", 800_000_000, "institutions"),
        holder("spdr", 300_000_000, "etfs"),
      ],
    });
    expect(r!.reported_total).toBe(1_000_000_000);
    expect(r!.total_shares).toBe(1_100_000_000);
    expect(r!.category_residual).toBe(0);
  });

  it("leaves total_shares unchanged when categories fit within reported_total", () => {
    const r = buildSunburstRings({
      ...DEFAULT_INPUT,
      institutions_total: 500_000_000,
    });
    expect(r!.total_shares).toBe(1_000_000_000);
    expect(r!.reported_total).toBe(1_000_000_000);
    expect(r!.category_residual).toBe(500_000_000);
  });
});

describe("buildSunburstRings — outer-ring threshold grouping", () => {
  it("filer above threshold gets its own outer-ring wedge", () => {
    const r = buildSunburstRings({
      ...DEFAULT_INPUT,
      institutions_total: 100_000_000,
      holders: [holder("vanguard", 100_000_000, "institutions")],
    });
    const inst = r!.categories.find((c) => c.key === "institutions")!;
    expect(inst.leaves).toHaveLength(1);
    expect(inst.leaves[0]!.key).toBe("vanguard");
  });

  it("sub-threshold filers aggregate into 'Other' with tail metadata", () => {
    const r = buildSunburstRings({
      ...DEFAULT_INPUT,
      institutions_total: 102_000_000,
      holders: [
        holder("vanguard", 100_000_000, "institutions"),
        holder("small1", 1_000_000, "institutions"),
        holder("small2", 800_000, "institutions"),
        holder("small3", 200_000, "institutions"),
      ],
    });
    const inst = r!.categories.find((c) => c.key === "institutions")!;
    expect(inst.leaves).toHaveLength(2);
    const other = inst.leaves[1]!;
    expect(other.is_other).toBe(true);
    expect(other.shares).toBe(2_000_000);
    expect(other.tail_meta!.count).toBe(3);
    expect(other.tail_meta!.largest_label).toBe("small1");
  });

  it("insiders bypass the threshold — every officer surfaces", () => {
    const r = buildSunburstRings({
      ...DEFAULT_INPUT,
      insiders_total: 1_600_000,
      holders: [
        holder("ceo", 1_000_000, "insiders"),
        holder("cfo", 500_000, "insiders"),
        holder("cto", 100_000, "insiders"),
      ],
    });
    const insiders = r!.categories.find((c) => c.key === "insiders")!;
    expect(insiders.leaves).toHaveLength(3);
    expect(insiders.leaves.every((l) => !l.is_other)).toBe(true);
  });

  it("micro-cap respects 10k-share floor — not 0.5% of total", () => {
    const r = buildSunburstRings({
      ...DEFAULT_INPUT,
      total_shares: 1_000_000,
      institutions_total: 58_000,
      holders: [
        holder("alice", 50_000, "institutions"),
        holder("bob", 8_000, "institutions"),
      ],
    });
    const inst = r!.categories.find((c) => c.key === "institutions")!;
    expect(inst.leaves.find((l) => l.key === "alice")).toBeDefined();
    const other = inst.leaves.find((l) => l.is_other);
    expect(other?.tail_meta?.count).toBe(1);
    expect(other?.shares).toBe(8_000);
  });
});
