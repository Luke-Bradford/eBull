import { describe, expect, it } from "vitest";

import {
  buildSunburstRings,
  visibilityThreshold,
} from "./ownershipRings";
import type { SunburstHolder, SunburstInputs } from "./ownershipRings";

const DEFAULT_INPUT: SunburstInputs = {
  free_float: 1_000_000_000,
  holders: [],
  treasury_shares: null,
  institutions_status: "ok",
  etfs_status: "ok",
  insiders_status: "ok",
};

function holder(
  key: string,
  shares: number,
  category: SunburstHolder["category"] = "institutions",
): SunburstHolder {
  return { key, label: key, shares, category };
}

describe("visibilityThreshold", () => {
  it("returns 0.5% of float for normal-cap floats", () => {
    expect(visibilityThreshold(1_000_000_000)).toBe(5_000_000);
    expect(visibilityThreshold(15_000_000_000)).toBe(75_000_000);
  });

  it("clamps to 10,000-share floor for micro-cap floats", () => {
    // 1M float * 0.5% = 5,000 shares — below 10k floor.
    expect(visibilityThreshold(1_000_000)).toBe(10_000);
    expect(visibilityThreshold(0)).toBe(10_000);
    expect(visibilityThreshold(-50_000)).toBe(10_000);
  });
});

describe("buildSunburstRings", () => {
  it("returns null on missing or zero float", () => {
    expect(buildSunburstRings({ ...DEFAULT_INPUT, free_float: 0 })).toBeNull();
    expect(buildSunburstRings({ ...DEFAULT_INPUT, free_float: NaN })).toBeNull();
  });

  it("inner ring reports known + residual sum / float", () => {
    const input: SunburstInputs = {
      ...DEFAULT_INPUT,
      holders: [holder("vanguard", 100_000_000, "institutions")],
    };
    const r = buildSunburstRings(input);
    expect(r).not.toBeNull();
    // Free float = 1B. Held = 100M (institutions) + 900M (unallocated residual) = 1B.
    expect(r!.inner.shares).toBe(1_000_000_000);
    expect(r!.inner.pct).toBeCloseTo(1);
  });

  it("filer above threshold gets its own outer-ring wedge", () => {
    // Threshold for 1B float = 5M. Vanguard at 100M passes.
    const input: SunburstInputs = {
      ...DEFAULT_INPUT,
      holders: [holder("vanguard", 100_000_000, "institutions")],
    };
    const r = buildSunburstRings(input);
    expect(r).not.toBeNull();
    const inst = r!.categories.find((c) => c.key === "institutions")!;
    expect(inst.leaves).toHaveLength(1);
    expect(inst.leaves[0]!.key).toBe("vanguard");
    expect(inst.leaves[0]!.is_other).toBe(false);
  });

  it("filer below threshold rolls into 'Other' aggregate wedge", () => {
    const input: SunburstInputs = {
      ...DEFAULT_INPUT,
      holders: [
        holder("vanguard", 100_000_000, "institutions"),  // visible
        holder("small1", 1_000_000, "institutions"),       // < 5M threshold
        holder("small2", 500_000, "institutions"),         // < 5M threshold
        holder("small3", 100_000, "institutions"),         // < 5M threshold
      ],
    };
    const r = buildSunburstRings(input);
    const inst = r!.categories.find((c) => c.key === "institutions")!;
    expect(inst.leaves).toHaveLength(2);
    const other = inst.leaves[1]!;
    expect(other.is_other).toBe(true);
    expect(other.shares).toBe(1_600_000);
    expect(other.tail_meta!.count).toBe(3);
    expect(other.tail_meta!.aggregate_shares).toBe(1_600_000);
    expect(other.tail_meta!.largest_label).toBe("small1");
  });

  it("'Other' tail meta surfaces the largest sub-threshold holder for context", () => {
    // Operator wants to know the top of the tail without expanding —
    // pin that ``largest_label`` is correct on a multi-holder tail.
    const input: SunburstInputs = {
      ...DEFAULT_INPUT,
      holders: [
        holder("BIG", 100_000_000, "institutions"),
        holder("renaissance", 4_000_000, "institutions"), // below 5M
        holder("citadel", 4_500_000, "institutions"),     // below 5M, biggest in tail
        holder("two_sigma", 500_000, "institutions"),
      ],
    };
    const r = buildSunburstRings(input);
    const inst = r!.categories.find((c) => c.key === "institutions")!;
    const other = inst.leaves.find((l) => l.is_other)!;
    expect(other.tail_meta!.largest_label).toBe("citadel");
    expect(other.tail_meta!.largest_pct).toBeCloseTo(0.0045);
  });

  it("micro-cap respects 10k-share floor — not 0.5%-of-float", () => {
    // 1M float, 0.5% = 5k shares — but floor is 10k.
    // Holder with 8k shares should fall into "Other" not visible.
    const input: SunburstInputs = {
      free_float: 1_000_000,
      holders: [
        holder("alice", 50_000, "institutions"),  // visible (>10k)
        holder("bob", 8_000, "institutions"),     // < 10k floor
      ],
      treasury_shares: null,
      institutions_status: "ok",
      etfs_status: "ok",
      insiders_status: "ok",
    };
    const r = buildSunburstRings(input);
    const inst = r!.categories.find((c) => c.key === "institutions")!;
    expect(inst.leaves.find((l) => l.key === "alice")).toBeDefined();
    const other = inst.leaves.find((l) => l.is_other);
    expect(other?.tail_meta?.count).toBe(1);
    expect(other?.shares).toBe(8_000);
  });

  it("insiders bypass the threshold — every officer surfaces", () => {
    // Threshold for 1B float = 5M. Officer holding 1M would normally
    // fall into "Other" for institutions, but for insiders every
    // officer should surface as their own wedge.
    const input: SunburstInputs = {
      ...DEFAULT_INPUT,
      holders: [
        holder("ceo", 1_000_000, "insiders"),
        holder("cfo", 500_000, "insiders"),
        holder("cto", 100_000, "insiders"),
      ],
    };
    const r = buildSunburstRings(input);
    const insiders = r!.categories.find((c) => c.key === "insiders")!;
    expect(insiders.leaves).toHaveLength(3);
    expect(insiders.leaves.every((l) => !l.is_other)).toBe(true);
  });

  it("category status='unknown' renders a coverage-gap leaf, not 0%", () => {
    const input: SunburstInputs = {
      ...DEFAULT_INPUT,
      institutions_status: "unknown",
      etfs_status: "unknown",
    };
    const r = buildSunburstRings(input);
    const inst = r!.categories.find((c) => c.key === "institutions")!;
    expect(inst.status).toBe("unknown");
    expect(inst.leaves).toHaveLength(1);
    expect(inst.leaves[0]!.label).toContain("Coverage gap");
    // Unallocated also flagged unknown when any category is unknown.
    const unalloc = r!.categories.find((c) => c.key === "unallocated")!;
    expect(unalloc.status).toBe("unknown");
  });

  it("treasury wedge present when treasury_shares > 0", () => {
    const input: SunburstInputs = {
      ...DEFAULT_INPUT,
      treasury_shares: 10_000_000,
    };
    const r = buildSunburstRings(input);
    const treasury = r!.categories.find((c) => c.key === "treasury")!;
    expect(treasury.status).toBe("ok");
    expect(treasury.shares).toBe(10_000_000);
    expect(treasury.leaves).toHaveLength(1);
  });

  it("treasury status='unknown' when input is null", () => {
    const input: SunburstInputs = {
      ...DEFAULT_INPUT,
      treasury_shares: null,
    };
    const r = buildSunburstRings(input);
    const treasury = r!.categories.find((c) => c.key === "treasury")!;
    expect(treasury.status).toBe("unknown");
  });

  it("unallocated absorbs the float residual when every category is known", () => {
    // 1B float, 100M institutions, 50M ETFs, 20M insiders, 10M
    // treasury → 820M unallocated.
    const input: SunburstInputs = {
      ...DEFAULT_INPUT,
      holders: [
        holder("inst", 100_000_000, "institutions"),
        holder("etf", 50_000_000, "etfs"),
        holder("officer", 20_000_000, "insiders"),
      ],
      treasury_shares: 10_000_000,
    };
    const r = buildSunburstRings(input);
    const unalloc = r!.categories.find((c) => c.key === "unallocated")!;
    expect(unalloc.shares).toBe(820_000_000);
    expect(unalloc.status).toBe("ok");
  });

  it("category sort: largest leaf first within each category", () => {
    const input: SunburstInputs = {
      ...DEFAULT_INPUT,
      holders: [
        holder("smaller", 50_000_000, "institutions"),
        holder("larger", 200_000_000, "institutions"),
      ],
    };
    const r = buildSunburstRings(input);
    const inst = r!.categories.find((c) => c.key === "institutions")!;
    expect(inst.leaves[0]!.key).toBe("larger");
    expect(inst.leaves[1]!.key).toBe("smaller");
  });

  it("empty category renders status='empty' with no leaves", () => {
    const r = buildSunburstRings({ ...DEFAULT_INPUT });
    const inst = r!.categories.find((c) => c.key === "institutions")!;
    expect(inst.status).toBe("empty");
    expect(inst.leaves).toHaveLength(0);
  });
});
