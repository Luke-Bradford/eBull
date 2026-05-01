import { describe, expect, it } from "vitest";

import {
  aggregateInsiderHoldings,
  computeOwnership,
  formatPct,
  formatShares,
  parseShareCount,
} from "./ownershipMetrics";

describe("parseShareCount", () => {
  it("returns null for null / undefined / blank", () => {
    expect(parseShareCount(null)).toBeNull();
    expect(parseShareCount(undefined)).toBeNull();
    expect(parseShareCount("")).toBeNull();
    expect(parseShareCount("   ")).toBeNull();
  });

  it("returns null for non-numeric strings", () => {
    expect(parseShareCount("not-a-number")).toBeNull();
    expect(parseShareCount("1234abc")).toBeNull();
  });

  it("parses Decimal-style strings emitted by the API", () => {
    expect(parseShareCount("1000000")).toBe(1_000_000);
    expect(parseShareCount("1000000.0000")).toBe(1_000_000);
    expect(parseShareCount("1.5")).toBe(1.5);
  });

  it("passes finite numbers through", () => {
    expect(parseShareCount(42)).toBe(42);
    expect(parseShareCount(0)).toBe(0);
  });

  it("rejects non-finite numbers", () => {
    expect(parseShareCount(Number.NaN)).toBeNull();
    expect(parseShareCount(Number.POSITIVE_INFINITY)).toBeNull();
  });
});

describe("computeOwnership", () => {
  it("returns null when shares_outstanding is missing", () => {
    expect(
      computeOwnership({
        shares_outstanding: null,
        treasury_shares: 0,
        institutions: { shares: 100 },
        etfs: { shares: 50 },
        insiders: { shares: 25 },
      }),
    ).toBeNull();
  });

  it("returns null when shares_outstanding is zero or negative", () => {
    expect(
      computeOwnership({
        shares_outstanding: 0,
        treasury_shares: 0,
        institutions: { shares: 100 },
        etfs: { shares: 50 },
        insiders: { shares: 25 },
      }),
    ).toBeNull();
  });

  it("computes free-float denominator as outstanding − treasury", () => {
    const r = computeOwnership({
      shares_outstanding: 1000,
      treasury_shares: 100,
      institutions: { shares: 0 },
      etfs: { shares: 0 },
      insiders: { shares: 0 },
    });
    expect(r).not.toBeNull();
    expect(r!.denominator).toBe(900);
  });

  it("computes per-slice percentages against the float denominator", () => {
    const r = computeOwnership({
      shares_outstanding: 1000,
      treasury_shares: 0,
      institutions: { shares: 350 },
      etfs: { shares: 200 },
      insiders: { shares: 100 },
    });
    expect(r).not.toBeNull();
    const slices = r!.slices;
    expect(slices[0]!.label).toBe("Institutions");
    expect(slices[0]!.pct).toBeCloseTo(0.35);
    expect(slices[1]!.label).toBe("ETFs");
    expect(slices[1]!.pct).toBeCloseTo(0.2);
    expect(slices[2]!.label).toBe("Insiders");
    expect(slices[2]!.pct).toBeCloseTo(0.1);
  });

  it("residual lands in the Unallocated slice", () => {
    const r = computeOwnership({
      shares_outstanding: 1000,
      treasury_shares: 0,
      institutions: { shares: 350 },
      etfs: { shares: 200 },
      insiders: { shares: 100 },
    });
    expect(r).not.toBeNull();
    const unallocated = r!.slices[3]!;
    expect(unallocated.label).toBe("Unallocated");
    expect(unallocated.pct).toBeCloseTo(0.35);
    // Slice percentages sum to ~100% (rounding tolerance).
    const total = r!.slices.reduce((a, s) => a + (s.pct ?? 0), 0);
    expect(total).toBeCloseTo(1, 4);
  });

  it("missing per-slice input renders as pct=null, not 0", () => {
    const r = computeOwnership({
      shares_outstanding: 1000,
      treasury_shares: 0,
      institutions: { shares: null },
      etfs: { shares: 200 },
      insiders: { shares: null },
    });
    expect(r).not.toBeNull();
    expect(r!.slices[0]!.pct).toBeNull();
    expect(r!.slices[1]!.pct).toBeCloseTo(0.2);
    expect(r!.slices[2]!.pct).toBeNull();
    // Unallocated must be null when some inputs are missing — we
    // cannot distinguish "genuinely unallocated equity" from
    // "unknown institutional / insider float". Pre-fix this
    // silently absorbed the unknown slices into Unallocated and
    // produced a misleading shares column.
    expect(r!.slices[3]!.pct).toBeNull();
    expect(r!.slices[3]!.shares).toBeNull();
  });

  it("Unallocated shares are computed when every slice is populated", () => {
    const r = computeOwnership({
      shares_outstanding: 1000,
      treasury_shares: 0,
      institutions: { shares: 350 },
      etfs: { shares: 200 },
      insiders: { shares: 100 },
    });
    expect(r).not.toBeNull();
    expect(r!.slices[3]!.shares).toBe(350);
    expect(r!.slices[3]!.pct).toBeCloseTo(0.35);
  });

  it("flags overflow when slices sum past 100%", () => {
    const r = computeOwnership({
      shares_outstanding: 1000,
      treasury_shares: 0,
      institutions: { shares: 600 },
      etfs: { shares: 500 },
      insiders: { shares: 50 },
    });
    expect(r).not.toBeNull();
    expect(r!.has_overflow).toBe(true);
    // Unallocated clamped to 0.
    expect(r!.slices[3]!.pct).toBe(0);
  });

  it("treasury slice uses TOTAL outstanding as denominator (memo line)", () => {
    const r = computeOwnership({
      shares_outstanding: 1000,
      treasury_shares: 100,
      institutions: { shares: 0 },
      etfs: { shares: 0 },
      insiders: { shares: 0 },
    });
    expect(r).not.toBeNull();
    expect(r!.treasury.shares).toBe(100);
    // 100 / 1000 = 10% (NOT 100 / 900 = 11.1%).
    expect(r!.treasury.pct).toBeCloseTo(0.1);
  });

  it("treasury defaults to zero when missing — slices still compute", () => {
    const r = computeOwnership({
      shares_outstanding: 1000,
      treasury_shares: null,
      institutions: { shares: 350 },
      etfs: { shares: 200 },
      insiders: { shares: 100 },
    });
    expect(r).not.toBeNull();
    expect(r!.denominator).toBe(1000);
    expect(r!.treasury.shares).toBeNull();
    expect(r!.treasury.pct).toBeNull();
  });
});

describe("aggregateInsiderHoldings", () => {
  it("returns null on empty rows", () => {
    expect(aggregateInsiderHoldings([])).toBeNull();
  });

  it("sums latest post-transaction shares per filer", () => {
    const rows = [
      {
        filer_cik: "0000111",
        filer_name: "Alice",
        txn_date: "2024-12-01",
        post_transaction_shares: "100",
        is_derivative: false,
      },
      {
        filer_cik: "0000111",
        filer_name: "Alice",
        txn_date: "2024-12-15",
        post_transaction_shares: "150", // newer — wins
        is_derivative: false,
      },
      {
        filer_cik: "0000222",
        filer_name: "Bob",
        txn_date: "2024-11-20",
        post_transaction_shares: "300",
        is_derivative: false,
      },
    ];
    expect(aggregateInsiderHoldings(rows)).toBe(450); // 150 + 300
  });

  it("excludes derivative positions", () => {
    const rows = [
      {
        filer_cik: "0000111",
        filer_name: "Alice",
        txn_date: "2024-12-15",
        post_transaction_shares: "100",
        is_derivative: false,
      },
      {
        filer_cik: "0000111",
        filer_name: "Alice",
        txn_date: "2024-12-15",
        post_transaction_shares: "9999",
        is_derivative: true, // ignored
      },
    ];
    expect(aggregateInsiderHoldings(rows)).toBe(100);
  });

  it("falls back to filer_name when filer_cik is null", () => {
    const rows = [
      {
        filer_cik: null,
        filer_name: "Cathy",
        txn_date: "2024-12-15",
        post_transaction_shares: "75",
        is_derivative: false,
      },
      {
        filer_cik: null,
        filer_name: "Cathy",
        txn_date: "2024-11-15",
        post_transaction_shares: "10000", // older — ignored
        is_derivative: false,
      },
    ];
    expect(aggregateInsiderHoldings(rows)).toBe(75);
  });

  it("returns null when every row's shares are unparseable", () => {
    const rows = [
      {
        filer_cik: "0000111",
        filer_name: "Alice",
        txn_date: "2024-12-15",
        post_transaction_shares: null,
        is_derivative: false,
      },
    ];
    expect(aggregateInsiderHoldings(rows)).toBeNull();
  });
});

describe("formatPct + formatShares", () => {
  it("formatPct renders fractional ratio as XX.XX%", () => {
    expect(formatPct(0.3525)).toBe("35.25%");
    expect(formatPct(0.1)).toBe("10.00%");
    expect(formatPct(0)).toBe("0.00%");
  });

  it("formatPct renders null and non-finite as em-dash", () => {
    expect(formatPct(null)).toBe("—");
    expect(formatPct(Number.NaN)).toBe("—");
    expect(formatPct(Number.POSITIVE_INFINITY)).toBe("—");
  });

  it("formatShares renders integer with thousands separators", () => {
    expect(formatShares(1_234_567)).toBe("1,234,567");
    expect(formatShares(0)).toBe("0");
  });

  it("formatShares renders null and NaN as em-dash", () => {
    expect(formatShares(null)).toBe("—");
    expect(formatShares(Number.NaN)).toBe("—");
  });
});
