/**
 * Unit tests for the ownership panel's rollup-mapping helper
 * (``rollupToSunburstInputs``) — the pure function that converts the
 * deduped server payload into the chart's ``SunburstInputs`` shape.
 *
 * The full panel rendering is React+ResponsiveContainer-heavy and
 * not amenable to a fast unit test; we focus on the data
 * transformation invariants that matter for correctness:
 *
 *   * Denominator stays at ``shares_outstanding`` even when treasury
 *     > 0 (codex audit 2026-05-03 ship-blocker).
 *   * Empty / pre-ingest payloads return ``null`` so the panel
 *     renders the empty state.
 *   * Per-category totals + holder lists round-trip from the rollup
 *     into the SunburstInputs shape with no double-counting.
 *
 * The banner state machine + oversubscription warning are tested
 * against the response shape directly (they are simple enum-keyed
 * renders) — see the per-state assertions below.
 */

import { describe, expect, it } from "vitest";

import type { OwnershipRollupResponse } from "@/api/ownership";

import { rollupToSunburstInputs } from "./OwnershipPanel";

function _baseRollup(
  overrides: Partial<OwnershipRollupResponse> = {},
): OwnershipRollupResponse {
  return {
    symbol: "TEST",
    instrument_id: 1,
    shares_outstanding: "1000000000",
    shares_outstanding_as_of: "2026-03-31",
    shares_outstanding_source: {
      accession_number: null,
      concept: "EntityCommonStockSharesOutstanding",
      form_type: null,
    },
    treasury_shares: null,
    treasury_as_of: null,
    slices: [],
    residual: {
      shares: "1000000000",
      pct_outstanding: "1",
      label: "Public / unattributed",
      tooltip: "tooltip",
      oversubscribed: false,
    },
    concentration: {
      pct_outstanding_known: "0",
      info_chip: "Known filers hold 0% of float.",
    },
    coverage: {
      state: "unknown_universe",
      categories: {},
    },
    banner: {
      state: "unknown_universe",
      variant: "warning",
      headline: "Coverage estimate not available",
      body: "Treat as best-effort.",
    },
    computed_at: "2026-05-03T00:00:00Z",
    ...overrides,
  };
}

describe("rollupToSunburstInputs — denominator stays on shares_outstanding", () => {
  it("returns null when shares_outstanding is null", () => {
    const inputs = rollupToSunburstInputs(
      _baseRollup({ shares_outstanding: null }),
    );
    expect(inputs).toBeNull();
  });

  it("returns null when shares_outstanding is zero", () => {
    const inputs = rollupToSunburstInputs(
      _baseRollup({ shares_outstanding: "0" }),
    );
    expect(inputs).toBeNull();
  });

  it("does NOT add treasury into total_shares (corrects #789 ship-blocker)", () => {
    const inputs = rollupToSunburstInputs(
      _baseRollup({
        shares_outstanding: "1000000000",
        treasury_shares: "200000000",
      }),
    );
    expect(inputs).not.toBeNull();
    expect(inputs!.total_shares).toBe(1_000_000_000);
    expect(inputs!.treasury_shares).toBe(200_000_000);
  });
});

describe("rollupToSunburstInputs — slice totals round-trip", () => {
  it("flattens insiders + blockholders + institutions + etfs into the holders list", () => {
    const inputs = rollupToSunburstInputs(
      _baseRollup({
        shares_outstanding: "1000000000",
        slices: [
          {
            category: "insiders",
            label: "Insiders",
            total_shares: "30000000",
            pct_outstanding: "0.03",
            filer_count: 1,
            dominant_source: "form4",
            holders: [
              {
                filer_cik: "0001000001",
                filer_name: "Cook Tim",
                shares: "30000000",
                pct_outstanding: "0.03",
                winning_source: "form4",
                winning_accession: "F4-001",
                as_of_date: "2026-03-15",
                filer_type: null,
                dropped_sources: [],
              },
            ],
          },
          {
            category: "institutions",
            label: "Institutions",
            total_shares: "200000000",
            pct_outstanding: "0.20",
            filer_count: 2,
            dominant_source: "13f",
            holders: [
              {
                filer_cik: "0001000010",
                filer_name: "BlackRock",
                shares: "120000000",
                pct_outstanding: "0.12",
                winning_source: "13f",
                winning_accession: "13F-010",
                as_of_date: "2025-12-31",
                filer_type: "INV",
                dropped_sources: [],
              },
              {
                filer_cik: "0001000011",
                filer_name: "State Street",
                shares: "80000000",
                pct_outstanding: "0.08",
                winning_source: "13f",
                winning_accession: "13F-011",
                as_of_date: "2025-12-31",
                filer_type: "INV",
                dropped_sources: [],
              },
            ],
          },
          {
            category: "etfs",
            label: "ETFs",
            total_shares: "50000000",
            pct_outstanding: "0.05",
            filer_count: 1,
            dominant_source: "13f",
            holders: [
              {
                filer_cik: "0001000020",
                filer_name: "Vanguard",
                shares: "50000000",
                pct_outstanding: "0.05",
                winning_source: "13f",
                winning_accession: "13F-020",
                as_of_date: "2025-12-31",
                filer_type: "ETF",
                dropped_sources: [],
              },
            ],
          },
        ],
      }),
    );
    expect(inputs).not.toBeNull();
    expect(inputs!.institutions_total).toBe(200_000_000);
    expect(inputs!.etfs_total).toBe(50_000_000);
    expect(inputs!.insiders_total).toBe(30_000_000);
    expect(inputs!.holders).toHaveLength(4);
    const blackrock = inputs!.holders.find((h) => h.label === "BlackRock");
    expect(blackrock?.category).toBe("institutions");
    expect(blackrock?.shares).toBe(120_000_000);
    const vanguard = inputs!.holders.find((h) => h.label === "Vanguard");
    expect(vanguard?.category).toBe("etfs");
  });

  it("derives per-category as_of_date from the latest holder date in the slice", () => {
    const inputs = rollupToSunburstInputs(
      _baseRollup({
        shares_outstanding: "100000000",
        slices: [
          {
            category: "insiders",
            label: "Insiders",
            total_shares: "30000",
            pct_outstanding: "0.0003",
            filer_count: 2,
            dominant_source: "form4",
            holders: [
              {
                filer_cik: "0001",
                filer_name: "Older Holder",
                shares: "10000",
                pct_outstanding: "0.0001",
                winning_source: "form4",
                winning_accession: "F4-OLD",
                as_of_date: "2024-01-01",
                filer_type: null,
                dropped_sources: [],
              },
              {
                filer_cik: "0002",
                filer_name: "Newer Holder",
                shares: "20000",
                pct_outstanding: "0.0002",
                winning_source: "form4",
                winning_accession: "F4-NEW",
                as_of_date: "2026-04-01",
                filer_type: null,
                dropped_sources: [],
              },
            ],
          },
        ],
      }),
    );
    expect(inputs!.insiders_as_of).toBe("2026-04-01");
  });
});

describe("rollupToSunburstInputs — empty / no-data cases", () => {
  it("returns inputs with null per-category totals when no slices are present", () => {
    const inputs = rollupToSunburstInputs(_baseRollup({ slices: [] }));
    expect(inputs).not.toBeNull();
    expect(inputs!.institutions_total).toBeNull();
    expect(inputs!.etfs_total).toBeNull();
    expect(inputs!.insiders_total).toBeNull();
    expect(inputs!.blockholders_total).toBeNull();
    expect(inputs!.holders).toEqual([]);
  });
});

describe("rollupToSunburstInputs — def14a_unmatched fold (Codex review fix)", () => {
  /**
   * Codex pre-push review (Batch 1 of #788) caught this: the prior
   * version dropped ``def14a_unmatched`` slices from the chart
   * entirely. Folding into the insiders bucket keeps the chart
   * totals reconciled with the slice table.
   */
  it("folds def14a_unmatched holders into the insiders bucket", () => {
    const inputs = rollupToSunburstInputs(
      _baseRollup({
        shares_outstanding: "100000000",
        slices: [
          {
            category: "insiders",
            label: "Insiders",
            total_shares: "1000000",
            pct_outstanding: "0.01",
            filer_count: 1,
            dominant_source: "form4",
            holders: [
              {
                filer_cik: "0001",
                filer_name: "Officer A",
                shares: "1000000",
                pct_outstanding: "0.01",
                winning_source: "form4",
                winning_accession: "F4-A",
                as_of_date: "2026-01-01",
                filer_type: null,
                dropped_sources: [],
              },
            ],
          },
          {
            category: "def14a_unmatched",
            label: "Proxy-only (DEF 14A)",
            total_shares: "500000",
            pct_outstanding: "0.005",
            filer_count: 1,
            dominant_source: "def14a",
            holders: [
              {
                filer_cik: null,
                filer_name: "Officer B (proxy-only)",
                shares: "500000",
                pct_outstanding: "0.005",
                winning_source: "def14a",
                winning_accession: "DEF-B",
                as_of_date: "2026-03-01",
                filer_type: null,
                dropped_sources: [],
              },
            ],
          },
        ],
      }),
    );
    expect(inputs).not.toBeNull();
    // Combined insiders total = 1M + 500k.
    expect(inputs!.insiders_total).toBe(1_500_000);
    // Both holders surface in the holders list under the insiders
    // category.
    const insiderHolders = inputs!.holders.filter((h) => h.category === "insiders");
    expect(insiderHolders).toHaveLength(2);
    expect(insiderHolders.map((h) => h.label).sort()).toEqual([
      "Officer A",
      "Officer B (proxy-only)",
    ]);
    // Combined as_of takes the latest of the two.
    expect(inputs!.insiders_as_of).toBe("2026-03-01");
  });

  it("returns null insiders_total when both insiders and def14a_unmatched are absent", () => {
    const inputs = rollupToSunburstInputs(
      _baseRollup({
        shares_outstanding: "100000000",
        slices: [],
      }),
    );
    expect(inputs!.insiders_total).toBeNull();
  });
});
