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
 * Banner rendering (per-state glyph / variant / copy) is covered in
 * ``OwnershipCoverageBanner.test.tsx`` since the #923 extraction —
 * this file only exercises the rollup→SunburstInputs transform.
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
      edgar_url: null,
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
    historical_symbols: [],
    corrections_applied: [],
    suppressed_by_notice: 0,
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
                winning_edgar_url: null,
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
                winning_edgar_url: null,
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
                winning_edgar_url: null,
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
                winning_edgar_url: null,
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
                winning_edgar_url: null,
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
                winning_edgar_url: null,
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

describe("rollupToSunburstInputs — def14a_unmatched is its own wedge (#1627)", () => {
  /**
   * #1627 un-folds ``def14a_unmatched`` from the insiders wedge. It
   * carries ``denominator_basis=pie_wedge`` (additive, already in the
   * server residual), and on large caps the unmatched proxy 5%+ holders
   * dwarf the real insiders, so folding mislabelled the majority of the
   * insiders wedge. It now renders as its own ``def14a`` category.
   */
  it("surfaces def14a_unmatched as its own category, not folded into insiders", () => {
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
                winning_edgar_url: null,
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
                filer_name: "Big Proxy Holder",
                shares: "500000",
                pct_outstanding: "0.005",
                winning_source: "def14a",
                winning_accession: "DEF-B",
                winning_edgar_url: null,
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
    // Insiders is ONLY insiders now — no def14a fold.
    expect(inputs!.insiders_total).toBe(1_000_000);
    expect(inputs!.insiders_as_of).toBe("2026-01-01");
    // def14a is its own category total + as_of.
    expect(inputs!.def14a_total).toBe(500_000);
    expect(inputs!.def14a_as_of).toBe("2026-03-01");
    // Each holder surfaces under its own chart category.
    expect(inputs!.holders.filter((h) => h.category === "insiders").map((h) => h.label)).toEqual([
      "Officer A",
    ]);
    expect(inputs!.holders.filter((h) => h.category === "def14a").map((h) => h.label)).toEqual([
      "Big Proxy Holder",
    ]);
  });

  it("returns null insiders_total AND def14a_total when both slices are absent", () => {
    const inputs = rollupToSunburstInputs(
      _baseRollup({ shares_outstanding: "100000000", slices: [] }),
    );
    expect(inputs!.insiders_total).toBeNull();
    expect(inputs!.def14a_total).toBeNull();
  });
});

describe("rollupToSunburstInputs — funds overlay is non-additive (#1627)", () => {
  /**
   * funds is the only ``institution_subset`` slice. Its shares are
   * fund-level N-PORT detail already inside the 13F-HR institutional
   * aggregate, so it must NEVER enter the chart — additive accounting
   * would double-count and visibly oversubscribe the pie. Here funds
   * (80M) is larger than institutions (30M), so a leak would be obvious.
   */
  it("never flattens funds holders into the chart or any category total", () => {
    const inputs = rollupToSunburstInputs(
      _baseRollup({
        shares_outstanding: "100000000",
        slices: [
          {
            category: "institutions",
            label: "Institutions",
            total_shares: "30000000",
            pct_outstanding: "0.3",
            filer_count: 1,
            dominant_source: "13f",
            holders: [
              {
                filer_cik: "0001000010",
                filer_name: "BlackRock",
                shares: "30000000",
                pct_outstanding: "0.3",
                winning_source: "13f",
                winning_accession: "13F-010",
                winning_edgar_url: null,
                as_of_date: "2025-12-31",
                filer_type: "INV",
                dropped_sources: [],
              },
            ],
          },
          {
            category: "funds",
            label: "Mutual funds (N-PORT)",
            total_shares: "80000000",
            pct_outstanding: "0.8",
            filer_count: 1,
            dominant_source: "nport",
            denominator_basis: "institution_subset",
            holders: [
              {
                filer_cik: "0000036405",
                filer_name: "Big Fund",
                shares: "80000000",
                pct_outstanding: "0.8",
                winning_source: "nport",
                winning_accession: "NPORT-1",
                winning_edgar_url: null,
                as_of_date: "2026-03-31",
                filer_type: null,
                dropped_sources: [],
              },
            ],
          },
        ],
      }),
    );
    expect(inputs).not.toBeNull();
    expect(inputs!.holders.some((h) => h.label === "Big Fund")).toBe(false);
    expect(inputs!.holders).toHaveLength(1);
    expect(inputs!.institutions_total).toBe(30_000_000);
  });
});

describe("rollupToSunburstInputs — source_url threading (#921)", () => {
  it("maps winning_edgar_url onto the holder's source_url", () => {
    const inputs = rollupToSunburstInputs(
      _baseRollup({
        shares_outstanding: "1000000000",
        slices: [
          {
            category: "institutions",
            label: "Institutions",
            total_shares: "120000000",
            pct_outstanding: "0.12",
            filer_count: 1,
            dominant_source: "13f",
            holders: [
              {
                filer_cik: "0001000010",
                filer_name: "BlackRock",
                shares: "120000000",
                pct_outstanding: "0.12",
                winning_source: "13f",
                winning_accession: "13F-010",
                winning_edgar_url:
                  "https://www.sec.gov/Archives/edgar/data/1000010/000100001026000010-index.html",
                as_of_date: "2025-12-31",
                filer_type: "INV",
                dropped_sources: [],
              },
            ],
          },
        ],
      }),
    );
    expect(inputs!.holders).toHaveLength(1);
    expect(inputs!.holders[0]).toMatchObject({
      key: "0001000010",
      source_url:
        "https://www.sec.gov/Archives/edgar/data/1000010/000100001026000010-index.html",
    });
  });
});
