/**
 * Unit tests for the L2 drilldown's rollup-mapping helper
 * (``rollupToFilerRows``) — the pure function that converts the
 * canonical deduped rollup payload into the per-filer table rows
 * (#1589 re-point off the legacy per-source endpoints).
 *
 * The invariants that matter:
 *
 *   * Row keys match the sunburst leaf keys produced by
 *     ``rollupToSunburstInputs`` (``filer_cik ?? name:`` fallback) —
 *     a wedge click navigates to ``?filer=<leaf key>`` and the table
 *     highlight resolves rows by that key. Key drift silently breaks
 *     the L1→L2 drill.
 *   * The share-parse skip predicate matches the chart's, so every
 *     wedge has a row and every row has a wedge.
 *   * ``def14a_unmatched`` folds into the insiders category (same
 *     fold as the chart); ``funds`` (N-PORT memo, non-additive) is
 *     excluded.
 *   * Treasury appends as a memo row with no source filing.
 *
 * CSV export contracts live server-side in
 * ``tests/test_ownership_rollup_csv.py`` since Chain 2.8 of #788.
 */

import { describe, expect, it } from "vitest";

import type {
  OwnershipHolder,
  OwnershipRollupResponse,
  OwnershipSlice,
} from "@/api/ownership";
import { rollupToSunburstInputs } from "@/components/instrument/OwnershipPanel";

import { rollupToFilerRows } from "./OwnershipPage";

function _holder(overrides: Partial<OwnershipHolder> = {}): OwnershipHolder {
  return {
    filer_cik: "0000102909",
    filer_name: "VANGUARD GROUP INC",
    shares: "100000000",
    pct_outstanding: "0.1",
    winning_source: "13f",
    winning_accession: "0000102909-26-000001",
    winning_edgar_url:
      "https://www.sec.gov/Archives/edgar/data/102909/000010290926000001-index.htm",
    as_of_date: "2026-03-31",
    filer_type: "INV",
    dropped_sources: [],
    ...overrides,
  };
}

function _slice(overrides: Partial<OwnershipSlice> = {}): OwnershipSlice {
  return {
    category: "institutions",
    label: "Institutions",
    total_shares: "100000000",
    pct_outstanding: "0.1",
    filer_count: 1,
    dominant_source: "13f",
    holders: [_holder()],
    ...overrides,
  };
}

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
    computed_at: "2026-06-12T00:00:00Z",
    ...overrides,
  };
}

describe("rollupToFilerRows — row shape and category mapping", () => {
  it("maps slice holders to rows with cik key, name fallback, and parsed fields", () => {
    const rollup = _baseRollup({
      slices: [
        _slice({
          holders: [
            _holder(),
            _holder({
              filer_cik: null,
              filer_name: "FAMILY TRUST LP",
              shares: "5000000",
              pct_outstanding: "0.005",
              winning_edgar_url: null,
            }),
          ],
          filer_count: 2,
        }),
      ],
    });
    const rows = rollupToFilerRows(rollup);
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({
      key: "0000102909",
      label: "VANGUARD GROUP INC",
      category: "institutions",
      category_label: "Institutions",
      shares: 100000000,
      pct_outstanding: 0.1,
      source: "13f",
      as_of_date: "2026-03-31",
    });
    expect(rows[1]).toMatchObject({
      key: "name:FAMILY TRUST LP",
      shares: 5000000,
      source_url: null,
    });
  });

  it("maps def14a_unmatched to its own def14a category (#1627 un-fold)", () => {
    const rollup = _baseRollup({
      slices: [
        _slice({
          category: "def14a_unmatched",
          label: "Proxy-only (DEF 14A)",
          holders: [
            _holder({
              filer_cik: null,
              filer_name: "JANE DOE",
              shares: "10000",
              winning_source: "def14a",
            }),
          ],
        }),
      ],
    });
    const rows = rollupToFilerRows(rollup);
    expect(rows).toHaveLength(1);
    expect(rows[0]!.category).toBe("def14a");
    expect(rows[0]!.category_label).toBe("DEF 14A");
  });

  it("keeps a same-name holder in insiders and def14a as two distinct rows (#1627)", () => {
    // A CIK-null ``name:`` key can collide across categories. The rows
    // must stay separate (category + key) so the L2 filer-label lookup
    // can be scoped by category to resolve the right one.
    const rollup = _baseRollup({
      slices: [
        _slice({
          category: "insiders",
          holders: [
            _holder({
              filer_cik: null,
              filer_name: "PAT SMITH",
              shares: "100",
              winning_source: "form4",
            }),
          ],
        }),
        _slice({
          category: "def14a_unmatched",
          holders: [
            _holder({
              filer_cik: null,
              filer_name: "PAT SMITH",
              shares: "200",
              winning_source: "def14a",
            }),
          ],
        }),
      ],
    });
    const rows = rollupToFilerRows(rollup).filter((r) => r.key === "name:PAT SMITH");
    expect(rows).toHaveLength(2);
    expect(new Set(rows.map((r) => r.category))).toEqual(new Set(["insiders", "def14a"]));
  });

  it("excludes the funds memo slice (N-PORT, non-additive)", () => {
    const rollup = _baseRollup({
      slices: [
        _slice({
          category: "funds",
          label: "Mutual funds (N-PORT)",
          denominator_basis: "institution_subset",
          holders: [_holder({ winning_source: "nport" })],
        }),
      ],
    });
    expect(rollupToFilerRows(rollup)).toHaveLength(0);
  });

  it("skips zero / unparseable share counts (same predicate as the chart)", () => {
    const rollup = _baseRollup({
      slices: [
        _slice({
          holders: [
            _holder({ shares: "0" }),
            _holder({ filer_cik: "0000000002", shares: "not-a-number" }),
          ],
        }),
      ],
    });
    expect(rollupToFilerRows(rollup)).toHaveLength(0);
  });

  it("appends a treasury memo row with no source filing", () => {
    const rollup = _baseRollup({
      treasury_shares: "50000000",
      treasury_as_of: "2026-03-31",
      slices: [_slice()],
    });
    const rows = rollupToFilerRows(rollup);
    const treasury = rows.at(-1)!;
    expect(treasury).toMatchObject({
      key: "treasury",
      category: "treasury",
      shares: 50000000,
      pct_outstanding: 0.05,
      source: null,
      source_url: null,
      as_of_date: "2026-03-31",
    });
  });

  it("sorts by category order then shares desc within category", () => {
    const rollup = _baseRollup({
      treasury_shares: "1000",
      slices: [
        _slice({
          category: "insiders",
          holders: [
            _holder({ filer_cik: "1", shares: "10", winning_source: "form4" }),
            _holder({ filer_cik: "2", shares: "20", winning_source: "form4" }),
          ],
        }),
        _slice({
          category: "blockholders",
          holders: [_holder({ filer_cik: "3", shares: "5", winning_source: "13d" })],
        }),
        _slice({
          category: "institutions",
          holders: [_holder({ filer_cik: "4", shares: "1" })],
        }),
      ],
    });
    const rows = rollupToFilerRows(rollup);
    expect(rows.map((r) => r.key)).toEqual(["4", "3", "2", "1", "treasury"]);
  });

  it("sorts def14a rows after insiders and before treasury (#1627)", () => {
    // Regression for the Codex ckpt-2 catch: def14a must be in
    // _CATEGORY_ORDER, else indexOf returns -1 and DEF 14A rows sort
    // before every known category.
    const rollup = _baseRollup({
      treasury_shares: "1000",
      slices: [
        _slice({
          category: "insiders",
          holders: [_holder({ filer_cik: "ins", shares: "100", winning_source: "form4" })],
        }),
        _slice({
          category: "def14a_unmatched",
          holders: [_holder({ filer_cik: "d14", shares: "200", winning_source: "def14a" })],
        }),
        _slice({
          category: "institutions",
          holders: [_holder({ filer_cik: "inst", shares: "300" })],
        }),
      ],
    });
    expect(rollupToFilerRows(rollup).map((r) => r.category)).toEqual([
      "institutions",
      "insiders",
      "def14a",
      "treasury",
    ]);
  });
});

describe("rollupToFilerRows — wedge ↔ row key parity", () => {
  it("table keys exactly match the sunburst leaf keys for the same rollup", () => {
    const rollup = _baseRollup({
      treasury_shares: "50000000",
      slices: [
        _slice({
          holders: [
            _holder(),
            _holder({ filer_cik: null, filer_name: "FAMILY TRUST LP", shares: "5000" }),
            // Zero-share holder must be absent from BOTH sides.
            _holder({ filer_cik: "0000000099", shares: "0" }),
          ],
        }),
        _slice({
          category: "etfs",
          holders: [_holder({ filer_cik: "0000036405", filer_type: "ETF" })],
        }),
        _slice({
          category: "insiders",
          holders: [_holder({ filer_cik: "0001214156", winning_source: "form4" })],
        }),
        _slice({
          category: "def14a_unmatched",
          holders: [
            _holder({ filer_cik: null, filer_name: "JANE DOE", winning_source: "def14a" }),
          ],
        }),
        _slice({
          category: "blockholders",
          holders: [_holder({ filer_cik: "0000937797", winning_source: "13d" })],
        }),
        _slice({
          category: "funds",
          denominator_basis: "institution_subset",
          holders: [_holder({ filer_cik: "0009999999", winning_source: "nport" })],
        }),
      ],
    });

    const inputs = rollupToSunburstInputs(rollup);
    expect(inputs).not.toBeNull();
    const wedgeKeys = new Set(inputs!.holders.map((h) => h.key));
    const rowKeys = new Set(
      rollupToFilerRows(rollup)
        .filter((r) => r.category !== "treasury")
        .map((r) => r.key),
    );
    expect(rowKeys).toEqual(wedgeKeys);
  });
});

describe("OwnershipPage", () => {
  it("module imports without throwing", async () => {
    // A bare import smoke — picks up syntax errors / missing exports
    // a stricter test would otherwise miss in this slim file.
    const mod = await import("./OwnershipPage");
    expect(typeof mod.OwnershipPage).toBe("function");
  });
});
