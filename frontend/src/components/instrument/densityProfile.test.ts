import { describe, expect, it } from "vitest";

import { selectProfile } from "./densityProfile";
import type { InstrumentSummary } from "@/api/types";

function fixture(overrides: Partial<InstrumentSummary["capabilities"]>): InstrumentSummary {
  return {
    instrument_id: 1,
    is_tradable: true,
    coverage_tier: 1,
    identity: { symbol: "X", display_name: null, sector: null, market_cap: null } as never,
    price: null,
    key_stats: null,
    source: {},
    has_sec_cik: false,
    has_filings_coverage: false,
    capabilities: { ...overrides } as InstrumentSummary["capabilities"],
  } as InstrumentSummary;
}

describe("selectProfile", () => {
  it("returns full-sec when sec_xbrl fundamentals + filings both active", () => {
    const summary = fixture({
      fundamentals: { providers: ["sec_xbrl"], data_present: { sec_xbrl: true } },
      filings: { providers: ["sec_edgar"], data_present: { sec_edgar: true } },
    });
    expect(selectProfile(summary)).toBe("full-sec");
  });

  it("returns partial-filings when filings active but no sec_xbrl fundamentals", () => {
    const summary = fixture({
      filings: { providers: ["companies_house"], data_present: { companies_house: true } },
    });
    expect(selectProfile(summary)).toBe("partial-filings");
  });

  it("returns partial-filings when sec_xbrl listed but no data present", () => {
    const summary = fixture({
      fundamentals: { providers: ["sec_xbrl"], data_present: { sec_xbrl: false } },
      filings: { providers: ["sec_edgar"], data_present: { sec_edgar: true } },
    });
    expect(selectProfile(summary)).toBe("partial-filings");
  });

  it("returns minimal when no fundamentals and no filings", () => {
    const summary = fixture({});
    expect(selectProfile(summary)).toBe("minimal");
  });

  it("returns partial-filings when has_sec_cik is true even without fundamentals or filings", () => {
    const summary: InstrumentSummary = {
      instrument_id: 1,
      is_tradable: true,
      coverage_tier: 1,
      identity: { symbol: "X", display_name: null, sector: null, market_cap: null } as never,
      price: null,
      key_stats: null,
      source: {},
      has_sec_cik: true,
      has_filings_coverage: false,
      capabilities: {},
    } as InstrumentSummary;
    expect(selectProfile(summary)).toBe("partial-filings");
  });

  it("returns partial-filings when only fundamentals active (no filings, no has_sec_cik)", () => {
    const summary: InstrumentSummary = {
      instrument_id: 1,
      is_tradable: true,
      coverage_tier: 1,
      identity: { symbol: "X", display_name: null, sector: null, market_cap: null } as never,
      price: null,
      key_stats: null,
      source: {},
      has_sec_cik: false,
      has_filings_coverage: false,
      capabilities: {
        fundamentals: { providers: ["sec_xbrl"], data_present: { sec_xbrl: true } },
      },
    } as InstrumentSummary;
    expect(selectProfile(summary)).toBe("partial-filings");
  });
});
