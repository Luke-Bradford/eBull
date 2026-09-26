import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";

import * as instrumentsApi from "@/api/instruments";
import type { FairValueBandLeg, InstrumentFairValueBand } from "@/api/types";
import {
  FairValueBandPanel,
  bandReasonText,
  legExclusionText,
} from "@/components/instrument/FairValueBandPanel";

// GME's dev-DB fvb_v5 row (2026-09-25), trimmed.
function band(overrides: Partial<InstrumentFairValueBand> = {}): InstrumentFairValueBand {
  return {
    symbol: "GME",
    currency: "USD",
    method_version: "fvb_v5",
    label: "Deterministic peer band — not a validated fair value.",
    definition: "Per-share bear / base / bull implied by comparables.",
    available: true,
    reason: "ok",
    quality_status: "medium",
    bear_value: "9.084547",
    base_value: "15.555652",
    bull_value: "21.730000",
    as_of_date: "2026-09-25",
    ttm_end: "2026-08-01",
    price_as_of: "2026-09-25",
    computed_at: "2026-09-26T05:15:40Z",
    stale: false,
    stale_after_days: 7,
    target_basis: "not_multiclass",
    cross_leg_base_ratio: null,
    legs: [
      {
        multiple: "pe",
        contributed: false,
        base_value: null,
        cohort_n: 5,
        own_points: 14,
        sic_level: 0,
        cohort_screened: null,
        earnings_nonrep: "G3_spiked",
        dropped_nonpositive: false,
      },
      {
        multiple: "ps",
        contributed: true,
        base_value: 15.555651713356415,
        cohort_n: 4,
        own_points: 17,
        sic_level: 0,
        cohort_screened: false,
        earnings_nonrep: null,
        dropped_nonpositive: false,
      },
    ],
    ...overrides,
  };
}

describe("FairValueBandPanel", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("shows the band, its label, legs and dates", async () => {
    vi.spyOn(instrumentsApi, "fetchInstrumentFairValueBand").mockResolvedValue(band());
    render(<FairValueBandPanel symbol="GME" />);
    expect(await screen.findByText("US$9.08")).toBeInTheDocument();
    expect(screen.getByText("US$21.73")).toBeInTheDocument();
    expect(screen.getAllByText("US$15.56")).toHaveLength(2);
    expect(screen.getByText(/not a validated fair value/)).toBeInTheDocument();
    expect(screen.getByText("not used: earnings not representative")).toBeInTheDocument();
    expect(screen.getByText("4 (unscreened)")).toBeInTheDocument();
    expect(screen.getByText(/fvb_v5/)).toBeInTheDocument();
    expect(screen.queryByText(/read it as history/)).not.toBeInTheDocument();
  });

  it("flags a band older than its own freshness rule", async () => {
    vi.spyOn(instrumentsApi, "fetchInstrumentFairValueBand").mockResolvedValue(
      band({ stale: true, as_of_date: "2026-07-17" }),
    );
    render(<FairValueBandPanel symbol="GME" />);
    expect(await screen.findByText(/read it as history/)).toBeInTheDocument();
  });

  it("shows the stored reason instead of figures when absent", async () => {
    vi.spyOn(instrumentsApi, "fetchInstrumentFairValueBand").mockResolvedValue(
      band({ available: false, reason: "thin_cohort", bear_value: null, base_value: null, bull_value: null, legs: [] }),
    );
    render(<FairValueBandPanel symbol="GME" />);
    expect(await screen.findByText(/Too few comparable peers/)).toBeInTheDocument();
    expect(screen.queryByText("bear")).not.toBeInTheDocument();
  });

  it("renders a retryable error without the exception text", async () => {
    vi.spyOn(instrumentsApi, "fetchInstrumentFairValueBand").mockRejectedValue(new Error("boom internal"));
    render(<FairValueBandPanel symbol="GME" />);
    expect(await screen.findByRole("alert")).toBeInTheDocument();
    expect(screen.queryByText(/boom internal/)).not.toBeInTheDocument();
  });
});

describe("band copy helpers", () => {
  it("names every stored reason", () => {
    for (const r of [
      "no_band",
      "no_multiple",
      "thin_cohort",
      "stale_price",
      "currency_mismatch",
      "multiclass_unavailable",
      "earnings_nonrepresentative",
    ]) {
      expect(bandReasonText(r, "not_multiclass")).not.toMatch(/^No band \(/);
    }
    expect(bandReasonText("new_reason", null)).toBe("No band (new_reason).");
  });

  it("does not blame fundamentals for an ADR's suppressed band", () => {
    expect(bandReasonText("no_multiple", "fpi_adr_unavailable")).toMatch(/ADS ratio/);
    expect(bandReasonText("no_multiple", "fpi_adr_ratio")).toMatch(/ADS ratio/);
    expect(bandReasonText("no_multiple", "not_multiclass")).toMatch(/not positive/);
  });

  it("explains a non-contributing leg", () => {
    const leg = band().legs[1] as FairValueBandLeg;
    expect(legExclusionText(leg)).toBeNull();
    expect(legExclusionText({ ...leg, contributed: false, dropped_nonpositive: true })).toMatch(/net debt/);
    expect(legExclusionText({ ...leg, contributed: false })).toBe("no comparator");
  });
});
