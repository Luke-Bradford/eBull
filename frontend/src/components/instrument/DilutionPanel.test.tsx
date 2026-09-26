import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";

import * as instrumentsApi from "@/api/instruments";
import type { InstrumentDilution } from "@/api/types";
import { DilutionPanel, comparisonDate } from "@/components/instrument/DilutionPanel";

// GME's dev-DB summary (2026-09-26); net_dilution_pct_yoy is in PERCENT units.
function dilution(overrides: Partial<InstrumentDilution["summary"]> = {}): InstrumentDilution {
  return {
    symbol: "GME",
    summary: {
      latest_shares: "504500990.000000",
      latest_as_of: "2026-09-03",
      yoy_shares: "448700000.000000",
      net_dilution_pct_yoy: "12.43614664586583463300",
      ttm_shares_issued: "4472000000.000000",
      ttm_buyback_shares: null,
      ttm_net_share_change: "4472000000.000000",
      dilution_posture: "dilutive",
      ...overrides,
    },
    history: [
      ["2026-09-03", "504500990.000000"],
      ["2026-08-01", "449100000.000000"],
      ["2026-06-05", "448691257.000000"],
      ["2026-05-02", "448700000.000000"],
      ["2026-03-18", "448375157.000000"],
    ].map(([period_end, shares_outstanding]) => ({
      period_end: period_end as string,
      fiscal_year: null,
      fiscal_period: null,
      shares_outstanding: shares_outstanding as string,
      shares_issued_new: null,
      buyback_shares: null,
    })),
  };
}

describe("DilutionPanel", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("shows counts, the percent-unit change and the comparison's real date", async () => {
    vi.spyOn(instrumentsApi, "fetchInstrumentDilution").mockResolvedValue(dilution());
    render(<DilutionPanel symbol="GME" />);
    expect(await screen.findByText("+12.44%")).toBeInTheDocument();
    // The view's "year ago" count is the 4th-newest: 2 May, not a year back.
    expect(screen.getByText("compared with (02 May 2026)")).toBeInTheDocument();
    expect(screen.queryByText("dilutive")).not.toBeInTheDocument();
    expect(screen.getByText(/5 reported counts/)).toBeInTheDocument();
    expect(screen.getByText(/Not split-adjusted/)).toBeInTheDocument();
    // TTM flow sums are deliberately not rendered.
    expect(screen.queryByText(/4\.47/)).not.toBeInTheDocument();
  });

  it("says when there is nothing to compare", async () => {
    vi.spyOn(instrumentsApi, "fetchInstrumentDilution").mockResolvedValue(
      dilution({ yoy_shares: null, net_dilution_pct_yoy: null, dilution_posture: "stable" }),
    );
    render(<DilutionPanel symbol="GME" />);
    expect(await screen.findByText("No earlier count to compare.")).toBeInTheDocument();
    expect(screen.queryByText("stable")).not.toBeInTheDocument();
  });

  it("renders an empty state with no count", async () => {
    vi.spyOn(instrumentsApi, "fetchInstrumentDilution").mockResolvedValue(
      dilution({ latest_shares: null, latest_as_of: null }),
    );
    render(<DilutionPanel symbol="VOD.L" />);
    expect(await screen.findByText("No share count on file")).toBeInTheDocument();
  });

  it("renders a retryable error without the exception text", async () => {
    vi.spyOn(instrumentsApi, "fetchInstrumentDilution").mockRejectedValue(new Error("boom internal"));
    render(<DilutionPanel symbol="GME" />);
    expect(await screen.findByRole("alert")).toBeInTheDocument();
    expect(screen.queryByText(/boom internal/)).not.toBeInTheDocument();
  });
});

describe("comparisonDate", () => {
  it("names the view's comparison row only when the counts agree", () => {
    const { history } = dilution();
    expect(comparisonDate(history, "448700000.000000")).toBe("2026-05-02");
    expect(comparisonDate(history, "1.000000")).toBeNull();
    expect(comparisonDate(history.slice(0, 3), "448700000.000000")).toBeNull();
    expect(comparisonDate(history, null)).toBeNull();
  });
});
