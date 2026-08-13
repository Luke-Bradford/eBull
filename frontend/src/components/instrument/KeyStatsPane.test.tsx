import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { KeyStatsPane } from "./KeyStatsPane";
import type { InstrumentSummary } from "@/api/types";

function fixture(stats: Partial<InstrumentSummary["key_stats"]> | null): InstrumentSummary {
  return {
    identity: {
      symbol: "X",
      display_name: null,
      sector: null,
      market_cap: "1000000000",
    },
    key_stats:
      stats === null
        ? null
        : ({
            pe_ratio: "32.48",
            pb_ratio: null,
            dividend_yield: null,
            payout_ratio: null,
            roe: null,
            roa: null,
            debt_to_equity: "3.15",
            revenue_growth_yoy: null,
            earnings_growth_yoy: null,
            field_source: { pe_ratio: "sec_xbrl", debt_to_equity: "sec_xbrl" },
            ...stats,
          } as InstrumentSummary["key_stats"]),
    capabilities: {},
  } as InstrumentSummary;
}

describe("KeyStatsPane", () => {
  it("renders rows with non-null values, drops fully-null rows, keeps per-row source tag", () => {
    render(<KeyStatsPane summary={fixture({})} />);
    expect(screen.getByText("P/E ratio")).toBeInTheDocument();
    expect(screen.getByText("Debt / Equity")).toBeInTheDocument();
    // Dividend yield is null → row dropped.
    expect(screen.queryByText("Dividend yield")).not.toBeInTheDocument();
    // Per-row source tag still rendered (e.g. "SEC" badge for pe_ratio + debt_to_equity).
    expect(screen.getAllByText("SEC").length).toBeGreaterThanOrEqual(2);
  });

  it("renders dividend yield as an already-percent value, not double-scaled ×100 (#1827)", () => {
    // The wire value is `ttm_yield_pct = (ttm_dps / price) * 100` — already a
    // percent. AAPL's 0.2744 must render "0.27%", NOT "27.45%" (the old
    // {percent:true} double-scale bug). Sibling roe stays a fraction → ×100.
    render(<KeyStatsPane summary={fixture({ dividend_yield: "0.27448", roe: "0.2777" })} />);
    expect(screen.getByText("Dividend yield")).toBeInTheDocument();
    expect(screen.getByText("0.27%")).toBeInTheDocument();
    expect(screen.queryByText("27.45%")).not.toBeInTheDocument();
    // roe is a raw fraction → still ×100.
    expect(screen.getByText("27.77%")).toBeInTheDocument();
  });

  it("renders empty state when key_stats is null", () => {
    render(<KeyStatsPane summary={fixture(null)} />);
    expect(screen.getByText(/No key stats/i)).toBeInTheDocument();
  });

  it("renders a per-class float row labelled by symbol when class_market_value is set (#1665)", () => {
    const base = fixture({});
    const dualClass = {
      ...base,
      identity: { ...base.identity, symbol: "GOOGL", class_market_value: "2154300000000" },
    } as InstrumentSummary;
    render(<KeyStatsPane summary={dualClass} />);
    // Both the total-company "Market cap" and the per-class value are shown,
    // clearly disambiguated by the symbol-anchored label.
    expect(screen.getByText("Market cap")).toBeInTheDocument();
    expect(screen.getByText("GOOGL market value")).toBeInTheDocument();
    expect(screen.getByText("2.15T")).toBeInTheDocument();
  });

  it("omits the per-class float row for a single-class issuer (class_market_value null)", () => {
    // The default fixture leaves class_market_value unset → row absent.
    render(<KeyStatsPane summary={fixture({})} />);
    expect(screen.queryByText(/market value$/)).not.toBeInTheDocument();
  });
});
