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

  it("renders empty state when key_stats is null", () => {
    render(<KeyStatsPane summary={fixture(null)} />);
    expect(screen.getByText(/No key stats/i)).toBeInTheDocument();
  });
});
