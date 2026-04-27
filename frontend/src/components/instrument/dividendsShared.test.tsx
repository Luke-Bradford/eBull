import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import {
  formatDps,
  formatYieldPct,
  HistoryBar,
} from "./dividendsShared";

import type { DividendPeriod } from "@/api/instruments";

function period(dps: string | null, currency: string | null): DividendPeriod {
  return {
    period_end_date: "2025-12-28",
    period_type: "Q4",
    fiscal_year: 2025,
    fiscal_quarter: 4,
    dps_declared: dps,
    dividends_paid: null,
    reported_currency: currency,
  };
}

describe("formatDps", () => {
  it("returns em-dash for null input", () => {
    expect(formatDps(null, "USD")).toBe("—");
  });

  it("returns em-dash for non-numeric string", () => {
    expect(formatDps("abc", "USD")).toBe("—");
  });

  it("formats with currency prefix and trims trailing zeros", () => {
    expect(formatDps("0.2500", "USD")).toBe("USD 0.25");
  });

  it("handles null currency — no prefix", () => {
    expect(formatDps("0.2500", null)).toBe("0.25");
  });

  it("preserves significant trailing decimal digits", () => {
    // 0.1234 should not be trimmed
    expect(formatDps("0.1234", "USD")).toBe("USD 0.1234");
  });
});

describe("formatYieldPct", () => {
  it("returns em-dash for null", () => {
    expect(formatYieldPct(null)).toBe("—");
  });

  it("formats to 2 decimal places with % suffix", () => {
    expect(formatYieldPct("0.52")).toBe("0.52%");
  });
});

describe("HistoryBar", () => {
  it("renders label and progressbar for a period", () => {
    render(<HistoryBar period={period("0.25", "USD")} max={0.5} />);
    expect(screen.getByText("FY2025 Q4")).toBeInTheDocument();
    const bar = screen.getByRole("progressbar");
    expect(bar).toHaveAttribute("aria-valuenow", "0.25");
    expect(bar).toHaveAttribute("aria-valuemax", "0.5");
  });

  it("renders em-dash when dps_declared is null", () => {
    render(<HistoryBar period={period(null, "USD")} max={0.5} />);
    expect(screen.getByText("—")).toBeInTheDocument();
    const bar = screen.getByRole("progressbar");
    // progressbar value should be 0 when dps_declared is null
    expect(bar).toHaveAttribute("aria-valuenow", "0");
  });
});
