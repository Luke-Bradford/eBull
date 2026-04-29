/**
 * Targeted regression tests for the dividend chart subcomponents
 * (#590 review). The metric helpers themselves are exercised in
 * `lib/dividendsMetrics.test.ts`; these tests pin the empty-state
 * branch logic that lives inside the chart components, where a
 * bug would otherwise manifest as a recharts frame with no series
 * instead of the inline "no data" hint.
 */

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { DividendPeriod } from "@/api/instruments";
import { CumulativeDpsChart } from "@/components/dividends/dividendsCharts";

function div(
  partial: Partial<DividendPeriod> & { period_end_date: string },
): DividendPeriod {
  return {
    period_end_date: partial.period_end_date,
    period_type: partial.period_type ?? "Q1",
    fiscal_year: partial.fiscal_year ?? 2026,
    fiscal_quarter: partial.fiscal_quarter ?? null,
    dps_declared: partial.dps_declared ?? null,
    dividends_paid: partial.dividends_paid ?? null,
    reported_currency: partial.reported_currency ?? "USD",
  };
}

describe("CumulativeDpsChart", () => {
  it("renders the inline no-data hint when every row has null dps_declared", () => {
    // Round-2 review regression: the gap-emitting cumulative_dps:
    // null change made the prior "=== 0" guard fail to fire when
    // every row was null. Without the extended guard, the chart
    // rendered an empty AreaChart frame instead of a "no data" hint.
    render(
      <CumulativeDpsChart
        history={[
          div({ period_end_date: "2025-03-31", dps_declared: null }),
          div({ period_end_date: "2025-06-30", dps_declared: null }),
        ]}
      />,
    );
    expect(
      screen.getByText(/No declared DPS history to accumulate/i),
    ).toBeInTheDocument();
  });

  it("renders the inline no-data hint on an empty history array", () => {
    render(<CumulativeDpsChart history={[]} />);
    expect(
      screen.getByText(/No declared DPS history to accumulate/i),
    ).toBeInTheDocument();
  });
});
