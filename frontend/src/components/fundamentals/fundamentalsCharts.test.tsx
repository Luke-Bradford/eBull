/**
 * Targeted regression tests for the fundamentals chart subcomponents (#2185).
 *
 * The metric helpers are exercised in `lib/fundamentalsMetrics.test.ts`; these
 * tests pin the empty-state branch that lives inside the chart component,
 * where a bug manifests as an empty recharts frame rather than an inline hint
 * — recharts' `ResponsiveContainer` has no layout in jsdom, so the drawn
 * series are not assertable here (same constraint as
 * `components/dividends/dividendsCharts.test.tsx`).
 */

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { NetDebtChart } from "@/components/fundamentals/fundamentalsCharts";
import { joinStatements } from "@/lib/fundamentalsMetrics";
import type { InstrumentFinancialRow } from "@/api/types";

function balanceRows(
  values: Record<string, string | null>,
): InstrumentFinancialRow[] {
  return [{ period_end: "2026-03-31", period_type: "Q1", values }];
}

function periodsWith(values: Record<string, string | null>) {
  return joinStatements([], balanceRows(values), []);
}

const NO_DATA = /Net debt needs debt .* and cash on the balance sheet/i;

describe("NetDebtChart", () => {
  it("renders the inline no-data hint when both debt components are missing", () => {
    render(
      <NetDebtChart
        periods={periodsWith({
          long_term_debt: null,
          short_term_debt: null,
          cash: "500",
        })}
        currency="USD"
      />,
    );
    expect(screen.getByText(NO_DATA)).toBeInTheDocument();
  });

  it("renders the inline no-data hint on an empty period array", () => {
    render(<NetDebtChart periods={[]} currency="USD" />);
    expect(screen.getByText(NO_DATA)).toBeInTheDocument();
  });

  it("still draws when cash is missing — gross debt is reportable on its own", () => {
    // A missing `cash` nulls the net-debt LINE but must not blank the pane:
    // gross debt is real data. Guarding on net_debt alone would have hidden
    // it, which is the degrade path the source rule explicitly rejects.
    render(
      <NetDebtChart
        periods={periodsWith({
          long_term_debt: "800",
          short_term_debt: null,
          cash: null,
        })}
        currency="USD"
      />,
    );
    expect(screen.queryByText(NO_DATA)).not.toBeInTheDocument();
  });
});
