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

import {
  FcfChart,
  NetDebtChart,
} from "@/components/fundamentals/fundamentalsCharts";
import { joinStatements } from "@/lib/fundamentalsMetrics";
import type { InstrumentFinancialRow } from "@/api/types";

function statementRows(
  values: Record<string, string | null>,
): InstrumentFinancialRow[] {
  return [{ period_end: "2026-03-31", period_type: "Q1", values }];
}

function periodsWith(values: Record<string, string | null>) {
  return joinStatements([], statementRows(values), []);
}

const NO_DATA = /Net debt needs a reported debt component/i;

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
    const hint = screen.getByText(NO_DATA);
    expect(hint).toBeInTheDocument();
    // This issuer DOES report cash, so the hint must not name cash as the
    // thing that is missing — the guard never required it.
    expect(hint.textContent).not.toMatch(/cash/i);
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

const FCF_NO_DATA = /FCF needs operating cash flow/i;

describe("FcfChart", () => {
  it("draws for an issuer that reports operating cash flow but never capex", () => {
    // The settled rule COALESCEs capex to 0 (app/services/fcf_yield.py:111,
    // :132) and spec §3.4 forbids gating on it. 1,142 FY instruments in the
    // dev corpus report OCF and never report capex — every one of them got
    // the empty state for a series that is computable.
    render(
      <FcfChart
        periods={joinStatements(
          [],
          [],
          statementRows({ operating_cf: "150", capex: null }),
        )}
        yieldSeries={null}
        currency="USD"
      />,
    );
    expect(screen.queryByText(FCF_NO_DATA)).not.toBeInTheDocument();
  });

  it("renders the no-data hint only when operating cash flow is absent", () => {
    render(
      <FcfChart
        periods={joinStatements([], [], statementRows({ capex: "40" }))}
        yieldSeries={null}
        currency="USD"
      />,
    );
    const hint = screen.getByText(FCF_NO_DATA);
    expect(hint).toBeInTheDocument();
    expect(hint.textContent).not.toMatch(/capex/i);
  });
});
