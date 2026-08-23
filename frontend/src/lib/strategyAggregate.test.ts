import { describe, expect, it } from "vitest";

import type { StrategyOverviewResponse } from "@/api/types";
import { aggregate } from "@/lib/strategyAggregate";

function strategy(overrides: {
  totalPnl?: string | null;
  forward?: boolean;
  fired?: number;
  resolved?: number;
  winners?: number;
  averageReturnPct?: string | null;
}) {
  // ⚠ `??` would collapse an EXPLICIT null into the default, which is the one
  // distinction these tests exist to make — check for the key instead.
  const totalPnl = "totalPnl" in overrides ? overrides.totalPnl : "0";
  return {
    forward_outcome_supported: overrides.forward ?? true,
    allocation_ready: false,
    pnl: { total_pnl: totalPnl, active_position_count: 0 },
    attribution: {
      fired_entries: overrides.fired ?? 0,
      resolved_entries: overrides.resolved ?? 0,
      winning_entries: overrides.winners ?? 0,
      shadow_average_return_pct: overrides.averageReturnPct ?? null,
    },
  };
}

function overview(strategies: ReturnType<typeof strategy>[]): StrategyOverviewResponse {
  return { strategies } as unknown as StrategyOverviewResponse;
}

/**
 * These pin the two honesty rules in `aggregate`'s docstring. Both are the same
 * shape of defect — a figure that LOOKS complete while silently covering a
 * subset — which is the class the operator cannot detect by reading the page.
 */
describe("aggregate", () => {
  it("refuses a partial P&L total when any strategy's P&L is unparseable", () => {
    const summary = aggregate(overview([strategy({ totalPnl: "12.5" }), strategy({ totalPnl: null })]));
    expect(summary.totalPnl).toBeNull();
  });

  it("sums P&L when every strategy reports one", () => {
    const summary = aggregate(overview([strategy({ totalPnl: "12.5" }), strategy({ totalPnl: "-2.5" })]));
    expect(summary.totalPnl).toBe(10);
  });

  it("collapses the average return to null when a contributing strategy has no average", () => {
    const summary = aggregate(
      overview([
        strategy({ resolved: 4, winners: 3, averageReturnPct: "2.0" }),
        strategy({ resolved: 2, winners: 1, averageReturnPct: null }),
      ]),
    );
    expect(summary.resolved).toBe(6);
    expect(summary.averageReturn).toBeNull();
  });

  it("weights the average return by resolved entries and converts points to a fraction", () => {
    const summary = aggregate(
      overview([
        strategy({ resolved: 3, winners: 3, averageReturnPct: "4.0" }),
        strategy({ resolved: 1, winners: 0, averageReturnPct: "0.0" }),
      ]),
    );
    // (4.0 * 3 + 0.0 * 1) / 4 = 3.0 points = 0.03.
    expect(summary.averageReturn).toBeCloseTo(0.03, 10);
    expect(summary.successRate).toBe(0.75);
  });

  it("ignores strategies that do not support forward outcomes", () => {
    const summary = aggregate(
      overview([
        strategy({ forward: false, fired: 99, resolved: 99, winners: 99 }),
        strategy({ forward: true, fired: 5, resolved: 2, winners: 1 }),
      ]),
    );
    expect(summary.resolved).toBe(2);
    expect(summary.awaitingOutcome).toBe(3);
    expect(summary.unsuccessful).toBe(1);
  });
});
