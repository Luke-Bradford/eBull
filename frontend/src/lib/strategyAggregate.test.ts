import { describe, expect, it } from "vitest";

import type { StrategyOverviewResponse, StrategyOwnedPosition } from "@/api/types";
import { NAMED_LIST_LIMIT, aggregate, namedList, positionsOutsideStrategyPnl, potWealthSummary } from "@/lib/strategyAggregate";

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

function ownedPosition(overrides: Partial<StrategyOwnedPosition>): StrategyOwnedPosition {
  return {
    strategy_id: "s2",
    strategy_title: "Mean reversion",
    currency: "USD",
    ...overrides,
  } as StrategyOwnedPosition;
}

/**
 * The scope limit on `aggregate().totalPnl`. These pin the SIGN of the answer in
 * both directions: a strategy-owned position must NOT raise a caveat (a warning
 * that is always present is not a signal), and a core one must.
 */
describe("positionsOutsideStrategyPnl", () => {
  it("names the core sleeve, whose strategy_id is null", () => {
    const outside = positionsOutsideStrategyPnl([
      ownedPosition({}),
      ownedPosition({ strategy_id: null, strategy_title: "Core / cash mandate", currency: "GBP" }),
    ]);
    expect(outside).toHaveLength(1);
    expect(outside[0]?.strategy_title).toBe("Core / cash mandate");
  });

  it("returns nothing when every position belongs to a strategy the roll-up sums", () => {
    expect(positionsOutsideStrategyPnl([ownedPosition({}), ownedPosition({ strategy_id: "s4" })])).toEqual([]);
  });

  it("returns nothing for an empty list, so an unresolved fetch asserts no exclusion", () => {
    expect(positionsOutsideStrategyPnl([])).toEqual([]);
  });
});

describe("namedList", () => {
  it("dedupes and keeps first-seen order", () => {
    expect(namedList(["GBP", "USD", "GBP"])).toEqual(["GBP", "USD"]);
  });

  it("names up to the limit without an overflow entry", () => {
    expect(namedList(["a", "b", "c"])).toEqual(["a", "b", "c"]);
    expect(namedList(["a", "b", "c"])).toHaveLength(NAMED_LIST_LIMIT);
  });

  it("COUNTS the overflow rather than truncating it silently", () => {
    // The point of the bound: a list that stops at three saying nothing reads
    // as "these are all of them" — the defect class the caveat exists to fix.
    expect(namedList(["a", "b", "c", "d", "e"])).toEqual(["a", "b", "c", "+2 more"]);
  });

  it("returns nothing for no values, so the caller renders nothing rather than '0 of'", () => {
    expect(namedList([])).toEqual([]);
  });
});

describe("potWealthSummary (#3334)", () => {
  const point = (date: string, pot: string, total: string, flow = "0", complete = true) => ({
    date,
    principal: "500",
    external_flow: flow,
    realised_pnl: "0",
    unrealised_pnl: total,
    total_pnl: total,
    pot_value: pot,
    complete,
    incomplete_reasons: complete ? [] : ["owned_position_mark_missing"],
    period_return: complete ? "0.001" : null,
    period_start: complete ? "2026-09-17" : null,
    cumulative_return: complete ? "0.001" : null,
  });

  it("reads the last close and nets a funding flow out of the day's change", () => {
    const summary = potWealthSummary([
      point("2026-09-17", "0", "0"),
      point("2026-09-18", "500.5", "0.5", "500"),
    ]);
    expect(summary?.date).toBe("2026-09-18");
    expect(summary?.potValue).toBe(500.5);
    expect(summary?.totalPnl).toBe(0.5);
    // 500.5 - 0 - 500 funding = 0.5, not +500.5.
    expect(summary?.dayPnl).toBeCloseTo(0.5);
    // Returns are passed through from the server, never derived here.
    expect(summary?.periodReturn).toBe(0.001);
    expect(summary?.periodStart).toBe("2026-09-17");
    expect(summary?.cumulativeReturn).toBe(0.001);
  });

  it("skips an incomplete close rather than showing its partial sum", () => {
    const summary = potWealthSummary([
      point("2026-09-21", "504", "4"),
      point("2026-09-22", "490", "-10", "0", false),
    ]);
    expect(summary?.date).toBe("2026-09-21");
    expect(summary?.totalPnl).toBe(4);
    expect(summary?.dayPnl).toBeNull();
  });

  it("returns null when no close is complete", () => {
    expect(potWealthSummary([])).toBeNull();
    expect(potWealthSummary([point("2026-09-22", "490", "-10", "0", false)])).toBeNull();
  });
});
