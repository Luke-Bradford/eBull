import { describe, expect, it } from "vitest";

import type { CoreSleeveResponse, StrategyOverviewResponse, StrategyOwnedPosition } from "@/api/types";
import { investNarrative } from "@/lib/investSummary";

function overview(ready: boolean, strategyCount = 11): StrategyOverviewResponse {
  return {
    automation_readiness: { ready, state: ready ? "ready" : "no_capital_candidates", capital_candidate_count: 0 },
    strategies: Array.from({ length: strategyCount }, () => ({})),
  } as unknown as StrategyOverviewResponse;
}

function core(state: CoreSleeveResponse["state"], enabled: boolean | null, symbol: string | null = "SPY"): CoreSleeveResponse {
  return {
    state,
    selected_symbol: state === "ready" ? symbol : null,
    earliest_possible_verdict_at: "2026-10-01T20:00:00Z",
    mandate: { enabled },
  } as unknown as CoreSleeveResponse;
}

function position(strategyId: string | null, symbol: string): StrategyOwnedPosition {
  return { strategy_id: strategyId, symbol } as unknown as StrategyOwnedPosition;
}

describe("investNarrative", () => {
  it("says the money is in the index sleeve only when a sleeve holding exists", () => {
    const n = investNarrative(overview(false), core("ready", true), [position(null, "SPY")]);
    expect(n.whereMoney).toBe("No strategy has passed its tests yet. Your money is in the index sleeve (SPY).");
    expect(n.next[0]).toMatch(/checked once a day/);
    expect(n.next[1]).toBe("11 strategies are under test. None is given money until it passes its evidence bar.");
  });

  it("does not claim the money is invested when the sleeve is on but holds nothing", () => {
    const n = investNarrative(overview(false), core("ready", true), []);
    expect(n.whereMoney).toContain("The index sleeve (SPY) is switched on but holds nothing yet.");
    expect(n.whereMoney).not.toContain("Your money is in");
  });

  it("ignores strategy-owned positions when deciding whether the sleeve holds anything", () => {
    const n = investNarrative(overview(false), core("ready", false), [position("s4", "AAPL")]);
    expect(n.whereMoney).toContain("is chosen but switched off, so it holds nothing.");
    expect(n.next).toContain("The index sleeve buys nothing until it is switched on.");
  });

  it("warns that a switched-off sleeve still holding a position will not be rebalanced", () => {
    const n = investNarrative(overview(false), core("ready", false), [position(null, "SPY")]);
    expect(n.whereMoney).toContain("Your money is in the index sleeve (SPY). The sleeve is switched off");
  });

  it.each([
    ["cash", "chose cash"],
    ["awaiting_verdict", "result is due"],
    ["evidence_collecting", "still being chosen"],
    ["unavailable", "unavailable right now"],
  ] as const)("names the %s sleeve state without inventing a holding", (state, phrase) => {
    const n = investNarrative(overview(false), core(state, null), []);
    expect(n.whereMoney).toContain(phrase);
    expect(n.whereMoney).not.toContain("Your money is in");
  });

  it("says checking while a read is unresolved, and says so plainly when it failed", () => {
    expect(investNarrative(overview(false), null, null).whereMoney).toContain("Checking what the index sleeve holds");
    expect(investNarrative(overview(false), null, []).whereMoney).toContain("Checking");
    expect(
      investNarrative(overview(false), core("ready", true), null, { core: false, positions: true }).whereMoney,
    ).toContain("could not be loaded");
    expect(investNarrative(overview(false), null, [], { core: true, positions: false }).whereMoney).toContain(
      "status could not be loaded",
    );
  });

  it("leads with the passed strategy when readiness is met", () => {
    const n = investNarrative(overview(true), core("ready", true), [position(null, "SPY")]);
    expect(n.whereMoney.startsWith("A strategy has passed its evidence bar")).toBe(true);
    expect(n.next.some((line) => line.includes("under test"))).toBe(false);
  });

  it("uses the singular for one strategy", () => {
    expect(investNarrative(overview(false, 1), null, null).next).toEqual([
      "1 strategy is under test. None is given money until it passes its evidence bar.",
    ]);
  });
});
