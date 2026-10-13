import { describe, expect, it } from "vitest";

import type { StrategyUcitsComparator } from "@/api/types";
import { comparatorLines, formatPoints } from "@/lib/ucitsComparator";

function comparator(overrides: Partial<StrategyUcitsComparator> = {}): StrategyUcitsComparator {
  return {
    symbol: "CSPX.L",
    instrument_id: 3434,
    label: "iShares Core S&P 500 UCITS ETF (USD, Acc)",
    basis: "accumulating_ucits_market_price_return_usd",
    window_start: "2026-09-17",
    window_end: "2026-10-09",
    start_close_date: "2026-09-17",
    end_close_date: "2026-10-09",
    return: "0.0227620737",
    unavailable_reason: null,
    engine_excess: "-0.0118247506",
    hurdle: {
      core_target_weight: "0.5000",
      core_weight: "0.35000000",
      active_weight: "0.3000",
      core_mandate_event_id: 35,
      pool_event_id: 5,
      multiplier: "2.166666666666666666666666667",
      hurdle_return: "0.0493178264",
      required_active_excess: "0.0265557527",
      unavailable_reason: null,
    },
    gbp: {
      fx_source: "ecb_reference_fx_rates_daily",
      fx_start_date: "2026-09-17",
      fx_end_date: "2026-10-09",
      fx_multiplier: "1.011811444928970812488295567",
      comparator_return: "0.0348423716522525655421635105",
      engine_return: "0.0228776267021589662644800557",
      engine_excess: "-0.0119647449500935992776834548",
      unavailable_reason: null,
    },
    ...overrides,
  };
}

describe("comparatorLines", () => {
  it("states the GBP basis, that it is not after tax, and what is known about the ISA", () => {
    const title = comparatorLines(comparator(), null).line1Title;
    expect(title).toContain("ECB reference rates of 17 Sept and 9 Oct");
    expect(title).toContain("the active return needed, in USD, is unchanged");
    expect(title).toContain("Not after UK tax.");
    expect(title).toContain("the same fund held in an ISA would pay no UK tax on gains");
    expect(title).toContain("separate Moneyfarm GBP account the engine cannot trade");
  });

  it("drops only the GBP clause when the GBP restatement is unavailable", () => {
    const lines = comparatorLines(
      comparator({
        gbp: {
          ...comparator().gbp,
          fx_multiplier: null,
          comparator_return: null,
          engine_return: null,
          engine_excess: null,
          unavailable_reason: "fx_rate_stale",
        },
      }),
      null,
    );
    expect(lines.line1).toBe("vs UCITS comparator iShares Core S&P 500 (USD, Acc), 17 Sept – 9 Oct: +2.28% · engine -1.18 pts");
    expect(lines.line1).not.toContain("0.00");
    expect(lines.line1Title).toContain("No GBP figures: the ECB USD/GBP rates do not cover the window.");
    expect(lines.line1Title).not.toContain("Not after UK tax.");
    expect(lines.line2).toContain("+4.93% in USD");
  });

  it("tolerates a payload from before the GBP restatement", () => {
    const { gbp: _omitted, ...older } = comparator();
    const lines = comparatorLines(older as StrategyUcitsComparator, null);
    expect(lines.line1).toBe("vs UCITS comparator iShares Core S&P 500 (USD, Acc), 17 Sept – 9 Oct: +2.28% · engine -1.18 pts");
  });

  it("formats the return, the excess in points and the hurdle", () => {
    const lines = comparatorLines(comparator(), "2026-09-18");
    expect(lines.line1).toBe(
      "vs UCITS comparator iShares Core S&P 500 (USD, Acc), 17 Sept – 9 Oct: +2.28% · engine -1.18 pts · in GBP +3.48% · engine -1.20 pts",
    );
    expect(lines.line2).toBe(
      "Active sleeves would need +4.93% in USD (+2.66 percentage points over the comparator) to match it",
    );
    expect(lines.line1Title).toContain("before first funding on 18 Sept");
    expect(lines.line1Title).toContain("CFD");
  });

  it("builds the assumption text from the returned weights", () => {
    const lines = comparatorLines(
      comparator({
        hurdle: { ...comparator().hurdle, core_target_weight: "0.4000", core_weight: "0.30000000", active_weight: "0.2500" },
      }),
      null,
    );
    expect(lines.line2Title).toBe(
      "At today's mandate: 25% active at its full cap, 30% core (its 40% target of the other 75%) assumed to track the comparator, 45% cash earning 0 in USD; held without rebalancing or leverage.",
    );
  });

  it("shows a dash and the reason, never 0, when the comparator is unavailable", () => {
    const lines = comparatorLines(
      comparator({
        return: null,
        engine_excess: null,
        unavailable_reason: "comparator_stale",
        hurdle: { ...comparator().hurdle, hurdle_return: null, required_active_excess: null, unavailable_reason: "comparator_return_unavailable" },
      }),
      null,
    );
    expect(lines.line1).toContain(": — (");
    expect(lines.line1).not.toContain("0.00%");
    expect(lines.line2).toBe("No hurdle: it needs the comparator's return");
  });

  it("drops the hurdle number but keeps line 1 when the hurdle is unavailable", () => {
    const lines = comparatorLines(
      comparator({
        hurdle: { ...comparator().hurdle, hurdle_return: null, required_active_excess: null, unavailable_reason: "hurdle_infeasible" },
      }),
      null,
    );
    expect(lines.line1).toContain("+2.28%");
    expect(lines.line2).toBe(
      "Under these assumptions, even a total loss in the active sleeves would leave the pot ahead",
    );
    expect(lines.line2Title).toBeNull();
  });

  it("notes a resolution to an earlier close", () => {
    const lines = comparatorLines(comparator({ end_close_date: "2026-10-08" }), null);
    expect(lines.line1Title).toContain("latest earlier close");
  });
});

describe("formatPoints", () => {
  it("formats a fraction difference in percentage points", () => {
    expect(formatPoints(-0.011726)).toBe("-1.17");
    expect(formatPoints(0)).toBe("0.00");
  });
});
