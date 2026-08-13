import { describe, expect, it } from "vitest";

import type { ScorePenaltyItem } from "@/api/types";
import { humanizePenaltyName, isReward, toScoreChips } from "@/lib/scoreExplanation";

describe("isReward", () => {
  it("trusts an explicit kind", () => {
    expect(isReward({ name: "strong_calmar", reason: "", kind: "reward", addition: 0.03 })).toBe(true);
    expect(isReward({ name: "deep_drawdown", reason: "", kind: "penalty", deduction: 0.04 })).toBe(false);
  });

  it("treats a kind-less legacy row as a penalty (pre-#1635 payload shape)", () => {
    // Rows written before rewards existed carry only {name, deduction, reason}.
    expect(isReward({ name: "high_realized_volatility", reason: "", deduction: 0.04 })).toBe(false);
  });

  it("falls back to the magnitude field when kind is absent but an addition is present", () => {
    expect(isReward({ name: "strong_calmar", reason: "", addition: 0.03 })).toBe(true);
  });
});

describe("humanizePenaltyName", () => {
  it("uses curated copy for every name the scorer can emit", () => {
    // The nine identifiers in app/services/scoring.py.
    const names = [
      "stale_thesis",
      "missing_critical_data",
      "wide_spread",
      "high_red_flag",
      "extreme_dilution",
      "low_confidence",
      "high_realized_volatility",
      "deep_drawdown",
      "strong_calmar",
    ];
    for (const n of names) {
      expect(humanizePenaltyName(n), n).not.toContain("_");
      expect(humanizePenaltyName(n)[0], n).toBe(humanizePenaltyName(n)[0]?.toUpperCase());
    }
    expect(humanizePenaltyName("high_realized_volatility")).toBe("High realized volatility");
  });

  it("degrades an unknown scorer name instead of dropping it — the backend may add penalties first", () => {
    expect(humanizePenaltyName("brand_new_penalty")).toBe("Brand new penalty");
  });
});

describe("toScoreChips", () => {
  it("returns nothing for a score with no penalties row", () => {
    expect(toScoreChips(null)).toEqual([]);
    expect(toScoreChips([])).toEqual([]);
  });

  it("signs a penalty negative and a reward positive, with the scorer reason on hover", () => {
    const items: ScorePenaltyItem[] = [
      {
        name: "high_realized_volatility",
        reason: "3y annualized vol=0.71 > 0.60",
        kind: "penalty",
        deduction: 0.04,
      },
      {
        name: "strong_calmar",
        reason: "3y total-return Calmar=1.85 > high threshold 0.75 (mode scale 0.75)",
        kind: "reward",
        addition: 0.03,
      },
    ];
    const [penalty, reward] = toScoreChips(items);

    expect(penalty).toMatchObject({
      label: "High realized volatility",
      delta: "−0.04", // U+2212 MINUS, not a hyphen
      tone: "risk",
      title: "3y annualized vol=0.71 > 0.60",
    });
    expect(reward).toMatchObject({
      label: "Strong Calmar",
      delta: "+0.03",
      tone: "ok",
    });
  });

  it("renders a magnitude-less entry as a label rather than 'NaN' or nothing", () => {
    const [chip] = toScoreChips([{ name: "wide_spread", reason: "spread flag set" }]);
    expect(chip?.label).toBe("Wide spread");
    expect(chip?.delta).toBe("");
    expect(chip?.tone).toBe("risk");
  });

  it("keys chips uniquely so a repeated scorer name cannot collide", () => {
    const chips = toScoreChips([
      { name: "deep_drawdown", reason: "a", kind: "penalty", deduction: 0.02 },
      { name: "deep_drawdown", reason: "b", kind: "penalty", deduction: 0.03 },
    ]);
    expect(new Set(chips.map((c) => c.key)).size).toBe(2);
  });
});
