import { describe, expect, it } from "vitest";

import type { BudgetStateResponse } from "@/api/types";
import { classifyDeployment } from "@/components/dashboard/SummaryCards";

function budget(overrides: Partial<BudgetStateResponse>): BudgetStateResponse {
  return {
    cash_balance: 1000,
    deployed_capital: 0,
    mirror_equity: 0,
    working_budget: 100000,
    estimated_tax_gbp: 0,
    estimated_tax_usd: 0,
    gbp_usd_rate: null,
    cash_buffer_reserve: 5000,
    available_for_deployment: 50000,
    cash_buffer_pct: 0.05,
    cgt_scenario: "higher",
    tax_year: "2026/27",
    ...overrides,
  };
}

describe("classifyDeployment", () => {
  it("flags unknown cash distinctly", () => {
    expect(classifyDeployment(budget({ available_for_deployment: null }))).toEqual({
      tone: undefined,
      hint: "Cash unknown",
      isNull: true,
    });
  });

  it("labels a negative budget as below cash buffer, not merely low (the bug)", () => {
    // Real dev figures: cash below the 5%-of-AUM reserve floor → guard blocks.
    expect(
      classifyDeployment(budget({ available_for_deployment: -3569.43, working_budget: 105457.84 })),
    ).toEqual({ tone: "negative", hint: "At or below cash buffer reserve", isNull: false });
  });

  it("treats zero as below buffer too (mirrors the guard's <= 0 threshold)", () => {
    expect(classifyDeployment(budget({ available_for_deployment: 0 })).hint).toBe(
      "At or below cash buffer reserve",
    );
  });

  it("labels a small positive budget (<5% of working) as low", () => {
    expect(
      classifyDeployment(budget({ available_for_deployment: 100, working_budget: 100000 })),
    ).toEqual({ tone: "negative", hint: "Low deployment capital", isNull: false });
  });

  it("leaves a healthy budget with positive tone and no hint", () => {
    expect(
      classifyDeployment(budget({ available_for_deployment: 50000, working_budget: 100000 })),
    ).toEqual({ tone: "positive", hint: undefined, isNull: false });
  });

  it("does not flag a positive budget as low when working_budget is unknown", () => {
    expect(
      classifyDeployment(budget({ available_for_deployment: 100, working_budget: null })),
    ).toEqual({ tone: "positive", hint: undefined, isNull: false });
  });
});
