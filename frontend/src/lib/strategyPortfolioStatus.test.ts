import { describe, expect, it } from "vitest";

import { strategyPortfolioStatus } from "@/lib/strategyPortfolioStatus";
import type { StrategyOverviewResponse } from "@/api/types";

/** Everything clear: pot funded and enabled, mandate set, readiness ready, no kill. */
function overview(patch: Record<string, unknown> = {}): StrategyOverviewResponse {
  const base = {
    as_of: "2026-08-23T00:00:00Z",
    demo_connection: true,
    execution_enabled: true,
    live_execution_enabled: false,
    live_strategy_activation_available: false,
    live_strategy_activation_blocker: "live_strategy_broker_contract_not_validated",
    storage_policy: "fired_signals_and_material_mutations_only",
    entry_block: {
      new_entries_blocked: false,
      global_kill_active: false,
      global_kill_reason: null,
      global_kill_activated_at: null,
      global_kill_activated_by: null,
      execution_block_reasons: [],
    },
    paper_pool: {
      configured: true,
      enabled: true,
      capital_limit: "10000",
      capital_mode: "fixed",
      approval_mode: "manual",
      effective_capital: "10000",
      currency: "USD",
      reserved_capital: "0",
      invested_capital: "0",
      remaining_capital: "10000",
      capital_observation_complete: true,
      mandate: { configured: true, risk_profile: "balanced" },
      available_mandates: [],
    },
    automation_readiness: { ready: true, state: "ready", capital_candidate_count: 2 },
    strategies: [{}, {}, {}],
  };
  return { ...base, ...patch } as unknown as StrategyOverviewResponse;
}

describe("strategyPortfolioStatus", () => {
  it("reports trading with no blockers when every gate is clear", () => {
    const status = strategyPortfolioStatus(overview());
    expect(status).toEqual({ trading: true, headline: "Trading", tone: "ok", blockers: [] });
  });

  it("orders the kill switch first — it is the outermost gate", () => {
    // Everything is wrong at once. The operator must see the kill switch first,
    // because funding a pot underneath an active kill switch changes nothing.
    const status = strategyPortfolioStatus(
      overview({
        execution_enabled: false,
        entry_block: {
          new_entries_blocked: true,
          global_kill_active: true,
          global_kill_reason: "autonomy loop unattended",
          global_kill_activated_at: "2026-06-28T01:32:30Z",
          global_kill_activated_by: "monitor",
          execution_block_reasons: ["automatic trading disabled"],
        },
        paper_pool: {
          configured: false,
          enabled: false,
          capital_limit: "0",
          capital_mode: "fixed",
          approval_mode: "manual",
          effective_capital: "0",
          currency: "USD",
          reserved_capital: "0",
          invested_capital: "0",
          remaining_capital: "0",
          mandate: { configured: false, risk_profile: "unconfigured" },
          available_mandates: [{ risk_profile: "cautious" }, { risk_profile: "balanced" }],
        },
        automation_readiness: { ready: false, state: "no_capital_candidates", capital_candidate_count: 0 },
      }),
    );
    expect(status.trading).toBe(false);
    expect(status.tone).toBe("risk");
    expect(status.blockers.map((b) => b.key)).toEqual([
      "global_kill",
      "entries_blocked",
      "no_capital",
      "no_mandate",
      "no_approved_strategies",
    ]);
    expect(status.blockers[0]?.detail).toBe("autonomy loop unattended");
  });

  it("is a warning, not a risk, when only setup steps are outstanding", () => {
    const status = strategyPortfolioStatus(
      overview({
        paper_pool: {
          ...overview().paper_pool,
          configured: false,
          effective_capital: "0",
          mandate: { configured: false, risk_profile: "unconfigured" },
          available_mandates: [],
        },
      }),
    );
    expect(status.tone).toBe("warn");
    expect(status.blockers.some((b) => b.key === "global_kill")).toBe(false);
  });

  it.each([
    ["null", null],
    ["empty", ""],
    ["zero", "0"],
    ["unparseable", "n/a"],
  ])("treats %s effective capital as no capital", (_label, effective) => {
    const pool = { ...overview().paper_pool, effective_capital: effective };
    const status = strategyPortfolioStatus(overview({ paper_pool: pool }));
    expect(status.blockers.map((b) => b.key)).toContain("no_capital");
  });

  it("distinguishes a funded-but-paused pot from an unfunded one", () => {
    const pool = { ...overview().paper_pool, enabled: false };
    const status = strategyPortfolioStatus(overview({ paper_pool: pool }));
    const capital = status.blockers.find((b) => b.key === "no_capital");
    expect(capital?.label).toBe("The pot is funded but paused");
  });

  it("says the pot exists when it is configured with a zero limit", () => {
    const pool = { ...overview().paper_pool, effective_capital: "0" };
    const capital = strategyPortfolioStatus(overview({ paper_pool: pool })).blockers.find(
      (b) => b.key === "no_capital",
    );
    expect(capital?.detail).toBe("The pot exists but its limit is zero");
  });

  it("counts candidates against the registered strategy total", () => {
    const status = strategyPortfolioStatus(
      overview({
        automation_readiness: { ready: false, state: "no_capital_candidates", capital_candidate_count: 0 },
      }),
    );
    const approved = status.blockers.find((b) => b.key === "no_approved_strategies");
    expect(approved?.detail).toBe("0 of 3 registered as capital candidates");
  });

  it("lists the mandates available to choose from", () => {
    const pool = {
      ...overview().paper_pool,
      mandate: { configured: false, risk_profile: "unconfigured" },
      available_mandates: [{ risk_profile: "cautious" }, { risk_profile: "balanced" }, { risk_profile: "growth" }],
    };
    const mandate = strategyPortfolioStatus(overview({ paper_pool: pool })).blockers.find(
      (b) => b.key === "no_mandate",
    );
    expect(mandate?.detail).toBe("3 available: cautious, balanced, growth");
  });
});

describe("strategyPortfolioStatus — entry blocks independent of the kill switch", () => {
  it("does not report trading when an execution block is active with the kill switch off", () => {
    // The regression Codex ckpt-2 caught: `execution_block_reasons` is populated
    // by `strategy_execution_blocks` independently of the kill switch and of the
    // auto-trading flag, so keying only off `execution_enabled` reported
    // "Trading" while the backend refused every entry.
    const status = strategyPortfolioStatus(
      overview({
        execution_enabled: true,
        entry_block: {
          new_entries_blocked: true,
          global_kill_active: false,
          global_kill_reason: null,
          global_kill_activated_at: null,
          global_kill_activated_by: null,
          execution_block_reasons: ["broker contract unverified"],
        },
      }),
    );
    expect(status.trading).toBe(false);
    // Unmapped reasons pass through verbatim rather than being swallowed.
    expect(status.blockers.map((b) => b.label)).toContain("broker contract unverified");
  });

  it("gives the backend's auto-trading block its friendlier wording", () => {
    const status = strategyPortfolioStatus(
      overview({
        execution_enabled: false,
        entry_block: {
          new_entries_blocked: true,
          global_kill_active: false,
          global_kill_reason: null,
          global_kill_activated_at: null,
          global_kill_activated_by: null,
          execution_block_reasons: ["automatic trading disabled"],
        },
      }),
    );
    expect(status.blockers.map((b) => b.label)).toContain("Automatic trading is switched off");
  });

  it("fails closed when entries are blocked for an unenumerated reason", () => {
    const status = strategyPortfolioStatus(
      overview({
        entry_block: {
          new_entries_blocked: true,
          global_kill_active: false,
          global_kill_reason: null,
          global_kill_activated_at: null,
          global_kill_activated_by: null,
          execution_block_reasons: [],
        },
      }),
    );
    expect(status.trading).toBe(false);
    expect(status.blockers.map((b) => b.label)).toContain("New entries are blocked");
  });
});

describe("strategyPortfolioStatus — blocker identity", () => {
  it("keeps every execution-block reason distinguishable when several are active", () => {
    // These share the `entries_blocked` kind, so the kind alone cannot identify
    // a row. The label is what separates them (Codex ckpt-2 on the React keys).
    const status = strategyPortfolioStatus(
      overview({
        entry_block: {
          new_entries_blocked: true,
          global_kill_active: false,
          global_kill_reason: null,
          global_kill_activated_at: null,
          global_kill_activated_by: null,
          execution_block_reasons: ["automatic trading disabled", "runtime configuration unavailable"],
        },
      }),
    );
    const entryBlockers = status.blockers.filter((b) => b.key === "entries_blocked");
    expect(entryBlockers).toHaveLength(2);
    expect(new Set(entryBlockers.map((b) => `${b.key}:${b.label}`)).size).toBe(2);
  });
});
