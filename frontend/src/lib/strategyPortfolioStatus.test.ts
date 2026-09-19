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

describe("strategyPortfolioStatus — the core sleeve is a second path (#3222)", () => {
  /** The backend's own verdict is `execution_action`; nothing else is read. */
  function core(patch: Record<string, unknown> = {}) {
    return { execution_action: "rebalance", state: "ready", ...patch } as never;
  }
  /** Only the strategy pipeline is blocked — the state measured on dev 2026-09-19. */
  const PIPELINE_ONLY = { automation_readiness: { ready: false, state: "no_capital_candidates", capital_candidate_count: 0 } };

  it("reports trading when the sleeve is open and only the pipeline is blocked", () => {
    const status = strategyPortfolioStatus(overview(PIPELINE_ONLY), core());
    expect(status.trading).toBe(true);
    expect(status.headline).toBe("Trading — core sleeve");
    expect(status.tone).toBe("ok");
  });

  it("still lists the evidence blocker — it is true, and it is what a STRATEGY must clear", () => {
    const status = strategyPortfolioStatus(overview(PIPELINE_ONLY), core());
    expect(status.blockers.map((b) => b.key)).toEqual(["no_approved_strategies"]);
  });

  it("reads the backend's execution_action, not an empty blocker list", () => {
    // Measured on dev: the live sleeve returns `rebalance` while ALSO carrying a
    // `core_live_snapshot_required` blocker, so "no blockers" is the wrong
    // predicate and would have read the working sleeve as blocked.
    const status = strategyPortfolioStatus(
      overview(PIPELINE_ONLY),
      core({ blockers: [{ code: "core_live_snapshot_required", detail: "…" }] }),
    );
    expect(status.trading).toBe(true);
  });

  it("does not flip when the sleeve itself is blocked", () => {
    const status = strategyPortfolioStatus(overview(PIPELINE_ONLY), core({ execution_action: "blocked" }));
    expect(status.trading).toBe(false);
    expect(status.headline).toBe("Not trading");
  });

  it("refuses rather than deciding while the sleeve is unknown", () => {
    // Codex ckpt-2 — `coreSleeve.data ?? null` is `null` while the request is in
    // flight AND after it fails, so collapsing that into "blocked" prints a
    // definitive `halted` over a path that may be live, permanently if the
    // endpoint errors. Same rule the `Open` tile follows.
    for (const sleeve of [null, undefined]) {
      const status = sleeve === undefined
        ? strategyPortfolioStatus(overview(PIPELINE_ONLY))
        : strategyPortfolioStatus(overview(PIPELINE_ONLY), sleeve);
      expect(status.trading).toBeNull();
      expect(status.headline).toBe("Checking the core sleeve…");
      expect(status.blockers.map((b) => b.key)).toEqual(["no_approved_strategies"]);
    }
  });

  it("does NOT defer when something the sleeve cannot clear is blocking", () => {
    // Deferring here would hide a kill switch behind a spinner. There is nothing
    // to wait for: the sleeve spends this pot and cannot clear these.
    const status = strategyPortfolioStatus(
      overview({
        ...PIPELINE_ONLY,
        entry_block: { new_entries_blocked: true, global_kill_active: true, global_kill_reason: "drill", global_kill_activated_at: null, global_kill_activated_by: null, execution_block_reasons: [] },
      }),
      null,
    );
    expect(status.trading).toBe(false);
    expect(status.tone).toBe("risk");
  });

  it("NEVER overrides the kill switch", () => {
    const status = strategyPortfolioStatus(
      overview({
        ...PIPELINE_ONLY,
        entry_block: { new_entries_blocked: true, global_kill_active: true, global_kill_reason: "drill", global_kill_activated_at: null, global_kill_activated_by: null, execution_block_reasons: [] },
      }),
      core(),
    );
    expect(status.trading).toBe(false);
    expect(status.tone).toBe("risk");
  });

  it.each([
    ["a backend execution block", { entry_block: { new_entries_blocked: true, global_kill_active: false, global_kill_reason: null, global_kill_activated_at: null, global_kill_activated_by: null, execution_block_reasons: ["automatic trading disabled"] } }],
    ["an unfunded pot", { paper_pool: { configured: true, enabled: true, effective_capital: "0", currency: "USD", capital_limit: "0", capital_mode: "fixed", approval_mode: "manual", reserved_capital: "0", invested_capital: "0", remaining_capital: "0", capital_observation_complete: true, mandate: { configured: true, risk_profile: "balanced" }, available_mandates: [] } }],
    ["a missing mandate", { paper_pool: { configured: true, enabled: true, effective_capital: "10000", currency: "USD", capital_limit: "10000", capital_mode: "fixed", approval_mode: "manual", reserved_capital: "0", invested_capital: "0", remaining_capital: "10000", capital_observation_complete: true, mandate: { configured: false, risk_profile: "unconfigured" }, available_mandates: [] } }],
  ])("does not paper over %s — the sleeve spends this same pot", (_name, patch) => {
    const status = strategyPortfolioStatus(overview({ ...PIPELINE_ONLY, ...patch }), core());
    expect(status.trading).toBe(false);
    expect(status.headline).toBe("Not trading");
  });
});
