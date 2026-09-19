import type { CoreSleeveResponse, StrategyOverviewResponse } from "@/api/types";

/**
 * The plain-English answer to "is the fenced-off pot trading, and if not, why?"
 *
 * #2868. The Strategies overview carries process state across ~25 panels and
 * 667 KB, and the two facts that actually decide whether anything can happen
 * — the kill switch and whether capital is assigned — were rendered mid-page
 * among zeroed metrics. This computes the verdict once, in order, so the
 * portfolio lens can lead with it.
 *
 * ⚠ Blockers are ordered OUTERMOST GATE FIRST, and that order is the point.
 * Funding the pot while the kill switch is on changes nothing, so telling the
 * operator "no capital assigned" first would send them to do work that cannot
 * have an effect. Each blocker is only actionable once every blocker above it
 * is cleared.
 *
 * Pure over the payload so it is table-testable without a DOM or a fixture DB.
 */
export type StrategyBlockerKey =
  | "global_kill"
  | "entries_blocked"
  | "no_capital"
  | "no_mandate"
  | "no_approved_strategies";

/**
 * Friendlier wording for the block reasons the backend emits today. Anything
 * unmapped is passed through verbatim rather than swallowed — a new backend
 * reason must still reach the operator, even if it reads like an enum.
 */
const BLOCK_REASON_LABELS: Record<string, string> = {
  "automatic trading disabled": "Automatic trading is switched off",
  "runtime configuration unavailable": "Runtime configuration is unavailable",
  "kill switch state unavailable": "Kill-switch state is unavailable",
};

export interface StrategyBlocker {
  readonly key: StrategyBlockerKey;
  /** One line, operator-facing. No enum names, no snake_case. */
  readonly label: string;
  /** Optional second line carrying the specifics (when, who, how many). */
  readonly detail: string | null;
}

export interface StrategyPortfolioStatus {
  /**
   * `null` = NOT YET KNOWN, and it is a third state rather than a falsy
   * `false` (#3222, Codex ckpt-2). The verdict depends on the core sleeve,
   * which arrives on its own request; collapsing "the request has not landed"
   * into "blocked" prints a definitive `halted` over a path that may be live,
   * and prints it permanently when `/strategies/core-sleeve` errors. Same rule
   * the `Open` tile follows: an unknown count refuses rather than reading 0.
   */
  readonly trading: boolean | null;
  readonly headline: string;
  readonly tone: "ok" | "warn" | "risk";
  readonly blockers: readonly StrategyBlocker[];
}

/** `"0"`, `""`, `null` and unparseable all mean "no capital". */
function hasCapital(amount: string | null): boolean {
  if (amount === null || amount.trim() === "") return false;
  const value = Number(amount);
  return Number.isFinite(value) && value > 0;
}

/**
 * Is the core/cash sleeve an OPEN path right now? (#3222)
 *
 * ⚠ Read off the backend's own verdict — `execution_action` — and not re-derived
 * from `state`, `mandate.enabled` and `blockers`, for the same reason the
 * entry-block loop below does not re-derive "can entries happen". Measured on
 * dev 2026-09-19: the live sleeve returns `execution_action: "rebalance"` while
 * ALSO carrying a `core_live_snapshot_required` blocker, so "no blockers" is the
 * wrong predicate and would have read as blocked.
 */
function coreSleeveIsOpen(core: CoreSleeveResponse): boolean {
  return core.execution_action !== "blocked";
}

/**
 * @param core the loaded core-sleeve payload, or `null` when its request has
 *   not landed or failed. ⚠ `null` means UNKNOWN, never "blocked": the endpoint
 *   always answers, and `state: "unavailable"` / `execution_action: "blocked"`
 *   are how it says the sleeve is not a path. Defaults to `null` so a caller
 *   that does not fetch it gets the honest answer rather than a confident wrong
 *   one.
 */
export function strategyPortfolioStatus(
  overview: StrategyOverviewResponse,
  core: CoreSleeveResponse | null = null,
): StrategyPortfolioStatus {
  const blockers: StrategyBlocker[] = [];
  const { entry_block: entryBlock, paper_pool: pool, automation_readiness: readiness } = overview;

  if (entryBlock.global_kill_active) {
    blockers.push({
      key: "global_kill",
      label: "Kill switch is on — no order path is open",
      detail: entryBlock.global_kill_reason,
    });
  }

  // ⚠ Do NOT re-derive "can entries happen" here. The backend owns that rule
  // (`StrategyEntryBlockState.new_entries_blocked` = kill OR any execution block
  // OR auto-trading off), and `execution_block_reasons` carries blocks that come
  // from `strategy_execution_blocks` independently of the kill switch and of the
  // auto-trading flag. An earlier version of this function keyed only off
  // `execution_enabled` and could therefore report "Trading" while the backend
  // was refusing every entry (Codex ckpt-2). Surface the backend's own reasons.
  for (const reason of entryBlock.execution_block_reasons) {
    blockers.push({
      key: "entries_blocked",
      label: BLOCK_REASON_LABELS[reason] ?? reason,
      detail: null,
    });
  }
  // Fail closed: entries are blocked for a reason the payload did not enumerate.
  if (entryBlock.new_entries_blocked && blockers.length === 0) {
    blockers.push({ key: "entries_blocked", label: "New entries are blocked", detail: null });
  }

  // `configured` and a positive limit are separate failures upstream but one
  // sentence to the operator: there is no money in the pot either way.
  if (!pool.configured || !hasCapital(pool.effective_capital)) {
    blockers.push({
      key: "no_capital",
      label: "No capital is assigned to the pot",
      detail: pool.configured ? "The pot exists but its limit is zero" : null,
    });
  } else if (!pool.enabled) {
    blockers.push({ key: "no_capital", label: "The pot is funded but paused", detail: null });
  }

  if (!pool.mandate.configured) {
    blockers.push({
      key: "no_mandate",
      label: "No risk mandate is set",
      detail: pool.available_mandates.length
        ? `${pool.available_mandates.length} available: ${pool.available_mandates
            .map((m) => m.risk_profile)
            .join(", ")}`
        : null,
    });
  }

  if (!readiness.ready) {
    blockers.push({
      key: "no_approved_strategies",
      label: "No strategy has passed the evidence bar",
      detail: `${readiness.capital_candidate_count} of ${overview.strategies.length} registered as capital candidates`,
    });
  }

  if (blockers.length === 0) {
    return { trading: true, headline: "Trading", tone: "ok", blockers: [] };
  }

  // ⚠⚠ #3222 — the core sleeve is a SEPARATE authority path and does not appear
  // in `StrategyOverviewResponse` at all, so before this branch the verdict was
  // computed over a payload that structurally could not see the one path that
  // was trading. Measured on dev 2026-09-19: the pot reported
  // `reserved_capital "225.038507"` of a `"500.000000"` limit, the page rendered
  // "Close all 1 positions", and the headline above them read "Not trading ·
  // halted" — from `no_approved_strategies` alone, which is TRUE (all 11
  // registered strategies are `harness_validation`; #3104 has the missing
  // promotion-evidence producer) and was being presented as the whole answer.
  //
  // ⚠ It neutralises `no_approved_strategies` and NOTHING ELSE, deliberately.
  // The kill switch, a backend execution block, an unfunded pot and a missing
  // mandate each stop the sleeve too — the sleeve spends this same pot — so a
  // sleeve that claims otherwise is reporting on a state it does not own. The
  // blocker also stays in the array: it is still true, the panel below still
  // lists it, and it is still the thing that has to change for a STRATEGY to
  // earn capital.
  if (blockers.every((b) => b.key === "no_approved_strategies")) {
    // ⚠ The sleeve is the DECIDING input only here. Above this line the pot is
    // blocked for a reason the sleeve cannot clear, so there is nothing to wait
    // for and deferring the verdict would hide a kill switch behind a spinner.
    if (core === null) {
      return { trading: null, headline: "Checking the core sleeve…", tone: "warn", blockers };
    }
    if (coreSleeveIsOpen(core)) {
      return { trading: true, headline: "Trading — core sleeve", tone: "ok", blockers };
    }
  }

  // The kill switch is a safety state, not a setup step — it reads as risk;
  // everything else is "not set up yet", which is a warning at most.
  const tone = blockers.some((b) => b.key === "global_kill") ? "risk" : "warn";
  return { trading: false, headline: "Not trading", tone, blockers };
}
