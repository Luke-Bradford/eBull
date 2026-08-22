import type { StrategyOverviewResponse } from "@/api/types";

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
  | "automatic_trading_disabled"
  | "no_capital"
  | "no_mandate"
  | "no_approved_strategies";

export interface StrategyBlocker {
  readonly key: StrategyBlockerKey;
  /** One line, operator-facing. No enum names, no snake_case. */
  readonly label: string;
  /** Optional second line carrying the specifics (when, who, how many). */
  readonly detail: string | null;
}

export interface StrategyPortfolioStatus {
  readonly trading: boolean;
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

export function strategyPortfolioStatus(overview: StrategyOverviewResponse): StrategyPortfolioStatus {
  const blockers: StrategyBlocker[] = [];
  const { entry_block: entryBlock, paper_pool: pool, automation_readiness: readiness } = overview;

  if (entryBlock.global_kill_active) {
    blockers.push({
      key: "global_kill",
      label: "Kill switch is on — no order path is open",
      detail: entryBlock.global_kill_reason,
    });
  }

  if (!overview.execution_enabled) {
    blockers.push({
      key: "automatic_trading_disabled",
      label: "Automatic trading is switched off",
      detail: null,
    });
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
  // The kill switch is a safety state, not a setup step — it reads as risk;
  // everything else is "not set up yet", which is a warning at most.
  const tone = blockers.some((b) => b.key === "global_kill") ? "risk" : "warn";
  return { trading: false, headline: "Not trading", tone, blockers };
}
