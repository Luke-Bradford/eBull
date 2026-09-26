import type { CoreSleeveResponse, StrategyOverviewResponse, StrategyOwnedPosition } from "@/api/types";
import { formatDate } from "@/lib/format";

/**
 * The plain-English answer on the Invest page (#3423): where the operator's
 * money is, and what the engine does next.
 *
 * Operator north star (2026-09-26): "a very simple to use page that allows me
 * to put money in and then take my hands off the wheel". So this speaks in
 * sentences, and every sentence is a claim that must be TRUE of the payload,
 * not inferred from the absence of something:
 *
 * - "Your money is in the index sleeve" is said only when the positions read
 *   shows a core-sleeve holding. The sleeve being chosen or switched on is not
 *   the same fact — the core badge's history (#3037) is a sleeve state that was
 *   rendered as a verdict it had not reached.
 * - `null` inputs mean NOT YET KNOWN and produce a "checking" sentence, never a
 *   confident one (the `strategyPortfolioStatus` rule, #3222).
 *
 * Pure over the payloads so it is table-testable without a DOM.
 */
export interface InvestNarrative {
  /** One or two sentences: where the money is, right now. */
  readonly whereMoney: string;
  /** What the engine will do next, one short line each. */
  readonly next: readonly string[];
}

/**
 * The core arm has no strategy identity (#2603): a mandate holding is
 * authorised by a rebalance intent, not a signal, so `strategy_id` is null.
 */
export function coreSleeveHoldings(positions: readonly StrategyOwnedPosition[]): StrategyOwnedPosition[] {
  return positions.filter((position) => position.strategy_id === null);
}

function uniqueSymbols(positions: readonly StrategyOwnedPosition[]): string {
  return [...new Set(positions.map((position) => position.symbol))].join(", ");
}

function coreSleeveSentence(core: CoreSleeveResponse): string {
  const symbol = core.selected_symbol;
  const enabled = core.mandate.enabled === true;
  switch (core.state) {
    case "ready":
      if (symbol === null) return "The index sleeve has no instrument selected, so it holds nothing.";
      return enabled
        ? `The index sleeve (${symbol}) is switched on but holds nothing yet.`
        : `The index sleeve (${symbol}) is chosen but switched off, so it holds nothing.`;
    case "cash":
      return "The index sleeve's own test chose cash, so it holds nothing.";
    case "awaiting_verdict":
      return "The index sleeve's test has finished and its result is due, so it holds nothing yet.";
    case "evidence_collecting":
      return `The index sleeve is still being chosen (earliest result ${formatDate(core.earliest_possible_verdict_at)}), so it holds nothing yet.`;
    case "unavailable":
      return "The index sleeve is unavailable right now, so it holds nothing.";
  }
}

/**
 * @param positions engine-owned positions, or `null` while that read is
 *   unresolved or failed (unknown, NOT empty).
 * @param core the core-sleeve payload, or `null` while unresolved or failed.
 * @param failed which of the two reads FAILED, as opposed to still loading.
 *   A failure must not read as "checking…" forever (#3222).
 */
export function investNarrative(
  overview: StrategyOverviewResponse,
  core: CoreSleeveResponse | null,
  positions: readonly StrategyOwnedPosition[] | null,
  failed: { readonly core: boolean; readonly positions: boolean } = { core: false, positions: false },
): InvestNarrative {
  const readiness = overview.automation_readiness;
  const lead = readiness.ready
    ? "A strategy has passed its evidence bar and can be given part of the pot."
    : "No strategy has passed its tests yet.";

  const coreHeld = positions === null ? null : coreSleeveHoldings(positions);
  // A strategy can hold a position after it stops meeting its bar for NEW money
  // (Codex ckpt-2): readiness gates entries, not what is already held.
  const strategyHeld = positions === null ? [] : positions.filter((position) => position.strategy_id !== null);
  const strategyClause =
    strategyHeld.length > 0 ? ` Strategy positions opened earlier are still held (${uniqueSymbols(strategyHeld)}).` : "";
  let sleeve: string;
  if (coreHeld !== null && coreHeld.length > 0) {
    sleeve = `Your money is in the index sleeve (${uniqueSymbols(coreHeld)}).`;
    if (core !== null && core.mandate.enabled !== true) {
      sleeve += " The sleeve is switched off, so it will not be topped up or rebalanced.";
    }
  } else if (coreHeld === null && failed.positions) {
    sleeve = "What the index sleeve holds could not be loaded.";
  } else if (core === null && failed.core) {
    sleeve = "The index sleeve's status could not be loaded.";
  } else if (core === null || coreHeld === null) {
    sleeve = "Checking what the index sleeve holds…";
  } else {
    sleeve = coreSleeveSentence(core);
  }

  const next: string[] = [];
  if (core !== null) {
    if (core.state === "ready" && core.mandate.enabled === true) {
      // ⚠ Never promise a trade the executor can refuse (Codex ckpt-2): the kill
      // switch and every execution block stop the sleeve too, and a gap below the
      // mandate's minimum trade is held rather than traded.
      next.push(
        overview.entry_block.new_entries_blocked
          ? "The index sleeve buys nothing while trading is blocked (see above)."
          : "The index sleeve is checked once a day while the US market is open. It trades only if it has drifted outside its band by at least the minimum trade, and every safety check passes.",
      );
    } else if (core.state === "evidence_collecting") {
      // The window closing decides nothing by itself: the result is reviewed
      // first and may be cash (Codex ckpt-2).
      next.push(
        `The index sleeve's evidence can first be judged after ${formatDate(core.earliest_possible_verdict_at)}. The result may be an instrument or cash.`,
      );
    } else if (core.state === "ready") {
      next.push("The index sleeve buys nothing until it is switched on.");
    }
  }
  const registered = overview.strategies.length;
  if (!readiness.ready) {
    next.push(
      registered === 0
        ? "No strategy is under test."
        : `${registered} ${registered === 1 ? "strategy is" : "strategies are"} under test. None is given new money until it passes its evidence bar.`,
    );
  }

  return { whereMoney: `${lead}${readiness.ready ? "" : strategyClause} ${sleeve}`, next };
}
