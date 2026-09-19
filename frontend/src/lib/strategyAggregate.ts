import type { StrategyOverviewResponse } from "@/api/types";
import { number } from "@/lib/strategyFormat";

/**
 * Cross-strategy roll-up shared by both `/strategies` lenses (#2868).
 *
 * Pure reduction over the overview payload — no rendering — so it lives in
 * `lib/` rather than beside the panels that display it: the components/logic
 * boundary the #2868 split exists to draw, and it makes the arithmetic
 * testable without mounting React.
 *
 * Two deliberate honesty rules, both load-bearing:
 *
 * - `totalPnl` is `null` unless EVERY strategy reports a parseable P&L. A
 *   partial sum rendered as a total is a wrong number, not a missing one.
 * - The attribution figures cover only `forward_outcome_supported` strategies,
 *   and `averageReturn` collapses to `null` the moment any contributing
 *   strategy has resolved entries but no average — a resolved-count-weighted
 *   mean over an unknown term is not a mean.
 */
export function aggregate(overview: StrategyOverviewResponse) {
  const pnlValues = overview.strategies.map((strategy) => number(strategy.pnl.total_pnl));
  const forwardStrategies = overview.strategies.filter((strategy) => strategy.forward_outcome_supported);
  const resolved = forwardStrategies.reduce(
    (sum, strategy) => sum + strategy.attribution.resolved_entries,
    0,
  );
  const winners = forwardStrategies.reduce(
    (sum, strategy) => sum + strategy.attribution.winning_entries,
    0,
  );
  let weightedReturn = 0;
  let averageReturnKnown = resolved > 0;
  for (const strategy of forwardStrategies) {
    if (strategy.attribution.resolved_entries === 0) continue;
    const average = number(strategy.attribution.shadow_average_return_pct);
    if (average === null) {
      averageReturnKnown = false;
      break;
    }
    weightedReturn += average * strategy.attribution.resolved_entries;
  }
  const fired = forwardStrategies.reduce(
    (sum, strategy) => sum + strategy.attribution.fired_entries,
    0,
  );
  return {
    totalPnl: pnlValues.every((value) => value !== null)
      ? pnlValues.reduce<number>((sum, value) => sum + (value ?? 0), 0)
      : null,
    resolved,
    winners,
    unsuccessful: Math.max(0, resolved - winners),
    awaitingOutcome: Math.max(0, fired - resolved),
    successRate: resolved > 0 ? winners / resolved : null,
    averageReturn: averageReturnKnown ? weightedReturn / resolved / 100 : null,
    // ⚠ #3222 removed `activePositions` (the sum of `pnl.active_position_count`
    // over `overview.strategies`). It had exactly one consumer — the portfolio
    // lens's `Open` tile — and it was the wrong operand there: the core sleeve's
    // position carries `strategy_id: null`, so it is in no strategy's count, and
    // the tile read 0 while the `Close all` button beside it read 1. Deleted
    // rather than left unused, because an available-but-wrong operand is how the
    // same tile gets re-wired to it. Open positions come from
    // `GET /strategies/positions`, which is the page's own list.
    approved: overview.strategies.filter((strategy) => strategy.allocation_ready).length,
  };
}
