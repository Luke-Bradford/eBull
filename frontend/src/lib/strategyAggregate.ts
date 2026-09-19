import type { StrategyOverviewResponse, StrategyOwnedPosition } from "@/api/types";
import { number } from "@/lib/strategyFormat";

/**
 * Open positions that NO strategy's `pnl` block covers — today, the core sleeve.
 *
 * `aggregate().totalPnl` sums `pnl.total_pnl` over `overview.strategies`, so its
 * scope is exactly "the registered strategies". The core sleeve is a separate
 * capital authority on a separate fetch and appears in no strategy's block, which
 * is why #3222's `Open` tile had to be re-pointed at `GET /strategies/positions`.
 * The P&L tile beside it was left alone in that pass and inherited the same gap.
 *
 * ⚠⚠ Measured 2026-09-19, and it is not a rounding-scale omission: all 11
 * strategies report `total_pnl: "0"` with `owned_position_count: 0`, so the tile
 * renders a confident **$0.00** while the pot holds a position whose unrealised
 * P&L is non-zero. A wrong number, not a missing one.
 *
 * ⚠ This does NOT fold the position into the total, deliberately. Its money
 * cells are display-converted to the operator's currency (`currency: "GBP"`,
 * #2129) while the pool's authority currency is USD, and converting between them
 * for capital accounting is `fx_unmodelled` — a standing refusal (#2363, recorded
 * at `app/services/strategy_base_currency.py`). The caller names the exclusion
 * instead; see `StrategyPortfolioLens`.
 *
 * `strategy_id === null` IS the core marker: `get_strategy_owned_positions` sets
 * it from `row["strategy_id"]`, which is NULL for a core-mandate trade, and
 * stamps `strategy_title: "Core / cash mandate"` off `core_rebalance_intent_id`.
 * Kept as the general "outside the strategy roll-up" test rather than a title
 * match, so a second non-strategy authority is caught by construction.
 *
 * ⚠ INVALIDATING CONDITION: if a strategy's `pnl` block ever starts covering the
 * core sleeve, or a pool-currency P&L becomes available for it, this exclusion
 * stops being correct and the tile should fold it in rather than name it.
 */
export function positionsOutsideStrategyPnl(
  positions: readonly StrategyOwnedPosition[],
): StrategyOwnedPosition[] {
  return positions.filter((position) => position.strategy_id === null);
}

/** How many distinct entries `namedList` will name before counting the rest. */
export const NAMED_LIST_LIMIT = 3;

/**
 * Distinct `values`, in first-seen order, bounded — the overflow COUNTED, never
 * dropped (review NITPICK on PR #3226: an unbounded comma list can run a caveat
 * sentence off the page).
 *
 * ⚠ The overflow is stated rather than truncated silently. A list that stops at
 * three and says nothing reads as "these are all of them", which is the same
 * class of defect as the total this caveat exists to qualify — a figure that
 * looks complete while covering a subset. `docs/review-prevention-log.md`'s
 * no-silent-caps rule is the general form.
 *
 * Returns `[]` for no values, so the caller renders nothing rather than "0 of".
 */
export function namedList(values: readonly string[]): string[] {
  const distinct = [...new Set(values)];
  if (distinct.length <= NAMED_LIST_LIMIT) return distinct;
  const named = distinct.slice(0, NAMED_LIST_LIMIT);
  return [...named, `+${distinct.length - named.length} more`];
}

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
 *   ⚠ That rule governs the STRATEGIES it sums; it cannot speak for capital
 *   authorities outside `overview.strategies`. `totalPnl` is therefore a
 *   strategy-scoped figure and callers must label it as one — see
 *   `positionsOutsideStrategyPnl`, which names what it leaves out.
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
