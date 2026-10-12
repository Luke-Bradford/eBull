import type { StrategyUcitsComparator } from "@/api/types";
import { formatCloseDate, formatPct } from "@/lib/format";
import { number } from "@/lib/strategyFormat";

/**
 * #3616 slice 1 — the two return-strip lines for the investable UCITS
 * comparator and the active hurdle. Each degrades on its own: an unavailable
 * comparator shows "—" with its reason (never 0), and an unavailable hurdle
 * shows its reason while line 1 stays.
 * Spec: docs/specs/metrics/2026-10-12-3616-investable-comparator.md §UI
 */

const COMPARATOR_REASON: Record<NonNullable<StrategyUcitsComparator["unavailable_reason"]>, string> = {
  comparator_instrument_missing: "the comparator fund is not in our instrument list",
  comparator_instrument_mismatch: "the listing found is not the USD class on the LSE",
  pot_return_unavailable: "the pot has no time-weighted return to compare",
  comparator_close_missing: "no valid comparator close covers the window",
  comparator_stale: "the comparator's closes do not cover the window",
};

const HURDLE_REASON: Record<NonNullable<StrategyUcitsComparator["hurdle"]["unavailable_reason"]>, string> = {
  mandate_unconfigured: "No hurdle: the core mandate or the pot's risk profile is not configured",
  mandate_weights_invalid: "No hurdle: the mandate's weights do not fit inside the cash reserve",
  no_active_budget: "No hurdle: the mandate gives the active sleeves no budget",
  comparator_return_unavailable: "No hurdle: it needs the comparator's return",
  hurdle_infeasible: "Under these assumptions, even a total loss in the active sleeves would leave the pot ahead",
};

const PTS = new Intl.NumberFormat("en-GB", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
  signDisplay: "exceptZero",
});

/** A fraction difference in percentage points (−0.011726 → "-1.17"). */
export function formatPoints(fraction: number): string {
  return PTS.format(fraction * 100);
}

function wholePct(fraction: number): string {
  return `${Math.round(fraction * 1000) / 10}%`;
}

export interface ComparatorLines {
  line1: string;
  line1Title: string;
  line2: string;
  line2Title: string | null;
}

export function comparatorLines(comparator: StrategyUcitsComparator, returnSince: string | null): ComparatorLines {
  const name = "iShares Core S&P 500 (USD, Acc)";
  const ret = number(comparator.return);
  const excess = number(comparator.engine_excess);
  const start = formatCloseDate(comparator.window_start);
  const end = formatCloseDate(comparator.window_end);

  const qualification =
    "Whether this account can hold the fund directly, rather than as a CFD, and inside an ISA is not yet established.";
  let line1: string;
  const title: string[] = [];
  if (ret === null || comparator.unavailable_reason !== null) {
    line1 = `vs UCITS comparator ${name}: — (${COMPARATOR_REASON[comparator.unavailable_reason ?? "pot_return_unavailable"]})`;
  } else {
    line1 = `vs UCITS comparator ${name}, ${start} – ${end}: ${formatPct(ret)}${
      excess === null ? "" : ` · engine ${formatPoints(excess)} pts`
    }`;
    title.push(
      `Market-price return of ${comparator.symbol} in USD, close ${formatCloseDate(comparator.start_close_date)} to close ${formatCloseDate(comparator.end_close_date)}. An accumulating fund, so the price carries reinvested income net of fund costs; it is not the fund's NAV.`,
      "Closes are assumed to be the LSE session close, earlier in the day than the US close the pot is valued at.",
    );
    if (
      comparator.start_close_date !== comparator.window_start ||
      comparator.end_close_date !== comparator.window_end
    ) {
      title.push("Where the LSE had no session that day, the latest earlier close is used (up to 4 calendar days).");
    }
    if (returnSince !== null && returnSince !== comparator.window_start) {
      title.push(
        `The window starts ${start}, the close before first funding on ${formatCloseDate(returnSince)}: the pot's first return runs from that close with the funding at its start.`,
      );
    }
  }
  title.push(qualification);

  const hurdle = comparator.hurdle;
  const hurdleReturn = number(hurdle.hurdle_return);
  const required = number(hurdle.required_active_excess);
  const core = number(hurdle.core_weight);
  const target = number(hurdle.core_target_weight);
  const active = number(hurdle.active_weight);
  if (hurdle.unavailable_reason !== null || hurdleReturn === null || required === null) {
    return {
      line1,
      line1Title: title.join(" "),
      line2: HURDLE_REASON[hurdle.unavailable_reason ?? "comparator_return_unavailable"],
      line2Title: null,
    };
  }
  return {
    line1,
    line1Title: title.join(" "),
    line2: `Active sleeves would need ${formatPct(hurdleReturn)} (${formatPoints(required)} percentage points over the comparator) to match it`,
    line2Title:
      core === null || active === null || target === null
        ? null
        : `At today's mandate: ${wholePct(active)} active at its full cap, ${wholePct(core)} core (its ${wholePct(target)} target of the other ${wholePct(1 - active)}) assumed to track the comparator, ${wholePct(1 - core - active)} cash earning 0; held without rebalancing or leverage.`,
  };
}
