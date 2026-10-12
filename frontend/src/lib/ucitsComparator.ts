import type { StrategyUcitsComparator } from "@/api/types";
import { formatCloseDate, formatPct } from "@/lib/format";
import { number } from "@/lib/strategyFormat";

/**
 * #3616 slices 1-2 — the two return-strip lines for the investable UCITS
 * comparator and the active hurdle, with the GBP restatement folded into
 * line 1. Each degrades on its own: an unavailable comparator shows "—" with
 * its reason (never 0), an unavailable hurdle shows its reason while line 1
 * stays, and an unavailable GBP restatement drops only its clause.
 * Specs: docs/specs/metrics/2026-10-12-3616-investable-comparator.md §UI and
 * docs/specs/metrics/2026-10-12-3616-gbp-isa-comparator.md §UI
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

const GBP_REASON: Record<NonNullable<StrategyUcitsComparator["gbp"]["unavailable_reason"]>, string> = {
  comparator_unavailable: "it needs the comparator's return",
  fx_rate_missing: "no ECB USD/GBP rate covers the window",
  fx_rate_stale: "the ECB USD/GBP rates do not cover the window",
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
    "The demo account offers this fund as the fund itself, not a CFD (the real account is not yet observed). eToro's ISA is a separate Moneyfarm GBP account the engine cannot trade, and whether it offers this share class is unverified.";
  // ⚠ Guarded on the field: an API older than slice 2 carries no `gbp`.
  const gbp = comparator.gbp ?? null;
  let line1: string;
  const title: string[] = [];
  if (ret === null || comparator.unavailable_reason !== null) {
    line1 = `vs UCITS comparator ${name}: — (${COMPARATOR_REASON[comparator.unavailable_reason ?? "pot_return_unavailable"]})`;
  } else {
    const gbpReturn = gbp === null ? null : number(gbp.comparator_return);
    const gbpExcess = gbp === null ? null : number(gbp.engine_excess);
    const gbpClause =
      gbp !== null && gbp.unavailable_reason === null && gbpReturn !== null && gbpExcess !== null
        ? ` · in GBP ${formatPct(gbpReturn)} · engine ${formatPoints(gbpExcess)} pts`
        : "";
    line1 = `vs UCITS comparator ${name}, ${start} – ${end}: ${formatPct(ret)}${
      excess === null ? "" : ` · engine ${formatPoints(excess)} pts`
    }${gbpClause}`;
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
    if (gbp !== null && gbp.unavailable_reason === null) {
      title.push(
        `GBP at ECB reference rates of ${formatCloseDate(gbp.fx_start_date)} and ${formatCloseDate(gbp.fx_end_date)}. In GBP the gap is the USD gap scaled by the dollar's move; the active return needed, in USD, is unchanged.`,
        "Not after UK tax. The engine's account is outside an ISA; the same fund held in an ISA would pay no UK tax on gains. An after-tax comparison needs the engine's trades matched under HMRC rules, which is not built yet.",
      );
    } else if (gbp !== null && gbp.unavailable_reason !== null) {
      title.push(`No GBP figures: ${GBP_REASON[gbp.unavailable_reason]}.`);
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
    line2: `Active sleeves would need ${formatPct(hurdleReturn)} in USD (${formatPoints(required)} percentage points over the comparator) to match it`,
    line2Title:
      core === null || active === null || target === null
        ? null
        : `At today's mandate: ${wholePct(active)} active at its full cap, ${wholePct(core)} core (its ${wholePct(target)} target of the other ${wholePct(1 - active)}) assumed to track the comparator, ${wholePct(1 - core - active)} cash earning 0 in USD; held without rebalancing or leverage.`,
  };
}
