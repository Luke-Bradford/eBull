/**
 * Current-rank semantics for a single instrument's verdict (#3389 (b)).
 *
 * A stored `rank` may belong to an older run, so it is only shown when the
 * backend says the row is `ranked` (same gate as `GET /rankings`). Otherwise
 * the surface says "not ranked" with the reason. Every reason is phrased as a
 * coverage fact, never as a judgement on the investment.
 */
import type { FamilyContribution, ScorePenaltyItem, VerdictScore } from "@/api/types";
import { isReward } from "@/lib/scoreExplanation";

export function notRankedReasonText(score: VerdictScore): string {
  switch (score.not_ranked_reason) {
    case "not_tradable":
      return "not tradable on eToro";
    case "not_analysable":
      return `no analysable SEC filings (${score.filings_status ?? "no coverage row"})`;
    case "not_in_latest_run":
      return `absent from the latest run (last scored ${score.scored_at.slice(0, 10)})`;
    case "no_rank":
      return "no rank assigned in the latest run";
    case null:
      return "";
  }
}

/** The largest `n` family contributions (already sorted by the backend). */
export function largestContributions(score: VerdictScore, n = 2): FamilyContribution[] {
  return score.contributions.slice(0, n);
}

/** Fired penalties with a deduction, largest first, at most `n`. */
export function largestDeductions(score: VerdictScore, n = 2): ScorePenaltyItem[] {
  return (score.penalties_json ?? [])
    .filter((p) => !isReward(p) && typeof p.deduction === "number" && p.deduction > 0)
    .sort((a, b) => (b.deduction ?? 0) - (a.deduction ?? 0))
    .slice(0, n);
}
