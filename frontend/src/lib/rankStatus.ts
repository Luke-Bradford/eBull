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
      return filingsStatusText(score.filings_status);
    case "not_in_latest_run":
      return `absent from the latest run (last scored ${score.scored_at.slice(0, 10)})`;
    case "no_rank":
      return "no rank assigned in the latest run";
    case null:
      return "";
  }
}

/**
 * `coverage.filings_status` values (`app/services/coverage.py::_classify`) other
 * than `analysable`. "insufficient" means our ingested 10-K/10-Q history is below
 * the ranking bar, not that the company files nothing.
 */
function filingsStatusText(status: string | null): string {
  switch (status) {
    case "insufficient":
      return "SEC 10-K/10-Q history too thin to rank";
    case "fpi":
      return "foreign private issuer (files 20-F/40-F, not 10-K/10-Q)";
    case "no_primary_sec_cik":
      return "no SEC registrant linked";
    case null:
      return "no filings coverage recorded";
    default:
      return `filings coverage: ${status}`;
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
