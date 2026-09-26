/**
 * RankSummary — the SummaryStrip's rank line (#3389 (b)).
 *
 * - Rank and its same-version Δ only when the verdict row is `ranked` (the
 *   `GET /rankings` gate). A stale stored rank never shows as current.
 * - Otherwise "Not ranked" with the reason, in neutral slate: absence of a
 *   rank is a coverage fact, not a bad verdict.
 * - The score's actual composition: the largest weight × score contributions
 *   and the largest fired deductions, from the structured payload.
 */
import type { JSX } from "react";

import type { VerdictScore } from "@/api/types";
import { HeuristicBadge } from "@/components/rankings/HeuristicBadge";
import { RankDeltaCell } from "@/components/rankings/RankDeltaCell";
import { largestContributions, largestDeductions, notRankedReasonText } from "@/lib/rankStatus";
import { humanizePenaltyName } from "@/lib/scoreExplanation";

export interface RankSummaryProps {
  /** `undefined` while loading; `null` when never scored. */
  readonly score: VerdictScore | null | undefined;
  readonly errored: boolean;
}

function titleCase(family: string): string {
  return family.charAt(0).toUpperCase() + family.slice(1);
}

export function RankSummary({ score, errored }: RankSummaryProps): JSX.Element | null {
  if (errored) {
    return (
      <span data-testid="rank-summary" className="text-xs text-slate-500">
        Rank unavailable
      </span>
    );
  }
  if (score === undefined) return null;
  if (score === null) {
    return (
      <span data-testid="rank-summary" className="text-xs text-slate-500">
        Not scored
      </span>
    );
  }

  const contributions = largestContributions(score);
  const deductions = largestDeductions(score);
  return (
    <span data-testid="rank-summary" className="flex flex-wrap items-center gap-x-2 gap-y-1 text-xs">
      {score.ranked && score.rank !== null ? (
        <span className="font-medium text-slate-700 dark:text-slate-200">
          Rank #{score.rank} <RankDeltaCell delta={score.rank_delta} />
        </span>
      ) : (
        <span className="text-slate-500" title={notRankedReasonText(score)}>
          Not ranked · {notRankedReasonText(score)}
        </span>
      )}
      <HeuristicBadge modelVersion={score.model_version} compact />
      {contributions.length > 0 && (
        <span className="text-slate-500" title="Largest weight × score contributions to the pre-penalty total">
          largest:{" "}
          {contributions
            .map((c) => `${titleCase(c.family)} +${c.contribution.toFixed(2)}`)
            .join(" · ")}
        </span>
      )}
      {deductions.map((d) => (
        <span key={d.name} className="text-red-600 dark:text-red-400" title={d.reason}>
          {humanizePenaltyName(d.name)} −{(d.deduction ?? 0).toFixed(2)}
        </span>
      ))}
    </span>
  );
}
