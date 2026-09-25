/**
 * HeuristicBadge — the honesty label every rank / score surface carries (#3389 (a)).
 *
 * The ranking is a deterministic scoring heuristic: no backtest or out-of-sample
 * validation has been passed by it (see
 * `docs/proposals/ta/2026-09-25-evidence-ranking-and-instrument-report.md`,
 * "Honesty first"). A rank shown without that caveat reads as evidence.
 *
 * The model version comes from the row being shown, never from a literal here,
 * so the label cannot outlive the model it describes. Where the surface has no
 * version to hand, the label says "ranking" rather than guessing one.
 */
import type { JSX } from "react";

import { Badge } from "@/components/ui/Badge";

export const HEURISTIC_TOOLTIP =
  "Deterministic scoring heuristic. It has not passed a backtest or out-of-sample validation, so a rank or score is not evidence of future returns.";

export function HeuristicBadge({
  modelVersion,
  compact = false,
}: {
  readonly modelVersion?: string | null;
  /** Short form ("heuristic") for dense rows; the tooltip still carries the full caveat. */
  readonly compact?: boolean;
}): JSX.Element {
  const label = compact
    ? "heuristic"
    : `${modelVersion ?? "ranking"} heuristic · not validated`;
  return (
    <Badge tone="warn" title={HEURISTIC_TOOLTIP} data-testid="heuristic-badge">
      {label}
    </Badge>
  );
}
