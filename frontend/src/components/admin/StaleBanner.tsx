/**
 * StaleBanner — at-a-glance stale-process summary above the
 * ProcessesTable (PR8 / #1083, umbrella #1064).
 *
 * Spec: `docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md`
 *       §"Stale-detection rule" line 602-604 — banner format pattern;
 *       §A1 four-case stale model (line 11-22) — reason vocabulary.
 *
 * The banner is rendered only when at least one row in the snapshot
 * carries non-empty `stale_reasons`. If every row is fresh, the
 * component returns `null` — no layout shift, no empty banner. When
 * all stale rows share one reason, the summary names it; otherwise
 * the copy says "multiple causes".
 */

import { Link } from "react-router-dom";

import type { ProcessRowResponse, StaleReason } from "@/api/types";
import { STALE_REASON_LABEL } from "@/components/admin/processStatus";

const MAX_NAMED_PROCESSES = 3;

export interface StaleBannerProps {
  readonly rows: readonly ProcessRowResponse[];
}

export function StaleBanner({ rows }: StaleBannerProps) {
  const stale = rows.filter((r) => r.stale_reasons.length > 0);
  const first = stale[0];
  if (first === undefined) return null;
  return (
    <div
      role="status"
      aria-live="polite"
      data-testid="stale-banner"
      className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-200"
    >
      <span aria-hidden="true">⚠ </span>
      {formatSummary(stale)}{" "}
      <Link
        to={`/admin/processes/${encodeURIComponent(first.process_id)}`}
        className="ml-1 font-medium underline hover:no-underline"
      >
        View
      </Link>
    </div>
  );
}

function formatSummary(stale: readonly ProcessRowResponse[]): string {
  const named = stale.slice(0, MAX_NAMED_PROCESSES).map((r) => r.process_id);
  const remainder = stale.length - named.length;
  const suffix = remainder > 0 ? `, +${remainder} more` : "";
  const cause = describeCause(stale);
  return `${stale.length} stale (${cause}): ${named.join(", ")}${suffix}.`;
}

function describeCause(stale: readonly ProcessRowResponse[]): string {
  const reasons = new Set<StaleReason>();
  for (const r of stale) {
    for (const reason of r.stale_reasons) reasons.add(reason);
  }
  if (reasons.size === 1) {
    const only: StaleReason | undefined = reasons.values().next().value;
    if (only !== undefined) return STALE_REASON_LABEL[only];
  }
  return "multiple causes";
}
