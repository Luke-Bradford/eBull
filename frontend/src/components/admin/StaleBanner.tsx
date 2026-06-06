/**
 * StaleBanner — at-a-glance health summary above the ProcessesTable.
 *
 * Originally (PR8 / #1083) driven by `stale_reasons`. Rewired in #1512
 * to the single computed `health_verdict` so the banner language matches
 * the row pills (no "stale" vocabulary that no longer appears on rows).
 *
 * Renders only when at least one row needs attention or is self-healing.
 * When every row is `current` / `working` it returns `null` (no layout
 * shift). #1513 expands this into the positive "All systems current"
 * clean-bill header; #1512 only keeps it consistent with the
 * single-axis model.
 */

import { Link } from "react-router-dom";

import type { ProcessRowResponse } from "@/api/types";

const MAX_NAMED_PROCESSES = 3;

export interface StaleBannerProps {
  readonly rows: readonly ProcessRowResponse[];
}

export function StaleBanner({ rows }: StaleBannerProps) {
  const attention = rows.filter((r) => r.health_verdict === "attention");
  const selfHealing = rows.filter((r) => r.health_verdict === "self_healing");
  if (attention.length === 0 && selfHealing.length === 0) return null;

  // Link target: the first attention row if any, else the first
  // self-healing row (so "View" always points somewhere useful).
  const target = attention[0] ?? selfHealing[0];
  return (
    <div
      role="status"
      aria-live="polite"
      data-testid="stale-banner"
      className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-200"
    >
      <span aria-hidden="true">⚠ </span>
      {formatSummary(attention, selfHealing)}{" "}
      {target ? (
        <Link
          to={`/admin/processes/${encodeURIComponent(target.process_id)}`}
          className="ml-1 font-medium underline hover:no-underline"
        >
          View
        </Link>
      ) : null}
    </div>
  );
}

function formatSummary(
  attention: readonly ProcessRowResponse[],
  selfHealing: readonly ProcessRowResponse[],
): string {
  const parts: string[] = [];
  if (attention.length > 0) {
    const named = attention
      .slice(0, MAX_NAMED_PROCESSES)
      .map((r) => r.process_id);
    const remainder = attention.length - named.length;
    const suffix = remainder > 0 ? `, +${remainder} more` : "";
    parts.push(`${attention.length} need attention: ${named.join(", ")}${suffix}`);
  }
  if (selfHealing.length > 0) {
    parts.push(`${selfHealing.length} self-healing`);
  }
  return `${parts.join(" · ")}.`;
}
