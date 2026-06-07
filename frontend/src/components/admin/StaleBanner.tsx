/**
 * StaleBanner — page-level clean-bill-of-health header above the
 * ProcessesTable.
 *
 * Originally (PR8 / #1083) driven by `stale_reasons`. Rewired in #1512
 * to the single computed `health_verdict` so the banner language matches
 * the row pills. #1513 turns it into the always-present header: a positive
 * "All systems current" all-clear when every row is `current` / `working`,
 * and the "N need attention · M self-healing" summary otherwise — a
 * deliberate, scoped reversal of admin-triage's "no 'No problems!' banner"
 * rule, limited to this header (never a toast).
 *
 * `checkedAt` is the client-side completion time of the last successful
 * poll (from `useProcesses`), rendered as the "checked HH:MM" freshness
 * anchor so the operator can trust the all-clear is live.
 */

import { Link } from "react-router-dom";

import type { ProcessRowResponse } from "@/api/types";
import { formatTime } from "@/lib/format";

const MAX_NAMED_PROCESSES = 3;

export interface StaleBannerProps {
  readonly rows: readonly ProcessRowResponse[];
  readonly checkedAt?: Date | null;
}

export function StaleBanner({ rows, checkedAt = null }: StaleBannerProps) {
  const attention = rows.filter((r) => r.health_verdict === "attention");
  const selfHealing = rows.filter((r) => r.health_verdict === "self_healing");
  const checkedSuffix =
    checkedAt !== null ? ` · checked ${formatTime(checkedAt)}` : "";

  // Healthy: nothing needs attention and nothing is mid-recovery. The
  // positive all-clear (#1513) — emerald, the OK colour (operator-ui
  // conventions). Rendered (not null) so the operator gets an explicit
  // "trust the page" signal without inspecting rows.
  if (attention.length === 0 && selfHealing.length === 0) {
    return (
      <div
        role="status"
        aria-live="polite"
        data-testid="health-header"
        className="rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs text-emerald-800 dark:border-emerald-900 dark:bg-emerald-950/40 dark:text-emerald-200"
      >
        <span aria-hidden="true">✓ </span>
        All systems current{checkedSuffix}.
      </div>
    );
  }

  // Link target: the first attention row if any, else the first
  // self-healing row (so "View" always points somewhere useful).
  const target = attention[0] ?? selfHealing[0];
  return (
    <div
      role="status"
      aria-live="polite"
      data-testid="health-header"
      className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-200"
    >
      <span aria-hidden="true">⚠ </span>
      {formatSummary(attention, selfHealing)}
      {checkedSuffix}{" "}
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
  return parts.join(" · ");
}
