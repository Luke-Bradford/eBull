/**
 * Shared process-health selectors (#1959).
 *
 * The /admin page surfaces "processes that need attention" in two
 * places — the top ProblemsPanel banner and the Processes control-hub
 * (`ProcessesTable` → `StaleBanner`). #1959: they drifted because each
 * derived the set independently off a different payload (the top banner
 * read the legacy `/system/jobs` list, which omits `ingest_sweep`
 * processes like `nport_sweep`). These selectors are the single source
 * of truth for the scope + attention predicate so the two surfaces
 * cannot disagree again.
 */
import type { ProcessRowResponse } from "@/api/types";

/**
 * Steady-state scope — the rows the control-hub "N need attention" count
 * operates on. Mirrors `ProcessesTable`'s `!isBootstrapOrBackfill`
 * (#1530 C7): bootstrap / backfill one-shots legitimately sit idle or
 * failed between runs and fold into a separate collapsed section rather
 * than the steady-state attention count.
 */
export function isSteadyStateProcess(row: ProcessRowResponse): boolean {
  return row.role === "steady_state" && row.mechanism !== "bootstrap";
}

/**
 * Processes that genuinely need operator attention: steady-state rows
 * whose computed `health_verdict` is `attention`. `attention` already
 * excludes `self_healing` / `stale_manual` / `paused` / `working` /
 * `current` (#1689 / #1831), so a transient or kill-switch-disabled row
 * is not counted.
 */
export function steadyStateAttentionRows(
  rows: readonly ProcessRowResponse[],
): ProcessRowResponse[] {
  return rows.filter(
    (row) => isSteadyStateProcess(row) && row.health_verdict === "attention",
  );
}
