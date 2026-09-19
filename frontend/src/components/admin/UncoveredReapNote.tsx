/**
 * #2274 — the Processes table's own coverage disclosure.
 *
 * The per-row reap chip (`ProcessRow`) answers "is this job repeatedly losing
 * runs to the orphan reaper?" only for jobs that HAVE a row. When this shipped
 * the table was chipping one job and silently hiding two that cleared the same
 * bar, one of them the loudest in the corpus. The census is deliberately NOT
 * written down here — it describes a moving seven-day window and would go
 * stale in the place a reader trusts most. It is measured, with the query, in
 * `docs/proposals/ops/2026-09-19-2274-uncovered-reap-disclosure.md`.
 *
 * ⚠ Tone is INFORMATIONAL, never alarm. #2274's binding constraint is "must
 * not train the operator to ignore it", and a reap is largely a deploy
 * artefact: #1831's measured bug was ~42 halted jobs painted red, burying the
 * real failures. Muted slate, same register as the per-row chip.
 *
 * ⚠ The copy says "not in this table" and NOTHING else. Both directions are
 * overclaims and the first two drafts shipped one each. "Nowhere else" is
 * false for the sync-orchestrator layer jobs, which do have a `/sync/layers/v2`
 * and DAG surface. "Some of these have other surfaces" is false whenever the
 * residual happens to hold only an outside-DAG job (`strategy_backtest_run`)
 * or one recently renamed — the backend filter proves only that the job has no
 * process row, so any sentence about other surfaces is an inference the data
 * does not support.
 *
 * ⚠ And it does not claim lost WORK. A reaped row may have committed its
 * writes before the worker died, and the reaper's auto-retry may have re-run
 * it; the reaper detects an orphaned row, it does not record a cause. "Written
 * off" is what was observed.
 */
import type { UncoveredReapResponse } from "@/api/types";

/**
 * ⚠ MIRRORS `scheduled_adapter.RECENT_REAP_WINDOW_DAYS` (and, through the
 * backend filter, `RECENT_REAP_CHIP_FLOOR`). Mirroring by name rather than
 * putting the duration on the wire is the existing decision for this pair —
 * see `ProcessRow.tsx`'s copy of the same two constants. Grep
 * `RECENT_REAP_CHIP_FLOOR` before changing either.
 */
const RECENT_REAP_WINDOW_DAYS = 7;

interface UncoveredReapNoteProps {
  /**
   * ⚠ `null`/absent means "not evaluated" — the backend does not compute the
   * residual when an adapter raised (a short covered set would make
   * well-covered jobs look uncovered) or when the read itself failed. `[]`
   * means "measured, none". Both render nothing; neither is an all-clear the
   * operator should be shown.
   *
   * No separate `partial` prop: nullability already carries this, and a second
   * condition could only disagree with the first.
   */
  readonly entries: readonly UncoveredReapResponse[] | null | undefined;
}

export function UncoveredReapNote({ entries }: UncoveredReapNoteProps) {
  if (!entries || entries.length === 0) return null;

  return (
    <p
      className="mb-3 border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600 dark:border-slate-800 dark:bg-slate-900/40 dark:text-slate-400"
      role="note"
      data-testid="uncovered-reap-note"
    >
      <strong className="font-medium text-slate-700 dark:text-slate-300">
        Not in this table:
      </strong>{" "}
      {entries.length} {entries.length === 1 ? "job has" : "jobs have"} no process row
      here and had runs written off by the orphan reaper in the last{" "}
      {RECENT_REAP_WINDOW_DAYS} days —{" "}
      {entries.map((entry, index) => (
        <span key={entry.job_name}>
          {index > 0 ? ", " : ""}
          <code className="font-mono">{entry.job_name}</code> ({entry.events}{" "}
          {entry.events === 1 ? "reap" : "reaps"}, {entry.runs}{" "}
          {entry.runs === 1 ? "run" : "runs"})
        </span>
      ))}
      .
    </p>
  );
}

/**
 * Summary-line fragment for the collapsed section header.
 *
 * ⚠ `CollapsibleSection` UNMOUNTS its body when closed, so a note inside the
 * section is only reachable while it happens to be open. That is exactly the
 * defect PR #3211 fixed for the collapsed `current` group — a correct field on
 * a collapsed row is invisible — so the count rides the disclosure label too.
 */
export function uncoveredReapSummary(
  entries: readonly UncoveredReapResponse[] | null | undefined,
): string | null {
  if (!entries || entries.length === 0) return null;
  return `${entries.length} ${entries.length === 1 ? "job" : "jobs"} not listed`;
}
