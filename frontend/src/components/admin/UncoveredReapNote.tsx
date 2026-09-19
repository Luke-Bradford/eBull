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

/**
 * Render cap, per the array-size rule in
 * `.claude/skills/frontend/api-shape-and-types.md` (#2178: a `.map()` over an
 * uncapped API array froze the tab with 29,281 rows).
 *
 * The backend applies NO limit — it returns every off-table job at or above
 * the chip floor — so this is the only bound, which is exactly the case the
 * rule is written for. It sits far above any plausible residual (the whole
 * `job_runs` corpus carried 78 distinct job names over 30 days, and only those
 * clearing the floor in 7 can qualify), so a later server-side limit cannot be
 * silently truncated here. `job_name` is unconstrained `TEXT`, so a malformed
 * or legacy producer is the realistic way this list gets long — not normal
 * operation.
 *
 * The pre-cap total needs no field of its own: the backend sends the whole
 * array, so `entries.length` IS the total.
 */
const MAX_RENDERED_JOBS = 25;

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

  const shown = entries.slice(0, MAX_RENDERED_JOBS);
  const hidden = entries.length - shown.length;

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
      {shown.map((entry, index) => (
        <span key={entry.job_name}>
          {index > 0 ? ", " : ""}
          <code className="font-mono">{entry.job_name}</code> ({entry.events}{" "}
          {entry.events === 1 ? "reap" : "reaps"}, {entry.runs}{" "}
          {entry.runs === 1 ? "run" : "runs"})
        </span>
      ))}
      {hidden > 0 ? (
        <>
          {" "}
          ({shown.length} of {entries.length} shown, loudest first)
        </>
      ) : null}
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
