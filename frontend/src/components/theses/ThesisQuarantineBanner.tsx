/**
 * Thesis quarantine banner (#2306) — the operator-facing half of #2431/#2436.
 *
 * The deterministic layer already refuses a thesis whose memo never names its
 * own instrument: `portfolio.py`, `scoring.py`, `entry_timing.py`,
 * `alerts.py` and `reporting.py` all fail closed on
 * `theses.subject_identity_ok IS NOT TRUE`. Until this component existed the
 * SCREEN did not — `grep -rn subject_identity_ok frontend/src` returned
 * nothing — so the engine and the operator disagreed about the same row with
 * nothing said about it. On the dev corpus that was 78 of 482 latest-per-
 * instrument theses.
 *
 * ⚠ The row is NOT hidden. `docs/settled-decisions.md:147` forbids overwriting
 * prior thesis rows and `app/api/theses.py:202-206` keeps them visible
 * deliberately: they are the truthful record of what the writer produced and
 * the evidence base for the write-side fix. Hiding would also make the page
 * lie in the other direction — it would read as "no thesis exists".
 *
 * ⚠⚠ COPY DISCIPLINE. The banner claims exactly what the stored rule decided
 * ("never names its subject") and hedges the consequence ("may describe a
 * different company"). Do not tighten it to "is about a different company":
 * the verdict does not establish that, and #2306's history is a chain of
 * measurements being substituted for one another.
 */
import type { JSX } from "react";

import { thesisRefusalBody, thesisSubjectState } from "@/lib/thesisQuarantine";

export interface ThesisQuarantineBannerProps {
  /** The stored verdict, straight off the thesis row. */
  readonly subjectIdentityOk: boolean | null | undefined;
}

/**
 * Renders nothing when the verdict is `true`. Callers may therefore mount it
 * unconditionally beside the memo, which is what keeps the two inseparable —
 * see the invariant test in `ThesisPane.test.tsx`.
 *
 * ⚠ Deliberately takes NO symbol. Naming the instrument ("never names AAPL")
 * would read better, but `ThesisDetail` carries `instrument_id`, not `symbol`,
 * so it would have to be threaded through `VerdictTab` / `ResearchTab` /
 * `DensityGrid` — and only `DensityGrid` has it to hand. Threading it where it
 * is cheap and falling back where it is not gives the SAME page two different
 * warnings for the same defect, which is worse than one uniform sentence.
 */
export function ThesisQuarantineBanner({
  subjectIdentityOk,
}: ThesisQuarantineBannerProps): JSX.Element | null {
  const state = thesisSubjectState(subjectIdentityOk);
  if (state === "usable") return null;

  return (
    <div
      className="rounded-md border px-3 py-2 text-xs border-red-200 bg-red-50 text-red-900 dark:border-red-900/60 dark:bg-red-900/20 dark:text-red-200"
      role="status"
      data-testid="thesis-quarantine-banner"
      data-subject-state={state}
    >
      <p className="font-medium">
        {/* aria-hidden glyph — the headline is the entire accessible label. */}
        <span aria-hidden="true" className="mr-1.5 font-semibold">
          ⊘
        </span>
        Thesis quarantined — subject identity failed
      </p>
      <p className="mt-0.5">{thesisRefusalBody(subjectIdentityOk)}</p>
    </div>
  );
}
