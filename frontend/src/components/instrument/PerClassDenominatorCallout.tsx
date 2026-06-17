/**
 * Per-class denominator info callout (#788).
 *
 * Shown when a verified FSDS per-class share count replaced the issuer's combined
 * all-class count (GOOG/GOOGL, HEI/HEI.A, METC/METCB), so every percentage in the
 * ownership rollup is per-class-TRUE — this SUPERSEDES the #1646 combined-basis
 * caveat (the two are mutually exclusive). The ``note`` is server-owned copy
 * (``rollup.per_class_denominator.note``); render it verbatim so the wording stays
 * a SINGLE source of truth across both the L1 ``OwnershipPanel`` and the L2
 * ``OwnershipPage`` (prevention-log "Multi-surface states share one copy source").
 *
 * Emerald (positive/verified) variant to distinguish from the sky-blue #1646
 * caveat — this is good news (a real per-class figure), not a degradation.
 */

import type { JSX } from "react";

export function PerClassDenominatorCallout({
  note,
}: {
  readonly note: string;
}): JSX.Element {
  return (
    <div
      className="rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs text-emerald-900 dark:border-emerald-900/60 dark:bg-emerald-900/20 dark:text-emerald-200"
      role="status"
      data-test="per-class-denominator-callout"
    >
      {note}
    </div>
  );
}
