/**
 * Multi-class denominator caveat callout (#1646).
 *
 * Shown when an instrument is one share class of a multi-class issuer whose
 * classes share one SEC CIK (GOOG/GOOGL, HEI/HEI.A), so every percentage in the
 * ownership rollup divides by the combined all-class share count (the per-class
 * count is not yet ingested — rides on #1590). The ``note`` is server-owned copy
 * (``rollup.dual_class_denominator.note``); render it verbatim so the caveat stays
 * a SINGLE source of truth across both the L1 ``OwnershipPanel`` and the L2
 * ``OwnershipPage`` (prevention-log "Multi-surface states share one copy source").
 */

import type { JSX } from "react";

export function DualClassDenominatorCallout({
  note,
}: {
  readonly note: string;
}): JSX.Element {
  return (
    <div
      className="rounded-md border border-sky-200 bg-sky-50 px-3 py-2 text-xs text-sky-900 dark:border-sky-900/60 dark:bg-sky-900/20 dark:text-sky-200"
      role="status"
      data-test="dual-class-denominator-callout"
    >
      {note}
    </div>
  );
}
