/**
 * Statement chrome (#1592 child 2, spec §4): masthead, footer,
 * numbered Notes & disclosures with footnote markers, nil lines and
 * the two-tier caveat treatment (muted slate for scope/methodology;
 * amber strictly for genuine degraded states, spec §6.7).
 */
import type { ReactNode } from "react";

import { formatDate, formatDateTime } from "@/lib/format";
import { formatPeriodRange } from "@/components/reports/snapshotMath";

export function Masthead({
  currency,
  periodStart,
  periodEnd,
}: {
  currency: string;
  periodStart: string;
  periodEnd: string;
}) {
  return (
    <p className="text-xs text-slate-500">
      eToro demo account · Reporting currency {currency} · Statement period{" "}
      {formatPeriodRange(periodStart, periodEnd)} · Data as at {formatDate(periodEnd)} close
    </p>
  );
}

export function StatementFooter({
  generatedAt,
  benchmarkLabel,
}: {
  generatedAt: string;
  benchmarkLabel: string;
}) {
  return (
    <p className="border-t border-slate-200 pt-2 text-xs text-slate-500 dark:border-slate-800">
      Generated {formatDateTime(generatedAt)} · Prices: period-end close · FX: generation date ·
      Benchmark: {benchmarkLabel}
    </p>
  );
}

/** Superscript footnote marker resolving to the Notes section. */
export function Fn({ n }: { n: number }) {
  return (
    <sup className="ml-0.5 select-none text-[10px] text-slate-400" aria-label={`note ${n}`}>
      {n}
    </sup>
  );
}

/** Statement-convention nil line — used when a section's data is
 *  PRESENT but empty. EmptyState stays reserved for missing keys. */
export function NilLine({ children }: { children: ReactNode }) {
  return <p className="py-2 text-sm text-slate-500">{children}</p>;
}

/** Tier-1 caveat: scope/methodology — muted slate, never amber. */
export function ScopeCaveat({ children }: { children: ReactNode }) {
  return <p className="mt-1 text-xs text-slate-500">{children}</p>;
}

/** Tier-2: genuine degraded state (fx_unavailable) — amber badge. */
export function DegradedBadge({ children }: { children: ReactNode }) {
  return (
    <span className="inline-block rounded bg-amber-50 px-1.5 py-0.5 text-[10px] font-medium text-amber-700 dark:bg-amber-950/40 dark:text-amber-400">
      {children}
    </span>
  );
}

export interface NoteIndex {
  dietz: number;
  benchmark: number;
  fx: number;
  scope1593: number;
  income?: number;
  smallN?: number;
}

export interface StatementNotes {
  items: string[];
  marker: NoteIndex;
}

/** Numbered notes per cadence (spec §4.11). Weekly omits the
 *  monthly-only income + small-n notes; numbering stays dense. */
export function notesFor(reportType: "weekly" | "monthly"): StatementNotes {
  const items = [
    "Return methodology: Modified Dietz — flow-adjusted; external capital flows are weighted by the fraction of the period remaining, so deposits and withdrawals never print as performance.",
    "Benchmark basis: S&P 500 price index — excludes dividends; understates the benchmark's total return by roughly 1.3–2% per year, so comparisons are flattered by about that much.",
    "FX basis: each snapshot's values are stamped at its generation-date rates; points on the trailing line therefore mix each report's generation-date rates.",
  ];
  const marker: NoteIndex = { dietz: 1, benchmark: 2, fx: 3, scope1593: 0 };
  if (reportType === "monthly") {
    items.push(
      "Income is estimated from declared dividends (declared DPS × period-end units) — not confirmed received; a position opened after the ex-date shows phantom income until the trade ledger (#1593).",
      "Small-sample statistics: risk metrics are gated below the minimum observation count and the win-rate percentage is suppressed below 5 closed trades.",
    );
    marker.income = 4;
    marker.smallN = 5;
  }
  items.push(
    "Scope: period activity and charges cover own-platform orders only — broker-side trade history and fees land with the trade ledger (#1593).",
  );
  marker.scope1593 = items.length;
  return { items, marker };
}

export function NotesSection({ notes }: { notes: StatementNotes }) {
  return (
    <ol className="list-none space-y-1.5 text-xs text-slate-500">
      {notes.items.map((text, i) => (
        <li key={i} className="flex gap-2">
          <span className="font-semibold">{i + 1}.</span>
          <span>{text}</span>
        </li>
      ))}
    </ol>
  );
}
