/**
 * Bounded report cards for the instrument report's evidence sections (#3390):
 * a small-caps title, label/value rows and definition notes. Shared so every
 * report panel reads the same.
 */

import { type JSX, type ReactNode } from "react";

export function ReportCard({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}): JSX.Element {
  return (
    <div className="rounded border border-slate-200 p-3 dark:border-slate-800">
      <div className="mb-1 text-[11px] font-medium uppercase tracking-wide text-slate-500">
        {title}
      </div>
      {children}
    </div>
  );
}

export function ReportRow({ label, value }: { label: string; value: string }): JSX.Element {
  return (
    <div className="flex items-baseline justify-between gap-2 text-xs">
      <span className="text-slate-500">{label}</span>
      <span className="font-medium tabular-nums text-slate-700 dark:text-slate-300">
        {value}
      </span>
    </div>
  );
}

export function ReportNote({ children }: { children: ReactNode }): JSX.Element {
  return <p className="mt-1.5 text-[10px] text-slate-400">{children}</p>;
}
