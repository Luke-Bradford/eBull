import type { ReactNode } from "react";

/**
 * Section card container used by every dashboard panel.
 *
 * Each section owns its own loading / error / empty / data state — a
 * failing /system/status must not blank /portfolio. Sections render an
 * inline ErrorBanner with a Retry button rather than throwing, so the
 * top-level ErrorBoundary is reserved for unexpected exceptions.
 *
 * ``scrollable=true`` (#194) switches the Section into a contained-
 * scroll layout: the section claims remaining flex space and its
 * body scrolls vertically, instead of growing the page-level scroll.
 * Use when the parent is a flex column with ``h-full`` and the
 * section sits below header/filter chrome that should stay visible.
 */
export function Section({
  title,
  action,
  children,
  scrollable = false,
}: {
  title: string;
  action?: ReactNode;
  children: ReactNode;
  scrollable?: boolean;
}) {
  const sectionClass = scrollable
    ? "flex min-h-0 flex-1 flex-col overflow-hidden rounded-md border border-slate-200 bg-white shadow-sm"
    : "rounded-md border border-slate-200 bg-white shadow-sm";
  const bodyClass = scrollable ? "min-h-0 flex-1 overflow-auto p-4" : "p-4";
  return (
    <section className={sectionClass}>
      <header className="flex flex-shrink-0 items-center justify-between border-b border-slate-100 px-4 py-3">
        <h2 className="text-sm font-semibold text-slate-700">{title}</h2>
        {action ? <div className="text-xs">{action}</div> : null}
      </header>
      <div className={bodyClass}>{children}</div>
    </section>
  );
}

export function SectionError({ onRetry }: { onRetry: () => void }) {
  return (
    <div
      role="alert"
      className="flex items-center justify-between rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
    >
      <span>Failed to load. Check the browser console for details.</span>
      <button
        type="button"
        onClick={onRetry}
        className="rounded border border-red-300 bg-white px-2 py-1 text-xs font-medium text-red-700 hover:bg-red-100"
      >
        Retry
      </button>
    </div>
  );
}

export function SectionSkeleton({ rows = 3 }: { rows?: number }) {
  return (
    <div role="status" aria-live="polite" className="animate-pulse space-y-2">
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="h-4 rounded bg-slate-100" />
      ))}
    </div>
  );
}
