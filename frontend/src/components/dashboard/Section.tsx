import type { ReactNode } from "react";

/**
 * Section — canonical card primitive used by every dashboard panel and
 * many list/detail pages.
 *
 * Each section owns its own loading / error / empty / data state — a
 * failing /system/status must not blank /portfolio. Sections render an
 * inline ErrorBanner with a Retry button rather than throwing, so the
 * top-level ErrorBoundary is reserved for unexpected exceptions.
 *
 * Design-system v1 chrome (issue #691): hairline top-rule + small-caps
 * uppercase title. Replaces the prior rounded card + border + shadow
 * pattern so the page reads as a continuous editorial spread instead
 * of a Trello-board of tiles. Matches the Pane component on the
 * instrument page.
 *
 * `scrollable=true` (#194) switches the Section into a contained-scroll
 * layout: the section claims remaining flex space and its body scrolls
 * vertically. Use when the parent is a flex column with `h-full` and
 * the section sits below header/filter chrome that should stay visible.
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
  // Hairline chrome — no background, no border, no shadow. The top-rule
  // + small-caps title pair carries the section break visually.
  const sectionClass = scrollable
    ? "flex min-h-0 flex-1 flex-col overflow-hidden border-t border-slate-200 pt-3"
    : "border-t border-slate-200 pt-3";
  const bodyClass = scrollable ? "min-h-0 flex-1 overflow-auto pt-3" : "pt-3";
  return (
    <section className={sectionClass}>
      <header className="flex flex-shrink-0 items-baseline justify-between gap-2">
        <h2 className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate-700">
          {title}
        </h2>
        {action ? (
          <div className="text-[11px] text-slate-600">{action}</div>
        ) : null}
      </header>
      <div className={bodyClass}>{children}</div>
    </section>
  );
}

export function SectionError({ onRetry }: { onRetry: () => void }) {
  return (
    <div
      role="alert"
      className="flex items-center justify-between rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
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
