/**
 * Collapsible section wrapper for AdminPage (#323).
 *
 * Chevron-led header; click to toggle. Secondary stat line in the
 * header (e.g. "Orchestrator details — 1 problem") stays informative
 * when the section is collapsed.
 *
 * `defaultOpen` seeds the initial state; callers that need to drive
 * open/close imperatively (e.g. the ProblemsPanel "Open orchestrator
 * details" link) pass `open` + `onOpenChange` to use the controlled
 * form. Both mutually exclusive — controlled form ignores
 * `defaultOpen`.
 */
import { useState, type ReactNode } from "react";

export interface CollapsibleSectionProps {
  readonly title: string;
  readonly summary?: ReactNode;
  readonly children: ReactNode;
  readonly defaultOpen?: boolean;
  readonly open?: boolean;
  readonly onOpenChange?: (next: boolean) => void;
  readonly sectionId?: string;
}

export function CollapsibleSection({
  title,
  summary,
  children,
  defaultOpen = false,
  open,
  onOpenChange,
  sectionId,
}: CollapsibleSectionProps): JSX.Element {
  const [uncontrolled, setUncontrolled] = useState<boolean>(defaultOpen);
  const isControlled = open !== undefined;
  const isOpen = isControlled ? open : uncontrolled;

  function toggle() {
    const next = !isOpen;
    if (isControlled) {
      onOpenChange?.(next);
    } else {
      setUncontrolled(next);
      onOpenChange?.(next);
    }
  }

  return (
    <section className="border-t border-slate-200 pt-3" id={sectionId}>
      <button
        type="button"
        onClick={toggle}
        aria-expanded={isOpen}
        className="flex w-full items-baseline justify-between gap-2 text-left transition-colors hover:text-amber-600"
      >
        <div className="flex items-baseline gap-2">
          <span
            aria-hidden
            className={`inline-block text-slate-400 transition-transform ${isOpen ? "rotate-90" : ""}`}
          >
            ▸
          </span>
          <h2 className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate-700">
            {title}
          </h2>
          {summary ? (
            <span className="text-xs text-slate-500">— {summary}</span>
          ) : null}
        </div>
        <span className="text-[11px] text-slate-500">
          {isOpen ? "Hide" : "Show"}
        </span>
      </button>
      {isOpen ? <div className="mt-3">{children}</div> : null}
    </section>
  );
}
