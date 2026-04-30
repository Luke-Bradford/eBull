/**
 * `<Term>` — wraps an abbreviation/short label so the operator gets
 * a hover/focus tooltip with the human-readable shortName + what +
 * why (#684).
 *
 * Renders semantically as `<abbr>` so the underline + cursor +
 * keyboard-focus story is handled by the browser. The tooltip body
 * is a styled popover (not the native `title` attribute) because
 * native tooltips can't render multi-line / multi-paragraph content
 * on most browsers, and the `why` line is the part that actually
 * justifies wrapping the term.
 *
 * Falls back to plain text when the term isn't in the glossary —
 * caller bug surfaces as plain rendering rather than a runtime
 * error. The optional ``children`` prop lets the operator override
 * the rendered text without breaking the lookup (e.g. wrap the long
 * "Filer category" label while looking up under that key).
 */

import { useState, type JSX, type ReactNode } from "react";

import { lookupTerm } from "@/lib/glossary";

export interface TermProps {
  /** The glossary key. Also rendered as the visible text unless
   *  ``children`` overrides. */
  readonly term: string;
  /** Optional label override. Useful when the visible text differs
   *  from the glossary key (e.g. ``<Term term="P/E ratio">P/E</Term>``). */
  readonly children?: ReactNode;
  /** Extra Tailwind classes for the underline / colour. */
  readonly className?: string;
}

export function Term({ term, children, className }: TermProps): JSX.Element {
  const entry = lookupTerm(term);
  const [open, setOpen] = useState(false);

  // Unknown term — render plain text so the operator at least sees
  // the label. Logged at DEV time would help future contributors;
  // skipping for now to keep the component pure.
  if (entry === null) {
    return <span className={className}>{children ?? term}</span>;
  }

  const visibleClass = [
    // `decoration-dotted` reads as "this is interactive, hover for
    // more" without the visual noise of a solid underline. The
    // cursor flip to `cursor-help` matches the `<abbr>` semantic.
    "decoration-dotted underline-offset-2 underline cursor-help",
    "focus-visible:outline-2 focus-visible:outline-sky-500 focus-visible:rounded",
    className ?? "",
  ]
    .filter((s) => s.length > 0)
    .join(" ");

  return (
    <span className="relative inline-block">
      <abbr
        title={`${entry.shortName} — ${entry.what}`}
        className={visibleClass}
        tabIndex={0}
        onMouseEnter={() => setOpen(true)}
        onMouseLeave={() => setOpen(false)}
        onFocus={() => setOpen(true)}
        onBlur={() => setOpen(false)}
        data-testid={`term-${entry.term}`}
      >
        {children ?? entry.term}
      </abbr>
      {open ? (
        <span
          role="tooltip"
          className="absolute left-0 top-full z-50 mt-1 w-72 rounded-md border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 px-3 py-2 text-xs leading-snug text-slate-700 shadow-lg"
        >
          <span className="block text-[10px] font-semibold uppercase tracking-wider text-slate-500">
            {entry.shortName}
          </span>
          <span className="mt-1 block">{entry.what}</span>
          <span className="mt-1.5 block text-[11px] text-slate-500">
            <span className="font-medium text-slate-600">Why it matters:</span>{" "}
            {entry.why}
          </span>
        </span>
      ) : null}
    </span>
  );
}
