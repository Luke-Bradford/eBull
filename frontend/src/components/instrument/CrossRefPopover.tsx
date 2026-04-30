/**
 * CrossRefPopover — small popover triggered by clicking a cross-ref
 * chip in a 10-K section. Shows a 240-char excerpt of the targeted
 * section + an "Open full" link. For unresolvable targets (Note 5
 * when notes ingestion isn't on, Exhibit 21 — out of doc) it shows
 * a "Source: SEC iXBRL viewer" link instead (#559).
 */

import { useState } from "react";
import type { BusinessCrossReference, BusinessSection } from "@/api/instruments";

const PREVIEW_LEN = 240;

export interface CrossRefPopoverProps {
  readonly cref: BusinessCrossReference;
  /** All sections in the current 10-K — used to resolve "Item 1A" etc. */
  readonly sections: ReadonlyArray<BusinessSection>;
  /** SEC EDGAR search URL for fall-back when target isn't ingested. */
  readonly secSearchUrl: string | null;
}

function findTargetSection(
  cref: BusinessCrossReference,
  sections: ReadonlyArray<BusinessSection>,
): BusinessSection | null {
  if (cref.reference_type !== "item") return null;
  // cref.target like "Item 1A" — match on section_label prefix.
  const wanted = cref.target.toLowerCase().replace(/\s+/g, " ").trim();
  return (
    sections.find((s) =>
      s.section_label.toLowerCase().includes(wanted),
    ) ?? null
  );
}

function shortenBody(body: string): string {
  const flat = body.replace(/\s+/g, " ").trim();
  if (flat.length <= PREVIEW_LEN) return flat;
  const slice = flat.slice(0, PREVIEW_LEN);
  const cut = slice.lastIndexOf(" ");
  return (cut > PREVIEW_LEN * 0.7 ? slice.slice(0, cut) : slice) + "…";
}

export function CrossRefPopover({
  cref,
  sections,
  secSearchUrl,
}: CrossRefPopoverProps): JSX.Element {
  const [open, setOpen] = useState(false);
  const target = findTargetSection(cref, sections);

  return (
    <span className="relative inline-block">
      <button
        type="button"
        className="rounded bg-sky-100 px-1.5 py-0.5 text-[11px] font-medium text-sky-700 hover:bg-sky-200"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
      >
        {cref.target}
      </button>
      {open && (
        <span className="absolute left-0 top-full z-20 mt-1 block w-72 rounded border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-3 text-xs shadow-lg">
          <span className="block text-[10px] uppercase tracking-wider text-slate-500">
            {cref.reference_type === "item" ? "Preview" : "Reference"} · {cref.target}
          </span>
          {target ? (
            <>
              <span className="mt-1 block font-medium text-slate-800 dark:text-slate-100">
                {target.section_label}
              </span>
              <span className="mt-1 block leading-relaxed text-slate-700">
                {shortenBody(target.body)}
              </span>
              <a
                href={`#s-${target.section_order}-${target.section_key}`}
                className="mt-2 block text-sky-700 hover:underline"
                onClick={() => setOpen(false)}
              >
                Open full ↗
              </a>
            </>
          ) : (
            <>
              <span className="mt-1 block leading-relaxed text-slate-600">
                Not yet ingested in eBull. View the source on SEC.
              </span>
              {secSearchUrl !== null && (
                <a
                  href={secSearchUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="mt-2 block text-sky-700 hover:underline"
                >
                  Search for filing on SEC EDGAR ↗
                </a>
              )}
            </>
          )}
        </span>
      )}
    </span>
  );
}
