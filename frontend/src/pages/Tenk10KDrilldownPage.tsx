/**
 * /instrument/:symbol/filings/10-k — full SEC 10-K Item 1 drilldown
 * (#552).
 *
 * Layout: left-rail TOC (built from section_order + section_label) +
 * scrollable main panel. Mirrors what AlphaSense / Sentieo do for
 * filings — operator picks a section from the TOC, scrolls the body.
 *
 * Routes back to ``/instrument/{symbol}`` via a Back link in the
 * page header.
 */

import { fetchBusinessSections } from "@/api/instruments";
import type {
  BusinessSection,
  BusinessSectionsResponse,
} from "@/api/instruments";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { useCallback } from "react";
import { Link, useParams } from "react-router-dom";

function sectionAnchorId(s: BusinessSection): string {
  // Stable anchor: section_order keeps each row unique even when
  // labels collide (rare, but pre-#550 the parser emitted multiple
  // "general" sections — this guards against that).
  return `s-${s.section_order}-${s.section_key}`;
}

function SectionBody({ section }: { section: BusinessSection }) {
  return (
    <article id={sectionAnchorId(section)} className="border-l-2 border-slate-200 pl-4">
      <h3 className="text-base font-semibold text-slate-800">{section.section_label}</h3>
      <p className="mt-2 whitespace-pre-wrap text-sm leading-relaxed text-slate-700">
        {section.body}
      </p>
      {section.cross_references.length > 0 && (
        <div className="mt-2 flex flex-wrap gap-1 text-[11px]">
          <span className="text-slate-500">Cross-refs:</span>
          {section.cross_references.map((ref, idx) => (
            <span
              key={`${ref.reference_type}-${ref.target}-${idx}`}
              className="rounded bg-slate-100 px-1.5 py-0.5 text-slate-700"
              title={ref.context}
            >
              {ref.target}
            </span>
          ))}
        </div>
      )}
    </article>
  );
}

function TOCRail({ sections }: { sections: ReadonlyArray<BusinessSection> }) {
  return (
    <nav className="sticky top-4 max-h-[calc(100vh-2rem)] overflow-y-auto rounded border border-slate-200 bg-slate-50 p-3 text-sm">
      <div className="mb-2 text-xs font-medium uppercase tracking-wider text-slate-500">
        Sections
      </div>
      <ul className="space-y-1">
        {sections.map((s) => (
          <li key={sectionAnchorId(s)}>
            <a
              href={`#${sectionAnchorId(s)}`}
              className="block truncate text-slate-700 hover:text-sky-700 hover:underline"
              title={s.section_label}
            >
              {s.section_label}
            </a>
          </li>
        ))}
      </ul>
    </nav>
  );
}

function Body({ data, symbol }: { data: BusinessSectionsResponse; symbol: string }) {
  if (data.sections.length === 0) {
    return (
      <EmptyState
        title="No 10-K Item 1 on file"
        description="No 10-K business description has been parsed for this instrument yet."
      />
    );
  }
  return (
    <div className="grid gap-4 md:grid-cols-[16rem_1fr]">
      <aside className="hidden md:block">
        <TOCRail sections={data.sections} />
      </aside>
      <div className="space-y-6">
        <header className="border-b border-slate-200 pb-2">
          <Link
            to={`/instrument/${encodeURIComponent(symbol)}`}
            className="text-xs text-sky-700 hover:underline"
          >
            ← Back to {symbol}
          </Link>
          <h2 className="mt-1 text-lg font-semibold text-slate-900">
            Form 10-K · Item 1 Business
          </h2>
          {data.source_accession !== null && (
            <div className="text-[11px] text-slate-500">
              accession <span className="font-mono">{data.source_accession}</span>
            </div>
          )}
        </header>
        {data.sections.map((s) => (
          <SectionBody key={sectionAnchorId(s)} section={s} />
        ))}
      </div>
    </div>
  );
}

export function Tenk10KDrilldownPage() {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const state = useAsync<BusinessSectionsResponse>(
    useCallback(() => fetchBusinessSections(symbol), [symbol]),
    [symbol],
  );

  return (
    <div className="mx-auto max-w-6xl p-4">
      <Section title={`${symbol} — 10-K narrative`}>
        {state.loading ? (
          <SectionSkeleton rows={6} />
        ) : state.error !== null ? (
          <SectionError onRetry={state.refetch} />
        ) : state.data === null ? (
          <EmptyState
            title="Business narrative unavailable"
            description="Could not load 10-K Item 1 sections for this instrument."
          />
        ) : (
          <Body data={state.data} symbol={symbol} />
        )}
      </Section>
    </div>
  );
}
