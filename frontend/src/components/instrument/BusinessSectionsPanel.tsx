/**
 * BusinessSectionsPanel — 10-K Item 1 subsection breakdown for the
 * instrument page. Backed by GET /instruments/{symbol}/business_sections
 * (#449).
 *
 * Replaces / complements the single-blob business summary by surfacing
 * every subsection with its original heading, body, and cross-
 * references to other items / exhibits / notes. The operator picks
 * which subsection to expand — no grep-a-wall-of-text needed.
 *
 * Layout:
 *   ┌─ Company narrative (SEC 10-K Item 1) ─────────┐
 *   │ [subsection label]                            │
 *   │   body paragraph…                             │
 *   │   Cross-refs: Item 7 · Exhibit 21 · Note 15   │
 *   │ [next subsection label]                       │
 *   │   …                                           │
 *   └───────────────────────────────────────────────┘
 */

import { fetchBusinessSections } from "@/api/instruments";
import type {
  BusinessSection,
  BusinessSectionsResponse,
} from "@/api/instruments";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { useCallback, useState } from "react";

export interface BusinessSectionsPanelProps {
  readonly symbol: string;
}

// Human label for canonical section_key. "other" + unrecognised keys
// fall back to the verbatim section_label from the filing.
const KEY_LABEL: Record<string, string> = {
  general: "Overview",
  overview: "Overview",
  strategy: "Strategy",
  history: "History",
  segments: "Business segments",
  products: "Products",
  services: "Services",
  products_and_services: "Products and services",
  customers: "Customers",
  markets: "Markets",
  competition: "Competition",
  seasonality: "Seasonality",
  backlog: "Backlog",
  raw_materials: "Raw materials & supply chain",
  manufacturing: "Manufacturing",
  sales_marketing: "Sales & marketing",
  ip: "Intellectual property",
  r_and_d: "Research & development",
  regulatory: "Government regulation",
  environmental: "Environmental matters",
  climate: "Climate & sustainability",
  human_capital: "Human capital",
  properties: "Properties",
  corporate_info: "Corporate information",
  available_information: "Available information",
};

function labelFor(section: BusinessSection): string {
  // Prefer the filing's verbatim heading when it's meaningful —
  // operators want to see what the issuer actually wrote. Fall back
  // to canonical key label when the verbatim is empty / generic.
  if (section.section_label && section.section_label.trim() !== "General") {
    return section.section_label;
  }
  return KEY_LABEL[section.section_key] ?? section.section_label ?? "Section";
}

function SectionBlock({
  section,
  initiallyExpanded,
}: {
  section: BusinessSection;
  initiallyExpanded: boolean;
}) {
  const [expanded, setExpanded] = useState(initiallyExpanded);
  const hasRefs = section.cross_references.length > 0;
  const showExpandToggle = section.body.length > 360;
  return (
    <div className="border-l-2 border-slate-200 pl-3">
      <div className="mb-1 flex items-center justify-between gap-2">
        <div className="text-sm font-semibold text-slate-800">
          {labelFor(section)}
        </div>
        {showExpandToggle && (
          <button
            type="button"
            onClick={() => setExpanded((v) => !v)}
            className="text-[11px] font-medium text-sky-700 hover:underline"
          >
            {expanded ? "Collapse" : "Expand"}
          </button>
        )}
      </div>
      <div
        className={`whitespace-pre-wrap text-sm text-slate-700 ${
          expanded ? "" : "line-clamp-3"
        }`}
      >
        {section.body}
      </div>
      {hasRefs && (
        <div className="mt-1 flex flex-wrap gap-1 text-[11px]">
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
    </div>
  );
}

function Body({ data }: { data: BusinessSectionsResponse }) {
  if (data.sections.length === 0) {
    return (
      <EmptyState
        title="No 10-K Item 1 on file"
        description="No 10-K business description has been parsed for this instrument yet. Either no 10-K is on file (non-US ticker, very new issuer), or the daily ingester has not yet picked up the latest filing."
      />
    );
  }
  return (
    <div className="space-y-3">
      {data.source_accession !== null && (
        <div className="text-[11px] text-slate-500">
          Source: Form 10-K, accession{" "}
          <span className="font-mono">{data.source_accession}</span>
        </div>
      )}
      {data.sections.map((s) => (
        <SectionBlock
          key={`${s.section_order}-${s.section_key}`}
          section={s}
          // Expand the first (overview) section by default; the rest
          // stay collapsed so the panel isn't a wall of text.
          initiallyExpanded={s.section_order === 0}
        />
      ))}
    </div>
  );
}

export function BusinessSectionsPanel({ symbol }: BusinessSectionsPanelProps) {
  const state = useAsync<BusinessSectionsResponse>(
    useCallback(() => fetchBusinessSections(symbol), [symbol]),
    [symbol],
  );
  return (
    <Section title="Company narrative (SEC 10-K Item 1)">
      {state.loading ? (
        <SectionSkeleton rows={4} />
      ) : state.error !== null ? (
        <SectionError onRetry={state.refetch} />
      ) : state.data === null ? (
        <EmptyState
          title="Business narrative unavailable"
          description="Could not load 10-K Item 1 sections for this instrument."
        />
      ) : (
        <Body data={state.data} />
      )}
    </Section>
  );
}
