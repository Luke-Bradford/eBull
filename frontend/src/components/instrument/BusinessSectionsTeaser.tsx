/**
 * BusinessSectionsTeaser — 240-char excerpt of the 10-K Item 1
 * narrative on the instrument page (#552). Replaces the full inline
 * BusinessSectionsPanel which was rendering the entire wall-of-text
 * (up to 102 KB pre-#550 fixes, still verbose post-fix).
 *
 * Pattern matches Bloomberg / Refinitiv / CapIQ — the main
 * instrument view shows a curated short summary + a link to the
 * full sectioned drilldown. Operator clicks through when they want
 * to read the issuer's authoritative wording.
 */

import { fetchBusinessSections } from "@/api/instruments";
import type {
  BusinessSection,
  BusinessSectionsResponse,
} from "@/api/instruments";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { Pane } from "@/components/instrument/Pane";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { useCallback } from "react";
import { useNavigate } from "react-router-dom";

export interface BusinessSectionsTeaserProps {
  readonly symbol: string;
}

const TEASER_LEN = 240;

function pickTeaser(sections: ReadonlyArray<BusinessSection>): string {
  // Prefer the first non-empty body — usually the "general" /
  // "overview" intro paragraph. Fall back to any section's body
  // if the first is unexpectedly empty.
  for (const s of sections) {
    if (s.body && s.body.length > 0) {
      const text = s.body
        .replace(/␞TABLE_\d+␞/g, "") // strip embedded-table sentinels
        .replace(/\s+/g, " ")
        .trim();
      if (text.length <= TEASER_LEN) return text;
      const slice = text.slice(0, TEASER_LEN);
      const lastSpace = slice.lastIndexOf(" ");
      const cut = lastSpace > TEASER_LEN * 0.7 ? lastSpace : TEASER_LEN;
      return text.slice(0, cut).trim() + "…";
    }
  }
  return "";
}

export function BusinessSectionsTeaser({ symbol }: BusinessSectionsTeaserProps) {
  const navigate = useNavigate();
  const state = useAsync<BusinessSectionsResponse>(
    useCallback(() => fetchBusinessSections(symbol), [symbol]),
    [symbol],
  );

  return (
    <Pane
      title="Company narrative"
      scope="10-K Item 1"
      source={{ providers: ["sec_10k_item1"] }}
      onExpand={() => navigate(`/instrument/${encodeURIComponent(symbol)}/filings/10-k`)}
    >
      {state.loading ? (
        <SectionSkeleton rows={2} />
      ) : state.error !== null ? (
        <SectionError onRetry={state.refetch} />
      ) : state.data === null || state.data.sections.length === 0 ? (
        <EmptyState
          title="No 10-K Item 1 on file"
          description="No 10-K business description has been parsed for this instrument yet."
        />
      ) : (
        <div className="space-y-2 text-sm">
          <p className="leading-relaxed text-slate-700">
            {pickTeaser(state.data.sections)}
          </p>
          {state.data.source_accession !== null && (
            <span className="text-[11px] text-slate-500">
              accession{" "}
              <span className="font-mono">{state.data.source_accession}</span>
            </span>
          )}
        </div>
      )}
    </Pane>
  );
}
