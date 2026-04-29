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
  BusinessSectionsParseStatus,
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

/**
 * Format the absolute timestamp for the empty-state hint. Keeps
 * conditionally-rendered absolute times consistent across the four
 * empty-state branches and makes a future "use relative time" swap
 * a one-line change.
 */
function formatStamp(iso: string | null): string | null {
  if (!iso) return null;
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return null;
    return d.toISOString().slice(0, 16).replace("T", " ") + " UTC";
  } catch {
    return null;
  }
}

interface ParseStatusEmptyStateProps {
  status: BusinessSectionsParseStatus;
}

function ParseStatusEmptyState({ status }: ParseStatusEmptyStateProps): JSX.Element {
  // Distinct copy per state so the operator can tell at a glance
  // whether the empty panel needs investigation, will fix itself, or
  // is intrinsic to the filing.
  if (status.state === "no_item_1") {
    return (
      <EmptyState
        title="10-K has no Item 1"
        description={
          "The latest 10-K filed by this issuer does not contain a parseable Item 1 " +
          "Business section — common for 10-K/A amendments and shell-company filings. " +
          "Nothing to investigate."
        }
      />
    );
  }
  if (status.state === "parse_failed") {
    const stamp = formatStamp(status.last_attempted_at);
    const retry = formatStamp(status.next_retry_at);
    const desc = [
      `Parser failed${status.failure_reason ? ` (${status.failure_reason})` : ""}.`,
      stamp ? ` Last attempted ${stamp}.` : "",
      retry ? ` Next retry after ${retry}.` : "",
    ]
      .join("")
      .trim();
    return (
      <EmptyState
        title="10-K Item 1 parse failed"
        description={desc || "Parser failed; will retry on the next ingester pass."}
      />
    );
  }
  if (status.state === "sections_pending") {
    return (
      <EmptyState
        title="Sections pending"
        description={
          "Item 1 was extracted but the section splitter has not written subsections " +
          "yet. Should appear shortly."
        }
      />
    );
  }
  // not_attempted
  return (
    <EmptyState
      title="10-K Item 1 not yet parsed"
      description={
        "The narrative ingester has not visited this instrument yet. It will be " +
        "picked up on the next scheduled SEC business-summary pass."
      }
    />
  );
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
        // #648 — render distinct empty states instead of the generic
        // "No 10-K Item 1 on file" so the operator can tell parse-
        // pending from parse-failed from genuinely-no-Item-1.
        state.data?.parse_status ? (
          <ParseStatusEmptyState status={state.data.parse_status} />
        ) : (
          <EmptyState
            title="No 10-K Item 1 on file"
            description="No 10-K business description has been parsed for this instrument yet."
          />
        )
      ) : (
        <div className="space-y-2 text-sm">
          <p className="max-w-prose leading-relaxed text-slate-700">
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
