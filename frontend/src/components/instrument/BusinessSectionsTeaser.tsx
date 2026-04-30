/**
 * BusinessSectionsTeaser — up to three short section previews from
 * the 10-K Item 1 narrative on the instrument page (#552). Replaces
 * the full inline BusinessSectionsPanel which was rendering the
 * entire wall-of-text (up to 102 KB pre-#550 fixes, still verbose
 * post-fix).
 *
 * Pattern matches Bloomberg / Refinitiv / CapIQ — the main
 * instrument view shows a curated short summary + a link to the
 * full sectioned drilldown. Operator clicks through when they want
 * to read the issuer's authoritative wording.
 *
 * Grid layout: up to three section cards (`section_label` + 200-char
 * body teaser) in a responsive 1/2/3-column grid that adapts to the
 * 8-col Pane width. Fewer than 3 sections render the available cards
 * across whatever columns the responsive grid resolves to — the
 * empty grid cells are dead space the parent grid cell would have
 * left anyway. More than 3 sections truncate; the page-level
 * "Open →" drills to the full sectioned drilldown.
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

const TEASER_LEN = 200;
const MAX_CARDS = 3;

/** Per-section teaser: strip table sentinels + collapse whitespace,
 *  truncate at the last word boundary inside the cap so the 200-char
 *  preview reads as a clean sentence fragment. Returns the empty
 *  string when the section body is empty so the caller can choose to
 *  drop the card. */
function teaseBody(body: string): string {
  if (!body) return "";
  const text = body
    .replace(/␞TABLE_\d+␞/g, "")
    .replace(/\s+/g, " ")
    .trim();
  if (text.length === 0) return "";
  if (text.length <= TEASER_LEN) return text;
  const slice = text.slice(0, TEASER_LEN);
  const lastSpace = slice.lastIndexOf(" ");
  const cut = lastSpace > TEASER_LEN * 0.7 ? lastSpace : TEASER_LEN;
  return text.slice(0, cut).trim() + "…";
}

/** Pick the first up-to-MAX_CARDS sections that have a non-empty
 *  body. `section_order` from the API is already authoritative — the
 *  ingester emits sections in 10-K presentation order — so a simple
 *  prefix is what we want. */
function pickCards(
  sections: ReadonlyArray<BusinessSection>,
): Array<{ key: string; label: string; teaser: string }> {
  const out: Array<{ key: string; label: string; teaser: string }> = [];
  for (const s of sections) {
    if (out.length >= MAX_CARDS) break;
    const t = teaseBody(s.body);
    if (t.length === 0) continue;
    out.push({ key: s.section_key, label: s.section_label, teaser: t });
  }
  return out;
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

/** Up-to-three card grid. When fewer than three sections have content,
 *  the responsive grid still wraps to whatever columns the available
 *  width resolves to — empty cells aren't created. When zero usable
 *  sections exist, falls back to an EmptyState so the operator gets
 *  the same parse-status branching as the original single-paragraph
 *  layout. */
function SectionCardGrid({
  sections,
}: {
  readonly sections: ReadonlyArray<BusinessSection>;
}): JSX.Element {
  const cards = pickCards(sections);
  if (cards.length === 0) {
    return (
      <EmptyState
        title="No 10-K Item 1 body on file"
        description="Item 1 sections were extracted but every body is empty — no preview to show."
      />
    );
  }
  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
      {cards.map((c) => (
        <div key={c.key} className="space-y-1">
          <h3 className="text-[11px] font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400">
            {c.label}
          </h3>
          <p className="leading-relaxed text-slate-700 dark:text-slate-300">
            {c.teaser}
          </p>
        </div>
      ))}
    </div>
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
        <div className="space-y-3 text-sm">
          <SectionCardGrid sections={state.data.sections} />
          {state.data.source_accession !== null && (
            <span className="text-[11px] text-slate-500 dark:text-slate-400">
              accession{" "}
              <span className="font-mono">{state.data.source_accession}</span>
            </span>
          )}
        </div>
      )}
    </Pane>
  );
}
