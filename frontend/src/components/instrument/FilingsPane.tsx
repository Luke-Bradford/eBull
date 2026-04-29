/**
 * FilingsPane — high-signal filings list (8-K + 10-K + 10-Q + foreign
 * issuer equivalents) on the instrument page density grid (#559 / #567).
 * Each row links to the corresponding drilldown route. An "Open →" button
 * in the pane header routes to the canonical Filings tab when that tab
 * is active for the instrument.
 *
 * The SIGNIFICANT_FILING_TYPES filter is applied only when the instrument
 * has ``sec_edgar`` as a filings provider — SEC-style form types are
 * meaningless on other providers (e.g. Companies House).
 */

import { fetchFilings } from "@/api/filings";
import type { FilingsListResponse, InstrumentSummary } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { Pane } from "@/components/instrument/Pane";
import { Term } from "@/components/Term";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { filingTypeFriendlyName } from "@/lib/glossary";
import { useCallback } from "react";
import { Link, useNavigate } from "react-router-dom";

const ROW_LIMIT = 6;

// US issuer types + foreign private issuer (FPI / ADR) equivalents.
// Applied only when the instrument's filings capability includes
// sec_edgar as a provider.
const SIGNIFICANT_FILING_TYPES = [
  "8-K",
  "8-K/A",
  "10-K",
  "10-K/A",
  "10-Q",
  "10-Q/A",
  "6-K",
  "6-K/A",
  "20-F",
  "20-F/A",
  "40-F",
  "40-F/A",
].join(",");

const TYPES_WITH_DRILLDOWN = new Set(["8-K", "8-K/A", "10-K", "10-K/A"]);

function drilldownLink(
  symbol: string,
  filingType: string | null,
  accessionNumber: string | null,
): string | null {
  if (filingType === null || !TYPES_WITH_DRILLDOWN.has(filingType)) return null;
  const symbolEnc = encodeURIComponent(symbol);
  if (filingType.startsWith("10-K")) {
    // #565: append ?accession=... so a click on a non-latest 10-K
    // (or a 10-K/A amendment) lands on that specific filing's
    // drilldown rather than always on the latest 10-K. Falls back
    // to the bare URL when accession is missing — Tenk10KDrilldownPage
    // handles that as "show the latest filing".
    if (accessionNumber !== null) {
      return `/instrument/${symbolEnc}/filings/10-k?accession=${encodeURIComponent(accessionNumber)}`;
    }
    return `/instrument/${symbolEnc}/filings/10-k`;
  }
  return `/instrument/${symbolEnc}/filings/8-k`;
}

/** True iff the instrument has an active filings capability
 *  (any provider has data_present === true). Mirrors the check in
 *  InstrumentPage.visibleTabs. */
function hasActiveFilingsCapability(summary: InstrumentSummary): boolean {
  const cell = summary.capabilities.filings;
  if (cell === undefined) return false;
  return cell.providers.some((p) => cell.data_present[p] === true);
}

export interface FilingsPaneProps {
  readonly instrumentId: number;
  readonly symbol: string;
  readonly summary: InstrumentSummary;
}

export function FilingsPane({
  instrumentId,
  symbol,
  summary,
}: FilingsPaneProps): JSX.Element {
  const navigate = useNavigate();
  const filingsCell = summary.capabilities.filings;
  const isSecEdgar =
    filingsCell !== undefined && filingsCell.providers.includes("sec_edgar");
  const typeFilter = isSecEdgar ? SIGNIFICANT_FILING_TYPES : undefined;
  const filingsTabActive = hasActiveFilingsCapability(summary);

  const sourceProviders = filingsCell?.providers ?? [];

  const state = useAsync<FilingsListResponse>(
    useCallback(
      () =>
        fetchFilings(instrumentId, 0, ROW_LIMIT, {
          filing_type: typeFilter,
        }),
      // eslint-disable-next-line react-hooks/exhaustive-deps
      [instrumentId, typeFilter],
    ),
    [instrumentId, typeFilter],
  );

  return (
    <Pane
      title="Recent filings"
      scope="high-signal types"
      source={{ providers: sourceProviders }}
      onExpand={
        filingsTabActive
          ? () => navigate(`/instrument/${encodeURIComponent(symbol)}?tab=filings`)
          : undefined
      }
    >
      {state.loading ? (
        <SectionSkeleton rows={5} />
      ) : state.error !== null ? (
        <SectionError onRetry={state.refetch} />
      ) : state.data === null || state.data.items.length === 0 ? (
        <EmptyState
          title="No filings"
          description={
            isSecEdgar
              ? "No 8-K / 10-K / 10-Q rows on file for this instrument."
              : "No filing rows on file for this instrument."
          }
        />
      ) : (
        <ul className="space-y-1.5 text-xs">
          {state.data.items.slice(0, ROW_LIMIT).map((f) => {
            const link = drilldownLink(
              symbol,
              f.filing_type,
              f.accession_number,
            );
            // When the XBRL ingest hasn't extracted a per-filing
            // summary, the row's third column would previously
            // render the raw form-type a SECOND time next to the
            // already-shown form-type chip — operator-reported as
            // a "10-K  10-K" / "8-K  8-K" duplicate (#684). Falls
            // back to the glossary's friendly short name instead
            // (e.g. "Annual report" / "Material event") so the
            // row carries useful information at a glance.
            const summary =
              f.extracted_summary ?? filingTypeFriendlyName(f.filing_type);
            const label = (
              <span className="flex items-baseline gap-2">
                <span className="text-slate-500">{f.filing_date}</span>
                {f.filing_type !== null ? (
                  <Term
                    term={f.filing_type}
                    className="rounded bg-slate-100 px-1 py-0.5 text-[10px] text-slate-600 no-underline"
                  >
                    {f.filing_type}
                  </Term>
                ) : (
                  <span className="rounded bg-slate-100 px-1 py-0.5 text-[10px] text-slate-600">
                    ?
                  </span>
                )}
                <span className="truncate text-slate-700">{summary}</span>
              </span>
            );
            return (
              <li key={f.filing_event_id}>
                {link !== null ? (
                  <Link to={link} className="hover:underline">
                    {label}
                  </Link>
                ) : (
                  label
                )}
              </li>
            );
          })}
        </ul>
      )}
    </Pane>
  );
}
