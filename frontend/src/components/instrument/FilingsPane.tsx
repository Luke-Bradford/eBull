/**
 * FilingsPane — high-signal filings list (8-K + 10-K + 10-Q + foreign
 * issuer equivalents) on the instrument page density grid (#559 / #567).
 * Each row links to the corresponding drilldown route. A "View all
 * filings →" footer routes to the canonical Filings tab.
 */

import { fetchFilings } from "@/api/filings";
import type { FilingsListResponse } from "@/api/types";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { useCallback } from "react";
import { Link } from "react-router-dom";

const ROW_LIMIT = 6;

// US issuer types + foreign private issuer (FPI / ADR) types in one
// list. The backend filters with `filing_type = ANY(...)`, so listing
// FPI types alongside US types is harmless on US instruments and
// correct for foreign issuers.
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

function drilldownLink(symbol: string, filingType: string | null): string | null {
  if (filingType === null || !TYPES_WITH_DRILLDOWN.has(filingType)) return null;
  const symbolEnc = encodeURIComponent(symbol);
  if (filingType.startsWith("10-K")) {
    return `/instrument/${symbolEnc}/filings/10-k`;
  }
  return `/instrument/${symbolEnc}/filings/8-k`;
}

export interface FilingsPaneProps {
  readonly instrumentId: number;
  readonly symbol: string;
}

export function FilingsPane({ instrumentId, symbol }: FilingsPaneProps): JSX.Element {
  const state = useAsync<FilingsListResponse>(
    useCallback(
      () =>
        fetchFilings(instrumentId, 0, ROW_LIMIT, {
          filing_type: SIGNIFICANT_FILING_TYPES,
        }),
      [instrumentId],
    ),
    [instrumentId],
  );

  return (
    <Section title="Recent filings">
      {state.loading ? (
        <SectionSkeleton rows={5} />
      ) : state.error !== null ? (
        <SectionError onRetry={state.refetch} />
      ) : state.data === null || state.data.items.length === 0 ? (
        <EmptyState
          title="No filings"
          description="No 8-K / 10-K / 10-Q rows on file for this instrument."
        />
      ) : (
        <ul className="space-y-1.5 text-xs">
          {state.data.items.slice(0, ROW_LIMIT).map((f) => {
            const link = drilldownLink(symbol, f.filing_type ?? null);
            const label = (
              <span className="flex items-baseline gap-2">
                <span className="text-slate-500">{f.filing_date}</span>
                <span className="rounded bg-slate-100 px-1 py-0.5 text-[10px] text-slate-600">
                  {f.filing_type ?? "?"}
                </span>
                <span className="truncate text-slate-700">
                  {f.extracted_summary ?? f.filing_type ?? "filing"}
                </span>
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
      <div className="mt-2 border-t border-slate-100 pt-1.5 text-right">
        <Link
          to={`/instrument/${encodeURIComponent(symbol)}?tab=filings`}
          className="text-[11px] text-sky-700 hover:underline"
        >
          View all filings →
        </Link>
      </div>
    </Section>
  );
}
