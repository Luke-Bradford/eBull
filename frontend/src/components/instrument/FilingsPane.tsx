/**
 * FilingsPane — 5-row recent-filings list (8-K + 10-K) on the
 * instrument page density grid (#559). Each row links to the
 * corresponding drilldown route. Read-only — the canonical filings
 * tab still lives in the page tabs nav.
 */

import { fetchFilings } from "@/api/filings";
import type { FilingsListResponse } from "@/api/types";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { useCallback } from "react";
import { Link } from "react-router-dom";

const ROW_LIMIT = 5;

const TYPES_WITH_DRILLDOWN = new Set(["8-K", "8-K/A", "10-K", "10-K/A"]);

function drilldownLink(symbol: string, filingType: string | null): string | null {
  if (filingType === null || !TYPES_WITH_DRILLDOWN.has(filingType)) return null;
  const symbolEnc = encodeURIComponent(symbol);
  if (filingType.startsWith("10-K")) {
    // 10-K drilldown defaults to the latest filing — no accession
    // needed from the row. Operator picks an older year via the
    // metadata rail's prior-10-Ks list once on the drilldown page.
    return `/instrument/${symbolEnc}/filings/10-k`;
  }
  // 8-K family — list page shows all filings; row click on the list
  // page itself handles per-accession selection.
  return `/instrument/${symbolEnc}/filings/8-k`;
}

export interface FilingsPaneProps {
  readonly instrumentId: number;
  readonly symbol: string;
}

export function FilingsPane({
  instrumentId,
  symbol,
}: FilingsPaneProps): JSX.Element {
  const state = useAsync<FilingsListResponse>(
    useCallback(() => fetchFilings(instrumentId, 0, ROW_LIMIT), [instrumentId]),
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
          description="Filings appear once SEC EDGAR has been crawled for this instrument."
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
    </Section>
  );
}
