/**
 * /instrument/:symbol/filings/analytics — filings-analytics drill (#592).
 *
 * Two charts over the server's per-(quarter, filing_type) counts
 * (`/filings/{id}/quarterly-counts`): a stacked filing-density timeline + a
 * form-type heatmap. The red-flag-score trend the issue also called for is
 * deferred to #1748 — `red_flag_score` is unpopulated across the filings corpus.
 */
import { useCallback } from "react";
import { Link, useParams } from "react-router-dom";

import { fetchFilingQuarterlyCounts } from "@/api/filings";
import { fetchInstrumentSummary } from "@/api/instruments";
import type { FilingQuarterlyCounts } from "@/api/types";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import {
  FilingDensityChart,
  FilingHeatmapChart,
} from "@/components/filings/filingsAnalyticsCharts";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";

export function FilingsAnalyticsPage(): JSX.Element {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const backHref = `/instrument/${encodeURIComponent(symbol)}`;

  // The /filings endpoints are instrument_id-keyed; resolve :symbol → id via the
  // summary, then pull the aggregated counts — one lifecycle, one error surface.
  const counts = useAsync<FilingQuarterlyCounts>(
    useCallback(
      () =>
        fetchInstrumentSummary(symbol).then((s) => fetchFilingQuarterlyCounts(s.instrument_id)),
      [symbol],
    ),
    [symbol],
  );

  const isEmpty = counts.data !== null && counts.data.counts.length === 0;

  return (
    <div className="mx-auto max-w-screen-xl space-y-4 p-4">
      <header className="border-b border-slate-200 dark:border-slate-800 pb-3">
        <Link to={backHref} className="text-xs text-sky-700 hover:underline">
          ← Back to {symbol}
        </Link>
        <div className="mt-1 flex flex-wrap items-baseline justify-between gap-2">
          <h1 className="text-lg font-semibold text-slate-900 dark:text-slate-100">
            Filing analytics — {symbol}
          </h1>
          <div className="flex items-center gap-2 text-xs">
            <Link to={`${backHref}/filings/8-k`} className="text-sky-700 hover:underline">
              8-K list →
            </Link>
            <Link to={`${backHref}/filings/10-k`} className="text-sky-700 hover:underline">
              10-K list →
            </Link>
          </div>
        </div>
        <p className="mt-1 text-xs text-slate-500">
          SEC filing cadence over the last 5 years, by quarter and form type. Spot the reporting
          rhythm (10-K / 10-Q), event clusters (8-K), and ownership activity (13D/G). Routine
          insider Form 3/4/5 is excluded — see the insider drill.
        </p>
      </header>

      {counts.loading ? (
        <SectionSkeleton rows={6} />
      ) : counts.error !== null ? (
        <SectionError onRetry={counts.refetch} />
      ) : counts.data === null || isEmpty ? (
        <EmptyState
          title="No filings on record"
          description="No SEC filing events for this instrument in the last 5 years — likely a non-US issuer or one without an SEC CIK."
        >
          <Link to={backHref} className="text-sm text-sky-700 hover:underline">
            ← Back to {symbol}
          </Link>
        </EmptyState>
      ) : (
        <>
          <Section title="Filing density — count per quarter by form type">
            <FilingDensityChart counts={counts.data.counts} />
          </Section>
          <Section title="Form-type heatmap">
            <FilingHeatmapChart counts={counts.data.counts} />
          </Section>
        </>
      )}
    </div>
  );
}
