/**
 * /instrument/:symbol/news-analysis — news-analytics drill (#593).
 *
 * Three charts over `/news/{instrument_id}` (#1750 RSS provider): a 7-day
 * rolling-mean sentiment trend (emerald > 0 / red < 0), weekly news volume,
 * and a source breakdown. Mirrors the #592 filings-analytics drill shell.
 *
 * News on dev is RSS-recent (single source, ~1 week) so the charts render
 * honestly-sparse with a low-history caption — the same "build the real
 * chart, annotate the limitation" posture as the #594 peer drill's
 * dev_limited flags. The charts fill as news accrues in production.
 */
import { useCallback } from "react";
import { Link, useParams } from "react-router-dom";

import { fetchAllNews, type AllNews } from "@/api/news";
import { fetchInstrumentSummary } from "@/api/instruments";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import {
  NewsVolumeChart,
  SentimentTrendChart,
  SourceBreakdownPie,
} from "@/components/news/newsAnalyticsCharts";
import { EmptyState } from "@/components/states/EmptyState";
import {
  buildSentimentSeries,
  buildSourceBreakdown,
  buildWeeklyVolume,
  newsCoverage,
} from "@/lib/newsAnalytics";
import { useAsync } from "@/lib/useAsync";

export function NewsAnalysisPage(): JSX.Element {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const backHref = `/instrument/${encodeURIComponent(symbol)}`;

  // /news is instrument_id-keyed; resolve :symbol → id via the summary, then
  // drain the paginated window — one lifecycle, one error surface (mirrors
  // FilingsAnalyticsPage).
  const news = useAsync<AllNews>(
    useCallback(
      () => fetchInstrumentSummary(symbol).then((s) => fetchAllNews(s.instrument_id)),
      [symbol],
    ),
    [symbol],
  );

  const items = news.data?.items ?? [];
  const isEmpty = news.data !== null && items.length === 0;

  return (
    <div className="mx-auto max-w-screen-xl space-y-4 p-4">
      <header className="border-b border-slate-200 dark:border-slate-800 pb-3">
        <Link to={backHref} className="text-xs text-sky-700 hover:underline">
          ← Back to {symbol}
        </Link>
        <div className="mt-1 flex flex-wrap items-baseline justify-between gap-2">
          <h1 className="text-lg font-semibold text-slate-900 dark:text-slate-100">
            News analytics — {symbol}
          </h1>
        </div>
        <p className="mt-1 text-xs text-slate-500">
          Sentiment direction, news cadence, and source mix over the last 12 months. Sentiment
          is a signed score smoothed to a 7-day rolling mean (emerald above zero, red below).
        </p>
      </header>

      {news.loading ? (
        <SectionSkeleton rows={6} />
      ) : news.error !== null ? (
        <SectionError onRetry={news.refetch} />
      ) : news.data === null || isEmpty ? (
        <EmptyState
          title="No news on record"
          description="No news events for this instrument in the last 12 months. News coverage is currently limited to a recently-enabled RSS set — most instruments have none yet."
        >
          <Link to={backHref} className="text-sm text-sky-700 hover:underline">
            ← Back to {symbol}
          </Link>
        </EmptyState>
      ) : (
        <NewsAnalyticsBody items={items} capped={news.data.capped} total={news.data.total} />
      )}
    </div>
  );
}

function NewsAnalyticsBody({
  items,
  capped,
  total,
}: {
  items: AllNews["items"];
  capped: boolean;
  total: number;
}): JSX.Element {
  const sentiment = buildSentimentSeries(items);
  const volume = buildWeeklyVolume(items);
  const sources = buildSourceBreakdown(items);
  const coverage = newsCoverage(items);

  return (
    <>
      {(coverage.limited || capped) && (
        <p className="text-[11px] text-amber-700 dark:text-amber-500">
          {capped
            ? `Showing the newest ${items.length.toLocaleString()} of ${total.toLocaleString()} news items.`
            : `Limited history — ${coverage.weeks} week${coverage.weeks === 1 ? "" : "s"} of news from ${coverage.sources} source${coverage.sources === 1 ? "" : "s"}. Trends fill out as coverage grows.`}
        </p>
      )}
      <Section title="Sentiment trend — 7-day rolling mean">
        <SentimentTrendChart series={sentiment} />
      </Section>
      <Section title="News volume — items per ISO week">
        <NewsVolumeChart data={volume} />
      </Section>
      <Section title="Source breakdown">
        <SourceBreakdownPie slices={sources} />
      </Section>
    </>
  );
}
