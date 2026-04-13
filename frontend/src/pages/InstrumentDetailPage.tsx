/**
 * /instruments/:instrumentId — single instrument deep-dive page (#62).
 *
 * Sections load independently via useAsync so a slow or failing endpoint
 * does not block the entire page.  Each section owns its own loading /
 * error / empty state per the async-data-loading skill.
 *
 * Content:
 *   - Header: symbol, name, sector, exchange, tier, latest quote
 *   - Thesis: latest memo (markdown rendered as preformatted), stance,
 *     confidence, thesis type, critic output
 *   - Score history: table of recent scores
 *   - Filings feed: recent filing events
 *   - News feed: recent news events
 *   - Recommendation history: past recommendations for this instrument
 *   - Position: quantity, cost basis, market value, P&L (hidden if not held)
 */

import { Link, useParams } from "react-router-dom";

import { ApiError } from "@/api/client";
import { fetchFilings } from "@/api/filings";
import { fetchInstrumentDetail } from "@/api/instruments";
import { fetchNews } from "@/api/news";
import { fetchPortfolio } from "@/api/portfolio";
import { fetchRecommendations } from "@/api/recommendations";
import { fetchScoreHistory } from "@/api/scoreHistory";
import { fetchLatestThesis } from "@/api/theses";
import type {
  FilingItem,
  InstrumentDetail,
  NewsItem,
  PositionItem,
  RecommendationListItem,
  ScoreHistoryItem,
  ThesisDetail,
} from "@/api/types";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { ErrorBanner } from "@/components/states/ErrorBanner";
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
import {
  formatDateTime,
  formatMoney,
  formatNumber,
  formatPct,
} from "@/lib/format";
import { safeExternalUrl } from "@/lib/safeUrl";
import { useAsync } from "@/lib/useAsync";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const TIER_LABELS: Record<number, string> = { 1: "Tier 1", 2: "Tier 2", 3: "Tier 3" };

function tierBadge(tier: number | null) {
  if (tier === null) return <span className="text-xs text-slate-400">—</span>;
  const label = TIER_LABELS[tier] ?? `Tier ${tier}`;
  const color =
    tier === 1
      ? "bg-emerald-100 text-emerald-700"
      : tier === 2
        ? "bg-blue-100 text-blue-700"
        : "bg-slate-100 text-slate-600";
  return (
    <span className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-medium ${color}`}>
      {label}
    </span>
  );
}

const STANCE_COLORS: Record<string, string> = {
  buy: "bg-emerald-100 text-emerald-700",
  hold: "bg-slate-100 text-slate-600",
  watch: "bg-amber-100 text-amber-700",
  avoid: "bg-red-100 text-red-700",
};

const ACTION_COLORS: Record<string, string> = {
  BUY: "bg-emerald-100 text-emerald-700",
  ADD: "bg-emerald-50 text-emerald-600",
  HOLD: "bg-slate-100 text-slate-600",
  EXIT: "bg-red-100 text-red-700",
};

const STATUS_COLORS: Record<string, string> = {
  proposed: "bg-amber-100 text-amber-700",
  approved: "bg-blue-100 text-blue-700",
  rejected: "bg-red-100 text-red-700",
  executed: "bg-emerald-100 text-emerald-700",
};

function pill(text: string, colorMap: Record<string, string>) {
  const cls = colorMap[text] ?? "bg-slate-100 text-slate-600";
  return (
    <span className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-medium uppercase ${cls}`}>
      {text}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export function InstrumentDetailPage() {
  const { instrumentId: rawId } = useParams<{ instrumentId: string }>();
  const instrumentId = Number(rawId);

  if (!rawId || Number.isNaN(instrumentId)) {
    return (
      <div className="space-y-4">
        <EmptyState title="Invalid instrument" description="The instrument ID in the URL is not valid.">
          <Link to="/instruments" className="text-sm text-blue-600 hover:underline">
            Back to instruments
          </Link>
        </EmptyState>
      </div>
    );
  }

  return <InstrumentDetailContent instrumentId={instrumentId} />;
}

function InstrumentDetailContent({ instrumentId }: { instrumentId: number }) {
  // Each section loads independently — useAsync captures fn via a ref.
  const instrument = useAsync(
    () => fetchInstrumentDetail(instrumentId),
    [instrumentId],
  );
  const thesis = useAsync(
    () => fetchLatestThesis(instrumentId),
    [instrumentId],
  );
  const scores = useAsync(
    () => fetchScoreHistory(instrumentId),
    [instrumentId],
  );
  const filings = useAsync(
    () => fetchFilings(instrumentId),
    [instrumentId],
  );
  const news = useAsync(
    () => fetchNews(instrumentId),
    [instrumentId],
  );
  const recommendations = useAsync(
    () => fetchRecommendations({ action: null, status: null, instrument_id: instrumentId }),
    [instrumentId],
  );
  const portfolio = useAsync(() => fetchPortfolio(), []);

  // Instrument 404 is a page-level error, not a section error.
  const is404 =
    instrument.error instanceof ApiError && instrument.error.status === 404;

  if (is404) {
    return (
      <div className="space-y-4">
        <EmptyState title="Instrument not found" description="This instrument does not exist or has been removed.">
          <Link to="/instruments" className="text-sm text-blue-600 hover:underline">
            Back to instruments
          </Link>
        </EmptyState>
      </div>
    );
  }

  // All sources failed — page-level banner.  Every useAsync result on
  // this page must be listed here; omitting one lets the banner fire
  // too eagerly.
  const allFailed =
    instrument.error &&
    thesis.error &&
    scores.error &&
    filings.error &&
    news.error &&
    recommendations.error &&
    portfolio.error;

  if (allFailed) {
    return (
      <div className="space-y-4">
        <ErrorBanner message="All data sources failed. Check the browser console for details." />
      </div>
    );
  }

  const position = portfolio.data?.positions.find(
    (p) => p.instrument_id === instrumentId,
  ) ?? null;

  return (
    <div className="space-y-6">
      {/* Header */}
      <HeaderSection instrument={instrument} />

      {/* Position (hidden if not held) */}
      {!portfolio.loading && !portfolio.error && position !== null && (
        <PositionSection position={position} />
      )}

      {/* Thesis */}
      <Section title="Thesis">
        {thesis.loading ? (
          <SectionSkeleton rows={5} />
        ) : thesis.error ? (
          isNotFound(thesis.error) ? (
            <EmptyState
              title="No thesis generated yet"
              description="A thesis will appear here once the thesis engine has run for this instrument."
            />
          ) : (
            <SectionError onRetry={thesis.refetch} />
          )
        ) : thesis.data ? (
          <ThesisContent thesis={thesis.data} instrumentCurrency={instrument.data?.currency ?? "USD"} />
        ) : null}
      </Section>

      {/* Score history */}
      <Section title="Score history">
        {scores.loading ? (
          <SectionSkeleton rows={4} />
        ) : scores.error ? (
          <SectionError onRetry={scores.refetch} />
        ) : scores.data && scores.data.items.length > 0 ? (
          <ScoreHistoryTable items={scores.data.items} />
        ) : (
          <EmptyState
            title="No scoring data available"
            description="Scores will appear here once the ranking engine has run."
          />
        )}
      </Section>

      {/* Filings */}
      <Section title="Filings">
        {filings.loading ? (
          <SectionSkeleton rows={3} />
        ) : filings.error ? (
          isNotFound(filings.error) ? (
            <EmptyState title="Instrument not found for filings" />
          ) : (
            <SectionError onRetry={filings.refetch} />
          )
        ) : filings.data && filings.data.items.length > 0 ? (
          <FilingsTable items={filings.data.items} />
        ) : (
          <EmptyState title="No filing events recorded" description="Filings will appear here once the filings ingestion job has run." />
        )}
      </Section>

      {/* News */}
      <Section title="News">
        {news.loading ? (
          <SectionSkeleton rows={3} />
        ) : news.error ? (
          isNotFound(news.error) ? (
            <EmptyState title="Instrument not found for news" />
          ) : (
            <SectionError onRetry={news.refetch} />
          )
        ) : news.data && news.data.items.length > 0 ? (
          <NewsTable items={news.data.items} />
        ) : (
          <EmptyState
            title="No news events in the last 30 days"
            description="News will appear here once the news ingestion job has run."
          />
        )}
      </Section>

      {/* Recommendations */}
      <Section title="Recommendation history">
        {recommendations.loading ? (
          <SectionSkeleton rows={3} />
        ) : recommendations.error ? (
          <SectionError onRetry={recommendations.refetch} />
        ) : recommendations.data && recommendations.data.items.length > 0 ? (
          <RecommendationsTable items={recommendations.data.items} />
        ) : (
          <EmptyState
            title="No recommendations yet"
            description="Recommendations will appear here once the portfolio manager has run."
          />
        )}
      </Section>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function isNotFound(err: unknown): boolean {
  return err instanceof ApiError && err.status === 404;
}

function HeaderSection({
  instrument,
}: {
  instrument: { data: InstrumentDetail | null; loading: boolean; error: unknown; refetch: () => void };
}) {
  if (instrument.loading) {
    return (
      <div className="animate-pulse space-y-2">
        <div className="h-6 w-48 rounded bg-slate-100" />
        <div className="h-4 w-72 rounded bg-slate-100" />
      </div>
    );
  }
  if (instrument.error) {
    return <SectionError onRetry={instrument.refetch} />;
  }
  const d = instrument.data;
  if (!d) return null;

  const q = d.latest_quote;
  // Quotes are in the instrument's native currency — not the display currency.
  const quoteCcy = d.currency ?? "USD";
  return (
    <div>
      <div className="flex items-center gap-3">
        <h1 className="text-xl font-semibold text-slate-800">{d.symbol}</h1>
        {tierBadge(d.coverage_tier)}
        {!d.is_tradable && (
          <span className="rounded bg-amber-100 px-1.5 py-0.5 text-[10px] font-medium text-amber-700">
            Not tradable
          </span>
        )}
      </div>
      <p className="mt-0.5 text-sm text-slate-600">{d.company_name}</p>
      <p className="mt-0.5 text-xs text-slate-500">
        {[d.sector, d.exchange, d.currency, d.country].filter(Boolean).join(" · ")}
      </p>
      {q && (
        <p className="mt-1 text-sm tabular-nums text-slate-700">
          Bid {formatMoney(q.bid, quoteCcy)} · Ask {formatMoney(q.ask, quoteCcy)}
          {q.spread_pct !== null && <> · Spread {formatPct(q.spread_pct)}</>}
          <span className="ml-2 text-xs text-slate-400">as of {formatDateTime(q.quoted_at)}</span>
        </p>
      )}
    </div>
  );
}

function PositionSection({ position }: { position: PositionItem }) {
  const currency = useDisplayCurrency();
  const p = position;
  const pnlColor = p.unrealized_pnl >= 0 ? "text-emerald-700" : "text-red-700";
  return (
    <Section title="Position">
      <div className="grid grid-cols-2 gap-x-8 gap-y-1 text-sm sm:grid-cols-4">
        <div>
          <span className="text-xs text-slate-500">Units</span>
          <p className="tabular-nums">{formatNumber(p.current_units, 2)}</p>
        </div>
        <div>
          <span className="text-xs text-slate-500">Cost basis</span>
          <p className="tabular-nums">{formatMoney(p.cost_basis, currency)}</p>
        </div>
        <div>
          <span className="text-xs text-slate-500">Market value</span>
          <p className="tabular-nums">{formatMoney(p.market_value, currency)}</p>
        </div>
        <div>
          <span className="text-xs text-slate-500">Unrealized P&L</span>
          <p className={`tabular-nums ${pnlColor}`}>
            {formatMoney(p.unrealized_pnl, currency)}
            {p.cost_basis > 0 && (
              <span className="ml-1 text-xs">
                ({formatPct(p.unrealized_pnl / p.cost_basis)})
              </span>
            )}
          </p>
        </div>
      </div>
    </Section>
  );
}

function ThesisContent({ thesis, instrumentCurrency }: { thesis: ThesisDetail; instrumentCurrency: string }) {
  // Thesis valuations are denominated in the instrument's native currency.
  const currency = instrumentCurrency;
  const t = thesis;
  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2 text-sm">
        {pill(t.stance, STANCE_COLORS)}
        <span className="text-xs text-slate-500">
          {t.thesis_type} · v{t.thesis_version}
        </span>
        {t.confidence_score !== null && (
          <span className="text-xs text-slate-500">
            Confidence: {formatNumber(t.confidence_score, 1)}
          </span>
        )}
        <span className="text-xs text-slate-400">
          {formatDateTime(t.created_at)}
        </span>
      </div>

      {/* Valuation range */}
      {(t.base_value !== null || t.bull_value !== null || t.bear_value !== null) && (
        <div className="flex gap-4 text-xs text-slate-600">
          {t.bear_value !== null && <span>Bear: {formatMoney(t.bear_value, currency)}</span>}
          {t.base_value !== null && <span>Base: {formatMoney(t.base_value, currency)}</span>}
          {t.bull_value !== null && <span>Bull: {formatMoney(t.bull_value, currency)}</span>}
        </div>
      )}

      {/* Buy zone */}
      {(t.buy_zone_low !== null || t.buy_zone_high !== null) && (
        <p className="text-xs text-slate-500">
          Buy zone: {formatMoney(t.buy_zone_low, currency)} – {formatMoney(t.buy_zone_high, currency)}
        </p>
      )}

      {/* Memo */}
      <pre className="whitespace-pre-wrap rounded bg-slate-50 p-3 text-sm text-slate-700">
        {t.memo_markdown}
      </pre>

      {/* Break conditions */}
      {t.break_conditions_json && t.break_conditions_json.length > 0 && (
        <div>
          <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            Break conditions
          </h3>
          <ul className="mt-1 list-inside list-disc text-sm text-slate-600">
            {t.break_conditions_json.map((c, i) => (
              <li key={i}>{c}</li>
            ))}
          </ul>
        </div>
      )}

      {/* Critic output */}
      {t.critic_json !== null && (
        <div className="rounded border border-amber-200 bg-amber-50 p-3">
          <h3 className="text-xs font-semibold uppercase tracking-wide text-amber-700">
            Critic
          </h3>
          <pre className="mt-1 whitespace-pre-wrap text-sm text-amber-800">
            {JSON.stringify(t.critic_json, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}

function ScoreHistoryTable({ items }: { items: ScoreHistoryItem[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-100 text-left text-xs text-slate-500">
            <th className="px-2 py-2">Date</th>
            <th className="px-2 py-2 text-right">Total</th>
            <th className="px-2 py-2 text-right">Rank</th>
            <th className="px-2 py-2 text-right">Δ</th>
            <th className="px-2 py-2 text-right">Quality</th>
            <th className="px-2 py-2 text-right">Value</th>
            <th className="px-2 py-2 text-right">Momentum</th>
            <th className="px-2 py-2 text-right">Sentiment</th>
          </tr>
        </thead>
        <tbody>
          {items.map((s, idx) => (
            <tr key={`${s.scored_at}-${idx}`} className="border-b border-slate-50">
              <td className="px-2 py-2 text-xs text-slate-500">{formatDateTime(s.scored_at)}</td>
              <td className="px-2 py-2 text-right tabular-nums">{formatNumber(s.total_score, 1)}</td>
              <td className="px-2 py-2 text-right tabular-nums">{s.rank ?? "—"}</td>
              <td className="px-2 py-2 text-right tabular-nums">
                {s.rank_delta !== null ? (s.rank_delta > 0 ? `+${s.rank_delta}` : String(s.rank_delta)) : "—"}
              </td>
              <td className="px-2 py-2 text-right tabular-nums">{formatNumber(s.quality_score, 1)}</td>
              <td className="px-2 py-2 text-right tabular-nums">{formatNumber(s.value_score, 1)}</td>
              <td className="px-2 py-2 text-right tabular-nums">{formatNumber(s.momentum_score, 1)}</td>
              <td className="px-2 py-2 text-right tabular-nums">{formatNumber(s.sentiment_score, 1)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function FilingsTable({ items }: { items: FilingItem[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-100 text-left text-xs text-slate-500">
            <th className="px-2 py-2">Date</th>
            <th className="px-2 py-2">Type</th>
            <th className="px-2 py-2">Summary</th>
            <th className="px-2 py-2 text-right">Risk</th>
            <th className="px-2 py-2">Link</th>
          </tr>
        </thead>
        <tbody>
          {items.map((f) => {
            const href = safeExternalUrl(f.source_url);
            return (
              <tr key={f.filing_event_id} className="border-b border-slate-50">
                <td className="px-2 py-2 text-xs text-slate-500">{f.filing_date}</td>
                <td className="px-2 py-2 text-xs">{f.filing_type ?? "—"}</td>
                <td className="max-w-sm truncate px-2 py-2">{f.extracted_summary ?? "—"}</td>
                <td className="px-2 py-2 text-right tabular-nums">
                  {f.red_flag_score !== null ? formatNumber(f.red_flag_score, 1) : "—"}
                </td>
                <td className="px-2 py-2">
                  {href ? (
                    <a
                      href={href}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-blue-600 hover:underline"
                    >
                      View
                    </a>
                  ) : (
                    "—"
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function NewsTable({ items }: { items: NewsItem[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-100 text-left text-xs text-slate-500">
            <th className="px-2 py-2">Time</th>
            <th className="px-2 py-2">Headline</th>
            <th className="px-2 py-2 text-right">Sentiment</th>
            <th className="px-2 py-2 text-right">Importance</th>
            <th className="px-2 py-2">Source</th>
          </tr>
        </thead>
        <tbody>
          {items.map((n) => {
            const href = safeExternalUrl(n.url);
            return (
            <tr key={n.news_event_id} className="border-b border-slate-50">
              <td className="px-2 py-2 text-xs text-slate-500">{formatDateTime(n.event_time)}</td>
              <td className="max-w-sm truncate px-2 py-2">
                {href ? (
                  <a
                    href={href}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-blue-600 hover:underline"
                  >
                    {n.headline}
                  </a>
                ) : (
                  n.headline
                )}
              </td>
              <td className="px-2 py-2 text-right tabular-nums">
                {n.sentiment_score !== null ? formatNumber(n.sentiment_score, 2) : "—"}
              </td>
              <td className="px-2 py-2 text-right tabular-nums">
                {n.importance_score !== null ? formatNumber(n.importance_score, 2) : "—"}
              </td>
              <td className="px-2 py-2 text-xs text-slate-500">{n.source ?? "—"}</td>
            </tr>
          );
          })}
        </tbody>
      </table>
    </div>
  );
}

function RecommendationsTable({ items }: { items: RecommendationListItem[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-100 text-left text-xs text-slate-500">
            <th className="px-2 py-2">Date</th>
            <th className="px-2 py-2">Action</th>
            <th className="px-2 py-2">Status</th>
            <th className="px-2 py-2">Rationale</th>
          </tr>
        </thead>
        <tbody>
          {items.map((r) => (
            <tr key={r.recommendation_id} className="border-b border-slate-50">
              <td className="px-2 py-2 text-xs text-slate-500">{formatDateTime(r.created_at)}</td>
              <td className="px-2 py-2">{pill(r.action, ACTION_COLORS)}</td>
              <td className="px-2 py-2">{pill(r.status, STATUS_COLORS)}</td>
              <td className="max-w-md truncate px-2 py-2">{r.rationale}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
