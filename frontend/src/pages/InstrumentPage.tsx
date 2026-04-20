/**
 * /instrument/:symbol — per-ticker research page (Phase 2.5).
 *
 * Six tabs per the 2026-04-19 research-tool refocus §2.5:
 *   1. Overview    — identity + price + key stats
 *   2. Financials  — income / balance / cashflow, quarterly / annual
 *   3. Analysis    — AI thesis (fetched on-demand)
 *   4. Positions   — held position or add-to-watchlist placeholder
 *   5. News        — recent news feed (reuses existing component)
 *   6. Filings     — SEC filings list (reuses existing component)
 *
 * MVP scope: tabs 1-3 fully wired. Positions/News/Filings show
 * placeholder messages pointing at existing pages — Phase 2.5 follow-up
 * work integrates them here once the data-loading patterns are settled.
 */

import { useState } from "react";
import { useParams } from "react-router-dom";

import {
  fetchInstrumentFinancials,
  fetchInstrumentSummary,
} from "@/api/instruments";
import { generateInstrumentThesis } from "@/api/theses";
import type {
  InstrumentFinancials,
  InstrumentSummary,
  GenerateThesisResponse,
} from "@/api/types";
import { Section, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";

function ErrorView({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  const message = error instanceof Error ? error.message : "Request failed.";
  return (
    <div className="rounded border border-red-200 bg-red-50 p-3 text-sm text-red-700">
      <p>{message}</p>
      {onRetry && (
        <button
          type="button"
          className="mt-1 text-xs underline"
          onClick={onRetry}
        >
          Retry
        </button>
      )}
    </div>
  );
}

type TabId = "overview" | "financials" | "analysis" | "positions" | "news" | "filings";

const TABS: { id: TabId; label: string }[] = [
  { id: "overview", label: "Overview" },
  { id: "financials", label: "Financials" },
  { id: "analysis", label: "Analysis" },
  { id: "positions", label: "Positions" },
  { id: "news", label: "News" },
  { id: "filings", label: "Filings" },
];

function formatDecimal(
  value: string | null | undefined,
  options: { percent?: boolean; currency?: string | null } = {},
): string {
  if (value === null || value === undefined) return "—";
  const num = Number(value);
  if (!Number.isFinite(num)) return "—";
  if (options.percent) return `${(num * 100).toFixed(2)}%`;
  const formatted = num.toLocaleString(undefined, {
    maximumFractionDigits: 2,
  });
  return options.currency ? `${options.currency} ${formatted}` : formatted;
}

function formatMarketCap(value: string | null): string {
  if (value === null) return "—";
  const num = Number(value);
  if (!Number.isFinite(num)) return "—";
  if (num >= 1e12) return `${(num / 1e12).toFixed(2)}T`;
  if (num >= 1e9) return `${(num / 1e9).toFixed(2)}B`;
  if (num >= 1e6) return `${(num / 1e6).toFixed(2)}M`;
  return num.toLocaleString();
}

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------

function Header({ summary }: { summary: InstrumentSummary }) {
  const { identity, price } = summary;
  const changePct = price?.day_change_pct ?? null;
  const changeNum = changePct !== null ? Number(changePct) : null;
  const changeColor =
    changeNum === null
      ? "text-slate-500"
      : changeNum >= 0
        ? "text-emerald-600"
        : "text-red-600";
  return (
    <div className="border-b border-slate-200 pb-4">
      <div className="flex items-baseline gap-3">
        <h1 className="text-2xl font-semibold">{identity.symbol}</h1>
        <span className="text-lg text-slate-600">
          {identity.display_name ?? "—"}
        </span>
        {summary.coverage_tier !== null && (
          <span className="rounded bg-blue-100 px-2 py-0.5 text-xs text-blue-700">
            Tier {summary.coverage_tier}
          </span>
        )}
      </div>
      <div className="mt-1 text-xs text-slate-500">
        {identity.sector ?? "—"}
        {identity.industry ? ` · ${identity.industry}` : ""}
        {identity.exchange ? ` · ${identity.exchange}` : ""}
        {identity.country ? ` · ${identity.country}` : ""}
      </div>
      {price && (
        <div className="mt-3 flex items-baseline gap-4">
          <span className="text-3xl font-semibold">
            {formatDecimal(price.current, { currency: price.currency })}
          </span>
          {price.day_change !== null && (
            <span className={`text-sm ${changeColor}`}>
              {Number(price.day_change) >= 0 ? "+" : ""}
              {formatDecimal(price.day_change)} (
              {formatDecimal(price.day_change_pct, { percent: true })})
            </span>
          )}
          <span className="text-xs text-slate-500">
            52w: {formatDecimal(price.week_52_low)} –{" "}
            {formatDecimal(price.week_52_high)}
          </span>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Overview tab
// ---------------------------------------------------------------------------

function OverviewTab({ summary }: { summary: InstrumentSummary }) {
  const stats = summary.key_stats;
  return (
    <div className="grid gap-4 md:grid-cols-2">
      <Section title="Key statistics">
        {stats === null ? (
          <EmptyState title="No key stats" description="No provider returned key stats for this ticker." />
        ) : (
          <dl className="grid grid-cols-2 gap-y-2 text-sm">
            <dt className="text-slate-500">Market cap</dt>
            <dd>{formatMarketCap(summary.identity.market_cap)}</dd>
            <dt className="text-slate-500">P/E ratio</dt>
            <dd>{formatDecimal(stats.pe_ratio)}</dd>
            <dt className="text-slate-500">P/B ratio</dt>
            <dd>{formatDecimal(stats.pb_ratio)}</dd>
            <dt className="text-slate-500">Dividend yield</dt>
            <dd>{formatDecimal(stats.dividend_yield, { percent: true })}</dd>
            <dt className="text-slate-500">Payout ratio</dt>
            <dd>{formatDecimal(stats.payout_ratio, { percent: true })}</dd>
            <dt className="text-slate-500">ROE</dt>
            <dd>{formatDecimal(stats.roe, { percent: true })}</dd>
            <dt className="text-slate-500">ROA</dt>
            <dd>{formatDecimal(stats.roa, { percent: true })}</dd>
            <dt className="text-slate-500">Debt / Equity</dt>
            <dd>{formatDecimal(stats.debt_to_equity)}</dd>
            <dt className="text-slate-500">Revenue growth (YoY)</dt>
            <dd>{formatDecimal(stats.revenue_growth_yoy, { percent: true })}</dd>
            <dt className="text-slate-500">Earnings growth (YoY)</dt>
            <dd>{formatDecimal(stats.earnings_growth_yoy, { percent: true })}</dd>
          </dl>
        )}
      </Section>

      <Section title="Source attribution">
        <ul className="space-y-1 text-xs text-slate-600">
          {Object.entries(summary.source).map(([section, provider]) => (
            <li key={section}>
              <span className="font-medium">{section}</span>: {provider}
            </li>
          ))}
        </ul>
      </Section>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Financials tab
// ---------------------------------------------------------------------------

function FinancialsTab({ symbol }: { symbol: string }) {
  const [statement, setStatement] = useState<"income" | "balance" | "cashflow">("income");
  const [period, setPeriod] = useState<"quarterly" | "annual">("quarterly");

  const { data, error, loading } = useAsync<InstrumentFinancials>(
    () => fetchInstrumentFinancials(symbol, { statement, period }),
    [symbol, statement, period],
  );

  const rows = data?.rows ?? [];
  const columns = rows.length
    ? Array.from(new Set(rows.flatMap((row) => Object.keys(row.values))))
    : [];

  return (
    <Section title={`${statement.charAt(0).toUpperCase()}${statement.slice(1)} statement`}>
      <div className="mb-3 flex gap-2 text-xs">
        <div className="flex rounded border border-slate-300">
          {(["income", "balance", "cashflow"] as const).map((s) => (
            <button
              key={s}
              type="button"
              className={`px-2 py-1 ${
                statement === s ? "bg-slate-800 text-white" : "bg-white"
              }`}
              onClick={() => setStatement(s)}
            >
              {s}
            </button>
          ))}
        </div>
        <div className="flex rounded border border-slate-300">
          {(["quarterly", "annual"] as const).map((p) => (
            <button
              key={p}
              type="button"
              className={`px-2 py-1 ${period === p ? "bg-slate-800 text-white" : "bg-white"}`}
              onClick={() => setPeriod(p)}
            >
              {p}
            </button>
          ))}
        </div>
        {data && (
          <span className="ml-auto self-end text-slate-500">
            Source: {data.source}
            {data.currency ? ` · ${data.currency}` : ""}
          </span>
        )}
      </div>

      {loading && <SectionSkeleton rows={4} />}
      {error !== null && <ErrorView error={error} />}
      {!loading && error === null && rows.length === 0 && (
        <EmptyState title="No statement data" description="Neither the local SEC XBRL cache nor yfinance returned data for this statement." />
      )}
      {!loading && !error && rows.length > 0 && (
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead>
              <tr className="border-b border-slate-200 text-left text-xs text-slate-500">
                <th className="px-2 py-1">Metric</th>
                {rows.map((row) => (
                  <th key={row.period_end} className="px-2 py-1 text-right">
                    {row.period_type}
                    <br />
                    <span className="font-normal">{row.period_end}</span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {columns.map((col) => (
                <tr key={col} className="border-b border-slate-100 last:border-0">
                  <td className="px-2 py-1 font-medium">{col}</td>
                  {rows.map((row) => (
                    <td key={row.period_end} className="px-2 py-1 text-right">
                      {formatDecimal(row.values[col] ?? null)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Section>
  );
}

// ---------------------------------------------------------------------------
// Analysis (thesis) tab
// ---------------------------------------------------------------------------

function AnalysisTab({ symbol }: { symbol: string }) {
  const [thesis, setThesis] = useState<GenerateThesisResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function generate() {
    setLoading(true);
    setError(null);
    try {
      const result = await generateInstrumentThesis(symbol);
      setThesis(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }

  return (
    <Section title="AI thesis">
      <div className="mb-3 flex items-center gap-3">
        <button
          type="button"
          className="rounded bg-blue-600 px-3 py-1 text-sm text-white disabled:opacity-50"
          onClick={generate}
          disabled={loading}
        >
          {loading ? "Generating…" : "Generate thesis"}
        </button>
        {thesis && (
          <span className="text-xs text-slate-500">
            {thesis.cached ? "Cached (24h window)" : "Freshly generated"}
          </span>
        )}
      </div>
      {error !== null && <ErrorView error={new Error(error)} />}
      {thesis && (
        <div className="space-y-3 text-sm">
          <div className="flex gap-2 text-xs">
            <span className="rounded bg-slate-100 px-2 py-0.5">
              stance: {thesis.thesis.stance}
            </span>
            <span className="rounded bg-slate-100 px-2 py-0.5">
              confidence: {thesis.thesis.confidence_score ?? "—"}
            </span>
            <span className="rounded bg-slate-100 px-2 py-0.5">
              v{thesis.thesis.thesis_version}
            </span>
          </div>
          <pre className="whitespace-pre-wrap rounded border border-slate-200 bg-slate-50 p-3 text-xs">
            {thesis.thesis.memo_markdown}
          </pre>
          {thesis.thesis.critic_json && (
            <details>
              <summary className="cursor-pointer text-xs text-slate-500">
                Critic output
              </summary>
              <pre className="mt-2 whitespace-pre-wrap rounded border border-slate-200 bg-slate-50 p-3 text-xs">
                {JSON.stringify(thesis.thesis.critic_json, null, 2)}
              </pre>
            </details>
          )}
        </div>
      )}
      {!thesis && !error && !loading && (
        <p className="text-xs text-slate-500">
          Click "Generate thesis" to produce an AI-written bull / bear analysis.
          Results are cached for 24h per ticker so repeat clicks don't spend
          on the LLM.
        </p>
      )}
    </Section>
  );
}

// ---------------------------------------------------------------------------
// Stub tabs (positions / news / filings) — deferred to follow-up work
// ---------------------------------------------------------------------------

function StubTab({ label, symbol }: { label: string; symbol: string }) {
  return (
    <Section title={label}>
      <EmptyState
        title={`${label} — coming soon`}
        description={`${label} for ${symbol} will surface here once the data-loading patterns are wired in a follow-up. See the existing pages under /instruments/:id and /filings for the current source of truth.`}
      />
    </Section>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export function InstrumentPage() {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const [activeTab, setActiveTab] = useState<TabId>("overview");

  const { data: summary, error, loading } = useAsync<InstrumentSummary>(
    () => fetchInstrumentSummary(symbol),
    [symbol],
  );

  if (loading) return <SectionSkeleton rows={4} />;
  if (error !== null) return <ErrorView error={error} />;
  if (!summary) return <EmptyState title="No data" description={`No data for ${symbol}.`} />;

  return (
    <div className="space-y-4">
      <Header summary={summary} />
      <nav className="flex gap-1 border-b border-slate-200">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            type="button"
            className={`px-3 py-2 text-sm ${
              activeTab === tab.id
                ? "border-b-2 border-blue-600 font-medium text-blue-700"
                : "text-slate-500 hover:text-slate-700"
            }`}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </nav>

      {activeTab === "overview" && <OverviewTab summary={summary} />}
      {activeTab === "financials" && <FinancialsTab symbol={symbol} />}
      {activeTab === "analysis" && <AnalysisTab symbol={symbol} />}
      {activeTab === "positions" && <StubTab label="Positions" symbol={symbol} />}
      {activeTab === "news" && <StubTab label="News" symbol={symbol} />}
      {activeTab === "filings" && <StubTab label="Filings" symbol={symbol} />}
    </div>
  );
}

export default InstrumentPage;
