/**
 * /instrument/:symbol — per-stock research page.
 *
 * Layout after Slice 1 of the per-stock research page spec
 * (docs/superpowers/specs/2026-04-20-per-stock-research-page.md):
 *   - Sticky SummaryStrip (identity + price + thesis/score/held badges + actions)
 *   - Tabs: Research (default) · Financials · Positions · News · Filings
 *   - Research tab replaces the old Overview tab; key stats + thesis memo
 *     live there so the operator lands on "is this worth owning?".
 *   - The old Analysis tab is gone — its Generate-thesis button moved
 *     into the SummaryStrip and the memo renders inside Research.
 *
 * The right rail (filings + peer + news preview) ships in Slice 2.
 */

import { useCallback, useEffect, useState } from "react";
import { useParams, useSearchParams } from "react-router-dom";

import { fetchFilings } from "@/api/filings";
import {
  fetchInstrumentFinancials,
  fetchInstrumentSummary,
} from "@/api/instruments";
import { fetchNews } from "@/api/news";
import { fetchInstrumentPositions } from "@/api/portfolio";
import {
  fetchLatestThesis,
  generateInstrumentThesis,
} from "@/api/theses";
import { ApiError } from "@/api/client";
import type {
  FilingsListResponse,
  InstrumentFinancials,
  InstrumentPositionDetail,
  InstrumentSummary,
  NewsListResponse,
  ThesisDetail,
} from "@/api/types";
import { ClosePositionModal } from "@/components/orders/ClosePositionModal";
import { OrderEntryModal } from "@/components/orders/OrderEntryModal";
import { Section, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { PriceChart } from "@/components/instrument/PriceChart";
import { ResearchTab } from "@/components/instrument/ResearchTab";
import { RightRail } from "@/components/instrument/RightRail";
import { SummaryStrip } from "@/components/instrument/SummaryStrip";
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

type TabId = "research" | "financials" | "positions" | "news" | "filings";

const TABS: { id: TabId; label: string }[] = [
  { id: "research", label: "Research" },
  { id: "financials", label: "Financials" },
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

// Header + Overview tab removed in Slice 1 — replaced by
// `components/instrument/SummaryStrip.tsx` (sticky strip) and
// `components/instrument/ResearchTab.tsx` (Research tab content).
// `formatDecimal` is still used by the Financials tab below;
// `formatMarketCap` moved to ResearchTab.

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
  // Collect the column set across all rows (periods may report different
  // concepts), then sort alphabetically so the ordering is stable across
  // re-fetches regardless of backend key order.
  const columns = rows.length
    ? Array.from(new Set(rows.flatMap((row) => Object.keys(row.values)))).sort()
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
        <EmptyState title="No statement data" description="The local SEC XBRL cache has no data for this statement." />
      )}
      {!loading && error === null && rows.length > 0 && (
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

// Analysis tab retired in Slice 1. The Generate-thesis button moved to
// `SummaryStrip`; the memo renders inside `ResearchTab`. Thesis history
// lives in a dedicated Thesis tab (landing in a follow-up slice).

// ---------------------------------------------------------------------------
// Stub tabs (positions / news / filings) — deferred to follow-up work
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Positions tab
// ---------------------------------------------------------------------------

function PositionsTab({ symbol, instrumentId }: { symbol: string; instrumentId: number }) {
  // Use the per-instrument endpoint so we never silently false-negative on a
  // paginated portfolio list (Codex review feedback on PR #366).
  const { data, error, loading } = useAsync<InstrumentPositionDetail>(
    () => fetchInstrumentPositions(instrumentId),
    [instrumentId],
  );

  if (loading) return <SectionSkeleton rows={3} />;
  if (error !== null) return <ErrorView error={error} />;
  if (!data || data.total_units === 0) {
    return (
      <Section title="Position">
        <EmptyState
          title="Not held"
          description={`You don't currently hold ${symbol}.`}
        />
      </Section>
    );
  }

  const pnlColor =
    data.total_pnl > 0
      ? "text-emerald-600"
      : data.total_pnl < 0
        ? "text-red-600"
        : "text-slate-600";

  return (
    <Section title="Position">
      <dl className="grid grid-cols-2 gap-y-2 text-sm md:grid-cols-4">
        <dt className="text-slate-500">Units</dt>
        <dd>{data.total_units.toLocaleString()}</dd>
        <dt className="text-slate-500">Avg entry</dt>
        <dd>{data.avg_entry !== null ? data.avg_entry.toFixed(2) : "—"}</dd>
        <dt className="text-slate-500">Current price</dt>
        <dd>{data.current_price !== null ? data.current_price.toFixed(2) : "—"}</dd>
        <dt className="text-slate-500">Currency</dt>
        <dd className="text-xs text-slate-500">{data.currency}</dd>
        <dt className="text-slate-500">Total invested</dt>
        <dd>{data.total_invested.toFixed(2)}</dd>
        <dt className="text-slate-500">Market value</dt>
        <dd>{data.total_value.toFixed(2)}</dd>
        <dt className="text-slate-500">Unrealised P&amp;L</dt>
        <dd className={pnlColor}>
          {data.total_pnl >= 0 ? "+" : ""}
          {data.total_pnl.toFixed(2)}
        </dd>
        <dt className="text-slate-500">Trades</dt>
        <dd>{data.trades.length}</dd>
      </dl>
    </Section>
  );
}

// ---------------------------------------------------------------------------
// News tab
// ---------------------------------------------------------------------------

function sentimentBadge(score: number | null) {
  if (score === null) return null;
  // Match sign prefix to colour bucket so a neutral-grey badge never
  // shows a "+" prefix (Codex feedback).
  const positive = score > 0.2;
  const negative = score < -0.2;
  const color = positive
    ? "bg-emerald-100 text-emerald-700"
    : negative
      ? "bg-red-100 text-red-700"
      : "bg-slate-100 text-slate-600";
  const prefix = positive ? "+" : negative ? "" : "";
  return (
    <span className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${color}`}>
      {prefix}
      {score.toFixed(2)}
    </span>
  );
}

function NewsTab({ instrumentId }: { instrumentId: number }) {
  const { data, error, loading } = useAsync<NewsListResponse>(
    () => fetchNews(instrumentId, 0, 25),
    [instrumentId],
  );

  if (loading) return <SectionSkeleton rows={5} />;
  if (error !== null) return <ErrorView error={error} />;
  if (!data || data.items.length === 0) {
    return (
      <Section title="News">
        <EmptyState
          title="No news yet"
          description="News events appear once the news feed has been ingested for this instrument."
        />
      </Section>
    );
  }

  return (
    <Section title={`News (${data.total})`}>
      <ul className="space-y-3 text-sm">
        {data.items.map((n) => (
          <li key={n.news_event_id} className="border-b border-slate-100 pb-2 last:border-0">
            <div className="flex items-baseline gap-2">
              <span className="text-xs text-slate-500">{n.event_time.slice(0, 10)}</span>
              {n.source && <span className="text-xs text-slate-500">· {n.source}</span>}
              {sentimentBadge(n.sentiment_score)}
              {n.category && (
                <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] text-slate-600">
                  {n.category}
                </span>
              )}
            </div>
            <div className="mt-0.5">
              {n.url ? (
                <a
                  href={n.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="font-medium text-blue-700 hover:underline"
                >
                  {n.headline}
                </a>
              ) : (
                <span className="font-medium">{n.headline}</span>
              )}
            </div>
            {n.snippet && <p className="text-xs text-slate-600">{n.snippet}</p>}
          </li>
        ))}
      </ul>
    </Section>
  );
}

// ---------------------------------------------------------------------------
// Filings tab
// ---------------------------------------------------------------------------

function redFlagBadge(score: number | null) {
  if (score === null) return null;
  const color =
    score > 0.5
      ? "bg-red-100 text-red-700"
      : score > 0.2
        ? "bg-amber-100 text-amber-700"
        : "bg-slate-100 text-slate-600";
  return (
    <span className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${color}`}>
      red-flag {score.toFixed(2)}
    </span>
  );
}

function FilingsTab({ instrumentId }: { instrumentId: number }) {
  const { data, error, loading } = useAsync<FilingsListResponse>(
    () => fetchFilings(instrumentId, 0, 25),
    [instrumentId],
  );

  if (loading) return <SectionSkeleton rows={5} />;
  if (error !== null) return <ErrorView error={error} />;
  if (!data || data.items.length === 0) {
    return (
      <Section title="Filings">
        <EmptyState
          title="No filings"
          description="Filings appear once SEC EDGAR or Companies House has been crawled for this instrument."
        />
      </Section>
    );
  }

  return (
    <Section title={`Filings (${data.total})`}>
      <ul className="space-y-3 text-sm">
        {data.items.map((f) => (
          <li key={f.filing_event_id} className="border-b border-slate-100 pb-2 last:border-0">
            <div className="flex items-baseline gap-2">
              <span className="text-xs text-slate-500">{f.filing_date}</span>
              <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] text-slate-600">
                {f.filing_type ?? "?"}
              </span>
              <span className="text-xs text-slate-500">{f.provider}</span>
              {redFlagBadge(f.red_flag_score)}
            </div>
            {f.extracted_summary && (
              <p className="mt-1 text-xs text-slate-600">{f.extracted_summary}</p>
            )}
            <div className="mt-1 flex gap-3 text-xs">
              {f.primary_document_url && (
                <a
                  href={f.primary_document_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-blue-700 hover:underline"
                >
                  document
                </a>
              )}
              {f.source_url && f.source_url !== f.primary_document_url && (
                <a
                  href={f.source_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-blue-700 hover:underline"
                >
                  index
                </a>
              )}
            </div>
          </li>
        ))}
      </ul>
    </Section>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export function InstrumentPage() {
  const { symbol = "" } = useParams<{ symbol: string }>();

  const summaryAsync = useAsync<InstrumentSummary>(
    () => fetchInstrumentSummary(symbol),
    [symbol],
  );

  if (summaryAsync.loading) return <SectionSkeleton rows={4} />;
  if (summaryAsync.error !== null) return <ErrorView error={summaryAsync.error} />;
  if (!summaryAsync.data)
    return <EmptyState title="No data" description={`No data for ${symbol}.`} />;

  // Per-instrument state (thesis, position, tab, modals) lives in a
  // child so its useAsync hooks only fire when we have a real
  // `instrument_id`. Without this split, the parent's hooks would run
  // once with `instrumentId=null` and settle (data=null, loading=false)
  // before the id became known, creating a brief "loaded + null"
  // window where the Generate-thesis gate would misfire (Codex slice-1
  // round-2 feedback).
  return <InstrumentPageBody summary={summaryAsync.data} symbol={symbol} />;
}

function InstrumentPageBody({
  summary,
  symbol,
}: {
  summary: InstrumentSummary;
  symbol: string;
}): JSX.Element {
  const instrumentId = summary.instrument_id;

  // Tab state lives in the URL so dashboard/portfolio drill-ins can
  // preselect the Positions tab via `?tab=positions` (Slice 3 of
  // per-stock research spec). `replace: true` on the setter so
  // tab-switching doesn't spam browser history.
  const [searchParams, setSearchParams] = useSearchParams();
  const tabParam = searchParams.get("tab");
  const activeTab: TabId = TABS.some((t) => t.id === tabParam)
    ? (tabParam as TabId)
    : "research";
  const setActiveTab = useCallback(
    (next: TabId) => {
      const params = new URLSearchParams(searchParams);
      if (next === "research") {
        params.delete("tab");
      } else {
        params.set("tab", next);
      }
      setSearchParams(params, { replace: true });
    },
    [searchParams, setSearchParams],
  );

  const thesisAsync = useAsync<ThesisDetail | null>(
    async () => {
      try {
        return await fetchLatestThesis(instrumentId);
      } catch (err) {
        if (err instanceof ApiError && err.status === 404) return null;
        throw err;
      }
    },
    [instrumentId],
  );

  const positionAsync = useAsync<InstrumentPositionDetail | null>(
    async () => {
      try {
        return await fetchInstrumentPositions(instrumentId);
      } catch (err) {
        if (err instanceof ApiError && err.status === 404) return null;
        throw err;
      }
    },
    [instrumentId],
  );

  const [addOpen, setAddOpen] = useState(false);
  const [closeOpen, setCloseOpen] = useState(false);
  // Capture the close target at click time so a mid-flight refetch
  // clearing `positionAsync.data` can't unmount an open modal
  // (Codex slice-1 round-3 finding).
  const [closeTarget, setCloseTarget] = useState<{
    positionId: number;
  } | null>(null);
  const [thesisBusy, setThesisBusy] = useState(false);
  const [thesisErr, setThesisErr] = useState<string | null>(null);

  // Sticky error flags. `useAsync.refetch()` clears `error` to null at
  // the start of the next run, which would briefly hide the error
  // badge + retry affordance. Keep a sticky bit that only clears when
  // the next fetch settles cleanly (non-loading + non-error).
  const [thesisErrSticky, setThesisErrSticky] = useState(false);
  const [positionErrSticky, setPositionErrSticky] = useState(false);
  useEffect(() => {
    if (thesisAsync.error !== null) setThesisErrSticky(true);
    else if (!thesisAsync.loading) setThesisErrSticky(false);
  }, [thesisAsync.error, thesisAsync.loading]);
  useEffect(() => {
    if (positionAsync.error !== null) setPositionErrSticky(true);
    else if (!positionAsync.loading) setPositionErrSticky(false);
  }, [positionAsync.error, positionAsync.loading]);

  async function handleGenerateThesis() {
    setThesisBusy(true);
    setThesisErr(null);
    try {
      await generateInstrumentThesis(symbol);
      thesisAsync.refetch();
    } catch (err) {
      setThesisErr(err instanceof Error ? err.message : String(err));
    } finally {
      setThesisBusy(false);
    }
  }

  function handleFilled() {
    setAddOpen(false);
    setCloseOpen(false);
    setCloseTarget(null);
    positionAsync.refetch();
  }

  const position = positionAsync.data;
  const singleTrade =
    position !== null && position.trades.length === 1
      ? position.trades[0]
      : null;

  function handleCloseClick() {
    if (singleTrade === null || singleTrade === undefined) return;
    setCloseTarget({ positionId: singleTrade.position_id });
    setCloseOpen(true);
  }

  return (
    <div className="space-y-4">
      <SummaryStrip
        summary={summary}
        thesis={thesisAsync.data}
        // `thesisLoaded=true` iff fetch settled cleanly (not errored,
        // even historically) AND not currently reloading.
        thesisLoaded={
          !thesisAsync.loading &&
          thesisAsync.error === null &&
          !thesisErrSticky
        }
        thesisError={thesisErrSticky}
        position={position}
        positionLoaded={
          !positionAsync.loading &&
          positionAsync.error === null &&
          !positionErrSticky
        }
        positionError={positionErrSticky}
        onAdd={() => setAddOpen(true)}
        onClose={handleCloseClick}
        onGenerateThesis={handleGenerateThesis}
        generatingThesis={thesisBusy}
      />
      {thesisErr !== null ? (
        <div
          role="status"
          className="rounded border border-red-200 bg-red-50 px-3 py-1.5 text-xs text-red-700"
        >
          Thesis generation failed: {thesisErr}
        </div>
      ) : null}
      {/* 8/12 + 4/12 split: tab content left, right rail right. Right
          rail is persistent across tab changes per the spec — filings
          / peer / news are always in the operator's peripheral view. */}
      <div className="grid gap-4 lg:grid-cols-12">
        <div className="space-y-4 lg:col-span-8">
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

          {activeTab === "research" && (
            <div className="space-y-4">
              {/* Chart sits at the top of Research — the operator
                  lands on the tab and sees price context before
                  drilling into thesis + stats. Slice B of #316. */}
              <div className="rounded-md border border-slate-200 bg-white p-3 shadow-sm">
                <PriceChart symbol={symbol} />
              </div>
              <ResearchTab
                summary={summary}
                thesis={thesisAsync.data}
                thesisErrored={thesisErrSticky}
              />
            </div>
          )}
          {activeTab === "financials" && <FinancialsTab symbol={symbol} />}
          {activeTab === "positions" && (
            <PositionsTab symbol={symbol} instrumentId={summary.instrument_id} />
          )}
          {activeTab === "news" && <NewsTab instrumentId={summary.instrument_id} />}
          {activeTab === "filings" && (
            <FilingsTab instrumentId={summary.instrument_id} />
          )}
        </div>
        <div className="lg:col-span-4">
          <RightRail
            instrumentId={summary.instrument_id}
            sector={summary.identity.sector}
            currentSymbol={summary.identity.symbol}
          />
        </div>
      </div>

      {addOpen ? (
        <OrderEntryModal
          isOpen
          instrumentId={summary.instrument_id}
          symbol={summary.identity.symbol}
          companyName={summary.identity.display_name ?? summary.identity.symbol}
          valuationSource="quote"
          onRequestClose={() => setAddOpen(false)}
          onFilled={handleFilled}
        />
      ) : null}

      {closeOpen && closeTarget !== null ? (
        <ClosePositionModal
          isOpen
          instrumentId={summary.instrument_id}
          positionId={closeTarget.positionId}
          valuationSource="quote"
          onRequestClose={() => {
            setCloseOpen(false);
            setCloseTarget(null);
          }}
          onFilled={handleFilled}
        />
      ) : null}
    </div>
  );
}

export default InstrumentPage;
