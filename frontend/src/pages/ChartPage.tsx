/**
 * /instrument/:symbol/chart — full-viewport chart workspace (#576).
 *
 * Phase 1: range picker (1W–MAX), URL-synced via `?range=<id>`.
 * Phase 2: SMA/EMA indicator overlays + rich OHLC tooltip, URL-synced
 *          via `?ind=sma20,sma50,...`. Back-link returns to the instrument
 *          overview.
 * Phase 3: compare-ticker overlays (URL: `?compare=AAPL,MSFT,SPY`),
 *          linear regression line + range channel toggles
 *          (URL: `?trend=regression,channel`).
 * Phase 4: raw OHLCV table + CSV export (URL: `?view=raw`).
 * Phase 5 (#601): range table flipped to 1D/5D/1M/3M/6M/YTD/1Y/5Y/MAX.
 *          Sub-day ranges hit the live intraday endpoint; longer
 *          ranges read `price_daily`. Compare overlays are disabled
 *          on intraday ranges — fanning out N intraday fetches per
 *          chart open would burn rate budget without analytical
 *          payoff.
 */
import { useCallback, useEffect, useRef, useState, type JSX } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";

import { fetchInstrumentSummary } from "@/api/instruments";
import type { ChartRange, InstrumentSummary } from "@/api/types";
import { LiveQuoteProvider } from "@/components/quotes/LiveQuoteProvider";
import {
  ChartWorkspaceCanvas,
  INDICATOR_IDS,
  type CompareSeries,
  type IndicatorId,
} from "@/pages/components/ChartWorkspaceCanvas";
import { RawOhlcvTable } from "@/pages/components/RawOhlcvTable";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import {
  fetchChartCandles,
  isIntraday,
  type NormalisedBar,
  type NormalisedChartCandles,
} from "@/lib/chartData";
import { useAsync } from "@/lib/useAsync";

const RANGES: { id: ChartRange; label: string }[] = [
  { id: "1d", label: "1D" },
  { id: "5d", label: "5D" },
  { id: "1m", label: "1M" },
  { id: "3m", label: "3M" },
  { id: "6m", label: "6M" },
  { id: "ytd", label: "YTD" },
  { id: "1y", label: "1Y" },
  { id: "5y", label: "5Y" },
  { id: "max", label: "MAX" },
];

const VALID_RANGES: readonly ChartRange[] = [
  "1d",
  "5d",
  "1m",
  "3m",
  "6m",
  "ytd",
  "1y",
  "5y",
  "max",
];

const DEFAULT_RANGE: ChartRange = "1y";

const INDICATOR_LABELS: Record<IndicatorId, string> = {
  sma20: "SMA 20",
  sma50: "SMA 50",
  ema20: "EMA 20",
  ema50: "EMA 50",
};

const INDICATOR_COLORS: Record<IndicatorId, string> = {
  sma20: "#3b82f6",
  sma50: "#a855f7",
  ema20: "#0ea5e9",
  ema50: "#ec4899",
};

const MAX_COMPARES = 3;

type TrendId = "regression" | "channel";
const VALID_TRENDS: readonly TrendId[] = ["regression", "channel"];

function parseNum(v: string | null | undefined): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

export function ChartPage(): JSX.Element {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const [searchParams, setSearchParams] = useSearchParams();

  // Range param
  const rawRange = searchParams.get("range");
  const range: ChartRange = VALID_RANGES.includes(rawRange as ChartRange)
    ? (rawRange as ChartRange)
    : DEFAULT_RANGE;

  const setRange = useCallback(
    (next: ChartRange) => {
      const params = new URLSearchParams(searchParams);
      if (next === DEFAULT_RANGE) {
        params.delete("range");
      } else {
        params.set("range", next);
      }
      setSearchParams(params, { replace: true });
    },
    [searchParams, setSearchParams],
  );

  const intraday = isIntraday(range);

  // Indicator param — CSV of enabled indicator ids
  const indParam = searchParams.get("ind");
  const enabledIndicators: IndicatorId[] =
    indParam !== null && indParam.length > 0
      ? indParam
          .split(",")
          .filter((x): x is IndicatorId => INDICATOR_IDS.includes(x as IndicatorId))
      : [];

  const toggleIndicator = useCallback(
    (id: IndicatorId) => {
      const params = new URLSearchParams(searchParams);
      const next = enabledIndicators.includes(id)
        ? enabledIndicators.filter((x) => x !== id)
        : [...enabledIndicators, id];
      if (next.length === 0) {
        params.delete("ind");
      } else {
        params.set("ind", next.join(","));
      }
      setSearchParams(params, { replace: true });
    },
    [searchParams, setSearchParams, enabledIndicators],
  );

  // Compare param — CSV of compare symbols (max 3, uppercase, deduped).
  // Disabled on intraday ranges — operator gets the persisted compare
  // list back when they switch to a daily range.
  const compareParam = searchParams.get("compare");
  const compareSymbols: string[] = (() => {
    if (intraday) return [];
    if (compareParam === null || compareParam.length === 0) return [];
    const seen = new Set<string>();
    const out: string[] = [];
    for (const raw of compareParam.split(",")) {
      const s = raw.trim().toUpperCase();
      if (s.length === 0 || seen.has(s) || s === symbol.toUpperCase()) continue;
      seen.add(s);
      out.push(s);
      if (out.length >= MAX_COMPARES) break;
    }
    return out;
  })();

  const addCompare = useCallback(
    (sym: string) => {
      const s = sym.trim().toUpperCase();
      if (!s || s === symbol.toUpperCase()) return;
      if (compareSymbols.includes(s)) return;
      if (compareSymbols.length >= MAX_COMPARES) return;
      const next = [...compareSymbols, s];
      const params = new URLSearchParams(searchParams);
      params.set("compare", next.join(","));
      setSearchParams(params, { replace: true });
    },
    [searchParams, setSearchParams, compareSymbols, symbol],
  );

  const removeCompare = useCallback(
    (sym: string) => {
      const next = compareSymbols.filter((s) => s !== sym);
      const params = new URLSearchParams(searchParams);
      if (next.length === 0) {
        params.delete("compare");
      } else {
        params.set("compare", next.join(","));
      }
      setSearchParams(params, { replace: true });
    },
    [searchParams, setSearchParams, compareSymbols],
  );

  // Trend param — CSV of trend ids
  const trendParam = searchParams.get("trend");
  const enabledTrends: TrendId[] =
    trendParam !== null && trendParam.length > 0
      ? trendParam
          .split(",")
          .filter((x): x is TrendId => VALID_TRENDS.includes(x as TrendId))
      : [];

  const toggleTrend = useCallback(
    (id: TrendId) => {
      const params = new URLSearchParams(searchParams);
      const next = enabledTrends.includes(id)
        ? enabledTrends.filter((x) => x !== id)
        : [...enabledTrends, id];
      if (next.length === 0) {
        params.delete("trend");
      } else {
        params.set("trend", next.join(","));
      }
      setSearchParams(params, { replace: true });
    },
    [searchParams, setSearchParams, enabledTrends],
  );

  // Session-visibility toggles. Default ON (omit param). Mirrors the
  // PriceChart contract so URL params are consistent across the two
  // surfaces.
  const showPm = searchParams.get("pm") !== "0";
  const showAh = searchParams.get("ah") !== "0";

  const toggleSessionParam = useCallback(
    (key: "pm" | "ah", currentlyOn: boolean) => {
      const params = new URLSearchParams(searchParams);
      if (currentlyOn) {
        params.set(key, "0");
      } else {
        params.delete(key);
      }
      setSearchParams(params, { replace: true });
    },
    [searchParams, setSearchParams],
  );

  // View param — "chart" (default) or "raw"
  const view = searchParams.get("view") === "raw" ? "raw" : "chart";

  const setView = useCallback(
    (next: "chart" | "raw") => {
      const params = new URLSearchParams(searchParams);
      if (next === "chart") {
        params.delete("view");
      } else {
        params.set("view", next);
      }
      setSearchParams(params, { replace: true });
    },
    [searchParams, setSearchParams],
  );

  // Compare input local state.
  const [compareInput, setCompareInput] = useState("");
  const compareInputRef = useRef<HTMLInputElement | null>(null);

  const handleCompareSubmit = useCallback(() => {
    const trimmed = compareInput.trim().toUpperCase();
    if (trimmed) {
      addCompare(trimmed);
      setCompareInput("");
    }
  }, [compareInput, addCompare]);

  const handleCompareKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      if (e.key === "Enter") {
        handleCompareSubmit();
      }
    },
    [handleCompareSubmit],
  );

  // Primary candle fetch via the unified dispatch.
  const summaryAsync = useAsync<InstrumentSummary>(
    () => fetchInstrumentSummary(symbol),
    [symbol],
  );
  const candlesAsync = useAsync<NormalisedChartCandles>(
    () => fetchChartCandles(symbol, range),
    [symbol, range],
  );

  // Coarser candle-window refetch as a backstop. SSE+REST live-rate
  // polling on the backend (#602) keeps the in-progress bar fresh at
  // 5s; this just picks up rare historical bar corrections.
  useEffect(() => {
    const intervalMs = intraday ? 60_000 : 300_000;
    const id = setInterval(() => {
      candlesAsync.refetch();
    }, intervalMs);
    return () => clearInterval(id);
  }, [intraday, candlesAsync.refetch]);

  // Compare candle fetches: parallel, keyed on [range, ...compareSymbols].
  // We use a single useEffect + useState<Map> to manage compare fetches.
  const [compareData, setCompareData] = useState<Map<string, NormalisedBar[]>>(new Map());
  // Symbols whose fetch failed — shown with an error chip.
  const [compareErrors, setCompareErrors] = useState<string[]>([]);
  // Track the set of symbols + range for which we've started fetching.
  const compareFetchKeyRef = useRef<string>("");

  useEffect(() => {
    // Dedup repeat fires with the same (range + symbols) tuple. The
    // effect's deps already gate on compareSymbols/range changing, but
    // a strict-mode double-invocation or a parent re-render that
    // produces a new array reference with the same contents would
    // otherwise re-fetch unnecessarily.
    const key = [range, ...compareSymbols].join(",");
    if (key === compareFetchKeyRef.current) return;
    compareFetchKeyRef.current = key;

    if (compareSymbols.length === 0) {
      setCompareData(new Map());
      setCompareErrors([]);
      return;
    }

    let cancelled = false;
    void Promise.allSettled(
      compareSymbols.map((sym) => fetchChartCandles(sym, range)),
    ).then((results) => {
      if (cancelled) return;
      const m = new Map<string, NormalisedBar[]>();
      const failures: string[] = [];
      results.forEach((r, i) => {
        const sym = compareSymbols[i]!;
        if (r.status === "fulfilled") {
          m.set(sym, r.value.rows);
        } else {
          failures.push(sym);
        }
      });
      setCompareData(m);
      setCompareErrors(failures);
    });

    return () => {
      cancelled = true;
    };
    // compareSymbols is rebuilt each render from URL params — stable key string prevents
    // spurious re-fetches. eslint-disable needed because array identity changes each render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [range, compareParam]);

  // Build CompareSeries array for the canvas.
  const compareSeries: CompareSeries[] = compareSymbols.map((sym) => ({
    symbol: sym,
    rows: compareData.get(sym) ?? [],
  }));

  const dataMatchesRange = candlesAsync.data?.range === range;
  const effectivelyLoading = candlesAsync.loading || !dataMatchesRange;
  const rows = dataMatchesRange && candlesAsync.data ? candlesAsync.data.rows : null;
  const hasChartData =
    rows !== null &&
    rows.filter(
      (r) =>
        parseNum(r.open) !== null &&
        parseNum(r.high) !== null &&
        parseNum(r.low) !== null &&
        parseNum(r.close) !== null,
    ).length >= 2;

  // Provider needs an array; pass [] when summary hasn't loaded yet so
  // the SSE stream waits for a real id.
  const liveIds: number[] =
    summaryAsync.data?.instrument_id !== undefined
      ? [summaryAsync.data.instrument_id]
      : [];

  return (
    <LiveQuoteProvider instrumentIds={liveIds}>
    <div className="space-y-3 p-4">
      {/* Header: back link + identity + price */}
      <div className="flex items-baseline gap-3">
        <Link
          to={`/instrument/${encodeURIComponent(symbol)}`}
          className="text-xs text-sky-700 hover:underline"
        >
          ← Back to overview
        </Link>
        <div className="flex items-baseline gap-2">
          <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">{symbol}</h1>
          {summaryAsync.data?.identity.display_name && (
            <span className="text-sm text-slate-500">
              {summaryAsync.data.identity.display_name}
            </span>
          )}
        </div>
        {summaryAsync.data?.price?.current && (
          <span className="ml-auto text-lg font-medium tabular-nums text-slate-800 dark:text-slate-100">
            {summaryAsync.data.price.currency ?? ""}{" "}
            {Number(summaryAsync.data.price.current).toLocaleString(undefined, {
              maximumFractionDigits: 2,
            })}
          </span>
        )}
      </div>

      {/* Controls: range picker + view toggle + chart-only controls */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex flex-wrap gap-1">
          {RANGES.map((r) => (
            <button
              key={r.id}
              type="button"
              onClick={() => setRange(r.id)}
              className={`rounded px-3 py-1 text-sm font-medium ${
                r.id === range
                  ? "bg-slate-800 text-white"
                  : "bg-slate-100 dark:bg-slate-800 text-slate-600 hover:bg-slate-200"
              }`}
              data-testid={`chart-range-${r.id}`}
            >
              {r.label}
            </button>
          ))}
        </div>

        {/* View toggle */}
        <div className="flex gap-1">
          <button
            type="button"
            onClick={() => setView("chart")}
            className={`rounded px-3 py-1 text-sm font-medium ${
              view === "chart" ? "bg-slate-800 text-white" : "bg-slate-100 dark:bg-slate-800 text-slate-600 hover:bg-slate-200"
            }`}
            data-testid="view-chart"
          >
            Chart
          </button>
          <button
            type="button"
            onClick={() => setView("raw")}
            className={`rounded px-3 py-1 text-sm font-medium ${
              view === "raw" ? "bg-slate-800 text-white" : "bg-slate-100 dark:bg-slate-800 text-slate-600 hover:bg-slate-200"
            }`}
            data-testid="view-raw"
          >
            Raw data
          </button>
        </div>

        {/* Chart-only: indicator toggles */}
        {view === "chart" && (
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-[11px] uppercase tracking-wider text-slate-500">Indicators</span>
            {INDICATOR_IDS.map((id) => {
              const active = enabledIndicators.includes(id);
              return (
                <button
                  key={id}
                  type="button"
                  onClick={() => toggleIndicator(id)}
                  className={`rounded border px-2 py-0.5 text-xs font-medium ${
                    active
                      ? "bg-white dark:bg-slate-900 text-slate-700"
                      : "bg-slate-50 text-slate-500 hover:bg-slate-100 dark:bg-slate-900 dark:hover:bg-slate-800"
                  }`}
                  style={
                    active
                      ? { borderColor: INDICATOR_COLORS[id], color: INDICATOR_COLORS[id] }
                      : { borderColor: "#e2e8f0" }
                  }
                  data-testid={`indicator-${id}`}
                >
                  {INDICATOR_LABELS[id]}
                </button>
              );
            })}
          </div>
        )}

        {/* Chart-only: trend toggles */}
        {view === "chart" && (
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-[11px] uppercase tracking-wider text-slate-500">Trend</span>
            {(["regression", "channel"] as const).map((id) => {
              const active = enabledTrends.includes(id);
              const label = id === "regression" ? "Regression" : "Range";
              return (
                <button
                  key={id}
                  type="button"
                  onClick={() => toggleTrend(id)}
                  className={`rounded border px-2 py-0.5 text-xs font-medium ${
                    active
                      ? "border-orange-400 bg-white dark:bg-slate-900 text-orange-600"
                      : "border-slate-200 dark:border-slate-800 bg-slate-50 text-slate-500 hover:bg-slate-100 dark:bg-slate-900 dark:hover:bg-slate-800"
                  }`}
                  data-testid={`trend-${id}`}
                >
                  {label}
                </button>
              );
            })}
          </div>
        )}

        {/* Chart-only, intraday-only: session-visibility + previous-close
            toggles. Mirrors PriceChart for cross-surface consistency. */}
        {view === "chart" && intraday && (
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-[11px] uppercase tracking-wider text-slate-500">Session</span>
            {(
              [
                { key: "pm", on: showPm, label: "PM", title: "Pre-market 04:00–09:30 ET" },
                { key: "ah", on: showAh, label: "AH", title: "After-hours 16:00–20:00 ET" },
              ] as const
            ).map(({ key, on, label, title }) => (
              <button
                key={key}
                type="button"
                onClick={() => toggleSessionParam(key, on)}
                aria-pressed={on}
                title={title}
                className={`rounded border px-2 py-0.5 text-xs font-medium ${
                  on
                    ? "border-slate-400 bg-white dark:bg-slate-900 text-slate-700"
                    : "border-slate-200 dark:border-slate-800 bg-slate-50 text-slate-500 hover:bg-slate-100 dark:bg-slate-900 dark:hover:bg-slate-800"
                }`}
                data-testid={`session-toggle-${key}`}
              >
                {label}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Chart-only: compare row. Hidden on intraday ranges — fanning
          out N intraday fetches per chart open would burn external
          quota without analytical payoff at sub-day timeframes. */}
      {view === "chart" && !intraday && (
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-[11px] uppercase tracking-wider text-slate-500">Compare</span>
          {compareSymbols.map((sym) => {
            const hasFailed = compareErrors.includes(sym);
            return (
              <span
                key={sym}
                className={`inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs font-medium ${
                  hasFailed
                    ? "border-red-400 bg-red-50 text-red-700"
                    : "border-slate-300 dark:border-slate-700 bg-slate-50 dark:bg-slate-900/40 text-slate-700"
                }`}
                title={hasFailed ? "Failed to fetch — check ticker" : undefined}
                aria-label={hasFailed ? `${sym} — failed to fetch` : sym}
                data-testid={`compare-chip-${sym}`}
                data-error={hasFailed ? "true" : undefined}
              >
                {sym}
                <button
                  type="button"
                  aria-label={`Remove ${sym}`}
                  onClick={() => removeCompare(sym)}
                  className={`ml-0.5 rounded-full ${hasFailed ? "text-red-400 hover:text-red-600" : "text-slate-400 hover:text-slate-600"}`}
                  data-testid={`compare-remove-${sym}`}
                >
                  ×
                </button>
              </span>
            );
          })}
          {compareSymbols.length < MAX_COMPARES && (
            <input
              ref={compareInputRef}
              type="text"
              value={compareInput}
              onChange={(e) => setCompareInput(e.target.value)}
              onKeyDown={handleCompareKeyDown}
              placeholder="Add ticker to compare..."
              className="rounded border border-slate-200 bg-white px-2 py-0.5 text-xs text-slate-700 placeholder-slate-400 focus:border-slate-400 focus:outline-none dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:placeholder-slate-500"
              data-testid="compare-input"
            />
          )}
          {compareSymbols.length >= MAX_COMPARES && (
            <span className="text-[11px] text-slate-400">Max {MAX_COMPARES} symbols</span>
          )}
        </div>
      )}

      {/* Body: chart or raw table */}
      <div className="border-t border-slate-200 dark:border-slate-800 pt-3">
        {effectivelyLoading && candlesAsync.error === null ? (
          <div className="p-4">
            <SectionSkeleton rows={10} />
          </div>
        ) : null}
        {candlesAsync.error !== null ? (
          <div className="p-4">
            <SectionError onRetry={candlesAsync.refetch} />
          </div>
        ) : null}
        {!effectivelyLoading && candlesAsync.error === null && dataMatchesRange && view === "chart" && !hasChartData ? (
          <div className="p-4">
            <EmptyState
              title="No price data"
              description={
                intraday
                  ? "No intraday bars from the provider for this range."
                  : "No candles in the local price_daily store for this range."
              }
            />
          </div>
        ) : null}
        {view === "chart" && hasChartData && rows !== null ? (
          <ChartWorkspaceCanvas
            rows={rows}
            symbol={symbol}
            instrumentId={summaryAsync.data?.instrument_id ?? null}
            range={range}
            indicators={enabledIndicators}
            compares={compareSeries}
            showRegression={enabledTrends.includes("regression")}
            showChannel={enabledTrends.includes("channel")}
            intraday={intraday}
            showPm={showPm}
            showAh={showAh}
            containerClassName="h-[70vh] w-full"
          />
        ) : null}
        {view === "raw" && !effectivelyLoading && candlesAsync.error === null && dataMatchesRange ? (
          <RawOhlcvTable rows={rows ?? []} symbol={symbol} range={range} intraday={intraday} />
        ) : null}
      </div>
    </div>
    </LiveQuoteProvider>
  );
}

export default ChartPage;
