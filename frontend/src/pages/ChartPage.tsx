/**
 * /instrument/:symbol/chart — full-viewport chart workspace (#576 Phase 1).
 *
 * Provides a full-height candlestick chart with a range picker (1W–MAX)
 * URL-synced via `?range=<id>`. The back-link returns to the instrument
 * overview. Phase 2 (indicators/tooltips), Phase 3 (compare/trends), and
 * Phase 4 (raw OHLCV table) are out of scope for this PR.
 */
import { useCallback } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";

import { fetchInstrumentCandles, fetchInstrumentSummary } from "@/api/instruments";
import type { CandleRange, InstrumentCandles, InstrumentSummary } from "@/api/types";
import { ChartCanvas } from "@/components/instrument/PriceChart";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";

const RANGES: { id: CandleRange; label: string }[] = [
  { id: "1w", label: "1W" },
  { id: "1m", label: "1M" },
  { id: "3m", label: "3M" },
  { id: "6m", label: "6M" },
  { id: "1y", label: "1Y" },
  { id: "5y", label: "5Y" },
  { id: "max", label: "MAX" },
];

const VALID_RANGES: readonly CandleRange[] = ["1w", "1m", "3m", "6m", "1y", "5y", "max"];

const DEFAULT_RANGE: CandleRange = "1y";

function parseNum(v: string | null | undefined): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function dateToTime(date: string): number | null {
  const parts = date.split("-");
  if (parts.length !== 3) return null;
  const y = Number(parts[0]);
  const m = Number(parts[1]);
  const d = Number(parts[2]);
  if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(d)) return null;
  const ts = Date.UTC(y, m - 1, d);
  if (!Number.isFinite(ts)) return null;
  return ts / 1000;
}

export function ChartPage(): JSX.Element {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const [searchParams, setSearchParams] = useSearchParams();
  const rawRange = searchParams.get("range");
  const range: CandleRange = VALID_RANGES.includes(rawRange as CandleRange)
    ? (rawRange as CandleRange)
    : DEFAULT_RANGE;

  const setRange = useCallback(
    (next: CandleRange) => {
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

  const summaryAsync = useAsync<InstrumentSummary>(
    () => fetchInstrumentSummary(symbol),
    [symbol],
  );
  const candlesAsync = useAsync<InstrumentCandles>(
    () => fetchInstrumentCandles(symbol, range),
    [symbol, range],
  );

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
        parseNum(r.close) !== null &&
        dateToTime(r.date) !== null,
    ).length >= 2;

  return (
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
          <h1 className="text-xl font-semibold text-slate-800">{symbol}</h1>
          {summaryAsync.data?.identity.display_name && (
            <span className="text-sm text-slate-500">
              {summaryAsync.data.identity.display_name}
            </span>
          )}
        </div>
        {summaryAsync.data?.price?.current && (
          <span className="ml-auto text-lg font-medium tabular-nums text-slate-800">
            {summaryAsync.data.price.currency ?? ""}{" "}
            {Number(summaryAsync.data.price.current).toLocaleString(undefined, {
              maximumFractionDigits: 2,
            })}
          </span>
        )}
      </div>

      {/* Range picker */}
      <div className="flex gap-1">
        {RANGES.map((r) => (
          <button
            key={r.id}
            type="button"
            onClick={() => setRange(r.id)}
            className={`rounded px-3 py-1 text-sm font-medium ${
              r.id === range
                ? "bg-slate-800 text-white"
                : "bg-slate-100 text-slate-600 hover:bg-slate-200"
            }`}
            data-testid={`chart-range-${r.id}`}
          >
            {r.label}
          </button>
        ))}
      </div>

      {/* Chart body */}
      <div className="rounded-md border border-slate-200 bg-white shadow-sm">
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
        {!effectivelyLoading && candlesAsync.error === null && dataMatchesRange && !hasChartData ? (
          <div className="p-4">
            <EmptyState
              title="No price data"
              description="No candles in the local price_daily store for this range."
            />
          </div>
        ) : null}
        {hasChartData && rows !== null ? (
          <ChartCanvas rows={rows} symbol={symbol} containerClassName="h-[70vh] w-full" />
        ) : null}
      </div>
    </div>
  );
}

export default ChartPage;
