/**
 * PortfolioValueChart — total portfolio value (positions + cash) over
 * time, rendered with lightweight-charts as a single-line area series.
 * Lives on the dashboard under SummaryCards / RollingPnlStrip (#204).
 *
 * Data from GET /portfolio/value-history. The endpoint uses the
 * **live** FX snapshot for all historical conversions — documented
 * via `fx_mode` in the response and surfaced as a muted caption here
 * so an operator with a mixed-currency portfolio understands the
 * approximation.
 *
 * Silent-on-error: if the fetch fails, the whole widget hides. A
 * broken chart shouldn't blank the rest of the dashboard.
 */
import { useEffect, useRef, useState, useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import {
  AreaSeries,
  createChart,
  type IChartApi,
  type ISeriesApi,
  type Time,
  type UTCTimestamp,
} from "lightweight-charts";

import { fetchValueHistory } from "@/api/portfolio";
import type { ValueHistoryPoint, ValueHistoryRange } from "@/api/types";
import { SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { formatMoney } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

const RANGES: { id: ValueHistoryRange; label: string }[] = [
  { id: "1m", label: "1M" },
  { id: "3m", label: "3M" },
  { id: "6m", label: "6M" },
  { id: "1y", label: "1Y" },
  { id: "5y", label: "5Y" },
  { id: "max", label: "MAX" },
];

// Derived rather than maintained separately — keeps the URL-parse
// whitelist in lock-step with what's rendered.
const VALID_RANGES: readonly string[] = RANGES.map((r) => r.id);

function isValidRange(v: string | null): v is ValueHistoryRange {
  return v !== null && VALID_RANGES.includes(v);
}

/** Same format as PriceChart — UTC-midnight epoch seconds; null on any
 * unparseable input so we drop bad rows rather than poison the time
 * scale with NaN. */
function dateToTime(date: string): UTCTimestamp | null {
  const parts = date.split("-");
  if (parts.length !== 3) return null;
  const y = Number(parts[0]);
  const m = Number(parts[1]);
  const d = Number(parts[2]);
  if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(d)) return null;
  const ts = Date.UTC(y, m - 1, d);
  if (!Number.isFinite(ts)) return null;
  return (ts / 1000) as UTCTimestamp;
}

interface HoverState {
  date: string;
  value: number;
}

export function PortfolioValueChart(): JSX.Element | null {
  // `?value=` URL-sync so the dashboard operator's range choice sticks
  // across navigation — distinct from the per-instrument `?chart=` key
  // so both can coexist if we ever merge these pages.
  const [searchParams, setSearchParams] = useSearchParams();
  const rawRange = searchParams.get("value");
  const range: ValueHistoryRange = isValidRange(rawRange) ? rawRange : "1y";

  const setRange = useCallback(
    (next: ValueHistoryRange) => {
      const params = new URLSearchParams(searchParams);
      if (next === "1y") {
        params.delete("value");
      } else {
        params.set("value", next);
      }
      setSearchParams(params, { replace: true });
    },
    [searchParams, setSearchParams],
  );

  const { data, error, loading } = useAsync(
    () => fetchValueHistory(range),
    [range],
  );

  const dataMatchesRange = data?.range === range;
  const effectivelyLoading = loading || !dataMatchesRange;

  const points = dataMatchesRange && data ? data.points : null;
  const hasData =
    points !== null && points.filter((p) => dateToTime(p.date) !== null).length >= 2;

  if (error !== null) {
    // Silent-on-error: dashboard already has SummaryCards + rolling pills.
    return null;
  }

  return (
    <div className="rounded-md border border-slate-200 bg-white p-3 shadow-sm">
      <div className="flex items-center justify-between">
        <div className="flex items-baseline gap-2">
          <h2 className="text-sm font-medium text-slate-700">Portfolio value</h2>
          {data?.fx_mode === "live" ? (
            <span className="text-[10px] text-slate-400">
              historical converted at today's FX
            </span>
          ) : null}
        </div>
        <div className="flex gap-1">
          {RANGES.map((r) => (
            <button
              key={r.id}
              type="button"
              onClick={() => setRange(r.id)}
              className={`rounded px-2 py-0.5 text-xs font-medium ${
                r.id === range
                  ? "bg-slate-800 text-white"
                  : "bg-slate-100 text-slate-600 hover:bg-slate-200"
              }`}
              data-testid={`value-range-${r.id}`}
            >
              {r.label}
            </button>
          ))}
        </div>
      </div>

      {effectivelyLoading ? <SectionSkeleton rows={5} /> : null}
      {!effectivelyLoading && !hasData ? (
        <EmptyState
          title={data !== null && data.fx_skipped > 0 ? "FX rates missing" : "No history yet"}
          description={
            data !== null && data.fx_skipped > 0
              ? `${data.fx_skipped} currency pair(s) missing from today's FX snapshot — all rows in those pairs were dropped. Wait for the FX refresh job to repopulate and retry.`
              : "Not enough daily valuations to plot a line. Try a wider range, or wait for more trading days to accrue."
          }
        />
      ) : null}
      {hasData && points !== null && data !== null ? (
        <ValueCanvas points={points} currency={data.display_currency} />
      ) : null}
    </div>
  );
}

function ValueCanvas({
  points,
  currency,
}: {
  points: ValueHistoryPoint[];
  currency: string;
}): JSX.Element {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Area"> | null>(null);
  const [hover, setHover] = useState<HoverState | null>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (container === null) return;

    const chart = createChart(container, {
      autoSize: true,
      layout: {
        background: { color: "#ffffff" },
        textColor: "#64748b",
        fontSize: 11,
      },
      grid: {
        vertLines: { color: "#f1f5f9" },
        horzLines: { color: "#f1f5f9" },
      },
      rightPriceScale: { borderColor: "#e2e8f0" },
      timeScale: { borderColor: "#e2e8f0" },
      crosshair: {
        vertLine: { width: 1, color: "#94a3b8", style: 3 },
        horzLine: { width: 1, color: "#94a3b8", style: 3 },
      },
    });

    const series = chart.addSeries(AreaSeries, {
      lineColor: "#2563eb",
      topColor: "rgba(37,99,235,0.25)",
      bottomColor: "rgba(37,99,235,0.02)",
      lineWidth: 2,
    });

    chart.subscribeCrosshairMove((param) => {
      const sp = seriesRef.current;
      if (!param.time || !sp || typeof param.time !== "number") {
        setHover(null);
        return;
      }
      const pt = param.seriesData.get(sp);
      if (!pt || typeof pt !== "object" || !("value" in pt)) {
        setHover(null);
        return;
      }
      const date = new Date(param.time * 1000).toISOString().slice(0, 10);
      setHover({ date, value: (pt as { value: number }).value });
    });

    chartRef.current = chart;
    seriesRef.current = series;

    return () => {
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, []);

  useEffect(() => {
    const series = seriesRef.current;
    const chart = chartRef.current;
    if (!series || !chart) return;

    const clean = points.flatMap((p) => {
      const time = dateToTime(p.date);
      if (time === null) return [];
      return [{ time: time as Time, value: p.value }];
    });
    series.setData(clean);
    chart.timeScale().fitContent();
  }, [points]);

  return (
    <div className="relative mt-2">
      {hover !== null ? (
        <div className="absolute right-2 top-2 z-10 rounded bg-white/90 px-2 py-1 text-xs tabular-nums shadow-sm">
          <span className="text-slate-400">{hover.date}</span>
          <span className="ml-2 font-medium text-slate-700">
            {formatMoney(hover.value, currency)}
          </span>
        </div>
      ) : null}
      <div
        ref={containerRef}
        data-testid="portfolio-value-chart"
        className="h-[220px] w-full"
      />
    </div>
  );
}
