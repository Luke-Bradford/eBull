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
  createSeriesMarkers,
  type IChartApi,
  type ISeriesApi,
  type SeriesMarker,
  type Time,
  type UTCTimestamp,
} from "lightweight-charts";

import { fetchValueHistory } from "@/api/portfolio";
import type { ValueHistoryEvent, ValueHistoryPoint, ValueHistoryRange } from "@/api/types";
import { SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { formatMoney } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";
import { useChartTheme } from "@/lib/useChartTheme";

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
  trades: ValueHistoryEvent[];
  /** Crosshair px coordinates relative to the chart container — the
   *  tooltip follows the cursor (a corner-pinned overlay reads as
   *  disconnected from the hovered point). */
  x: number;
  y: number;
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
  const validPoints =
    points !== null ? points.filter((p) => dateToTime(p.date) !== null) : null;
  const hasData = validPoints !== null && validPoints.length >= 2;

  // Chart is meaningful only when there are ≥2 points AND at least
  // one diverges from the first — demo eToro collapses to cash-only
  // flat, fresh accounts collapse to a single-point series, both
  // produce noise. Preserved branches:
  //   - fx_skipped > 0  → show the "FX rates missing" empty state so
  //                       the operator knows why values are absent.
  //   - loading         → show skeleton (before data arrives).
  // Every other no-signal state silent-hides the whole card.
  const hasMovement =
    hasData && validPoints.some((p) => p.value !== validPoints[0]!.value);
  const fxSkipped = data?.fx_skipped ?? 0;

  if (error !== null) return null;
  if (!effectivelyLoading && !hasMovement && fxSkipped === 0) return null;

  return (
    <div className="border-t border-slate-200 dark:border-slate-800 pt-3">
      <div className="flex items-baseline justify-between">
        <div className="flex items-baseline gap-2">
          <h2 className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate-700">
            Portfolio value
          </h2>
          {/* Two mutually-exclusive FX signals:
              - caption  → fine state (live FX applied cleanly)
              - badge    → partial state (some pairs dropped)
              When both conditions match we keep the badge only,
              since it already implies the live-FX context and the
              caption would just duplicate. */}
          {data?.fx_mode === "live" && hasMovement && fxSkipped === 0 ? (
            <span className="text-[10px] text-slate-400 dark:text-slate-500">
              historical converted at today's FX · excludes copy-portfolio equity
            </span>
          ) : null}
          {/* Keep the FX-missing signal even when the chart has
              real movement. Without this the operator only sees the
              warning when the series is *entirely* dropped, hiding
              partial-coverage from view. */}
          {fxSkipped > 0 && hasMovement ? (
            <span
              className="text-[10px] text-amber-700"
              data-testid="value-fx-missing-badge"
            >
              {fxSkipped} FX pair(s) missing — some rows dropped
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
                  : "bg-slate-100 dark:bg-slate-800 text-slate-600 hover:bg-slate-200"
              }`}
              data-testid={`value-range-${r.id}`}
            >
              {r.label}
            </button>
          ))}
        </div>
      </div>

      {effectivelyLoading ? <SectionSkeleton rows={5} /> : null}
      {!effectivelyLoading && fxSkipped > 0 && !hasMovement ? (
        <EmptyState
          title="FX rates missing"
          description={`${fxSkipped} currency pair(s) missing from today's FX snapshot — all rows in those pairs were dropped. Wait for the FX refresh job to repopulate and retry.`}
        />
      ) : null}
      {hasMovement && validPoints !== null && data !== null ? (
        // Pass the date-filtered array so the canvas and the movement
        // guard share the same view of the series. The canvas would
        // still re-filter internally, but passing `points` raw means
        // two different "what counts as a row" rules in the same file.
        <ValueCanvas points={validPoints} events={data.events} currency={data.display_currency} />
      ) : null}
    </div>
  );
}

function ValueCanvas({
  points,
  events,
  currency,
}: {
  points: ValueHistoryPoint[];
  events: ValueHistoryEvent[];
  currency: string;
}): JSX.Element {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Area"> | null>(null);
  const markersRef = useRef<ReturnType<typeof createSeriesMarkers<Time>> | null>(null);
  // The crosshair callback reads events through a ref so the run-once
  // construction effect never closes over a stale array (the
  // subscription outlives every refetch).
  const eventsRef = useRef<ValueHistoryEvent[]>(events);
  eventsRef.current = events;
  const [hover, setHover] = useState<HoverState | null>(null);
  const theme = useChartTheme();

  useEffect(() => {
    const container = containerRef.current;
    if (container === null) return;

    // Theme values here are construction-time only; the applyOptions
    // effect below re-applies them on light/dark toggle (PriceChart
    // pattern — recreating the chart would drop pan/zoom).
    const chart = createChart(container, {
      autoSize: true,
      layout: {
        background: { color: theme.bg },
        textColor: theme.textSecondary,
        fontSize: 11,
      },
      grid: {
        vertLines: { color: theme.gridLine },
        horzLines: { color: theme.gridLine },
      },
      rightPriceScale: { borderColor: theme.borderColor },
      timeScale: { borderColor: theme.borderColor },
      crosshair: {
        vertLine: { width: 1, color: theme.crosshair, style: 3 },
        horzLine: { width: 1, color: theme.crosshair, style: 3 },
      },
    });

    const series = chart.addSeries(AreaSeries, {
      lineColor: theme.accent[1],
      topColor: theme.areaTopAlpha,
      bottomColor: theme.areaBottomAlpha,
      lineWidth: 2,
    });

    // Assign refs BEFORE subscribing so the callback can never fire
    // against a null seriesRef in the same effect tick (latent-but-
    // practically-unreachable race the bot flagged).
    chartRef.current = chart;
    seriesRef.current = series;

    chart.subscribeCrosshairMove((param) => {
      const sp = seriesRef.current;
      if (!param.time || !sp || typeof param.time !== "number" || !param.point) {
        setHover(null);
        return;
      }
      const pt = param.seriesData.get(sp);
      if (!pt || typeof pt !== "object" || !("value" in pt)) {
        setHover(null);
        return;
      }
      const date = new Date(param.time * 1000).toISOString().slice(0, 10);
      setHover({
        date,
        value: (pt as { value: number }).value,
        trades: eventsRef.current.filter((e) => e.date === date),
        x: param.point.x,
        y: param.point.y,
      });
    });

    return () => {
      markersRef.current?.detach();
      markersRef.current = null;
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, []);

  // Re-apply theme-driven options on light/dark toggle (PriceChart /
  // InsiderPriceMarkers pattern — applyOptions, never recreate).
  useEffect(() => {
    const chart = chartRef.current;
    const series = seriesRef.current;
    if (!chart || !series) return;
    chart.applyOptions({
      layout: { background: { color: theme.bg }, textColor: theme.textSecondary },
      grid: {
        vertLines: { color: theme.gridLine },
        horzLines: { color: theme.gridLine },
      },
      rightPriceScale: { borderColor: theme.borderColor },
      timeScale: { borderColor: theme.borderColor },
      crosshair: {
        vertLine: { color: theme.crosshair },
        horzLine: { color: theme.crosshair },
      },
    });
    series.applyOptions({
      lineColor: theme.accent[1],
      topColor: theme.areaTopAlpha,
      bottomColor: theme.areaBottomAlpha,
    });
  }, [theme]);

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

  // Buy/sell bubbles (#1594): one marker per (day, side) — BUY below
  // the bar in `up` green, SELL above in `down` red. The symbol ×
  // units detail lives in the hover overlay, not marker text — 60+
  // position opens as permanent labels would bury the line. detach +
  // re-attach so marker state never accumulates (InsiderPriceMarkers
  // pattern).
  useEffect(() => {
    const series = seriesRef.current;
    if (!series) return;
    const byDay = new Map<string, { buys: boolean; sells: boolean }>();
    for (const e of events) {
      const entry = byDay.get(e.date) ?? { buys: false, sells: false };
      if (e.side === "BUY") entry.buys = true;
      else entry.sells = true;
      byDay.set(e.date, entry);
    }
    const markers: SeriesMarker<Time>[] = [];
    for (const [date, { buys, sells }] of byDay) {
      const time = dateToTime(date);
      if (time === null) continue;
      if (buys) {
        markers.push({ time: time as Time, position: "belowBar", color: theme.up, shape: "circle", size: 1 });
      }
      if (sells) {
        markers.push({ time: time as Time, position: "aboveBar", color: theme.down, shape: "circle", size: 1 });
      }
    }
    markers.sort((a, b) => (a.time as number) - (b.time as number));
    markersRef.current?.detach();
    markersRef.current = createSeriesMarkers(series, markers);
    // `theme` intentionally NOT in deps: marker colours are theme.up /
    // theme.down, identical across light and dark (saturated palette).
  }, [events]);

  // Tooltip placement: offset right of the crosshair, flipped left
  // when it would clip the container edge, vertically clamped.
  // pointer-events-none keeps the tooltip out of hit-testing — it can
  // never sit between the cursor and the chart, so hover stays stable
  // (the previous corner-pinned card also read as disconnected from
  // the hovered point; operator feedback 2026-06-13).
  const TIP_W = 200;
  const TIP_OFFSET = 14;
  const containerWidth = containerRef.current?.clientWidth ?? 0;
  const tipLeft =
    hover !== null
      ? hover.x + TIP_OFFSET + TIP_W > containerWidth && hover.x - TIP_OFFSET - TIP_W > 0
        ? hover.x - TIP_OFFSET - TIP_W
        : hover.x + TIP_OFFSET
      : 0;
  // 220 = the container's h-[220px]; 110 ≈ tallest card (date row +
  // 4 trade lines) so a marker-day tooltip never clips the bottom.
  const tipTop = hover !== null ? Math.max(4, Math.min(hover.y - 12, 220 - 110)) : 0;

  return (
    <div className="relative mt-2">
      {hover !== null ? (
        <div
          className="pointer-events-none absolute z-10 w-max max-w-[200px] rounded border border-slate-200 bg-white/95 px-2 py-1 text-xs tabular-nums shadow-sm dark:border-slate-700 dark:bg-slate-900/95"
          style={{ left: tipLeft, top: tipTop }}
          data-testid="value-chart-tooltip"
        >
          <div>
            <span className="text-slate-400 dark:text-slate-500">{hover.date}</span>
            <span className="ml-2 font-medium text-slate-700 dark:text-slate-200">
              {formatMoney(hover.value, currency)}
            </span>
          </div>
          {hover.trades.length > 0 ? (
            <ul className="mt-1 space-y-0.5 border-t border-slate-100 pt-1 dark:border-slate-800">
              {hover.trades.map((t, i) => (
                <li key={`${t.symbol}-${t.side}-${i}`} className="flex items-baseline gap-1.5">
                  <span
                    className={
                      t.side === "BUY"
                        ? "font-medium text-emerald-600 dark:text-emerald-400"
                        : "font-medium text-red-600 dark:text-red-400"
                    }
                  >
                    {t.side}
                  </span>
                  <span className="text-slate-700 dark:text-slate-200">{t.symbol}</span>
                  <span className="text-slate-500 dark:text-slate-400">
                    {t.units.toLocaleString("en-GB", { maximumFractionDigits: 4 })} units
                  </span>
                </li>
              ))}
            </ul>
          ) : null}
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
