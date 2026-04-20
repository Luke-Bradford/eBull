/**
 * PriceChart — hand-rolled SVG line chart of daily close + volume bars
 * (Slice B of #316 Instrument terminal).
 *
 * Scope trade-off: a real candlestick chart needs a library
 * (lightweight-charts, recharts) — we deliberately ship zero-deps
 * here per CLAUDE.md "Do not add libraries casually". Close price is
 * the single most operator-relevant number for long-horizon
 * investment decisions; OHLC / volume detail can be a follow-up if
 * an operator flow actually needs them.
 *
 * Layout:
 *   ┌────────────────────────────────────────┐
 *   │  price line + hover tooltip            │ 70%
 *   │                                        │
 *   ├────────────────────────────────────────┤
 *   │  volume bars                           │ 30%
 *   └────────────────────────────────────────┘
 *
 * Range picker sits above the chart — 1w · 1m · 3m · 6m · 1y · 5y · max.
 * Selection is URL-synced via `?chart=<range>` so the operator's last
 * selection survives tab switches within the research page.
 */
import { useCallback, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { fetchInstrumentCandles } from "@/api/instruments";
import type { CandleBar, CandleRange, InstrumentCandles } from "@/api/types";
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

// Plot area dimensions — uses viewBox + SVG scale to fluid-fit the
// parent container (responsive via preserveAspectRatio below).
const W = 800;
const PRICE_H = 240;
const VOL_H = 60;
const PAD_LEFT = 56;
const PAD_RIGHT = 12;
const PAD_TOP = 12;
const PAD_BOTTOM = 20;

interface PricePoint {
  x: number;
  y: number;
  close: number;
  date: string;
}

interface VolumeBar {
  x: number;
  h: number;
  up: boolean;
}

interface ChartGeometry {
  path: string;
  points: PricePoint[];
  volume: VolumeBar[];
  volBarW: number;
  priceMin: number;
  priceMax: number;
  firstDate: string | null;
  lastDate: string | null;
}

function parseNum(v: string | null | undefined): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function geometry(rows: CandleBar[]): ChartGeometry | null {
  const clean = rows.filter((r) => parseNum(r.close) !== null);
  if (clean.length < 2) return null;

  const closes = clean.map((r) => parseNum(r.close) ?? 0);
  const priceMin = Math.min(...closes);
  const priceMax = Math.max(...closes);
  const priceRange = priceMax - priceMin || 1;

  const volumes = clean.map((r) => parseNum(r.volume) ?? 0);
  const volMax = Math.max(...volumes, 1);

  const plotW = W - PAD_LEFT - PAD_RIGHT;
  const plotH = PRICE_H - PAD_TOP - PAD_BOTTOM;
  const step = plotW / (clean.length - 1);

  const points: PricePoint[] = clean.map((r, i) => {
    const c = parseNum(r.close) ?? 0;
    return {
      x: PAD_LEFT + i * step,
      y: PAD_TOP + plotH - ((c - priceMin) / priceRange) * plotH,
      close: c,
      date: r.date,
    };
  });

  const path = points
    .map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(2)},${p.y.toFixed(2)}`)
    .join(" ");

  // Width and position share the same derivation so bars render
  // consistently regardless of `n`. Cap at 12px so a 2-point series
  // doesn't produce a wall-to-wall bar that leaks off the chart.
  const volBarW = Math.max(1, Math.min(step * 0.8, 12));
  const volume: VolumeBar[] = clean.map((r, i) => {
    const v = parseNum(r.volume) ?? 0;
    const h = (v / volMax) * VOL_H;
    const prev = i > 0 ? (parseNum(clean[i - 1]!.close) ?? 0) : 0;
    const curr = parseNum(r.close) ?? 0;
    return {
      x: PAD_LEFT + i * step - volBarW / 2,
      h,
      up: curr >= prev,
    };
  });

  return {
    path,
    points,
    volume,
    volBarW,
    priceMin,
    priceMax,
    firstDate: clean[0]?.date ?? null,
    lastDate: clean[clean.length - 1]?.date ?? null,
  };
}

export interface PriceChartProps {
  symbol: string;
  initialRange?: CandleRange;
}

const VALID_RANGES: readonly CandleRange[] = [
  "1w",
  "1m",
  "3m",
  "6m",
  "1y",
  "5y",
  "max",
];

export function PriceChart({
  symbol,
  initialRange = "1m",
}: PriceChartProps): JSX.Element {
  // URL-sync so the operator's range choice survives tab switches
  // inside the research page (and lives in shareable links). `replace`
  // on change so range-toggles don't spam browser history.
  const [searchParams, setSearchParams] = useSearchParams();
  const rawChart = searchParams.get("chart");
  const range: CandleRange = VALID_RANGES.includes(rawChart as CandleRange)
    ? (rawChart as CandleRange)
    : initialRange;
  const setRange = useCallback(
    (next: CandleRange) => {
      const params = new URLSearchParams(searchParams);
      if (next === initialRange) {
        params.delete("chart");
      } else {
        params.set("chart", next);
      }
      setSearchParams(params, { replace: true });
    },
    [searchParams, setSearchParams, initialRange],
  );
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  const { data, error, loading, refetch } = useAsync<InstrumentCandles>(
    () => fetchInstrumentCandles(symbol, range),
    [symbol, range],
  );

  const geom = useMemo<ChartGeometry | null>(
    () => (data ? geometry(data.rows) : null),
    [data],
  );

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
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
              data-testid={`chart-range-${r.id}`}
            >
              {r.label}
            </button>
          ))}
        </div>
        {geom && hoverIdx !== null && geom.points[hoverIdx] ? (
          <div className="text-xs tabular-nums text-slate-600">
            <span className="text-slate-400">
              {geom.points[hoverIdx]!.date}
            </span>
            <span className="ml-2 font-medium">
              {geom.points[hoverIdx]!.close.toLocaleString(undefined, {
                maximumFractionDigits: 2,
              })}
            </span>
          </div>
        ) : null}
      </div>

      {loading ? <SectionSkeleton rows={6} /> : null}
      {error !== null ? <SectionError onRetry={refetch} /> : null}
      {!loading && error === null && geom === null ? (
        <EmptyState
          title="No price data"
          description="No candles in the local price_daily store for this range. Widen the range or wait for the next market-data refresh."
        />
      ) : null}

      {geom !== null ? (
        <ChartSvg
          geom={geom}
          onHover={setHoverIdx}
          data-testid={`price-chart-${symbol}`}
        />
      ) : null}
    </div>
  );
}

function ChartSvg({
  geom,
  onHover,
  "data-testid": testId,
}: {
  geom: ChartGeometry;
  onHover: (idx: number | null) => void;
  "data-testid": string;
}): JSX.Element {
  const totalH = PRICE_H + VOL_H + 8;
  const findNearestIdx = (evt: React.MouseEvent<SVGSVGElement>): number => {
    const svg = evt.currentTarget;
    const rect = svg.getBoundingClientRect();
    // Container aspect-ratio is pinned to the viewBox below, so the
    // svg element's width ↔ viewBox.width mapping is uniform and this
    // linear rescale is correct (would need getScreenCTM otherwise).
    const x = ((evt.clientX - rect.left) / rect.width) * W;
    // Nearest-x lookup. Points are evenly spaced so linear search is
    // fine for the MVP; bisect later if large ranges feel slow.
    let best = 0;
    let bestDx = Infinity;
    for (let i = 0; i < geom.points.length; i++) {
      const dx = Math.abs(geom.points[i]!.x - x);
      if (dx < bestDx) {
        best = i;
        bestDx = dx;
      }
    }
    return best;
  };

  return (
    <svg
      data-testid={testId}
      viewBox={`0 0 ${W} ${totalH}`}
      // `xMidYMid meet` + container aspectRatio pinned to viewBox
      // ensures uniform scaling — no stretched line slopes or
      // tall-looking volume bars on narrow screens (Codex slice-B
      // round-2 finding). `maxHeight` still caps the chart on very
      // wide viewports.
      preserveAspectRatio="xMidYMid meet"
      className="w-full"
      style={{
        aspectRatio: `${W} / ${totalH}`,
        maxHeight: `${PRICE_H + VOL_H + 24}px`,
      }}
      onMouseMove={(e) => onHover(findNearestIdx(e))}
      onMouseLeave={() => onHover(null)}
    >
      {/* Y-axis ticks — 4 evenly-spaced price labels */}
      {[0, 0.25, 0.5, 0.75, 1].map((frac) => {
        const price = geom.priceMax - frac * (geom.priceMax - geom.priceMin);
        const y = PAD_TOP + frac * (PRICE_H - PAD_TOP - PAD_BOTTOM);
        return (
          <g key={frac}>
            <line
              x1={PAD_LEFT}
              x2={W - PAD_RIGHT}
              y1={y}
              y2={y}
              stroke="#e2e8f0"
              strokeWidth={1}
              strokeDasharray="2 2"
            />
            <text
              x={PAD_LEFT - 6}
              y={y + 3}
              textAnchor="end"
              className="fill-slate-400"
              style={{ fontSize: "10px" }}
            >
              {price.toLocaleString(undefined, { maximumFractionDigits: 2 })}
            </text>
          </g>
        );
      })}

      {/* Price line */}
      <path d={geom.path} fill="none" stroke="#2563eb" strokeWidth={1.5} />

      {/* Volume bars in the bottom strip */}
      {geom.volume.map((v, i) => (
        <rect
          key={i}
          x={v.x}
          y={PRICE_H + 8 + (VOL_H - v.h)}
          width={geom.volBarW}
          height={v.h}
          fill={v.up ? "#10b981" : "#ef4444"}
          opacity={0.5}
        />
      ))}

      {/* Period boundary labels */}
      {geom.firstDate ? (
        <text
          x={PAD_LEFT}
          y={PRICE_H + VOL_H + 6}
          textAnchor="start"
          className="fill-slate-400"
          style={{ fontSize: "10px" }}
        >
          {geom.firstDate}
        </text>
      ) : null}
      {geom.lastDate ? (
        <text
          x={W - PAD_RIGHT}
          y={PRICE_H + VOL_H + 6}
          textAnchor="end"
          className="fill-slate-400"
          style={{ fontSize: "10px" }}
        >
          {geom.lastDate}
        </text>
      ) : null}
    </svg>
  );
}
