/**
 * Charts for the peer-comparison drill (#594): a multi-factor radar, a sector
 * heatmap (hand-rolled CSS grid, like the filings heatmap), and a same-day
 * peer-return scatter. All read the pure shapes from `@/lib/peerComparison` and
 * take every colour from `useChartTheme()` `theme.*` (never `lightTheme.*` —
 * prevention-log 1917).
 */
import { type CSSProperties } from "react";
import {
  CartesianGrid,
  Legend,
  PolarAngleAxis,
  PolarGrid,
  PolarRadiusAxis,
  Radar,
  RadarChart,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";

import { ChartTooltip } from "@/components/charts/ChartTooltip";
import {
  type Heatmap,
  type RadarPoint,
  type ScatterData,
} from "@/lib/peerComparison";
import { useChartTheme } from "@/lib/useChartTheme";

const CHART_HEIGHT = 320;

function NoPeers({ message }: { message: string }): JSX.Element {
  return <p className="px-2 py-6 text-xs text-slate-500">{message}</p>;
}

function fmtRaw(v: number | null): string {
  if (v === null) return "—";
  // Margins / ratios are fractional; large values (P/E) are not.
  return Math.abs(v) < 10 ? v.toFixed(3) : v.toFixed(2);
}

function pct(v: number): string {
  return `${(v * 100).toFixed(1)}%`;
}

function hexToRgb(hex: string): { r: number; g: number; b: number } {
  const h = hex.replace("#", "");
  const n = parseInt(h.length === 3 ? h.replace(/./g, "$&$&") : h, 16);
  return { r: (n >> 16) & 255, g: (n >> 8) & 255, b: n & 255 };
}

// ---------------------------------------------------------------------------
// 1. Multi-factor radar
// ---------------------------------------------------------------------------

interface RadarTooltipProps {
  active?: boolean;
  payload?: ReadonlyArray<{ payload?: RadarPoint }>;
  symbol: string;
}

function RadarTooltip({ active, payload, symbol }: RadarTooltipProps): JSX.Element | null {
  if (active !== true || !payload || payload.length === 0) return null;
  const pt = payload[0]?.payload;
  if (!pt) return null;
  return (
    <ChartTooltip>
      <div className="font-medium text-slate-700 dark:text-slate-200">
        {pt.label}
        {pt.devLimited ? " ⚠" : ""}
      </div>
      <div className="tabular-nums text-slate-600 dark:text-slate-300">
        {symbol} {fmtRaw(pt.instrumentRaw)} · sector median {fmtRaw(pt.medianRaw)}
      </div>
      <div className="text-slate-500 dark:text-slate-400">
        better {pt.betterWhen} · n={pt.sectorN.toLocaleString()}
        {pt.devLimited ? " · thin coverage" : ""}
      </div>
    </ChartTooltip>
  );
}

export function PeerRadarChart({
  radar,
  symbol,
}: {
  radar: readonly RadarPoint[];
  symbol: string;
}): JSX.Element {
  const theme = useChartTheme();
  const hasSignal = radar.some((p) => p.instrument !== null || p.median !== null);
  if (radar.length === 0 || !hasSignal) {
    return <NoPeers message="No comparable factors for this instrument." />;
  }
  // Custom angle tick: grey + flag dev_limited factor labels. Look up by the
  // label recharts passes in payload.value (robust to recharts' index-vs-
  // payload.index quirk — Codex ckpt-2); factor labels are unique.
  const byLabel = new Map(radar.map((p) => [p.label, p]));
  const renderTick = (props: {
    x?: number;
    y?: number;
    textAnchor?: "start" | "middle" | "end" | "inherit";
    payload?: { value?: string };
  }): JSX.Element => {
    const pt = props.payload?.value !== undefined ? byLabel.get(props.payload.value) : undefined;
    const dev = pt?.devLimited ?? false;
    return (
      <text
        x={props.x}
        y={props.y}
        textAnchor={props.textAnchor}
        fill={dev ? theme.textMuted : theme.textSecondary}
        fontSize={10}
      >
        {props.payload?.value}
        {dev ? " ⚠" : ""}
      </text>
    );
  };
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <RadarChart data={[...radar]} margin={{ top: 8, right: 40, bottom: 8, left: 40 }}>
          <PolarGrid stroke={theme.gridLine} />
          <PolarAngleAxis dataKey="label" tick={renderTick} />
          <PolarRadiusAxis domain={[0, 1]} tick={false} axisLine={false} />
          <Radar
            name={symbol}
            dataKey="instrument"
            stroke={theme.accent[1]}
            fill={theme.accent[1]}
            fillOpacity={0.3}
            isAnimationActive={false}
          />
          <Radar
            name="Sector median"
            dataKey="median"
            stroke={theme.accent[2]}
            fill={theme.accent[2]}
            fillOpacity={0.12}
            strokeDasharray="4 2"
            isAnimationActive={false}
          />
          <Tooltip content={<RadarTooltip symbol={symbol} />} />
          <Legend wrapperStyle={{ fontSize: "11px" }} />
        </RadarChart>
      </ResponsiveContainer>
      <p className="mt-1 px-2 text-[10px] text-slate-400">
        Axes normalized per factor across the instrument + sector median + peers; outward = better
        (orientation per factor). ⚠ = thin sector coverage (price-gated or &lt;20% of members) —
        median is noisy; a missing vertex is a data gap, not worst-in-class. Hover for raw values.
      </p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 2. Sector heatmap — hand-rolled grid, red→green by relative rank
// ---------------------------------------------------------------------------

export function SectorHeatmap({ heatmap }: { heatmap: Heatmap }): JSX.Element {
  const theme = useChartTheme();
  if (heatmap.factors.length === 0 || heatmap.rows.length === 0) {
    return <NoPeers message="No peers to map." />;
  }
  const lo = hexToRgb(theme.down);
  const hi = hexToRgb(theme.up);
  const cellBg = (score: number | null): CSSProperties => {
    if (score === null) return { backgroundColor: "transparent" };
    const r = Math.round(lo.r + (hi.r - lo.r) * score);
    const g = Math.round(lo.g + (hi.g - lo.g) * score);
    const b = Math.round(lo.b + (hi.b - lo.b) * score);
    return { backgroundColor: `rgba(${r}, ${g}, ${b}, 0.55)` };
  };
  return (
    <div className="overflow-x-auto">
      <div
        className="grid gap-px text-[11px]"
        style={{ gridTemplateColumns: `minmax(64px, auto) repeat(${heatmap.factors.length}, minmax(56px, 1fr))` }}
      >
        {/* header row: corner + factor labels */}
        <div />
        {heatmap.factors.map((f) => (
          <div
            key={f.key}
            className={`px-1 text-center leading-tight ${
              f.devLimited ? "text-slate-400" : "text-slate-600 dark:text-slate-300"
            }`}
            title={`${f.label}${f.devLimited ? " (thin coverage)" : ""} · sector n=${f.sectorN}`}
          >
            {f.label}
            {f.devLimited ? " ⚠" : ""}
          </div>
        ))}
        {/* one row per instrument/peer */}
        {heatmap.rows.map((row) => (
          <div key={row.symbol} className="contents">
            <div
              className={`truncate pr-2 ${
                row.isInstrument
                  ? "font-semibold text-slate-900 dark:text-slate-100"
                  : "text-slate-600 dark:text-slate-300"
              }`}
              title={row.companyName ?? row.symbol}
            >
              {row.symbol}
            </div>
            {heatmap.factors.map((f) => {
              const cell = row.cells[f.key];
              const score = cell?.score ?? null;
              return (
                <div
                  key={`${row.symbol}|${f.key}`}
                  className="flex h-6 items-center justify-center rounded-sm border border-slate-100 dark:border-slate-800 tabular-nums text-[10px] text-slate-700 dark:text-slate-200"
                  style={cellBg(score)}
                  title={`${row.symbol} · ${f.label}: ${fmtRaw(cell?.raw ?? null)}`}
                >
                  {fmtRaw(cell?.raw ?? null)}
                </div>
              );
            })}
          </div>
        ))}
      </div>
      <p className="mt-2 text-[10px] text-slate-400">
        Cell shade red→green by relative rank within each factor (green = better, per factor
        orientation). Instrument row pinned on top. ⚠ = thin coverage; — = no data (not worst).
      </p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 3. Peer return scatter — instrument vs median-peer same-day return
// ---------------------------------------------------------------------------

interface ScatterTooltipProps {
  active?: boolean;
  payload?: ReadonlyArray<{ payload?: { date: string; x: number; y: number; nPeers: number } }>;
}

function ScatterTooltip({ active, payload }: ScatterTooltipProps): JSX.Element | null {
  if (active !== true || !payload || payload.length === 0) return null;
  const p = payload[0]?.payload;
  if (!p) return null;
  // Three-way: equal same-day returns sit ON the diagonal — neutral, not a loss.
  const verdict = p.x === p.y ? "in line with" : p.x > p.y ? "outperformed" : "underperformed";
  const verdictClass =
    p.x === p.y
      ? "text-slate-500 dark:text-slate-400"
      : p.x > p.y
        ? "text-emerald-600 dark:text-emerald-400"
        : "text-red-600 dark:text-red-400";
  return (
    <ChartTooltip>
      <div className="font-medium text-slate-700 dark:text-slate-200">{p.date}</div>
      <div className="tabular-nums text-slate-600 dark:text-slate-300">
        instrument {pct(p.x)} · median peer {pct(p.y)}
      </div>
      <div className={verdictClass}>
        {verdict} the sector · {p.nPeers} peer{p.nPeers === 1 ? "" : "s"}
      </div>
    </ChartTooltip>
  );
}

export function PeerReturnScatter({ data }: { data: ScatterData }): JSX.Element {
  const theme = useChartTheme();
  if (data.points.length === 0) {
    return <NoPeers message="Not enough overlapping price history to compare daily returns." />;
  }
  const d = data.domain;
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <ScatterChart margin={{ top: 8, right: 16, bottom: 16, left: 8 }}>
          <CartesianGrid stroke={theme.gridLine} />
          <XAxis
            type="number"
            dataKey="x"
            domain={[-d, d]}
            tickFormatter={pct}
            stroke={theme.textSecondary}
            tick={{ fill: theme.textMuted, fontSize: 10 }}
            name="instrument"
          >
          </XAxis>
          <YAxis
            type="number"
            dataKey="y"
            domain={[-d, d]}
            tickFormatter={pct}
            stroke={theme.textSecondary}
            tick={{ fill: theme.textMuted, fontSize: 10 }}
            width={48}
            name="median peer"
          />
          <ZAxis range={[28, 28]} />
          <ReferenceLine
            segment={[
              { x: -d, y: -d },
              { x: d, y: d },
            ]}
            stroke={theme.borderColor}
            strokeDasharray="4 2"
          />
          <Tooltip content={<ScatterTooltip />} cursor={{ strokeDasharray: "3 3" }} />
          <Scatter data={[...data.points]} fill={theme.accent[1]} fillOpacity={0.6} isAnimationActive={false} />
        </ScatterChart>
      </ResponsiveContainer>
      <p className="mt-1 px-2 text-[10px] text-slate-400">
        Each point = one day. Below the diagonal = the instrument outperformed the sector that day
        (same-day relative return, not lead/lag).
      </p>
    </div>
  );
}
