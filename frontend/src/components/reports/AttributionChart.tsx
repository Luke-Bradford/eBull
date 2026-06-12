/**
 * §4.4 Attribution — horizontal diverging bars: top 5 contributors /
 * top 5 detractors by period P&L delta (realised fold landed with the
 * #1596 contract). Follows the InsiderByOfficer idiom:
 * layout="vertical", per-Cell up/down fill. Symbols are link-styled
 * axis ticks (keyboard Enter); bar rects are NOT silently clickable
 * (read-only-vs-interactive convention).
 */
import { useNavigate } from "react-router-dom";
import {
  Bar,
  BarChart,
  Cell,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { PeriodContributionV2 } from "@/api/reportSnapshot";
import { ChartTooltip } from "@/components/charts/ChartTooltip";
import { NilLine } from "@/components/reports/StatementChrome";
import { dec } from "@/components/reports/snapshotMath";
import { formatMoney, formatPct } from "@/lib/format";
import { useChartTheme } from "@/lib/useChartTheme";

interface AttributionRow {
  symbol: string;
  delta: number;
  pct: number | null;
}

export function buildAttributionRows(contribution: PeriodContributionV2): AttributionRow[] {
  const rows: AttributionRow[] = [];
  for (const r of [...contribution.contributors.slice(0, 5), ...contribution.drags.slice(0, 5)]) {
    const delta = dec(r.pnl_delta);
    if (delta === null) continue;
    rows.push({ symbol: r.symbol, delta, pct: dec(r.pnl_pct) });
  }
  rows.sort((a, b) => b.delta - a.delta);
  return rows;
}

interface TooltipProps {
  active?: boolean;
  payload?: ReadonlyArray<{ payload?: AttributionRow }>;
}

function AttributionTooltip({ active, payload, currency }: TooltipProps & { currency: string }) {
  if (active !== true || !payload || payload.length === 0) return null;
  const row = payload[0]?.payload;
  if (!row) return null;
  return (
    <ChartTooltip>
      <div className="font-medium text-slate-700 dark:text-slate-200">{row.symbol}</div>
      <div
        className={`tabular-nums ${row.delta >= 0 ? "text-emerald-700 dark:text-emerald-400" : "text-red-700 dark:text-red-400"}`}
      >
        {formatMoney(row.delta, currency)}
        {row.pct !== null ? <span className="ml-1 text-[10px]">({formatPct(row.pct)})</span> : null}
      </div>
    </ChartTooltip>
  );
}

const ROW_HEIGHT_PX = 24;

export function AttributionChart({
  contribution,
  currency,
}: {
  contribution: PeriodContributionV2;
  currency: string;
}) {
  const theme = useChartTheme();
  const navigate = useNavigate();
  const rows = buildAttributionRows(contribution);

  if (rows.length === 0) {
    return <NilLine>No period contribution to attribute — no prior snapshot baseline or no open positions.</NilLine>;
  }

  const height = Math.max(96, rows.length * ROW_HEIGHT_PX + 32);
  const goTo = (symbol: string) => navigate(`/instrument/${encodeURIComponent(symbol)}`);

  return (
    <div style={{ height }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={rows} layout="vertical" margin={{ top: 4, right: 24, left: 8, bottom: 4 }}>
          <XAxis
            type="number"
            tickFormatter={(v: number) => formatMoney(v, currency)}
            stroke={theme.textSecondary}
            tick={{ fill: theme.textMuted, fontSize: 10 }}
          />
          <YAxis
            type="category"
            dataKey="symbol"
            width={72}
            interval={0}
            stroke={theme.textSecondary}
            tick={(props: { x: number; y: number; payload: { value: string } }) => (
              <text
                x={props.x}
                y={props.y}
                dy={4}
                textAnchor="end"
                role="link"
                tabIndex={0}
                className="cursor-pointer fill-blue-600 text-[10px] hover:underline dark:fill-blue-400"
                onClick={() => goTo(props.payload.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") goTo(props.payload.value);
                }}
              >
                {props.payload.value}
              </text>
            )}
          />
          <ReferenceLine x={0} stroke={theme.borderColor} />
          <Tooltip
            content={<AttributionTooltip currency={currency} />}
            cursor={{ fill: theme.gridLine }}
          />
          <Bar dataKey="delta" isAnimationActive={false}>
            {rows.map((r) => (
              <Cell key={r.symbol} fill={r.delta >= 0 ? theme.up : theme.down} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
