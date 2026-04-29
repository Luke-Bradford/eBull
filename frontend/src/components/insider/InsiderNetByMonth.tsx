/**
 * InsiderNetByMonth — net (acquired - disposed) shares per calendar
 * month, last 24 months, ±coloured bar chart (#588).
 *
 * Buckets transactions by `txn_date` calendar month (UTC). Empty
 * months render as zero-height bars so the time axis stays continuous
 * even when an issuer has gaps. Months with zero net activity
 * (acquired = disposed) render flat.
 */

import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { InsiderTransactionDetail } from "@/api/instruments";
import { chartTheme } from "@/lib/chartTheme";
import { directionOf, signedShares } from "@/lib/insiderClassify";

interface MonthBucket {
  /** ISO month key `YYYY-MM` for stable sort + axis labelling. */
  readonly month: string;
  /** Net signed shares for the month (acquired positive, disposed
   *  negative). */
  readonly net: number;
}

interface BuildResult {
  readonly buckets: MonthBucket[];
  /** True iff at least one classified non-derivative row landed in
   *  the window. Used to distinguish "no activity at all" from
   *  "activity netted to zero" when picking the empty state. */
  readonly hadActivity: boolean;
}

const MONTHS_BACK = 24;

/** Build a `YYYY-MM` key in UTC so the bucket of a transaction does
 *  not depend on the operator's locale offset. Form 4 dates are
 *  bare calendar dates, so timezone interpretation is a render-time
 *  choice — UTC is the convention used elsewhere in the chart layer. */
function monthKey(date: Date): string {
  const y = date.getUTCFullYear();
  const m = date.getUTCMonth() + 1;
  return `${y}-${String(m).padStart(2, "0")}`;
}

function shortMonthLabel(key: string): string {
  // "2026-04" → "Apr '26"
  const [y, m] = key.split("-");
  if (y === undefined || m === undefined) return key;
  const monthNum = Number(m);
  if (!Number.isInteger(monthNum) || monthNum < 1 || monthNum > 12) return key;
  const names = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ];
  return `${names[monthNum - 1]} '${y.slice(2)}`;
}

export function buildMonthBuckets(
  rows: ReadonlyArray<InsiderTransactionDetail>,
): BuildResult {
  // Pre-seed every month in [now-24m, now] with zero so empty months
  // still render and the axis is continuous.
  const now = new Date();
  const map = new Map<string, number>();
  for (let i = MONTHS_BACK - 1; i >= 0; i--) {
    const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - i, 1));
    map.set(monthKey(d), 0);
  }
  const earliestKey = [...map.keys()][0]!;
  let hadActivity = false;

  for (const row of rows) {
    if (row.is_derivative) continue; // mirror summary lens — non-derivative only
    if (directionOf(row.acquired_disposed_code, row.txn_code) === "unknown") {
      continue;
    }
    const dt = new Date(`${row.txn_date}T00:00:00Z`);
    if (Number.isNaN(dt.getTime())) continue;
    const key = monthKey(dt);
    if (key < earliestKey) continue; // older than 24m back
    if (!map.has(key)) continue; // future-dated outliers — skip
    const signed = signedShares(row.shares, row.acquired_disposed_code, row.txn_code);
    map.set(key, (map.get(key) ?? 0) + signed);
    hadActivity = true;
  }

  return {
    buckets: [...map.entries()].map(([month, net]) => ({ month, net })),
    hadActivity,
  };
}

function formatShares(n: number): string {
  const abs = Math.abs(n);
  const sign = n < 0 ? "-" : "";
  if (abs >= 1e9) return `${sign}${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}${(abs / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${sign}${(abs / 1e3).toFixed(1)}K`;
  return `${sign}${abs.toLocaleString()}`;
}

interface TooltipProps {
  active?: boolean;
  payload?: ReadonlyArray<{ payload?: MonthBucket }>;
}

function NetTooltip({ active, payload }: TooltipProps) {
  if (active !== true || !payload || payload.length === 0) return null;
  const bucket = payload[0]?.payload;
  if (!bucket) return null;
  const colorClass =
    bucket.net > 0
      ? "text-emerald-700"
      : bucket.net < 0
        ? "text-red-700"
        : "text-slate-700";
  return (
    <div className="rounded border border-slate-200 bg-white px-2 py-1 text-xs shadow-md">
      <div className="font-medium text-slate-700">
        {shortMonthLabel(bucket.month)}
      </div>
      <div className={`font-mono tabular-nums ${colorClass}`}>
        {bucket.net > 0 ? "+" : ""}
        {formatShares(bucket.net)} sh
      </div>
    </div>
  );
}

export interface InsiderNetByMonthProps {
  readonly transactions: ReadonlyArray<InsiderTransactionDetail>;
}

export function InsiderNetByMonth({
  transactions,
}: InsiderNetByMonthProps): JSX.Element {
  const { buckets, hadActivity } = buildMonthBuckets(transactions);
  if (!hadActivity) {
    return (
      <p className="px-2 py-3 text-xs text-slate-500">
        No non-derivative insider transactions in the last 24 months.
      </p>
    );
  }
  return (
    <div className="h-72 w-full" data-testid="insider-net-by-month">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={buckets} margin={{ top: 8, right: 8, left: 8, bottom: 4 }}>
          <CartesianGrid stroke={chartTheme.gridLine} vertical={false} />
          <XAxis
            dataKey="month"
            tickFormatter={shortMonthLabel}
            stroke={chartTheme.textSecondary}
            tick={{ fill: chartTheme.textMuted, fontSize: 10 }}
            interval="preserveStartEnd"
            minTickGap={20}
          />
          <YAxis
            tickFormatter={formatShares}
            stroke={chartTheme.textSecondary}
            tick={{ fill: chartTheme.textMuted, fontSize: 10 }}
            width={56}
          />
          <ReferenceLine y={0} stroke={chartTheme.borderColor} />
          <Tooltip content={<NetTooltip />} cursor={{ fill: chartTheme.gridLine }} />
          <Bar dataKey="net" isAnimationActive={false}>
            {buckets.map((b) => (
              <Cell
                key={b.month}
                fill={
                  b.net > 0 ? chartTheme.up : b.net < 0 ? chartTheme.down : chartTheme.borderColor
                }
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
