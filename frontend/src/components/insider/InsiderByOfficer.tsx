/**
 * InsiderByOfficer — horizontal bar chart of every officer's 90-day
 * net activity (#588). Acquired = green, disposed = red. Sorted by
 * absolute net activity descending so the heaviest movers anchor
 * the top.
 *
 * No `top N` cap — the operator wants the complete picture so a tail
 * of small disposers can still be seen against a large acquirer (and
 * vice versa). The container scrolls when the list is long; chart
 * height scales with row count so each officer keeps a readable bar.
 */

import {
  Bar,
  BarChart,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { InsiderTransactionDetail } from "@/api/instruments";
import { lightTheme } from "@/lib/chartTheme";
import { useChartTheme } from "@/lib/useChartTheme";
import {
  directionOf,
  signedShares,
  startOfNDaysAgoUtcMs,
} from "@/lib/insiderClassify";

interface OfficerBucket {
  /** Display name with role suffix when available, e.g. "Jane Doe — director". */
  readonly officer: string;
  /** Net signed shares over the last 90d for this officer. */
  readonly net: number;
  /** Number of classified non-derivative transactions in the window
   *  for this officer. Used to keep officers visible when their net
   *  is zero from offsetting buys + sells (real activity, not idle). */
  readonly txnCount: number;
  /** Stable identity for the React key + tooltip lookup. */
  readonly key: string;
}

const WINDOW_DAYS = 90;
const ROW_HEIGHT_PX = 22;
const MIN_HEIGHT_PX = 120;

export function buildOfficerBuckets(
  rows: ReadonlyArray<InsiderTransactionDetail>,
  cutoffMs: number = startOfNDaysAgoUtcMs(WINDOW_DAYS),
): OfficerBucket[] {
  // Aggregate by CIK when present (stable SEC identifier); fall back
  // to filer_name for rows without a CIK so they still appear.
  const map = new Map<
    string,
    { name: string; role: string | null; net: number; count: number }
  >();

  for (const row of rows) {
    if (row.is_derivative) continue;
    const dt = new Date(`${row.txn_date}T00:00:00Z`);
    if (Number.isNaN(dt.getTime()) || dt.getTime() < cutoffMs) continue;
    if (directionOf(row.acquired_disposed_code, row.txn_code) === "unknown") {
      continue;
    }
    const id = row.filer_cik ?? `name:${row.filer_name}`;
    const signed = signedShares(row.shares, row.acquired_disposed_code, row.txn_code);
    const existing = map.get(id);
    if (existing) {
      existing.net += signed;
      existing.count += 1;
    } else {
      map.set(id, {
        name: row.filer_name,
        role: row.filer_role,
        net: signed,
        count: 1,
      });
    }
  }

  const buckets: OfficerBucket[] = [];
  // Every entry in `map` was created from at least one classified
  // non-derivative row (count >= 1) — derivatives and unknown
  // directions are filtered upstream — so the bucket itself proves
  // real activity. Net == 0 on its own is fine and keeps offsetting
  // buy + sell traders visible.
  for (const [key, { name, role, net, count }] of map.entries()) {
    const roleLabel = role !== null ? roleSummary(role) : null;
    buckets.push({
      key,
      officer: roleLabel !== null ? `${name} — ${roleLabel}` : name,
      net,
      txnCount: count,
    });
  }
  // Largest absolute net first; ties (incl. net=0) by transaction
  // count so a busy offset-trader sits above a quiet zero-tail.
  buckets.sort((a, b) => {
    const cmp = Math.abs(b.net) - Math.abs(a.net);
    if (cmp !== 0) return cmp;
    return b.txnCount - a.txnCount;
  });
  return buckets;
}

/** Compress the pipe-joined `filer_role` string into a short, human
 *  label. Picks the most informative role token; officer titles win
 *  over generic flags. */
function roleSummary(role: string): string {
  const parts = role.split("|");
  for (const p of parts) {
    if (p.startsWith("officer:") && p.length > "officer:".length) {
      return p.slice("officer:".length);
    }
  }
  if (parts.includes("director")) return "director";
  if (parts.includes("officer")) return "officer";
  if (parts.includes("ten_percent_owner")) return "10% owner";
  return parts[0] ?? "";
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
  payload?: ReadonlyArray<{ payload?: OfficerBucket }>;
}

function OfficerTooltip({ active, payload }: TooltipProps) {
  if (active !== true || !payload || payload.length === 0) return null;
  const bucket = payload[0]?.payload;
  if (!bucket) return null;
  const colorClass =
    bucket.net > 0
      ? "text-emerald-700"
      : bucket.net < 0
        ? "text-red-700"
        : "text-slate-600";
  return (
    <div className="rounded border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 px-2 py-1 text-xs shadow-md">
      <div className="font-medium text-slate-700">{bucket.officer}</div>
      <div className={`font-mono tabular-nums ${colorClass}`}>
        {bucket.net > 0 ? "+" : ""}
        {formatShares(bucket.net)} sh
      </div>
      <div className="text-[10px] text-slate-500">
        {bucket.txnCount} txn{bucket.txnCount === 1 ? "" : "s"}
      </div>
    </div>
  );
}

export interface InsiderByOfficerProps {
  readonly transactions: ReadonlyArray<InsiderTransactionDetail>;
}

export function InsiderByOfficer({
  transactions,
}: InsiderByOfficerProps): JSX.Element {
  const theme = useChartTheme();
  const buckets = buildOfficerBuckets(transactions);
  if (buckets.length === 0) {
    return (
      <p className="px-2 py-3 text-xs text-slate-500">
        No officer-level net activity in the last 90 days.
      </p>
    );
  }
  // Match height to row count so 30+ officers stay readable. Cap the
  // outer scroll container so the page itself doesn't span 10 screens.
  const innerHeight = Math.max(MIN_HEIGHT_PX, buckets.length * ROW_HEIGHT_PX + 24);
  return (
    <div
      className="max-h-[60vh] overflow-y-auto"
      data-testid="insider-by-officer"
    >
      <div style={{ height: innerHeight }} className="w-full">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={buckets}
            layout="vertical"
            margin={{ top: 4, right: 24, left: 8, bottom: 4 }}
          >
            <XAxis
              type="number"
              tickFormatter={formatShares}
              stroke={theme.textSecondary}
              tick={{ fill: theme.textMuted, fontSize: 10 }}
            />
            <YAxis
              type="category"
              dataKey="officer"
              width={180}
              stroke={theme.textSecondary}
              tick={{ fill: theme.textPrimary, fontSize: 10 }}
              interval={0}
            />
            <Tooltip
              content={<OfficerTooltip />}
              cursor={{ fill: theme.gridLine }}
            />
            <Bar dataKey="net" isAnimationActive={false}>
              {buckets.map((b) => (
                <Cell
                  key={b.key}
                  fill={
                    b.net > 0
                      ? lightTheme.up
                      : b.net < 0
                        ? lightTheme.down
                        : theme.borderColor
                  }
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
