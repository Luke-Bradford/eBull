/**
 * Ownership history pane (#922) — trend companion to the rollup pie.
 * The pie answers "where is the float TODAY"; this answers "where is
 * it TRENDING".
 *
 * Selection-driven (spec D4): consumes the L2 page's existing
 * ``?category=`` / ``?filer=`` params via props — no second
 * selection mechanism. Aggregate lines exist only for institutions
 * (13F quarterly, dedup-before-sum on the backend) and treasury
 * (issuer-level); event-driven categories chart per-holder only.
 *
 * Y axis is ABSOLUTE SHARES, not percent (spec D5): a percent of
 * TODAY's outstanding applied to old quarters misstates history
 * (buybacks shrink the denominator). The tooltip shows the percent
 * explicitly labelled "of current outstanding".
 */

import { useCallback, useMemo, useState } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { fetchOwnershipHistory } from "@/api/ownershipHistory";
import {
  HISTORY_WINDOWS,
  type HistoryLine,
  type HistoryWindow,
  buildHistoryRows,
  historyModeSignature,
  linesByNature,
  resolveHistoryMode,
  windowFromDate,
} from "@/components/instrument/ownershipHistorySeries";
import { formatPct, formatShares } from "@/components/instrument/ownershipMetrics";
import { EmptyState } from "@/components/states/EmptyState";
import { useChartTheme } from "@/lib/useChartTheme";
import { useAsync } from "@/lib/useAsync";

export interface OwnershipHistoryChartProps {
  readonly symbol: string;
  readonly categoryFilter: string | null;
  readonly filerFilter: string | null;
  /** Label for the selected filer (from the table rows); falls back
   *  to the raw holder id in the line legend. */
  readonly filerLabel: string | null;
  /** Current shares outstanding — tooltip percent denominator,
   *  explicitly labelled "current". */
  readonly outstanding: number | null;
}

const AGGREGATE_LABEL: Record<"institutions" | "treasury", string> = {
  // "(13F)" is load-bearing: this series is raw-13F-by-quarter and
  // does NOT reconcile 1:1 with the pie's survivor-deduped slice.
  institutions: "Institutions (13F)",
  treasury: "Treasury",
};

interface HistoryFetchResult {
  readonly lines: readonly HistoryLine[];
  /** Aggregate categories whose fetch failed while a sibling
   *  succeeded — rendered as a per-section note (spec D4). */
  readonly failed: readonly string[];
}

export function OwnershipHistoryChart({
  symbol,
  categoryFilter,
  filerFilter,
  filerLabel,
  outstanding,
}: OwnershipHistoryChartProps): JSX.Element {
  const theme = useChartTheme();
  const [historyWindow, setHistoryWindow] = useState<HistoryWindow>("3Y");

  const mode = useMemo(
    () => resolveHistoryMode(categoryFilter, filerFilter),
    [categoryFilter, filerFilter],
  );
  const modeSig = historyModeSignature(mode);
  // Memoized so the date string is computed once per window change,
  // not per render (review WARNING on PR #1586; the value is a
  // primitive so deps were already value-stable, but the memo makes
  // the stability explicit instead of incidental).
  const fromDate = useMemo(
    () => windowFromDate(historyWindow, new Date()),
    [historyWindow],
  );

  const state = useAsync<HistoryFetchResult>(
    useCallback(async () => {
      if (mode.kind === "unsupported") return { lines: [], failed: [] };
      if (mode.kind === "holder") {
        const resp = await fetchOwnershipHistory(symbol, {
          category: mode.category,
          holderId: mode.holder_id,
          fromDate,
        });
        return {
          lines: linesByNature(resp.points, filerLabel ?? mode.holder_id),
          failed: [],
        };
      }
      const settled = await Promise.allSettled(
        mode.categories.map((c) =>
          fetchOwnershipHistory(symbol, { category: c, aggregate: true, fromDate }),
        ),
      );
      const lines: HistoryLine[] = [];
      const failed: string[] = [];
      settled.forEach((result, i) => {
        const category = mode.categories[i]!;
        if (result.status === "fulfilled") {
          lines.push({
            key: `agg-${category}`,
            label: AGGREGATE_LABEL[category],
            points: result.value.points,
          });
        } else {
          console.error(`ownership-history ${category} fetch failed:`, result.reason);
          failed.push(AGGREGATE_LABEL[category]);
        }
      });
      // All requested series failing = a real error state, not an
      // empty chart.
      if (lines.length === 0 && failed.length > 0) {
        throw new Error("all ownership-history fetches failed");
      }
      return { lines, failed };
      // ``modeSig`` stands in for ``mode`` in the deps — it encodes
      // the full fetch plan as a stable string.
    }, [symbol, modeSig, fromDate, filerLabel]),
    [symbol, modeSig, fromDate, filerLabel],
  );

  return (
    <section className="mt-6 rounded border border-slate-200 p-4 dark:border-slate-800">
      <div className="mb-3 flex items-center justify-between">
        <h2 className="text-sm font-semibold text-slate-700 dark:text-slate-200">
          Ownership history
        </h2>
        <div className="flex gap-1" role="group" aria-label="History window">
          {HISTORY_WINDOWS.map((w) => (
            <button
              key={w}
              type="button"
              onClick={() => setHistoryWindow(w)}
              className={`rounded border px-2 py-0.5 text-xs ${
                w === historyWindow
                  ? "border-blue-600 bg-blue-50 text-blue-700 dark:border-blue-500 dark:bg-blue-900/30 dark:text-blue-300"
                  : "border-slate-300 text-slate-600 hover:bg-slate-50 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800"
              }`}
            >
              {w === "ALL" ? "All" : w}
            </button>
          ))}
        </div>
      </div>
      <HistoryBody
        mode={mode}
        state={state}
        outstanding={outstanding}
        gridLine={theme.gridLine}
        textMuted={theme.textMuted}
        accents={theme.accent}
      />
    </section>
  );
}

interface HistoryBodyProps {
  readonly mode: ReturnType<typeof resolveHistoryMode>;
  readonly state: ReturnType<typeof useAsync<HistoryFetchResult>>;
  readonly outstanding: number | null;
  readonly gridLine: string;
  readonly textMuted: string;
  readonly accents: readonly string[];
}

function HistoryBody({
  mode,
  state,
  outstanding,
  gridLine,
  textMuted,
  accents,
}: HistoryBodyProps): JSX.Element {
  if (mode.kind === "unsupported") {
    if (mode.reason === "no_cik") {
      return (
        <EmptyState
          title="No per-holder history"
          description="This holder has no resolved CIK, so no filing series can be queried. Pick a holder with a CIK from the table."
        />
      );
    }
    return (
      <EmptyState
        title="Per-holder view only"
        description={
          mode.reason === "etfs"
            ? "ETF filers are part of the 13F institutions line (no per-type split in the observations yet). Click a holder row to chart one filer."
            : "Insider and blockholder filings are event-driven — only a per-holder series is honest. Click a holder row to chart one filer."
        }
      />
    );
  }
  if (state.loading) return <SectionSkeleton rows={6} />;
  if (state.error !== null || state.data === null) {
    return <SectionError onRetry={state.refetch} />;
  }

  const { rows, lines } = buildHistoryRows(state.data.lines);
  // The partial-failure note must survive the empty branch — an empty
  // surviving series + a hidden failure would read as a clean empty
  // dataset (Codex ckpt-2 S3).
  const failedNote =
    state.data.failed.length > 0 ? (
      <p className="mb-2 text-xs text-amber-700 dark:text-amber-400">
        {state.data.failed.join(", ")} failed to load — showing the rest.
      </p>
    ) : null;
  if (rows.length === 0) {
    return (
      <div>
        {failedNote}
        <EmptyState
          title="No history yet"
          description="Ownership history appears after the 13F / XBRL backfill drains for this instrument."
        />
      </div>
    );
  }

  return (
    <div>
      {failedNote}
      <div style={{ width: "100%", height: 280 }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={[...rows]} margin={{ top: 8, right: 16, bottom: 4, left: 8 }}>
            <CartesianGrid stroke={gridLine} strokeDasharray="3 3" />
            <XAxis
              dataKey="period_end"
              tick={{ fontSize: 11, fill: textMuted }}
              tickFormatter={formatPeriodTick}
            />
            <YAxis
              tick={{ fontSize: 11, fill: textMuted }}
              tickFormatter={formatSharesTick}
              width={56}
            />
            <Tooltip
              content={
                <HistoryTooltip
                  outstanding={outstanding}
                  points={state.data.lines}
                />
              }
            />
            {lines.map((l, i) => (
              <Line
                key={l.key}
                dataKey={l.key}
                name={l.label}
                type="monotone"
                stroke={accents[i % accents.length]}
                dot={{ r: 2 }}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
      <ul className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs">
        {lines.map((l, i) => (
          <li key={l.key} className="flex items-center gap-1.5">
            <span
              aria-hidden
              className="inline-block h-0.5 w-4"
              style={{ backgroundColor: accents[i % accents.length] }}
            />
            <span className="text-slate-700 dark:text-slate-200">{l.label}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

/** ``2026-03-31`` → ``Q1 '26`` (13F buckets are quarter ends; other
 *  sources still read sensibly as month-year). */
function formatPeriodTick(period_end: string): string {
  const y = period_end.slice(2, 4);
  const m = Number(period_end.slice(5, 7));
  if (m === 3) return `Q1 '${y}`;
  if (m === 6) return `Q2 '${y}`;
  if (m === 9) return `Q3 '${y}`;
  if (m === 12) return `Q4 '${y}`;
  return `${period_end.slice(5, 7)}/${y}`;
}

function formatSharesTick(n: number): string {
  const abs = Math.abs(n);
  if (abs >= 1e9) return `${(n / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${(n / 1e3).toFixed(0)}K`;
  return String(n);
}

interface HistoryTooltipProps {
  readonly active?: boolean;
  readonly label?: string;
  readonly payload?: readonly {
    readonly dataKey?: string | number;
    readonly name?: string | number;
    readonly value?: number | string;
  }[];
  readonly outstanding: number | null;
  readonly points: readonly HistoryLine[];
}

function HistoryTooltip(props: HistoryTooltipProps): JSX.Element | null {
  if (!props.active || props.payload === undefined || props.payload.length === 0) return null;
  const period = props.label ?? "";
  return (
    <div className="rounded border border-slate-300 bg-white px-3 py-2 text-xs shadow-md dark:border-slate-700 dark:bg-slate-900">
      <div className="font-medium text-slate-900 dark:text-slate-100">{period}</div>
      {props.payload.map((entry) => {
        const shares = typeof entry.value === "number" ? entry.value : null;
        const line = props.points.find((l) => l.key === entry.dataKey);
        const point = line?.points.find((p) => p.period_end === period);
        return (
          <div key={String(entry.dataKey)} className="text-slate-600 dark:text-slate-400">
            {String(entry.name)}: {formatShares(shares)}
            {props.outstanding !== null && shares !== null && props.outstanding > 0 && (
              <> · {formatPct(shares / props.outstanding)} of current outstanding</>
            )}
            {point?.holder_count != null && <> · {point.holder_count} filers</>}
          </div>
        );
      })}
    </div>
  );
}
