/**
 * /instrument/:symbol/risk — risk & return drill page (#591 PR-C).
 *
 * Pure renderer of GET /instruments/{symbol}/risk-metrics. The backend
 * (risk_metrics.py) owns every estimator — drawdown, rolling vol, the
 * histogram bins, the OLS beta — and serves both the persisted scalars
 * and the on-read display series. This page does NO risk math: it picks
 * the window matching the range, slices the dated series client-side for
 * the charts, and formats. Single math source, no TS/Python drift.
 *
 * Range picker (1Y / 3Y / 5Y / All) is display-only: 5Y ≡ All ≡ the
 * persisted `full` window given the ~4yr data ceiling.
 */

import { useState } from "react";
import { Link, useParams } from "react-router-dom";

import { fetchInstrumentRiskMetrics } from "@/api/instruments";
import type { InstrumentRiskMetrics, RiskWindowMetrics } from "@/api/types";
import {
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { StatTile } from "@/components/dashboard/StatTile";
import { Pane } from "@/components/instrument/Pane";
import {
  BetaScatterChart,
  ReturnsHistogram,
  RollingVolChart,
  UnderwaterChart,
} from "@/components/risk/riskCharts";
import { EmptyState } from "@/components/states/EmptyState";
import { formatNumber, formatPct, formatUnsignedPct } from "@/lib/format";
import {
  parseDecimal,
  pickWindow,
  RISK_RANGES,
  type RiskRange,
  riskStatusCopy,
  sliceByRange,
} from "@/lib/riskView";
import { useAsync } from "@/lib/useAsync";

// ---------------------------------------------------------------------------
// Range picker
// ---------------------------------------------------------------------------

function RangePicker({
  range,
  onChange,
}: {
  readonly range: RiskRange;
  readonly onChange: (r: RiskRange) => void;
}): JSX.Element {
  return (
    <div
      className="inline-flex rounded-md border border-slate-200 dark:border-slate-700"
      role="group"
      aria-label="Date range"
    >
      {RISK_RANGES.map((r) => {
        const active = r === range;
        return (
          <button
            key={r}
            type="button"
            aria-pressed={active}
            onClick={() => onChange(r)}
            className={[
              "px-3 py-1 text-xs font-medium transition-colors first:rounded-l-md last:rounded-r-md",
              active
                ? "bg-slate-800 text-white dark:bg-slate-200 dark:text-slate-900"
                : "text-slate-600 hover:bg-slate-100 dark:text-slate-300 dark:hover:bg-slate-800",
            ].join(" ")}
          >
            {r}
          </button>
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Scalar summary tiles for the selected window
// ---------------------------------------------------------------------------

function pctTone(v: number | null): "positive" | "negative" | undefined {
  if (v === null) return undefined;
  return v >= 0 ? "positive" : "negative";
}

function ScalarSummary({
  win,
}: {
  readonly win: RiskWindowMetrics | null;
}): JSX.Element {
  const cagr = parseDecimal(win?.cagr ?? null);
  const excessCagr = parseDecimal(win?.excess_cagr_vs_spy ?? null);
  const vol = parseDecimal(win?.vol_annualized ?? null);
  const beta = parseDecimal(win?.beta ?? null);
  const r2 = parseDecimal(win?.beta_r2 ?? null);
  const maxDd = parseDecimal(win?.max_drawdown ?? null);
  const curDd = parseDecimal(win?.current_drawdown ?? null);
  const calmar = parseDecimal(win?.calmar ?? null);
  const var5 = parseDecimal(win?.var_5 ?? null);
  const trailing1y = parseDecimal(win?.trailing_1y ?? null);
  const excessTrailing1y = parseDecimal(win?.excess_trailing_1y ?? null);

  return (
    <div className="grid grid-cols-2 gap-x-4 sm:grid-cols-3 lg:grid-cols-4">
      <StatTile
        label="CAGR"
        value={formatPct(cagr)}
        tone={pctTone(cagr)}
        hint={`${formatPct(excessCagr)} vs SPY`}
      />
      <StatTile label="Annualized vol" value={formatUnsignedPct(vol)} />
      <StatTile
        label="Beta vs SPY"
        value={formatNumber(beta, 2)}
        hint={`R² ${formatNumber(r2, 2)}`}
      />
      <StatTile
        label="Max drawdown"
        value={formatPct(maxDd)}
        tone={pctTone(maxDd)}
        hint={`now ${formatPct(curDd)}`}
      />
      <StatTile
        label="Calmar"
        value={formatNumber(calmar, 2)}
        hint="return ÷ |max drawdown|"
      />
      <StatTile
        label="VaR 5%"
        value={formatPct(var5)}
        tone={pctTone(var5)}
        hint="worst typical day (5%)"
      />
      <StatTile
        label="Trailing 1Y"
        value={formatPct(trailing1y)}
        tone={pctTone(trailing1y)}
        hint={`${formatPct(excessTrailing1y)} vs SPY`}
      />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Per-card wrapper — keeps the chart's "empty / flagged" branch out of the
// page body. `note` is the status-keyed honest flag; `emptyMessage` shows
// when the card's own series has no points to render.
// ---------------------------------------------------------------------------

function RiskCard({
  title,
  scope,
  note,
  empty,
  emptyMessage,
  children,
}: {
  readonly title: string;
  readonly scope?: string;
  readonly note: string | null;
  readonly empty: boolean;
  readonly emptyMessage: string;
  readonly children: React.ReactNode;
}): JSX.Element {
  return (
    <Pane title={title} scope={scope}>
      {empty ? (
        <p className="px-2 py-3 text-xs text-slate-500">
          {note ?? emptyMessage}
        </p>
      ) : (
        <>
          {note !== null ? (
            <p className="px-2 pb-1 text-[11px] text-amber-700 dark:text-amber-500">
              {note}
            </p>
          ) : null}
          {children}
        </>
      )}
    </Pane>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export function RiskPage(): JSX.Element {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const [range, setRange] = useState<RiskRange>("1Y");

  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const risk = useAsync<InstrumentRiskMetrics>(
    () => fetchInstrumentRiskMetrics(symbol),
    [symbol],
  );

  const backHref = `/instrument/${encodeURIComponent(symbol)}`;

  const data = risk.data;
  const selectedWindow = data !== null ? pickWindow(data.windows, range) : null;
  // The histogram + beta scatter are full-history (no date axis to slice), so
  // their honest status flags read from the full window regardless of range.
  const fullWindow = data !== null ? pickWindow(data.windows, "All") : null;
  const series = data?.series ?? null;

  // Time-series charts slice the full series to the picked range; the
  // histogram + scatter are full-history (no date axis to slice).
  const ddPoints = sliceByRange(
    series?.drawdown_curve ?? [],
    data?.as_of_date ?? null,
    range,
  );
  const volPoints = sliceByRange(
    series?.rolling_vol ?? [],
    data?.as_of_date ?? null,
    range,
  );

  return (
    <div className="mx-auto max-w-screen-xl space-y-4 p-6 pt-6">
      <header className="border-b border-slate-200 dark:border-slate-800 pb-3">
        <Link to={backHref} className="text-xs text-blue-600 hover:underline">
          ← Back to {symbol}
        </Link>
        <div className="mt-1 flex flex-wrap items-center justify-between gap-2">
          <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">
            Risk &amp; returns — {symbol}
          </h1>
          {data !== null && data.windows.length > 0 ? (
            <RangePicker range={range} onChange={setRange} />
          ) : null}
        </div>
        <p className="mt-1 text-xs text-slate-500">
          Realized risk from daily price returns (not the scorer's TA
          volatility term). Beta vs {data?.benchmark_symbol ?? "SPY"}; returns
          are price-only (no dividend adjustment in v1).
          {data?.as_of_date !== null && data?.as_of_date !== undefined
            ? ` As of ${data.as_of_date}.`
            : ""}
        </p>
      </header>

      {risk.loading ? (
        <SectionSkeleton rows={6} />
      ) : risk.error !== null || data === null ? (
        <SectionError onRetry={risk.refetch} />
      ) : data.windows.length === 0 || series === null ? (
        <EmptyState
          title="No risk metrics yet"
          description="Risk metrics are computed once the instrument has enough daily price history. They populate after the next candle + risk-metrics refresh."
        >
          <Link to={backHref} className="text-sm text-blue-600 hover:underline">
            ← Back to {symbol}
          </Link>
        </EmptyState>
      ) : (
        <div className="space-y-4">
          <ScalarSummary win={selectedWindow} />

          <RiskCard
            title="Drawdown (underwater)"
            scope="peak-to-trough % · selected range"
            note={riskStatusCopy(selectedWindow?.drawdown_status ?? null)}
            empty={ddPoints.length === 0}
            emptyMessage="No price history to chart drawdown."
          >
            <UnderwaterChart points={ddPoints} />
          </RiskCard>

          <RiskCard
            title="Rolling volatility"
            scope="30-day annualized · selected range"
            note={riskStatusCopy(selectedWindow?.vol_status ?? null)}
            empty={volPoints.length === 0}
            emptyMessage="Not enough history for a rolling-volatility window yet."
          >
            <RollingVolChart points={volPoints} />
          </RiskCard>

          <RiskCard
            title="Return distribution"
            scope="daily returns · full history"
            note={riskStatusCopy(fullWindow?.distribution_status ?? null)}
            empty={series.return_histogram.length === 0}
            emptyMessage="Not enough returns to chart a distribution."
          >
            <ReturnsHistogram bins={series.return_histogram} />
          </RiskCard>

          <RiskCard
            title="Beta vs benchmark"
            scope={`daily returns vs ${data.benchmark_symbol ?? "SPY"} · full history`}
            note={riskStatusCopy(fullWindow?.beta_status ?? null)}
            empty={series.beta_scatter.length === 0 || series.beta === null}
            emptyMessage="No overlapping benchmark history to fit a beta."
          >
            <BetaScatterChart
              points={series.beta_scatter}
              beta={series.beta}
              r2={series.beta_r2}
              benchmarkSymbol={data.benchmark_symbol}
            />
          </RiskCard>
        </div>
      )}
    </div>
  );
}
