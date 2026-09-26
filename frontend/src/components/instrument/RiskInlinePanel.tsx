/**
 * RiskInlinePanel — the instrument report's inline risk (#3390 slice 3).
 *
 * Reads the persisted 3-year window from `/instruments/{symbol}/risk-metrics`
 * (`instrument_risk_metrics_current`, `risk_metrics.py` owns every estimator;
 * this does no risk math). Keeps what makes each figure readable: the basis
 * (price vs total return), the window, per-metric status and coverage.
 * The full drill (1Y / 3Y / All + charts) stays at `/instrument/:symbol/risk`.
 */

import { type JSX } from "react";
import { Link } from "react-router-dom";

import { fetchInstrumentRiskMetrics } from "@/api/instruments";
import type { InstrumentRiskMetrics, RiskWindowMetrics } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import {
  ReportCard,
  ReportNote,
  ReportRow,
} from "@/components/instrument/ReportCard";
import { EmptyState } from "@/components/states/EmptyState";
import { formatDate, formatNumber, formatPct, formatUnsignedPct } from "@/lib/format";
import { parseDecimal, riskStatusCopy } from "@/lib/riskView";
import { useAsync } from "@/lib/useAsync";

const INLINE_WINDOW_KEY = "3y";

const BASIS_NOTE =
  "Price-return basis unless marked: simple daily returns on closes. Volatility is the annualised sample standard deviation (√252); max drawdown is measured from the running peak; Calmar is CAGR ÷ |max drawdown|. Total-return Calmar reinvests dividends over the same drawdown.";

/** Total-return Calmar is only exact when the dividend record covers the window. */
export function totalReturnCalmarText(win: RiskWindowMetrics): string {
  switch (win.tr_status) {
    case "ok":
    case "no_dividends":
      return formatNumber(parseDecimal(win.tr_calmar), 2);
    case "tr_incomplete":
      return "withheld";
    default:
      return "—";
  }
}

/** One "label: copy" line per flagged metric, in display order. */
export function riskFlags(win: RiskWindowMetrics): string[] {
  const flags: [string, string | null][] = [
    ["Volatility", riskStatusCopy(win.vol_status)],
    ["Max drawdown", riskStatusCopy(win.drawdown_status)],
    ["Calmar", riskStatusCopy(win.calmar_status)],
    ["Beta", riskStatusCopy(win.beta_status)],
  ];
  const out = flags
    .filter((f): f is [string, string] => f[1] !== null)
    .map(([label, copy]) => `${label}: ${copy}`);
  if (win.tr_status === "tr_incomplete") {
    out.push(
      "Total return: the dividend record does not cover this window, so a total-return figure would be understated.",
    );
  }
  return out;
}

export function RiskInlinePanel({ symbol }: { readonly symbol: string }): JSX.Element {
  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const risk = useAsync<InstrumentRiskMetrics>(
    () => fetchInstrumentRiskMetrics(symbol),
    [symbol],
  );
  return (
    <ReportCard title="Risk (3-year window)">
      {risk.loading ? (
        <SectionSkeleton rows={4} />
      ) : risk.error !== null || risk.data === null ? (
        <SectionError onRetry={risk.refetch} />
      ) : (
        <RiskBody symbol={symbol} data={risk.data} />
      )}
    </ReportCard>
  );
}

function RiskBody({
  symbol,
  data,
}: {
  symbol: string;
  data: InstrumentRiskMetrics;
}): JSX.Element {
  const win = data.windows.find((w) => w.window_key === INLINE_WINDOW_KEY) ?? null;
  if (win === null) {
    return (
      <EmptyState
        title="No risk metrics computed"
        description="The weekly risk-metrics refresh writes these once the instrument has a price history."
      />
    );
  }
  const beta = parseDecimal(win.beta);
  const r2 = parseDecimal(win.beta_r2);
  const flags = riskFlags(win);
  return (
    <>
      <div className="space-y-0.5">
        <ReportRow
          label="volatility (annualised)"
          value={formatUnsignedPct(parseDecimal(win.vol_annualized))}
        />
        <ReportRow label="max drawdown" value={formatPct(parseDecimal(win.max_drawdown))} />
        <ReportRow
          label="Calmar (price return)"
          value={formatNumber(parseDecimal(win.calmar), 2)}
        />
        <ReportRow label="Calmar (total return)" value={totalReturnCalmarText(win)} />
        <ReportRow
          label={`beta vs ${data.benchmark_symbol ?? "benchmark"}`}
          value={
            beta === null
              ? "—"
              : `${formatNumber(beta, 2)}${r2 === null ? "" : ` (R² ${formatNumber(r2, 2)})`}`
          }
        />
      </div>
      <ReportNote>
        As of {formatDate(data.as_of_date)} · {win.n_returns ?? "—"} daily returns in the
        window · {data.metric_version}
      </ReportNote>
      {flags.length > 0 && (
        <ul className="mt-1 list-disc space-y-0.5 pl-4 text-[10px] text-amber-700 dark:text-amber-400">
          {flags.map((f) => (
            <li key={f}>{f}</li>
          ))}
        </ul>
      )}
      <ReportNote>{BASIS_NOTE}</ReportNote>
      <Link
        to={`/instrument/${encodeURIComponent(symbol)}/risk`}
        className="mt-1.5 inline-block text-xs text-blue-600 hover:underline dark:text-blue-400"
      >
        Risk &amp; returns →
      </Link>
    </>
  );
}
