import { Pane } from "@/components/instrument/Pane";
import { Term } from "@/components/Term";
import { EmptyState } from "@/components/states/EmptyState";
import { lookupTerm } from "@/lib/glossary";
import type { InstrumentSummary, KeyStatsFieldSource } from "@/api/types";

function formatDecimal(
  value: string | null | undefined,
  opts: { percent?: boolean } = {},
): string | null {
  if (value === null || value === undefined) return null;
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  if (opts.percent) return `${(num * 100).toFixed(2)}%`;
  return num.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function formatMarketCap(value: string | null): string | null {
  if (value === null) return null;
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  if (num >= 1e12) return `${(num / 1e12).toFixed(2)}T`;
  if (num >= 1e9) return `${(num / 1e9).toFixed(2)}B`;
  if (num >= 1e6) return `${(num / 1e6).toFixed(2)}M`;
  return num.toLocaleString();
}

function FieldSourceTag({ source }: { source: KeyStatsFieldSource | undefined }) {
  if (!source) return null;
  let tone = "bg-slate-100 dark:bg-slate-800 text-slate-600";
  let label: string = source;
  switch (source) {
    case "sec_xbrl":
      tone = "bg-emerald-50 text-emerald-700";
      label = "SEC";
      break;
    case "sec_dividend_summary":
      tone = "bg-emerald-50 text-emerald-700";
      label = "SEC · div";
      break;
    case "sec_xbrl_price_missing":
      tone = "bg-amber-50 text-amber-700";
      label = "SEC · price?";
      break;
    case "unavailable":
      tone = "bg-slate-100 dark:bg-slate-800 text-slate-500";
      label = "—";
      break;
  }
  return (
    <span className={`ml-2 rounded px-1.5 py-0.5 text-[10px] uppercase ${tone}`}>
      {label}
    </span>
  );
}

interface Row {
  label: string;
  value: string;
  source?: KeyStatsFieldSource;
}

function makeRow(
  value: string | null,
  label: string,
  source: KeyStatsFieldSource | undefined,
): Row | null {
  if (value === null) return null;
  return { label, value, source };
}

function buildRows(summary: InstrumentSummary): Row[] {
  const stats = summary.key_stats;
  if (stats === null) return [];
  const fs = stats.field_source ?? {};
  const candidates: Array<Row | null> = [
    makeRow(formatMarketCap(summary.identity.market_cap), "Market cap", undefined),
    makeRow(formatDecimal(stats.pe_ratio), "P/E ratio", fs["pe_ratio"]),
    makeRow(formatDecimal(stats.pb_ratio), "P/B ratio", fs["pb_ratio"]),
    makeRow(formatDecimal(stats.dividend_yield, { percent: true }), "Dividend yield", fs["dividend_yield"]),
    makeRow(formatDecimal(stats.payout_ratio, { percent: true }), "Payout ratio", fs["payout_ratio"]),
    makeRow(formatDecimal(stats.roe, { percent: true }), "ROE", fs["roe"]),
    makeRow(formatDecimal(stats.roa, { percent: true }), "ROA", fs["roa"]),
    makeRow(formatDecimal(stats.debt_to_equity), "Debt / Equity", fs["debt_to_equity"]),
    makeRow(formatDecimal(stats.revenue_growth_yoy, { percent: true }), "Revenue growth (YoY)", fs["revenue_growth_yoy"]),
    makeRow(formatDecimal(stats.earnings_growth_yoy, { percent: true }), "Earnings growth (YoY)", fs["earnings_growth_yoy"]),
  ];
  return candidates.filter((r): r is Row => r !== null);
}

export interface KeyStatsPaneProps {
  readonly summary: InstrumentSummary;
}

export function KeyStatsPane({ summary }: KeyStatsPaneProps): JSX.Element {
  const rows = buildRows(summary);
  return (
    <Pane title="Key statistics">
      {summary.key_stats === null ? (
        <EmptyState
          title="No key stats"
          description="No provider returned key stats for this ticker."
        />
      ) : (
        // Cap dl width so on a wide grid cell the key/value columns
        // don't stretch with whitespace between them — narrow
        // stat-block content reads tighter at ~28rem (#684).
        <dl className="grid max-w-md grid-cols-[auto_1fr] gap-x-4 gap-y-2 text-sm">
          {rows.map((r) => (
            <KeyStatRow key={r.label} row={r} />
          ))}
        </dl>
      )}
    </Pane>
  );
}

function KeyStatRow({ row }: { row: Row }): JSX.Element {
  // Wrap the label in <Term> when the glossary recognises it (P/E,
  // P/B, ROE, ROA, Debt / Equity, Payout ratio, etc. are all
  // covered) — gives the operator a hover-tooltip with the formula
  // and why-it-matters line. #684.
  const hasGlossary = lookupTerm(row.label) !== null;
  return (
    <>
      <dt className="text-slate-500">
        {hasGlossary ? <Term term={row.label} /> : row.label}
      </dt>
      <dd className="flex items-center tabular-nums">
        <span>{row.value}</span>
        <FieldSourceTag source={row.source} />
      </dd>
    </>
  );
}
