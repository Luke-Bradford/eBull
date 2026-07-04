import { useMemo, useState } from "react";
import { useAsync } from "@/lib/useAsync";
import {
  fetchTaxSummary,
  fetchTaxDisposals,
  fetchTaxPools,
  fetchTaxYears,
  type TaxSummary,
  type TaxDisposal,
  type S104Pool,
} from "@/api/tax";
import { formatMoney, formatNumber, formatDate, formatDateTime } from "@/lib/format";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { ErrorBanner } from "@/components/states/ErrorBanner";

/**
 * Tax & CGT page (#1905 PR2) — read-only view over the UK tax engine.
 *
 * Money is always GBP: the disposal engine denominates in statutory GBP by
 * law, so every figure here is formatted with a hardcoded "GBP" and does NOT
 * follow the operator's display currency (settled-decisions: the tax
 * `fx_rates` table, sql/013). All CGT treatment lives in the engine; this
 * page only shapes its four read endpoints for the operator.
 *
 * Each endpoint drives its own {loading, error} surface (async-data-loading
 * skill). The top banner fires only when every source has failed.
 */

const GBP = "GBP";

const RULE_LABELS: Record<string, string> = {
  same_day: "Same-day",
  bed_and_breakfast: "Bed & breakfast (30-day)",
  s104_pool: "Section 104 pool",
};

function ruleLabel(rule: string): string {
  return RULE_LABELS[rule] ?? rule;
}

export function TaxPage() {
  // `null` selection means "current year"; the backend defaults to it and
  // the <select> renders years.current, so state and display stay consistent
  // without an extra refetch on first paint.
  const [selectedYear, setSelectedYear] = useYearSelection();

  const years = useAsync(() => fetchTaxYears(), []);
  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const summary = useAsync(() => fetchTaxSummary(selectedYear ?? undefined), [selectedYear]);
  const disposals = useAsync(() => fetchTaxDisposals(selectedYear ?? undefined), [selectedYear]);
  const pools = useAsync(() => fetchTaxPools(), []);

  const allFailed =
    years.error !== null &&
    summary.error !== null &&
    disposals.error !== null &&
    pools.error !== null;

  // Prefer the year the summary actually resolved to (backend echoes it), so
  // labels/CSV names track the real year even while `selectedYear` is null.
  const effectiveYear = summary.data?.tax_year ?? selectedYear ?? years.data?.current ?? "";

  return (
    <div className="space-y-6 p-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">Tax &amp; CGT</h1>
        {years.error !== null ? (
          // The selector owns its own error surface: a tax-years failure leaves
          // the rest of the page usable (summary/disposals default to the
          // current year), so surface an inline retry rather than the top
          // banner or a dead disabled dropdown.
          <button
            type="button"
            onClick={years.refetch}
            className="rounded border border-amber-300 bg-amber-50 px-2 py-1 text-[11px] font-medium text-amber-700 hover:bg-amber-100 dark:border-amber-900/60 dark:bg-amber-950/40 dark:text-amber-300"
          >
            Tax years unavailable — retry
          </button>
        ) : (
          <YearSelect
            years={years.data}
            value={selectedYear ?? years.data?.current ?? ""}
            onChange={setSelectedYear}
          />
        )}
      </div>

      <p className="text-xs text-slate-500 dark:text-slate-400">
        UK capital-gains treatment for realised disposals — same-day, bed-&amp;-breakfast (30-day)
        and Section 104 pool matching. All figures are statutory GBP. CGT estimates are indicative
        only and assume the whole taxable gain falls in a single rate band.
      </p>

      {allFailed ? (
        <ErrorBanner message="Failed to load tax data. Check the browser console for details." />
      ) : null}

      <Section title={`Summary — ${effectiveYear || "current year"}`}>
        {summary.error !== null ? (
          <SectionError onRetry={summary.refetch} />
        ) : summary.loading || summary.data === null ? (
          <SectionSkeleton rows={3} />
        ) : (
          <TaxSummaryView summary={summary.data} />
        )}
      </Section>

      <Section
        title="Disposals"
        action={
          disposals.data && disposals.data.length > 0 ? (
            <button
              type="button"
              onClick={() => downloadDisposalsCsv(disposals.data ?? [], effectiveYear)}
              className="rounded border border-slate-300 bg-white px-2 py-1 text-[11px] font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800"
            >
              Export CSV
            </button>
          ) : null
        }
      >
        {disposals.error !== null ? (
          <SectionError onRetry={disposals.refetch} />
        ) : disposals.loading || disposals.data === null ? (
          <SectionSkeleton rows={4} />
        ) : disposals.data.length === 0 ? (
          <EmptyState
            title="No disposals in this tax year"
            description="Disposal matches appear once positions are closed and the daily tax reconciliation job has run for a date with a USD→GBP FX rate."
          />
        ) : (
          <DisposalsTable rows={disposals.data} />
        )}
      </Section>

      <Section title="Section 104 pools">
        {pools.error !== null ? (
          <SectionError onRetry={pools.refetch} />
        ) : pools.loading || pools.data === null ? (
          <SectionSkeleton rows={4} />
        ) : pools.data.length === 0 ? (
          <EmptyState
            title="No open pools"
            description="Each held instrument gets a Section 104 pool once shares are acquired and reconciled."
          />
        ) : (
          <PoolsTable rows={pools.data} />
        )}
      </Section>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Year selection — state hook + <select>
// ---------------------------------------------------------------------------

/** `null` = current tax year (backend default). */
function useYearSelection(): [string | null, (v: string | null) => void] {
  const [year, setYear] = useState<string | null>(null);
  return [year, setYear];
}

function YearSelect({
  years,
  value,
  onChange,
}: {
  years: { current: string; available: string[] } | null;
  value: string;
  onChange: (v: string | null) => void;
}) {
  // Option set: the current year plus every year with data (newest first),
  // de-duplicated. `current` is always selectable even before any disposals
  // exist, so the operator can view the empty-but-valid current-year summary.
  const options = useMemo(() => {
    if (!years) return value ? [value] : [];
    const seen = new Set<string>();
    const out: string[] = [];
    for (const y of [years.current, ...years.available]) {
      if (!seen.has(y)) {
        seen.add(y);
        out.push(y);
      }
    }
    return out;
  }, [years, value]);

  return (
    <label className="flex items-center gap-2 text-xs text-slate-500 dark:text-slate-400">
      Tax year
      <select
        value={value}
        onChange={(e) => onChange(e.target.value || null)}
        disabled={years === null}
        className="rounded border border-slate-200 bg-white px-2 py-1.5 text-sm text-slate-700 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
      >
        {options.map((y) => (
          <option key={y} value={y}>
            {y}
          </option>
        ))}
      </select>
    </label>
  );
}

// ---------------------------------------------------------------------------
// Summary — headline cards + exempt-allowance gauge
// ---------------------------------------------------------------------------

function TaxSummaryView({ summary: s }: { summary: TaxSummary }) {
  const disposalCount =
    s.disposals_same_day + s.disposals_bed_and_breakfast + s.disposals_s104;
  const netPositive = s.net_gain_gbp >= 0;

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-x-6 gap-y-3 sm:grid-cols-3 lg:grid-cols-4">
        <Metric
          label="Net gain"
          value={formatMoney(s.net_gain_gbp, GBP)}
          tone={netPositive ? "pos" : "neg"}
        />
        <Metric label="Gains" value={formatMoney(s.total_gains_gbp, GBP)} tone="pos" />
        <Metric label="Losses" value={formatMoney(s.total_losses_gbp, GBP)} tone="neg" />
        <Metric label="Dividends" value={formatMoney(s.dividend_total_gbp, GBP)} />
        <Metric
          label="Est. CGT (basic)"
          value={formatMoney(s.estimated_cgt_basic_scenario, GBP)}
        />
        <Metric
          label="Est. CGT (higher)"
          value={formatMoney(s.estimated_cgt_higher_scenario, GBP)}
        />
        <Metric label="Disposals" value={formatNumber(disposalCount, 0)} />
        <Metric
          label="Exempt remaining"
          value={formatMoney(s.exempt_remaining_gbp, GBP)}
        />
      </div>

      <ExemptGauge
        netGain={s.net_gain_gbp}
        allowance={s.annual_exempt_gbp}
        remaining={s.exempt_remaining_gbp}
      />

      <p className="text-[11px] text-slate-500 dark:text-slate-400">
        Matched: {s.disposals_same_day} same-day · {s.disposals_bed_and_breakfast} bed-&amp;-breakfast
        · {s.disposals_s104} Section 104.
      </p>
    </div>
  );
}

function Metric({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone?: "pos" | "neg";
}) {
  const valueClass =
    tone === "pos"
      ? "text-emerald-600 dark:text-emerald-400"
      : tone === "neg"
        ? "text-red-600 dark:text-red-400"
        : "text-slate-800 dark:text-slate-100";
  return (
    <div>
      <div className="text-xs text-slate-500 dark:text-slate-400">{label}</div>
      <div className={`text-lg font-semibold tabular-nums ${valueClass}`}>{value}</div>
    </div>
  );
}

/**
 * Annual-exempt allowance gauge: how much of the £3,000 CGT allowance the
 * year's *positive* net gain has consumed. Losses do not restore allowance,
 * so a net loss leaves the full allowance intact (bar empty).
 */
function ExemptGauge({
  netGain,
  allowance,
  remaining,
}: {
  netGain: number;
  allowance: number;
  remaining: number;
}) {
  const used = Math.max(0, allowance - remaining);
  const overBy = Math.max(0, netGain - allowance);
  const pctUsed = allowance > 0 ? Math.min(1, used / allowance) : 0;
  const breached = overBy > 0;

  return (
    <div className="rounded-md border border-slate-200 bg-white p-3 dark:border-slate-800 dark:bg-slate-900">
      <div className="mb-1 flex items-baseline justify-between text-xs">
        <span className="font-medium text-slate-600 dark:text-slate-300">
          Annual exempt allowance
        </span>
        <span className="tabular-nums text-slate-500 dark:text-slate-400">
          {formatMoney(used, GBP)} of {formatMoney(allowance, GBP)} used
        </span>
      </div>
      <div className="h-2 w-full overflow-hidden rounded-full bg-slate-100 dark:bg-slate-800">
        <div
          className={`h-full rounded-full ${breached ? "bg-red-500" : "bg-emerald-500"}`}
          style={{ width: `${(pctUsed * 100).toFixed(1)}%` }}
        />
      </div>
      <div className="mt-1 text-[11px] text-slate-500 dark:text-slate-400">
        {breached ? (
          <span className="text-red-600 dark:text-red-400">
            Taxable: {formatMoney(overBy, GBP)} above the allowance.
          </span>
        ) : (
          <span>{formatMoney(remaining, GBP)} of allowance remaining.</span>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Disposals table
// ---------------------------------------------------------------------------

function DisposalsTable({ rows }: { rows: TaxDisposal[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase text-slate-500 dark:text-slate-400">
          <tr>
            <th className="px-2 py-2 text-left">Date</th>
            <th className="px-2 py-2 text-left">Instrument</th>
            <th className="px-2 py-2 text-left">Rule</th>
            <th className="px-2 py-2 text-right">Units</th>
            <th className="px-2 py-2 text-right">Cost</th>
            <th className="px-2 py-2 text-right">Proceeds</th>
            <th className="px-2 py-2 text-right">Gain / loss</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => {
            const positive = r.gain_or_loss_gbp >= 0;
            return (
              <tr key={r.match_id} className="border-t border-slate-100 dark:border-slate-800">
                <td className="px-2 py-2 text-left tabular-nums text-slate-600 dark:text-slate-300">
                  {formatDate(r.disposal_uk_date)}
                </td>
                <td className="px-2 py-2 text-left font-medium text-slate-800 dark:text-slate-100">
                  {r.symbol}
                </td>
                <td className="px-2 py-2 text-left text-slate-600 dark:text-slate-300">
                  {ruleLabel(r.matching_rule)}
                </td>
                <td className="px-2 py-2 text-right tabular-nums">
                  {formatNumber(r.matched_units)}
                </td>
                <td className="px-2 py-2 text-right tabular-nums">
                  {formatMoney(r.acquisition_cost_gbp, GBP)}
                </td>
                <td className="px-2 py-2 text-right tabular-nums">
                  {formatMoney(r.disposal_proceeds_gbp, GBP)}
                </td>
                <td className="px-2 py-2 text-right tabular-nums">
                  <span
                    className={
                      positive ? "text-emerald-600 dark:text-emerald-400" : "text-red-600 dark:text-red-400"
                    }
                  >
                    {formatMoney(r.gain_or_loss_gbp, GBP)}
                  </span>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Section 104 pools table
// ---------------------------------------------------------------------------

function PoolsTable({ rows }: { rows: S104Pool[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase text-slate-500 dark:text-slate-400">
          <tr>
            <th className="px-2 py-2 text-left">Instrument</th>
            <th className="px-2 py-2 text-right">Pool units</th>
            <th className="px-2 py-2 text-right">Pool cost</th>
            <th className="px-2 py-2 text-right">Avg cost / unit</th>
            <th className="px-2 py-2 text-right">Updated</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr
              key={r.instrument_id}
              className="border-t border-slate-100 dark:border-slate-800"
            >
              <td className="px-2 py-2 text-left font-medium text-slate-800 dark:text-slate-100">
                {r.symbol}
              </td>
              <td className="px-2 py-2 text-right tabular-nums">{formatNumber(r.pool_units)}</td>
              <td className="px-2 py-2 text-right tabular-nums">
                {formatMoney(r.pool_cost_gbp, GBP)}
              </td>
              <td className="px-2 py-2 text-right tabular-nums">
                {formatMoney(r.pool_avg_cost_gbp, GBP)}
              </td>
              <td className="px-2 py-2 text-right tabular-nums text-slate-500 dark:text-slate-400">
                {formatDateTime(r.updated_at)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// CSV export — client-side blob download (mirrors RawOhlcvTable.downloadCsv)
// ---------------------------------------------------------------------------

function csvCell(value: string | number): string {
  const s = String(value);
  // Quote if the value contains a comma, quote, or newline; double embedded quotes.
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function downloadDisposalsCsv(rows: TaxDisposal[], taxYear: string): void {
  const header = [
    "disposal_date",
    "symbol",
    "matching_rule",
    "matched_units",
    "acquisition_cost_gbp",
    "disposal_proceeds_gbp",
    "gain_or_loss_gbp",
  ].join(",");
  const body = rows
    .map((r) =>
      [
        r.disposal_uk_date,
        csvCell(r.symbol),
        csvCell(ruleLabel(r.matching_rule)),
        r.matched_units,
        r.acquisition_cost_gbp,
        r.disposal_proceeds_gbp,
        r.gain_or_loss_gbp,
      ].join(","),
    )
    .join("\n");
  const csv = `${header}\n${body}\n`;
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `tax-disposals-${taxYear.replace("/", "-") || "current"}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
