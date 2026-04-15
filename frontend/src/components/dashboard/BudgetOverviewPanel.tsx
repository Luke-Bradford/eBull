import type { BudgetStateResponse } from "@/api/types";
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
import { formatMoney, formatPct } from "@/lib/format";
import { SectionSkeleton } from "@/components/dashboard/Section";

/**
 * Read-only budget overview for the Dashboard sidebar.
 *
 * Shows working budget breakdown, cash buffer, tax provision, and tax year.
 * Fetched via an independent `useAsync(fetchBudget)` in DashboardPage —
 * loading / error / data states are handled per the async-data-loading skill.
 */
export function BudgetOverviewPanel({
  budget,
  loading,
  hasError,
  onRetry,
}: {
  budget: BudgetStateResponse | null;
  loading: boolean;
  hasError: boolean;
  onRetry: () => void;
}) {
  const currency = useDisplayCurrency();

  if (hasError) {
    return (
      <div
        role="alert"
        className="flex items-center justify-between rounded border border-red-200 bg-red-50 px-2 py-1 text-xs text-red-700"
      >
        <span>/budget failed to load.</span>
        <button
          type="button"
          onClick={onRetry}
          className="rounded border border-red-300 bg-white px-2 py-0.5 text-[10px] font-medium text-red-700 hover:bg-red-100"
        >
          Retry
        </button>
      </div>
    );
  }

  if (loading || budget === null) {
    return <SectionSkeleton rows={5} />;
  }

  return (
    <div className="space-y-3">
      <dl className="space-y-2 text-sm">
        <Row label="Working budget" value={formatMoney(budget.working_budget, currency)} />
        <Row label="Cash balance" value={formatMoney(budget.cash_balance, currency)} />
        <Row label="Deployed capital" value={formatMoney(budget.deployed_capital, currency)} />
        <Row label="Mirror equity" value={formatMoney(budget.mirror_equity, currency)} />
        <Row
          label={`Cash buffer (${formatPct(budget.cash_buffer_pct)})`}
          value={formatMoney(budget.cash_buffer_reserve, currency)}
        />
        <Row
          label={`Tax provision (${budget.cgt_scenario})`}
          value={`${formatMoney(budget.estimated_tax_gbp, "GBP")} / ${formatMoney(budget.estimated_tax_usd, "USD")}`}
        />
        <Row label="Tax year" value={budget.tax_year} />
      </dl>
    </div>
  );
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between gap-4">
      <dt className="text-slate-500">{label}</dt>
      <dd className="shrink-0 text-right tabular-nums text-slate-700">{value}</dd>
    </div>
  );
}
