import type { BudgetStateResponse, PortfolioResponse } from "@/api/types";
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
import { formatMoney, formatPct, pnlPct } from "@/lib/format";
import { SectionSkeleton } from "@/components/dashboard/Section";

/**
 * Four top-level cards: Total AUM, Cash, Unrealized P&L, Available for
 * deployment.
 *
 * AUM honours the settled decision: backend uses mark-to-market first and
 * falls back to cost basis when no quote exists (see app/api/portfolio.py).
 * The frontend just displays the value the API returns; it does not
 * recompute. Cash may be `null` (unknown), distinct from `0`.
 *
 * Unrealized P&L is summed from positions because the API does not yet
 * expose a top-line `unrealized_pnl` field. Percentage uses sum-of-PnL over
 * sum-of-cost-basis (capital-weighted), not an average of per-position
 * percentages.
 *
 * Design-system v1 chrome (#691): borderless, divided by hairlines on
 * sm+ screens. Replaces the prior bordered+shadowed card pattern so
 * the page reads as one editorial spread.
 */
export function SummaryCards({
  data,
  budgetData,
  budgetError,
}: {
  data: PortfolioResponse | null;
  budgetData: BudgetStateResponse | null;
  budgetError?: boolean;
}) {
  const currency = useDisplayCurrency();
  if (data === null) {
    return (
      <div className="grid grid-cols-1 gap-x-6 sm:grid-cols-2 lg:grid-cols-4">
        {[0, 1, 2, 3].map((i) => (
          <div key={i} className="border-t border-slate-200 dark:border-slate-800 px-1 pt-3 pb-1">
            <SectionSkeleton rows={2} />
          </div>
        ))}
      </div>
    );
  }

  let totalPnl = 0;
  let totalCost = 0;
  for (const p of data.positions) {
    totalPnl += p.unrealized_pnl;
    totalCost += p.cost_basis;
  }
  // Include mirror P&L in the total: mirrors contribute both to the
  // unrealized total and to the cost basis denominator (via funded amount).
  for (const m of data.mirrors ?? []) {
    totalPnl += m.unrealized_pnl;
    totalCost += m.funded;
  }
  const pnlFraction = pnlPct(totalPnl, totalCost);

  return (
    <div className="grid grid-cols-1 gap-x-6 sm:grid-cols-2 lg:grid-cols-4">
      <Card label="Total AUM" value={formatMoney(data.total_aum, currency)} />
      <Card
        label="Cash balance"
        value={formatMoney(data.cash_balance, currency)}
        hint={data.cash_balance === null ? "unknown" : undefined}
      />
      <Card
        label="Unrealized P&L"
        value={formatMoney(totalPnl, currency)}
        hint={pnlFraction === null ? undefined : formatPct(pnlFraction)}
        tone={totalPnl >= 0 ? "positive" : "negative"}
      />
      <DeploymentCard budget={budgetData} budgetError={budgetError} currency={currency} />
    </div>
  );
}

function DeploymentCard({
  budget,
  budgetError,
  currency,
}: {
  budget: BudgetStateResponse | null;
  budgetError?: boolean;
  currency: string;
}) {
  if (budget === null) {
    // Distinguish "still loading" (skeleton) from "failed" (dash + hint).
    if (budgetError) {
      return <Card label="Available for deployment" value="—" hint="Budget unavailable" />;
    }
    return (
      <div className="border-t border-slate-200 dark:border-slate-800 px-1 pt-3 pb-1">
        <SectionSkeleton rows={2} />
      </div>
    );
  }

  const available = budget.available_for_deployment;
  const isNull = available === null;
  const isLow =
    !isNull &&
    budget.working_budget !== null &&
    budget.working_budget > 0 &&
    available / budget.working_budget < 0.05;
  const isNegative = !isNull && available < 0;

  const tone: "positive" | "negative" | undefined = isNull
    ? undefined
    : isNegative || isLow
      ? "negative"
      : "positive";

  return (
    <Card
      label="Available for deployment"
      value={isNull ? "—" : formatMoney(available, currency)}
      hint={isNull ? "Cash unknown" : isLow ? "Low deployment capital" : undefined}
      tone={tone}
    />
  );
}

function Card({
  label,
  value,
  hint,
  tone,
}: {
  label: string;
  value: string;
  hint?: string;
  tone?: "positive" | "negative";
}) {
  const toneClass =
    tone === "positive"
      ? "text-emerald-600 dark:text-emerald-400"
      : tone === "negative"
        ? "text-rose-600 dark:text-rose-400"
        : "text-slate-900 dark:text-slate-100";
  return (
    <div className="border-t border-slate-200 dark:border-slate-800 px-1 pt-3 pb-1">
      <div className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate-500">
        {label}
      </div>
      <div className={`mt-1 text-2xl font-semibold tabular-nums ${toneClass}`}>
        {value}
      </div>
      {hint ? <div className="mt-1 text-xs tabular-nums text-slate-500">{hint}</div> : null}
    </div>
  );
}
