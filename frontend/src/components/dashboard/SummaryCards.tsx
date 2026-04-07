import type { PortfolioResponse } from "@/api/types";
import { formatMoney, formatPct, pnlPct } from "@/lib/format";
import { SectionSkeleton } from "@/components/dashboard/Section";

/**
 * Three top-level cards: Total AUM, Cash, Unrealized P&L.
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
 */
export function SummaryCards({ data }: { data: PortfolioResponse | null }) {
  if (data === null) {
    return (
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
        {[0, 1, 2].map((i) => (
          <div key={i} className="rounded-md border border-slate-200 bg-white p-4 shadow-sm">
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
  const pnlFraction = pnlPct(totalPnl, totalCost);

  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
      <Card label="Total AUM" value={formatMoney(data.total_aum)} />
      <Card
        label="Cash balance"
        value={formatMoney(data.cash_balance)}
        hint={data.cash_balance === null ? "unknown" : undefined}
      />
      <Card
        label="Unrealized P&L"
        value={formatMoney(totalPnl)}
        hint={pnlFraction === null ? undefined : formatPct(pnlFraction)}
        tone={totalPnl >= 0 ? "positive" : "negative"}
      />
    </div>
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
      ? "text-emerald-600"
      : tone === "negative"
        ? "text-red-600"
        : "text-slate-900";
  return (
    <div className="rounded-md border border-slate-200 bg-white p-4 shadow-sm">
      <div className="text-xs uppercase tracking-wide text-slate-500">{label}</div>
      <div className={`mt-1 text-2xl font-semibold ${toneClass}`}>{value}</div>
      {hint ? <div className="mt-1 text-xs text-slate-500">{hint}</div> : null}
    </div>
  );
}
