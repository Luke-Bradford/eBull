import type { PortfolioResponse } from "@/api/types";
import { pnlPct } from "@/lib/format";

export interface PortfolioTotals {
  /** Σ unrealized P&L over positions AND mirrors. */
  totalPnl: number;
  /** Cost denominator: Σ position cost_basis + Σ mirror funded. */
  totalCost: number;
  /** Capital-weighted P&L fraction (totalPnl / totalCost), null when totalCost is 0. */
  pnlFraction: number | null;
  /** #2129: the currency the backend actually converted the totals to
   *  (`response.display_currency`), NOT the /config context — those can observe
   *  different config states. Label every totals cell with this. */
  displayCurrency: string;
  /** #2129: true when any money source (position/cash/mirror) stayed native on an
   *  FX-degrade, so the summed totals mix currencies under one symbol. The backend
   *  flags this (it covers cash/mirror, which carry no per-row currency); the FE
   *  must not re-derive it from positions alone. */
  hasUnconverted: boolean;
}

/**
 * Single source for the portfolio total P&L / cost / display-currency labeling
 * shared by the Dashboard cockpit (`SummaryCards`) and the Portfolio workstation
 * (`SummaryBar`) — #1901 dedup. Both previously inlined this same computation,
 * including the #2129 FX-degrade logic, risking divergence.
 *
 * Mirrors contribute to BOTH the P&L total (`unrealized_pnl`) and the cost
 * denominator (`funded`). Pure — table-tested without React.
 */
export function portfolioTotals(data: PortfolioResponse): PortfolioTotals {
  let totalPnl = 0;
  let totalCost = 0;
  for (const p of data.positions) {
    totalPnl += p.unrealized_pnl;
    totalCost += p.cost_basis;
  }
  for (const m of data.mirrors ?? []) {
    totalPnl += m.unrealized_pnl;
    totalCost += m.funded;
  }
  return {
    totalPnl,
    totalCost,
    pnlFraction: pnlPct(totalPnl, totalCost),
    displayCurrency: data.display_currency,
    hasUnconverted: data.fx_incomplete,
  };
}
