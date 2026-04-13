import { Link } from "react-router-dom";
import type { PositionItem } from "@/api/types";
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
import { formatMoney, formatNumber, formatPct, pnlPct } from "@/lib/format";
import { EmptyState } from "@/components/states/EmptyState";

/**
 * Positions table.
 *
 * Sector column intentionally omitted: PositionItem on the backend does not
 * expose `sector`. Adding it would require widening the API in the same PR
 * and is tracked as a follow-up — see PR description.
 *
 * Each row links to the instrument detail page (#62). The route exists but
 * is currently a placeholder; the link is correct so it lights up for free
 * when #62 lands.
 */
export function PositionsTable({ positions }: { positions: PositionItem[] }) {
  const currency = useDisplayCurrency();
  if (positions.length === 0) {
    return (
      <EmptyState
        title="No positions yet"
        description="Open a position from the rankings page to see it here."
      >
        <Link to="/rankings" className="text-sm font-medium text-blue-600 hover:underline">
          Go to rankings →
        </Link>
      </EmptyState>
    );
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase text-slate-500">
          <tr>
            <Th>Symbol</Th>
            <Th>Company</Th>
            <Th align="right">Units</Th>
            <Th align="right">Avg cost</Th>
            <Th align="right">Price</Th>
            <Th align="right">Market value</Th>
            <Th align="right">Unrealized P&L</Th>
          </tr>
        </thead>
        <tbody>
          {positions.map((p) => {
            const pct = pnlPct(p.unrealized_pnl, p.cost_basis);
            const positive = p.unrealized_pnl >= 0;
            return (
              <tr key={p.instrument_id} className="border-t border-slate-100">
                <Td>
                  <Link
                    to={`/instruments/${p.instrument_id}`}
                    className="font-medium text-blue-600 hover:underline"
                  >
                    {p.symbol}
                  </Link>
                </Td>
                <Td>
                  <span className="text-slate-700">{p.company_name}</span>
                </Td>
                <Td align="right">{formatNumber(p.current_units)}</Td>
                <Td align="right">{formatMoney(p.avg_cost, currency)}</Td>
                <Td align="right">
                  {p.current_price != null ? formatMoney(p.current_price, currency) : "—"}
                </Td>
                <Td align="right">{formatMoney(p.market_value, currency)}</Td>
                <Td align="right">
                  <span className={positive ? "text-emerald-600" : "text-red-600"}>
                    {formatMoney(p.unrealized_pnl, currency)}
                    {pct === null ? "" : ` (${formatPct(pct)})`}
                  </span>
                </Td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function Th({ children, align = "left" }: { children: React.ReactNode; align?: "left" | "right" }) {
  return (
    <th className={`px-2 py-2 ${align === "right" ? "text-right" : "text-left"}`}>{children}</th>
  );
}

function Td({ children, align = "left" }: { children: React.ReactNode; align?: "left" | "right" }) {
  return (
    <td className={`px-2 py-2 ${align === "right" ? "text-right tabular-nums" : "text-left"}`}>
      {children}
    </td>
  );
}
