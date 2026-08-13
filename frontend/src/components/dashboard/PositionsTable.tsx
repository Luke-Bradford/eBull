import { Link } from "react-router-dom";
import type { PositionItem, PortfolioMirrorItem } from "@/api/types";
import { formatMoney, formatNumber, formatPct, pnlPct } from "@/lib/format";
import { EmptyState } from "@/components/states/EmptyState";
import { UnconvertedBadge } from "@/components/portfolio/UnconvertedBadge";
import { LivePriceCell } from "@/components/quotes/LivePriceCell";
import { Avatar } from "@/lib/avatar";
import { buildSortedRows } from "@/lib/portfolioRows";

/**
 * Positions table — unified view of direct positions and copy-trading mirrors.
 *
 * Mirror rows appear alongside position rows, sorted together by market value
 * descending. Mirrors render with an eToro-style initials avatar and show
 * invested / equity / P&L in the existing financial columns.
 *
 * Each position row links to the instrument detail page (#62).
 * Each mirror row links to /copy-trading/:mirrorId for drill-down.
 */
export function PositionsTable({
  positions,
  mirrors = [],
  displayCurrency,
  cashCurrency,
}: {
  positions: PositionItem[];
  mirrors?: PortfolioMirrorItem[];
  /** The currency the backend converted to (PortfolioResponse.display_currency).
   *  Per-position money is labelled with `position.currency` (which may be native
   *  on an FX-degrade). (#2129) */
  displayCurrency: string;
  /** Currency of mirror money (PortfolioResponse.cash_currency; USD-base, may be
   *  native "USD" on an FX-degrade). (#2129) */
  cashCurrency: string;
}) {
  if (positions.length === 0 && mirrors.length === 0) {
    return (
      <EmptyState
        title="No positions yet"
        description="Open a position from the rankings page to see it here."
      >
        <Link to="/research?view=ranked" className="text-sm font-medium text-blue-600 hover:underline">
          Go to rankings →
        </Link>
      </EmptyState>
    );
  }

  // Positions + mirrors merged, sorted by dollar value (#1901 shared builder).
  const rows = buildSortedRows(positions, mirrors);

  // The live-quote stream is owned by the parent page (Dashboard or
  // Portfolio) so the instrument-id union — held positions + every
  // active-mirror underlying — is computed once at the page level
  // off ``PortfolioResponse.live_quote_instrument_ids``. Wrapping
  // here would either duplicate the SSE stream or hard-fix the set
  // to held positions only (regression for #501 mirror underlyings).
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase text-slate-500 dark:text-slate-400">
          <tr>
            <Th>Name</Th>
            <Th className="hidden sm:table-cell" />
            <Th align="right">Units</Th>
            <Th align="right">Invested</Th>
            <Th align="right">Price</Th>
            <Th align="right">Value</Th>
            <Th align="right">P&L</Th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) =>
            row.kind === "position" ? (
              <PositionRow
                key={`pos-${row.data.instrument_id}`}
                p={row.data}
                displayCurrency={displayCurrency}
              />
            ) : (
              <MirrorRow
                key={`mir-${row.data.mirror_id}`}
                m={row.data}
                currency={cashCurrency}
                displayCurrency={displayCurrency}
              />
            ),
          )}
        </tbody>
      </table>
    </div>
  );
}

function PositionRow({ p, displayCurrency }: { p: PositionItem; displayCurrency: string }) {
  const pct = pnlPct(p.unrealized_pnl, p.cost_basis);
  const positive = p.unrealized_pnl >= 0;
  // Money is denominated in the position's own currency — the display currency
  // normally, or the native currency when the FX rate was missing (#2129). Label
  // every cell with it (not the account display currency) so a native magnitude is
  // never stamped with the display symbol.
  const rowCurrency = p.currency;
  const unconverted = rowCurrency !== displayCurrency;
  return (
    <tr className="border-t border-slate-100">
      <Td>
        <Link
          to={`/instrument/${encodeURIComponent(p.symbol)}`}
          className="font-medium text-blue-600 hover:underline"
        >
          {p.symbol}
        </Link>
        {unconverted && <UnconvertedBadge currency={rowCurrency} displayCurrency={displayCurrency} />}
      </Td>
      <Td className="hidden sm:table-cell">
        <span className="text-slate-700">{p.company_name}</span>
      </Td>
      <Td align="right">{formatNumber(p.current_units)}</Td>
      <Td align="right">{formatMoney(p.cost_basis, rowCurrency)}</Td>
      <Td align="right">
        <LivePriceCell
          instrumentId={p.instrument_id}
          fallback={p.current_price}
          currency={rowCurrency}
        />
      </Td>
      <Td align="right">{formatMoney(p.market_value, rowCurrency)}</Td>
      <Td align="right">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {formatMoney(p.unrealized_pnl, rowCurrency)}
          {pct === null ? "" : ` (${formatPct(pct)})`}
        </span>
      </Td>
    </tr>
  );
}

function MirrorRow({
  m,
  currency,
  displayCurrency,
}: {
  m: PortfolioMirrorItem;
  currency: string;
  displayCurrency: string;
}) {
  const pct = pnlPct(m.unrealized_pnl, m.funded);
  const positive = m.unrealized_pnl >= 0;
  const unconverted = currency !== displayCurrency;
  return (
    <tr className="border-t border-slate-100">
      <Td>
        <Link
          to={`/copy-trading/${m.mirror_id}`}
          className="group flex items-center gap-2 hover:no-underline"
        >
          <Avatar username={m.parent_username} size="md" />
          <span className="font-medium text-blue-600 group-hover:underline">
            {m.parent_username}
          </span>
        </Link>
        {unconverted && <UnconvertedBadge currency={currency} displayCurrency={displayCurrency} />}
      </Td>
      <Td className="hidden sm:table-cell">
        <span className="text-slate-500 dark:text-slate-400">
          {m.position_count} position{m.position_count !== 1 ? "s" : ""}
        </span>
      </Td>
      <Td align="right">
        <span className="text-slate-400 dark:text-slate-500">—</span>
      </Td>
      <Td align="right">{formatMoney(m.funded, currency)}</Td>
      <Td align="right">
        <span className="text-slate-400 dark:text-slate-500">—</span>
      </Td>
      <Td align="right">{formatMoney(m.mirror_equity, currency)}</Td>
      <Td align="right">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {formatMoney(m.unrealized_pnl, currency)}
          {pct === null ? "" : ` (${formatPct(pct)})`}
        </span>
      </Td>
    </tr>
  );
}

function Th({
  children,
  align = "left",
  className = "",
}: {
  children?: React.ReactNode;
  align?: "left" | "right";
  className?: string;
}) {
  return (
    <th className={`px-2 py-2 ${align === "right" ? "text-right" : "text-left"} ${className}`}>
      {children}
    </th>
  );
}

function Td({
  children,
  align = "left",
  className = "",
}: {
  children?: React.ReactNode;
  align?: "left" | "right";
  className?: string;
}) {
  return (
    <td
      className={`px-2 py-2 ${align === "right" ? "text-right tabular-nums" : "text-left"} ${className}`}
    >
      {children}
    </td>
  );
}
