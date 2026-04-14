import { Link } from "react-router-dom";
import type { PositionItem, PortfolioMirrorItem } from "@/api/types";
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
import { formatMoney, formatNumber, formatPct, pnlPct } from "@/lib/format";
import { EmptyState } from "@/components/states/EmptyState";

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
}: {
  positions: PositionItem[];
  mirrors?: PortfolioMirrorItem[];
}) {
  const currency = useDisplayCurrency();
  if (positions.length === 0 && mirrors.length === 0) {
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

  // Build a unified sorted list: positions use market_value, mirrors use mirror_equity.
  type RowItem =
    | { kind: "position"; data: PositionItem }
    | { kind: "mirror"; data: PortfolioMirrorItem };

  const rows: RowItem[] = [
    ...positions.map((p) => ({ kind: "position" as const, data: p })),
    ...mirrors.map((m) => ({ kind: "mirror" as const, data: m })),
  ];

  rows.sort((a, b) => {
    const mvA = a.kind === "position" ? a.data.market_value : a.data.mirror_equity;
    const mvB = b.kind === "position" ? b.data.market_value : b.data.mirror_equity;
    return mvB - mvA;
  });

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase text-slate-500">
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
              <PositionRow key={`pos-${row.data.instrument_id}`} p={row.data} currency={currency} />
            ) : (
              <MirrorRow key={`mir-${row.data.mirror_id}`} m={row.data} currency={currency} />
            ),
          )}
        </tbody>
      </table>
    </div>
  );
}

function PositionRow({ p, currency }: { p: PositionItem; currency: string }) {
  const pct = pnlPct(p.unrealized_pnl, p.cost_basis);
  const positive = p.unrealized_pnl >= 0;
  return (
    <tr className="border-t border-slate-100">
      <Td>
        <Link
          to={`/instruments/${p.instrument_id}`}
          className="font-medium text-blue-600 hover:underline"
        >
          {p.symbol}
        </Link>
      </Td>
      <Td className="hidden sm:table-cell">
        <span className="text-slate-700">{p.company_name}</span>
      </Td>
      <Td align="right">{formatNumber(p.current_units)}</Td>
      <Td align="right">{formatMoney(p.cost_basis, currency)}</Td>
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
}

/** eToro-style colour derived from the username string. */
const AVATAR_TONES = [
  "bg-blue-600",
  "bg-emerald-600",
  "bg-amber-600",
  "bg-rose-600",
  "bg-violet-600",
  "bg-cyan-600",
] as const;

function avatarTone(name: string): string {
  let hash = 0;
  for (let i = 0; i < name.length; i++) hash = (hash * 31 + name.charCodeAt(i)) | 0;
  return AVATAR_TONES[Math.abs(hash) % AVATAR_TONES.length] ?? "bg-blue-600";
}

function MirrorRow({ m, currency }: { m: PortfolioMirrorItem; currency: string }) {
  const pct = pnlPct(m.unrealized_pnl, m.funded);
  const positive = m.unrealized_pnl >= 0;
  return (
    <tr className="border-t border-slate-100">
      <Td>
        <Link
          to={`/copy-trading/${m.mirror_id}`}
          className="group flex items-center gap-2 hover:no-underline"
        >
          <span
            className={`inline-flex h-7 w-7 shrink-0 items-center justify-center rounded-full text-xs font-semibold text-white ${avatarTone(m.parent_username)}`}
          >
            {m.parent_username.charAt(0).toUpperCase()}
          </span>
          <span className="font-medium text-blue-600 group-hover:underline">
            {m.parent_username}
          </span>
        </Link>
      </Td>
      <Td className="hidden sm:table-cell">
        <span className="text-slate-500">
          {m.position_count} position{m.position_count !== 1 ? "s" : ""}
        </span>
      </Td>
      <Td align="right">
        <span className="text-slate-400">—</span>
      </Td>
      <Td align="right">{formatMoney(m.funded, currency)}</Td>
      <Td align="right">
        <span className="text-slate-400">—</span>
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
