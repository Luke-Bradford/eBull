import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { fetchPortfolio } from "@/api/portfolio";
import { useAsync } from "@/lib/useAsync";
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
import { formatMoney, formatNumber, formatPct, pnlPct } from "@/lib/format";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { ClosePositionModal } from "@/components/orders/ClosePositionModal";
import { OrderEntryModal } from "@/components/orders/OrderEntryModal";
import type {
  BrokerPositionItem,
  PositionItem,
  PortfolioMirrorItem,
} from "@/api/types";

type ValuationSource = "quote" | "daily_close" | "cost_basis";

interface CloseTarget {
  instrumentId: number;
  trade: BrokerPositionItem;
  valuationSource: ValuationSource;
}

/**
 * Portfolio page — the operator's main working view.
 *
 * Dense, financial-tool aesthetic. Unified positions+mirrors table sorted
 * by value. Clicking a stock row navigates to /portfolio/:instrumentId
 * (native-currency detail view). Clicking a mirror row navigates to
 * /copy-trading/:mirrorId.
 */
export function PortfolioPage() {
  const portfolio = useAsync(fetchPortfolio, []);
  const currency = useDisplayCurrency();
  const [search, setSearch] = useState("");
  const [addFor, setAddFor] = useState<PositionItem | null>(null);
  const [closeFor, setCloseFor] = useState<CloseTarget | null>(null);

  function handleFilled() {
    // Close modals BEFORE the portfolio refetch fires so a refetch
    // error is never hidden behind an open dialog (prevention #125).
    setAddFor(null);
    setCloseFor(null);
    portfolio.refetch();
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-slate-800">Portfolio</h1>
      </div>

      {portfolio.error !== null ? (
        <SectionError onRetry={portfolio.refetch} />
      ) : portfolio.loading || portfolio.data === null ? (
        <SectionSkeleton rows={8} />
      ) : (
        <>
          <SummaryBar data={portfolio.data} currency={currency} />
          <PortfolioTable
            positions={portfolio.data.positions}
            mirrors={portfolio.data.mirrors}
            currency={currency}
            search={search}
            onSearchChange={setSearch}
            onAdd={setAddFor}
            onClose={setCloseFor}
          />
        </>
      )}

      {addFor !== null ? (
        <OrderEntryModal
          isOpen
          instrumentId={addFor.instrument_id}
          symbol={addFor.symbol}
          companyName={addFor.company_name}
          valuationSource={addFor.valuation_source}
          onRequestClose={() => setAddFor(null)}
          onFilled={handleFilled}
        />
      ) : null}

      {closeFor !== null ? (
        <ClosePositionModal
          isOpen
          instrumentId={closeFor.instrumentId}
          positionId={closeFor.trade.position_id}
          valuationSource={closeFor.valuationSource}
          onRequestClose={() => setCloseFor(null)}
          onFilled={handleFilled}
        />
      ) : null}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Summary bar
// ---------------------------------------------------------------------------

function SummaryBar({
  data,
  currency,
}: {
  data: { total_aum: number; cash_balance: number | null; positions: PositionItem[]; mirrors?: PortfolioMirrorItem[] };
  currency: string;
}) {
  const mirrors = data.mirrors ?? [];
  const totalPnl =
    data.positions.reduce((s, p) => s + p.unrealized_pnl, 0) +
    mirrors.reduce((s, m) => s + m.unrealized_pnl, 0);
  const totalInvested =
    data.positions.reduce((s, p) => s + p.cost_basis, 0) +
    mirrors.reduce((s, m) => s + m.funded, 0);
  const pct = totalInvested !== 0 ? totalPnl / totalInvested : null;
  const posCount = data.positions.length + mirrors.length;
  const mirrorCount = mirrors.length;

  return (
    <div className="flex flex-wrap gap-6 rounded-md border border-slate-200 bg-white px-5 py-3 text-sm shadow-sm">
      <Stat label="AUM" value={formatMoney(data.total_aum, currency)} />
      <Stat label="Cash" value={formatMoney(data.cash_balance, currency)} />
      <Stat
        label="P&L"
        value={formatMoney(totalPnl, currency)}
        hint={pct === null ? undefined : formatPct(pct)}
        tone={totalPnl >= 0 ? "positive" : "negative"}
      />
      <Stat label="Positions" value={String(posCount)} />
      <Stat label="Instruments" value={String(data.positions.length)} />
      {mirrorCount > 0 ? <Stat label="Mirrors" value={String(mirrorCount)} /> : null}
    </div>
  );
}

function Stat({
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
  return (
    <div className="min-w-[64px]">
      <div className="text-[11px] font-medium uppercase tracking-wider text-slate-400">{label}</div>
      <div className="text-sm font-semibold text-slate-800">{value}</div>
      {hint ? (
        <div className={`text-xs font-medium ${tone === "positive" ? "text-emerald-600" : "text-red-600"}`}>
          {hint}
        </div>
      ) : null}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Unified table — positions + mirrors, sorted by value, click to navigate
// ---------------------------------------------------------------------------

type RowItem =
  | { kind: "position"; data: PositionItem }
  | { kind: "mirror"; data: PortfolioMirrorItem };

function matchesSearch(row: RowItem, q: string): boolean {
  if (!q) return true;
  const lower = q.toLowerCase();
  if (row.kind === "position") {
    return (
      row.data.symbol.toLowerCase().includes(lower) ||
      row.data.company_name.toLowerCase().includes(lower)
    );
  }
  return row.data.parent_username.toLowerCase().includes(lower);
}

const AVATAR_TONES = [
  "bg-blue-600", "bg-emerald-600", "bg-amber-600",
  "bg-rose-600", "bg-violet-600", "bg-cyan-600",
] as const;

function avatarTone(name: string): string {
  let hash = 0;
  for (let i = 0; i < name.length; i++) hash = (hash * 31 + name.charCodeAt(i)) | 0;
  return AVATAR_TONES[Math.abs(hash) % AVATAR_TONES.length] ?? "bg-blue-600";
}

function PortfolioTable({
  positions,
  mirrors,
  currency,
  search,
  onSearchChange,
  onAdd,
  onClose,
}: {
  positions: PositionItem[];
  mirrors?: PortfolioMirrorItem[];
  currency: string;
  search: string;
  onSearchChange: (v: string) => void;
  onAdd: (p: PositionItem) => void;
  onClose: (t: CloseTarget) => void;
}) {
  const allRows: RowItem[] = [
    ...positions.map((p) => ({ kind: "position" as const, data: p })),
    ...(mirrors ?? []).map((m) => ({ kind: "mirror" as const, data: m })),
  ];
  allRows.sort((a, b) => {
    const mvA = a.kind === "position" ? a.data.market_value : a.data.mirror_equity;
    const mvB = b.kind === "position" ? b.data.market_value : b.data.mirror_equity;
    return mvB - mvA;
  });

  const filtered = allRows.filter((r) => matchesSearch(r, search));

  if (positions.length === 0 && (mirrors ?? []).length === 0) {
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
    <div className="rounded-md border border-slate-200 bg-white shadow-sm">
      <div className="border-b border-slate-100 px-4 py-2">
        <input
          type="text"
          value={search}
          onChange={(e) => onSearchChange(e.target.value)}
          placeholder="Search positions…"
          className="w-full rounded border border-slate-200 bg-slate-50 px-3 py-1.5 text-sm text-slate-700 placeholder-slate-400 outline-none focus:border-blue-300 focus:ring-1 focus:ring-blue-200"
        />
      </div>
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-200 bg-slate-50 text-[11px] font-semibold uppercase tracking-wider text-slate-500">
            <th className="px-4 py-2 text-left">Instrument</th>
            <th className="px-2 py-2 text-right">Trades</th>
            <th className="px-2 py-2 text-right">Units</th>
            <th className="px-2 py-2 text-right">Avg Entry</th>
            <th className="px-2 py-2 text-right">Price</th>
            <th className="px-2 py-2 text-right">Invested</th>
            <th className="px-2 py-2 text-right">Value</th>
            <th className="px-2 py-2 text-right">P&L</th>
            <th className="px-2 py-2 text-right">%</th>
            <th className="px-2 py-2 text-right">Actions</th>
          </tr>
        </thead>
        <tbody>
          {filtered.map((row) =>
            row.kind === "position" ? (
              <PositionRow
                key={`pos-${row.data.instrument_id}`}
                p={row.data}
                currency={currency}
                onAdd={onAdd}
                onClose={onClose}
              />
            ) : (
              <MirrorRow key={`mir-${row.data.mirror_id}`} m={row.data} currency={currency} />
            ),
          )}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Position row — click navigates to /portfolio/:instrumentId
// ---------------------------------------------------------------------------

function PositionRow({
  p,
  currency,
  onAdd,
  onClose,
}: {
  p: PositionItem;
  currency: string;
  onAdd: (p: PositionItem) => void;
  onClose: (t: CloseTarget) => void;
}) {
  const navigate = useNavigate();
  const pct = pnlPct(p.unrealized_pnl, p.cost_basis);
  const positive = p.unrealized_pnl >= 0;
  const trades = p.trades;
  // Close is only exposed when a single broker position backs the
  // instrument. Aggregated positions defer to #314's detail panel,
  // where per-broker-position rows get their own Close buttons.
  const singleTrade: BrokerPositionItem | null =
    trades.length === 1 && trades[0] !== undefined ? trades[0] : null;

  return (
    <tr
      className="cursor-pointer border-t border-slate-100 transition-colors hover:bg-slate-50/70"
      onClick={() => navigate(`/portfolio/${p.instrument_id}`)}
    >
      <td className="px-4 py-2 text-left">
        <span className="font-medium text-slate-800">{p.symbol}</span>
        <span className="ml-1.5 text-xs text-slate-500">{p.company_name}</span>
        <span className="ml-1.5 text-[10px] text-slate-400">→</span>
      </td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-600">{trades.length || "—"}</td>
      <td className="px-2 py-2 text-right tabular-nums">{formatNumber(p.current_units)}</td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-500">
        {p.avg_cost != null ? formatMoney(p.avg_cost, currency) : "—"}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        {p.current_price != null ? formatMoney(p.current_price, currency) : "—"}
      </td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-600">
        {formatMoney(p.cost_basis, currency)}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">{formatMoney(p.market_value, currency)}</td>
      <td className="px-2 py-2 text-right tabular-nums">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {formatMoney(p.unrealized_pnl, currency)}
        </span>
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {pct === null ? "—" : formatPct(pct)}
        </span>
      </td>
      <td className="px-2 py-2 text-right whitespace-nowrap">
        <button
          type="button"
          onClick={(e) => {
            e.stopPropagation();
            onAdd(p);
          }}
          aria-label={`Add to ${p.symbol}`}
          className="mr-1 rounded border border-blue-300 bg-white px-2 py-0.5 text-xs font-medium text-blue-700 hover:bg-blue-50"
        >
          Add
        </button>
        {singleTrade !== null ? (
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              onClose({
                instrumentId: p.instrument_id,
                trade: singleTrade,
                valuationSource: p.valuation_source,
              });
            }}
            aria-label={`Close ${p.symbol}`}
            className="rounded border border-red-300 bg-white px-2 py-0.5 text-xs font-medium text-red-700 hover:bg-red-50"
          >
            Close
          </button>
        ) : null}
      </td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Mirror row — click navigates to /copy-trading/:mirrorId
// ---------------------------------------------------------------------------

function MirrorRow({ m, currency }: { m: PortfolioMirrorItem; currency: string }) {
  const navigate = useNavigate();
  const pct = pnlPct(m.unrealized_pnl, m.funded);
  const positive = m.unrealized_pnl >= 0;

  return (
    <tr
      className="cursor-pointer border-t border-slate-100 transition-colors hover:bg-slate-50/70"
      onClick={() => navigate(`/copy-trading/${m.mirror_id}`)}
    >
      <td className="px-4 py-2 text-left">
        <span className="inline-flex items-center gap-2">
          <span
            className={`inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-full text-[10px] font-semibold text-white ${avatarTone(m.parent_username)}`}
          >
            {m.parent_username.charAt(0).toUpperCase()}
          </span>
          <span className="font-medium text-slate-800">{m.parent_username}</span>
          <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] font-medium text-slate-500">
            COPY
          </span>
          <span className="text-[10px] text-slate-400">→</span>
        </span>
      </td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-600">{m.position_count}</td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-300">—</td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-300">—</td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-300">—</td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-600">
        {formatMoney(m.funded, currency)}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">{formatMoney(m.mirror_equity, currency)}</td>
      <td className="px-2 py-2 text-right tabular-nums">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {formatMoney(m.unrealized_pnl, currency)}
        </span>
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {pct === null ? "—" : formatPct(pct)}
        </span>
      </td>
      {/* No Actions for mirror rows — copy trading is a separate flow. */}
      <td className="px-2 py-2" />
    </tr>
  );
}
