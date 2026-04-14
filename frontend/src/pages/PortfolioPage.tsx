import { useState } from "react";
import { Link } from "react-router-dom";
import { fetchPortfolio } from "@/api/portfolio";
import { fetchMirrorDetail } from "@/api/copyTrading";
import { useAsync } from "@/lib/useAsync";
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
import { formatMoney, formatNumber, formatPct, formatDateTime, pnlPct } from "@/lib/format";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import type { PositionItem, PortfolioMirrorItem, BrokerPositionItem, MirrorPositionItem } from "@/api/types";

/**
 * Portfolio page — the operator's main working view.
 *
 * Dense, financial-tool aesthetic. Unified positions+mirrors table with
 * search. Positions expand to individual trades with SL/TP. Mirrors expand
 * to show instrument groups (lazy-fetched). Both row types sort together
 * by value for a single ranked view of the entire portfolio.
 */
export function PortfolioPage() {
  const portfolio = useAsync(fetchPortfolio, []);
  const currency = useDisplayCurrency();
  const [search, setSearch] = useState("");

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-slate-800">Portfolio</h1>
      </div>

      {portfolio.error !== null ? (
        <div className="rounded-md border border-slate-200 bg-white p-4 shadow-sm">
          <SectionError onRetry={portfolio.refetch} />
        </div>
      ) : portfolio.loading || portfolio.data === null ? (
        <div className="space-y-4">
          <div className="rounded-md border border-slate-200 bg-white p-3 shadow-sm">
            <SectionSkeleton rows={1} />
          </div>
          <div className="rounded-md border border-slate-200 bg-white p-4 shadow-sm">
            <SectionSkeleton rows={8} />
          </div>
        </div>
      ) : (
        <>
          <SummaryBar data={portfolio.data} currency={currency} />
          <PortfolioTable
            positions={portfolio.data.positions}
            mirrors={portfolio.data.mirrors}
            currency={currency}
            search={search}
            onSearchChange={setSearch}
          />
        </>
      )}
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
  const pct = pnlPct(totalPnl, totalCost);
  const posCount = data.positions.reduce((n, p) => n + ((p.trades?.length) || 1), 0);
  const mirrorCount = (data.mirrors ?? []).length;

  return (
    <div className="flex flex-wrap items-center gap-x-6 gap-y-1 rounded-md border border-slate-200 bg-white px-4 py-2.5 text-sm shadow-sm">
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
  const toneClass =
    tone === "positive" ? "text-emerald-600" : tone === "negative" ? "text-red-600" : "text-slate-900";
  return (
    <div className="flex items-baseline gap-1.5">
      <span className="text-xs uppercase tracking-wide text-slate-400">{label}</span>
      <span className={`font-semibold tabular-nums ${toneClass}`}>{value}</span>
      {hint ? <span className="text-xs text-slate-500">{hint}</span> : null}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Unified table
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
}: {
  positions: PositionItem[];
  mirrors?: PortfolioMirrorItem[];
  currency: string;
  search: string;
  onSearchChange: (v: string) => void;
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
          className="w-full rounded border border-slate-200 bg-slate-50 px-3 py-1.5 text-sm text-slate-700 placeholder:text-slate-400 focus:border-blue-400 focus:outline-none focus:ring-1 focus:ring-blue-400"
        />
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-b border-slate-100 text-xs uppercase tracking-wide text-slate-400">
              <th className="px-4 py-2 text-left font-medium">Instrument</th>
              <th className="px-2 py-2 text-right font-medium">Trades</th>
              <th className="px-2 py-2 text-right font-medium">Units</th>
              <th className="px-2 py-2 text-right font-medium">Avg Entry</th>
              <th className="px-2 py-2 text-right font-medium">Price</th>
              <th className="px-2 py-2 text-right font-medium">Invested</th>
              <th className="px-2 py-2 text-right font-medium">Value</th>
              <th className="px-2 py-2 text-right font-medium">P&L</th>
              <th className="px-2 py-2 text-right font-medium">%</th>
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0 ? (
              <tr>
                <td colSpan={9} className="px-4 py-8 text-center text-sm text-slate-400">
                  No matches for &ldquo;{search}&rdquo;
                </td>
              </tr>
            ) : (
              filtered.map((row) =>
                row.kind === "position" ? (
                  <PositionRow key={`pos-${row.data.instrument_id}`} p={row.data} currency={currency} />
                ) : (
                  <MirrorRow key={`mir-${row.data.mirror_id}`} m={row.data} currency={currency} />
                ),
              )
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Position row — stock-level aggregate, click to expand individual trades
// ---------------------------------------------------------------------------

function PositionRow({ p, currency }: { p: PositionItem; currency: string }) {
  const [expanded, setExpanded] = useState(false);
  const pct = pnlPct(p.unrealized_pnl, p.cost_basis);
  const positive = p.unrealized_pnl >= 0;
  const trades = p.trades ?? [];
  const tradeCount = trades.length;
  const hasMultiple = tradeCount > 1;

  return (
    <>
      <tr
        className={`cursor-pointer border-t border-slate-100 transition-colors ${
          expanded ? "bg-blue-50/50" : "hover:bg-slate-50/70"
        }`}
        onClick={() => setExpanded((v) => !v)}
      >
        <td className="px-4 py-2 text-left">
          <Link
            to={`/instruments/${p.instrument_id}`}
            className="font-medium text-slate-800 hover:text-blue-600 hover:underline"
            onClick={(e) => e.stopPropagation()}
          >
            {p.symbol}
          </Link>
          <span className="ml-1.5 text-xs text-slate-500">{p.company_name}</span>
          {tradeCount > 0 ? (
            <span className="ml-1.5 text-[10px] text-slate-400">
              {expanded ? "▾" : "▸"}
            </span>
          ) : null}
        </td>
        <td className="px-2 py-2 text-right tabular-nums text-slate-600">{tradeCount || "—"}</td>
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
      </tr>
      {expanded && tradeCount > 0 ? (
        <>
          {hasMultiple ? (
            <tr className="border-t border-slate-100 bg-slate-50/60 text-[10px] uppercase tracking-wide text-slate-400">
              <td className="py-1 pl-8 pr-2">Entry</td>
              <td className="px-2 py-1 text-right">Open</td>
              <td className="px-2 py-1 text-right">Units</td>
              <td className="px-2 py-1 text-right" colSpan={2}>SL / TP</td>
              <td className="px-2 py-1 text-right">Invested</td>
              <td className="px-2 py-1 text-right">Value</td>
              <td className="px-2 py-1 text-right">P&L</td>
              <td className="px-2 py-1 text-right">%</td>
            </tr>
          ) : null}
          {trades.map((t) => (
            <TradeRow key={t.position_id} t={t} currency={currency} />
          ))}
        </>
      ) : null}
    </>
  );
}

// ---------------------------------------------------------------------------
// Individual trade row
// ---------------------------------------------------------------------------

function TradeRow({ t, currency }: { t: BrokerPositionItem; currency: string }) {
  const positive = t.unrealized_pnl >= 0;
  const pct = pnlPct(t.unrealized_pnl, t.amount);

  return (
    <tr className="border-t border-slate-50 bg-slate-50/60 text-xs text-slate-600">
      <td className="py-1.5 pl-8 pr-2 text-left">
        <span
          className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-medium ${
            t.is_buy ? "bg-emerald-50 text-emerald-700" : "bg-red-50 text-red-700"
          }`}
        >
          {t.is_buy ? "LONG" : "SHORT"}
        </span>
        <span className="ml-2 tabular-nums text-slate-500">
          {formatMoney(t.open_rate, currency)}
        </span>
        {t.leverage > 1 ? (
          <span className="ml-1.5 rounded bg-amber-50 px-1 py-0.5 text-[10px] font-medium text-amber-700">
            x{t.leverage}
          </span>
        ) : null}
      </td>
      <td className="px-2 py-1.5 text-right tabular-nums text-slate-400">
        {formatDateTime(t.open_date_time).split(",")[0]}
      </td>
      <td className="px-2 py-1.5 text-right tabular-nums">{formatNumber(t.units)}</td>
      <td className="px-2 py-1.5 text-right tabular-nums" colSpan={2}>
        <span className="text-red-400">{t.stop_loss_rate != null ? formatMoney(t.stop_loss_rate, currency) : "—"}</span>
        <span className="mx-0.5 text-slate-300">/</span>
        <span className="text-emerald-500">{t.take_profit_rate != null ? formatMoney(t.take_profit_rate, currency) : "—"}</span>
      </td>
      <td className="px-2 py-1.5 text-right tabular-nums text-slate-500">
        {formatMoney(t.amount, currency)}
      </td>
      <td className="px-2 py-1.5 text-right tabular-nums">
        {formatMoney(t.market_value, currency)}
      </td>
      <td className="px-2 py-1.5 text-right tabular-nums">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {formatMoney(t.unrealized_pnl, currency)}
        </span>
      </td>
      <td className="px-2 py-1.5 text-right tabular-nums">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {pct === null ? "—" : formatPct(pct)}
        </span>
      </td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Mirror row — expandable inline, lazy-fetches positions
// ---------------------------------------------------------------------------

function MirrorRow({ m, currency }: { m: PortfolioMirrorItem; currency: string }) {
  const [expanded, setExpanded] = useState(false);
  const [mirrorPositions, setMirrorPositions] = useState<MirrorPositionItem[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [fetchError, setFetchError] = useState<string | null>(null);
  const pct = pnlPct(m.unrealized_pnl, m.funded);
  const positive = m.unrealized_pnl >= 0;

  const handleToggle = () => {
    if (loading) return;
    if (!expanded && mirrorPositions === null) {
      setLoading(true);
      setFetchError(null);
      fetchMirrorDetail(m.mirror_id)
        .then((detail) => {
          setMirrorPositions(detail.mirror.positions);
          setLoading(false);
          setExpanded(true);
        })
        .catch((err: unknown) => {
          setLoading(false);
          setFetchError(err instanceof Error ? err.message : "Failed to load positions");
        });
    } else {
      setExpanded((v) => !v);
    }
  };

  // Group positions by instrument for inline display
  const groups = mirrorPositions ? groupByInstrument(mirrorPositions) : [];

  return (
    <>
      <tr
        className={`cursor-pointer border-t border-slate-100 transition-colors ${
          expanded ? "bg-blue-50/50" : "hover:bg-slate-50/70"
        }`}
        onClick={handleToggle}
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
            <span className="text-[10px] text-slate-400">
              {loading ? "…" : expanded ? "▾" : "▸"}
            </span>
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
      </tr>
      {fetchError ? (
        <tr className="border-t border-slate-100 bg-red-50/50">
          <td colSpan={9} className="px-4 py-2 text-xs text-red-600">
            {fetchError}
          </td>
        </tr>
      ) : null}
      {expanded && mirrorPositions !== null ? (
        <>
          {groups.map((g) => (
            <MirrorInstrumentRow key={g.instrument_id} group={g} currency={currency} />
          ))}
          <tr className="border-t border-slate-100 bg-slate-50/40">
            <td colSpan={9} className="px-4 py-1.5">
              <Link
                to={`/copy-trading/${m.mirror_id}`}
                className="text-xs font-medium text-blue-600 hover:underline"
              >
                View full detail →
              </Link>
            </td>
          </tr>
        </>
      ) : null}
    </>
  );
}

// ---------------------------------------------------------------------------
// Mirror instrument group — one row per instrument within an expanded mirror
// ---------------------------------------------------------------------------

interface InstrumentGroup {
  instrument_id: number;
  symbol: string | null;
  company_name: string | null;
  total_units: number;
  total_market_value: number;
  total_pnl: number;
  total_invested: number;
  current_price: number | null;
  positions: MirrorPositionItem[];
}

function groupByInstrument(positions: MirrorPositionItem[]): InstrumentGroup[] {
  const map = new Map<number, MirrorPositionItem[]>();
  for (const p of positions) {
    const existing = map.get(p.instrument_id);
    if (existing) existing.push(p);
    else map.set(p.instrument_id, [p]);
  }

  const groups: InstrumentGroup[] = [];
  for (const [instrument_id, items] of map) {
    const first = items[0];
    if (!first) continue;
    groups.push({
      instrument_id,
      symbol: first.symbol,
      company_name: first.company_name,
      total_units: items.reduce((s, p) => s + p.units, 0),
      total_market_value: items.reduce((s, p) => s + p.market_value, 0),
      total_pnl: items.reduce((s, p) => s + p.unrealized_pnl, 0),
      total_invested: items.reduce((s, p) => s + p.amount, 0),
      current_price: items.find((p) => p.current_price != null)?.current_price ?? null,
      positions: items,
    });
  }

  groups.sort((a, b) => b.total_market_value - a.total_market_value);
  return groups;
}

function MirrorInstrumentRow({ group, currency }: { group: InstrumentGroup; currency: string }) {
  const [expanded, setExpanded] = useState(false);
  const positive = group.total_pnl >= 0;
  const pct = pnlPct(group.total_pnl, group.total_invested);
  const hasMultiple = group.positions.length > 1;

  return (
    <>
      <tr
        className={`border-t border-slate-50 bg-slate-50/60 text-xs text-slate-600 ${hasMultiple ? "cursor-pointer hover:bg-slate-100/60" : ""}`}
        onClick={hasMultiple ? () => setExpanded((v) => !v) : undefined}
      >
        <td className="py-1.5 pl-8 pr-2 text-left">
          <span className="font-medium text-slate-700">
            {group.symbol ?? `#${group.instrument_id}`}
          </span>
          {group.company_name ? (
            <span className="ml-1.5 text-[10px] text-slate-400">{group.company_name}</span>
          ) : null}
          {hasMultiple ? (
            <span className="ml-1 text-[10px] text-slate-400">{expanded ? "▾" : "▸"}</span>
          ) : null}
        </td>
        <td className="px-2 py-1.5 text-right tabular-nums">{group.positions.length}</td>
        <td className="px-2 py-1.5 text-right tabular-nums">{formatNumber(group.total_units)}</td>
        <td className="px-2 py-1.5 text-right tabular-nums text-slate-300" colSpan={2}>
          {group.current_price != null ? formatMoney(group.current_price, currency) : "—"}
        </td>
        <td className="px-2 py-1.5 text-right tabular-nums">
          {formatMoney(group.total_invested, currency)}
        </td>
        <td className="px-2 py-1.5 text-right tabular-nums">
          {formatMoney(group.total_market_value, currency)}
        </td>
        <td className="px-2 py-1.5 text-right tabular-nums">
          <span className={positive ? "text-emerald-600" : "text-red-600"}>
            {formatMoney(group.total_pnl, currency)}
          </span>
        </td>
        <td className="px-2 py-1.5 text-right tabular-nums">
          <span className={positive ? "text-emerald-600" : "text-red-600"}>
            {pct === null ? "—" : formatPct(pct)}
          </span>
        </td>
      </tr>
      {expanded
        ? group.positions.map((p) => (
            <MirrorSubPositionRow key={p.position_id} position={p} currency={currency} />
          ))
        : null}
    </>
  );
}

function MirrorSubPositionRow({ position, currency }: { position: MirrorPositionItem; currency: string }) {
  const positive = position.unrealized_pnl >= 0;
  const pct = pnlPct(position.unrealized_pnl, position.amount);
  return (
    <tr className="border-t border-slate-50 bg-slate-100/40 text-[11px] text-slate-500">
      <td className="py-1 pl-12 pr-2 text-left">
        <span
          className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-medium ${
            position.is_buy ? "bg-emerald-50 text-emerald-700" : "bg-red-50 text-red-700"
          }`}
        >
          {position.is_buy ? "LONG" : "SHORT"}
        </span>
        <span className="ml-2 tabular-nums text-slate-400">
          entry {formatNumber(position.open_rate, 2)}
        </span>
      </td>
      <td className="px-2 py-1 text-right" />
      <td className="px-2 py-1 text-right tabular-nums">{formatNumber(position.units)}</td>
      <td className="px-2 py-1 text-right tabular-nums text-slate-300" colSpan={2}>
        {position.current_price != null ? formatMoney(position.current_price, currency) : "—"}
      </td>
      <td className="px-2 py-1 text-right tabular-nums">
        {formatMoney(position.amount, currency)}
      </td>
      <td className="px-2 py-1 text-right tabular-nums">
        {formatMoney(position.market_value, currency)}
      </td>
      <td className="px-2 py-1 text-right tabular-nums">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {formatMoney(position.unrealized_pnl, currency)}
        </span>
      </td>
      <td className="px-2 py-1 text-right tabular-nums">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {pct === null ? "—" : formatPct(pct)}
        </span>
      </td>
    </tr>
  );
}
