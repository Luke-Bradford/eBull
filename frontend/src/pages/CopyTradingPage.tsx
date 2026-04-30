import { useMemo, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { fetchMirrorDetail } from "@/api/copyTrading";
import { useAsync } from "@/lib/useAsync";
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
import { formatMoney, formatNumber, formatPct, formatDateTime, pnlPct } from "@/lib/format";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { LiveQuoteProvider } from "@/components/quotes/LiveQuoteProvider";
import { LivePriceCell } from "@/components/quotes/LivePriceCell";
import type { MirrorSummary, MirrorPositionItem } from "@/api/types";

/**
 * Mirror detail page (#221 — mirrors as positions).
 *
 * Drill-down from a mirror row in the dashboard positions table.
 * Shows per-mirror stats and component positions grouped by instrument.
 * Each instrument row is expandable to reveal individual positions.
 */
export function CopyTradingPage() {
  const { mirrorId } = useParams<{ mirrorId: string }>();
  const currency = useDisplayCurrency();
  const parsedId = Number(mirrorId);
  const detail = useAsync(() => fetchMirrorDetail(parsedId), [parsedId]);

  if (!mirrorId || Number.isNaN(parsedId)) {
    return (
      <EmptyState
        title="Invalid mirror"
        description="No mirror ID was provided in the URL."
      >
        <Link to="/portfolio" className="text-sm font-medium text-blue-600 hover:underline">
          Back to portfolio
        </Link>
      </EmptyState>
    );
  }

  const username = detail.data?.parent_username;

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <Link to="/portfolio" className="text-sm text-slate-500 hover:text-slate-700">
          ← Portfolio
        </Link>
        {username ? (
          <h1 className="flex items-center gap-2 text-xl font-semibold text-slate-800 dark:text-slate-100">
            <TraderAvatar username={username} />
            {username}
          </h1>
        ) : (
          <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">Mirror detail</h1>
        )}
      </div>

      {detail.error !== null ? (
        <div className="border-t border-slate-200 pt-3">
          <SectionError onRetry={detail.refetch} />
        </div>
      ) : detail.loading || detail.data === null ? (
        <SectionSkeleton rows={6} />
      ) : (
        <>
          <MirrorStats mirror={detail.data.mirror} currency={currency} />
          <Section title="Positions">
            <GroupedPositionsTable positions={detail.data.mirror.positions} currency={currency} />
          </Section>
        </>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Trader avatar — eToro-style initials circle
// ---------------------------------------------------------------------------

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

function TraderAvatar({ username }: { username: string }) {
  return (
    <span
      className={`inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-sm font-semibold text-white ${avatarTone(username)}`}
    >
      {username.charAt(0).toUpperCase()}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Mirror stats
// ---------------------------------------------------------------------------

function MirrorStats({ mirror, currency }: { mirror: MirrorSummary; currency: string }) {
  return (
    <div className="grid grid-cols-2 gap-x-6 gap-y-2 border-t border-slate-200 px-1 pt-3 pb-2 text-sm sm:grid-cols-3">
      <LabelValue label="Initial investment" value={formatMoney(mirror.initial_investment, currency)} />
      <LabelValue label="Deposits" value={formatMoney(mirror.deposit_summary, currency)} />
      <LabelValue label="Withdrawals" value={formatMoney(mirror.withdrawal_summary, currency)} />
      <LabelValue label="Available cash" value={formatMoney(mirror.available_amount, currency)} />
      <LabelValue label="Closed P&L" value={formatMoney(mirror.closed_positions_net_profit, currency)} />
      <LabelValue label="Copying since" value={formatDateTime(mirror.started_copy_date)} />
    </div>
  );
}

function LabelValue({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <span className="text-xs text-slate-500">{label}: </span>
      <span className="font-medium tabular-nums text-slate-700">{value}</span>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Instrument grouping
// ---------------------------------------------------------------------------

interface InstrumentGroup {
  instrument_id: number;
  symbol: string | null;
  company_name: string | null;
  total_units: number;
  total_market_value: number;
  total_pnl: number;
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
    if (!first) continue; // shouldn't happen — we only create entries with items
    groups.push({
      instrument_id,
      symbol: first.symbol,
      company_name: first.company_name,
      total_units: items.reduce((s, p) => s + p.units, 0),
      total_market_value: items.reduce((s, p) => s + p.market_value, 0),
      total_pnl: items.reduce((s, p) => s + p.unrealized_pnl, 0),
      current_price: items.find((p) => p.current_price != null)?.current_price ?? null,
      positions: items,
    });
  }

  // Sort by total market value descending (largest holdings first).
  groups.sort((a, b) => b.total_market_value - a.total_market_value);
  return groups;
}

// ---------------------------------------------------------------------------
// Grouped positions table
// ---------------------------------------------------------------------------

function GroupedPositionsTable({
  positions,
  currency,
}: {
  positions: MirrorPositionItem[];
  currency: string;
}) {
  // Collect every instrument id rendered (group rows AND any
  // expanded sub-rows share the same id) so one SSE stream covers
  // the whole table. Same-id-twice de-dup is handled by the
  // provider; both cells consume from the same context tick.
  const liveQuoteIds = useMemo(
    () => positions.map((p) => p.instrument_id),
    [positions],
  );

  if (positions.length === 0) {
    return <p className="text-xs text-slate-500">No open positions in this mirror.</p>;
  }

  const groups = groupByInstrument(positions);

  return (
    <LiveQuoteProvider instrumentIds={liveQuoteIds}>
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase text-slate-500">
          <tr>
            <th className="px-2 py-2 text-left">Instrument</th>
            <th className="px-2 py-2 text-right">Positions</th>
            <th className="px-2 py-2 text-right">Units</th>
            <th className="px-2 py-2 text-right">Price</th>
            <th className="px-2 py-2 text-right">Value</th>
            <th className="px-2 py-2 text-right">P&L</th>
          </tr>
        </thead>
        <tbody>
          {groups.map((g) => (
            <InstrumentGroupRow key={g.instrument_id} group={g} currency={currency} />
          ))}
        </tbody>
      </table>
    </div>
    </LiveQuoteProvider>
  );
}

function InstrumentGroupRow({
  group,
  currency,
}: {
  group: InstrumentGroup;
  currency: string;
}) {
  const [expanded, setExpanded] = useState(false);
  const positive = group.total_pnl >= 0;
  const invested = group.positions.reduce((s, p) => s + p.amount, 0);
  const pct = pnlPct(group.total_pnl, invested);
  const hasMultiple = group.positions.length > 1;

  return (
    <>
      <tr
        className={`border-t border-slate-100 ${hasMultiple ? "cursor-pointer hover:bg-slate-50 dark:hover:bg-slate-800/40" : ""}`}
        onClick={hasMultiple ? () => setExpanded((v) => !v) : undefined}
      >
        <td className="px-2 py-2 text-left">
          <span className="font-medium text-slate-800 dark:text-slate-100">
            {group.symbol ?? `#${group.instrument_id}`}
          </span>
          {group.company_name ? (
            <span className="ml-1.5 text-xs text-slate-500">{group.company_name}</span>
          ) : null}
          {hasMultiple ? (
            <span className="ml-1.5 text-[10px] text-slate-400">
              {expanded ? "▾" : "▸"}
            </span>
          ) : null}
        </td>
        <td className="px-2 py-2 text-right tabular-nums text-slate-600">
          {group.positions.length}
        </td>
        <td className="px-2 py-2 text-right tabular-nums">{formatNumber(group.total_units)}</td>
        <td className="px-2 py-2 text-right tabular-nums">
          <LivePriceCell
            instrumentId={group.instrument_id}
            fallback={group.current_price}
            currency={currency}
          />
        </td>
        <td className="px-2 py-2 text-right tabular-nums">
          {formatMoney(group.total_market_value, currency)}
        </td>
        <td className="px-2 py-2 text-right tabular-nums">
          <span className={positive ? "text-emerald-600" : "text-red-600"}>
            {formatMoney(group.total_pnl, currency)}
            {pct === null ? "" : ` (${formatPct(pct)})`}
          </span>
        </td>
      </tr>
      {expanded
        ? group.positions.map((p) => (
            <SubPositionRow key={p.position_id} position={p} currency={currency} />
          ))
        : null}
    </>
  );
}

function SubPositionRow({
  position,
  currency,
}: {
  position: MirrorPositionItem;
  currency: string;
}) {
  const positive = position.unrealized_pnl >= 0;
  return (
    <tr className="border-t border-slate-50 bg-slate-50/60 text-xs text-slate-600">
      <td className="py-1.5 pl-6 pr-2 text-left">
        <span
          className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-medium ${
            position.is_buy ? "bg-emerald-50 text-emerald-700" : "bg-red-50 text-red-700"
          }`}
        >
          {position.is_buy ? "LONG" : "SHORT"}
        </span>
        <span className="ml-2 text-slate-400">
          entry {formatNumber(position.open_rate, 2)}
        </span>
      </td>
      <td className="px-2 py-1.5 text-right" />
      <td className="px-2 py-1.5 text-right tabular-nums">{formatNumber(position.units)}</td>
      <td className="px-2 py-1.5 text-right tabular-nums">
        <LivePriceCell
          instrumentId={position.instrument_id}
          fallback={position.current_price}
          currency={currency}
        />
      </td>
      <td className="px-2 py-1.5 text-right tabular-nums">
        {formatMoney(position.market_value, currency)}
      </td>
      <td className="px-2 py-1.5 text-right tabular-nums">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {formatMoney(position.unrealized_pnl, currency)}
        </span>
      </td>
    </tr>
  );
}
