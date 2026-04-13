import { useState } from "react";
import { fetchCopyTrading } from "@/api/copyTrading";
import { useAsync } from "@/lib/useAsync";
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
import { formatMoney, formatNumber, formatDateTime } from "@/lib/format";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import type { CopyTraderSummary, MirrorSummary, MirrorPositionItem } from "@/api/types";

/**
 * Copy-trading browsing page (#188 — Track 1.5).
 *
 * Shows per-trader cards with mirror-level aggregates and an expandable
 * nested-position drill-down. Closed mirrors are shown in a separate
 * history section at the bottom.
 */
export function CopyTradingPage() {
  const currency = useDisplayCurrency();
  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const ct = useAsync(fetchCopyTrading, []);

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold text-slate-800">Copy Trading</h1>

      {ct.error !== null ? (
        <div className="rounded-md border border-slate-200 bg-white p-4 shadow-sm">
          <SectionError onRetry={ct.refetch} />
        </div>
      ) : ct.loading || ct.data === null ? (
        <SectionSkeleton rows={6} />
      ) : ct.data.traders.length === 0 ? (
        <EmptyState
          title="No copy traders"
          description="Mirror positions will appear here once a copy-trading relationship is synced from eToro."
        />
      ) : (
        <>
          <MirrorEquitySummary
            totalMirrorEquity={ct.data.total_mirror_equity}
            currency={currency}
            traderCount={ct.data.traders.length}
          />
          <ActiveTraders traders={ct.data.traders} currency={currency} />
          <ClosedMirrors traders={ct.data.traders} currency={currency} />
        </>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

function MirrorEquitySummary({
  totalMirrorEquity,
  currency,
  traderCount,
}: {
  totalMirrorEquity: number;
  currency: string;
  traderCount: number;
}) {
  return (
    <div className="grid grid-cols-2 gap-4 sm:grid-cols-3">
      <StatCard label="Mirror equity" value={formatMoney(totalMirrorEquity, currency)} />
      <StatCard label="Copied traders" value={String(traderCount)} />
    </div>
  );
}

function StatCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-slate-200 bg-white p-4 shadow-sm">
      <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-1 text-lg font-semibold text-slate-800">{value}</div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Active traders
// ---------------------------------------------------------------------------

function ActiveTraders({
  traders,
  currency,
}: {
  traders: CopyTraderSummary[];
  currency: string;
}) {
  const activeMirrors = traders.flatMap((t) =>
    t.mirrors.filter((m) => m.active).map((m) => ({ trader: t, mirror: m })),
  );

  if (activeMirrors.length === 0) return null;

  return (
    <Section title="Active mirrors">
      <div className="space-y-4">
        {activeMirrors.map(({ trader, mirror }) => (
          <TraderMirrorCard
            key={mirror.mirror_id}
            trader={trader}
            mirror={mirror}
            currency={currency}
          />
        ))}
      </div>
    </Section>
  );
}

// ---------------------------------------------------------------------------
// Trader card
// ---------------------------------------------------------------------------

function TraderMirrorCard({
  trader,
  mirror,
  currency,
}: {
  trader: CopyTraderSummary;
  mirror: MirrorSummary;
  currency: string;
}) {
  const [expanded, setExpanded] = useState(false);
  const funded = mirror.initial_investment + mirror.deposit_summary - mirror.withdrawal_summary;
  const pnl = mirror.mirror_equity - funded;

  return (
    <div className="rounded-md border border-slate-200 bg-white">
      <button
        type="button"
        className="flex w-full items-center justify-between px-4 py-3 text-left hover:bg-slate-50"
        onClick={() => setExpanded(!expanded)}
      >
        <div>
          <span className="text-sm font-semibold text-slate-800">{trader.parent_username}</span>
          <span className="ml-2 text-xs text-slate-500">
            {mirror.position_count} position{mirror.position_count !== 1 ? "s" : ""}
          </span>
        </div>
        <div className="flex items-center gap-4">
          <div className="text-right">
            <div className="text-xs text-slate-500">Equity</div>
            <div className="text-sm font-medium tabular-nums text-slate-800">
              {formatMoney(mirror.mirror_equity, currency)}
            </div>
          </div>
          <div className="text-right">
            <div className="text-xs text-slate-500">P&L</div>
            <div
              className={`text-sm font-medium tabular-nums ${pnl >= 0 ? "text-emerald-600" : "text-red-600"}`}
            >
              {formatMoney(pnl, currency)}
            </div>
          </div>
          <span className="text-slate-400">{expanded ? "▲" : "▼"}</span>
        </div>
      </button>

      {expanded && (
        <div className="border-t border-slate-100 px-4 py-3">
          <MirrorStats mirror={mirror} currency={currency} />
          <MirrorPositionsTable positions={mirror.positions} currency={currency} />
        </div>
      )}
    </div>
  );
}

function MirrorStats({ mirror, currency }: { mirror: MirrorSummary; currency: string }) {
  return (
    <div className="mb-3 grid grid-cols-2 gap-x-6 gap-y-1 text-xs sm:grid-cols-4">
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
      <span className="text-slate-500">{label}: </span>
      <span className="font-medium tabular-nums text-slate-700">{value}</span>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Mirror positions table
// ---------------------------------------------------------------------------

function MirrorPositionsTable({
  positions,
  currency,
}: {
  positions: MirrorPositionItem[];
  currency: string;
}) {
  if (positions.length === 0) {
    return <p className="text-xs text-slate-500">No open positions in this mirror.</p>;
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase text-slate-500">
          <tr>
            <th className="px-2 py-2 text-left">Symbol</th>
            <th className="px-2 py-2 text-left">Side</th>
            <th className="px-2 py-2 text-right">Units</th>
            <th className="px-2 py-2 text-right">Entry</th>
            <th className="px-2 py-2 text-right">Price</th>
            <th className="px-2 py-2 text-right">Value</th>
            <th className="px-2 py-2 text-right">P&L</th>
          </tr>
        </thead>
        <tbody>
          {positions.map((p) => (
            <MirrorPositionRow key={p.position_id} position={p} currency={currency} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function MirrorPositionRow({
  position,
  currency,
}: {
  position: MirrorPositionItem;
  currency: string;
}) {
  return (
    <tr className="border-t border-slate-100">
      <td className="px-2 py-2 text-left">
        <span className="font-medium text-slate-800">
          {position.symbol ?? `#${position.instrument_id}`}
        </span>
        {position.company_name ? (
          <span className="ml-1 text-xs text-slate-500">{position.company_name}</span>
        ) : null}
      </td>
      <td className="px-2 py-2 text-left">
        <span
          className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-medium ${
            position.is_buy
              ? "bg-emerald-50 text-emerald-700"
              : "bg-red-50 text-red-700"
          }`}
        >
          {position.is_buy ? "LONG" : "SHORT"}
        </span>
      </td>
      <td className="px-2 py-2 text-right tabular-nums">{formatNumber(position.units)}</td>
      <td className="px-2 py-2 text-right tabular-nums">{formatNumber(position.open_rate, 2)}</td>
      <td className="px-2 py-2 text-right tabular-nums">
        {position.current_price != null ? formatMoney(position.current_price, currency) : "—"}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">{formatMoney(position.market_value, currency)}</td>
      <td className="px-2 py-2 text-right tabular-nums">
        <span className={position.unrealized_pnl >= 0 ? "text-emerald-600" : "text-red-600"}>
          {formatMoney(position.unrealized_pnl, currency)}
        </span>
      </td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Closed mirrors
// ---------------------------------------------------------------------------

function ClosedMirrors({
  traders,
  currency,
}: {
  traders: CopyTraderSummary[];
  currency: string;
}) {
  const closedMirrors = traders.flatMap((t) =>
    t.mirrors.filter((m) => !m.active).map((m) => ({ trader: t, mirror: m })),
  );

  if (closedMirrors.length === 0) return null;

  return (
    <Section title="Closed mirrors">
      <div className="space-y-4">
        {closedMirrors.map(({ trader, mirror }) => (
          <TraderMirrorCard
            key={mirror.mirror_id}
            trader={trader}
            mirror={mirror}
            currency={currency}
          />
        ))}
      </div>
    </Section>
  );
}
