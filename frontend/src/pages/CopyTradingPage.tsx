import { useParams, Link } from "react-router-dom";
import { fetchMirrorDetail } from "@/api/copyTrading";
import { useAsync } from "@/lib/useAsync";
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
import { formatMoney, formatNumber, formatDateTime } from "@/lib/format";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import type { MirrorSummary, MirrorPositionItem } from "@/api/types";

/**
 * Mirror detail page (#221 — mirrors as positions).
 *
 * Drill-down from a mirror row in the dashboard positions table.
 * Shows per-mirror stats and the component positions held by the
 * copied trader. Replaces the standalone CopyTradingPage (#188).
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
        <Link to="/" className="text-sm font-medium text-blue-600 hover:underline">
          Back to dashboard
        </Link>
      </EmptyState>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <Link to="/" className="text-sm text-slate-500 hover:text-slate-700">
          ← Dashboard
        </Link>
        <h1 className="text-xl font-semibold text-slate-800">
          {detail.data ? `Copy: ${detail.data.parent_username}` : "Mirror detail"}
        </h1>
      </div>

      {detail.error !== null ? (
        <div className="rounded-md border border-slate-200 bg-white p-4 shadow-sm">
          <SectionError onRetry={detail.refetch} />
        </div>
      ) : detail.loading || detail.data === null ? (
        <SectionSkeleton rows={6} />
      ) : (
        <>
          <MirrorStats mirror={detail.data.mirror} currency={currency} />
          <Section title="Positions">
            <MirrorPositionsTable positions={detail.data.mirror.positions} currency={currency} />
          </Section>
        </>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Mirror stats (reused from the original CopyTradingPage)
// ---------------------------------------------------------------------------

function MirrorStats({ mirror, currency }: { mirror: MirrorSummary; currency: string }) {
  return (
    <div className="grid grid-cols-2 gap-x-6 gap-y-2 rounded-md border border-slate-200 bg-white p-4 text-sm shadow-sm sm:grid-cols-3">
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
// Mirror positions table (reused from the original CopyTradingPage)
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
