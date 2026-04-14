import { useParams, Link } from "react-router-dom";
import { fetchInstrumentPositions } from "@/api/portfolio";
import { useAsync } from "@/lib/useAsync";
import { formatNumber, formatPct } from "@/lib/format";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import type { InstrumentPositionDetail, NativeTradeItem } from "@/api/types";

// ---------------------------------------------------------------------------
// Currency formatting in native currency (no conversion)
// ---------------------------------------------------------------------------

const CURRENCY_FORMATTERS = new Map<string, Intl.NumberFormat>();

function nativeMoney(value: number | null | undefined, currency: string): string {
  if (value === null || value === undefined) return "—";
  let fmt = CURRENCY_FORMATTERS.get(currency);
  if (!fmt) {
    fmt = new Intl.NumberFormat("en-GB", {
      style: "currency",
      currency,
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    CURRENCY_FORMATTERS.set(currency, fmt);
  }
  return fmt.format(value);
}

function nativePrice(value: number | null | undefined, currency: string): string {
  if (value === null || value === undefined) return "—";
  let fmt = CURRENCY_FORMATTERS.get(currency + "_price");
  if (!fmt) {
    fmt = new Intl.NumberFormat("en-GB", {
      style: "currency",
      currency,
      minimumFractionDigits: 2,
      maximumFractionDigits: 4,
    });
    CURRENCY_FORMATTERS.set(currency + "_price", fmt);
  }
  return fmt.format(value);
}

// ---------------------------------------------------------------------------
// Page component
// ---------------------------------------------------------------------------

export function PositionDetailPage() {
  const { instrumentId } = useParams<{ instrumentId: string }>();
  const parsedId = Number(instrumentId);
  const detail = useAsync(() => fetchInstrumentPositions(parsedId), [parsedId]);

  if (!instrumentId || Number.isNaN(parsedId)) {
    return (
      <EmptyState
        title="Invalid instrument"
        description="No instrument ID was provided in the URL."
      >
        <Link to="/portfolio" className="text-sm font-medium text-blue-600 hover:underline">
          Back to portfolio
        </Link>
      </EmptyState>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <Link to="/portfolio" className="text-sm text-slate-500 hover:text-slate-700">
          ← Portfolio
        </Link>
        {detail.data ? (
          <h1 className="text-xl font-semibold text-slate-800">
            <span className="font-bold">{detail.data.symbol}</span>
            <span className="ml-2 text-base font-normal text-slate-500">
              {detail.data.company_name}
            </span>
          </h1>
        ) : (
          <h1 className="text-xl font-semibold text-slate-800">Position detail</h1>
        )}
      </div>

      {detail.error !== null ? (
        <div className="rounded-md border border-slate-200 bg-white p-4 shadow-sm">
          <SectionError onRetry={detail.refetch} />
        </div>
      ) : detail.loading || detail.data === null ? (
        <SectionSkeleton rows={6} />
      ) : (
        <>
          <SummaryStats data={detail.data} />
          <TradesTable trades={detail.data.trades} currency={detail.data.currency} />
        </>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Summary stats bar
// ---------------------------------------------------------------------------

function SummaryStats({ data }: { data: InstrumentPositionDetail }) {
  const ccy = data.currency;
  const pnlPctVal = data.total_invested !== 0 ? data.total_pnl / data.total_invested : null;
  const positive = data.total_pnl >= 0;

  return (
    <div className="flex flex-wrap gap-6 rounded-md border border-slate-200 bg-white px-6 py-4 shadow-sm">
      <StatItem label="Currency" value={ccy} />
      <StatItem label="Price" value={nativePrice(data.current_price, ccy)} />
      <StatItem label="Units" value={formatNumber(data.total_units)} />
      <StatItem label="Avg Entry" value={nativePrice(data.avg_entry, ccy)} />
      <StatItem label="Invested" value={nativeMoney(data.total_invested, ccy)} />
      <StatItem label="Value" value={nativeMoney(data.total_value, ccy)} />
      <StatItem
        label="P&L"
        value={nativeMoney(data.total_pnl, ccy)}
        hint={pnlPctVal !== null ? formatPct(pnlPctVal) : undefined}
        tone={positive ? "positive" : "negative"}
      />
      <StatItem label="Trades" value={String(data.trades.length)} />
    </div>
  );
}

function StatItem({
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
    <div className="min-w-[80px]">
      <div className="text-[11px] font-medium uppercase tracking-wider text-slate-400">
        {label}
      </div>
      <div className="text-sm font-semibold text-slate-800">{value}</div>
      {hint ? (
        <div
          className={`text-xs font-medium ${tone === "positive" ? "text-emerald-600" : "text-red-600"}`}
        >
          {hint}
        </div>
      ) : null}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Trades table — individual positions in native currency
// ---------------------------------------------------------------------------

function TradesTable({ trades, currency }: { trades: NativeTradeItem[]; currency: string }) {
  return (
    <div className="overflow-x-auto rounded-md border border-slate-200 bg-white shadow-sm">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-200 bg-slate-50 text-[11px] font-semibold uppercase tracking-wider text-slate-500">
            <th className="px-4 py-2 text-left">Direction</th>
            <th className="px-2 py-2 text-right">Entry</th>
            <th className="px-2 py-2 text-right">Open</th>
            <th className="px-2 py-2 text-right">Units</th>
            <th className="px-2 py-2 text-right">Leverage</th>
            <th className="px-2 py-2 text-right">SL</th>
            <th className="px-2 py-2 text-right">TP</th>
            <th className="px-2 py-2 text-right">Invested</th>
            <th className="px-2 py-2 text-right">Value</th>
            <th className="px-2 py-2 text-right">P&L</th>
            <th className="px-2 py-2 text-right">%</th>
          </tr>
        </thead>
        <tbody>
          {trades.map((t) => (
            <TradeRow key={t.position_id} trade={t} currency={currency} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function TradeRow({ trade, currency }: { trade: NativeTradeItem; currency: string }) {
  const pnlPctVal = trade.amount !== 0 ? trade.unrealized_pnl / trade.amount : null;
  const positive = trade.unrealized_pnl >= 0;
  const openDate = new Date(trade.open_date_time).toLocaleDateString("en-GB", {
    day: "numeric",
    month: "short",
    year: "numeric",
  });

  return (
    <tr className="border-t border-slate-100 hover:bg-slate-50/70">
      <td className="px-4 py-2 text-left">
        <span
          className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-bold ${
            trade.is_buy
              ? "bg-emerald-50 text-emerald-700"
              : "bg-red-50 text-red-700"
          }`}
        >
          {trade.is_buy ? "LONG" : "SHORT"}
        </span>
      </td>
      <td className="px-2 py-2 text-right tabular-nums">{nativePrice(trade.open_rate, currency)}</td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-500">{openDate}</td>
      <td className="px-2 py-2 text-right tabular-nums">{formatNumber(trade.units)}</td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-500">
        {trade.leverage > 1 ? `×${trade.leverage}` : "—"}
      </td>
      <td className="px-2 py-2 text-right tabular-nums text-red-500">
        {trade.stop_loss_rate !== null ? nativePrice(trade.stop_loss_rate, currency) : "—"}
      </td>
      <td className="px-2 py-2 text-right tabular-nums text-emerald-600">
        {trade.take_profit_rate !== null ? nativePrice(trade.take_profit_rate, currency) : "—"}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">{nativeMoney(trade.amount, currency)}</td>
      <td className="px-2 py-2 text-right tabular-nums">{nativeMoney(trade.market_value, currency)}</td>
      <td className="px-2 py-2 text-right tabular-nums">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {nativeMoney(trade.unrealized_pnl, currency)}
        </span>
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {pnlPctVal !== null ? formatPct(pnlPctVal) : "—"}
        </span>
      </td>
    </tr>
  );
}
