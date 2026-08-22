import { useEffect, useState } from "react";

import { closeStrategyOwnedPosition } from "@/api/strategies";
import type { StrategyOwnedPosition } from "@/api/types";
import { ApiError } from "@/api/client";
import { Badge } from "@/components/ui/Badge";
import { Modal } from "@/components/ui/Modal";
import { formatDate, formatMoney, formatPct } from "@/lib/format";
import { useLiveTick } from "@/components/quotes/LiveQuoteProvider";
import { liveTickPriceIn } from "@/lib/useLiveQuote";

/**
 * Strategy-owned position rendering, shared by both `/strategies` lenses (#2868).
 *
 * Extracted verbatim from `StrategiesPage` when the page split into a portfolio
 * lens and a research lens: open positions belong on the portfolio, and the
 * close modal belongs wherever the positions are. Extracted rather than copied
 * — a fifth near-copy of a money-rendering row is exactly what the
 * `information-architecture` skill's "extract once and share" rule exists to stop.
 */
function number(value: string | null): number | null {
  if (value === null) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function StrategyPositionRow({
  position,
  onClose,
}: {
  position: StrategyOwnedPosition;
  onClose: (position: StrategyOwnedPosition) => void;
}) {
  const tick = useLiveTick(position.instrument_id);
  const livePrice = liveTickPriceIn(tick, position.currency);
  const snapshotPrice = number(position.current_price);
  const parsedLivePrice = livePrice === null ? null : Number(livePrice.value);
  const currentPrice = parsedLivePrice !== null && Number.isFinite(parsedLivePrice)
    ? parsedLivePrice
    : snapshotPrice;
  const assigned = number(position.assigned_value);
  const units = number(position.units);
  const openRate = number(position.open_rate);
  const liveValuationAvailable = assigned !== null
    && units !== null
    && openRate !== null
    && currentPrice !== null
    && position.direction !== null;
  const currentValue = liveValuationAvailable
    ? assigned + units * (
      position.direction === "long" ? currentPrice - openRate : openRate - currentPrice
    )
    : number(position.current_value);
  const pnl = currentValue !== null && assigned !== null
    ? currentValue - assigned
    : number(position.unrealised_pnl);
  const pnlRatio = pnl !== null && assigned !== null && assigned !== 0 ? pnl / assigned : null;
  const closing = position.trade_status === "closing";
  const needsReconciliation = position.trade_status === "reconcile_required" || !position.valuation_available;

  return (
    <tr className="border-t border-slate-200 dark:border-slate-800">
      <td className="px-4 py-3">
        <div className="flex items-center gap-2">
          <strong className="text-sm">{position.symbol}</strong>
          <Badge tone={closing ? "info" : needsReconciliation ? "warn" : "neutral"}>
            {closing ? "Closing" : needsReconciliation ? "Check required" : position.direction ?? "Owned"}
          </Badge>
        </div>
        <span className="mt-0.5 block max-w-48 truncate text-xs text-slate-500">
          {position.company_name ?? `Position #${position.broker_position_id}`}
        </span>
      </td>
      <td className="px-4 py-3 text-xs text-slate-600 dark:text-slate-300">
        <span className="block font-medium text-slate-800 dark:text-slate-100">{position.strategy_title}</span>
        <span className="text-slate-500">Opened {formatDate(position.opened_at)}</span>
      </td>
      <td className="px-4 py-3 text-right text-sm tabular-nums">{formatMoney(assigned, position.currency)}</td>
      <td className="px-4 py-3 text-right text-sm tabular-nums">{formatMoney(currentValue, position.currency)}</td>
      <td className={`px-4 py-3 text-right text-sm font-semibold tabular-nums ${pnl !== null && pnl < 0 ? "text-red-600 dark:text-red-300" : pnl !== null && pnl > 0 ? "text-emerald-700 dark:text-emerald-300" : ""}`}>
        <span className="block">{formatMoney(pnl, position.currency)}</span>
        <span className="text-xs">{formatPct(pnlRatio)}</span>
      </td>
      <td className="px-4 py-3 text-right text-sm tabular-nums">
        <span className="block">{formatMoney(number(position.stop_loss_rate), position.currency)}</span>
        <span className="text-[10px] text-slate-500">Stop loss</span>
      </td>
      <td className="px-4 py-3 text-right text-sm tabular-nums">
        <span className="block">{formatMoney(number(position.take_profit_rate), position.currency)}</span>
        <span className="text-[10px] text-slate-500">Take profit</span>
      </td>
      <td className="px-4 py-3 text-right">
        <button
          type="button"
          disabled={closing}
          onClick={() => onClose(position)}
          className="min-h-11 cursor-pointer border border-slate-300 px-3 text-xs font-medium hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700 dark:hover:bg-slate-800"
        >
          {closing ? "Closing…" : "Close"}
        </button>
      </td>
    </tr>
  );
}

export function OpenStrategyPositions({
  positions,
  onClose,
}: {
  positions: StrategyOwnedPosition[];
  onClose: (position: StrategyOwnedPosition) => void;
}) {
  return (
    <section className="border border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900">
      <div className="flex flex-wrap items-start justify-between gap-2 px-5 py-4">
        <div>
          <h2 className="text-sm font-semibold">Open automated positions</h2>
          <p className="mt-1 text-xs text-slate-500">
            Exact strategy-owned trades only. They also remain visible in the main Portfolio.
          </p>
        </div>
        <span className="text-xs text-slate-500">{positions.length} open</span>
      </div>
      {positions.length === 0 ? (
        <div className="border-t border-slate-200 px-5 py-5 text-sm text-slate-500 dark:border-slate-800">
          No automated positions are open. This section appears when an approved strategy receives a broker position.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[64rem]">
            <thead className="border-t border-slate-200 text-left text-[10px] font-semibold uppercase tracking-wider text-slate-500 dark:border-slate-800">
              <tr>
                <th className="px-4 py-2">Position</th>
                <th className="px-4 py-2">Strategy</th>
                <th className="px-4 py-2 text-right">Assigned</th>
                <th className="px-4 py-2 text-right">Current</th>
                <th className="px-4 py-2 text-right">Gain / loss</th>
                <th className="px-4 py-2 text-right">SL</th>
                <th className="px-4 py-2 text-right">TP</th>
                <th className="px-4 py-2"><span className="sr-only">Actions</span></th>
              </tr>
            </thead>
            <tbody>
              {positions.map((position) => (
                <StrategyPositionRow
                  key={`${position.strategy_trade_id}:${position.broker_position_id}`}
                  position={position}
                  onClose={onClose}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

export function StrategyCloseModal({
  position,
  onRequestClose,
  onAccepted,
}: {
  position: StrategyOwnedPosition | null;
  onRequestClose: () => void;
  onAccepted: () => void;
}) {
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    setSubmitting(false);
    setError(null);
  }, [position]);

  async function submit() {
    if (position === null || submitting) return;
    setSubmitting(true);
    setError(null);
    try {
      await closeStrategyOwnedPosition(position.strategy_trade_id, position.broker_position_id);
      onAccepted();
      onRequestClose();
    } catch (caught) {
      setError(caught instanceof ApiError ? caught.message : "The close request could not be submitted.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Modal
      isOpen={position !== null}
      onRequestClose={submitting ? () => undefined : onRequestClose}
      label={`Close ${position?.symbol ?? "automated position"}`}
    >
      {position ? (
        <div className="space-y-4">
          <div>
            <h2 className="text-base font-semibold">Close {position.symbol}</h2>
            <p className="mt-1 text-xs text-slate-500">
              {position.strategy_title} · broker position #{position.broker_position_id}
            </p>
          </div>
          <p className="text-sm text-slate-700 dark:text-slate-200">
            This submits a full close to the connected demo account. The strategy will show this position as Closing until the exact broker order reconciles.
          </p>
          <p className="text-xs text-slate-500">
            A separate manual position in {position.symbol} is not part of this request and will remain untouched.
          </p>
          {error ? <p role="alert" className="text-xs text-red-700 dark:text-red-300">{error}</p> : null}
          <div className="flex justify-end gap-2 border-t border-slate-200 pt-4 dark:border-slate-800">
            <button type="button" disabled={submitting} onClick={onRequestClose} className="min-h-11 cursor-pointer border border-slate-300 px-4 text-sm disabled:opacity-50 dark:border-slate-700">Cancel</button>
            <button type="button" disabled={submitting} onClick={() => void submit()} className="min-h-11 cursor-pointer border border-red-700 bg-red-700 px-4 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-50">
              {submitting ? "Submitting…" : "Close position"}
            </button>
          </div>
        </div>
      ) : null}
    </Modal>
  );
}
