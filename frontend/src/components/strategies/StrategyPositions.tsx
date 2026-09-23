import { useEffect, useState } from "react";

import { closeStrategyOwnedPosition } from "@/api/strategies";
import type { StrategyOwnedPosition } from "@/api/types";
import { ApiError } from "@/api/client";
import { Badge } from "@/components/ui/Badge";
import { Modal } from "@/components/ui/Modal";
import { formatDate, formatMoney, formatNumber, formatPct } from "@/lib/format";
import { number } from "@/lib/strategyFormat";
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
 * That rule binds the formatters too: `number` comes from `@/lib/strategyFormat`,
 * never a private re-declaration.
 */
/**
 * Signed distance from the CURRENT price to a protective level, as a fraction
 * (#3334). "How far is the stop from here" is the question the operator asks of
 * an open position; distance from entry answers a different one and is carried
 * by the horizon tooltip instead. `null` when either side is unknown or the
 * price is not positive, so a missing mark renders `—` rather than `-100%`.
 */
export function levelDistance(level: number | null, current: number | null): number | null {
  if (level === null || current === null || current <= 0) return null;
  return level / current - 1;
}

/**
 * The hold horizon column (#3334). A strategy position shows the strategy's own
 * declared exit timing; a core-mandate position (no `strategy_id`) is a
 * long-term index hold whose SL/TP are a safety net, not an exit plan.
 *
 * ⚠ The tooltip's percentages are DERIVED from this row's own levels against its
 * entry price, never written as constants here: the sizes are owned by
 * `app/services/core_exit_levels.py` (#3284), and a hand-copied "-50%" would go
 * stale silently the day that module changes.
 */
export function positionHorizon(
  position: StrategyOwnedPosition,
  exitTimingById: ReadonlyMap<string, string>,
): { label: string; why: string } {
  if (position.strategy_id !== null) {
    return {
      label: exitTimingById.get(position.strategy_id) ?? "Strategy exit rule",
      why: `Held until ${position.strategy_title}'s own exit rule fires.`,
    };
  }
  const entry = number(position.open_rate);
  const stop = levelDistance(number(position.stop_loss_rate), entry);
  const target = levelDistance(number(position.take_profit_rate), entry);
  const levels = stop !== null && target !== null
    ? `The stop (${formatPct(stop)} from entry) and target (${formatPct(target)}) are a broker-side safety net, not an exit plan: `
    : "The stop and target are a broker-side safety net, not an exit plan: ";
  return {
    label: "Long-term index hold",
    why: levels
      + "the stop is set wide enough to cut a genuine collapse without selling an ordinary bear market, "
      + "and the target is far enough away that only a bad price tick would reach it. "
      + "Profit-taking is the rebalance band's job.",
  };
}

function levelCell(level: number | null, current: number | null, currency: string, kind: string) {
  return (
    <td className="px-3 py-3 text-right text-sm tabular-nums">
      <span className="block">{formatMoney(level, currency)}</span>
      <span className="text-xs text-slate-500">
        {formatPct(levelDistance(level, current))}
        <span className="sr-only"> from current price ({kind})</span>
      </span>
    </td>
  );
}

function StrategyPositionRow({
  position,
  exitTimingById,
  onClose,
}: {
  position: StrategyOwnedPosition;
  exitTimingById: ReadonlyMap<string, string>;
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
  const horizon = positionHorizon(position, exitTimingById);

  return (
    <tr className="border-t border-slate-200 dark:border-slate-800">
      <td className="px-3 py-3">
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
      <td className="px-3 py-3 text-xs font-medium text-slate-800 dark:text-slate-100">{position.strategy_title}</td>
      <td className="px-3 py-3 text-right text-sm tabular-nums">{formatNumber(units, 6)}</td>
      <td className="px-3 py-3 text-right text-sm tabular-nums">{formatMoney(openRate, position.currency)}</td>
      <td className="px-3 py-3 text-right text-sm tabular-nums">{formatMoney(currentPrice, position.currency)}</td>
      <td className="px-3 py-3 text-right text-sm tabular-nums">
        <span className="block">{formatMoney(currentValue, position.currency)}</span>
        <span className="whitespace-nowrap text-xs text-slate-500">cost {formatMoney(assigned, position.currency)}</span>
      </td>
      <td className={`px-3 py-3 text-right text-sm font-semibold tabular-nums ${pnl !== null && pnl < 0 ? "text-red-600 dark:text-red-300" : pnl !== null && pnl > 0 ? "text-emerald-700 dark:text-emerald-300" : ""}`}>
        <span className="block">{formatMoney(pnl, position.currency)}</span>
        <span className="text-xs">{formatPct(pnlRatio)}</span>
      </td>
      {levelCell(number(position.stop_loss_rate), currentPrice, position.currency, "stop loss")}
      {levelCell(number(position.take_profit_rate), currentPrice, position.currency, "take profit")}
      <td className="px-3 py-3 text-xs text-slate-600 dark:text-slate-300">
        <span className="block whitespace-nowrap">{formatDate(position.opened_at)}</span>
        <span
          title={horizon.why}
          className="cursor-help text-slate-500 underline decoration-dotted underline-offset-2"
        >
          {horizon.label}
        </span>
      </td>
      <td className="px-3 py-3 text-right">
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
  exitTimingById,
  onClose,
}: {
  positions: StrategyOwnedPosition[];
  /** `strategy_id -> exit_timing` from the overview; the horizon column's source. */
  exitTimingById: ReadonlyMap<string, string>;
  onClose: (position: StrategyOwnedPosition) => void;
}) {
  return (
    <section className="border border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900">
      <div className="flex flex-wrap items-start justify-between gap-2 px-5 py-4">
        <div>
          <h2 className="text-sm font-semibold">Engine positions</h2>
          <p className="mt-1 text-xs text-slate-500">
            Exact engine-owned trades only. They also remain visible in the main Portfolio. SL / TP % is the distance from the current price.
          </p>
        </div>
      </div>
      {positions.length === 0 ? (
        <div className="border-t border-slate-200 px-5 py-5 text-sm text-slate-500 dark:border-slate-800">
          No automated positions are open. This section appears when an approved strategy receives a broker position.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[72rem]" aria-label="Engine positions">
            <thead className="border-t border-slate-200 text-left text-[10px] font-semibold uppercase tracking-wider text-slate-500 dark:border-slate-800">
              <tr>
                <th className="px-3 py-2">Position</th>
                <th className="px-3 py-2">Sleeve / strategy</th>
                <th className="px-3 py-2 text-right">Units</th>
                <th className="px-3 py-2 text-right">Entry price</th>
                <th className="px-3 py-2 text-right">Current price</th>
                <th className="px-3 py-2 text-right">Value</th>
                <th className="px-3 py-2 text-right">P&amp;L</th>
                <th className="px-3 py-2 text-right">Stop loss</th>
                <th className="px-3 py-2 text-right">Take profit</th>
                <th className="px-3 py-2">Opened · horizon</th>
                <th className="px-3 py-2"><span className="sr-only">Actions</span></th>
              </tr>
            </thead>
            <tbody>
              {positions.map((position) => (
                <StrategyPositionRow
                  key={`${position.strategy_trade_id}:${position.broker_position_id}`}
                  position={position}
                  exitTimingById={exitTimingById}
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
