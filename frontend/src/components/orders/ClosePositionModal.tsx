/**
 * ClosePositionModal — full or partial close of a specific broker
 * position (issue #313).
 *
 * Launches from PortfolioPage when a position has exactly one broker
 * row backing it (aggregated positions are deferred to #314).
 *
 * Closes operate on `broker_positions.position_id`, not on an
 * aggregated instrument position — see app/api/orders.py:473.
 *
 * Full close uses `{units_to_deduct: null}` to mirror the backend's
 * `None = close entire position` contract. Partial close uses a
 * numeric value. The mode is carried by a radio state, not by a
 * float comparison against `units`, so a full-close slider sitting
 * at max never accidentally becomes partial.
 */
import { useEffect, useRef, useState } from "react";

import { ApiError } from "@/api/client";
import { closePosition } from "@/api/orders";
import { fetchInstrumentPositions } from "@/api/portfolio";
import type { NativeTradeItem } from "@/api/types";
import { Modal } from "@/components/ui/Modal";
import { DemoLivePill } from "@/components/orders/DemoLivePill";
import { formatNumber } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

type ValuationSource = "quote" | "daily_close" | "cost_basis";
type CloseMode = "full" | "partial";

/**
 * Minimum partial-close quantity. Matches the backend's
 * `Decimal.quantize(Decimal("0.000001"))` at app/api/orders.py:115 —
 * a value smaller than this rounds to 0 server-side and would be
 * rejected with a confusing "units_to_deduct must be positive" 400.
 */
const MIN_UNITS = 0.000001;

export interface ClosePositionModalProps {
  readonly isOpen: boolean;
  readonly instrumentId: number;
  readonly positionId: number;
  readonly valuationSource: ValuationSource;
  readonly onRequestClose: () => void;
  readonly onFilled: () => void;
}

const DISCLAIMER =
  "Preview uses your latest known portfolio price. At submission time the " +
  "fill may use a different quote — if no quote is available the fill " +
  "uses your open rate and realized P&L may be ~0.";

const NETWORK_ERROR_PHRASE = "Network error — check connection and try again.";

export function ClosePositionModal({
  isOpen,
  instrumentId,
  positionId,
  valuationSource,
  onRequestClose,
  onFilled,
}: ClosePositionModalProps): JSX.Element {
  const detail = useAsync(
    () => fetchInstrumentPositions(instrumentId),
    [instrumentId],
  );

  const [mode, setMode] = useState<CloseMode>("full");
  const [rawUnits, setRawUnits] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const mountedRef = useRef(true);
  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  const trade = findTrade(detail.data?.trades ?? null, positionId);
  const currency = detail.data?.currency ?? "";
  const symbol = detail.data?.symbol ?? "";

  const parsedUnits = parsePositiveFinite(rawUnits);
  const partialValid =
    parsedUnits !== null &&
    trade !== null &&
    parsedUnits >= MIN_UNITS &&
    parsedUnits <= trade.units;
  const canSubmit =
    trade !== null &&
    !submitting &&
    !detail.error &&
    (mode === "full" || partialValid);

  async function handleSubmit(): Promise<void> {
    if (trade === null) return;
    const unitsToDeduct = mode === "full" ? null : parsedUnits;
    if (mode === "partial" && (unitsToDeduct === null || unitsToDeduct <= 0)) {
      return;
    }
    setSubmitting(true);
    setErrorMessage(null);
    try {
      await closePosition(positionId, { units_to_deduct: unitsToDeduct });
      // onFilled must run on server success even if the operator
      // unmounted us via Escape between submit and response — the
      // portfolio would otherwise stay stale while the server-side
      // close really happened.
      onFilled();
      if (mountedRef.current) {
        onRequestClose();
      }
    } catch (err) {
      // Failures are not persisted; if unmounted, drop the error.
      if (!mountedRef.current) return;
      setSubmitting(false);
      if (err instanceof ApiError) {
        setErrorMessage(err.message);
      } else {
        setErrorMessage(NETWORK_ERROR_PHRASE);
      }
    }
  }

  const title = `Close — ${symbol || "position"}`;
  const staleTrade = detail.data !== null && trade === null;

  return (
    <Modal isOpen={isOpen} onRequestClose={onRequestClose} label={title}>
      <div className="flex flex-col gap-3">
        <header className="flex items-center justify-between">
          <div>
            <h2 className="text-sm font-semibold text-slate-800">{title}</h2>
            <p className="text-xs text-slate-500">Position #{positionId}</p>
          </div>
          <DemoLivePill />
        </header>

        {detail.error !== null ? (
          <div className="rounded border border-red-200 bg-red-50 px-2 py-1.5 text-xs text-red-700">
            <span>Could not load position details.</span>{" "}
            <button
              type="button"
              onClick={detail.refetch}
              className="font-semibold underline"
            >
              Retry
            </button>
          </div>
        ) : detail.loading || detail.data === null ? (
          <div className="text-xs text-slate-500">Loading position…</div>
        ) : staleTrade ? (
          <p
            role="alert"
            className="rounded border border-red-300 bg-red-50 px-2 py-1.5 text-xs text-red-700"
          >
            This position no longer exists — refresh the portfolio.
          </p>
        ) : trade !== null ? (
          <>
            <InfoStrip
              trade={trade}
              currency={currency}
              valuationSource={valuationSource}
            />
            <fieldset
              className="flex flex-col gap-2 text-xs"
              disabled={submitting}
            >
              <legend className="sr-only">Close mode</legend>
              <div className="flex gap-4">
                <label className="inline-flex items-center gap-1">
                  <input
                    type="radio"
                    name="close-mode"
                    value="full"
                    checked={mode === "full"}
                    onChange={() => setMode("full")}
                  />
                  <span>Full close</span>
                </label>
                <label className="inline-flex items-center gap-1">
                  <input
                    type="radio"
                    name="close-mode"
                    value="partial"
                    checked={mode === "partial"}
                    onChange={() => setMode("partial")}
                  />
                  <span>Partial close</span>
                </label>
              </div>
              {mode === "partial" ? (
                <label className="flex flex-col gap-1">
                  <span>Units to close (max {formatNumber(trade.units, 6)})</span>
                  <div className="flex items-center gap-2">
                    <input
                      type="range"
                      min={Math.min(MIN_UNITS, trade.units)}
                      max={trade.units}
                      step={MIN_UNITS}
                      value={
                        parsedUnits !== null && parsedUnits <= trade.units
                          ? parsedUnits
                          : Math.min(MIN_UNITS, trade.units)
                      }
                      onChange={(e) => setRawUnits(e.target.value)}
                      aria-label="Units to close (slider)"
                      className="flex-1"
                    />
                    <input
                      type="number"
                      inputMode="decimal"
                      min={MIN_UNITS}
                      step={MIN_UNITS}
                      value={rawUnits}
                      onChange={(e) => setRawUnits(e.target.value)}
                      placeholder={formatNumber(trade.units, 6)}
                      aria-label="Units to close"
                      className="w-28 rounded border border-slate-300 bg-white px-2 py-1 text-sm focus:border-blue-400 focus:outline-none"
                    />
                  </div>
                  {parsedUnits !== null && parsedUnits > trade.units ? (
                    <span className="text-[11px] text-red-600">
                      Exceeds position units ({formatNumber(trade.units, 6)}).
                    </span>
                  ) : parsedUnits !== null && parsedUnits < MIN_UNITS ? (
                    <span className="text-[11px] text-red-600">
                      Must be at least {MIN_UNITS} units (backend precision floor).
                    </span>
                  ) : null}
                </label>
              ) : null}
            </fieldset>

            <PreviewBlock
              mode={mode}
              parsedUnits={parsedUnits}
              trade={trade}
              currency={currency}
              valuationSource={valuationSource}
            />

            <p className="text-[11px] leading-snug text-slate-500">
              {DISCLAIMER}
            </p>
          </>
        ) : null}

        {errorMessage !== null ? (
          <p
            role="alert"
            className="rounded border border-red-300 bg-red-50 px-2 py-1.5 text-xs text-red-700"
          >
            {errorMessage}
          </p>
        ) : null}

        <div className="flex justify-end gap-2">
          <button
            type="button"
            onClick={onRequestClose}
            disabled={submitting}
            className="rounded border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-50"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={handleSubmit}
            disabled={!canSubmit}
            className="rounded bg-red-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-red-500 disabled:opacity-50"
          >
            {submitting ? "Closing…" : "Close position"}
          </button>
        </div>
      </div>
    </Modal>
  );
}

function findTrade(
  trades: NativeTradeItem[] | null,
  positionId: number,
): NativeTradeItem | null {
  if (trades === null) return null;
  return trades.find((t) => t.position_id === positionId) ?? null;
}

function parsePositiveFinite(raw: string): number | null {
  if (raw.trim() === "") return null;
  const n = Number(raw);
  if (!Number.isFinite(n)) return null;
  if (n <= 0) return null;
  return n;
}

function InfoStrip({
  trade,
  currency,
  valuationSource,
}: {
  trade: NativeTradeItem;
  currency: string;
  valuationSource: ValuationSource;
}): JSX.Element {
  const amber = valuationSource !== "quote";
  return (
    <div className="rounded border border-slate-200 bg-slate-50 px-2 py-1.5 text-xs text-slate-700">
      <div>
        {formatNumber(trade.units, 6)} units @ {formatNumber(trade.open_rate, 4)}{" "}
        {currency}
      </div>
      <div>
        Latest price:{" "}
        <span className={amber ? "font-semibold text-amber-700" : ""}>
          {trade.current_price !== null
            ? `${formatNumber(trade.current_price, 4)} ${currency}`
            : "—"}
        </span>{" "}
        <span className="text-slate-500">({valuationSource})</span>
        {amber ? (
          <span className="ml-1 font-semibold text-amber-700">
            (may not reflect fill price)
          </span>
        ) : null}
      </div>
    </div>
  );
}

function PreviewBlock({
  mode,
  parsedUnits,
  trade,
  currency,
  valuationSource,
}: {
  mode: CloseMode;
  parsedUnits: number | null;
  trade: NativeTradeItem;
  currency: string;
  valuationSource: ValuationSource;
}): JSX.Element {
  const unitsToClose = mode === "full" ? trade.units : parsedUnits ?? 0;
  const havePrice = trade.current_price !== null;
  const fillPrice = trade.current_price ?? trade.open_rate;
  const pnl = (fillPrice - trade.open_rate) * unitsToClose;
  const pnlColor = pnl >= 0 ? "text-emerald-700" : "text-red-700";
  // The caption must reflect BOTH whether we have a price AND
  // whether that price came from a live quote. A daily_close
  // fallback is not a quote — the backend may still fill at
  // open_rate if its own raw quote lookup turns up null.
  const caption = !havePrice
    ? "using open rate — no quote available; realized P&L will be ~0"
    : valuationSource === "quote"
      ? "using latest quote"
      : `using latest ${valuationSource} — backend may fall back to open rate`;
  return (
    <div className="rounded border border-slate-200 bg-slate-50 px-2 py-1.5 text-xs">
      <div>
        Closing: {formatNumber(unitsToClose, 6)} /{" "}
        {formatNumber(trade.units, 6)} units
      </div>
      <div>
        Open rate: {formatNumber(trade.open_rate, 4)} {currency}
      </div>
      <div>
        Est. fill price:{" "}
        {havePrice ? `${formatNumber(fillPrice, 4)} ${currency}` : "—"}
      </div>
      <div className={pnlColor}>
        Est. realized P&amp;L: {formatNumber(pnl, 2)} {currency}
      </div>
      <div className="text-slate-500">{caption}</div>
    </div>
  );
}
