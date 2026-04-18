/**
 * OrderEntryModal — ADD-to-existing-position flow (issue #313).
 *
 * Launches from PortfolioPage. Renders:
 *   - DemoLivePill (safety-state-ui pattern)
 *   - Native-currency price context from /portfolio/instruments/:id
 *   - Amount / Units toggle + numeric input with live preview
 *   - Submit -> POST /portfolio/orders with action=ADD
 *
 * Intentionally minimal vs the spec:
 *   - Action is always "ADD" in this PR (new-instrument BUY ships in #316).
 *   - No SL / TP / TSL / leverage UI. Payload sends the backend defaults
 *     explicitly for readability.
 *
 * Safety:
 *   - Kill-switch / live-trading checks live on the backend; we simply
 *     surface the fixed-phrase detail on any non-2xx.
 *   - mountedRef guards every post-await setState (prevention log #127).
 *   - Modal closes BEFORE the portfolio refetch fires, so refetch errors
 *     cannot hide behind the modal (prevention log #125).
 */
import { useEffect, useRef, useState } from "react";

import { ApiError } from "@/api/client";
import { placeOrder } from "@/api/orders";
import { fetchInstrumentPositions } from "@/api/portfolio";
import type { InstrumentPositionDetail, PlaceOrderRequest } from "@/api/types";
import { Modal } from "@/components/ui/Modal";
import { DemoLivePill } from "@/components/orders/DemoLivePill";
import { formatNumber } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

type ValuationSource = "quote" | "daily_close" | "cost_basis";

export interface OrderEntryModalProps {
  readonly isOpen: boolean;
  readonly instrumentId: number;
  readonly symbol: string;
  readonly companyName: string;
  readonly valuationSource: ValuationSource;
  readonly onRequestClose: () => void;
  readonly onFilled: () => void;
}

type InputMode = "amount" | "units";

const DISCLAIMER =
  "Preview uses your latest known portfolio price. At submission time the " +
  "fill may use a different quote — if no quote is available the backend " +
  "returns 422 and no order is placed.";

const NETWORK_ERROR_PHRASE = "Network error — check connection and try again.";

export function OrderEntryModal({
  isOpen,
  instrumentId,
  symbol,
  companyName,
  valuationSource,
  onRequestClose,
  onFilled,
}: OrderEntryModalProps): JSX.Element {
  const detail = useAsync(
    // useAsync captures fn via a ref — fresh arrow per render is fine.
    () => fetchInstrumentPositions(instrumentId),
    [instrumentId],
  );

  const [mode, setMode] = useState<InputMode>("amount");
  const [rawInput, setRawInput] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  // mountedRef: guard setState against the "user escapes the modal
  // mid-submit then the parent unmounts us" race (prevention #127).
  const mountedRef = useRef(true);
  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  const parsedValue = parsePositiveFinite(rawInput);
  const priceIsUsable =
    detail.data?.current_price != null && detail.data.current_price > 0;
  const detailLoaded = detail.data !== null;
  const canSubmit =
    detailLoaded &&
    !detail.loading &&
    !detail.error &&
    parsedValue !== null &&
    !submitting;

  async function handleSubmit(): Promise<void> {
    if (parsedValue === null) return;
    setSubmitting(true);
    setErrorMessage(null);
    const body: PlaceOrderRequest = {
      instrument_id: instrumentId,
      action: "ADD",
      amount: mode === "amount" ? parsedValue : null,
      units: mode === "units" ? parsedValue : null,
      stop_loss_rate: null,
      take_profit_rate: null,
      is_tsl_enabled: false,
      leverage: 1,
    };
    try {
      await placeOrder(body);
      // Reset submitting before handing control to the parent. The
      // parent normally unmounts us immediately via onRequestClose,
      // but if a future caller delays that the button must not stay
      // locked in "Placing…" indefinitely.
      if (mountedRef.current) setSubmitting(false);
      // onFilled is owned by the parent page — we must call it on
      // success REGARDLESS of whether this modal is still mounted,
      // otherwise an operator who presses Escape between submit and
      // response leaves the portfolio stale while the server-side
      // fill really did happen.
      onFilled();
      if (mountedRef.current) {
        // Close BEFORE the refetch fires. handleFilled in the parent
        // calls portfolio.refetch(), whose errors must not be hidden
        // behind this modal (prevention #125). If we've already been
        // unmounted (Escape mid-submit), onRequestClose is a no-op.
        onRequestClose();
      }
    } catch (err) {
      // On error we do NOT call onFilled (nothing was persisted). If
      // we're unmounted we drop the error display silently — the
      // operator has already moved on and no portfolio drift is
      // possible on a failure path.
      if (!mountedRef.current) return;
      setSubmitting(false);
      if (err instanceof ApiError) {
        setErrorMessage(err.message);
      } else {
        setErrorMessage(NETWORK_ERROR_PHRASE);
      }
    }
  }

  const title = `Add — ${symbol}`;

  return (
    <Modal isOpen={isOpen} onRequestClose={onRequestClose} label={title}>
      <div className="flex flex-col gap-3">
        <header className="flex items-center justify-between">
          <div>
            <h2 className="text-sm font-semibold text-slate-800">{title}</h2>
            <p className="text-xs text-slate-500">{companyName}</p>
          </div>
          <DemoLivePill />
        </header>

        <PriceContext
          detail={detail.data}
          detailLoading={detail.loading}
          detailError={detail.error}
          onRetry={detail.refetch}
          valuationSource={valuationSource}
        />

        <fieldset className="flex flex-col gap-2" disabled={submitting}>
          <legend className="sr-only">Order amount</legend>
          <div className="flex gap-4 text-xs">
            <label className="inline-flex items-center gap-1">
              <input
                type="radio"
                name="order-input-mode"
                value="amount"
                checked={mode === "amount"}
                onChange={() => {
                  setMode("amount");
                  setRawInput("");
                }}
              />
              <span>Amount</span>
            </label>
            <label className="inline-flex items-center gap-1">
              <input
                type="radio"
                name="order-input-mode"
                value="units"
                checked={mode === "units"}
                onChange={() => {
                  setMode("units");
                  setRawInput("");
                }}
              />
              <span>Units</span>
            </label>
          </div>
          <label className="flex flex-col gap-1 text-xs text-slate-600">
            <span>
              {mode === "amount"
                ? `Notional (${detail.data?.currency ?? "native"})`
                : "Units to buy"}
            </span>
            <input
              type="number"
              inputMode="decimal"
              min="0"
              step={mode === "amount" ? "0.01" : "0.000001"}
              value={rawInput}
              onChange={(e) => setRawInput(e.target.value)}
              placeholder={mode === "amount" ? "250.00" : "2.000000"}
              className="rounded border border-slate-300 bg-white px-2 py-1.5 text-sm focus:border-blue-400 focus:outline-none"
            />
          </label>
        </fieldset>

        <PreviewBlock
          mode={mode}
          parsedValue={parsedValue}
          detail={detail.data}
          priceIsUsable={priceIsUsable}
        />

        <p className="text-[11px] leading-snug text-slate-500">{DISCLAIMER}</p>

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
            className="rounded bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-blue-500 disabled:opacity-50"
          >
            {submitting ? "Placing…" : "Place demo order"}
          </button>
        </div>
      </div>
    </Modal>
  );
}

function parsePositiveFinite(raw: string): number | null {
  if (raw.trim() === "") return null;
  const n = Number(raw);
  if (!Number.isFinite(n)) return null;
  if (n <= 0) return null;
  return n;
}

function PriceContext({
  detail,
  detailLoading,
  detailError,
  onRetry,
  valuationSource,
}: {
  detail: InstrumentPositionDetail | null;
  detailLoading: boolean;
  detailError: unknown;
  onRetry: () => void;
  valuationSource: ValuationSource;
}): JSX.Element {
  if (detailError !== null) {
    return (
      <div className="rounded border border-red-200 bg-red-50 px-2 py-1.5 text-xs text-red-700">
        <span>Could not load price context.</span>{" "}
        <button
          type="button"
          onClick={onRetry}
          className="font-semibold underline"
        >
          Retry
        </button>
      </div>
    );
  }
  if (detailLoading || detail === null) {
    return (
      <div className="text-xs text-slate-500">Loading price context…</div>
    );
  }
  const price = detail.current_price;
  const amber = valuationSource !== "quote";
  return (
    <div className={`text-xs ${amber ? "text-amber-700" : "text-slate-600"}`}>
      <span>
        Currency: {detail.currency} · Latest price:{" "}
        {price !== null ? formatNumber(price, 4) : "—"} ({valuationSource})
      </span>
      {amber ? (
        <span className="ml-1 font-semibold">
          (may not reflect fill price)
        </span>
      ) : null}
    </div>
  );
}

function PreviewBlock({
  mode,
  parsedValue,
  detail,
  priceIsUsable,
}: {
  mode: InputMode;
  parsedValue: number | null;
  detail: InstrumentPositionDetail | null;
  priceIsUsable: boolean;
}): JSX.Element {
  const price = detail?.current_price;
  let estimate = "—";
  if (parsedValue !== null && priceIsUsable && price !== null && price !== undefined) {
    if (mode === "amount") {
      estimate = `Estimated units: ${formatNumber(parsedValue / price, 6)}`;
    } else {
      estimate = `Estimated cost: ${formatNumber(parsedValue * price, 2)} ${detail?.currency ?? ""}`;
    }
  } else if (parsedValue !== null && !priceIsUsable) {
    estimate = "No usable quote — submit will likely 422";
  }
  return (
    <div className="rounded border border-slate-200 bg-slate-50 px-2 py-1.5 text-xs text-slate-700">
      <div>{estimate}</div>
      <div className="text-slate-500">Estimated fees: 0.00 (demo)</div>
    </div>
  );
}
