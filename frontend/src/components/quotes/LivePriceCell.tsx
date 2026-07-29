/**
 * LivePriceCell — visibility-driven live price overlay (#501).
 *
 * Reads the latest tick for ``instrumentId`` from the surrounding
 * :func:`LiveQuoteProvider` and renders the operator's display
 * currency price. Until the first tick lands, falls back to the
 * REST snapshot ``fallback`` so first paint is never blank for an
 * instrument the operator has just been viewing.
 *
 * Per the spec
 * (docs/superpowers/specs/2026-04-25-visibility-driven-live-prices-spec.md
 * Invariant 3): the snapshot frame is best-effort. For halted /
 * illiquid / never-traded instruments eToro may not push a tick,
 * in which case the REST fallback stays on screen indefinitely —
 * that's correct behaviour, not a bug.
 */
import { liveTickDisplayPrice } from "@/lib/useLiveQuote";
import { formatMoney } from "@/lib/format";
import { useLiveTick } from "./LiveQuoteProvider";

interface LivePriceCellProps {
  instrumentId: number | null | undefined;
  /** REST-snapshot price already on the page when this cell mounts.
   *  Rendered until the first live tick arrives. */
  fallback: number | null | undefined;
  /** Operator's display currency (e.g. "USD", "GBP"). Live ticks
   *  carry their own currency code in the ``display`` block; the
   *  fallback uses this. */
  currency: string | null;
}

export function LivePriceCell({
  instrumentId,
  fallback,
  currency,
}: LivePriceCellProps) {
  const tick = useLiveTick(instrumentId);
  const live = liveTickDisplayPrice(tick);
  if (live !== null) {
    const numeric = Number(live.value);
    if (Number.isFinite(numeric)) {
      return <span>{formatMoney(numeric, live.currency ?? currency ?? "USD")}</span>;
    }
  }
  if (fallback === null || fallback === undefined) {
    return <span className="text-slate-300">—</span>;
  }
  return <span className="text-slate-500">{formatMoney(fallback, currency ?? "USD")}</span>;
}
