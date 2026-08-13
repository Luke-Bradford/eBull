/**
 * LivePriceCell — visibility-driven live price overlay (#501).
 *
 * Reads the latest tick for ``instrumentId`` from the surrounding
 * :func:`LiveQuoteProvider` and renders the price **in ``currency``** —
 * the one currency its row is denominated in. Until the first tick
 * lands, falls back to the REST snapshot ``fallback`` so first paint is
 * never blank for an instrument the operator has just been viewing.
 *
 * The cell renders one currency on both paths (#2133). Previously the
 * fallback used ``currency`` while a live tick re-labelled itself with
 * the tick's own display block, so a row left in native currency by an
 * FX-degrade (#2129) flipped its Price to the display currency on the
 * first tick while Invested / Value / P&L stayed native.
 *
 * Per the spec
 * (docs/superpowers/specs/2026-04-25-visibility-driven-live-prices-spec.md
 * Invariant 3): the snapshot frame is best-effort. For halted /
 * illiquid / never-traded instruments eToro may not push a tick,
 * in which case the REST fallback stays on screen indefinitely —
 * that's correct behaviour, not a bug.
 */
import { liveTickPriceIn } from "@/lib/useLiveQuote";
import { formatMoney } from "@/lib/format";
import { useLiveTick } from "./LiveQuoteProvider";

interface LivePriceCellProps {
  instrumentId: number | null | undefined;
  /** REST-snapshot price already on the page when this cell mounts.
   *  Rendered until the first live tick arrives — and kept whenever no
   *  tick block is denominated in ``currency``. */
  fallback: number | null | undefined;
  /** The currency this cell's ROW is denominated in — the operator's
   *  display currency normally, the instrument's native currency on an
   *  FX-degrade (#2129). Both the fallback and the live tick are
   *  rendered in it; a tick that cannot supply it is not shown. */
  currency: string | null;
}

export function LivePriceCell({
  instrumentId,
  fallback,
  currency,
}: LivePriceCellProps) {
  const tick = useLiveTick(instrumentId);
  const live = liveTickPriceIn(tick, currency);
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
