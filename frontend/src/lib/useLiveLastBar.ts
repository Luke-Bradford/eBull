/**
 * useLiveLastBar — feed eToro tick stream into a lightweight-charts
 * series so the in-progress bar updates live (#602).
 *
 * Connects to the existing /sse/quotes endpoint via `useLiveQuote` —
 * the same SSE pipeline the quote panels use — so this hook adds no
 * new backend infrastructure. Each tick lands in `aggregateTick`
 * which decides:
 *
 *   * Same bucket as the chart's last historical bar → update its
 *     high/low/close in-place via `series.update()`. Open is preserved
 *     from the historical fetch.
 *   * Newer bucket than any historical bar → emit a fresh bar that
 *     opens at the tick price (open = high = low = close = tick) and
 *     persists in the aggregator's local state until the next bucket
 *     boundary. Volume stays at 0 — the tick stream does not carry
 *     aggregate per-bar volume; that's the historical fetch's job.
 *   * Older bucket than the last historical bar → ignore. Backfill
 *     from REST handles late-arriving bars on the next range refetch.
 *
 * The aggregator owns one mutable bar in `liveBarRef`. The hook
 * re-syncs that bar from the historical `rows` whenever they change
 * (range switch, refetch, etc.), so a chart that just loaded never
 * starts in a stale-aggregator state.
 *
 * Caller responsibility:
 *   * Provide stable refs to the candle / line / area series so the
 *     aggregator can call `update()` on each.
 *   * Call `attach()` on every render to keep refs current.
 *
 * Volume series is intentionally not touched — see ticket scope
 * notes. Volume aggregation needs a separate per-tick volume field
 * eToro's quote stream doesn't expose; deferred to V2.
 */

import { useEffect, useRef } from "react";
import type { ISeriesApi, Time, UTCTimestamp } from "lightweight-charts";

import { useLiveTick, useLiveQuoteConnection } from "@/components/quotes/LiveQuoteProvider";
import { floorToBucket } from "@/lib/chartData";
import type { LiveTickPayload } from "@/lib/useLiveQuote";

interface LiveBarState {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
}

export interface LiveLastBarRefs {
  candle: ISeriesApi<"Candlestick"> | null;
  line: ISeriesApi<"Line"> | null;
  area: ISeriesApi<"Area"> | null;
}

/**
 * Pull the best price out of a tick. Prefers `last`, falls back to
 * the bid/ask midpoint when last is null (typical at market open
 * before the first execution).
 */
function tickPrice(tick: LiveTickPayload): number | null {
  const last = tick.last !== null && tick.last !== undefined ? Number(tick.last) : null;
  if (last !== null && Number.isFinite(last)) return last;
  const bid = Number(tick.bid);
  const ask = Number(tick.ask);
  if (Number.isFinite(bid) && Number.isFinite(ask)) return (bid + ask) / 2;
  if (Number.isFinite(bid)) return bid;
  if (Number.isFinite(ask)) return ask;
  return null;
}

export interface HistoricalLastBar {
  time: number;
  open: number;
  high: number;
  low: number;
}

/**
 * Aggregate one tick into a new live-bar state.
 *
 * Pure function — no I/O, no series mutation. Consumes the previous
 * live bar (if any), the historical last bar (if the chart has rendered
 * one), the tick price + timestamp, and the bucket size.
 *
 * Three outcomes:
 *   * `update` — tick lands in the same bucket as the historical last
 *     bar OR the live bar; preserves the historical open and extends
 *     high/low from whichever existing values are tracked.
 *   * `append` — tick crosses into a fresh bucket beyond the
 *     historical anchor; opens a new bar at the tick price.
 *   * `skip` — tick predates the historical anchor (a late delivery
 *     from a stale connection); the next REST refetch handles it.
 */
export function aggregateTick(args: {
  prev: LiveBarState | null;
  histLastBar: HistoricalLastBar | null;
  bucketSeconds: number;
  tickEpochSeconds: number;
  tickPrice: number;
}): { next: LiveBarState; verdict: "update" | "append" } | { verdict: "skip" } {
  const { prev, histLastBar, bucketSeconds, tickEpochSeconds, tickPrice: price } = args;
  const histLastTime = histLastBar !== null ? histLastBar.time : null;
  const bucket = floorToBucket(tickEpochSeconds, bucketSeconds);

  // Tick is older than the chart's last historical bar — backfill
  // from the next REST refetch will catch it; ignore here.
  if (histLastTime !== null && bucket < histLastTime) {
    return { verdict: "skip" };
  }

  // Pick the open / high / low to extend from. Priority:
  //   1. The live bar in this bucket (preserves earlier ticks).
  //   2. The historical bar in this bucket (preserves the OHLC the
  //      REST fetch produced — rewriting the open from a tick
  //      mid-bar visibly rewrites a candle on first tick, which the
  //      operator reads as a price spike that didn't happen).
  //   3. Otherwise this tick is opening a brand-new bucket.
  let carryOpen: number;
  let carryHigh: number;
  let carryLow: number;
  if (prev !== null && prev.time === bucket) {
    carryOpen = prev.open;
    carryHigh = Math.max(prev.high, price);
    carryLow = Math.min(prev.low, price);
  } else if (histLastBar !== null && histLastBar.time === bucket) {
    carryOpen = histLastBar.open;
    carryHigh = Math.max(histLastBar.high, price);
    carryLow = Math.min(histLastBar.low, price);
  } else {
    carryOpen = price;
    carryHigh = price;
    carryLow = price;
  }
  const next: LiveBarState = {
    time: bucket,
    open: carryOpen,
    high: carryHigh,
    low: carryLow,
    close: price,
  };
  const verdict =
    (histLastTime !== null && bucket === histLastTime) ||
    (prev !== null && prev.time === bucket)
      ? "update"
      : "append";
  return { next, verdict };
}

export interface UseLiveLastBarParams {
  instrumentId: number | null | undefined;
  bucketSeconds: number;
  /** Last bar already rendered on the chart (post any null-OHLC
   *  filter), or null when the chart hasn't loaded yet. The
   *  aggregator uses its OHLC to preserve the open across the first
   *  live tick of the in-progress bar. */
  historicalLastBar: HistoricalLastBar | null;
  refs: LiveLastBarRefs;
}

export interface UseLiveLastBarResult {
  /** True while an SSE stream is active for this instrument. */
  connected: boolean;
  /** True if the SSE stream errored irrecoverably — the chart should
   *  not show a "LIVE" indicator. */
  unavailable: boolean;
}

/**
 * Subscribe to the page-level LiveQuoteProvider stream for a given
 * instrument and feed each tick into `aggregateTick`. Reading from
 * the shared provider (not opening a per-component SSE) means a
 * page that already renders a live-quote consumer for the same
 * instrument (e.g. SummaryStrip) shares one SSE handshake — the
 * "one stream per page, same id rendered twice shares the stream"
 * invariant documented in LiveQuoteProvider.
 *
 * The page must mount LiveQuoteProvider with the instrument id in
 * its visible-id list — otherwise no ticks arrive and the chart
 * silently degrades to its REST snapshot.
 */
export function useLiveLastBar({
  instrumentId,
  bucketSeconds,
  historicalLastBar,
  refs,
}: UseLiveLastBarParams): UseLiveLastBarResult {
  const tick = useLiveTick(instrumentId);
  const { connected, unavailable } = useLiveQuoteConnection();
  const liveBarRef = useRef<LiveBarState | null>(null);

  const histAnchorTime = historicalLastBar !== null ? historicalLastBar.time : null;

  // Reset the aggregator when the historical anchor moves (range
  // switch, refetch, instrument change). Without this the next tick
  // could update the wrong bar's H/L/C.
  useEffect(() => {
    liveBarRef.current = null;
  }, [instrumentId, bucketSeconds, histAnchorTime]);

  useEffect(() => {
    if (tick === null) return;
    if (tick.instrument_id !== instrumentId) return;
    const price = tickPriceFor(tick);
    if (price === null) return;
    const tickEpoch = Math.floor(new Date(tick.quoted_at).getTime() / 1000);
    if (!Number.isFinite(tickEpoch)) return;

    const result = aggregateTick({
      prev: liveBarRef.current,
      histLastBar: historicalLastBar,
      bucketSeconds,
      tickEpochSeconds: tickEpoch,
      tickPrice: price,
    });
    if (result.verdict === "skip") return;

    liveBarRef.current = result.next;

    const time = result.next.time as UTCTimestamp;
    if (refs.candle !== null) {
      refs.candle.update({
        time: time as Time,
        open: result.next.open,
        high: result.next.high,
        low: result.next.low,
        close: result.next.close,
      });
    }
    if (refs.line !== null) {
      refs.line.update({ time: time as Time, value: result.next.close });
    }
    if (refs.area !== null) {
      refs.area.update({ time: time as Time, value: result.next.close });
    }
  }, [tick, bucketSeconds, historicalLastBar, refs, instrumentId]);

  return { connected, unavailable };
}

// Renamed exports to keep tickPrice as a private helper.
const tickPriceFor = tickPrice;
