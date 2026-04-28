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

import { useEffect, useRef, useState, type MutableRefObject } from "react";
import type { ISeriesApi, Time, UTCTimestamp } from "lightweight-charts";

import { useLiveTick, useLiveQuoteConnection } from "@/components/quotes/LiveQuoteProvider";
import { floorToBucket } from "@/lib/chartData";
import { classifyUsSession } from "@/lib/chartFormatters";
import type { LiveTickPayload } from "@/lib/useLiveQuote";

interface LiveBarState {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
}

/**
 * The hook accepts the caller's lightweight-charts series **refs**
 * (not their `.current` snapshots) so the dependency list stays
 * stable across renders. Passing `{ candle: ref.current }` would
 * make a fresh object on every parent render and re-fire the effect,
 * re-applying the last tick's `series.update()` on every re-render —
 * see Codex pre-push #602 review feedback.
 */
export interface LiveLastBarRefs {
  candle: MutableRefObject<ISeriesApi<"Candlestick"> | null>;
  line: MutableRefObject<ISeriesApi<"Line"> | null> | null;
  area: MutableRefObject<ISeriesApi<"Area"> | null> | null;
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
  /** Drop pre-market ticks before they reach the aggregator. Defaults
   *  true (no filtering). When false, an arriving 04:00–09:30 ET tick
   *  is silently dropped — pairs with the chart-level PM/AH visibility
   *  toggles so a hidden session never gets a fresh bar appended. */
  acceptPre?: boolean;
  /** Drop after-hours ticks (16:00–20:00 ET). Same contract as
   *  `acceptPre`. */
  acceptAh?: boolean;
  /**
   * Fired after the aggregator applies a tick to the visible series
   * (post-`series.update`). Lets the caller mirror the live state into
   * any state derived from the historical fetch — e.g. PriceChart's
   * setData fingerprint, which would otherwise treat the next REST
   * refetch (when it catches up to the live bar) as a fresh dataset
   * and trigger a wholesale repaint flash. The argument is the bar
   * the aggregator just rendered: `kind: "update"` mutated the
   * historical last bar in place, `kind: "append"` added a fresh
   * bucket beyond it. Open/high/low/close come straight from
   * aggregateTick's output. Volume is null because the tick stream
   * does not carry per-bar volume — the caller must keep its own
   * (e.g. carry the historical volume forward on update, leave
   * volume null on append until REST backfills).
   */
  onApplied?: (applied: AppliedTick) => void;
}

export interface AppliedTick {
  kind: "update" | "append";
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
}

export interface UseLiveLastBarResult {
  /** True while an SSE stream is active for this instrument. */
  connected: boolean;
  /** True if the SSE stream errored irrecoverably — the chart should
   *  not show a "LIVE" indicator. */
  unavailable: boolean;
  /** Diagnostics: count of ticks the aggregator has actually applied
   *  to the chart (post-skip, post-filter). Surfaces stuck-stream
   *  bugs to the operator without needing devtools. */
  appliedTicks: number;
  /** Diagnostics: ISO timestamp of the most recent applied tick. */
  lastAppliedAt: string | null;
  /** Diagnostics: latest verdict from `aggregateTick` — "update",
   *  "append", "skip", or null when no tick has arrived. */
  lastVerdict: "update" | "append" | "skip" | null;
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
  acceptPre = true,
  acceptAh = true,
  onApplied,
}: UseLiveLastBarParams): UseLiveLastBarResult {
  // Stash in a ref so the apply effect doesn't re-fire when the
  // caller passes an inline callback. Pattern matches the `refs`
  // contract above — caller can pass a fresh function each render.
  const onAppliedRef = useRef(onApplied);
  onAppliedRef.current = onApplied;
  const tick = useLiveTick(instrumentId);
  const { connected, unavailable } = useLiveQuoteConnection();
  const liveBarRef = useRef<LiveBarState | null>(null);
  // Dedupe key so toggling effect deps (`acceptPre`/`acceptAh`,
  // `bucketSeconds`, `historicalLastBar`) doesn't re-apply the SAME
  // tick we already processed. The effect is keyed on `tick` (and
  // those props) — without this guard, the latest tick replays on
  // every prop change, inflating `appliedTicks` and issuing redundant
  // `series.update()` calls (Codex review #602).
  const lastAppliedKeyRef = useRef<string | null>(null);
  const [appliedTicks, setAppliedTicks] = useState(0);
  const [lastAppliedAt, setLastAppliedAt] = useState<string | null>(null);
  const [lastVerdict, setLastVerdict] = useState<
    "update" | "append" | "skip" | null
  >(null);

  const histAnchorTime = historicalLastBar !== null ? historicalLastBar.time : null;

  // Reset the aggregator when the historical anchor moves (range
  // switch, refetch, instrument change). Without this the next tick
  // could update the wrong bar's H/L/C.
  useEffect(() => {
    liveBarRef.current = null;
    lastAppliedKeyRef.current = null;
    setAppliedTicks(0);
    setLastAppliedAt(null);
    setLastVerdict(null);
  }, [instrumentId, bucketSeconds, histAnchorTime]);

  useEffect(() => {
    if (tick === null) return;
    if (tick.instrument_id !== instrumentId) return;
    // Dedupe by quoted_at — re-running the effect on prop change
    // (e.g. toggling acceptPre/acceptAh) must not replay the last tick.
    const dedupeKey = `${tick.instrument_id}:${tick.quoted_at}`;
    if (lastAppliedKeyRef.current === dedupeKey) return;
    const price = tickPriceFor(tick);
    if (price === null) return;
    const tickEpoch = Math.floor(new Date(tick.quoted_at).getTime() / 1000);
    if (!Number.isFinite(tickEpoch)) return;

    // Session-visibility gate (pairs with PriceChart's PM/AH toggles).
    // Drop ticks whose session is hidden — without this a fresh bar
    // would be appended into a session the operator chose to hide.
    //
    // IMPORTANT: record the dedupe key BEFORE the early return so a
    // subsequent toggle of `acceptPre`/`acceptAh` from false→true
    // doesn't retroactively apply the stale tick when the effect
    // re-fires. The filter is "going-forward" — a tick that was
    // rejected stays rejected even if the user later opens that
    // session. PR #610 round 3 review WARNING.
    const tickSession = classifyUsSession(tickEpoch);
    if (tickSession === "pre" && !acceptPre) {
      lastAppliedKeyRef.current = dedupeKey;
      return;
    }
    if (tickSession === "ah" && !acceptAh) {
      lastAppliedKeyRef.current = dedupeKey;
      return;
    }

    const result = aggregateTick({
      prev: liveBarRef.current,
      histLastBar: historicalLastBar,
      bucketSeconds,
      tickEpochSeconds: tickEpoch,
      tickPrice: price,
    });
    setLastVerdict(result.verdict);
    if (result.verdict === "skip") return;

    liveBarRef.current = result.next;
    lastAppliedKeyRef.current = dedupeKey;
    setAppliedTicks((n) => n + 1);
    setLastAppliedAt(tick.quoted_at);

    const time = result.next.time as UTCTimestamp;
    const candleSeries = refs.candle.current;
    if (candleSeries !== null) {
      candleSeries.update({
        time: time as Time,
        open: result.next.open,
        high: result.next.high,
        low: result.next.low,
        close: result.next.close,
      });
    }
    const lineSeries = refs.line !== null ? refs.line.current : null;
    if (lineSeries !== null) {
      lineSeries.update({ time: time as Time, value: result.next.close });
    }
    const areaSeries = refs.area !== null ? refs.area.current : null;
    if (areaSeries !== null) {
      areaSeries.update({ time: time as Time, value: result.next.close });
    }
    // Notify the caller post-apply so it can mirror the live state
    // into derived caches (e.g. PriceChart's setData fingerprint).
    // Read through the ref so an inline callback identity change in
    // the parent does not refire this effect.
    if (onAppliedRef.current !== undefined) {
      onAppliedRef.current({
        kind: result.verdict,
        time: result.next.time,
        open: result.next.open,
        high: result.next.high,
        low: result.next.low,
        close: result.next.close,
      });
    }
    // ESLint: refs.* are MutableRefObjects with stable identity, so
    // omitting them from deps is correct. The effect must re-fire only
    // on tick / anchor / bucket / instrument / session-visibility
    // changes — adding `refs` would re-fire on every parent render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tick, bucketSeconds, historicalLastBar, instrumentId, acceptPre, acceptAh]);

  return { connected, unavailable, appliedTicks, lastAppliedAt, lastVerdict };
}

// Renamed exports to keep tickPrice as a private helper.
const tickPriceFor = tickPrice;
