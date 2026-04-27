/**
 * SessionBands — translucent overlay strips that mark pre-market and
 * after-hours bars on an intraday lightweight-charts canvas (#602).
 *
 * The chart is ordinal by default — closed-market gaps (overnight,
 * weekends, holidays) are collapsed and don't need shading. The two
 * windows that DO render but visually differ from RTH are:
 *
 *   * pre-market    04:00–09:30 ET → faint sky tint
 *   * after-hours   16:00–20:00 ET → faint amber tint
 *
 * Closed bars (rare in feed-of-record data — eToro doesn't emit them
 * for US equities) collapse via the ordinal axis so we don't render a
 * band for them.
 *
 * Implementation: absolute-positioned `<div>` siblings of the chart
 * canvas, sized in pixels via `timeScale().timeToCoordinate()`. Both
 * tints use `rgba` with low alpha so they read on light AND dark
 * backgrounds — when the dark theme arrives (#596) no rework needed
 * for bands. Pure overlay, pointer-events none — never intercepts
 * crosshair / clicks.
 *
 * Recomputes on:
 *   * `bars` identity change (range switch, fresh fetch)
 *   * `subscribeVisibleLogicalRangeChange` (zoom / pan)
 *   * `ResizeObserver` on the chart container (layout shift)
 */
import { useEffect, useState, type JSX, type RefObject } from "react";
import type { IChartApi, Time } from "lightweight-charts";

import { classifyUsSession, type SessionKind } from "@/lib/chartFormatters";

interface Band {
  readonly kind: SessionKind;
  readonly left: number;
  readonly width: number;
}

interface PaneInset {
  readonly right: number;
  readonly bottom: number;
}

// Tints chosen to read against both white and slate-900 backgrounds.
// Anything that is NOT regular-trading-hours gets a tint so the
// operator can see at a glance which slice of the day a candle
// belongs to:
//   * pre-market    → sky tint   (US PM, 04:00–09:30 ET)
//   * after-hours   → amber tint (US AH, 16:00–20:00 ET)
//   * closed        → slate grey (overnight, weekends, holidays —
//                                  any bar eToro emits outside
//                                  the three canonical sessions)
//   * RTH           → no tint (the canonical "live market" baseline
//                              the eye reads as default)
const TINT: Record<SessionKind, string | null> = {
  pre: "rgba(56, 189, 248, 0.18)", // sky-400 @ 18%
  rth: null,
  ah: "rgba(245, 158, 11, 0.18)", // amber-500 @ 18%
  closed: "rgba(148, 163, 184, 0.18)", // slate-400 @ 18%
};

export interface SessionBandsProps {
  readonly chartRef: RefObject<IChartApi | null>;
  readonly bars: ReadonlyArray<{ readonly time: number }>;
  /** Daily / weekly / monthly charts have one bar per session — no
   *  intra-bar boundaries to mark. Caller passes `false` so the
   *  overlay short-circuits. */
  readonly enabled: boolean;
}

export function SessionBands({ chartRef, bars, enabled }: SessionBandsProps): JSX.Element | null {
  const [bands, setBands] = useState<Band[]>([]);
  const [inset, setInset] = useState<PaneInset>({ right: 0, bottom: 0 });

  useEffect(() => {
    if (!enabled) {
      setBands([]);
      setInset({ right: 0, bottom: 0 });
      return;
    }

    let cancelled = false;
    let rafReady: number | undefined;
    let rafFirstPaint: number | undefined;
    let attachedTs: ReturnType<IChartApi["timeScale"]> | null = null;
    let ro: ResizeObserver | null = null;

    // recompute reads chartRef.current EVERY call rather than capturing
    // it. The chart may not exist yet at first invocation (React fires
    // child effects before parent effects on mount, so the parent's
    // createChart() runs AFTER this useEffect body).
    const recompute = (): void => {
      const chart = chartRef.current;
      if (chart === null) return;
      const ts = chart.timeScale();

      try {
        setInset({
          right: chart.priceScale("right").width(),
          bottom: ts.height(),
        });
      } catch {
        return;
      }

      if (bars.length < 2) {
        setBands([]);
        return;
      }
      const sessions: SessionKind[] = bars.map((b) => classifyUsSession(b.time));

      // Group consecutive bars with the same session kind into runs.
      const runs: Array<{ kind: SessionKind; startIdx: number; endIdx: number }> = [];
      for (let i = 0; i < bars.length; i++) {
        const k = sessions[i]!;
        const tail = runs[runs.length - 1];
        if (tail !== undefined && tail.kind === k) {
          tail.endIdx = i;
        } else {
          runs.push({ kind: k, startIdx: i, endIdx: i });
        }
      }

      const next: Band[] = [];
      for (const run of runs) {
        if (TINT[run.kind] === null) continue;
        const startBar = bars[run.startIdx]!;
        const endBar = bars[run.endIdx]!;
        const startCoord = ts.timeToCoordinate(startBar.time as Time);
        const endCoord = ts.timeToCoordinate(endBar.time as Time);
        if (startCoord === null || endCoord === null) continue;

        // Local bar spacing — one bar's pixel width. lightweight-charts
        // centers each candle on its bar-time tick; the candle's body
        // visually spans `[coord(T) - barSpacing/2, coord(T) + barSpacing/2]`.
        let barSpacing = 0;
        if (run.endIdx > run.startIdx) {
          barSpacing = Math.abs(endCoord - startCoord) / Math.max(1, run.endIdx - run.startIdx);
        } else {
          const neighbourIdx = run.startIdx > 0 ? run.startIdx - 1 : run.startIdx + 1;
          if (neighbourIdx >= 0 && neighbourIdx < bars.length) {
            const nbCoord = ts.timeToCoordinate(bars[neighbourIdx]!.time as Time);
            if (nbCoord !== null) barSpacing = Math.abs(startCoord - nbCoord);
          }
        }

        // Clock-snap mode (TradingView convention): band left edge
        // sits at `coord(first session bar)` itself — i.e. the
        // session-start clock tick — rather than half-a-bar earlier.
        // Band right edge sits one full bucket past `coord(last
        // session bar)`, which is the session-end clock tick. This
        // means the first/last candle body straddles the band edge,
        // which is what TradingView/Robinhood do because the band's
        // job is to mark THE CLOCK WINDOW, not the candle extent.
        const left = Math.min(startCoord, endCoord);
        const right = Math.max(startCoord, endCoord) + barSpacing;
        next.push({ kind: run.kind, left, width: Math.max(0, right - left) });
      }
      setBands(next);
    };

    // attach() runs once the chart ref is populated AND lightweight-
    // charts has laid out its time scale enough that
    // timeToCoordinate() returns real pixels. Because the parent's
    // createChart() effect runs AFTER this child effect on mount,
    // the chart ref starts null. Poll via RAF until present.
    const attach = (): void => {
      if (cancelled) return;
      const chart = chartRef.current;
      if (chart === null) {
        rafReady = requestAnimationFrame(attach);
        return;
      }
      const ts = chart.timeScale();
      attachedTs = ts;
      ts.subscribeVisibleLogicalRangeChange(recompute);

      try {
        const el = chart.chartElement();
        if (el !== null) {
          ro = new ResizeObserver(recompute);
          ro.observe(el);
        }
      } catch {
        // chart torn down between createChart and observe — skip.
      }

      // First synchronous compute often races the chart's first
      // layout pass — timeToCoordinate returns null for every bar.
      // Schedule a follow-up frame so the time scale is in place
      // before we recompute. The visibleLogicalRangeChange callback
      // and ResizeObserver above provide independent triggers, but
      // both can miss the first paint when fitContent is the only
      // layout-changing call.
      recompute();
      rafFirstPaint = requestAnimationFrame(recompute);
    };

    attach();

    return () => {
      cancelled = true;
      if (rafReady !== undefined) cancelAnimationFrame(rafReady);
      if (rafFirstPaint !== undefined) cancelAnimationFrame(rafFirstPaint);
      if (attachedTs !== null) {
        try {
          attachedTs.unsubscribeVisibleLogicalRangeChange(recompute);
        } catch {
          // chart already torn down — nothing to unsubscribe.
        }
      }
      if (ro !== null) ro.disconnect();
    };
  }, [chartRef, bars, enabled]);

  if (!enabled || bands.length === 0) return null;

  return (
    <div
      aria-hidden="true"
      data-testid="session-bands"
      // z-[5] sits above the chart canvas (which has no stacking
      // context of its own) and below the tooltip + live indicator
      // (z-10). z-0 was tried first but created a sibling stacking
      // context that made the bands invisible against the chart's
      // white fill in some browsers.
      className="pointer-events-none absolute z-[5] overflow-hidden"
      style={{ left: 0, top: 0, right: inset.right, bottom: inset.bottom }}
    >
      {bands.map((b, i) => (
        <div
          key={`${b.kind}-${i}-${b.left}`}
          data-session={b.kind}
          className="absolute bottom-0 top-0"
          style={{ left: `${b.left}px`, width: `${b.width}px`, background: TINT[b.kind] ?? "" }}
        />
      ))}
    </div>
  );
}
