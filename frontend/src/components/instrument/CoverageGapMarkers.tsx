/**
 * CoverageGapMarkers — dashed vertical lines marking intrasession data gaps
 * on an intraday lightweight-charts canvas (#1754 Phase C).
 *
 * eToro intraday feeds are sometimes sparse: a run of buckets is simply
 * missing inside a trading session. On the chart's ordinal axis those bars
 * render adjacent, hiding the hole. This overlay draws a faint dashed vertical
 * line at each gap (`detectCoverageGaps`) so the operator can see where
 * coverage is missing — distinct from the expected overnight/weekend gaps the
 * ordinal axis collapses (and which the detector deliberately ignores).
 *
 * Only US-equity profiles have a precise enough session model; the detector
 * returns nothing for foreign/continuous, so this renders nothing there.
 *
 * Implementation mirrors `SessionBands`: absolute-positioned, pointer-events
 * none, coordinates via `timeScale().timeToCoordinate()`, recomputed on
 * zoom/pan/resize. The interval (bucket size) is derived from the smallest
 * adjacent bar delta — robust to the gaps themselves.
 */
import { useEffect, useState, type JSX, type RefObject } from "react";
import type { IChartApi, Time } from "lightweight-charts";

import type { SessionProfile } from "@/api/types";
import { detectCoverageGaps, type MarketSpecials } from "@/lib/chartFormatters";

function _minInterval(bars: ReadonlyArray<{ readonly time: number }>): number {
  let min = Infinity;
  for (let i = 1; i < bars.length; i++) {
    const d = bars[i]!.time - bars[i - 1]!.time;
    if (d > 0 && d < min) min = d;
  }
  return Number.isFinite(min) ? min : 0;
}

export interface CoverageGapMarkersProps {
  readonly chartRef: RefObject<IChartApi | null>;
  /** Full provider bar set (NOT the PM/AH-visibility-filtered subset), so a
   *  user toggle can't fabricate a gap. */
  readonly bars: ReadonlyArray<{ readonly time: number }>;
  /** Daily/weekly charts have one bar per session — no intraday gaps. */
  readonly enabled: boolean;
  readonly profile: SessionProfile;
  readonly specials: MarketSpecials;
}

export function CoverageGapMarkers({
  chartRef,
  bars,
  enabled,
  profile,
  specials,
}: CoverageGapMarkersProps): JSX.Element | null {
  const [lefts, setLefts] = useState<number[]>([]);
  const [bottomInset, setBottomInset] = useState(0);
  const [rightInset, setRightInset] = useState(0);

  useEffect(() => {
    if (!enabled) {
      setLefts([]);
      return;
    }
    let cancelled = false;
    let rafReady: number | undefined;
    let rafFirstPaint: number | undefined;
    let attachedTs: ReturnType<IChartApi["timeScale"]> | null = null;
    let ro: ResizeObserver | null = null;

    const recompute = (): void => {
      const chart = chartRef.current;
      if (chart === null) return;
      const ts = chart.timeScale();
      try {
        setRightInset(chart.priceScale("right").width());
        setBottomInset(ts.height());
      } catch {
        return;
      }
      const interval = _minInterval(bars);
      const gapIdx = detectCoverageGaps(bars, interval, profile, specials);
      const next: number[] = [];
      for (const i of gapIdx) {
        const a = ts.timeToCoordinate(bars[i - 1]!.time as Time);
        const b = ts.timeToCoordinate(bars[i]!.time as Time);
        if (a === null || b === null) continue;
        next.push((a + b) / 2); // midpoint of the collapsed (adjacent) pair
      }
      setLefts(next);
    };

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
        // torn down between createChart and observe — skip.
      }
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
          // already torn down.
        }
      }
      if (ro !== null) ro.disconnect();
    };
  }, [chartRef, bars, enabled, profile, specials]);

  if (!enabled || lefts.length === 0) return null;

  return (
    <div
      aria-hidden="true"
      data-testid="coverage-gap-markers"
      className="pointer-events-none absolute z-[5] overflow-hidden"
      style={{ left: 0, top: 0, right: rightInset, bottom: bottomInset }}
    >
      {lefts.map((x, i) => (
        <div
          key={`gap-${i}-${x}`}
          data-testid="coverage-gap-line"
          className="absolute bottom-0 top-0 border-l border-dashed border-slate-400/60"
          style={{ left: `${x}px` }}
          title="Missing intraday data (coverage gap)"
        />
      ))}
    </div>
  );
}
