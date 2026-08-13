/**
 * Sparkline — hand-coded SVG <polyline> sparkline. No external chart
 * dependency.
 *
 * Its one caller is the score-history strip in `VerdictTab` (#2151 corrected
 * the previous claim here that `FundamentalsPane` uses it — that pane replaced
 * its four sparklines with Recharts AreaCharts in the design-system v1
 * redesign, and has not rendered a Sparkline since).
 *
 * Phase 2 (#576): hover tooltip shows the value at the cursor index.
 * #2151: optional last-value label, and a height floor so the plot can never
 * collapse to a sliver.
 *
 * Coloring: default `stroke="currentColor"` lets callers drive the polyline
 * color via a Tailwind `text-*` class on `className` (e.g. `text-emerald-500`).
 * Since #2151 `className` lands on the WRAPPER rather than the <svg>, so the
 * last-value label inherits the same `currentColor` as the stroke and cannot
 * drift from it — including in dark mode, where a bare text colour has no
 * lint gate (prevention-log → "A bare text-<color> class has no dark-mode
 * gate"). For chart-theme alignment (#586), a caller that wants a series
 * colour rather than the inherited text colour should read the RESOLVED
 * palette from the `useChartTheme` hook in `@/lib/useChartTheme` and pass an
 * accent from it as `stroke`. It must not import either raw palette from
 * `@/lib/chartTheme` by name — that hardcodes one mode's colours into a
 * dark-capable component, which is the #2185 defect and is now a
 * `charts:check` failure (#2190). This paragraph deliberately describes the
 * call in prose: these gates are line-based, so quoting the literal here would
 * trip the check on a doc comment that has no violation to fix
 * (prevention-log → #1908 PR-2).
 */

import { useState, useCallback, type JSX } from "react";

/**
 * Floor for the rendered plot height. A sparkline shorter than this reads as a
 * sliver rather than a trend, and the empty (<2 points) branch would otherwise
 * reserve a differently-sized box and shift the row (#2151).
 */
const MIN_HEIGHT = 24;

export interface SparklineProps {
  readonly values: ReadonlyArray<number>;
  readonly width?: number;
  /** Requested plot height. Clamped up to `MIN_HEIGHT` (24px). */
  readonly height?: number;
  readonly stroke?: string;
  /** Applied to the wrapper, so the svg AND the last-value label inherit it. */
  readonly className?: string;
  /** Custom value formatter. Default: 2 decimal places with locale separators. */
  readonly formatValue?: (v: number) => string;
  /**
   * Render the final value as a label after the line. Off by default — a
   * sparkline in a dense table cell usually has no room for it.
   */
  readonly showLastValue?: boolean;
}

interface HoverState {
  idx: number;
}

function defaultFormat(v: number): string {
  return v.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

export function Sparkline({
  values,
  width = 80,
  height = MIN_HEIGHT,
  stroke = "currentColor",
  className,
  formatValue = defaultFormat,
  showLastValue = false,
}: SparklineProps): JSX.Element {
  const [hover, setHover] = useState<HoverState | null>(null);
  const plotHeight = Math.max(height, MIN_HEIGHT);

  const handleMove = useCallback(
    (e: React.MouseEvent<SVGSVGElement>) => {
      if (values.length < 2) return;
      const rect = e.currentTarget.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const xStep = width / (values.length - 1);
      const idx = Math.min(
        Math.max(0, Math.round(x / xStep)),
        values.length - 1,
      );
      setHover({ idx });
    },
    [values.length, width],
  );

  const handleLeave = useCallback(() => {
    setHover(null);
  }, []);

  if (values.length < 2) {
    return (
      <div className={`relative inline-flex items-center ${className ?? ""}`}>
        <svg width={width} height={plotHeight} />
      </div>
    );
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min;
  const xStep = width / (values.length - 1);
  const points = values
    .map((v, i) => {
      const x = i * xStep;
      // When all values are equal (range === 0) center the flat line
      // at plotHeight/2 rather than clipping it to the bottom boundary.
      const y =
        range === 0
          ? plotHeight / 2
          : plotHeight - ((v - min) / range) * plotHeight;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");

  const hoveredValue = hover !== null ? values[hover.idx] : undefined;
  const lastValue = values[values.length - 1];

  return (
    <div
      className={`relative inline-flex items-center gap-1 ${className ?? ""}`}
      onMouseLeave={handleLeave}
    >
      <svg
        width={width}
        height={plotHeight}
        aria-hidden="true"
        onMouseMove={handleMove}
      >
        <polyline
          points={points}
          fill="none"
          stroke={stroke}
          strokeWidth={1.5}
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
      {/* Laid out AFTER the svg rather than absolutely positioned at the final
          point's y: an out-of-flow label in a scrollable list is the #1858
          escape trap, and an in-flow label cannot be clipped by a tight cell.
          Colour is inherited from the wrapper, so it always matches the line. */}
      {showLastValue && lastValue !== undefined ? (
        <span className="text-[10px] tabular-nums" data-testid="sparkline-last-value">
          {formatValue(lastValue)}
        </span>
      ) : null}
      {hover !== null && hoveredValue !== undefined ? (
        <div
          className="absolute left-0 top-full z-10 mt-0.5 whitespace-nowrap rounded bg-slate-800 px-1.5 py-0.5 text-[10px] text-white shadow"
          data-testid="sparkline-tooltip"
        >
          {formatValue(hoveredValue)}
        </div>
      ) : null}
    </div>
  );
}
