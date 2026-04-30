/**
 * Sparkline — hand-coded SVG <polyline> sparkline. No external chart
 * dependency. Used in `FundamentalsPane` for compact 8-point time
 * series (revenue, op income, net income, total debt over 8 quarters).
 *
 * Phase 2 (#576): hover tooltip shows the value at the cursor index.
 *
 * Coloring: default `stroke="currentColor"` lets callers drive the
 * polyline color via a Tailwind `text-*` class on `className` (e.g.
 * `text-emerald-500`). Existing FundamentalsPane callers rely on this.
 * For chart-theme alignment, new callers should pass
 * `stroke={lightTheme.accent[N]}` from `@/lib/chartTheme` — see #586.
 */

import { useState, useCallback, type JSX } from "react";

export interface SparklineProps {
  readonly values: ReadonlyArray<number>;
  readonly width?: number;
  readonly height?: number;
  readonly stroke?: string;
  readonly className?: string;
  /** Custom value formatter. Default: 2 decimal places with locale separators. */
  readonly formatValue?: (v: number) => string;
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
  height = 24,
  stroke = "currentColor",
  className,
  formatValue = defaultFormat,
}: SparklineProps): JSX.Element {
  const [hover, setHover] = useState<HoverState | null>(null);

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
    return <svg width={width} height={height} className={className} />;
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min;
  const xStep = width / (values.length - 1);
  const points = values
    .map((v, i) => {
      const x = i * xStep;
      // When all values are equal (range === 0) center the flat line
      // at height/2 rather than clipping it to the bottom boundary.
      const y =
        range === 0
          ? height / 2
          : height - ((v - min) / range) * height;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");

  const hoveredValue = hover !== null ? values[hover.idx] : undefined;

  return (
    <div className="relative inline-block" onMouseLeave={handleLeave}>
      <svg
        width={width}
        height={height}
        className={className}
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
