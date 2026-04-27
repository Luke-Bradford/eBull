/**
 * Sparkline — hand-coded SVG <polyline> sparkline. No external chart
 * dependency. Used in `FundamentalsPane` for compact 8-point time
 * series (revenue, op income, net income, total debt over 8 quarters).
 */

import type { JSX } from "react";

export interface SparklineProps {
  readonly values: ReadonlyArray<number>;
  readonly width?: number;
  readonly height?: number;
  readonly stroke?: string;
  readonly className?: string;
}

export function Sparkline({
  values,
  width = 80,
  height = 24,
  stroke = "currentColor",
  className,
}: SparklineProps): JSX.Element {
  if (values.length < 2) {
    return <svg width={width} height={height} className={className} />;
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const xStep = width / (values.length - 1);
  const points = values
    .map((v, i) => {
      const x = i * xStep;
      const y = height - ((v - min) / range) * height;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  return (
    <svg
      width={width}
      height={height}
      className={className}
      aria-hidden="true"
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
  );
}
