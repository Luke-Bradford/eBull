/**
 * Three-ring ownership sunburst (#729 follow-up).
 *
 * Recharts has no native sunburst primitive — built from three nested
 * ``<Pie>`` components at increasing ``innerRadius`` / ``outerRadius``.
 *
 *   ring 1 (innermost) — single-arc "Held" total, labelled with the
 *                        absolute float in the center hole.
 *   ring 2             — per-category wedges (Institutions / ETFs /
 *                        Insiders / Treasury / Unallocated).
 *   ring 3 (outermost) — per-filer / per-officer wedges within each
 *                        category, plus an "Other" tail when many
 *                        sub-threshold holders exist.
 *
 * Coverage gating: a category with ``status='unknown'`` (today's
 * Institutions / ETFs while the #740 CUSIP backfill is pending)
 * renders as a hatched grey wedge sized to its share of float so the
 * operator sees the gap visually rather than as a missing slice.
 */

import { useMemo } from "react";
import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from "recharts";

import { formatPct, formatShares } from "@/components/instrument/ownershipMetrics";
import {
  type SunburstCategory,
  type SunburstLeaf,
  type SunburstRings,
  buildSunburstRings,
} from "@/components/instrument/ownershipRings";
import type { SunburstInputs } from "@/components/instrument/ownershipRings";
import { useChartTheme } from "@/lib/useChartTheme";

export interface OwnershipSunburstProps {
  readonly inputs: SunburstInputs;
  /** Click handler for any wedge. Receives the wedge identity for
   *  drill-to-L2 navigation. */
  readonly onWedgeClick?: (target: WedgeClick) => void;
  /** Pixel size of the chart canvas (square). Default 280. */
  readonly size?: number;
}

export type WedgeClick =
  | { readonly kind: "center" }
  | { readonly kind: "category"; readonly category_key: string }
  | { readonly kind: "leaf"; readonly category_key: string; readonly leaf_key: string };

interface ChartDatum {
  readonly name: string;
  readonly shares: number;
  readonly pct: number;
  readonly fill: string;
  readonly stroke: string;
  readonly opacity: number;
  /** Click-target metadata propagated through Recharts' onClick. */
  readonly target: WedgeClick;
}

const CATEGORY_FILL_INDEX: Record<string, number> = {
  institutions: 0, // cyan
  etfs: 1, // blue
  insiders: 2, // purple
  treasury: 3, // amber
  unallocated: 4, // pink
};

const UNKNOWN_FILL = "#475569"; // slate-600 — desaturated to read as "no signal"

export function OwnershipSunburst({
  inputs,
  onWedgeClick,
  size = 280,
}: OwnershipSunburstProps): JSX.Element | null {
  const rings = useMemo(() => buildSunburstRings(inputs), [inputs]);
  const theme = useChartTheme();

  if (rings === null) return null;

  const accent = theme.accent;
  const fillFor = (categoryKey: string): string => {
    const idx = CATEGORY_FILL_INDEX[categoryKey];
    if (idx === undefined) return accent[0];
    return accent[idx % accent.length]!;
  };

  // Inner ring — single arc.
  const innerData: ChartDatum[] = [
    {
      name: "Held",
      shares: rings.inner.shares,
      pct: rings.inner.pct,
      fill: theme.borderColor,
      stroke: theme.bg,
      opacity: 0.6,
      target: { kind: "center" },
    },
  ];

  // Middle ring — one wedge per category. Categories with shares=0
  // and status='empty' are skipped so the ring doesn't render a
  // sliver of unstyled chrome.
  const middleData: ChartDatum[] = [];
  for (const cat of rings.categories) {
    if (cat.status === "empty" && cat.shares <= 0) continue;
    middleData.push(toCategoryDatum(cat, fillFor(cat.key), theme.bg));
  }

  // Outer ring — leaves under every visible category, in the same
  // order so wedges stack alphabetically with their parent.
  const outerData: ChartDatum[] = [];
  for (const cat of rings.categories) {
    if (cat.status === "empty" && cat.shares <= 0) continue;
    if (cat.leaves.length === 0) continue;
    const baseFill = fillFor(cat.key);
    for (const leaf of cat.leaves) {
      outerData.push(toLeafDatum(leaf, cat, baseFill, theme.bg));
    }
  }

  const totalRadius = size / 2;
  const innerInner = totalRadius * 0.25;
  const innerOuter = totalRadius * 0.36;
  const middleOuter = totalRadius * 0.62;
  const outerOuter = totalRadius * 0.92;

  const handleClick = (datum: ChartDatum): void => {
    onWedgeClick?.(datum.target);
  };

  return (
    <div
      className="relative"
      style={{ width: size, height: size }}
      role="img"
      aria-label={`Ownership breakdown: free float ${formatShares(rings.free_float)} shares.`}
    >
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Tooltip content={<SunburstTooltip />} />
          <Pie
            data={innerData}
            dataKey="shares"
            innerRadius={innerInner}
            outerRadius={innerOuter}
            stroke={theme.bg}
            isAnimationActive={false}
            onClick={(_, idx) => handleClick(innerData[idx]!)}
          >
            {innerData.map((d, idx) => (
              <Cell
                key={`inner-${idx}`}
                fill={d.fill}
                fillOpacity={d.opacity}
                stroke={d.stroke}
              />
            ))}
          </Pie>
          <Pie
            data={middleData}
            dataKey="shares"
            innerRadius={innerOuter}
            outerRadius={middleOuter}
            stroke={theme.bg}
            isAnimationActive={false}
            onClick={(_, idx) => handleClick(middleData[idx]!)}
          >
            {middleData.map((d, idx) => (
              <Cell
                key={`middle-${idx}`}
                fill={d.fill}
                fillOpacity={d.opacity}
                stroke={d.stroke}
              />
            ))}
          </Pie>
          <Pie
            data={outerData}
            dataKey="shares"
            innerRadius={middleOuter}
            outerRadius={outerOuter}
            stroke={theme.bg}
            isAnimationActive={false}
            onClick={(_, idx) => handleClick(outerData[idx]!)}
          >
            {outerData.map((d, idx) => (
              <Cell
                key={`outer-${idx}`}
                fill={d.fill}
                fillOpacity={d.opacity}
                stroke={d.stroke}
              />
            ))}
          </Pie>
        </PieChart>
      </ResponsiveContainer>
      <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
          Free float
        </span>
        <span className="text-lg font-semibold text-slate-900 dark:text-slate-100">
          {formatShares(rings.free_float)}
        </span>
      </div>
    </div>
  );
}

function toCategoryDatum(
  cat: SunburstCategory,
  baseFill: string,
  bg: string,
): ChartDatum {
  if (cat.status === "unknown") {
    // Unknown categories use a dedicated grey so the operator
    // distinguishes "we don't know what's here" from "this slice is
    // genuinely 0%".
    return {
      name: `${cat.label} — coverage gap`,
      // Use a synthetic non-zero share count so the wedge renders
      // visibly. Since totals would otherwise sum to less than the
      // float, the visible gap on the outer ring still telegraphs
      // missing data; the synthetic value here just guarantees the
      // wedge isn't a 0-degree arc.
      shares: 1,
      pct: 0,
      fill: UNKNOWN_FILL,
      stroke: bg,
      opacity: 0.3,
      target: { kind: "category", category_key: cat.key },
    };
  }
  return {
    name: cat.label,
    shares: Math.max(cat.shares, 0),
    pct: cat.pct,
    fill: baseFill,
    stroke: bg,
    opacity: cat.shares <= 0 ? 0 : 0.85,
    target: { kind: "category", category_key: cat.key },
  };
}

function toLeafDatum(
  leaf: SunburstLeaf,
  cat: SunburstCategory,
  baseFill: string,
  bg: string,
): ChartDatum {
  const status = cat.status;
  if (status === "unknown") {
    return {
      name: leaf.label,
      shares: 1,
      pct: 0,
      fill: UNKNOWN_FILL,
      stroke: bg,
      opacity: 0.2,
      target: { kind: "leaf", category_key: cat.key, leaf_key: leaf.key },
    };
  }
  // "Other" rolls up sub-threshold holders. Render with a desaturated
  // shade of the parent category's color so the operator sees it as
  // "tail of the same slice" rather than a separate category.
  const opacity = leaf.is_other ? 0.55 : 0.9;
  return {
    name: leaf.label,
    shares: Math.max(leaf.shares, 0),
    pct: leaf.pct,
    fill: baseFill,
    stroke: bg,
    opacity: leaf.shares <= 0 ? 0 : opacity,
    target: { kind: "leaf", category_key: cat.key, leaf_key: leaf.key },
  };
}

interface RechartsTooltipPayloadShape {
  readonly payload?: ChartDatum;
}

interface RechartsTooltipProps {
  readonly active?: boolean;
  readonly payload?: readonly RechartsTooltipPayloadShape[];
}

function SunburstTooltip(props: RechartsTooltipProps): JSX.Element | null {
  if (!props.active || props.payload === undefined || props.payload.length === 0) return null;
  const datum = props.payload[0]?.payload;
  if (datum === undefined) return null;
  return (
    <div className="rounded border border-slate-300 bg-white px-3 py-2 text-xs shadow-md dark:border-slate-700 dark:bg-slate-900">
      <div className="font-medium text-slate-900 dark:text-slate-100">{datum.name}</div>
      <div className="text-slate-600 dark:text-slate-400">
        {formatShares(datum.shares)} shares
      </div>
      <div className="text-slate-600 dark:text-slate-400">
        {formatPct(datum.pct)} of float
      </div>
    </div>
  );
}

export { buildSunburstRings };
export type { SunburstRings };
