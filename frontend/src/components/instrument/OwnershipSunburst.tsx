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
  /**
   * True for synthetic wedges that exist only to convey "we don't
   * know the number" — e.g. a Coverage gap (#740) category rendered
   * with a placeholder ``shares=1`` so the arc has visible thickness.
   * The tooltip suppresses the numeric share/pct rows for these
   * datums so hovering doesn't surface "1 shares / 0% of float".
   */
  readonly is_gap: boolean;
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

/**
 * Opacity applied to a category's accent color when the category
 * status is ``unknown``. Pre-fix every unknown wedge collapsed to a
 * single shared slate-600 so the chart read as a solid grey blob —
 * operator couldn't distinguish "Institutions gap" from "ETFs gap"
 * from "Treasury gap". Each category keeps its accent identity; the
 * lower opacity carries the "no signal" semantic.
 *
 * Opacity bumped on the second pass — at 0.35 / 0.22 over a
 * slate-950 dark-mode background every accent washed out to muddy
 * indistinguishable purples. Higher values keep the accent
 * recognisable while still reading as desaturated vs known data
 * (~85% opacity).
 */
const GAP_CATEGORY_OPACITY = 0.7;
const GAP_LEAF_OPACITY = 0.5;

/**
 * Module-level CSS string for the sunburst's wedge interactions.
 * Defined out-of-line because TypeScript's JSX strict-mode child
 * typing rejects template-literal children of bare ``<style>`` (it
 * sees ``string`` as a value-not-callable when it expects
 * ``ReactNode``). Plain string assignment side-steps the parse trip.
 *
 * Click affordance: suppress only the mouse-click focus rect so the
 * browser's default white rectangle stops drawing on click. Keyboard
 * focus (``:focus-visible``) keeps a custom outline so keyboard
 * users retain visual feedback.
 *
 * Hover affordance: stroke-width bump + ``filter: brightness(...)``.
 * Crucially does NOT set CSS ``opacity`` — the wedges encode
 * known-vs-coverage-gap via SVG ``fillOpacity``; a CSS opacity hover
 * rule would override that and snap a 0.5-opacity gap wedge to
 * fully opaque, erasing the "no signal" semantic.
 */
const SUNBURST_STYLES = [
  ".ownership-sunburst .recharts-pie-sector path {",
  "  transition: stroke-width 120ms ease, filter 120ms ease;",
  "  cursor: pointer;",
  "}",
  ".ownership-sunburst .recharts-pie-sector path:focus {",
  "  outline: none;",
  "}",
  ".ownership-sunburst .recharts-pie-sector path:focus-visible {",
  "  outline: 2px solid currentColor;",
  "  outline-offset: -1px;",
  "}",
  ".ownership-sunburst .recharts-pie-sector:hover path {",
  "  stroke-width: 2;",
  "  filter: brightness(1.2);",
  "}",
].join("\n");

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

  // Inner ring — known + gap split. Pre-#746-followup the inner
  // ring rendered as a single 100% arc that included synthetic
  // unknown-padding from upstream categories — visually misleading
  // when most of the float was in coverage gaps. Two arcs make the
  // gap proportion legible at the very center of the chart.
  const innerData: ChartDatum[] = [];
  if (rings.inner.known_shares > 0) {
    innerData.push({
      name: "Known",
      shares: rings.inner.known_shares,
      pct: rings.inner.known_pct,
      fill: theme.borderColor,
      stroke: theme.bg,
      opacity: 0.7,
      is_gap: false,
      target: { kind: "center" },
    });
  }
  if (rings.inner.gap_shares > 0) {
    // Inner-ring gap arc represents the aggregate "we don't know"
    // share — keep it as a single neutral grey so it reads as
    // "missing data" rather than implying it belongs to any one
    // accent-colored category.
    innerData.push({
      name: "Coverage gap",
      shares: rings.inner.gap_shares,
      pct: rings.inner.gap_pct,
      fill: theme.gridLine,
      stroke: theme.bg,
      opacity: 0.6,
      is_gap: true,
      target: { kind: "center" },
    });
  }
  // Defensive: if both segments are zero (degenerate), still render
  // a single placeholder so the inner ring outline is preserved.
  if (innerData.length === 0) {
    innerData.push({
      name: "Held",
      shares: 1,
      pct: 0,
      fill: theme.borderColor,
      stroke: theme.bg,
      opacity: 0.4,
      is_gap: true,
      target: { kind: "center" },
    });
  }

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

  // Wedge stroke uses the theme's grid-line slate (slate-100 light /
  // slate-800 dark) rather than the page background. With
  // bg-coloured strokes between two adjacent dark-mode wedges at
  // <100% opacity, the dark stroke + dark fill merged into one
  // blob; a slightly lighter slate stroke keeps wedge boundaries
  // legible without dominating the canvas.
  const wedgeStroke = theme.gridLine;

  return (
    <div
      className="ownership-sunburst relative"
      style={{ width: size, height: size }}
      role="img"
      aria-label={`Ownership breakdown: free float ${formatShares(rings.free_float)} shares.`}
    >
      {/*
        Suppress the browser's default focus rect on Recharts'
        inner SVG path elements -- clicking a wedge moved focus to
        the path which then drew a white rectangular outline that
        read as "selected" but ignored the wedge geometry. Use a
        wedge-shaped feedback affordance instead: hover bumps
        stroke + brightness; click triggers the existing
        onWedgeClick navigation. The ownership-sunburst class
        scopes the outline removal so it does not bleed to other
        charts.
      */}
      <style>{SUNBURST_STYLES}</style>
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Tooltip content={<SunburstTooltip />} />
          <Pie
            data={innerData}
            dataKey="shares"
            innerRadius={innerInner}
            outerRadius={innerOuter}
            stroke={wedgeStroke}
            isAnimationActive={false}
            onClick={(_, idx) => handleClick(innerData[idx]!)}
          >
            {innerData.map((d, idx) => (
              <Cell
                key={`inner-${idx}`}
                fill={d.fill}
                fillOpacity={d.opacity}
                stroke={wedgeStroke}
              />
            ))}
          </Pie>
          <Pie
            data={middleData}
            dataKey="shares"
            innerRadius={innerOuter}
            outerRadius={middleOuter}
            stroke={wedgeStroke}
            isAnimationActive={false}
            onClick={(_, idx) => handleClick(middleData[idx]!)}
          >
            {middleData.map((d, idx) => (
              <Cell
                key={`middle-${idx}`}
                fill={d.fill}
                fillOpacity={d.opacity}
                stroke={wedgeStroke}
              />
            ))}
          </Pie>
          <Pie
            data={outerData}
            dataKey="shares"
            innerRadius={middleOuter}
            outerRadius={outerOuter}
            stroke={wedgeStroke}
            isAnimationActive={false}
            onClick={(_, idx) => handleClick(outerData[idx]!)}
          >
            {outerData.map((d, idx) => (
              <Cell
                key={`outer-${idx}`}
                fill={d.fill}
                fillOpacity={d.opacity}
                stroke={wedgeStroke}
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
    // Unknown categories keep their accent color but at low opacity
    // so the chart distinguishes "Institutions gap" from "ETFs gap"
    // from "Treasury gap" visually. Pre-fix every unknown wedge
    // collapsed to a single shared slate-600 → solid grey blob with
    // no per-category identity.
    return {
      name: `${cat.label} — coverage gap`,
      // Synthetic non-zero share count so the wedge renders visibly.
      // ``is_gap=true`` tells the tooltip to suppress the numeric
      // share/pct rows so hovering does not surface a misleading
      // "1 shares".
      shares: 1,
      pct: 0,
      fill: baseFill,
      stroke: bg,
      opacity: GAP_CATEGORY_OPACITY,
      is_gap: true,
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
    is_gap: false,
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
    // Outer-ring leaf for an unknown category inherits the parent
    // accent at a lower opacity than the middle wedge so the rings
    // remain visually distinguishable while preserving category
    // identity. Pre-fix the leaf used the same shared slate-600 as
    // the middle wedge → both rings merged into one solid grey arc.
    return {
      name: leaf.label,
      shares: 1,
      pct: 0,
      fill: baseFill,
      stroke: bg,
      opacity: GAP_LEAF_OPACITY,
      is_gap: true,
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
    is_gap: false,
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
  // Coverage-gap wedges carry synthetic ``shares=1`` so the arc has
  // visible thickness; suppress the numeric rows so the tooltip
  // does not surface a misleading "1 shares / 0% of float" — show
  // the operator-facing copy explaining the gap instead.
  if (datum.is_gap) {
    return (
      <div className="rounded border border-slate-300 bg-white px-3 py-2 text-xs shadow-md dark:border-slate-700 dark:bg-slate-900">
        <div className="font-medium text-slate-900 dark:text-slate-100">{datum.name}</div>
        <div className="text-slate-600 dark:text-slate-400">
          Data not available — gated on the #740 CUSIP backfill.
        </div>
      </div>
    );
  }
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
