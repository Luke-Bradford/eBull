/**
 * Three-ring ownership sunburst (#729).
 *
 * Faithful proportional rendering. Single denominator
 * (``shares_outstanding``); every wedge is sized as its true share of
 * the total. Categories or filers we don't have data for render as
 * transparent arcs — literal empty space — so the operator sees the
 * coverage gap in proper proportion rather than a synthetic
 * placeholder.
 *
 *   ring 1 (inner)  — single solid band, decorative; denominator
 *                     label sits in the center hole.
 *   ring 2 (middle) — per-category wedges (Institutions / ETFs /
 *                     Insiders / Treasury) plus a transparent wedge
 *                     for the unaccounted residual.
 *   ring 3 (outer)  — per-filer / per-officer wedges within each
 *                     category, plus a transparent wedge for any
 *                     within-category gap (filer detail incomplete)
 *                     and a transparent wedge for the same outer
 *                     residual so ring 3 reaches the same
 *                     circumference as ring 2.
 *
 * Recharts has no native sunburst primitive — built from three nested
 * ``<Pie>`` components at increasing ``innerRadius`` / ``outerRadius``.
 */

import { useMemo } from "react";
import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from "recharts";

import { formatPct, formatShares } from "@/components/instrument/ownershipMetrics";
import {
  type CategoryKey,
  type SunburstCategory,
  type SunburstInputs,
  type SunburstLeaf,
  type SunburstRings,
  buildSunburstRings,
} from "@/components/instrument/ownershipRings";
import { type ChartTheme } from "@/lib/chartTheme";
import { useChartTheme } from "@/lib/useChartTheme";

export interface OwnershipSunburstProps {
  readonly inputs: SunburstInputs;
  /** Click handler for any colored (known-data) wedge. Transparent
   *  gap wedges are non-interactive — they have no identity to
   *  drill into. */
  readonly onWedgeClick?: (target: WedgeClick) => void;
  /** Pixel size of the chart canvas (square). Default 280. */
  readonly size?: number;
}

export type WedgeClick =
  | { readonly kind: "center" }
  | { readonly kind: "category"; readonly category_key: CategoryKey }
  | {
      readonly kind: "leaf";
      readonly category_key: CategoryKey;
      readonly leaf_key: string;
    };

/** Index into ``ChartTheme.accent`` for each category's color. */
export const CATEGORY_FILL_INDEX: Record<CategoryKey, number> = {
  institutions: 0, // cyan-500
  etfs: 1, // blue-500
  insiders: 2, // purple-500
  treasury: 3, // amber-500
};

export function categoryFill(theme: ChartTheme, key: CategoryKey): string {
  const idx = CATEGORY_FILL_INDEX[key];
  return theme.accent[idx % theme.accent.length] ?? theme.accent[0]!;
}

interface ChartDatum {
  readonly id: string;
  readonly name: string;
  readonly shares: number;
  readonly pct: number;
  readonly fill: string;
  /** True for transparent wedges — they are non-interactive and
   *  the tooltip / hover affordances suppress on them. */
  readonly is_gap: boolean;
  readonly target: WedgeClick | null;
}

/**
 * Module-level CSS for wedge interactions. Defined out-of-line because
 * TypeScript's strict JSX child typing rejects template-literal
 * children of bare ``<style>`` (sees ``string`` as not-callable). The
 * ``[data-known='true']`` attribute, set per-Cell, scopes hover and
 * focus affordances to colored wedges only — gap arcs stay inert.
 */
const SUNBURST_STYLES = [
  ".ownership-sunburst .recharts-pie-sector path {",
  "  transition: stroke-width 120ms ease, filter 120ms ease;",
  "}",
  ".ownership-sunburst .recharts-pie-sector path:focus {",
  "  outline: none;",
  "}",
  // Sector wrapper hit-testing — Recharts' click + tooltip handlers
  // attach on the parent ``.recharts-pie-sector`` Layer, not on the
  // path Cell ``style`` lands on. Use ``:has()`` to disable hit
  // tests on the wrapper for any sector whose Cell carries
  // ``data-known='false'`` so transparent gap wedges don't absorb
  // clicks/hover at all (instead of relying on the JS click filter
  // to no-op them after the fact).
  ".ownership-sunburst .recharts-pie-sector:has(path[data-known='false']) {",
  "  pointer-events: none;",
  "}",
  ".ownership-sunburst .recharts-pie-sector:has(path[data-known='true']) path {",
  "  cursor: pointer;",
  "}",
  // slate-400 outline reads on both light (white) and dark
  // (slate-950) backgrounds. Default ``currentColor`` defaults to
  // black — invisible on dark mode.
  ".ownership-sunburst .recharts-pie-sector:has(path[data-known='true']) path:focus-visible {",
  "  outline: 2px solid #94a3b8;",
  "  outline-offset: -1px;",
  "}",
  // Hover affordance via ``filter: brightness`` — never via CSS
  // ``opacity`` so the gap-vs-known semantic survives hover.
  ".ownership-sunburst .recharts-pie-sector:has(path[data-known='true']):hover path {",
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

  const denom = rings.total_shares;
  const fillFor = (key: CategoryKey): string => categoryFill(theme, key);

  // Middle ring — per-category wedges + transparent residual.
  const middleData: ChartDatum[] = rings.categories.map((cat) =>
    toCategoryDatum(cat, fillFor(cat.key), denom),
  );
  if (rings.category_residual > 0) {
    middleData.push(makeGapDatum("middle-residual", "Unaccounted", rings.category_residual, denom));
  }

  // Outer ring — leaves under each visible category, then the
  // category's within_category_gap (transparent), then the same
  // outer residual so ring 3 closes flush with ring 2.
  const outerData: ChartDatum[] = [];
  for (const cat of rings.categories) {
    const baseFill = fillFor(cat.key);
    for (const leaf of cat.leaves) {
      outerData.push(toLeafDatum(leaf, cat.key, baseFill, denom));
    }
    if (cat.within_category_gap > 0) {
      outerData.push(
        makeGapDatum(
          `${cat.key}-gap`,
          `${cat.label} — unresolved filers`,
          cat.within_category_gap,
          denom,
        ),
      );
    }
  }
  if (rings.category_residual > 0) {
    outerData.push(makeGapDatum("outer-residual", "Unaccounted", rings.category_residual, denom));
  }

  const totalRadius = size / 2;
  const innerInner = totalRadius * 0.25;
  const innerOuter = totalRadius * 0.36;
  const middleOuter = totalRadius * 0.62;
  const outerOuter = totalRadius * 0.92;

  const handleClick = (datum: ChartDatum | undefined): void => {
    if (datum === undefined || datum.target === null) return;
    onWedgeClick?.(datum.target);
  };

  const wedgeStroke = theme.gridLine;

  // ``middleData`` and ``outerData`` are always non-empty when
  // ``denom > 0``: buildSunburstRings guarantees ``category_residual
  // = total_shares - sum_known``, so when no category renders the
  // residual gap is the entire denominator and gets pushed below.
  // (Codex review pin — no defensive fallback needed.)

  return (
    <div
      className="ownership-sunburst relative"
      style={{ width: size, height: size }}
      role="img"
      aria-label={`Ownership breakdown: ${formatShares(denom)} total shares.`}
    >
      <style>{SUNBURST_STYLES}</style>
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Tooltip content={<SunburstTooltip />} />
          {/* Inner ring — single solid arc. Click navigates without a
              filter (operator-side: "show me the L2 view"). */}
          <Pie
            data={[{ name: "Total", shares: 1 }]}
            dataKey="shares"
            innerRadius={innerInner}
            outerRadius={innerOuter}
            stroke={wedgeStroke}
            isAnimationActive={false}
            onClick={() => onWedgeClick?.({ kind: "center" })}
          >
            <Cell fill={theme.borderColor} fillOpacity={0.6} stroke={wedgeStroke} />
          </Pie>
          <Pie
            data={middleData}
            dataKey="shares"
            innerRadius={innerOuter}
            outerRadius={middleOuter}
            stroke={wedgeStroke}
            isAnimationActive={false}
            onClick={(_, idx) => handleClick(middleData[idx])}
          >
            {middleData.map((d) => (
              <Cell
                key={d.id}
                fill={d.fill}
                stroke={d.is_gap ? "transparent" : wedgeStroke}
                data-known={d.is_gap ? "false" : "true"}
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
            onClick={(_, idx) => handleClick(outerData[idx])}
          >
            {outerData.map((d) => (
              <Cell
                key={d.id}
                fill={d.fill}
                stroke={d.is_gap ? "transparent" : wedgeStroke}
                data-known={d.is_gap ? "false" : "true"}
              />
            ))}
          </Pie>
        </PieChart>
      </ResponsiveContainer>
      <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
          Total shares
        </span>
        <span className="text-lg font-semibold text-slate-900 dark:text-slate-100">
          {formatShares(denom)}
        </span>
      </div>
    </div>
  );
}

function toCategoryDatum(
  cat: SunburstCategory,
  baseFill: string,
  denom: number,
): ChartDatum {
  return {
    id: `cat-${cat.key}`,
    name: cat.label,
    shares: cat.shares,
    pct: denom > 0 ? cat.shares / denom : 0,
    fill: baseFill,
    is_gap: false,
    target: { kind: "category", category_key: cat.key },
  };
}

function toLeafDatum(
  leaf: SunburstLeaf,
  category_key: CategoryKey,
  baseFill: string,
  denom: number,
): ChartDatum {
  return {
    id: `leaf-${category_key}-${leaf.key}`,
    name: leaf.label,
    shares: leaf.shares,
    pct: denom > 0 ? leaf.shares / denom : 0,
    fill: baseFill,
    is_gap: false,
    target: { kind: "leaf", category_key, leaf_key: leaf.key },
  };
}

function makeGapDatum(id: string, name: string, shares: number, denom: number): ChartDatum {
  return {
    id,
    name,
    shares,
    pct: denom > 0 ? shares / denom : 0,
    fill: "transparent",
    is_gap: true,
    target: null,
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
  if (datum === undefined || datum.is_gap) return null;
  return (
    <div className="rounded border border-slate-300 bg-white px-3 py-2 text-xs shadow-md dark:border-slate-700 dark:bg-slate-900">
      <div className="font-medium text-slate-900 dark:text-slate-100">{datum.name}</div>
      <div className="text-slate-600 dark:text-slate-400">
        {formatShares(datum.shares)} shares
      </div>
      <div className="text-slate-600 dark:text-slate-400">
        {formatPct(datum.pct)} of total shares
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Color legend
// ---------------------------------------------------------------------------

export interface OwnershipLegendProps {
  readonly rings: SunburstRings;
}

/**
 * Color legend for the sunburst. Renders one row per category that
 * actually rendered, plus an "Unaccounted" row for the residual. Each
 * row shows the swatch, label, share count, and % of outstanding so
 * the operator can read the ring at a glance without hovering.
 */
export function OwnershipLegend({ rings }: OwnershipLegendProps): JSX.Element | null {
  const theme = useChartTheme();
  const denom = rings.total_shares;
  if (denom <= 0) return null;

  interface LegendRow {
    readonly key: string;
    readonly label: string;
    readonly shares: number;
    readonly pct: number;
    readonly swatch_fill: string;
    readonly swatch_outline: boolean;
  }

  const rows: LegendRow[] = rings.categories.map((cat) => ({
    key: cat.key,
    label: cat.label,
    shares: cat.shares,
    pct: cat.shares / denom,
    swatch_fill: categoryFill(theme, cat.key),
    swatch_outline: false,
  }));
  if (rings.category_residual > 0) {
    rows.push({
      key: "unaccounted",
      label: "Unaccounted",
      shares: rings.category_residual,
      pct: rings.category_residual / denom,
      swatch_fill: "transparent",
      swatch_outline: true,
    });
  }
  if (rows.length === 0) return null;

  return (
    <ul className="flex flex-wrap gap-x-4 gap-y-1 text-xs">
      {rows.map((row) => (
        <li key={row.key} className="flex items-center gap-1.5">
          <span
            aria-hidden
            className={`inline-block h-3 w-3 rounded-sm ${
              row.swatch_outline
                ? "border border-dashed border-slate-400 dark:border-slate-500"
                : ""
            }`}
            style={{ backgroundColor: row.swatch_fill }}
          />
          <span className="text-slate-700 dark:text-slate-200">{row.label}</span>
          <span className="font-mono text-slate-500 dark:text-slate-400">
            {formatShares(row.shares)}
          </span>
          <span className="font-mono text-slate-400 dark:text-slate-500">
            ({formatPct(row.pct)})
          </span>
        </li>
      ))}
    </ul>
  );
}

export { buildSunburstRings };
export type { SunburstRings };
