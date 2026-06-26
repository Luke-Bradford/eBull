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
 *                     Insiders / Treasury) plus a hatched wedge for
 *                     the ``Public / unattributed`` residual (#920).
 *   ring 3 (outer)  — per-filer / per-officer wedges within each
 *                     category, plus a transparent wedge for any
 *                     within-category gap (filer detail incomplete)
 *                     and a hatched wedge for the same outer
 *                     residual so ring 3 reaches the same
 *                     circumference as ring 2.
 *
 * Residual vs within-category gap (#920): the residual is hatched
 * (subtle diagonal lines) and hover shows a tooltip, so the operator
 * SEES the coverage gap instead of reading it as a chart bug. It is
 * never clickable. Within-category gaps stay fully transparent and
 * inert — they have no identity to describe.
 *
 * Recharts has no native sunburst primitive — built from three nested
 * ``<Pie>`` components at increasing ``innerRadius`` / ``outerRadius``.
 */

import { type KeyboardEvent, useId, useMemo } from "react";
import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from "recharts";

import { ChartTooltip } from "@/components/charts/ChartTooltip";
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
      /** SEC archive index URL for the holder's winning accession
       *  (#921). ``null`` = no click-through target (aggregated
       *  "Other" tail, treasury, feeds without URLs) — consumers
       *  fall back to the in-app drill. */
      readonly source_url: string | null;
    };

/** Fail-closed host guard for wedge click-through (#921). The backend
 *  builds these URLs from accession numbers, but a corrupted payload
 *  should degrade to the in-app drill, not open an arbitrary URL. */
const _EDGAR_URL_PREFIX = "https://www.sec.gov/";

/**
 * Open the SEC source filing for a leaf wedge in a new tab (#921).
 * Returns ``true`` only when the tab actually opened — callers fall
 * back to their in-app navigate/drill on ``false`` (no URL, non-SEC
 * URL, category/center wedge, or popup-blocked open), so a wedge
 * click is never swallowed.
 *
 * Opener isolation is done by nulling ``opener`` on the returned
 * handle, NOT via the ``noopener`` feature string: per spec,
 * ``noopener`` makes ``window.open`` return ``null`` even on
 * SUCCESS, which would make every successful open also fire the
 * caller's in-app fallback (double action — Codex ckpt-2 High).
 */
export function openWedgeSource(target: WedgeClick): boolean {
  if (target.kind !== "leaf" || target.source_url === null) return false;
  if (!target.source_url.startsWith(_EDGAR_URL_PREFIX)) return false;
  const opened = window.open(target.source_url, "_blank");
  if (opened === null) return false; // popup blocked → in-app fallback
  opened.opener = null;
  return true;
}

/** Index into ``ChartTheme.accent`` for each category's color. */
export const CATEGORY_FILL_INDEX: Record<CategoryKey, number> = {
  institutions: 0, // cyan-500
  etfs: 1, // blue-500
  insiders: 2, // purple-500
  def14a: 5, // lime-500 — DEF 14A proxy-only holders (#1627); the last
  //                       free accent so the un-folded wedge reads as
  //                       its own category, distinct from insiders.
  treasury: 3, // amber-500
  blockholders: 4, // rose-500 — distinct from the other four; activist
  //                            holds usually warrant a high-contrast
  //                            wedge so an Icahn / Ackman position
  //                            does not blend into the institutions
  //                            cyan band on small-caps.
};

export function categoryFill(theme: ChartTheme, key: CategoryKey): string {
  const idx = CATEGORY_FILL_INDEX[key];
  return theme.accent[idx % theme.accent.length] ?? theme.accent[0]!;
}

export interface ChartDatum {
  readonly id: string;
  readonly name: string;
  readonly shares: number;
  readonly pct: number;
  readonly fill: string;
  /** True for gap wedges (residual + within-category) — they are
   *  never clickable. */
  readonly is_gap: boolean;
  /** True only for the ``Public / unattributed`` residual wedges
   *  (#920) — hatched fill + hover tooltip. Within-category gaps
   *  stay transparent and inert. */
  readonly is_residual: boolean;
  readonly target: WedgeClick | null;
}

/** Display label for the residual wedge — matches the backend
 *  rollup's ``residual.label`` so chart, legend, and payload agree. */
export const RESIDUAL_LABEL = "Public / unattributed";

/**
 * Tooltip copy for the residual wedge (#920). ``pct`` is the fraction
 * already carried on the chart datum (``shares / effective
 * denominator``). Says "of outstanding" (the chart's denominator is
 * shares_outstanding) and "not attributed to" (the residual can
 * include filers outside our seeded coverage cohort — "not held by
 * any disclosed filer" would overclaim). Spec D4 / Codex ckpt-1.
 */
export function residualTooltipText(shares: number, pct: number): string {
  return `${RESIDUAL_LABEL}: ${formatPct(pct)} of outstanding — ${formatShares(shares)} shares not attributed to any disclosed filer.`;
}

export interface SunburstChartData {
  readonly middleData: ChartDatum[];
  readonly outerData: ChartDatum[];
}

/**
 * Pure chart-datum construction — extracted from the component so the
 * gap/residual flag placement is unit-testable (jsdom renders a
 * zero-size ResponsiveContainer, so sector-level DOM assertions are
 * not viable).
 */
export function buildSunburstChartData(
  rings: SunburstRings,
  theme: ChartTheme,
): SunburstChartData {
  const denom = rings.total_shares;
  const fillFor = (key: CategoryKey): string => categoryFill(theme, key);

  // Middle ring — per-category wedges + hatched residual.
  const middleData: ChartDatum[] = rings.categories.map((cat) =>
    toCategoryDatum(cat, fillFor(cat.key), denom),
  );
  if (rings.category_residual > 0) {
    middleData.push(
      makeGapDatum("middle-residual", RESIDUAL_LABEL, rings.category_residual, denom, true),
    );
  }

  // Outer ring — leaves under each visible category, then the
  // category's within_category_gap (transparent), then the same
  // residual so ring 3 closes flush with ring 2.
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
          false,
        ),
      );
    }
  }
  if (rings.category_residual > 0) {
    outerData.push(
      makeGapDatum("outer-residual", RESIDUAL_LABEL, rings.category_residual, denom, true),
    );
  }

  return { middleData, outerData };
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
  // to no-op them after the fact). Residual wedges (#920) are the
  // exception: they keep hit-testing ON so the hover tooltip fires,
  // but get no cursor / hover affordance — informational, never
  // clickable.
  ".ownership-sunburst .recharts-pie-sector:has(path[data-known='false'][data-residual='false']) {",
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
  // ``useId`` emits colon-delimited ids (":r1:") that break ``url(#…)``
  // fragment references in some engines — strip them.
  const patternId = `residual-hatch-${useId().replace(/:/g, "")}`;

  if (rings === null) return null;

  const denom = rings.total_shares;
  const { middleData, outerData } = buildSunburstChartData(rings, theme);
  // Known coverage derives from the SAME rings the wedges render
  // from, so label and chart cannot diverge. Under the
  // oversubscription bump this honestly reads 100% against the
  // bumped denominator — the panel-level OversubscribedWarning
  // carries the diagnostic (spec D5).
  const coveragePct = (denom - rings.category_residual) / denom;

  const totalRadius = size / 2;
  const innerInner = totalRadius * 0.25;
  const innerOuter = totalRadius * 0.36;
  const middleOuter = totalRadius * 0.62;
  const outerOuter = totalRadius * 0.92;

  const handleClick = (datum: ChartDatum | undefined): void => {
    if (datum === undefined || datum.target === null) return;
    onWedgeClick?.(datum.target);
  };

  // Keyboard activation (#921). Recharts' own keyboard layer makes the
  // pie root Tab-reachable and moves focus across sectors on
  // ArrowLeft/Right, but has NO Enter handling (verified in
  // node_modules Pie.js). Keydown bubbles from the focused sector
  // <g> to this wrapper; ``e.target`` is the focused sector.
  const handleKeyDown = (e: KeyboardEvent<HTMLDivElement>): void => {
    if (e.key !== "Enter") return;
    const datum = focusedSectorDatum(
      e.target instanceof Element ? e.target : null,
      middleData,
      outerData,
    );
    if (datum === null) return;
    e.preventDefault();
    handleClick(datum);
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
      // ``group``, not ``img`` (#921): an atomic image role would hide
      // the keyboard-interactive sectors from assistive tech.
      role="group"
      aria-label={`Ownership breakdown: ${formatShares(denom)} total shares.`}
      onKeyDown={handleKeyDown}
    >
      <style>{SUNBURST_STYLES}</style>
      {/* Hatch paint-server for the residual wedges (#920). Recharts
          filters unknown children of <PieChart>, so the <defs> lives
          in a zero-size sibling svg — same-document url(#…) references
          resolve across SVG elements. slate-400 stroke reads on both
          light and dark backgrounds. */}
      <svg aria-hidden focusable="false" width={0} height={0} className="absolute">
        <defs>
          <pattern
            id={patternId}
            width={6}
            height={6}
            patternUnits="userSpaceOnUse"
            patternTransform="rotate(45)"
          >
            <line x1={0} y1={0} x2={0} y2={6} stroke="#94a3b8" strokeOpacity={0.45} strokeWidth={1.2} />
          </pattern>
        </defs>
      </svg>
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
            // Decorative band — its click ("show L2 view") duplicates
            // page-level navigation, so it stays out of the Tab order
            // (#921, Codex ckpt-1). Keyboard stops = middle + outer.
            rootTabIndex={-1}
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
            {middleData.map((d, i) => (
              <Cell
                key={d.id}
                fill={d.is_residual ? `url(#${patternId})` : d.fill}
                stroke={d.is_gap ? "transparent" : wedgeStroke}
                data-known={d.is_gap ? "false" : "true"}
                data-residual={d.is_residual ? "true" : "false"}
                data-ring="middle"
                data-idx={i}
                aria-label={cellAriaLabel(d)}
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
            {outerData.map((d, i) => (
              <Cell
                key={d.id}
                fill={d.is_residual ? `url(#${patternId})` : d.fill}
                stroke={d.is_gap ? "transparent" : wedgeStroke}
                data-known={d.is_gap ? "false" : "true"}
                data-residual={d.is_residual ? "true" : "false"}
                data-ring="outer"
                data-idx={i}
                aria-label={cellAriaLabel(d)}
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
        <span className="text-xs text-slate-500 dark:text-slate-400">
          {formatPct(coveragePct)} known coverage
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
    is_residual: false,
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
    is_residual: false,
    target: {
      kind: "leaf",
      category_key,
      leaf_key: leaf.key,
      source_url: leaf.source_url,
    },
  };
}

/**
 * Resolve a keyboard event's target sector to its chart datum (#921).
 * Reads the ``data-ring`` / ``data-idx`` attributes stamped on each
 * Cell path — deliberately NOT positional DOM-order mapping, so a
 * Recharts render-order change cannot silently remap Enter presses to
 * the wrong wedge (Codex ckpt-1 High). Returns ``null`` for the inner
 * decorative ring, labels, or anything that is not a sector.
 */
export function focusedSectorDatum(
  eventTarget: Element | null,
  middleData: readonly ChartDatum[],
  outerData: readonly ChartDatum[],
): ChartDatum | null {
  if (eventTarget === null) return null;
  const sector = eventTarget.closest(".recharts-pie-sector");
  if (sector === null) return null;
  const path = sector.querySelector("path[data-ring]");
  if (path === null) return null;
  const ring = path.getAttribute("data-ring");
  const idx = Number.parseInt(path.getAttribute("data-idx") ?? "", 10);
  if (!Number.isInteger(idx) || idx < 0) return null;
  if (ring === "middle") return middleData[idx] ?? null;
  if (ring === "outer") return outerData[idx] ?? null;
  return null;
}

/** Accessible name for an arrow-focused sector (#921). Recharts
 *  spreads unknown Cell props onto the sector ``<path>`` (same
 *  passthrough as the ``data-*`` attributes). */
function cellAriaLabel(d: ChartDatum): string {
  return `${d.name}: ${formatShares(d.shares)} shares, ${formatPct(d.pct)} of outstanding`;
}

function makeGapDatum(
  id: string,
  name: string,
  shares: number,
  denom: number,
  is_residual: boolean,
): ChartDatum {
  return {
    id,
    name,
    shares,
    pct: denom > 0 ? shares / denom : 0,
    // Residual cells override fill with the hatch paint-server at
    // render time (the pattern id is per-mount, so it cannot live in
    // this pure datum).
    fill: "transparent",
    is_gap: true,
    is_residual,
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
  if (datum === undefined) return null;
  // Within-category gaps stay suppressed; the residual gets its own
  // single-line copy (#920).
  if (datum.is_gap && !datum.is_residual) return null;
  if (datum.is_residual) {
    return (
      <ChartTooltip>
        <div className="max-w-[18rem] text-slate-700 dark:text-slate-300">
          {residualTooltipText(datum.shares, datum.pct)}
        </div>
      </ChartTooltip>
    );
  }
  return (
    <ChartTooltip>
      <div className="font-medium text-slate-900 dark:text-slate-100">{datum.name}</div>
      <div className="text-slate-600 dark:text-slate-400">
        {formatShares(datum.shares)} shares
      </div>
      <div className="text-slate-600 dark:text-slate-400">
        {formatPct(datum.pct)} of total shares
      </div>
    </ChartTooltip>
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
 * actually rendered, plus a "Public / unattributed" row for the
 * residual (hatched swatch, mirroring the wedge — #920). Each
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
      key: "residual",
      label: RESIDUAL_LABEL,
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
            // The residual swatch mirrors the wedge hatch. An HTML
            // span cannot reference the SVG paint-server, so the
            // hatch is a CSS gradient (spec D7). rgba(148,163,184,…)
            // = slate-400, same token as the wedge pattern stroke.
            style={{
              background: row.swatch_outline
                ? "repeating-linear-gradient(45deg, transparent, transparent 3px, rgba(148, 163, 184, 0.6) 3px, rgba(148, 163, 184, 0.6) 4px)"
                : row.swatch_fill,
            }}
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
