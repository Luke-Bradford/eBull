/**
 * Chart theme — single source of truth for chart colors.
 *
 * Light-mode only in v1. Dark variant + theme-mode switch (light / dark /
 * system, persisted via Settings) are tracked at #596. When that ships,
 * this module exports `lightTheme` + `darkTheme` and consumers stop
 * importing the constants directly — they read via `useChartTheme()`.
 *
 * Consumers in this PR (#586):
 *   - lightweight-charts options in `PriceChart` and `ChartWorkspaceCanvas`
 *   - `Sparkline` (referenced from JSDoc; default stroke remains
 *     `currentColor` so callers retain Tailwind `text-*`-driven coloring)
 *
 * Consumers in upcoming chart-redesign tickets (#587-#594):
 *   - `recharts` components in per-domain L2 drill pages
 *
 * Adding a new color: prefer reusing an existing slot. Add a new key only
 * if no existing slot fits the semantic role. Do NOT inline hex values in
 * chart components.
 */

export const chartTheme = {
  /** Background of the chart surface. Matches the page card chrome. */
  bg: "#ffffff",

  /**
   * Text scale.
   * - `textPrimary` = hover values, important figures (slate-800)
   * - `textSecondary` = axis labels, lightweight-charts `textColor` (slate-500)
   * - `textMuted` = hover dates, ticks (slate-400)
   */
  textPrimary: "#1e293b",
  textSecondary: "#64748b",
  textMuted: "#94a3b8",

  /** Grid lines (slate-100) and scale-axis borders (slate-200). */
  gridLine: "#f1f5f9",
  borderColor: "#e2e8f0",

  /** Crosshair vertical + horizontal guides. */
  crosshair: "#94a3b8",

  /** Market direction. Used by candles, deltas, and channel high/low. */
  up: "#10b981",
  down: "#ef4444",

  /** Volume histogram fill — translucent so it sits visibly under candles. */
  volumeUpAlpha: "rgba(16,185,129,0.4)",
  volumeDownAlpha: "rgba(239,68,68,0.4)",

  /**
   * Accent rotation for series overlays in recharts components and any
   * future multi-line viz. Cycle by index when rendering N series.
   */
  accent: [
    "#06b6d4", // cyan-500
    "#3b82f6", // blue-500
    "#a855f7", // purple-500
    "#f59e0b", // amber-500
    "#ec4899", // pink-500
    "#84cc16", // lime-500
  ] as const,

  /**
   * Indicator overlays (chart workspace SMA / EMA). Named slots keep the
   * operator's mental map of "which color = which line" stable across
   * sessions. Picks from `accent` plus sky-500 for the EMA(20) slot.
   */
  indicator: {
    sma20: "#3b82f6", // blue-500 (matches accent[1])
    sma50: "#a855f7", // purple-500 (matches accent[2])
    ema20: "#0ea5e9", // sky-500
    ema50: "#ec4899", // pink-500 (matches accent[4])
  },

  /** Compare-overlay rotation in the chart workspace. Distinct from SMA palette. */
  compare: [
    "#0ea5e9", // sky-500
    "#a855f7", // purple-500
    "#f59e0b", // amber-500
  ] as const,

  /** Trend overlays (chart workspace). */
  regression: "#f97316", // orange-500
  channelHigh: "#10b981",
  channelLow: "#ef4444",

  /** Primary normalized line in compare mode (slate-800, picks reading order). */
  primaryLine: "#1e293b",
} as const;

export type ChartTheme = typeof chartTheme;
