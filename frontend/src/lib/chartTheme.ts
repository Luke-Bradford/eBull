/**
 * Chart theme — single source of truth for chart colors.
 *
 * Two palettes (`lightTheme`, `darkTheme`) keyed identically. Consumers
 * read the resolved palette via `useChartTheme()` so charts react to
 * the operator's theme preference without remounting.
 *
 * Consumers in this module:
 *   - lightweight-charts options in `PriceChart`, `ChartWorkspaceCanvas`,
 *     and `InsiderPriceMarkers`
 *   - recharts components in fundamentals / dividends / insider drill pages
 *   - `Sparkline` (default stroke remains `currentColor` so callers retain
 *     Tailwind `text-*`-driven coloring)
 *
 * Adding a new color: prefer reusing an existing slot. Add a new key
 * only if no existing slot fits the semantic role. Add the key to BOTH
 * palettes. Do NOT inline hex values in chart components.
 */
export interface ChartTheme {
  /** Background of the chart surface. Matches the page card chrome. */
  readonly bg: string;

  /**
   * Text scale.
   * - `textPrimary` = hover values, important figures
   * - `textSecondary` = axis labels, lightweight-charts `textColor`
   * - `textMuted` = hover dates, ticks
   */
  readonly textPrimary: string;
  readonly textSecondary: string;
  readonly textMuted: string;

  /** Grid lines and scale-axis borders. */
  readonly gridLine: string;
  readonly borderColor: string;

  /** Crosshair vertical + horizontal guides. */
  readonly crosshair: string;

  /** Market direction. Used by candles, deltas, and channel high/low. */
  readonly up: string;
  readonly down: string;

  /** Volume histogram fill — translucent so it sits visibly under candles. */
  readonly volumeUpAlpha: string;
  readonly volumeDownAlpha: string;

  /**
   * Accent rotation for series overlays in recharts components and any
   * future multi-line viz. Cycle by index when rendering N series.
   * Pinned to a 6-tuple so positional reads stay non-nullable under
   * strict mode.
   */
  readonly accent: readonly [string, string, string, string, string, string];

  /**
   * Indicator overlays (chart workspace SMA / EMA). Named slots keep the
   * operator's mental map of "which color = which line" stable across
   * sessions.
   */
  readonly indicator: {
    readonly sma20: string;
    readonly sma50: string;
    readonly ema20: string;
    readonly ema50: string;
  };

  /** Compare-overlay rotation in the chart workspace. Distinct from SMA palette.
   *  Pinned to a 3-tuple for positional non-null reads. */
  readonly compare: readonly [string, string, string];

  /** Trend overlays (chart workspace). */
  readonly regression: string;
  readonly channelHigh: string;
  readonly channelLow: string;

  /** Primary normalized line in compare mode. */
  readonly primaryLine: string;
}

export const lightTheme: ChartTheme = {
  bg: "#ffffff",

  textPrimary: "#1e293b", // slate-800
  textSecondary: "#64748b", // slate-500
  textMuted: "#94a3b8", // slate-400

  gridLine: "#f1f5f9", // slate-100
  borderColor: "#e2e8f0", // slate-200

  crosshair: "#94a3b8", // slate-400

  up: "#10b981", // emerald-500
  down: "#ef4444", // red-500

  volumeUpAlpha: "rgba(16,185,129,0.4)",
  volumeDownAlpha: "rgba(239,68,68,0.4)",

  accent: [
    "#06b6d4", // cyan-500
    "#3b82f6", // blue-500
    "#a855f7", // purple-500
    "#f59e0b", // amber-500
    "#ec4899", // pink-500
    "#84cc16", // lime-500
  ],

  indicator: {
    sma20: "#3b82f6", // blue-500 (matches accent[1])
    sma50: "#a855f7", // purple-500 (matches accent[2])
    ema20: "#0ea5e9", // sky-500
    ema50: "#ec4899", // pink-500 (matches accent[4])
  },

  compare: [
    "#0ea5e9", // sky-500
    "#a855f7", // purple-500
    "#f59e0b", // amber-500
  ],

  regression: "#f97316", // orange-500
  channelHigh: "#10b981",
  channelLow: "#ef4444",

  primaryLine: "#1e293b", // slate-800
};

/**
 * Dark palette. Keys mirror `lightTheme`; only surface / chrome tones
 * are overridden. Saturated accents (up/down/accent rotation/indicator
 * slots/regression/channel) reuse the light-mode references — they
 * remain legible on dark surfaces and preserve the operator's color
 * memory ("blue line = SMA20" across sessions).
 */
export const darkTheme: ChartTheme = {
  // slate-950 matches the dark body bg so the chart canvas blends
  // with the page instead of rendering as a visible card outline.
  // Chart elevation is conveyed by the Pane title rule above the
  // chart, not by a panel surface tint.
  bg: "#020617",

  textPrimary: "#f1f5f9", // slate-100
  textSecondary: "#94a3b8", // slate-400
  textMuted: "#64748b", // slate-500

  gridLine: "#1e293b", // slate-800
  borderColor: "#334155", // slate-700

  crosshair: "#64748b", // slate-500

  up: lightTheme.up,
  down: lightTheme.down,

  volumeUpAlpha: lightTheme.volumeUpAlpha,
  volumeDownAlpha: lightTheme.volumeDownAlpha,

  accent: lightTheme.accent,
  indicator: lightTheme.indicator,
  compare: lightTheme.compare,

  regression: lightTheme.regression,
  channelHigh: lightTheme.channelHigh,
  channelLow: lightTheme.channelLow,

  primaryLine: "#f1f5f9", // slate-100 — keeps reading-order weight in dark
};
