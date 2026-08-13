/**
 * Theme-aware chart palette accessor.
 *
 * Resolves the operator's current theme preference (light / dark /
 * system) to the matching `ChartTheme` object. Charts call this at the
 * top of their component body and either:
 *
 *   - pass the resolved theme through `useEffect` deps so a
 *     lightweight-charts instance re-applies options on theme change
 *     (without recreating the chart and dropping operator pan/zoom);
 *   - reference the resolved theme directly in JSX, since recharts
 *     re-renders on prop change.
 *
 * Falls back to `lightTheme` when no `ThemeProvider` is in the tree.
 * The App root always wraps in `ThemeProvider`, so the fallback only
 * fires in unit tests that mount chart components in isolation —
 * keeping every existing chart test from needing a Provider wrapper.
 */
import { useContext, useMemo } from "react";

import { darkTheme, lightTheme, type ChartTheme } from "@/lib/chartTheme";
import { ThemeContext } from "@/lib/theme";

export function useChartTheme(): ChartTheme {
  const ctx = useContext(ThemeContext);
  const resolved = ctx?.resolved ?? "light";
  return useMemo<ChartTheme>(
    () => (resolved === "dark" ? darkTheme : lightTheme),
    [resolved],
  );
}
