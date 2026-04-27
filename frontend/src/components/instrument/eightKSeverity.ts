/**
 * Shared severity → Tailwind tone map for 8-K item / filing chips.
 * Defined once so adding a new severity level (or rebalancing the
 * palette) doesn't drift across `EightKDetailPanel` + `EightKListPage`.
 */
export const SEVERITY_TONE: Record<string, string> = {
  high: "bg-red-100 text-red-700",
  medium: "bg-amber-100 text-amber-700",
  low: "bg-slate-100 text-slate-600",
};
