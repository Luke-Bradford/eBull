import { formatMoney, formatPct } from "@/lib/format";

/**
 * The three string→number conversions the strategy surfaces share (#2868).
 *
 * Extracted when `/strategies` split into two lenses: both needed them, and
 * each file having its own private copy is how a formatter drifts. The pot is
 * USD-denominated by contract (`StrategyPaperPool.currency`), which is why
 * `money` can hard-code it.
 */
export function number(value: string | null): number | null {
  if (value === null) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function money(value: string | null): string {
  return formatMoney(number(value), "USD");
}

/** Backend sends percentage POINTS (`1.23`); `formatPct` wants a fraction. */
export function pctPoints(value: string | null): string {
  const parsed = number(value);
  return formatPct(parsed === null ? null : parsed / 100);
}
