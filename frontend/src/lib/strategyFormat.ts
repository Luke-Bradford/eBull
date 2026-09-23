import { formatMoney, formatPct, formatUnsignedPct } from "@/lib/format";

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

/** Unsigned twin of `pctPoints` for limits and ceilings (#3336): a signed
 *  "+10.00%" max drawdown reads as a gain. Returns stay on `pctPoints`. */
export function pctPointsUnsigned(value: string | null): string {
  const parsed = number(value);
  return formatUnsignedPct(parsed === null ? null : parsed / 100);
}

/** Seed value for a money `<input>` (#3336). The API serialises NUMERIC(…, 6),
 *  so `"500.000000"` would otherwise sit in the field; cents are the unit an
 *  operator edits. A non-numeric value passes through untouched. */
export function moneyInput(value: string): string {
  const parsed = number(value);
  return parsed === null ? value : parsed.toFixed(2);
}

/** Seed value for a percent `<input>`: `"80.0000"` → `"80"`, `"2.5000"` → `"2.5"`. */
export function decimalInput(value: string): string {
  const parsed = number(value);
  return parsed === null ? value : String(parsed);
}
