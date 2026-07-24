/**
 * Small marker for a money row whose values could NOT be converted to the
 * account display currency (the backend left them in the position's native
 * currency because the FX rate was missing — #2129). Shows the native currency
 * code so the magnitude — already rendered with the native symbol — is
 * unambiguous, and explains why on hover.
 */
export function UnconvertedBadge({
  currency,
  displayCurrency,
}: {
  currency: string;
  displayCurrency: string;
}) {
  return (
    <span
      title={`Not converted to ${displayCurrency} — shown in ${currency} (FX rate unavailable)`}
      className="ml-1.5 inline-flex items-center rounded bg-amber-100 px-1 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-amber-700 align-middle dark:bg-amber-500/15 dark:text-amber-300"
    >
      {currency}
    </span>
  );
}
