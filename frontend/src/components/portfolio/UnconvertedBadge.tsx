/**
 * Small marker for a money row whose values could NOT be converted to the
 * account display currency (the backend left them in the position's native
 * currency because the FX rate was missing — #2129). Shows the native currency
 * code so the magnitude — already rendered with the native symbol — is
 * unambiguous, and explains why on hover.
 *
 * Tone is `warn`: a degraded-but-honest figure, not an error (#1908 PR-2 —
 * colour classes live once in `Badge`).
 */
import { Badge } from "@/components/ui/Badge";

export function UnconvertedBadge({
  currency,
  displayCurrency,
}: {
  currency: string;
  displayCurrency: string;
}) {
  return (
    <Badge
      tone="warn"
      uppercase
      className="ml-1.5 align-middle"
      title={`Not converted to ${displayCurrency} — shown in ${currency} (FX rate unavailable)`}
    >
      {currency}
    </Badge>
  );
}
