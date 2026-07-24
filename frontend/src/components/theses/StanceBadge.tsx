/**
 * StanceBadge — pill for the thesis stance enum (#1902).
 *
 * Vocabulary is the settled thesis-semantics set (docs/settled-decisions.md):
 * buy | hold | watch | avoid. This file owns only the domain mapping
 * stance → semantic tone; the colour classes live once in `Badge` (#1908).
 * watch → warn (attention, not action). Unknown strings fall back to the
 * neutral tone rather than hiding.
 */
import { Badge, type BadgeTone } from "@/components/ui/Badge";

const TONE: Record<string, BadgeTone> = {
  buy: "ok",
  hold: "neutral",
  watch: "warn",
  avoid: "risk",
};

export function StanceBadge({
  stance,
}: {
  readonly stance: string;
}): JSX.Element {
  return (
    <Badge tone={TONE[stance.toLowerCase()] ?? "neutral"} uppercase>
      {stance}
    </Badge>
  );
}
