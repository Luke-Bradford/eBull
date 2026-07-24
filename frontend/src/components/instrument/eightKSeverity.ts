/**
 * Shared 8-K severity → `Badge` tone. Defined once so adding a new severity
 * level doesn't drift across `EightKDetailPanel` + `EightKListPage`.
 *
 * Previously this module held raw light-only Tailwind tint classes with no
 * dark-mode partners — and `frontend/scripts/check-dark-classes.mjs` walked
 * only `.tsx`, so a `.ts` tone map was structurally invisible to the gate
 * (that walk now covers `.ts` too, same PR). Holding tones as semantic
 * `BadgeTone` values removes the failure mode entirely: the colour classes
 * live once in `Badge` (#1908).
 */
import type { BadgeTone } from "@/components/ui/Badge";

const SEVERITY_TONE: Record<string, BadgeTone> = {
  high: "risk",
  medium: "warn",
  low: "neutral",
};

export function severityTone(severity: string | null): BadgeTone {
  return SEVERITY_TONE[severity ?? ""] ?? "neutral";
}
