/**
 * ChartTooltip — shared dark-safe tooltip CONTAINER for recharts
 * custom tooltip content (#1592 child 2, spec §6.1).
 *
 * The recharts default tooltip is a white card with black text and
 * breaks on dark `slate-950` surfaces; every existing consumer
 * hand-rolls a replacement. This container carries the surface
 * (bg/border/shadow) and base text tones; callers compose their own
 * rows inside.
 *
 * Adoption is page-local to the period statement for now — migrating
 * the other recharts consumers is a follow-up ticket filed at child-2
 * merge (spec §8 out-of-scope list).
 */
import type { ReactNode } from "react";

export function ChartTooltip({ children }: { children: ReactNode }) {
  return (
    <div className="rounded border border-slate-200 bg-white px-2 py-1 text-xs shadow-md dark:border-slate-700 dark:bg-slate-900">
      {children}
    </div>
  );
}
