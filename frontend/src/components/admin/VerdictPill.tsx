/**
 * The single computed health verdict, as one pill (#1512 / #2274).
 *
 * Extracted from `ProcessRow`'s private `StatusPill` so the Processes table and
 * the `/admin/processes/{id}` drill-in render THE SAME COMPONENT rather than
 * two hand-rolled Badges over the same map. Before #2274 the drill-in rendered
 * `STATUS_VISUAL[row.status]` instead — a different field — so the two surfaces
 * printed a different health word for every row in the live snapshot, and on a
 * `schedule_missed` row printed them in different alarm colours: the table
 * pinned it red, the page you reached by clicking the red said grey `idle`.
 *
 * ⚠ The extraction is NOT the fix — switching the drill-in to `health_verdict`
 * is. It is here so the tooltip, the `aria-label` and the `data-verdict` hook
 * exist once. `AdminPage.tsx::JobStatusCell` renders `VERDICT_VISUAL` with its
 * own markup as a third site and is deliberately not converted here.
 *
 * ⚠ Sharing a component does NOT make the two surfaces agree at any given
 * instant: the table polls, the drill-in does not, so they can be looking at
 * different payloads. What it guarantees is that GIVEN THE SAME PAYLOAD they
 * make the same claim.
 */
import type { ProcessRowResponse } from "@/api/types";
import { VERDICT_VISUAL } from "@/components/admin/processStatus";
import { Badge } from "@/components/ui/Badge";

/**
 * ⚠ Pre-existing overclaim, carried across the extraction unchanged and
 * deliberately not reworded here (#2274 spec §Design 2). `self_healing` also
 * arises from a watchdog re-enqueue with no prior error, and the adapter
 * suppresses prior errors for `running` / `pending_retry` only — so "hiding
 * prior errors" is not true of every row that shows it. Fixing the copy is a
 * separate change; moving it is not the moment to also alter it.
 */
const PENDING_RETRY_TOOLTIP =
  "hiding prior errors during retry — re-shown if retry also fails or fails to reattempt failed subjects.";

export function VerdictPill({ row }: { row: ProcessRowResponse }) {
  // #1512 — render the single computed verdict, not the raw status.
  const visual = VERDICT_VISUAL[row.health_verdict];
  const tooltip = row.self_healing ? PENDING_RETRY_TOOLTIP : undefined;
  return (
    <Badge
      tone={visual.tone}
      uppercase
      className={visual.extraClass}
      data-testid="status-pill"
      data-verdict={row.health_verdict}
      title={tooltip}
      aria-label={`Health: ${visual.label}`}
    >
      {visual.label}
    </Badge>
  );
}
