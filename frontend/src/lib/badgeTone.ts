/**
 * Domain enum → `Badge` tone. ONE mapping per vocabulary (#1908 PR-2).
 *
 * The vocabularies below are settled in
 * `.claude/skills/frontend/operator-ui-conventions.md` ("Status pill
 * vocabulary"). Before this module the action + status maps were re-declared
 * as raw Tailwind strings in BOTH `RecentRecommendations` and
 * `RecommendationsTable` — two copies of one enum, free to drift.
 *
 * Unknown values resolve to `neutral` rather than blank: the backend enum can
 * grow ahead of the frontend, and an unstyled-but-visible pill is always the
 * safer failure (#1808 class).
 */
import type { BadgeTone } from "@/components/ui/Badge";

const ACTION_TONE: Record<string, BadgeTone> = {
  // BUY and ADD share the `ok` tone. They previously differed only by tint
  // depth (emerald-100 vs emerald-50) — a distinction the operator cannot
  // reliably read at pill size, and one the label already carries in text.
  BUY: "ok",
  ADD: "ok",
  HOLD: "neutral",
  EXIT: "risk",
  // Informational only — evaluated but blocked from BUY (#1820).
  CONSIDERED: "warn",
};

const STATUS_TONE: Record<string, BadgeTone> = {
  proposed: "warn",
  approved: "info",
  rejected: "risk",
  executed: "ok",
  considered: "neutral",
};

/** `scores.completeness_tier` — how much of the data surface the score saw. */
const COMPLETENESS_TONE: Record<string, BadgeTone> = {
  full: "ok",
  thin_data: "warn",
  insufficient_data: "risk",
};

export function actionTone(action: string): BadgeTone {
  return ACTION_TONE[action] ?? "neutral";
}

export function statusTone(status: string): BadgeTone {
  return STATUS_TONE[status] ?? "neutral";
}

export function completenessTone(tier: string): BadgeTone {
  return COMPLETENESS_TONE[tier] ?? "neutral";
}
