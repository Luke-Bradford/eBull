/**
 * Badge — the ONE status-pill primitive (#1908 PR-2, the spec #559 asked for).
 *
 * Before this component, ~130 inline pill class-strings were hand-rolled
 * across ~22 files, and the tone maps in `StanceBadge` / `CriticVerdictBadge`
 * were byte-identical copies of each other. Every one of those strings was a
 * separate line the dark-class gate had to police, and a separate place the
 * operator colour table could drift.
 *
 * Contract (`.claude/skills/frontend/design-system.md`):
 *   - Meaning is carried by the TEXT. Colour is decorative reinforcement,
 *     never the sole signal (a11y). A badge with no readable label is a bug.
 *   - Tones are SEMANTIC, not colour names, and map 1:1 onto the
 *     `operator-ui-conventions.md` colour table:
 *       ok = emerald · warn = amber · risk = red · info = blue · neutral = slate
 *     Callers map their domain enum → tone; they never write colour classes.
 *   - One look: flat, `rounded`, 1px border, no shadow (the #691 line).
 *     Geometry/density comes from `operator-ui-conventions.md`
 *     (pill padding `px-1.5 py-0.5`, pill text `text-[10px] font-medium`).
 *
 * Extra span attributes (`title`, `data-*`, `aria-*`) pass through, so
 * callers keep their hover explanations and test hooks.
 */
import type { ComponentPropsWithoutRef, JSX } from "react";

/** Semantic tone. Maps onto the operator colour table — see module docstring. */
export type BadgeTone = "ok" | "warn" | "risk" | "info" | "neutral";

/**
 * Tone → class pairs. Light and dark utilities are kept on ONE LINE each
 * because `frontend/scripts/check-dark-classes.mjs` reasons line-by-line;
 * splitting a pair across lines would defeat the gate.
 */
const TONE_CLASSES: Record<BadgeTone, string> = {
  ok: "border-emerald-300 bg-emerald-50 text-emerald-700 dark:border-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-300",
  warn: "border-amber-300 bg-amber-50 text-amber-700 dark:border-amber-700 dark:bg-amber-950/40 dark:text-amber-300",
  risk: "border-red-300 bg-red-50 text-red-700 dark:border-red-700 dark:bg-red-950/40 dark:text-red-300",
  info: "border-blue-300 bg-blue-50 text-blue-700 dark:border-blue-700 dark:bg-blue-950/40 dark:text-blue-300",
  neutral:
    "border-slate-300 bg-slate-100 text-slate-700 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300",
};

const BASE = "inline-flex items-center gap-1 rounded border px-1.5 py-0.5 text-[10px] font-medium";

export type BadgeProps = ComponentPropsWithoutRef<"span"> & {
  /** Semantic tone. Defaults to `neutral` — an unmapped value must degrade
   *  to a visible neutral badge, never to a blank or a thrown error. */
  readonly tone?: BadgeTone;
  /** Uppercase the label. Use for short enum vocabularies (BUY / EXIT /
   *  LIVE); leave off for prose labels ("Strong challenge"). */
  readonly uppercase?: boolean;
};

export function Badge({
  tone = "neutral",
  uppercase = false,
  className = "",
  children,
  ...rest
}: BadgeProps): JSX.Element {
  const caps = uppercase ? " uppercase tracking-wide" : "";
  const extra = className === "" ? "" : ` ${className}`;
  return (
    <span className={`${BASE}${caps} ${TONE_CLASSES[tone]}${extra}`} {...rest}>
      {children}
    </span>
  );
}
