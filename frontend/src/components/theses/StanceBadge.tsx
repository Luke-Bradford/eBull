/**
 * StanceBadge — pill for the thesis stance enum (#1902).
 *
 * Vocabulary is the settled thesis-semantics set (docs/settled-decisions.md):
 * buy | hold | watch | avoid. Colour follows the operator convention:
 * buy → emerald, hold → slate, watch → amber (attention, not action),
 * avoid → red. Unknown strings fall back to slate rather than hiding.
 */

const TONE: Record<string, string> = {
  buy: "bg-emerald-50 dark:bg-emerald-950/40 text-emerald-700 dark:text-emerald-300 border-emerald-300 dark:border-emerald-700",
  hold: "bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border-slate-300 dark:border-slate-700",
  watch:
    "bg-amber-50 dark:bg-amber-950/40 text-amber-700 dark:text-amber-300 border-amber-300 dark:border-amber-700",
  avoid:
    "bg-red-50 dark:bg-red-950/40 text-red-700 dark:text-red-300 border-red-300 dark:border-red-700",
};

const FALLBACK =
  "bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border-slate-300 dark:border-slate-700";

export function StanceBadge({
  stance,
}: {
  readonly stance: string;
}): JSX.Element {
  return (
    <span
      className={`inline-block rounded border px-1.5 py-0.5 text-[10px] font-medium uppercase ${TONE[stance.toLowerCase()] ?? FALLBACK}`}
    >
      {stance}
    </span>
  );
}
