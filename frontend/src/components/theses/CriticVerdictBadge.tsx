/**
 * CriticVerdictBadge — pill for the adversarial critic's verdict (#1902).
 *
 * Verdict vocabulary is written by app/services/thesis.py::_validate_critic_output
 * ("Strong challenge" | "Moderate challenge" | "Weak challenge"). Colour
 * semantics follow the operator convention (red = risk): a STRONG challenge
 * means the critic found a strong case AGAINST the thesis, so it renders red;
 * a weak challenge means the thesis survived scrutiny → emerald. Unknown /
 * legacy strings fall back to slate rather than being hidden (#1808 class —
 * the column is open text; never let an unexpected value blank the cell).
 */

const TONE: Record<string, string> = {
  "Strong challenge":
    "bg-red-50 dark:bg-red-950/40 text-red-700 dark:text-red-300 border-red-300 dark:border-red-700",
  "Moderate challenge":
    "bg-amber-50 dark:bg-amber-950/40 text-amber-700 dark:text-amber-300 border-amber-300 dark:border-amber-700",
  "Weak challenge":
    "bg-emerald-50 dark:bg-emerald-950/40 text-emerald-700 dark:text-emerald-300 border-emerald-300 dark:border-emerald-700",
};

const FALLBACK_TONE =
  "bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border-slate-300 dark:border-slate-700";

export function CriticVerdictBadge({
  verdict,
}: {
  readonly verdict: string | null;
}): JSX.Element {
  if (verdict === null) {
    // Stored-without-critic is a legitimate state (critic is best-effort,
    // e.g. length-failure on a large context) — say so instead of blank.
    return (
      <span className="text-xs text-slate-400 dark:text-slate-500">
        no critic
      </span>
    );
  }
  return (
    <span
      className={`inline-block rounded border px-1.5 py-0.5 text-[10px] font-medium ${TONE[verdict] ?? FALLBACK_TONE}`}
    >
      {verdict}
    </span>
  );
}
