/**
 * CriticVerdictBadge — pill for the adversarial critic's verdict (#1902).
 *
 * Verdict vocabulary is written by app/services/thesis.py::_validate_critic_output
 * ("Strong challenge" | "Moderate challenge" | "Weak challenge"). Tone
 * semantics follow the operator convention (risk = red): a STRONG challenge
 * means the critic found a strong case AGAINST the thesis, so it renders
 * `risk`; a weak challenge means the thesis survived scrutiny → `ok`. Unknown /
 * legacy strings fall back to `neutral` rather than being hidden (#1808 class —
 * the column is open text; never let an unexpected value blank the cell).
 *
 * Colour classes live once in `Badge` (#1908); this file owns only the mapping.
 */
import { Badge, type BadgeTone } from "@/components/ui/Badge";

const TONE: Record<string, BadgeTone> = {
  "Strong challenge": "risk",
  "Moderate challenge": "warn",
  "Weak challenge": "ok",
};

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
  return <Badge tone={TONE[verdict] ?? "neutral"}>{verdict}</Badge>;
}
